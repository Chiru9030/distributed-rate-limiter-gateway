package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/config"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/proxy"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/ratelimit"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/telemetry"
)

func parseUpstreams(rawURLs, singleURL string, logger *slog.Logger) ([]config.UpstreamTarget, func()) {
	raw := strings.TrimSpace(rawURLs)
	if raw == "" {
		raw = strings.TrimSpace(singleURL)
	}
	if raw != "" {
		parts := strings.Split(raw, ",")
		targets := make([]config.UpstreamTarget, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			uPart, wPart, hasWeight := strings.Cut(p, "|")
			weight := 1
			if hasWeight {
				if parsed, err := strconv.Atoi(strings.TrimSpace(wPart)); err == nil && parsed > 0 {
					weight = parsed
				}
			}
			targets = append(targets, config.UpstreamTarget{URL: strings.TrimSpace(uPart), Weight: weight})
		}
		if len(targets) > 0 {
			return targets, func() {}
		}
	}

	// Self-contained ephemeral loopback upstream pool when UPSTREAM_URL(S) is unset.
	var servers []*http.Server
	var targets []config.UpstreamTarget
	for i, weight := range []int{2, 1} {
		idx := i + 1
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			logger.Error("builtin_upstream_listen_failed", "error", err.Error())
			continue
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		})
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":   "ok",
				"upstream": fmt.Sprintf("builtin-node-%d", idx),
				"path":     r.URL.Path,
				"tenant":   r.Header.Get("X-Authenticated-Tenant"),
			})
		})
		srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		servers = append(servers, srv)
		go func(s *http.Server, l net.Listener) {
			_ = s.Serve(l)
		}(srv, ln)
		targets = append(targets, config.UpstreamTarget{
			URL:    fmt.Sprintf("http://%s", ln.Addr().String()),
			Weight: weight,
		})
	}

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		for _, s := range servers {
			_ = s.Shutdown(ctx)
		}
	}
	return targets, cleanup
}

func main() {
	logger := telemetry.NewLogger()
	cfg := config.DefaultConfig()

	if addr := strings.TrimSpace(os.Getenv("LISTEN_ADDR")); addr != "" {
		cfg.ListenAddr = addr
	} else if port := strings.TrimSpace(os.Getenv("PORT")); port != "" {
		cfg.ListenAddr = ":" + port
	}
	cfg.RedisAddr = strings.TrimSpace(os.Getenv("REDIS_ADDR"))
	cfg.RedisPassword = os.Getenv("REDIS_PASSWORD")
	cfg.AdminAPIKey = strings.TrimSpace(os.Getenv("ADMIN_API_KEY"))
	cfg.AllowedOrigins = strings.TrimSpace(os.Getenv("ALLOWED_ORIGINS"))

	if demoKey := strings.TrimSpace(os.Getenv("GATEWAY_DEMO_API_KEY")); demoKey != "" {
		cfg.Tenants[demoKey] = config.TenantConfig{
			APIKey: demoKey,
			Tenant: "acme_corp",
			Tier:   "pro",
			Policy: config.RateLimitPolicy{
				Algorithm:  config.AlgoTokenBucket,
				Limit:      100,
				Window:     time.Minute,
				RefillRate: 5.0,
			},
		}
	}

	if cfg.AdminAPIKey == "" {
		logger.Warn("admin_api_key_unset", "detail", "/v1/admin/* endpoints are running without admin key enforcement; set ADMIN_API_KEY in production")
	}

	targets, cleanupUpstreams := parseUpstreams(os.Getenv("UPSTREAM_URLS"), os.Getenv("UPSTREAM_URL"), logger)
	defer cleanupUpstreams()

	strategy := strings.TrimSpace(os.Getenv("UPSTREAM_STRATEGY"))
	if strategy == "" {
		strategy = "weighted"
	}
	cfg.Routes = append(cfg.Routes, config.RouteConfig{
		PathPrefix: "/v1/",
		Upstreams:  targets,
		Strategy:   strategy,
	})

	var limiter ratelimit.Limiter
	if cfg.RedisAddr != "" {
		logger.Info("using_redis_rate_limiter", "redis_addr", cfg.RedisAddr)
		limiter = ratelimit.NewRedisLimiterWithAuth(cfg.RedisAddr, cfg.RedisPassword, 32)
	} else {
		logger.Info("using_in_memory_rate_limiter")
		limiter = ratelimit.NewMemoryLimiter()
	}

	gw, err := proxy.NewGateway(cfg, limiter, logger)
	if err != nil {
		logger.Error("gateway_init_failed", "error", err.Error())
		os.Exit(1)
	}

	hcInterval := 5 * time.Second
	if rawHC := strings.TrimSpace(os.Getenv("HEALTH_CHECK_INTERVAL")); rawHC != "" {
		if d, err := time.ParseDuration(rawHC); err == nil && d > 0 {
			hcInterval = d
		}
	}
	hcCtx, cancelHC := context.WithCancel(context.Background())
	defer cancelHC()
	gw.StartHealthChecks(hcCtx, hcInterval)

	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      gw,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		logger.Info("gateway_listening", "addr", cfg.ListenAddr, "upstream_nodes", len(targets), "strategy", strategy)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server_error", "error", err.Error())
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	cancelHC()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("gateway_shutdown_error", "error", err.Error())
	}
	logger.Info("gateway_stopped")
}
