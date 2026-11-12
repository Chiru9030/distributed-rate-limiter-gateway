package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/config"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/proxy"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/ratelimit"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/telemetry"
)

func main() {
	logger := telemetry.NewLogger()
	cfg := config.DefaultConfig()

	if addr := os.Getenv("LISTEN_ADDR"); addr != "" {
		cfg.ListenAddr = addr
	}
	if redisAddr := os.Getenv("REDIS_ADDR"); redisAddr != "" {
		cfg.RedisAddr = redisAddr
	}
	if upstream := os.Getenv("UPSTREAM_URL"); upstream != "" {
		cfg.Routes = append(cfg.Routes, config.RouteConfig{
			PathPrefix: "/v1/",
			Upstream:   upstream,
		})
	} else {
		cfg.Routes = append(cfg.Routes, config.RouteConfig{
			PathPrefix: "/v1/",
			Upstream:   "http://localhost:9000",
		})
	}

	var limiter ratelimit.Limiter
	if cfg.RedisAddr != "" {
		logger.Info("using_redis_rate_limiter", "redis_addr", cfg.RedisAddr)
		limiter = ratelimit.NewRedisLimiter(cfg.RedisAddr)
	} else {
		logger.Info("using_in_memory_rate_limiter")
		limiter = ratelimit.NewMemoryLimiter()
	}

	gw, err := proxy.NewGateway(cfg, limiter, logger)
	if err != nil {
		logger.Error("gateway_init_failed", "error", err.Error())
		os.Exit(1)
	}

	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      gw,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		logger.Info("gateway_listening", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server_error", "error", err.Error())
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	logger.Info("gateway_stopped")
}

