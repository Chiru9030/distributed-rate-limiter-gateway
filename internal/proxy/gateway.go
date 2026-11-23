package proxy

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/auth"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/config"
	apierrors "github.com/Chiru9030/distributed-rate-limiter-gateway/internal/errors"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/ratelimit"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/telemetry"
)

type Gateway struct {
	cfg       config.GatewayConfig
	auth      *auth.Authenticator
	limiter   ratelimit.Limiter
	logger    *slog.Logger
	metrics   *telemetry.Metrics
	balancers map[string]*LoadBalancer
	breakers  map[string]*CircuitBreaker
}

func NewGateway(
	cfg config.GatewayConfig,
	limiter ratelimit.Limiter,
	logger *slog.Logger,
) (*Gateway, error) {
	balancers := make(map[string]*LoadBalancer, len(cfg.Routes))
	breakers := make(map[string]*CircuitBreaker, len(cfg.Routes))

	for _, route := range cfg.Routes {
		cb := NewCircuitBreaker(5, 15*time.Second)
		breakers[route.PathPrefix] = cb

		// Build upstream nodes
		var targets []config.UpstreamTarget
		if len(route.Upstreams) > 0 {
			targets = route.Upstreams
		} else if route.Upstream != "" {
			targets = []config.UpstreamTarget{{URL: route.Upstream, Weight: 1}}
		}

		nodes := make([]*UpstreamNode, 0, len(targets))
		for _, t := range targets {
			u, err := url.Parse(t.URL)
			if err != nil {
				return nil, err
			}
			rp := httputil.NewSingleHostReverseProxy(u)
			rp.Transport = &http.Transport{
				ResponseHeaderTimeout: cfg.UpstreamTimeout,
				MaxIdleConnsPerHost:   64,
			}
			rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
				cb.RecordFailure()
				reqID := r.Header.Get("X-Request-ID")
				traceID := r.Header.Get("X-Trace-ID")
				logger.Error("upstream_error", "request_id", reqID, "trace_id", traceID, "error", err.Error())
				apierrors.BadGateway(w, reqID, traceID, "Upstream service failed to respond")
			}
			node := &UpstreamNode{
				URL:    u,
				Weight: t.Weight,
				Proxy:  rp,
			}
			node.Healthy.Store(true)
			nodes = append(nodes, node)
		}

		balancers[route.PathPrefix] = NewLoadBalancer(nodes, route.Strategy)
	}

	return &Gateway{
		cfg:       cfg,
		auth:      auth.NewAuthenticator(cfg.Tenants),
		limiter:   limiter,
		logger:    logger,
		metrics:   &telemetry.Metrics{},
		balancers: balancers,
		breakers:  breakers,
	}, nil
}

func (g *Gateway) Metrics() *telemetry.Metrics {
	return g.metrics
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqID := r.Header.Get("X-Request-ID")
	if reqID == "" {
		reqID = "req_" + randomHex(8)
	}
	traceID := r.Header.Get("X-Trace-ID")
	if traceID == "" {
		traceID = "tr_" + randomHex(12)
	}

	r.Header.Set("X-Request-ID", reqID)
	r.Header.Set("X-Trace-ID", traceID)
	w.Header().Set("X-Request-ID", reqID)
	w.Header().Set("X-Trace-ID", traceID)

	if r.URL.Path == "/health" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		return
	}
	if r.URL.Path == "/metrics" {
		g.metrics.ServePrometheus(w, r)
		return
	}

	// V2: Dynamic Hot-Reloading of Tenant Tiers
	if r.Method == http.MethodPost && r.URL.Path == "/v1/admin/tenants" {
		var tenant config.TenantConfig
		if err := json.NewDecoder(r.Body).Decode(&tenant); err != nil || tenant.APIKey == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid tenant config"})
			return
		}
		g.auth.UpsertTenant(tenant)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "tenant_updated", "tenant": tenant.Tenant})
		return
	}

	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	tenantName := "anonymous"

	defer func() {
		latency := time.Since(start)
		g.metrics.Record(rec.status, latency)
		g.logger.Info("http_request",
			"request_id", reqID,
			"trace_id", traceID,
			"tenant", tenantName,
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"latency_ms", float64(latency.Microseconds())/1000.0,
		)
	}()

	tenant, ok := g.auth.Authenticate(r)
	if !ok {
		apierrors.Unauthorized(rec, reqID, traceID, "Missing or invalid API key")
		return
	}
	tenantName = tenant.Tenant

	route, balancer := g.matchRoute(r.URL.Path)
	if balancer == nil {
		apierrors.NotFound(rec, reqID, traceID, r.URL.Path)
		return
	}

	node := balancer.Next()
	if node == nil || node.Proxy == nil {
		apierrors.BadGateway(rec, reqID, traceID, "No available upstream instances in load balancer pool")
		return
	}

	policy := tenant.Policy
	if route.Policy != nil {
		policy = *route.Policy
	}

	rlKey := tenant.Tenant + ":" + route.PathPrefix
	decision, err := g.limiter.Allow(r.Context(), rlKey, policy)
	if err != nil {
		// Fail-open or fail-closed? Log and fail-open with telemetry if Redis is transiently unreachable
		g.logger.Warn("rate_limiter_error", "request_id", reqID, "error", err.Error())
	} else {
		rec.Header().Set("X-RateLimit-Limit", strconv.Itoa(decision.Limit))
		rec.Header().Set("X-RateLimit-Remaining", strconv.Itoa(decision.Remaining))
		rec.Header().Set("X-RateLimit-Reset", strconv.FormatInt(decision.ResetAt.Unix(), 10))
		if !decision.Allowed {
			retrySec := int(math.Max(1, math.Ceil(decision.RetryAfter.Seconds())))
			rec.Header().Set("Retry-After", strconv.Itoa(retrySec))
			apierrors.RateLimited(rec, reqID, traceID, retrySec)
			return
		}
	}

	cb := g.breakers[route.PathPrefix]
	if cb != nil && !cb.Allow() {
		apierrors.ServiceUnavailable(rec, reqID, traceID, "Upstream circuit breaker is open")
		return
	}

	r.Body = http.MaxBytesReader(rec, r.Body, 2*1024*1024)
	r.Header.Set("X-Authenticated-Tenant", tenant.Tenant)
	r.Header.Set("X-Authenticated-Tier", tenant.Tier)
	node.Proxy.ServeHTTP(rec, r)
	if cb != nil {
		if rec.status >= 500 {
			cb.RecordFailure()
		} else {
			cb.RecordSuccess()
		}
	}
}

func (g *Gateway) matchRoute(path string) (config.RouteConfig, *LoadBalancer) {
	var best config.RouteConfig
	var bestBalancer *LoadBalancer
	for _, route := range g.cfg.Routes {
		if strings.HasPrefix(path, route.PathPrefix) && len(route.PathPrefix) > len(best.PathPrefix) {
			best = route
			bestBalancer = g.balancers[route.PathPrefix]
		}
	}
	return best, bestBalancer
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func randomHex(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

