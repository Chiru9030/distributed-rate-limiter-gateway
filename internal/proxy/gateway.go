package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/auth"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/config"
	apierrors "github.com/Chiru9030/distributed-rate-limiter-gateway/internal/errors"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/ratelimit"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/telemetry"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/web"
)

const maxProxyBodyBytes = 2 * 1024 * 1024 // 2 MiB

type proxyErrorFlagKey struct{}

type pinger interface {
	Ping(ctx context.Context) error
}

type Gateway struct {
	cfg            config.GatewayConfig
	auth           *auth.Authenticator
	limiter        ratelimit.Limiter
	logger         *slog.Logger
	metrics        *telemetry.Metrics
	balancers      map[string]*LoadBalancer
	breakers       map[string]*CircuitBreaker
	allowedOrigins []string
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
				if flag, ok := r.Context().Value(proxyErrorFlagKey{}).(*atomic.Bool); ok && flag != nil {
					flag.Store(true)
				}
				reqID := r.Header.Get("X-Request-ID")
				traceID := r.Header.Get("X-Trace-ID")
				if errors.Is(err, context.Canceled) {
					return
				}
				cb.RecordFailure()
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

	var origins []string
	for _, part := range strings.Split(cfg.AllowedOrigins, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			origins = append(origins, trimmed)
		}
	}

	return &Gateway{
		cfg:            cfg,
		auth:           auth.NewAuthenticator(cfg.Tenants),
		limiter:        limiter,
		logger:         logger,
		metrics:        &telemetry.Metrics{},
		balancers:      balancers,
		breakers:       breakers,
		allowedOrigins: origins,
	}, nil
}

func (g *Gateway) Metrics() *telemetry.Metrics {
	return g.metrics
}

// StartHealthChecks starts active /health probes for every configured load balancer pool.
func (g *Gateway) StartHealthChecks(ctx context.Context, interval time.Duration) {
	for _, lb := range g.balancers {
		if lb != nil {
			lb.StartHealthCheck(ctx, interval)
		}
	}
}

func (g *Gateway) authorizeAdmin(r *http.Request) bool {
	if g.cfg.AdminAPIKey == "" {
		return true
	}
	token := strings.TrimSpace(r.Header.Get("X-API-Key"))
	if token == "" {
		authz := strings.TrimSpace(r.Header.Get("Authorization"))
		if len(authz) > 7 && strings.EqualFold(authz[:7], "bearer ") {
			token = strings.TrimSpace(authz[7:])
		}
	}
	if token == "" {
		return false
	}
	given := sha256.Sum256([]byte(token))
	expected := sha256.Sum256([]byte(g.cfg.AdminAPIKey))
	return subtle.ConstantTimeCompare(given[:], expected[:]) == 1
}

func (g *Gateway) applyCORS(w http.ResponseWriter, r *http.Request) {
	if len(g.allowedOrigins) == 0 {
		return
	}
	reqOrigin := strings.TrimSpace(r.Header.Get("Origin"))
	if len(g.allowedOrigins) == 1 && g.allowedOrigins[0] == "*" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
	} else {
		w.Header().Add("Vary", "Origin")
		matched := ""
		for _, allowed := range g.allowedOrigins {
			if allowed == "*" {
				matched = "*"
				break
			}
			if reqOrigin != "" && strings.EqualFold(allowed, reqOrigin) {
				matched = allowed
				break
			}
		}
		if matched == "" && reqOrigin == "" && len(g.allowedOrigins) == 1 {
			matched = g.allowedOrigins[0]
		}
		if matched != "" {
			w.Header().Set("Access-Control-Allow-Origin", matched)
		}
	}
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-API-Key, Idempotency-Key")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
}

func (g *Gateway) handleAdmin(w http.ResponseWriter, r *http.Request, reqID, traceID string) {
	if !g.authorizeAdmin(r) {
		apierrors.Unauthorized(w, reqID, traceID, "Missing or invalid admin API key")
		return
	}

	switch r.URL.Path {
	case "/v1/admin/status":
		if r.Method != http.MethodGet {
			apierrors.MethodNotAllowed(w, reqID, traceID, "GET required for /v1/admin/status")
			return
		}
		type nodeStatus struct {
			URL     string `json:"url"`
			Weight  int    `json:"weight"`
			Healthy bool   `json:"healthy"`
		}
		type routeStatus struct {
			PathPrefix string       `json:"path_prefix"`
			Strategy   string       `json:"strategy"`
			Nodes      []nodeStatus `json:"nodes"`
		}
		routes := make([]routeStatus, 0, len(g.cfg.Routes))
		for _, rt := range g.cfg.Routes {
			lb := g.balancers[rt.PathPrefix]
			ns := []nodeStatus{}
			if lb != nil {
				for _, n := range lb.Nodes() {
					ns = append(ns, nodeStatus{
						URL:     n.URL.String(),
						Weight:  n.Weight,
						Healthy: n.Healthy.Load(),
					})
				}
			}
			strategy := rt.Strategy
			if strategy == "" {
				strategy = "round_robin"
			}
			routes = append(routes, routeStatus{
				PathPrefix: rt.PathPrefix,
				Strategy:   strategy,
				Nodes:      ns,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"routes": routes})

	case "/v1/admin/tenants":
		if r.Method != http.MethodPost {
			apierrors.MethodNotAllowed(w, reqID, traceID, "POST required for /v1/admin/tenants")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		dec := json.NewDecoder(r.Body)
		var tenant config.TenantConfig
		if err := dec.Decode(&tenant); err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				apierrors.PayloadTooLarge(w, reqID, traceID, "Tenant config exceeds 1 MiB limit")
				return
			}
			apierrors.BadRequest(w, reqID, traceID, "body", "Invalid tenant JSON payload")
			return
		}
		var extra any
		if err := dec.Decode(&extra); err != io.EOF {
			apierrors.BadRequest(w, reqID, traceID, "body", "Unexpected trailing tokens after JSON payload")
			return
		}

		tenant.APIKey = strings.TrimSpace(tenant.APIKey)
		tenant.Tenant = strings.TrimSpace(tenant.Tenant)
		tenant.Tier = strings.TrimSpace(tenant.Tier)
		if tenant.APIKey == "" {
			apierrors.BadRequest(w, reqID, traceID, "APIKey", "APIKey is required")
			return
		}
		if tenant.Tenant == "" {
			apierrors.BadRequest(w, reqID, traceID, "Tenant", "Tenant slug is required")
			return
		}
		if tenant.Policy.Limit <= 0 {
			apierrors.BadRequest(w, reqID, traceID, "Policy.Limit", "Policy.Limit must be positive")
			return
		}
		if tenant.Policy.Window < 0 || tenant.Policy.RefillRate < 0 {
			apierrors.BadRequest(w, reqID, traceID, "Policy", "Window and RefillRate must be non-negative")
			return
		}
		if tenant.Policy.Algorithm == "" {
			tenant.Policy.Algorithm = config.AlgoTokenBucket
		}
		switch tenant.Policy.Algorithm {
		case config.AlgoTokenBucket, config.AlgoSlidingWindow, config.AlgoLeakyBucket:
		default:
			apierrors.BadRequest(w, reqID, traceID, "Policy.Algorithm", "Algorithm must be token_bucket, sliding_window, or leaky_bucket")
			return
		}

		g.auth.UpsertTenant(tenant)
		g.logger.Info("admin_tenant_upserted", "request_id", reqID, "tenant", tenant.Tenant, "algorithm", tenant.Policy.Algorithm)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "tenant_updated", "tenant": tenant.Tenant})

	default:
		apierrors.NotFound(w, reqID, traceID, r.URL.Path)
	}
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Referrer-Policy", "no-referrer")
	g.applyCORS(w, r)

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

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

	if r.Method == http.MethodGet && (r.URL.Path == "/" || r.URL.Path == "/dashboard") {
		data, err := fs.ReadFile(web.DashboardFS, "static/index.html")
		if err != nil {
			http.Error(w, "dashboard asset unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self' https://code.jquery.com; style-src 'self' 'unsafe-inline'; connect-src 'self'")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/dashboard/app.js" {
		data, err := fs.ReadFile(web.DashboardFS, "static/app.js")
		if err != nil {
			http.Error(w, "script asset unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
		return
	}

	if r.URL.Path == "/health" || r.URL.Path == "/healthz" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		return
	}
	if r.URL.Path == "/readyz" {
		redisOK := true
		if p, ok := g.limiter.(pinger); ok && g.cfg.RedisAddr != "" {
			pingCtx, cancel := context.WithTimeout(r.Context(), 1500*time.Millisecond)
			if err := p.Ping(pingCtx); err != nil {
				redisOK = false
			}
			cancel()
		}
		healthyUpstreams := 0
		totalUpstreams := 0
		for _, lb := range g.balancers {
			if lb == nil {
				continue
			}
			for _, n := range lb.Nodes() {
				totalUpstreams++
				if n.Healthy.Load() {
					healthyUpstreams++
				}
			}
		}
		upstreamsOK := totalUpstreams == 0 || healthyUpstreams > 0
		w.Header().Set("Content-Type", "application/json")
		if !redisOK || !upstreamsOK {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":            "unready",
				"redis_ok":          redisOK,
				"healthy_upstreams": healthyUpstreams,
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":            "ready",
			"routes":            len(g.cfg.Routes),
			"redis_configured":  g.cfg.RedisAddr != "",
			"healthy_upstreams": healthyUpstreams,
		})
		return
	}
	if r.URL.Path == "/metrics" {
		g.metrics.ServePrometheus(w, r)
		return
	}

	// All /v1/admin* endpoints are strictly handled in the gateway control plane and never forwarded upstream.
	if r.URL.Path == "/v1/admin" || strings.HasPrefix(r.URL.Path, "/v1/admin/") {
		g.handleAdmin(w, r, reqID, traceID)
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

	// Enforce 2 MiB body cap before proxying so oversized client payloads return 413 without tripping circuit breakers.
	if r.ContentLength > maxProxyBodyBytes {
		rec.Header().Set("Connection", "close")
		apierrors.PayloadTooLarge(rec, reqID, traceID, "Request payload exceeds 2 MiB maximum limit")
		return
	}
	if r.Body != nil && r.Body != http.NoBody {
		limited, readErr := io.ReadAll(io.LimitReader(r.Body, maxProxyBodyBytes+1))
		_ = r.Body.Close()
		if readErr != nil {
			apierrors.BadRequest(rec, reqID, traceID, "body", "Unable to read request body cleanly")
			return
		}
		if int64(len(limited)) > maxProxyBodyBytes {
			apierrors.PayloadTooLarge(rec, reqID, traceID, "Request payload exceeds 2 MiB maximum limit")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(limited))
		r.ContentLength = int64(len(limited))
	}

	cb := g.breakers[route.PathPrefix]
	if cb != nil && !cb.Allow() {
		apierrors.ServiceUnavailable(rec, reqID, traceID, "Upstream circuit breaker is open")
		return
	}

	var errored atomic.Bool
	r = r.WithContext(context.WithValue(r.Context(), proxyErrorFlagKey{}, &errored))
	r.Header.Set("X-Authenticated-Tenant", tenant.Tenant)
	r.Header.Set("X-Authenticated-Tier", tenant.Tier)

	func() {
		defer func() {
			if p := recover(); p != nil {
				if cb != nil && !errored.Load() {
					cb.RecordFailure()
				}
				panic(p)
			}
		}()
		node.Proxy.ServeHTTP(rec, r)
	}()

	if cb != nil && !errored.Load() {
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
