package tests

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/config"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/proxy"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/ratelimit"
)

func TestGatewayAuthRoutingRateLimitAndTelemetry(t *testing.T) {
	// Mock upstream service
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"ok":         "true",
			"tenant":     r.Header.Get("X-Authenticated-Tenant"),
			"request_id": r.Header.Get("X-Request-ID"),
			"trace_id":   r.Header.Get("X-Trace-ID"),
		})
	}))
	defer upstream.Close()

	cfg := config.GatewayConfig{
		UpstreamTimeout: 2 * time.Second,
		Tenants: map[string]config.TenantConfig{
			"sk_test_123": {
				APIKey: "sk_test_123",
				Tenant: "acme",
				Tier:   "pro",
				Policy: config.RateLimitPolicy{
					Algorithm: config.AlgoSlidingWindow,
					Limit:     2,
					Window:    time.Minute,
				},
			},
		},
		Routes: []config.RouteConfig{
			{PathPrefix: "/v1/", Upstream: upstream.URL},
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	gw, err := proxy.NewGateway(cfg, ratelimit.NewMemoryLimiter(), logger)
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}

	srv := httptest.NewServer(gw)
	defer srv.Close()

	// 1. Unauthenticated request -> 401 standardized JSON error
	res, err := http.Get(srv.URL + "/v1/charges")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", res.StatusCode)
	}
	if res.Header.Get("X-Request-ID") == "" || res.Header.Get("X-Trace-ID") == "" {
		t.Fatal("expected X-Request-ID and X-Trace-ID headers on error response")
	}

	// 2. Two authenticated requests succeed with rate-limit headers
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/charges", nil)
		req.Header.Set("Authorization", "Bearer sk_test_123")
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 on request %d, got %d", i, r.StatusCode)
		}
		if body["tenant"] != "acme" {
			t.Fatalf("expected upstream to receive tenant=acme, got %q", body["tenant"])
		}
	}

	// 3. Third request -> 429 Too Many Requests with Retry-After
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/charges", nil)
	req.Header.Set("X-API-Key", "sk_test_123")
	r3, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r3.Body.Close()
	if r3.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", r3.StatusCode)
	}
	if r3.Header.Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header on 429 response")
	}

	// 4. /metrics exposes Prometheus counters
	mRes, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	mBody, _ := io.ReadAll(mRes.Body)
	mRes.Body.Close()
	if !strings.Contains(string(mBody), "gateway_rate_limited_total 1") {
		t.Fatalf("expected gateway_rate_limited_total 1 in metrics, got:\n%s", string(mBody))
	}
}

func TestCircuitBreakerStateTransitions(t *testing.T) {
	cb := proxy.NewCircuitBreaker(2, 50*time.Millisecond)
	if !cb.Allow() {
		t.Fatal("expected closed circuit to allow requests")
	}

	cb.RecordFailure()
	cb.RecordFailure()
	if cb.Allow() {
		t.Fatal("expected open circuit after threshold failures")
	}

	time.Sleep(60 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("expected half-open probe after reset timeout")
	}
	cb.RecordSuccess()
	if !cb.Allow() {
		t.Fatal("expected circuit to close after successful probe")
	}
}

