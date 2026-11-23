package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/config"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/proxy"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/ratelimit"
)

func TestLoadBalancerRoundRobin(t *testing.T) {
	var count1, count2 atomic.Int64

	s1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count1.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"server":1}`))
	}))
	defer s1.Close()

	s2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count2.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"server":2}`))
	}))
	defer s2.Close()

	cfg := config.GatewayConfig{
		UpstreamTimeout: 2 * time.Second,
		Tenants: map[string]config.TenantConfig{
			"sk_test_lb": {
				APIKey: "sk_test_lb",
				Tenant: "lb_tenant",
				Tier:   "scale",
				Policy: config.RateLimitPolicy{
					Algorithm: config.AlgoTokenBucket,
					Limit:     100,
					Window:    time.Minute,
				},
			},
		},
		Routes: []config.RouteConfig{
			{
				PathPrefix: "/v1/api",
				Upstreams: []config.UpstreamTarget{
					{URL: s1.URL, Weight: 1},
					{URL: s2.URL, Weight: 1},
				},
				Strategy: "round_robin",
			},
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	gw, err := proxy.NewGateway(cfg, ratelimit.NewMemoryLimiter(), logger)
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}

	srv := httptest.NewServer(gw)
	defer srv.Close()

	client := &http.Client{}
	for i := 0; i < 10; i++ {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/api/data", nil)
		req.Header.Set("Authorization", "Bearer sk_test_lb")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
	}

	if count1.Load() != 5 || count2.Load() != 5 {
		t.Fatalf("expected 5 requests each to s1 and s2, got s1=%d, s2=%d", count1.Load(), count2.Load())
	}
}

func TestLeakyBucketTrafficShaping(t *testing.T) {
	now := time.Unix(1700000000, 0)
	limiter := ratelimit.NewMemoryLimiterWithClock(func() time.Time { return now })

	policy := config.RateLimitPolicy{
		Algorithm:  config.AlgoLeakyBucket,
		Limit:      3,
		Window:     3 * time.Second,
		RefillRate: 1.0, // leaks 1 request per second
	}

	ctx := context.Background()

	// 1-3 fill bucket to capacity
	for i := 0; i < 3; i++ {
		d, err := limiter.Allow(ctx, "leaky:key", policy)
		if err != nil || !d.Allowed {
			t.Fatalf("expected request %d to be allowed", i)
		}
	}

	// 4th request overflows leaky bucket -> rejected with retry after
	d4, _ := limiter.Allow(ctx, "leaky:key", policy)
	if d4.Allowed {
		t.Fatal("expected 4th request to overflow leaky bucket")
	}
	if d4.RetryAfter <= 0 {
		t.Fatalf("expected positive RetryAfter, got %v", d4.RetryAfter)
	}

	// Advance clock by 1 second -> 1 item leaked out
	now = now.Add(time.Second)
	d5, _ := limiter.Allow(ctx, "leaky:key", policy)
	if !d5.Allowed {
		t.Fatal("expected request after 1s leak to be allowed")
	}
}

func TestDynamicAdminTenantHotReload(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	cfg := config.GatewayConfig{
		UpstreamTimeout: 2 * time.Second,
		Tenants:         map[string]config.TenantConfig{}, // Empty initial tenants
		Routes: []config.RouteConfig{
			{PathPrefix: "/v1/", Upstream: upstream.URL},
		},
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	gw, err := proxy.NewGateway(cfg, ratelimit.NewMemoryLimiter(), logger)
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(gw)
	defer srv.Close()

	// Request with unknown key initially returns 401 Unauthorized
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/items", nil)
	req.Header.Set("Authorization", "Bearer sk_new_hot_tenant")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}

	// Hot reload tenant via POST /v1/admin/tenants
	newTenant := config.TenantConfig{
		APIKey: "sk_new_hot_tenant",
		Tenant: "hot_company",
		Tier:   "enterprise",
		Policy: config.RateLimitPolicy{
			Algorithm: config.AlgoTokenBucket,
			Limit:     50,
			Window:    time.Minute,
		},
	}
	body, _ := json.Marshal(newTenant)
	adminReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/admin/tenants", bytes.NewReader(body))
	adminResp, err := http.DefaultClient.Do(adminReq)
	if err != nil {
		t.Fatal(err)
	}
	adminResp.Body.Close()
	if adminResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from admin hot reload, got %d", adminResp.StatusCode)
	}

	// Now the request succeeds immediately with 200 OK!
	req2, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/items", nil)
	req2.Header.Set("Authorization", "Bearer sk_new_hot_tenant")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 after hot reload, got %d", resp2.StatusCode)
	}
}
