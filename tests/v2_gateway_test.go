package tests

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
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

func TestWeightedLoadBalancerAndAdminProtection(t *testing.T) {
	var hits1, hits2 atomic.Int64
	s1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		hits1.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer s1.Close()

	s2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		hits2.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer s2.Close()

	cfg := config.GatewayConfig{
		UpstreamTimeout: 2 * time.Second,
		AdminAPIKey:     "gw_admin_secret_99",
		AllowedOrigins:  "https://console.example.com",
		Tenants: map[string]config.TenantConfig{
			"sk_weighted_user": {
				APIKey: "sk_weighted_user",
				Tenant: "weighted_co",
				Tier:   "scale",
				Policy: config.RateLimitPolicy{Algorithm: config.AlgoTokenBucket, Limit: 50, Window: time.Minute},
			},
		},
		Routes: []config.RouteConfig{
			{
				PathPrefix: "/v1/weighted",
				Upstreams: []config.UpstreamTarget{
					{URL: s1.URL, Weight: 3},
					{URL: s2.URL, Weight: 1},
				},
				Strategy: "weighted",
			},
		},
	}

	gw, err := proxy.NewGateway(cfg, ratelimit.NewMemoryLimiter(), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(gw)
	defer srv.Close()

	// 1. Admin endpoints reject missing or wrong AdminAPIKey
	unauthRes, err := http.Get(srv.URL + "/v1/admin/status")
	if err != nil {
		t.Fatal(err)
	}
	unauthRes.Body.Close()
	if unauthRes.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 on protected /v1/admin/status, got %d", unauthRes.StatusCode)
	}

	unauthPost, _ := http.Post(srv.URL+"/v1/admin/tenants", "application/json", bytes.NewBufferString(`{"APIKey":"k","Tenant":"t","Policy":{"Limit":5}}`))
	unauthPost.Body.Close()
	if unauthPost.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 on protected /v1/admin/tenants, got %d", unauthPost.StatusCode)
	}

	// 2. Authorized /v1/admin/status & invalid payload rejection
	reqStatus, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/admin/status", nil)
	reqStatus.Header.Set("X-API-Key", "gw_admin_secret_99")
	resStatus, _ := http.DefaultClient.Do(reqStatus)
	resStatus.Body.Close()
	if resStatus.StatusCode != http.StatusOK || resStatus.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("expected 200 and nosniff security header, got %d", resStatus.StatusCode)
	}

	// 3. Weighted distribution: 8 requests across weights (3, 1) -> 6 hits s1, 2 hits s2
	for i := 0; i < 8; i++ {
		r, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/weighted/ok", nil)
		r.Header.Set("Authorization", "Bearer sk_weighted_user")
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 weighted call %d, got %d", i, resp.StatusCode)
		}
	}
	if hits1.Load() != 6 || hits2.Load() != 2 {
		t.Fatalf("expected 3:1 weighted distribution (6,2), got (%d,%d)", hits1.Load(), hits2.Load())
	}

	// 4. Dashboard & /readyz
	dashRes, _ := http.Get(srv.URL + "/dashboard")
	dashBody, _ := io.ReadAll(dashRes.Body)
	dashRes.Body.Close()
	if dashRes.StatusCode != http.StatusOK || !bytes.Contains(dashBody, []byte("/dashboard/app.js")) {
		t.Fatalf("expected /dashboard HTML with app.js script, status=%d", dashRes.StatusCode)
	}
	readyRes, _ := http.Get(srv.URL + "/readyz")
	readyRes.Body.Close()
	if readyRes.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 /readyz, got %d", readyRes.StatusCode)
	}
}

func startMockRESP2Server(t *testing.T, password string, evalFn func(callIdx int, args []string) string) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var callCounter atomic.Int32
	var closed atomic.Bool
	go func() {
		for !closed.Load() {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				for {
					line, err := br.ReadString('\n')
					if err != nil {
						return
					}
					line = strings.TrimSpace(line)
					if !strings.HasPrefix(line, "*") {
						return
					}
					n, err := strconv.Atoi(line[1:])
					if err != nil || n <= 0 {
						return
					}
					args := make([]string, 0, n)
					for i := 0; i < n; i++ {
						hdr, err := br.ReadString('\n')
						if err != nil {
							return
						}
						sz, _ := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(hdr, "$")))
						buf := make([]byte, sz+2)
						if _, err := io.ReadFull(br, buf); err != nil {
							return
						}
						args = append(args, string(buf[:sz]))
					}
					cmd := strings.ToUpper(args[0])
					switch cmd {
					case "AUTH":
						if password != "" && (len(args) < 2 || args[1] != password) {
							_, _ = io.WriteString(c, "-WRONGPASS invalid password\r\n")
							return
						}
						_, _ = io.WriteString(c, "+OK\r\n")
					case "PING":
						_, _ = io.WriteString(c, "+PONG\r\n")
					case "EVAL":
						idx := int(callCounter.Add(1))
						resp := evalFn(idx, args)
						if resp == "DROP_EOF" {
							return
						}
						_, _ = io.WriteString(c, resp)
					default:
						_, _ = io.WriteString(c, "+OK\r\n")
					}
				}
			}(conn)
		}
	}()
	return ln.Addr().String(), func() {
		closed.Store(true)
		_ = ln.Close()
	}
}

func TestGatewayV1AuditRegressions(t *testing.T) {
	// 1. RedisLimiter AUTH, Ping, Token Bucket, Sliding Window, Leaky Bucket, stale-EOF reconnect & tiny rate format
	var seenTinyRate atomic.Bool
	addr, stopRedis := startMockRESP2Server(t, "redis_pass", func(callIdx int, args []string) string {
		if callIdx == 2 {
			// Verify tiny rate formatting is scientific/exact ('g') rather than truncated 0.000000
			for _, a := range args {
				if strings.Contains(a, "e-") || strings.HasPrefix(a, "3.85") {
					seenTinyRate.Store(true)
				}
			}
			// Simulate abrupt EOF on pooled socket once to test transparent eviction + retry
			return "DROP_EOF"
		}
		if len(args) >= 6 && strings.HasPrefix(args[4], "rl:sw:") {
			return "*3\r\n:1\r\n:4\r\n:0\r\n"
		}
		return "*3\r\n:1\r\n:9\r\n:0\r\n"
	})
	defer stopRedis()

	rl := ratelimit.NewRedisLimiterWithAuth(addr, "redis_pass", 4)
	if err := rl.Ping(context.Background()); err != nil {
		t.Fatalf("expected Redis PING success, got %v", err)
	}
	d1, err := rl.Allow(context.Background(), "k1", config.RateLimitPolicy{Algorithm: config.AlgoTokenBucket, Limit: 10, Window: time.Minute})
	if err != nil || !d1.Allowed {
		t.Fatalf("expected initial token bucket allow, got d=%+v err=%v", d1, err)
	}
	// Second call hits DROP_EOF once on pooled socket -> must reconnect transparently cleanly!
	d2, err := rl.Allow(context.Background(), "k2", config.RateLimitPolicy{Algorithm: config.AlgoTokenBucket, Limit: 1, Window: 30 * 24 * time.Hour})
	if err != nil || !d2.Allowed || !seenTinyRate.Load() {
		t.Fatalf("expected transparent pooled reconnect and exact tiny rate, got d=%+v err=%v tiny=%v", d2, err, seenTinyRate.Load())
	}
	dSW, err := rl.Allow(context.Background(), "sw1", config.RateLimitPolicy{Algorithm: config.AlgoSlidingWindow, Limit: 5, Window: time.Minute})
	if err != nil || !dSW.Allowed {
		t.Fatalf("expected Redis sliding window allow, got d=%+v err=%v", dSW, err)
	}
	dLB, err := rl.Allow(context.Background(), "lb1", config.RateLimitPolicy{Algorithm: config.AlgoLeakyBucket, Limit: 5, Window: time.Minute, RefillRate: 2})
	if err != nil || !dLB.Allowed {
		t.Fatalf("expected Redis leaky bucket allow, got d=%+v err=%v", dLB, err)
	}

	// Ensure MemoryLimiter Limit<=0 / Window<=0 does not panic on sliding_window
	mem := ratelimit.NewMemoryLimiter()
	if _, err := mem.Allow(context.Background(), "zero", config.RateLimitPolicy{Algorithm: config.AlgoSlidingWindow, Limit: 0, Window: 0}); err != nil {
		t.Fatalf("unexpected error on normalized zero policy: %v", err)
	}

	// 2. Oversized 2MiB+100 body returns 413 WITHOUT opening circuit breaker; single-counted 5xx threshold + 404/503 + admin protection
	var failUpstream atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if failUpstream.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	cfg := config.GatewayConfig{
		UpstreamTimeout: 2 * time.Second,
		AdminAPIKey:     "adm_secret_1",
		AllowedOrigins:  "https://one.example.com, https://two.example.com",
		Tenants: map[string]config.TenantConfig{
			"tok_client": {
				APIKey: "tok_client",
				Tenant: "client_co",
				Tier:   "pro",
				Policy: config.RateLimitPolicy{Algorithm: config.AlgoTokenBucket, Limit: 500, Window: time.Minute},
			},
		},
		Routes: []config.RouteConfig{
			{PathPrefix: "/v1/", Upstream: upstream.URL},
		},
	}

	gw, err := proxy.NewGateway(cfg, ratelimit.NewMemoryLimiter(), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	hcCtx, cancelHC := context.WithCancel(context.Background())
	defer cancelHC()
	gw.StartHealthChecks(hcCtx, 40*time.Millisecond)

	srv := httptest.NewServer(gw)
	defer srv.Close()

	// CORS multi-origin Vary check
	corsReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/metrics", nil)
	corsReq.Header.Set("Origin", "https://two.example.com")
	corsRes, _ := http.DefaultClient.Do(corsReq)
	corsRes.Body.Close()
	if corsRes.Header.Get("Access-Control-Allow-Origin") != "https://two.example.com" || corsRes.Header.Get("Vary") != "Origin" {
		t.Fatalf("unexpected multi-origin CORS: origin=%q vary=%q", corsRes.Header.Get("Access-Control-Allow-Origin"), corsRes.Header.Get("Vary"))
	}

	// Admin subpath bypass must NEVER reach upstream!
	for _, path := range []string{"/v1/admin/tenants", "/v1/admin/extra"} {
		r, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		r.Header.Set("Authorization", "Bearer tok_client") // tenant key, NOT admin key
		resp, _ := http.DefaultClient.Do(r)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("expected 401 blocking admin path %s even with valid tenant token, got %d", path, resp.StatusCode)
		}
	}

	// Admin tenant key with whitespace is trimmed cleanly and usable via trimmed Bearer token
	admPayload := `{"APIKey":"  trimmed_key_42  ","Tenant":"trimmed_co","Tier":"pro","Policy":{"Limit":25}}`
	admReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/admin/tenants", strings.NewReader(admPayload))
	admReq.Header.Set("X-API-Key", "adm_secret_1")
	admRes, _ := http.DefaultClient.Do(admReq)
	admRes.Body.Close()
	if admRes.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 registering trimmed tenant key, got %d", admRes.StatusCode)
	}
	trimmedReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/ok", nil)
	trimmedReq.Header.Set("Authorization", "Bearer trimmed_key_42")
	trimmedRes, _ := http.DefaultClient.Do(trimmedReq)
	trimmedRes.Body.Close()
	if trimmedRes.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 using trimmed tenant key, got %d", trimmedRes.StatusCode)
	}

	// 404 on unmatched path
	nfReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/v2/missing", nil)
	nfReq.Header.Set("Authorization", "Bearer tok_client")
	nfRes, _ := http.DefaultClient.Do(nfReq)
	nfRes.Body.Close()
	if nfRes.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 NotFound, got %d", nfRes.StatusCode)
	}

	// 4 consecutive >2MiB bodies return 413 cleanly and do NOT trip circuit breaker
	bigBody := bytes.Repeat([]byte("a"), (2*1024*1024)+64)
	for i := 0; i < 6; i++ {
		bigReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/upload", bytes.NewReader(bigBody))
		bigReq.Header.Set("Authorization", "Bearer tok_client")
		bigRes, _ := http.DefaultClient.Do(bigReq)
		bigRes.Body.Close()
		if bigRes.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("expected 413 PayloadTooLarge, got %d", bigRes.StatusCode)
		}
	}
	okReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/upload", nil)
	okReq.Header.Set("Authorization", "Bearer tok_client")
	okRes, _ := http.DefaultClient.Do(okReq)
	okRes.Body.Close()
	if okRes.StatusCode != http.StatusOK {
		t.Fatalf("oversized 413 requests poisoned circuit breaker; expected 200, got %d", okRes.StatusCode)
	}

	// Upstream 5xx threshold is strictly 5 (not double-counted -> 6th call is 503 circuit_open)
	failUpstream.Store(true)
	for i := 0; i < 5; i++ {
		r, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/err", nil)
		r.Header.Set("Authorization", "Bearer tok_client")
		resp, _ := http.DefaultClient.Do(r)
		resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("expected 500 on failure %d before threshold=5, got %d", i+1, resp.StatusCode)
		}
	}
	openReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/err", nil)
	openReq.Header.Set("Authorization", "Bearer tok_client")
	openRes, _ := http.DefaultClient.Do(openReq)
	openRes.Body.Close()
	if openRes.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 circuit_open on 6th call after 5 failures, got %d", openRes.StatusCode)
	}

	// Shutting down upstream causes active health checks to mark node unhealthy -> /readyz returns 503
	upstream.Close()
	deadline := time.Now().Add(2 * time.Second)
	var readyCode int
	for time.Now().Before(deadline) {
		rr, _ := http.Get(srv.URL + "/readyz")
		readyCode = rr.StatusCode
		rr.Body.Close()
		if readyCode == http.StatusServiceUnavailable {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if readyCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 /readyz after upstream failure & health check, got %d", readyCode)
	}
	_ = fmt.Sprintf("verified")
}
