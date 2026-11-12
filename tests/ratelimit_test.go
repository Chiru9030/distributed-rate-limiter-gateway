package tests

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/config"
	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/ratelimit"
)

func TestTokenBucketRefillAndBurst(t *testing.T) {
	now := time.Unix(1700000000, 0)
	limiter := ratelimit.NewMemoryLimiterWithClock(func() time.Time { return now })

	policy := config.RateLimitPolicy{
		Algorithm:  config.AlgoTokenBucket,
		Limit:      3,
		Window:     3 * time.Second,
		RefillRate: 1.0, // 1 token per second
	}

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		d, err := limiter.Allow(ctx, "tenant:route", policy)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !d.Allowed {
			t.Fatalf("expected request %d to be allowed", i)
		}
	}

	// 4th immediate request should be rejected
	d, _ := limiter.Allow(ctx, "tenant:route", policy)
	if d.Allowed {
		t.Fatal("expected 4th request to be rate-limited")
	}
	if d.RetryAfter <= 0 {
		t.Fatalf("expected positive RetryAfter, got %v", d.RetryAfter)
	}

	// Advance clock by 2 seconds -> 2 tokens refilled
	now = now.Add(2 * time.Second)
	for i := 0; i < 2; i++ {
		d, _ = limiter.Allow(ctx, "tenant:route", policy)
		if !d.Allowed {
			t.Fatalf("expected refilled request %d to be allowed", i)
		}
	}
	d, _ = limiter.Allow(ctx, "tenant:route", policy)
	if d.Allowed {
		t.Fatal("expected bucket to be empty again")
	}
}

func TestSlidingWindowLogPrecision(t *testing.T) {
	now := time.Unix(1700000000, 0)
	limiter := ratelimit.NewMemoryLimiterWithClock(func() time.Time { return now })

	policy := config.RateLimitPolicy{
		Algorithm: config.AlgoSlidingWindow,
		Limit:     2,
		Window:    10 * time.Second,
	}

	ctx := context.Background()
	d1, _ := limiter.Allow(ctx, "sw:key", policy)
	now = now.Add(4 * time.Second)
	d2, _ := limiter.Allow(ctx, "sw:key", policy)
	if !d1.Allowed || !d2.Allowed {
		t.Fatal("expected first 2 requests in window to pass")
	}

	// At t=5s, still 2 requests in [t-10s, t] -> rejected
	now = now.Add(1 * time.Second)
	d3, _ := limiter.Allow(ctx, "sw:key", policy)
	if d3.Allowed {
		t.Fatal("expected 3rd request within sliding window to be rejected")
	}

	// At t=11s, first request (t=0s) expired -> 1 slot opens
	now = now.Add(6 * time.Second)
	d4, _ := limiter.Allow(ctx, "sw:key", policy)
	if !d4.Allowed {
		t.Fatal("expected request after oldest timestamp expired to be allowed")
	}
}

func TestConcurrentGoroutineRateLimitSafety(t *testing.T) {
	limiter := ratelimit.NewMemoryLimiter()
	policy := config.RateLimitPolicy{
		Algorithm:  config.AlgoSlidingWindow,
		Limit:      15,
		Window:     time.Minute,
	}

	var allowed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := limiter.Allow(context.Background(), "concurrent-key", policy)
			if err == nil && d.Allowed {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := allowed.Load(); got != 15 {
		t.Fatalf("expected exactly 15 allowed under concurrency, got %d", got)
	}
}

