package ratelimit

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/config"
)

type tokenBucketState struct {
	tokens     float64
	lastRefill time.Time
}

// MemoryLimiter implements both Token Bucket and Sliding Window Log rate limiting
// in-memory with per-key mutex safety and an injectable clock for deterministic tests.
type MemoryLimiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucketState
	windows map[string][]time.Time
	nowFn   func() time.Time
}

func NewMemoryLimiter() *MemoryLimiter {
	return &MemoryLimiter{
		buckets: make(map[string]*tokenBucketState),
		windows: make(map[string][]time.Time),
		nowFn:   time.Now,
	}
}

func NewMemoryLimiterWithClock(nowFn func() time.Time) *MemoryLimiter {
	m := NewMemoryLimiter()
	m.nowFn = nowFn
	return m
}

func (m *MemoryLimiter) Allow(_ context.Context, key string, policy config.RateLimitPolicy) (Decision, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.nowFn()
	if policy.Algorithm == config.AlgoSlidingWindow {
		return m.allowSlidingWindow(key, policy, now), nil
	}
	return m.allowTokenBucket(key, policy, now), nil
}

func (m *MemoryLimiter) allowTokenBucket(key string, policy config.RateLimitPolicy, now time.Time) Decision {
	capacity := float64(policy.Limit)
	refillRate := policy.RefillRate
	if refillRate <= 0 && policy.Window > 0 {
		refillRate = capacity / policy.Window.Seconds()
	}

	b, ok := m.buckets[key]
	if !ok {
		b = &tokenBucketState{tokens: capacity, lastRefill: now}
		m.buckets[key] = b
	}

	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed > 0 {
		b.tokens = math.Min(capacity, b.tokens+elapsed*refillRate)
		b.lastRefill = now
	}

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		remaining := int(math.Floor(b.tokens))
		fullIn := time.Duration(((capacity - b.tokens) / refillRate) * float64(time.Second))
		return Decision{
			Allowed:   true,
			Limit:     policy.Limit,
			Remaining: remaining,
			ResetAt:   now.Add(fullIn),
		}
	}

	needed := 1.0 - b.tokens
	retryAfter := time.Duration(math.Ceil((needed / refillRate) * float64(time.Second)))
	return Decision{
		Allowed:    false,
		Limit:      policy.Limit,
		Remaining:  0,
		ResetAt:    now.Add(retryAfter),
		RetryAfter: retryAfter,
	}
}

func (m *MemoryLimiter) allowSlidingWindow(key string, policy config.RateLimitPolicy, now time.Time) Decision {
	cutoff := now.Add(-policy.Window)
	timestamps := m.windows[key]

	// Prune timestamps outside the sliding window
	idx := 0
	for idx < len(timestamps) && !timestamps[idx].After(cutoff) {
		idx++
	}
	timestamps = timestamps[idx:]

	if len(timestamps) < policy.Limit {
		timestamps = append(timestamps, now)
		m.windows[key] = timestamps
		resetAt := now.Add(policy.Window)
		if len(timestamps) > 0 {
			resetAt = timestamps[0].Add(policy.Window)
		}
		return Decision{
			Allowed:   true,
			Limit:     policy.Limit,
			Remaining: policy.Limit - len(timestamps),
			ResetAt:   resetAt,
		}
	}

	m.windows[key] = timestamps
	oldest := timestamps[0]
	retryAfter := oldest.Add(policy.Window).Sub(now)
	if retryAfter < 0 {
		retryAfter = 0
	}
	return Decision{
		Allowed:    false,
		Limit:      policy.Limit,
		Remaining:  0,
		ResetAt:    oldest.Add(policy.Window),
		RetryAfter: retryAfter,
	}
}
