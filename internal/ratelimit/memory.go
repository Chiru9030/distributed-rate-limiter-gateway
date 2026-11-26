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

type leakyBucketState struct {
	water    float64
	lastLeak time.Time
}

// MemoryLimiter implements Token Bucket, Sliding Window Log, and Leaky Bucket rate limiting
// in-memory with per-key mutex safety and an injectable clock for deterministic tests.
type MemoryLimiter struct {
	mu           sync.Mutex
	buckets      map[string]*tokenBucketState
	leakyBuckets map[string]*leakyBucketState
	windows      map[string][]time.Time
	nowFn        func() time.Time
}

func NewMemoryLimiter() *MemoryLimiter {
	return &MemoryLimiter{
		buckets:      make(map[string]*tokenBucketState),
		leakyBuckets: make(map[string]*leakyBucketState),
		windows:      make(map[string][]time.Time),
		nowFn:        time.Now,
	}
}

func NewMemoryLimiterWithClock(nowFn func() time.Time) *MemoryLimiter {
	m := NewMemoryLimiter()
	m.nowFn = nowFn
	return m
}

func normalizePolicy(policy config.RateLimitPolicy) config.RateLimitPolicy {
	if policy.Limit <= 0 {
		policy.Limit = 1
	}
	if policy.Window <= 0 {
		policy.Window = time.Second
	}
	return policy
}

func (m *MemoryLimiter) Allow(_ context.Context, key string, policy config.RateLimitPolicy) (Decision, error) {
	policy = normalizePolicy(policy)

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.nowFn()
	switch policy.Algorithm {
	case config.AlgoSlidingWindow:
		return m.allowSlidingWindow(key, policy, now), nil
	case config.AlgoLeakyBucket:
		return m.allowLeakyBucket(key, policy, now), nil
	default:
		return m.allowTokenBucket(key, policy, now), nil
	}
}

func (m *MemoryLimiter) allowTokenBucket(key string, policy config.RateLimitPolicy, now time.Time) Decision {
	capacity := float64(policy.Limit)
	refillRate := policy.RefillRate
	if refillRate <= 0 && policy.Window > 0 {
		refillRate = capacity / policy.Window.Seconds()
	}
	if refillRate <= 0 {
		refillRate = 1.0
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

	idx := 0
	for idx < len(timestamps) && !timestamps[idx].After(cutoff) {
		idx++
	}
	timestamps = timestamps[idx:]

	if len(timestamps) < policy.Limit {
		timestamps = append(timestamps, now)
		m.windows[key] = timestamps
		resetAt := timestamps[0].Add(policy.Window)
		return Decision{
			Allowed:   true,
			Limit:     policy.Limit,
			Remaining: policy.Limit - len(timestamps),
			ResetAt:   resetAt,
		}
	}

	m.windows[key] = timestamps
	oldest := now
	if len(timestamps) > 0 {
		oldest = timestamps[0]
	}
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

func (m *MemoryLimiter) allowLeakyBucket(key string, policy config.RateLimitPolicy, now time.Time) Decision {
	capacity := float64(policy.Limit)
	leakRate := policy.RefillRate
	if leakRate <= 0 && policy.Window > 0 {
		leakRate = capacity / policy.Window.Seconds()
	}
	if leakRate <= 0 {
		leakRate = 1.0
	}

	b, ok := m.leakyBuckets[key]
	if !ok {
		b = &leakyBucketState{water: 0, lastLeak: now}
		m.leakyBuckets[key] = b
	}

	elapsed := now.Sub(b.lastLeak).Seconds()
	if elapsed > 0 {
		b.water = math.Max(0, b.water-elapsed*leakRate)
		b.lastLeak = now
	}

	if b.water+1.0 <= capacity {
		b.water += 1.0
		remaining := int(math.Floor(capacity - b.water))
		return Decision{
			Allowed:   true,
			Limit:     policy.Limit,
			Remaining: remaining,
			ResetAt:   now.Add(time.Duration((b.water / leakRate) * float64(time.Second))),
		}
	}

	excess := (b.water + 1.0) - capacity
	retryAfter := time.Duration(math.Ceil((excess / leakRate) * float64(time.Second)))
	return Decision{
		Allowed:    false,
		Limit:      policy.Limit,
		Remaining:  0,
		ResetAt:    now.Add(retryAfter),
		RetryAfter: retryAfter,
	}
}
