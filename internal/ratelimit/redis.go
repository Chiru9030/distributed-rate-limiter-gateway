package ratelimit

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/config"
)

// Atomic Lua script for distributed Token Bucket in Redis.
// KEYS[1] = bucket key
// ARGV[1] = capacity, ARGV[2] = refill_rate_per_sec, ARGV[3] = now_ms, ARGV[4] = ttl_ms
const tokenBucketLua = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])

local data = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(data[1])
local last_ts = tonumber(data[2])

if tokens == nil then
  tokens = capacity
  last_ts = now_ms
else
  local elapsed_sec = math.max(0, (now_ms - last_ts) / 1000.0)
  tokens = math.min(capacity, tokens + elapsed_sec * rate)
  last_ts = now_ms
end

local allowed = 0
local retry_ms = 0
if tokens >= 1.0 then
  tokens = tokens - 1.0
  allowed = 1
else
  retry_ms = math.ceil(((1.0 - tokens) / rate) * 1000.0)
end

redis.call("HSET", key, "tokens", tostring(tokens), "ts", tostring(last_ts))
redis.call("PEXPIRE", key, ttl_ms)
return {allowed, math.floor(tokens), retry_ms}
`

// Atomic Lua script for distributed Sliding Window Log using Redis ZSET.
// KEYS[1] = zset key
// ARGV[1] = window_ms, ARGV[2] = limit, ARGV[3] = now_ms, ARGV[4] = unique_member
const slidingWindowLua = `
local key = KEYS[1]
local window_ms = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local member = ARGV[4]
local cutoff = now_ms - window_ms

redis.call("ZREMRANGEBYSCORE", key, "-inf", cutoff)
local count = redis.call("ZCARD", key)

if count < limit then
  redis.call("ZADD", key, now_ms, member)
  redis.call("PEXPIRE", key, window_ms)
  return {1, limit - count - 1, 0}
end

local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
local retry_ms = 0
if #oldest >= 2 then
  retry_ms = math.max(0, tonumber(oldest[2]) + window_ms - now_ms)
end
return {0, 0, retry_ms}
`

// RedisLimiter executes atomic Lua rate-limiting scripts against Redis over RESP2.
type RedisLimiter struct {
	addr string
}

func NewRedisLimiter(addr string) *RedisLimiter {
	return &RedisLimiter{addr: addr}
}

func (r *RedisLimiter) Allow(ctx context.Context, key string, policy config.RateLimitPolicy) (Decision, error) {
	now := time.Now()
	nowMs := now.UnixMilli()

	var vals []int64
	var err error

	if policy.Algorithm == config.AlgoSlidingWindow {
		member := fmt.Sprintf("%d-%d", now.UnixNano(), nowMs)
		vals, err = r.evalIntArray(ctx, slidingWindowLua, []string{"rl:sw:" + key},
			strconv.FormatInt(policy.Window.Milliseconds(), 10),
			strconv.Itoa(policy.Limit),
			strconv.FormatInt(nowMs, 10),
			member,
		)
	} else {
		rate := policy.RefillRate
		if rate <= 0 && policy.Window > 0 {
			rate = float64(policy.Limit) / policy.Window.Seconds()
		}
		ttlMs := policy.Window.Milliseconds() * 2
		if ttlMs < 60000 {
			ttlMs = 60000
		}
		vals, err = r.evalIntArray(ctx, tokenBucketLua, []string{"rl:tb:" + key},
			strconv.Itoa(policy.Limit),
			strconv.FormatFloat(rate, 'f', 6, 64),
			strconv.FormatInt(nowMs, 10),
			strconv.FormatInt(ttlMs, 10),
		)
	}

	if err != nil {
		return Decision{}, err
	}
	if len(vals) < 3 {
		return Decision{}, errors.New("unexpected redis lua response length")
	}

	allowed := vals[0] == 1
	remaining := int(vals[1])
	retryAfter := time.Duration(vals[2]) * time.Millisecond
	resetAt := now.Add(policy.Window)
	if !allowed && retryAfter > 0 {
		resetAt = now.Add(retryAfter)
	}

	return Decision{
		Allowed:    allowed,
		Limit:      policy.Limit,
		Remaining:  remaining,
		ResetAt:    resetAt,
		RetryAfter: retryAfter,
	}, nil
}

func (r *RedisLimiter) evalIntArray(ctx context.Context, script string, keys []string, args ...string) ([]int64, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", r.addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	}

	cmdArgs := make([]string, 0, 3+len(keys)+len(args))
	cmdArgs = append(cmdArgs, "EVAL", script, strconv.Itoa(len(keys)))
	cmdArgs = append(cmdArgs, keys...)
	cmdArgs = append(cmdArgs, args...)

	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(cmdArgs))
	for _, a := range cmdArgs {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}

	if _, err := io.WriteString(conn, sb.String()); err != nil {
		return nil, err
	}

	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if strings.HasPrefix(line, "-") {
		return nil, errors.New(strings.TrimPrefix(line, "-"))
	}
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected RESP array, got %q", line)
	}

	n, err := strconv.Atoi(strings.TrimPrefix(line, "*"))
	if err != nil {
		return nil, err
	}
	out := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		item, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		item = strings.TrimSuffix(strings.TrimSuffix(item, "\n"), "\r")
		if !strings.HasPrefix(item, ":") {
			return nil, fmt.Errorf("expected RESP integer, got %q", item)
		}
		v, err := strconv.ParseInt(strings.TrimPrefix(item, ":"), 10, 64)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
