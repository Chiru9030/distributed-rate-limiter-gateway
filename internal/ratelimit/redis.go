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
	"sync/atomic"
	"time"

	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/config"
)

// Atomic Lua script for distributed Token Bucket in Redis.
const tokenBucketLua = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
if rate == nil or rate <= 0 then
  rate = 1
end
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
const slidingWindowLua = `
local key = KEYS[1]
local window_ms = tonumber(ARGV[1])
if window_ms == nil or window_ms <= 0 then
  window_ms = 1000
end
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

// Atomic Lua script for distributed Leaky Bucket traffic shaping in Redis.
const leakyBucketLua = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local leak_rate = tonumber(ARGV[2])
if leak_rate == nil or leak_rate <= 0 then
  leak_rate = 1
end
local now_ms = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])

local data = redis.call("HMGET", key, "water", "ts")
local water = tonumber(data[1])
local last_ts = tonumber(data[2])

if water == nil then
  water = 0
  last_ts = now_ms
else
  local elapsed_sec = math.max(0, (now_ms - last_ts) / 1000.0)
  water = math.max(0, water - elapsed_sec * leak_rate)
  last_ts = now_ms
end

local allowed = 0
local retry_ms = 0
if water + 1.0 <= capacity then
  water = water + 1.0
  allowed = 1
else
  local excess = (water + 1.0) - capacity
  retry_ms = math.ceil((excess / leak_rate) * 1000.0)
end

redis.call("HSET", key, "water", tostring(water), "ts", tostring(last_ts))
redis.call("PEXPIRE", key, ttl_ms)
return {allowed, math.floor(math.max(0, capacity - water)), retry_ms}
`

type pooledRedisConn struct {
	conn net.Conn
	br   *bufio.Reader
}

// RedisLimiter executes atomic Lua rate-limiting scripts against Redis over a pooled RESP2 connection.
type RedisLimiter struct {
	addr      string
	password  string
	pool      chan *pooledRedisConn
	memberSeq atomic.Uint64
}

func NewRedisLimiter(addr string) *RedisLimiter {
	return NewRedisLimiterWithAuth(addr, "", 32)
}

func NewRedisLimiterWithAuth(addr, password string, poolSize int) *RedisLimiter {
	if poolSize <= 0 {
		poolSize = 32
	}
	return &RedisLimiter{
		addr:     addr,
		password: password,
		pool:     make(chan *pooledRedisConn, poolSize),
	}
}

func (r *RedisLimiter) dialNew(ctx context.Context) (*pooledRedisConn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", r.addr)
	if err != nil {
		return nil, err
	}
	pc := &pooledRedisConn{conn: conn, br: bufio.NewReader(conn)}
	if r.password != "" {
		if err := r.sendAuth(pc); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	return pc, nil
}

func (r *RedisLimiter) acquireConn(ctx context.Context) (*pooledRedisConn, bool, error) {
	select {
	case pc := <-r.pool:
		return pc, true, nil
	default:
		pc, err := r.dialNew(ctx)
		return pc, false, err
	}
}

func (r *RedisLimiter) releaseConn(pc *pooledRedisConn) {
	select {
	case r.pool <- pc:
	default:
		_ = pc.conn.Close()
	}
}

func (r *RedisLimiter) sendAuth(pc *pooledRedisConn) error {
	_ = pc.conn.SetDeadline(time.Now().Add(2 * time.Second))
	cmd := fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(r.password), r.password)
	if _, err := io.WriteString(pc.conn, cmd); err != nil {
		return err
	}
	line, err := pc.br.ReadString('\n')
	if err != nil {
		return err
	}
	if strings.HasPrefix(line, "-") {
		return errors.New(strings.TrimSpace(strings.TrimPrefix(line, "-")))
	}
	return nil
}

// Ping verifies Redis RESP2 connectivity for readiness probes.
func (r *RedisLimiter) Ping(ctx context.Context) error {
	pc, fromPool, err := r.acquireConn(ctx)
	if err != nil {
		return err
	}
	if err := r.doPing(ctx, pc); err != nil {
		_ = pc.conn.Close()
		if !fromPool {
			return err
		}
		fresh, dialErr := r.dialNew(ctx)
		if dialErr != nil {
			return dialErr
		}
		if err2 := r.doPing(ctx, fresh); err2 != nil {
			_ = fresh.conn.Close()
			return err2
		}
		r.releaseConn(fresh)
		return nil
	}
	r.releaseConn(pc)
	return nil
}

func (r *RedisLimiter) doPing(ctx context.Context, pc *pooledRedisConn) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = pc.conn.SetDeadline(deadline)
	} else {
		_ = pc.conn.SetDeadline(time.Now().Add(2 * time.Second))
	}
	if _, err := io.WriteString(pc.conn, "*1\r\n$4\r\nPING\r\n"); err != nil {
		return err
	}
	line, err := pc.br.ReadString('\n')
	if err != nil {
		return err
	}
	if strings.HasPrefix(line, "-") {
		return errors.New(strings.TrimSpace(strings.TrimPrefix(line, "-")))
	}
	return nil
}

func (r *RedisLimiter) Allow(ctx context.Context, key string, policy config.RateLimitPolicy) (Decision, error) {
	policy = normalizePolicy(policy)
	now := time.Now()
	nowMs := now.UnixMilli()

	var vals []int64
	var err error

	if policy.Algorithm == config.AlgoSlidingWindow {
		member := fmt.Sprintf("%d-%d", now.UnixNano(), r.memberSeq.Add(1))
		vals, err = r.evalIntArray(ctx, slidingWindowLua, []string{"rl:sw:" + key},
			strconv.FormatInt(policy.Window.Milliseconds(), 10),
			strconv.Itoa(policy.Limit),
			strconv.FormatInt(nowMs, 10),
			member,
		)
	} else if policy.Algorithm == config.AlgoLeakyBucket {
		leakRate := policy.RefillRate
		if leakRate <= 0 && policy.Window > 0 {
			leakRate = float64(policy.Limit) / policy.Window.Seconds()
		}
		if leakRate <= 0 {
			leakRate = 1.0
		}
		ttlMs := policy.Window.Milliseconds() * 2
		if ttlMs < 60000 {
			ttlMs = 60000
		}
		vals, err = r.evalIntArray(ctx, leakyBucketLua, []string{"rl:lb:" + key},
			strconv.Itoa(policy.Limit),
			strconv.FormatFloat(leakRate, 'g', -1, 64),
			strconv.FormatInt(nowMs, 10),
			strconv.FormatInt(ttlMs, 10),
		)
	} else {
		rate := policy.RefillRate
		if rate <= 0 && policy.Window > 0 {
			rate = float64(policy.Limit) / policy.Window.Seconds()
		}
		if rate <= 0 {
			rate = 1.0
		}
		ttlMs := policy.Window.Milliseconds() * 2
		if ttlMs < 60000 {
			ttlMs = 60000
		}
		vals, err = r.evalIntArray(ctx, tokenBucketLua, []string{"rl:tb:" + key},
			strconv.Itoa(policy.Limit),
			strconv.FormatFloat(rate, 'g', -1, 64),
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
	pc, fromPool, err := r.acquireConn(ctx)
	if err != nil {
		return nil, err
	}
	out, keep, evalErr := r.evalOnConn(ctx, pc, script, keys, args...)
	if !keep {
		_ = pc.conn.Close()
		if evalErr != nil && fromPool {
			fresh, dialErr := r.dialNew(ctx)
			if dialErr != nil {
				return nil, dialErr
			}
			out2, keep2, evalErr2 := r.evalOnConn(ctx, fresh, script, keys, args...)
			if keep2 {
				r.releaseConn(fresh)
			} else {
				_ = fresh.conn.Close()
			}
			return out2, evalErr2
		}
		return out, evalErr
	}
	r.releaseConn(pc)
	return out, evalErr
}

func (r *RedisLimiter) evalOnConn(ctx context.Context, pc *pooledRedisConn, script string, keys []string, args ...string) ([]int64, bool, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = pc.conn.SetDeadline(deadline)
	} else {
		_ = pc.conn.SetDeadline(time.Now().Add(2 * time.Second))
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

	if _, err := io.WriteString(pc.conn, sb.String()); err != nil {
		return nil, false, err
	}

	line, err := pc.br.ReadString('\n')
	if err != nil {
		return nil, false, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if strings.HasPrefix(line, "-") {
		// Server responded with a valid RESP error frame; stream remains synchronized.
		return nil, true, errors.New(strings.TrimPrefix(line, "-"))
	}
	if !strings.HasPrefix(line, "*") {
		return nil, false, fmt.Errorf("expected RESP array, got %q", line)
	}

	n, err := strconv.Atoi(strings.TrimPrefix(line, "*"))
	if err != nil || n < 0 {
		return nil, false, fmt.Errorf("invalid RESP array header %q", line)
	}
	out := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		item, err := pc.br.ReadString('\n')
		if err != nil {
			return nil, false, err
		}
		item = strings.TrimSuffix(strings.TrimSuffix(item, "\n"), "\r")
		if !strings.HasPrefix(item, ":") {
			return nil, false, fmt.Errorf("expected RESP integer, got %q", item)
		}
		v, err := strconv.ParseInt(strings.TrimPrefix(item, ":"), 10, 64)
		if err != nil {
			return nil, false, err
		}
		out = append(out, v)
	}
	return out, true, nil
}
