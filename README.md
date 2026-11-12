# Distributed Rate-Limiter & API Gateway

A high-performance Go API Gateway sitting in front of backend microservices with distributed Redis rate limiting, constant-time API key authentication, structured telemetry, and a typed TypeScript client SDK.

## Key Engineering Highlights

1. **Distributed Rate Limiting (Token Bucket & Sliding Window Log)**:
   - **Redis Lua Scripts** (`internal/ratelimit/redis.go`): Atomic `EVAL` scripts (`HMGET`/`HSET` for Token Bucket, `ZREMRANGEBYSCORE`/`ZCARD`/`ZADD` for Sliding Window Log) guarantee race-free enforcement across multiple gateway replicas.
   - **In-Memory Limiter** (`internal/ratelimit/memory.go`): Zero-dependency thread-safe implementation with injectable clock for deterministic unit tests.
   - Emits standard `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`, and `Retry-After` headers.
2. **Standardized Error Handling & Telemetry**:
   - Stripe-style structured JSON error envelopes (`type`, `code`, `message`, `param`, `request_id`, `trace_id`).
   - Automatic `X-Request-ID` & `X-Trace-ID` generation and upstream propagation.
   - Structured JSON access logs (`log/slog`) + Prometheus `/metrics` endpoint.
3. **Typed TypeScript Client SDK (`sdk-ts/`)**:
   - Automatic `Idempotency-Key` generation for mutating requests.
   - Automatic retries on `429` (honoring `Retry-After`) and `5xx` with exponential backoff.
   - Typed `GatewayApiError` and parsed `rateLimit` metadata on every response.

## Quick Start

```bash
# Run Go unit & integration tests with race detector
go test -v -race ./...

# Build & test the TypeScript SDK
cd sdk-ts && npm install && npm run build && npm test

# Run multi-replica gateway + Redis stack
docker compose up --build
```
