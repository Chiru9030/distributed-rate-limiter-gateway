package ratelimit

import (
	"context"
	"time"

	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/config"
)

type Decision struct {
	Allowed    bool
	Limit      int
	Remaining  int
	ResetAt    time.Time
	RetryAfter time.Duration
}

type Limiter interface {
	Allow(ctx context.Context, key string, policy config.RateLimitPolicy) (Decision, error)
}
