package config

import "time"

type Algorithm string

const (
	AlgoTokenBucket   Algorithm = "token_bucket"
	AlgoSlidingWindow Algorithm = "sliding_window"
)

type RateLimitPolicy struct {
	Algorithm  Algorithm
	Limit      int           // Max tokens (bucket capacity) or max requests per window
	Window     time.Duration // Window duration (sliding window) or full-refill period
	RefillRate float64       // Tokens per second (for token_bucket)
}

type RouteConfig struct {
	PathPrefix string
	Upstream   string
	Policy     *RateLimitPolicy // Optional route-specific override
}

type TenantConfig struct {
	APIKey string
	Tenant string
	Tier   string
	Policy RateLimitPolicy
}

type GatewayConfig struct {
	ListenAddr    string
	RedisAddr     string
	DefaultPolicy RateLimitPolicy
	Tenants       map[string]TenantConfig
	Routes        []RouteConfig
	UpstreamTimeout time.Duration
}

func DefaultConfig() GatewayConfig {
	return GatewayConfig{
		ListenAddr:      ":8080",
		UpstreamTimeout: 5 * time.Second,
		DefaultPolicy: RateLimitPolicy{
			Algorithm:  AlgoTokenBucket,
			Limit:      60,
			Window:     time.Minute,
			RefillRate: 1.0,
		},
		Tenants: map[string]TenantConfig{
			"sk_test_acme_123": {
				APIKey: "sk_test_acme_123",
				Tenant: "acme_corp",
				Tier:   "pro",
				Policy: RateLimitPolicy{
					Algorithm:  AlgoTokenBucket,
					Limit:      100,
					Window:     time.Minute,
					RefillRate: 5.0,
				},
			},
		},
	}
}
