package auth

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/config"
)

type Authenticator struct {
	tenants map[string]config.TenantConfig
}

func NewAuthenticator(tenants map[string]config.TenantConfig) *Authenticator {
	return &Authenticator{tenants: tenants}
}

func (a *Authenticator) Authenticate(r *http.Request) (config.TenantConfig, bool) {
	token := extractToken(r)
	if token == "" {
		return config.TenantConfig{}, false
	}

	for key, tenant := range a.tenants {
		if len(token) == len(key) && subtle.ConstantTimeCompare([]byte(token), []byte(key)) == 1 {
			return tenant, true
		}
	}
	return config.TenantConfig{}, false
}

func extractToken(r *http.Request) string {
	if apiKey := strings.TrimSpace(r.Header.Get("X-API-Key")); apiKey != "" {
		return apiKey
	}
	authz := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(authz), "bearer ") {
		return strings.TrimSpace(authz[7:])
	}
	return ""
}
