package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
	"sync"

	"github.com/Chiru9030/distributed-rate-limiter-gateway/internal/config"
)

type Authenticator struct {
	mu      sync.RWMutex
	tenants map[string]config.TenantConfig
}

func NewAuthenticator(tenants map[string]config.TenantConfig) *Authenticator {
	m := make(map[string]config.TenantConfig, len(tenants))
	for k, v := range tenants {
		m[k] = v
	}
	return &Authenticator{tenants: m}
}

func (a *Authenticator) UpsertTenant(tenant config.TenantConfig) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tenants[tenant.APIKey] = tenant
}

func (a *Authenticator) Authenticate(r *http.Request) (config.TenantConfig, bool) {
	token := extractToken(r)
	if token == "" {
		return config.TenantConfig{}, false
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	tokenDigest := sha256.Sum256([]byte(token))
	for key, tenant := range a.tenants {
		keyDigest := sha256.Sum256([]byte(key))
		if subtle.ConstantTimeCompare(tokenDigest[:], keyDigest[:]) == 1 {
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

