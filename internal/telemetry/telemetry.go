package telemetry

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

type Metrics struct {
	TotalRequests     atomic.Uint64
	RateLimited       atomic.Uint64
	AuthFailures      atomic.Uint64
	UpstreamErrors    atomic.Uint64
	TotalLatencyMicro atomic.Uint64
}

func NewLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}

func (m *Metrics) Record(status int, latency time.Duration) {
	m.TotalRequests.Add(1)
	m.TotalLatencyMicro.Add(uint64(latency.Microseconds()))
	switch status {
	case http.StatusTooManyRequests:
		m.RateLimited.Add(1)
	case http.StatusUnauthorized:
		m.AuthFailures.Add(1)
	default:
		if status >= 500 {
			m.UpstreamErrors.Add(1)
		}
	}
}

// ServePrometheus writes Prometheus text exposition format metrics.
func (m *Metrics) ServePrometheus(w http.ResponseWriter, _ *http.Request) {
	total := m.TotalRequests.Load()
	rl := m.RateLimited.Load()
	auth := m.AuthFailures.Load()
	up := m.UpstreamErrors.Load()
	lat := m.TotalLatencyMicro.Load()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w,
		"# HELP gateway_requests_total Total HTTP requests handled\n"+
			"# TYPE gateway_requests_total counter\n"+
			"gateway_requests_total %d\n"+
			"# HELP gateway_rate_limited_total Requests rejected with HTTP 429\n"+
			"# TYPE gateway_rate_limited_total counter\n"+
			"gateway_rate_limited_total %d\n"+
			"# HELP gateway_auth_failures_total Requests rejected with HTTP 401\n"+
			"# TYPE gateway_auth_failures_total counter\n"+
			"gateway_auth_failures_total %d\n"+
			"# HELP gateway_upstream_errors_total Upstream 5xx/connection errors\n"+
			"# TYPE gateway_upstream_errors_total counter\n"+
			"gateway_upstream_errors_total %d\n"+
			"# HELP gateway_request_duration_microseconds_sum Cumulative latency in microseconds\n"+
			"# TYPE gateway_request_duration_microseconds_sum counter\n"+
			"gateway_request_duration_microseconds_sum %d\n",
		total, rl, auth, up, lat,
	)
}

