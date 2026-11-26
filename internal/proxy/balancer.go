package proxy

import (
	"context"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"
)

type UpstreamNode struct {
	URL     *url.URL
	Weight  int
	Healthy atomic.Bool
	Proxy   *httputil.ReverseProxy
}

type LoadBalancer struct {
	nodes    []*UpstreamNode
	index    atomic.Uint64
	strategy string
}

func NewLoadBalancer(nodes []*UpstreamNode, strategy string) *LoadBalancer {
	if strategy == "" {
		strategy = "round_robin"
	}
	return &LoadBalancer{
		nodes:    nodes,
		strategy: strategy,
	}
}

// Next selects the next healthy upstream node using the configured strategy.
// If all nodes are unhealthy, it returns the first node as a graceful degraded fallback.
func (lb *LoadBalancer) Next() *UpstreamNode {
	if len(lb.nodes) == 0 {
		return nil
	}
	if len(lb.nodes) == 1 {
		return lb.nodes[0]
	}

	healthyNodes := make([]*UpstreamNode, 0, len(lb.nodes))
	for _, n := range lb.nodes {
		if n.Healthy.Load() {
			healthyNodes = append(healthyNodes, n)
		}
	}
	if len(healthyNodes) == 0 {
		// All nodes unhealthy: fall back to first node so circuit breaker can evaluate
		return lb.nodes[0]
	}

	idx := lb.index.Add(1) - 1
	if lb.strategy == "weighted" {
		totalWeight := 0
		for _, n := range healthyNodes {
			w := n.Weight
			if w <= 0 {
				w = 1
			}
			if w > 50 {
				w = 50
			}
			totalWeight += w
		}
		slot := int(idx % uint64(totalWeight))
		for _, n := range healthyNodes {
			w := n.Weight
			if w <= 0 {
				w = 1
			}
			if w > 50 {
				w = 50
			}
			if slot < w {
				return n
			}
			slot -= w
		}
	}
	return healthyNodes[idx%uint64(len(healthyNodes))]
}

func (lb *LoadBalancer) Nodes() []*UpstreamNode {
	return lb.nodes
}

// StartHealthCheck starts a background goroutine that periodically pings each node's /health endpoint.
func (lb *LoadBalancer) StartHealthCheck(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	client := &http.Client{Timeout: 2 * time.Second}
	ticker := time.NewTicker(interval)

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, node := range lb.nodes {
					checkURL := node.URL.String() + "/health"
					resp, err := client.Get(checkURL)
					isHealthy := err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300
					if resp != nil && resp.Body != nil {
						_ = resp.Body.Close()
					}
					node.Healthy.Store(isHealthy)
				}
			}
		}
	}()
}
