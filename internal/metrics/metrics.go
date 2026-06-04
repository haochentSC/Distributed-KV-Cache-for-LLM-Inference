// Package metrics is the Prometheus instrumentation layer for the cache server. It is the
// ONLY package that imports Prometheus: the core (internal/cache) stays metrics-free and the
// gRPC layer (internal/server) talks to it through narrow interfaces it owns. main builds one
// *Metrics, hands the interceptors to grpc.NewServer, the per-event methods to the server and
// replicator, and exposes Handler() on a /metrics HTTP endpoint (ADR 0025).
//
// Two design choices worth understanding:
//
//   - OWN REGISTRY, not the global default. Collectors are registered into a private
//     *prometheus.Registry, not promauto's global one. The global registry is process-wide
//     mutable state: a second New() (or a test) registering the same metric name panics with
//     "duplicate metrics collector registration". A private registry makes Metrics a normal
//     value you can construct twice (prod + a test) with no global side effects.
//
//   - LABEL CARDINALITY IS BUDGETED. Every distinct label-value combination is a separate
//     time series held in the scrape target's memory. So labels here are ONLY bounded, small
//     sets — gRPC method (5), status code (~17), op (lookup/fetch), result (hit/miss),
//     eviction reason (pressure/ttl/manual). High-cardinality identifiers (model_id,
//     tenant_id, block_hash) are deliberately NOT labels: they would let a caller mint
//     unbounded series and OOM the target — the exact failure Phase 4 exists to prevent.
package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// Metrics holds every collector for one cache-server, registered into its own registry.
// Construct with New; it is safe to call all methods concurrently (Prometheus collectors are
// goroutine-safe).
type Metrics struct {
	reg *prometheus.Registry

	rpcDuration *prometheus.HistogramVec // grpc handler latency, by method
	rpcTotal    *prometheus.CounterVec   // grpc calls, by method+code (carries the error-rate signal)

	cacheReqs *prometheus.CounterVec // cache lookups/fetches, by op+result (hit-rate numerator/denominator)
	evictions *prometheus.CounterVec // evicted entries, by reason (pressure/ttl/manual)

	residentBytes   prometheus.Gauge // current resident size of the shard (polled from Store.Bytes)
	residentEntries prometheus.Gauge // current entry count (polled from Store.Len)

	replication   *prometheus.CounterVec // replication outcomes, by result (forwarded/dropped/error)
	replicaQueued prometheus.Gauge       // current replication backlog depth (polled from Replicator.QueueDepth)
}

// New builds and registers all collectors into a fresh private registry. The namespace prefixes
// every metric name (so they read as kvcache_grpc_requests_total, etc.) and keeps them grouped
// in Grafana's metric browser.
func New() *Metrics {
	const ns = "kvcache"
	m := &Metrics{reg: prometheus.NewRegistry()}

	m.rpcDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns,
		Name:      "grpc_request_duration_seconds",
		Help:      "gRPC server handler latency in seconds, by method.",
		// DefBuckets (5ms..10s) cover Lookup/Fetch/Write well enough for v1. Buckets are the
		// latency RESOLUTION knob — p99 can only be reported to the nearest bucket edge, so if
		// the real tail clusters somewhere these miss, refine here (this is the one tuning point).
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})

	m.rpcTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "grpc_requests_total",
		Help:      "Total gRPC calls handled, by method and gRPC status code.",
	}, []string{"method", "code"})

	m.cacheReqs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "cache_requests_total",
		Help:      "Cache block lookups/fetches, by op (lookup|fetch) and result (hit|miss).",
	}, []string{"op", "result"})

	m.evictions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "evictions_total",
		Help:      "Entries evicted, by reason (pressure|ttl|manual).",
	}, []string{"reason"})

	m.residentBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns,
		Name:      "resident_bytes",
		Help:      "Current resident size of this shard in bytes.",
	})
	m.residentEntries = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns,
		Name:      "resident_entries",
		Help:      "Current number of entries held by this shard.",
	})

	m.replication = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "replication_total",
		Help:      "Replication forwards, by result (forwarded|dropped|error).",
	}, []string{"result"})
	m.replicaQueued = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns,
		Name:      "replication_queue_depth",
		Help:      "Current depth of the async replication queue (proxy for replication lag).",
	})

	m.reg.MustRegister(
		m.rpcDuration, m.rpcTotal, m.cacheReqs, m.evictions,
		m.residentBytes, m.residentEntries, m.replication, m.replicaQueued,
	)
	return m
}

// Handler serves the registry in the Prometheus text exposition format. Mount it at /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// --- gRPC interceptors --------------------------------------------------------------------
// One interceptor records both latency and the per-code count for EVERY RPC, in one place, so
// no handler has to remember to instrument itself. Method+code are the only labels (both small,
// bounded sets). status.Code(err) maps a nil error to codes.OK, so success is counted too.

// UnaryInterceptor instruments unary RPCs (Lookup, Evict, Health).
func (m *Metrics) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		m.observe(info.FullMethod, status.Code(err).String(), time.Since(start))
		return resp, err
	}
}

// StreamInterceptor instruments streaming RPCs (Fetch, Write, Replicate). It times the whole
// handler — for a server-stream that is the full send, for a client-stream the full receive.
func (m *Metrics) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		m.observe(info.FullMethod, status.Code(err).String(), time.Since(start))
		return err
	}
}

func (m *Metrics) observe(method, code string, d time.Duration) {
	m.rpcDuration.WithLabelValues(method).Observe(d.Seconds())
	m.rpcTotal.WithLabelValues(method, code).Inc()
}

// --- per-event methods (called via the narrow interfaces in internal/server) --------------

// Hit/Miss record a cache lookup/fetch outcome. op is "lookup" or "fetch".
func (m *Metrics) Hit(op string)  { m.cacheReqs.WithLabelValues(op, "hit").Inc() }
func (m *Metrics) Miss(op string) { m.cacheReqs.WithLabelValues(op, "miss").Inc() }

// Eviction records n entries evicted for the given reason (pressure|ttl|manual). Its signature
// matches the Evictor's onEvict callback exactly, so m.Eviction can be passed straight to
// NewEvictor — the same method feeds both the background loop and the manual Evict RPC.
func (m *Metrics) Eviction(reason string, n int) {
	if n > 0 {
		m.evictions.WithLabelValues(reason).Add(float64(n))
	}
}

// Replica records one replication outcome: "forwarded", "dropped", or "error".
func (m *Metrics) Replica(result string) { m.replication.WithLabelValues(result).Inc() }

// SetResident publishes the shard's current size. main polls Store.Bytes/Len and calls this on
// a ticker — gauges are a point-in-time level, so polling (not event-driven updates) is the
// natural fit and keeps the hot write path metrics-free.
func (m *Metrics) SetResident(bytes int64, entries int) {
	m.residentBytes.Set(float64(bytes))
	m.residentEntries.Set(float64(entries))
}

// SetReplicaQueueDepth publishes the current replication backlog. Also polled by main.
func (m *Metrics) SetReplicaQueueDepth(n int) { m.replicaQueued.Set(float64(n)) }
