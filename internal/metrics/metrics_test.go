package metrics

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// White-box (package metrics) so we can read individual series with testutil.ToFloat64 instead
// of parsing the exposition text — less brittle than substring matching on the scrape output.

// TestEventCounters pins the per-event methods: each lands on the right (label) series and the
// Eviction n>0 guard holds.
func TestEventCounters(t *testing.T) {
	m := New()

	m.Hit("lookup")
	m.Hit("lookup")
	m.Miss("fetch")
	m.Eviction("ttl", 3)
	m.Eviction("pressure", 0) // guarded: a 0-count drain must not create/inc the series
	m.Replica("forwarded")
	m.Replica("dropped")

	cases := []struct {
		name string
		got  float64
		want float64
	}{
		{"lookup hit", testutil.ToFloat64(m.cacheReqs.WithLabelValues("lookup", "hit")), 2},
		{"fetch miss", testutil.ToFloat64(m.cacheReqs.WithLabelValues("fetch", "miss")), 1},
		{"evict ttl", testutil.ToFloat64(m.evictions.WithLabelValues("ttl")), 3},
		{"evict pressure (0-guarded)", testutil.ToFloat64(m.evictions.WithLabelValues("pressure")), 0},
		{"repl forwarded", testutil.ToFloat64(m.replication.WithLabelValues("forwarded")), 1},
		{"repl dropped", testutil.ToFloat64(m.replication.WithLabelValues("dropped")), 1},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// TestGauges pins the polled level metrics.
func TestGauges(t *testing.T) {
	m := New()
	m.SetResident(4096, 7)
	m.SetReplicaQueueDepth(12)

	if got := testutil.ToFloat64(m.residentBytes); got != 4096 {
		t.Errorf("residentBytes = %v, want 4096", got)
	}
	if got := testutil.ToFloat64(m.residentEntries); got != 7 {
		t.Errorf("residentEntries = %v, want 7", got)
	}
	if got := testutil.ToFloat64(m.replicaQueued); got != 12 {
		t.Errorf("replicaQueued = %v, want 12", got)
	}
}

// TestUnaryInterceptor checks the interceptor counts by method+code (OK on nil error, the
// mapped code on failure) and records a latency observation per call.
func TestUnaryInterceptor(t *testing.T) {
	m := New()
	interceptor := m.UnaryInterceptor()
	const method = "/kvcache.v1.KVCache/Lookup"
	info := &grpc.UnaryServerInfo{FullMethod: method}

	ok := func(context.Context, any) (any, error) { return "resp", nil }
	fail := func(context.Context, any) (any, error) {
		return nil, status.Error(codes.NotFound, "miss")
	}

	if _, err := interceptor(context.Background(), nil, info, ok); err != nil {
		t.Fatalf("ok handler returned err: %v", err)
	}
	if _, err := interceptor(context.Background(), nil, info, fail); status.Code(err) != codes.NotFound {
		t.Fatalf("fail handler code = %v, want NotFound", status.Code(err))
	}

	if got := testutil.ToFloat64(m.rpcTotal.WithLabelValues(method, "OK")); got != 1 {
		t.Errorf("rpcTotal OK = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.rpcTotal.WithLabelValues(method, "NotFound")); got != 1 {
		t.Errorf("rpcTotal NotFound = %v, want 1", got)
	}
	// Two calls => two latency observations on the method's histogram (one series, count=2).
	if got := testutil.CollectAndCount(m.rpcDuration); got != 1 {
		t.Errorf("rpcDuration series = %v, want 1", got)
	}
}
