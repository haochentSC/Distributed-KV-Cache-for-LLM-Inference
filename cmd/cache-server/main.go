// Command cache-server runs a single KV-cache shard over gRPC. When -etcd is given it
// registers itself into cluster membership under a lease, so clients discover it via the
// etcd watch (Sub-stage A); with -etcd empty it runs standalone (local single-node).
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	kvcachev1 "github.com/haochentSC/distributed-kv-cache/gen/kvcache/v1"
	"github.com/haochentSC/distributed-kv-cache/internal/cache"
	"github.com/haochentSC/distributed-kv-cache/internal/cluster"
	"github.com/haochentSC/distributed-kv-cache/internal/coldtier"
	"github.com/haochentSC/distributed-kv-cache/internal/coord"
	"github.com/haochentSC/distributed-kv-cache/internal/metrics"
	"github.com/haochentSC/distributed-kv-cache/internal/server"
	"github.com/haochentSC/distributed-kv-cache/internal/spot"
)

// coldAdapter bridges coldtier.Tier (keyed by [32]byte) to the server's coldReader seam (keyed by
// cache.BlockHash), so the server package stays free of both the coldtier and AWS-SDK imports.
type coldAdapter struct{ t coldtier.Tier }

func (a coldAdapter) Get(ctx context.Context, model string, h cache.BlockHash) ([]byte, uint64, []int32, bool, error) {
	return a.t.Get(ctx, model, [32]byte(h))
}

// ringVnodes must match the value every client (loadgen) and peer server uses, or they'd
// compute different rings and disagree on placement. 128 is the calibrated value (ADR 0014).
const ringVnodes = 128

func main() {
	addr := flag.String("addr", ":50051", "gRPC listen address")
	advertise := flag.String("advertise", "", "address clients use to reach this node (host:port); defaults to -addr")
	etcdEndpoints := flag.String("etcd", "", "comma-separated etcd endpoints; empty = run standalone (no registration)")
	nodeID := flag.String("node-id", "", "stable unique node id for the ring; defaults to the advertise address")
	leaseTTL := flag.Int64("lease-ttl", 10, "etcd membership lease TTL in seconds (also the failure-detection window)")
	rf := flag.Int("rf", 2, "replication factor: primary + (rf-1) replicas (ADR 0021); 1 disables replication")
	spotWatch := flag.Bool("spot", false, "watch EC2 IMDS for Spot interruption and drain on notice (ADR 0023); no-op off EC2")
	maxBytes := flag.Int64("max-bytes", 0, "soft+hard memory bound for this shard in bytes; 0 = unbounded (Phase 4 eviction)")
	hiWater := flag.Float64("hi-water", 0.90, "high-water fraction of -max-bytes that wakes the evictor")
	loWater := flag.Float64("lo-water", 0.75, "low-water fraction of -max-bytes the evictor frees down to")
	ttl := flag.Duration("ttl", 0, "idle TTL: evict blocks not read within this window; 0 disables the TTL sweep")
	evictSweep := flag.Duration("evict-sweep", 30*time.Second, "how often the TTL sweep runs")
	metricsAddr := flag.String("metrics-addr", ":9090", "address for the Prometheus /metrics endpoint; empty disables it")
	coldBucket := flag.String("cold-bucket", "", "S3 bucket for the cold tier (spill-on-evict + Fetch read-through, ADR 0027); empty = disabled. Region/creds come from the instance role / AWS_REGION")
	flag.Parse()

	// Surface watermark misconfiguration loudly: the Store would otherwise silently clamp a bad
	// pair back to its defaults, which hides an operator typo. Only meaningful when a bound is set.
	if *maxBytes > 0 && !(*hiWater > 0 && *hiWater <= 1 && *loWater > 0 && *loWater < *hiWater) {
		log.Fatalf("invalid watermarks: need 0 < -lo-water (%.2f) < -hi-water (%.2f) <= 1", *loWater, *hiWater)
	}

	// The advertise address is what gets published to etcd; ":50051" is not dialable by a
	// remote client, so it must carry a reachable host in a real cluster.
	adv := *advertise
	if adv == "" {
		adv = *addr
	}
	id := *nodeID
	if id == "" {
		id = adv // the address doubles as the ring label, matching the static driver's convention
	}

	// ctx bounds the cold-tier workers + membership watch + replicator + evictor + metrics-poller.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Optional S3 cold tier (ADR 0027): entries evicted under pressure/TTL spill to S3, and a hot
	// miss reads through to it. Empty -cold-bucket keeps the node cloud-free — the default for all
	// local/chaos runs. Region + credentials come from the instance role / AWS_REGION (no static
	// creds, ADR 0004).
	var cold coldtier.Tier
	if *coldBucket != "" {
		t, err := coldtier.NewS3(ctx, *coldBucket)
		if err != nil {
			log.Fatalf("cold tier: %v", err)
		}
		cold = t
		defer cold.Close()
		log.Printf("cold tier enabled: s3://%s (spill-on-evict + Fetch read-through)", *coldBucket)
	}

	// Phase 4: an LRU-backed, optionally bounded Store. With -max-bytes unset it stays
	// unbounded (NoopPolicy-equivalent behaviour). The LRU is the baseline policy Phase 5's
	// cost-aware/fairness engine will be measured against — swapped here, nowhere else. When a
	// cold tier is configured, entries evicted under pressure/TTL are demoted to it (ADR 0027)
	// instead of being lost; the SpillFunc just adapts cache.BlockHash to the tier's [32]byte.
	storeOpts := []cache.StoreOption{
		cache.WithMaxBytes(*maxBytes),
		cache.WithWatermarks(*hiWater, *loWater),
	}
	if cold != nil {
		storeOpts = append(storeOpts, cache.WithSpiller(
			func(model string, h cache.BlockHash, version uint64, tokenIDs []int32, kv []byte) {
				cold.Spill(model, [32]byte(h), version, tokenIDs, kv)
			}))
	}
	store := cache.NewStore(cache.NewLRU(), storeOpts...)

	// Process-wide Prometheus instrumentation (ADR 0025). One *Metrics owns a private registry;
	// it feeds the gRPC interceptors, the evictor's reason counter, the server/replicator event
	// counters, and the polled resident/queue gauges below. Cheap to build even when -metrics-addr
	// is empty (the collectors just never get scraped).
	m := metrics.New()

	// The background evictor handles memory pressure (watermark drain) and TTL expiry. It is a
	// no-op until -max-bytes/-ttl are set, so it's always safe to start. m.Eviction counts what
	// it frees, by reason (pressure|ttl).
	go cache.NewEvictor(store, *ttl, *evictSweep, m.Eviction).Run(ctx)

	// Register into etcd membership (optional). release() deregisters by revoking the lease.
	// With etcd configured we ALSO watch membership ourselves and run a Replicator: as the
	// primary for the blocks routed to us, we forward each Write to its replica (ADR 0021).
	// The server reuses the SAME cluster.Router the clients use, driven by the SAME etcd watch
	// (coord.DriveRouter) — so server and clients agree on who the replica is.
	var release func()
	var repl *server.Replicator
	if *etcdEndpoints != "" {
		cli, err := coord.Dial(splitCSV(*etcdEndpoints), 5*time.Second)
		if err != nil {
			log.Fatalf("etcd dial: %v", err)
		}
		defer cli.Close()
		release, err = cli.Register(ctx, id, adv, *leaseTTL)
		if err != nil {
			log.Fatalf("etcd register: %v", err)
		}
		log.Printf("registered in etcd as %q -> %q (lease ttl %ds)", id, adv, *leaseTTL)

		if *rf > 1 {
			router := cluster.New(ringVnodes)
			defer router.Close()
			snaps, err := cli.WatchMembers(ctx)
			if err != nil {
				log.Fatalf("watch members: %v", err)
			}
			go coord.DriveRouter(ctx, snaps, router) // keep the router's view current
			repl = server.NewReplicator(id, *rf, router).WithMetrics(m)
			go repl.Run(ctx)
			log.Printf("replication enabled: rf=%d, self=%q", *rf, id)
		}
	}

	// NewWithReplicator only when replication is on; otherwise a plain single-node Server.
	// Either way attach metrics so Lookup/Fetch hit-miss and manual Evicts are counted.
	var srv *server.Server
	if repl != nil {
		srv = server.NewWithReplicator(store, repl)
	} else {
		srv = server.New(store)
	}
	srv.WithMetrics(m)
	if cold != nil {
		srv.WithColdReader(coldAdapter{cold}) // read-through on a hot miss (ADR 0027)
	}

	// Chain the metrics interceptors so EVERY RPC's latency + per-code count is recorded in one
	// place, no per-handler bookkeeping (ADR 0025). Unary covers Lookup/Evict/Health; stream
	// covers Fetch/Write/Replicate.
	gs := grpc.NewServer(
		grpc.ChainUnaryInterceptor(m.UnaryInterceptor()),
		grpc.ChainStreamInterceptor(m.StreamInterceptor()),
	)
	kvcachev1.RegisterKVCacheServer(gs, srv)

	// Expose Prometheus metrics on a SEPARATE HTTP port (the gRPC port speaks HTTP/2 framing, so
	// a scraper can't share it). Empty -metrics-addr disables the endpoint (e.g. local runs).
	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", m.Handler())
		ms := &http.Server{Addr: *metricsAddr, Handler: mux}
		go func() {
			log.Printf("metrics endpoint on %s/metrics", *metricsAddr)
			if err := ms.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("metrics server: %v", err) // non-fatal: metrics down must not kill the cache
			}
		}()
		defer ms.Close()
	}

	// Poll the gauges that are LEVELS, not events: resident size/entries and the replication
	// backlog. Polling (vs updating on every write) keeps the hot path metrics-free and is the
	// natural shape for a gauge — Prometheus only samples at scrape time anyway.
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.SetResident(store.Bytes(), store.Len())
				if repl != nil {
					m.SetReplicaQueueDepth(repl.QueueDepth())
				}
			}
		}
	}()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}

	go func() {
		log.Printf("kv-cache server listening on %s (advertise %s)", *addr, adv)
		if err := gs.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// Spot interruption wires into the SAME drain path SIGTERM does: a notice closes the
	// stop channel via this goroutine, no parallel shutdown code (ADR 0023). The watcher
	// is a no-op off EC2, so this is safe to enable unconditionally in production AMIs.
	if *spotWatch {
		go spot.Watch(ctx, func() {
			select {
			case stop <- syscall.SIGTERM: // share the drain path
			default: // already draining
			}
		})
	}

	<-stop
	log.Println("shutting down")
	// Deregister FIRST so clients stop routing here, THEN drain. (Sub-stage D extends this
	// to wire the EC2 Spot interruption notice into the same path.)
	if release != nil {
		release()
	}
	gs.GracefulStop()
}

// splitCSV parses a comma-separated list, trimming blanks.
func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if v := strings.TrimSpace(part); v != "" {
			out = append(out, v)
		}
	}
	return out
}
