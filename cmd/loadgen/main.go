// Command loadgen drives the cache with synthetic, GPU-free traffic (plan §3 component 6).
//
// It models shared-prefix LLM traffic: a fraction of requests reuse a hot prefix (think
// a shared system prompt), each with its own divergent tail. For every request it chains
// the tokens into block hashes (internal/blockhash), routes the prompt to its owning
// shard via the consistent-hash ring (prefix-affinity on block_hash[0], ADR 0014), Looks
// the blocks up, Fetches the cached prefix run, and Writes the misses — then reports
// throughput, block hit rate, latency percentiles, and the per-shard request distribution
// (ADR 0014's hot-shard measurement). No GPU or vLLM needed (plan §1, §3).
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	kvcachev1 "github.com/haochentSC/distributed-kv-cache/gen/kvcache/v1"
	"github.com/haochentSC/distributed-kv-cache/internal/blockhash"
	"github.com/haochentSC/distributed-kv-cache/internal/cluster"
	"github.com/haochentSC/distributed-kv-cache/internal/coord"
)

// writeChunkBytes bounds each Write frame under gRPC's 4 MB cap (ADR 0012), mirroring
// the server's Fetch chunking.
const writeChunkBytes = 1 << 20 // 1 MiB

// ringVnodes must match the vnode count every other client (and Phase 3's etcd-published
// ring) uses, or clients would route the same key to different shards. 128 is the value
// the ring is calibrated and tested against (internal/ring, ADR 0014).
const ringVnodes = 128

func main() {
	members := flag.String("members", "localhost:50051", "comma-separated cache shard addresses (host:port); used when -etcd is empty")
	etcdEndpoints := flag.String("etcd", "", "comma-separated etcd endpoints; when set, membership is discovered via the etcd watch instead of -members")
	payloadBytes := flag.Int("payload-bytes", 2<<20, "bytes of KV per block (default 2MiB; lower it for cheap soak runs)")
	blockTokens := flag.Int("block-tokens", 16, "tokens per block")
	prefixShare := flag.Float64("prefix-share", 0.8, "fraction of requests reusing the hot prefix")
	concurrency := flag.Int("concurrency", 8, "concurrent clients")
	requests := flag.Int("requests", 200, "requests per client")
	prefixBlocks := flag.Int("prefix-blocks", 8, "blocks in the shared/hot prefix")
	tailBlocks := flag.Int("tail-blocks", 2, "blocks in each request's divergent tail")
	model := flag.String("model", "tinyllama-1.1b", "model id")
	seed := flag.Int64("seed", 1, "RNG seed (logged for reproducibility)")
	flag.Parse()

	log.Printf("loadgen payload=%dB block=%dtok prefix-share=%.2f conc=%d req/cli=%d prefix=%dblk tail=%dblk seed=%d",
		*payloadBytes, *blockTokens, *prefixShare, *concurrency, *requests, *prefixBlocks, *tailBlocks, *seed)

	router := cluster.New(ringVnodes)
	defer router.Close()

	// Membership comes from one of two drivers, both feeding the SAME SetMembers seam
	// (ADR 0019): the static -members list, or the etcd watch (Sub-stage A). Switching to
	// etcd changed no routing code — only which driver calls SetMembers.
	if *etcdEndpoints != "" {
		log.Printf("loadgen membership via etcd=%s", *etcdEndpoints)
		cleanup := startEtcdMembership(router, *etcdEndpoints)
		defer cleanup()
	} else {
		addrs := splitMembers(*members)
		if len(addrs) == 0 {
			log.Fatalf("need -members or -etcd")
		}
		log.Printf("loadgen membership static=%v", addrs)
		router.SetMembers(staticMembers(addrs))
	}

	// The hot prefix is identical for every hot request, so once any worker writes it the
	// rest hit it — that warming is exactly what we want to measure. With prefix-affinity
	// it also all lands on one shard, so a high prefix-share intentionally creates a hot
	// shard (the effect this run measures).
	hotPrefix := randomTokens(rand.New(rand.NewSource(*seed)), *prefixBlocks**blockTokens)

	w := workload{
		router:       router,
		model:        *model,
		blockTokens:  *blockTokens,
		payloadBytes: *payloadBytes,
		prefixShare:  *prefixShare,
		prefixBlocks: *prefixBlocks,
		tailBlocks:   *tailBlocks,
		hotPrefix:    hotPrefix,
	}

	results := make([]result, *concurrency)
	var wg sync.WaitGroup
	start := time.Now()
	for c := 0; c < *concurrency; c++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Per-worker RNG + per-worker result => no shared mutable state, no locks.
			results[id] = w.run(rand.New(rand.NewSource(*seed+int64(id)+1)), *requests)
		}(c)
	}
	wg.Wait()
	report(results, time.Since(start))
}

// splitMembers parses a comma-separated address list, trimming blanks.
func splitMembers(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if a := strings.TrimSpace(part); a != "" {
			out = append(out, a)
		}
	}
	return out
}

// staticMembers builds ring members from addresses. The address doubles as the stable,
// unique ring label in Phase 2; Phase 3 replaces it with the etcd-assigned node ID.
func staticMembers(addrs []string) []cluster.Member {
	m := make([]cluster.Member, len(addrs))
	for i, a := range addrs {
		m[i] = cluster.Member{ID: a, Addr: a}
	}
	return m
}

// startEtcdMembership wires the etcd membership watch into the router and returns a cleanup
// func. It blocks until the FIRST non-empty snapshot is applied so load doesn't start firing
// at an empty ring, then a background DriveRouter applies later joins/leaves.
func startEtcdMembership(router *cluster.Router, endpoints string) func() {
	cli, err := coord.Dial(splitMembers(endpoints), 5*time.Second)
	if err != nil {
		log.Fatalf("etcd dial: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	snaps, err := cli.WatchMembers(ctx)
	if err != nil {
		cancel()
		_ = cli.Close()
		log.Fatalf("watch members: %v", err)
	}

	ready, cancelReady := context.WithTimeout(ctx, 10*time.Second)
	defer cancelReady()
	for {
		select {
		case snap, ok := <-snaps:
			if !ok {
				cancel()
				_ = cli.Close()
				log.Fatal("membership channel closed before any members appeared")
			}
			router.SetMembers(snap)
			if len(snap) > 0 {
				go coord.DriveRouter(ctx, snaps, router) // apply subsequent joins/leaves
				return func() { cancel(); _ = cli.Close() }
			}
		case <-ready.Done():
			cancel()
			_ = cli.Close()
			log.Fatal("timed out waiting for cache members in etcd (are the servers registered?)")
		}
	}
}

type workload struct {
	router       *cluster.Router
	model        string
	blockTokens  int
	payloadBytes int
	prefixShare  float64
	prefixBlocks int
	tailBlocks   int
	hotPrefix    []int32
}

type result struct {
	requests  int
	blocks    int            // total blocks looked up
	hits      int            // blocks served from cache (Fetched)
	writes    int            // blocks written on a miss
	errors    int            // RPC errors mid-request (degrade-to-miss)
	degraded  int            // requests with no reachable owner (routed to a miss)
	perShard  map[string]int // requests routed to each shard (hot-shard measurement)
	latencies []time.Duration
}

func (w workload) run(rng *rand.Rand, n int) result {
	res := result{latencies: make([]time.Duration, 0, n), perShard: make(map[string]int)}
	payload := make([]byte, w.payloadBytes) // reused per worker; contents irrelevant
	for i := 0; i < n; i++ {
		blocks := blockhash.ChainBlocks(w.model, w.buildTokens(rng), w.blockTokens)
		if len(blocks) == 0 {
			continue
		}
		res.requests++

		// Prefix-affinity: the whole prompt is owned by the shard that owns block 0's hash.
		root := blocks[0].Hash[:]
		owner, _ := w.router.Owner(root)
		// OwnerConns returns [primary, replica, ...] so we can fail over on a dead primary
		// before degrading to miss (ADR 0021). rf=2 here matches the server's default.
		conns := w.router.OwnerConns(root, 2)
		if len(conns) == 0 {
			res.degraded++
			continue
		}
		res.perShard[owner]++

		t0 := time.Now()
		err := w.oneRequestWithFailover(context.Background(), conns, blocks, payload, root, &res)
		res.latencies = append(res.latencies, time.Since(t0))
		res.blocks += len(blocks)
		if err != nil {
			res.errors++ // every owner exhausted; treated as a degrade-to-miss
		}
	}
	return res
}

// buildTokens produces a request's token sequence: a prefix (the hot one with
// probability prefixShare, otherwise a fresh random one) plus a unique divergent tail.
func (w workload) buildTokens(rng *rand.Rand) []int32 {
	prefix := w.hotPrefix
	if rng.Float64() >= w.prefixShare {
		prefix = randomTokens(rng, w.prefixBlocks*w.blockTokens)
	}
	tokens := make([]int32, 0, len(prefix)+w.tailBlocks*w.blockTokens)
	tokens = append(tokens, prefix...)
	tokens = append(tokens, randomTokens(rng, w.tailBlocks*w.blockTokens)...)
	return tokens
}

// oneRequestWithFailover tries each owner (primary then replica) for the read side, and
// always Writes to the primary (only the primary mints versions; ADR 0021 keeps replication
// off the client path). A primary that is unreachable for Lookup/Fetch is treated as down —
// we fall through to the replica. Writes hit only the primary because routing a Write
// elsewhere would corrupt placement (the dead primary will re-take ownership when it returns
// and replication on the next write rebalances). If every owner errors we surface it as a
// degrade-to-miss to the caller.
func (w workload) oneRequestWithFailover(ctx context.Context, conns []*grpc.ClientConn, blocks []blockhash.Block, payload []byte, root []byte, res *result) error {
	var lastErr error
	for i, conn := range conns {
		client := kvcachev1.NewKVCacheClient(conn)
		// Writes only on the primary (i == 0). On a failover read (i > 0) we still attempt
		// Lookup+Fetch; if blocks aren't present on the replica, the run is short and the
		// remaining misses just won't be written this round — the next request to the new
		// primary (after the dead one is removed from the ring) will handle the writes.
		err := w.oneRequest(ctx, client, blocks, payload, res, i == 0, root)
		if err == nil {
			return nil
		}
		lastErr = err
		// Only fall through on transport-ish failures, not on per-block NotFound that Fetch
		// returns inside oneRequest — but oneRequest already handles miss-then-write inline,
		// so any returned error here means an RPC layer problem worth trying the next owner.
	}
	return lastErr
}

func (w workload) oneRequest(ctx context.Context, client kvcachev1.KVCacheClient, blocks []blockhash.Block, payload []byte, res *result, allowWrites bool, root []byte) error {
	bh := make([][]byte, len(blocks))
	for i := range blocks {
		bh[i] = blocks[i].Hash[:]
	}
	lookup, err := client.Lookup(ctx, &kvcachev1.LookupRequest{ModelId: w.model, BlockHashes: bh})
	if err != nil {
		return err
	}

	// Longest contiguous run of present blocks from index 0 (client-side assembly, ADR 0011).
	run := 0
	for run < len(lookup.GetBlocks()) && lookup.GetBlocks()[run].GetHasEntry() {
		run++
	}

	for i := 0; i < run; i++ { // reuse the cached prefix
		if err := w.fetch(ctx, client, blocks[i]); err != nil {
			return err
		}
		res.hits++
	}
	if !allowWrites {
		// Failover read path: we found whatever the replica had and won't write misses here
		// (writes must hit the primary so versions are minted correctly).
		return nil
	}
	for i := run; i < len(blocks); i++ { // simulate prefill + store of the misses
		if err := w.write(ctx, client, blocks[i], payload, root); err != nil {
			return err
		}
		res.writes++
	}
	return nil
}

func (w workload) fetch(ctx context.Context, client kvcachev1.KVCacheClient, b blockhash.Block) error {
	stream, err := client.Fetch(ctx, &kvcachev1.FetchRequest{ModelId: w.model, BlockHash: b.Hash[:], TokenIds: b.TokenIDs})
	if err != nil {
		return err
	}
	for {
		_, err := stream.Recv() // drain the chunks; we only care about transfer cost
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (w workload) write(ctx context.Context, client kvcachev1.KVCacheClient, b blockhash.Block, payload []byte, root []byte) error {
	stream, err := client.Write(ctx)
	if err != nil {
		return err
	}
	// routing_key = the prompt's root hash, so the primary's replicator places the replica
	// where read-failover will later look for it (ADR 0021 placement-by-root).
	hdr := &kvcachev1.WriteHeader{ModelId: w.model, BlockHash: b.Hash[:], TokenIds: b.TokenIDs, TotalSize: uint64(len(payload)), RoutingKey: root}
	if err := stream.Send(&kvcachev1.WriteChunk{Msg: &kvcachev1.WriteChunk_Header{Header: hdr}}); err != nil {
		return err
	}
	for off := 0; off < len(payload); off += writeChunkBytes {
		end := off + writeChunkBytes
		if end > len(payload) {
			end = len(payload)
		}
		chunk := &kvcachev1.KVChunk{Data: payload[off:end], Last: end == len(payload)}
		if err := stream.Send(&kvcachev1.WriteChunk{Msg: &kvcachev1.WriteChunk_Chunk{Chunk: chunk}}); err != nil {
			return err
		}
	}
	_, err = stream.CloseAndRecv()
	return err
}

func report(results []result, elapsed time.Duration) {
	var reqs, blocks, hits, writes, errs, degraded int
	var lats []time.Duration
	perShard := map[string]int{}
	for _, r := range results {
		reqs += r.requests
		blocks += r.blocks
		hits += r.hits
		writes += r.writes
		errs += r.errors
		degraded += r.degraded
		lats = append(lats, r.latencies...)
		for node, c := range r.perShard {
			perShard[node] += c
		}
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	hitRate := 0.0
	if blocks > 0 {
		hitRate = float64(hits) / float64(blocks) * 100
	}

	fmt.Println("---- loadgen report ----")
	fmt.Printf("requests:            %d (%d errors, %d degraded-to-miss)\n", reqs, errs, degraded)
	fmt.Printf("blocks:              %d  (hits %d / writes %d)\n", blocks, hits, writes)
	fmt.Printf("block hit rate:      %.1f%%\n", hitRate)
	fmt.Printf("elapsed:             %s\n", elapsed.Round(time.Millisecond))
	if s := elapsed.Seconds(); s > 0 {
		fmt.Printf("throughput:          %.0f req/s\n", float64(reqs)/s)
	}
	fmt.Printf("latency p50/p95/p99: %s / %s / %s\n",
		percentile(lats, 0.50).Round(time.Microsecond),
		percentile(lats, 0.95).Round(time.Microsecond),
		percentile(lats, 0.99).Round(time.Microsecond))
	reportDistribution(perShard, reqs)
}

// reportDistribution prints requests-per-shard, sorted, with each shard's share. This is
// the hot-shard effect ADR 0014 left for Phase 2 to measure: with prefix-affinity a high
// prefix-share concentrates the hot prompt on one shard.
func reportDistribution(perShard map[string]int, total int) {
	if len(perShard) == 0 {
		return
	}
	nodes := make([]string, 0, len(perShard))
	for n := range perShard {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	fmt.Println("per-shard distribution:")
	for _, n := range nodes {
		share := 0.0
		if total > 0 {
			share = float64(perShard[n]) / float64(total) * 100
		}
		fmt.Printf("  %-22s %d (%.1f%%)\n", n, perShard[n], share)
	}
}

// percentile returns the p-th percentile of an already-sorted slice (nearest-rank).
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func randomTokens(rng *rand.Rand, n int) []int32 {
	t := make([]int32, n)
	for i := range t {
		t[i] = int32(rng.Intn(32000)) // a plausible vocab range
	}
	return t
}
