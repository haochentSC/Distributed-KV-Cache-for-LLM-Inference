// Command loadgen drives the cache with synthetic, GPU-free traffic (plan §3 component 6).
//
// It models shared-prefix LLM traffic: a fraction of requests reuse a hot prefix (think
// a shared system prompt), each with its own divergent tail. For every request it chains
// the tokens into block hashes (internal/blockhash), routes the prompt to its owning
// shard via the consistent-hash ring (prefix-affinity on block_hash[0], ADR 0014), Looks
// the blocks up, Fetches the cached prefix run, and Writes the misses — then reports
// throughput, block hit rate, latency percentiles, and the per-shard request distribution
// (ADR 0014's hot-shard measurement). No GPU or vLLM needed (plan §1, §3).
//
// Phase 4 (chaos, ADR 0026) adds three things used by cmd/chaos:
//   - -verify: a CORRECTNESS ORACLE. Each block's payload is a deterministic function of its
//     hash, so a reader can regenerate the expected bytes and detect any corruption or
//     mis-served block. The process exits non-zero if any violation is seen — that is the
//     hard assertion the chaos run makes (ADR 0016: never serve KV mismatching the key).
//   - -duration: run for a wall-clock window instead of a fixed request count, so the load
//     spans a chaos run that is killing nodes underneath it.
//   - -stats-every: periodic throughput/error/violation line, so the recovery dip after a
//     node kill is visible in the terminal (and lines up with the Grafana panels).
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	requests := flag.Int("requests", 200, "requests per client (ignored when -duration > 0)")
	duration := flag.Duration("duration", 0, "run for this wall-clock window instead of -requests (for chaos runs)")
	statsEvery := flag.Duration("stats-every", 0, "if > 0, print a live throughput/violation line at this interval")
	verify := flag.Bool("verify", false, "correctness oracle: payload=f(hash); verify every Fetch and exit non-zero on any mismatch (ADR 0016)")
	prefixBlocks := flag.Int("prefix-blocks", 8, "blocks in the shared/hot prefix")
	tailBlocks := flag.Int("tail-blocks", 2, "blocks in each request's divergent tail")
	model := flag.String("model", "tinyllama-1.1b", "model id")
	seed := flag.Int64("seed", 1, "RNG seed (logged for reproducibility)")
	trace := flag.String("trace", "", "replay a real-workload trace (JSONL from scripts/prep_sharegpt.py) instead of synthetic traffic; ignores prefix/tail flags")
	flag.Parse()

	log.Printf("loadgen payload=%dB block=%dtok prefix-share=%.2f conc=%d req/cli=%d dur=%s verify=%v prefix=%dblk tail=%dblk seed=%d",
		*payloadBytes, *blockTokens, *prefixShare, *concurrency, *requests, *duration, *verify, *prefixBlocks, *tailBlocks, *seed)

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

	// live is the lock-free counter set behind -stats-every; nil disables it so the hot path
	// pays nothing when stats are off. Workers bump it with atomics; a ticker prints deltas.
	var live *liveStats
	if *statsEvery > 0 {
		live = &liveStats{}
	}

	w := workload{
		router:       router,
		model:        *model,
		blockTokens:  *blockTokens,
		payloadBytes: *payloadBytes,
		prefixShare:  *prefixShare,
		prefixBlocks: *prefixBlocks,
		tailBlocks:   *tailBlocks,
		hotPrefix:    hotPrefix,
		verify:       *verify,
		live:         live,
	}

	// Trace mode: replay a real multi-turn workload (ShareGPT etc.) instead of synthesizing.
	// All workers share one trace slice and one atomic cursor, so each request is handed out
	// once (count mode) or wrapped around (duration mode). The synthetic prefix/tail flags are
	// ignored — reuse now comes from the conversations themselves.
	if *trace != "" {
		recs, err := loadTrace(*trace)
		if err != nil {
			log.Fatalf("load trace: %v", err)
		}
		if len(recs) == 0 {
			log.Fatalf("trace %s has no usable requests", *trace)
		}
		log.Printf("loadgen trace=%s requests=%d (synthetic prefix/tail flags ignored)", *trace, len(recs))
		w.trace = recs
		w.traceCursor = new(atomic.Int64)
	}

	// Duration mode: every worker loops until a shared deadline rather than a fixed count, so
	// the load spans the whole chaos window. requests is ignored in that case.
	var deadline time.Time
	if *duration > 0 {
		deadline = time.Now().Add(*duration)
	}

	// Live stats ticker (optional). Stops when load finishes via the done channel.
	done := make(chan struct{})
	if live != nil {
		go live.printLoop(*statsEvery, time.Now(), done)
	}

	results := make([]result, *concurrency)
	var wg sync.WaitGroup
	start := time.Now()
	for c := 0; c < *concurrency; c++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Per-worker RNG + per-worker result => no shared mutable state, no locks.
			results[id] = w.run(rand.New(rand.NewSource(*seed+int64(id)+1)), *requests, deadline)
		}(c)
	}
	wg.Wait()
	close(done)
	violations := report(results, time.Since(start))

	// The chaos run keys off this exit code: a single mis-served byte fails the build.
	if *verify && violations > 0 {
		log.Printf("FAIL: %d correctness violation(s) — served KV mismatching the requested key (ADR 0016)", violations)
		os.Exit(1)
	}
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
	verify       bool       // correctness oracle on (payload=f(hash), check every Fetch)
	live         *liveStats // nil unless -stats-every; lock-free live counters

	// Trace mode (set only with -trace): a shared, read-only slice of pre-tokenized requests
	// and a shared atomic cursor so workers consume it exactly once (or wrap in duration mode).
	// nil trace => synthetic mode (buildTokens).
	trace       []traceRecord
	traceCursor *atomic.Int64
}

type result struct {
	requests   int
	blocks     int            // total blocks looked up
	hits       int            // blocks served from cache (Fetched)
	writes     int            // blocks written on a miss
	errors     int            // RPC errors mid-request (degrade-to-miss)
	degraded   int            // requests with no reachable owner (routed to a miss)
	violations int            // Fetched bytes that did NOT match payload=f(hash) — must be 0 (ADR 0016)
	perShard   map[string]int // requests routed to each shard (hot-shard measurement)
	latencies  []time.Duration
}

func (w workload) run(rng *rand.Rand, n int, deadline time.Time) result {
	res := result{latencies: make([]time.Duration, 0, n), perShard: make(map[string]int)}
	payload := make([]byte, w.payloadBytes) // reused per worker; contents irrelevant unless -verify
	var expected []byte
	if w.verify {
		expected = make([]byte, w.payloadBytes) // scratch the Fetch result is compared against
	}

	// Termination: duration mode runs until the shared deadline; otherwise synthetic mode does
	// n requests per worker, and trace mode runs until the shared trace is consumed (signaled by
	// nextTokens returning ok=false). The deadline check is per-request — fine, since each
	// request is many ms of RPC work.
	for i := 0; ; i++ {
		if !deadline.IsZero() {
			if !time.Now().Before(deadline) {
				break
			}
		} else if w.trace == nil && i >= n {
			break
		}
		tokens, ok := w.nextTokens(rng, deadline)
		if !ok {
			break // trace exhausted (count mode)
		}
		blocks := blockhash.ChainBlocks(w.model, tokens, w.blockTokens)
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
			if w.live != nil {
				w.live.degraded.Add(1)
			}
			continue
		}
		res.perShard[owner]++

		t0 := time.Now()
		err := w.oneRequestWithFailover(context.Background(), conns, blocks, payload, expected, root, &res)
		res.latencies = append(res.latencies, time.Since(t0))
		res.blocks += len(blocks)
		if w.live != nil {
			w.live.requests.Add(1)
		}
		if err != nil {
			res.errors++ // every owner exhausted; treated as a degrade-to-miss
			if w.live != nil {
				w.live.errors.Add(1)
			}
		}
	}
	return res
}

// nextTokens returns the next request's token sequence and ok=false when there is no more
// work. In synthetic mode it just builds a fresh sequence. In trace mode it hands out the
// shared trace via an atomic cursor: count mode (no deadline) stops once the trace is
// consumed; duration mode wraps around so the load can span a chaos window.
func (w workload) nextTokens(rng *rand.Rand, deadline time.Time) ([]int32, bool) {
	if w.trace == nil {
		return w.buildTokens(rng), true
	}
	n := int64(len(w.trace))
	idx := w.traceCursor.Add(1) - 1
	if deadline.IsZero() {
		if idx >= n {
			return nil, false
		}
		return w.trace[idx].Tokens, true
	}
	return w.trace[idx%n].Tokens, true
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
func (w workload) oneRequestWithFailover(ctx context.Context, conns []*grpc.ClientConn, blocks []blockhash.Block, payload, expected []byte, root []byte, res *result) error {
	var lastErr error
	for i, conn := range conns {
		client := kvcachev1.NewKVCacheClient(conn)
		// Writes only on the primary (i == 0). On a failover read (i > 0) we still attempt
		// Lookup+Fetch; if blocks aren't present on the replica, the run is short and the
		// remaining misses just won't be written this round — the next request to the new
		// primary (after the dead one is removed from the ring) will handle the writes.
		err := w.oneRequest(ctx, client, blocks, payload, expected, res, i == 0, root)
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

func (w workload) oneRequest(ctx context.Context, client kvcachev1.KVCacheClient, blocks []blockhash.Block, payload, expected []byte, res *result, allowWrites bool, root []byte) error {
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
		hit, err := w.fetch(ctx, client, blocks[i], expected, res)
		if err != nil {
			return err
		}
		if !hit {
			// Block vanished between Lookup and Fetch (evicted, or a failover changed the
			// owner) — a MISS, not a violation. Stop reusing the prefix here; the remaining
			// blocks fall through to the write path below.
			run = i
			break
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

// fetch streams a block's bytes. It returns hit=false (not an error) on a NotFound — that is a
// legitimate miss (eviction / failover), never a violation. When -verify is on it assembles the
// bytes and compares them against payload=f(hash); a mismatch is the ADR 0016 violation we are
// here to catch, and it is recorded but NOT treated as fatal mid-run (we count them all and the
// process exits non-zero at the end).
func (w workload) fetch(ctx context.Context, client kvcachev1.KVCacheClient, b blockhash.Block, expected []byte, res *result) (bool, error) {
	stream, err := client.Fetch(ctx, &kvcachev1.FetchRequest{ModelId: w.model, BlockHash: b.Hash[:], TokenIds: b.TokenIDs})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return false, nil // miss, not a transport error
		}
		return false, err
	}

	var got []byte // assembled only when verifying; otherwise we just drain for transfer cost
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return false, nil
			}
			return false, err
		}
		if w.verify {
			got = append(got, chunk.GetData()...)
		}
	}

	if w.verify {
		fillVerifiable(expected, b.Hash) // regenerate what we SHOULD have stored for this hash
		if len(got) != len(expected) || !equalBytes(got, expected) {
			res.violations++
			if w.live != nil {
				w.live.violations.Add(1)
			}
			log.Printf("VIOLATION: block %x served %d bytes that do not match payload=f(hash)", b.Hash[:6], len(got))
		}
	}
	return true, nil
}

func (w workload) write(ctx context.Context, client kvcachev1.KVCacheClient, b blockhash.Block, payload []byte, root []byte) error {
	if w.verify {
		// Stamp this block's content as a deterministic function of its hash, so any reader can
		// regenerate and verify it. Cheap and overwrites the shared per-worker buffer in place.
		fillVerifiable(payload, b.Hash)
	}
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

// fillVerifiable writes a deterministic byte pattern derived from the block hash into buf, so a
// reader that knows the hash can regenerate the exact bytes and detect corruption or a mis-served
// block. It is NOT cryptographic — the point is only that block A's content can never equal block
// B's, so "served the wrong block" and "served corrupt bytes" both surface as a byte mismatch.
// An xorshift64* PRNG seeded from the hash fills 8 bytes per step (fast even at 2 MiB).
func fillVerifiable(buf []byte, hash [32]byte) {
	x := binary.LittleEndian.Uint64(hash[0:8]) ^ binary.LittleEndian.Uint64(hash[8:16]) ^
		binary.LittleEndian.Uint64(hash[16:24]) ^ binary.LittleEndian.Uint64(hash[24:32])
	if x == 0 {
		x = 0x9e3779b97f4a7c15 // avoid the xorshift fixed point at 0
	}
	i := 0
	for ; i+8 <= len(buf); i += 8 {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		binary.LittleEndian.PutUint64(buf[i:], x*0x2545F4914F6CDD1D)
	}
	for j := 0; i < len(buf); i, j = i+1, j+1 {
		buf[i] = byte(x >> (uint(j%8) * 8)) // tail bytes
	}
}

// equalBytes is bytes.Equal without the import; kept local to avoid pulling "bytes" in for one call.
func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// liveStats are lock-free counters printed periodically under -stats-every so the recovery dip
// after a node kill is visible in real time. Updated with atomics on the hot path (only when
// enabled), read by printLoop.
type liveStats struct {
	requests   atomic.Int64
	errors     atomic.Int64
	degraded   atomic.Int64
	violations atomic.Int64
}

// printLoop prints cumulative-and-delta counters every interval until done is closed. It runs in
// its own goroutine; the atomics make the read race-free without locking the workers.
func (s *liveStats) printLoop(interval time.Duration, start time.Time, done <-chan struct{}) {
	t := time.NewTicker(interval)
	defer t.Stop()
	var prevReq, prevErr int64
	for {
		select {
		case <-done:
			return
		case <-t.C:
			req, errs, deg, vio := s.requests.Load(), s.errors.Load(), s.degraded.Load(), s.violations.Load()
			dReq, dErr := req-prevReq, errs-prevErr
			prevReq, prevErr = req, errs
			rate := float64(dReq) / interval.Seconds()
			fmt.Printf("[%5.1fs] %.0f req/s  reqs=%d errors=%d(+%d) degraded=%d violations=%d\n",
				time.Since(start).Seconds(), rate, req, errs, dErr, deg, vio)
		}
	}
}

// report prints the final aggregate and returns the total correctness violations so main can set
// the process exit code.
func report(results []result, elapsed time.Duration) int {
	var reqs, blocks, hits, writes, errs, degraded, violations int
	var lats []time.Duration
	perShard := map[string]int{}
	for _, r := range results {
		reqs += r.requests
		blocks += r.blocks
		hits += r.hits
		writes += r.writes
		errs += r.errors
		degraded += r.degraded
		violations += r.violations
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
	fmt.Printf("correctness:         %d violations (must be 0; ADR 0016)\n", violations)
	fmt.Printf("elapsed:             %s\n", elapsed.Round(time.Millisecond))
	if s := elapsed.Seconds(); s > 0 {
		fmt.Printf("throughput:          %.0f req/s\n", float64(reqs)/s)
	}
	fmt.Printf("latency p50/p95/p99: %s / %s / %s\n",
		percentile(lats, 0.50).Round(time.Microsecond),
		percentile(lats, 0.95).Round(time.Microsecond),
		percentile(lats, 0.99).Round(time.Microsecond))
	reportDistribution(perShard, reqs)
	return violations
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
