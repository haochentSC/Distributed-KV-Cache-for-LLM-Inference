// Command loadgen drives the cache with synthetic, GPU-free traffic (plan §3 component 6).
//
// It models shared-prefix LLM traffic: a fraction of requests reuse a hot prefix (think
// a shared system prompt), each with its own divergent tail. For every request it chains
// the tokens into block hashes (internal/blockhash), Looks them up, Fetches the cached
// prefix run, and Writes the misses — then reports throughput, block hit rate, and
// latency percentiles. No GPU or vLLM needed (plan §1, §3).
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvcachev1 "github.com/haochentSC/distributed-kv-cache/gen/kvcache/v1"
	"github.com/haochentSC/distributed-kv-cache/internal/blockhash"
)

// writeChunkBytes bounds each Write frame under gRPC's 4 MB cap (ADR 0012), mirroring
// the server's Fetch chunking.
const writeChunkBytes = 1 << 20 // 1 MiB

func main() {
	addr := flag.String("addr", "localhost:50051", "cache server address")
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

	log.Printf("loadgen target=%s payload=%dB block=%dtok prefix-share=%.2f conc=%d req/cli=%d prefix=%dblk tail=%dblk seed=%d",
		*addr, *payloadBytes, *blockTokens, *prefixShare, *concurrency, *requests, *prefixBlocks, *tailBlocks, *seed)

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()

	// The hot prefix is identical for every hot request, so once any worker writes it
	// the rest hit it — that warming is exactly what we want to measure.
	hotPrefix := randomTokens(rand.New(rand.NewSource(*seed)), *prefixBlocks**blockTokens)

	w := workload{
		client:       kvcachev1.NewKVCacheClient(conn),
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

type workload struct {
	client       kvcachev1.KVCacheClient
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
	blocks    int // total blocks looked up
	hits      int // blocks served from cache (Fetched)
	writes    int // blocks written on a miss
	errors    int
	latencies []time.Duration
}

func (w workload) run(rng *rand.Rand, n int) result {
	res := result{latencies: make([]time.Duration, 0, n)}
	payload := make([]byte, w.payloadBytes) // reused per worker; contents irrelevant
	for i := 0; i < n; i++ {
		blocks := blockhash.ChainBlocks(w.model, w.buildTokens(rng), w.blockTokens)
		if len(blocks) == 0 {
			continue
		}
		t0 := time.Now()
		err := w.oneRequest(context.Background(), blocks, payload, &res)
		res.latencies = append(res.latencies, time.Since(t0))
		res.requests++
		res.blocks += len(blocks)
		if err != nil {
			res.errors++
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

func (w workload) oneRequest(ctx context.Context, blocks []blockhash.Block, payload []byte, res *result) error {
	bh := make([][]byte, len(blocks))
	for i := range blocks {
		bh[i] = blocks[i].Hash[:]
	}
	lookup, err := w.client.Lookup(ctx, &kvcachev1.LookupRequest{ModelId: w.model, BlockHashes: bh})
	if err != nil {
		return err
	}

	// Longest contiguous run of present blocks from index 0 (client-side assembly, ADR 0011).
	run := 0
	for run < len(lookup.GetBlocks()) && lookup.GetBlocks()[run].GetHasEntry() {
		run++
	}

	for i := 0; i < run; i++ { // reuse the cached prefix
		if err := w.fetch(ctx, blocks[i]); err != nil {
			return err
		}
		res.hits++
	}
	for i := run; i < len(blocks); i++ { // simulate prefill + store of the misses
		if err := w.write(ctx, blocks[i], payload); err != nil {
			return err
		}
		res.writes++
	}
	return nil
}

func (w workload) fetch(ctx context.Context, b blockhash.Block) error {
	stream, err := w.client.Fetch(ctx, &kvcachev1.FetchRequest{ModelId: w.model, BlockHash: b.Hash[:], TokenIds: b.TokenIDs})
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

func (w workload) write(ctx context.Context, b blockhash.Block, payload []byte) error {
	stream, err := w.client.Write(ctx)
	if err != nil {
		return err
	}
	hdr := &kvcachev1.WriteHeader{ModelId: w.model, BlockHash: b.Hash[:], TokenIds: b.TokenIDs, TotalSize: uint64(len(payload))}
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
	var reqs, blocks, hits, writes, errs int
	var lats []time.Duration
	for _, r := range results {
		reqs += r.requests
		blocks += r.blocks
		hits += r.hits
		writes += r.writes
		errs += r.errors
		lats = append(lats, r.latencies...)
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	hitRate := 0.0
	if blocks > 0 {
		hitRate = float64(hits) / float64(blocks) * 100
	}

	fmt.Println("---- loadgen report ----")
	fmt.Printf("requests:            %d (%d errors)\n", reqs, errs)
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
