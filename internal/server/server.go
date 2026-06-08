// Package server implements the gRPC KVCache service over an in-memory Store.
package server

import (
	"context"
	"io"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kvcachev1 "github.com/haochentSC/distributed-kv-cache/gen/kvcache/v1"
	"github.com/haochentSC/distributed-kv-cache/internal/cache"
)

// fetchChunkBytes bounds each Fetch frame well under gRPC's 4 MB message cap (ADR 0012).
const fetchChunkBytes = 1 << 20 // 1 MiB

// replicaEnqueuer schedules an async primary->replica copy of a freshly-written block
// (ADR 0021). It is an interface so the Write hook can be tested with a fake and so a
// single-node Server (repl == nil) skips replication entirely. The concrete impl is
// *Replicator (replicator.go).
type replicaEnqueuer interface {
	Enqueue(job ReplicaJob)
}

// coldReader is the read-through seam to the cold tier (ADR 0027): on a hot miss, Fetch asks it
// for the block. It is a narrow interface (not coldtier.Tier) so this package — and its tests —
// stay free of the AWS SDK; main injects an adapter over coldtier.Tier. nil disables read-through.
// A storage error or a miss both mean "not here" (the caller serves NotFound); the tier can never
// return a wrong block because it is keyed by (model, block_hash) (ADR 0016).
type coldReader interface {
	Get(ctx context.Context, model string, h cache.BlockHash) (kv []byte, version uint64, tokenIDs []int32, ok bool, err error)
}

// metricsSink receives operational counters from the gRPC handlers. It is a narrow interface
// (not a concrete *metrics.Metrics) so this package stays decoupled from Prometheus and tests
// can pass a fake; *metrics.Metrics satisfies it (ADR 0025). The replicator uses the Replica
// method too — see replicator.go. nil is never stored: New* default to noopMetrics, so the
// handlers can call s.metrics.* unconditionally.
type metricsSink interface {
	Hit(op string)
	Miss(op string)
	Eviction(reason string, n int)
	Replica(result string)
}

// noopMetrics is the default sink: every method is a no-op, so a Server built without metrics
// (the test/single-node path) needs no nil checks on the hot path.
type noopMetrics struct{}

func (noopMetrics) Hit(string)           {}
func (noopMetrics) Miss(string)          {}
func (noopMetrics) Eviction(string, int) {}
func (noopMetrics) Replica(string)       {}

// Server implements the generated kvcachev1.KVCacheServer. Embedding the
// Unimplemented base makes future RPC additions non-breaking.
type Server struct {
	kvcachev1.UnimplementedKVCacheServer
	store   *cache.Store
	repl    replicaEnqueuer // nil on a single node (no replication); set in Phase 3 Sub-stage B
	metrics metricsSink     // never nil; noopMetrics until WithMetrics is called
	cold    coldReader      // nil = no cold tier; set by WithColdReader (ADR 0027)
}

// New wires a Server to a Store with NO replication (single-node / tests).
func New(store *cache.Store) *Server { return &Server{store: store, metrics: noopMetrics{}} }

// NewWithReplicator wires a Server that, after each successful Write (it is the PRIMARY for
// that block), hands the block to repl for async forwarding to the replica (ADR 0021).
func NewWithReplicator(store *cache.Store, repl replicaEnqueuer) *Server {
	return &Server{store: store, repl: repl, metrics: noopMetrics{}}
}

// WithMetrics installs a metrics sink and returns the Server for chaining (main wires the
// process-wide *metrics.Metrics here). A nil m is ignored, keeping the noop default.
func (s *Server) WithMetrics(m metricsSink) *Server {
	if m != nil {
		s.metrics = m
	}
	return s
}

// WithColdReader enables Fetch read-through to a cold tier (ADR 0027) and returns the Server for
// chaining. A nil c leaves read-through disabled (the default).
func (s *Server) WithColdReader(c coldReader) *Server {
	if c != nil {
		s.cold = c
	}
	return s
}

// toBlockHash converts a wire []byte to a cache.BlockHash, rejecting wrong lengths.
func toBlockHash(b []byte) (cache.BlockHash, bool) {
	var h cache.BlockHash
	if len(b) != len(h) {
		return h, false
	}
	copy(h[:], b)
	return h, true
}

// Lookup reports per-block presence (metadata only — never tensor bytes). The response
// blocks slice is parallel to req.BlockHashes; a wrong-length or absent hash yields a
// zero BlockPresence (HasEntry=false). Uses Peek so a presence check is not counted as
// a reuse (ADR 0011). The CLIENT assembles the longest contiguous run from index 0.
func (s *Server) Lookup(ctx context.Context, req *kvcachev1.LookupRequest) (*kvcachev1.LookupResponse, error) {
	model := req.GetModelId()
	hashes := req.GetBlockHashes()
	blocks := make([]*kvcachev1.BlockPresence, len(hashes))
	for i, hb := range hashes {
		bp := &kvcachev1.BlockPresence{}
		if h, ok := toBlockHash(hb); ok {
			if e, hit := s.store.Peek(model, h); hit {
				bp.HasEntry = true
				bp.Version = e.Version
				bp.SizeBytes = uint64(e.SizeBytes)
				s.metrics.Hit("lookup")
			} else {
				s.metrics.Miss("lookup")
			}
		} else {
			s.metrics.Miss("lookup") // a malformed hash is, from the cache's view, a miss
		}
		blocks[i] = bp
	}
	return &kvcachev1.LookupResponse{Blocks: blocks}, nil
}

// Fetch server-streams a block's KV bytes in bounded chunks, terminating with
// Last=true. Returns NotFound on a miss, version mismatch, or token verification
// mismatch. The model + optional token checks uphold ADR 0016; block_hash stays opaque.
func (s *Server) Fetch(req *kvcachev1.FetchRequest, stream kvcachev1.KVCache_FetchServer) error {
	h, ok := toBlockHash(req.GetBlockHash())
	if !ok {
		return status.Error(codes.InvalidArgument, "block_hash must be 32 bytes")
	}
	e, hit := s.store.Get(req.GetModelId(), h)
	if !hit {
		// Hot miss — read through to the cold tier before giving up (ADR 0027). A cold hit is
		// streamed (and re-admitted); a cold miss falls through to NotFound below.
		if served, err := s.fetchFromCold(stream, req, h); err != nil || served {
			return err
		}
		s.metrics.Miss("fetch")
		return status.Error(codes.NotFound, "block not cached")
	}
	s.metrics.Hit("fetch")
	if v := req.GetVersion(); v != 0 && v != e.Version {
		return status.Error(codes.NotFound, "requested version not available")
	}
	if toks := req.GetTokenIds(); len(toks) > 0 && !slices.Equal(toks, e.TokenIDs) {
		return status.Error(codes.NotFound, "token_ids do not match cached block")
	}

	return streamKV(stream, e.KV) // e.KV is an immutable snapshot — safe to stream lock-free
}

// fetchFromCold serves a hot miss from the cold tier (ADR 0027). It returns served=true ONLY when
// it streamed matching bytes; a cold miss, a storage error, or a version/token mismatch all return
// served=false so Fetch emits NotFound (upholding ADR 0016 — we never stream non-matching bytes).
func (s *Server) fetchFromCold(stream kvcachev1.KVCache_FetchServer, req *kvcachev1.FetchRequest, h cache.BlockHash) (bool, error) {
	kv, ok := s.coldHit(stream.Context(), req.GetModelId(), h, req.GetVersion(), req.GetTokenIds())
	if !ok {
		return false, nil
	}
	return true, streamKV(stream, kv)
}

// coldHit is the cold-tier read-through decision shared by Fetch and BatchFetch: fetch from the
// cold tier, apply the SAME version/token guards as the hot path, re-admit on a hit, and return
// the bytes to stream. ok=false means "not a usable hit" (cold miss, storage error, or guard
// mismatch) — the caller degrades to a recompute, never to wrong bytes (ADR 0013/0016). On a hit
// it re-admits to the hot store so repeat fetches skip the cold round-trip, but skips the re-admit
// if that would breach the hard ceiling (the evictor would just spill it straight back —
// admit→evict→spill thrash). Re-admit is at the ORIGINAL version so a replica's copy still agrees.
func (s *Server) coldHit(ctx context.Context, model string, h cache.BlockHash, version uint64, tokenIDs []int32) ([]byte, bool) {
	if s.cold == nil {
		return nil, false
	}
	kv, ver, toks, ok, err := s.cold.Get(ctx, model, h)
	if err != nil || !ok {
		return nil, false
	}
	if version != 0 && version != ver {
		return nil, false
	}
	if len(tokenIDs) > 0 && !slices.Equal(tokenIDs, toks) {
		return nil, false
	}
	if !s.store.OverHardLimit(int64(len(kv))) {
		s.store.PutWithVersion(h, &cache.Entry{ModelID: model, KV: kv, TokenIDs: toks}, ver)
	}
	s.metrics.Hit("fetch_cold")
	return kv, true
}

// BatchFetch streams many blocks' KV bytes over one call (proto comment has the contract). Each
// block is resolved exactly like Fetch — hot Get, then cold read-through, then the version/token
// guards — but failures are per-block: a malformed hash, a miss, or a guard mismatch emits a single
// found=false terminal frame for that index and the loop continues, so one absent block never
// fails the batch (ADR 0013/0016). Blocks are streamed in request order; the index tag lets the
// client demux regardless.
func (s *Server) BatchFetch(req *kvcachev1.BatchFetchRequest, stream kvcachev1.KVCache_BatchFetchServer) error {
	model := req.GetModelId()
	for i, fb := range req.GetBlocks() {
		idx := uint32(i)
		h, ok := toBlockHash(fb.GetBlockHash())
		if !ok {
			s.metrics.Miss("batch_fetch")
			if err := sendNotFound(stream, idx); err != nil {
				return err
			}
			continue
		}
		kv, ok := s.batchResolve(stream.Context(), model, fb, h)
		if !ok {
			s.metrics.Miss("batch_fetch")
			if err := sendNotFound(stream, idx); err != nil {
				return err
			}
			continue
		}
		s.metrics.Hit("batch_fetch")
		if err := streamBatchKV(stream, idx, kv); err != nil {
			return err
		}
	}
	return nil
}

// batchResolve returns one block's bytes for BatchFetch: the hot store first (with the version/
// token guards), then the cold tier via the shared coldHit. ok=false => not a usable hit.
func (s *Server) batchResolve(ctx context.Context, model string, fb *kvcachev1.FetchBlock, h cache.BlockHash) ([]byte, bool) {
	e, hit := s.store.Get(model, h)
	if !hit {
		return s.coldHit(ctx, model, h, fb.GetVersion(), fb.GetTokenIds())
	}
	if v := fb.GetVersion(); v != 0 && v != e.Version {
		return nil, false
	}
	if toks := fb.GetTokenIds(); len(toks) > 0 && !slices.Equal(toks, e.TokenIDs) {
		return nil, false
	}
	return e.KV, true // e.KV is an immutable snapshot — safe to stream lock-free
}

// sendNotFound emits the single terminal frame that marks a batch block as absent/mismatched.
func sendNotFound(stream kvcachev1.KVCache_BatchFetchServer, idx uint32) error {
	return stream.Send(&kvcachev1.BatchKVChunk{Index: idx, Found: false, Last: true})
}

// streamBatchKV streams one present block's bytes as found=true frames, tagged with idx and
// terminating with last=true (one empty frame for an empty payload), mirroring streamKV.
func streamBatchKV(stream kvcachev1.KVCache_BatchFetchServer, idx uint32, data []byte) error {
	for off := 0; off < len(data); off += fetchChunkBytes {
		end := off + fetchChunkBytes
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&kvcachev1.BatchKVChunk{Index: idx, Data: data[off:end], Found: true, Last: end == len(data)}); err != nil {
			return err
		}
	}
	if len(data) == 0 {
		return stream.Send(&kvcachev1.BatchKVChunk{Index: idx, Found: true, Last: true})
	}
	return nil
}

// streamKV server-streams data in bounded chunks, terminating with Last=true (one empty frame for
// an empty payload). Shared by the hot and cold-tier Fetch paths.
func streamKV(stream kvcachev1.KVCache_FetchServer, data []byte) error {
	for off := 0; off < len(data); off += fetchChunkBytes {
		end := off + fetchChunkBytes
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&kvcachev1.KVChunk{Data: data[off:end], Last: end == len(data)}); err != nil {
			return err
		}
	}
	if len(data) == 0 { // an empty payload still needs one terminating frame
		return stream.Send(&kvcachev1.KVChunk{Last: true})
	}
	return nil
}

// blockStream is the common shape of the Write and Replicate server streams (both are
// gRPC client-streaming: a header then KVChunk frames). Declaring our own tiny interface
// lets assembleBlock serve both without importing grpc here.
type blockStream interface {
	Recv() (*kvcachev1.WriteChunk, error)
}

// assembleBlock reads a Write/Replicate stream: the FIRST message must be a WriteHeader,
// the rest KVChunk data frames, returning the header and the assembled tensor bytes.
// Shared by Write and Replicate (identical wire shape, ADR 0021). [scaffold — extracted
// verbatim from the original Write body.]
func assembleBlock(stream blockStream) (*kvcachev1.WriteHeader, []byte, error) {
	first, err := stream.Recv()
	if err != nil {
		return nil, nil, err
	}
	hdr := first.GetHeader()
	if hdr == nil {
		return nil, nil, status.Error(codes.InvalidArgument, "first message must be a WriteHeader")
	}

	var buf []byte
	if n := hdr.GetTotalSize(); n > 0 {
		buf = make([]byte, 0, n) // pre-size to avoid repeated regrow on large payloads
	}
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		if msg.GetHeader() != nil {
			return nil, nil, status.Error(codes.InvalidArgument, "unexpected second WriteHeader")
		}
		if c := msg.GetChunk(); c != nil {
			buf = append(buf, c.GetData()...)
		}
	}
	return hdr, buf, nil
}

// Write client-streams one block to its PRIMARY (this shard, the ring owner). It assembles
// the bytes, stores the entry (minting the version), returns the assigned version (ADR
// 0012), and — if replication is configured — hands the block to the replicator for async
// forwarding to the replica (ADR 0021). The client is acked as soon as the PRIMARY stored
// it; replication is best-effort and off the ack path (ADR 0013).
func (s *Server) Write(stream kvcachev1.KVCache_WriteServer) error {
	hdr, buf, err := assembleBlock(stream)
	if err != nil {
		return err
	}
	h, ok := toBlockHash(hdr.GetBlockHash())
	if !ok {
		return status.Error(codes.InvalidArgument, "block_hash must be 32 bytes")
	}

	// Write-admission guard (ADR 0017). Reject-fast if this block would breach the hard byte
	// ceiling: the evictor wakes at the high-water mark, but a burst of large writes can outrun
	// it, and the hi-water..max headroom is exactly the cushion that absorbs that race. A
	// rejected write is just a future recompute (ADR 0013) — never a correctness violation, so
	// shedding load here is safe. Unbounded stores never reject (OverHardLimit is false).
	if s.store.OverHardLimit(int64(len(buf))) {
		return status.Error(codes.ResourceExhausted, "cache full")
	}

	// buf is freshly built and not reused, so Put may take ownership (no copy).
	ver := s.store.Put(h, &cache.Entry{
		ModelID:       hdr.GetModelId(),
		KV:            buf,
		TokenIDs:      hdr.GetTokenIds(),
		TenantID:      hdr.GetTenantId(),
		RecomputeCost: hdr.GetRecomputeCost(),
	})

	if s.repl != nil {
		// Buf is shared with the stored Entry. Safe: Put took ownership and publishes the
		// entry as an immutable snapshot — neither side mutates it, so the replicator can
		// stream the same bytes off-thread without copying.
		s.repl.Enqueue(ReplicaJob{
			Hash:          h,
			ModelID:       hdr.GetModelId(),
			KV:            buf,
			TokenIDs:      hdr.GetTokenIds(),
			TenantID:      hdr.GetTenantId(),
			RecomputeCost: hdr.GetRecomputeCost(),
			Version:       ver,
			RoutingKey:    hdr.GetRoutingKey(),
		})
	}

	return stream.SendAndClose(&kvcachev1.WriteResponse{Version: ver, Stored: true})
}

// Replicate is the PRIMARY->REPLICA copy path (ADR 0021): a peer primary forwards a block
// it owns so this node holds the replica. Same wire shape as Write, but it stores at the
// header's AUTHORITATIVE version (via Store.PutWithVersion) and MUST NOT re-forward — that
// is what stops a replication loop. Fire-and-forget from the primary's side.
func (s *Server) Replicate(stream kvcachev1.KVCache_ReplicateServer) error {
	hdr, buf, err := assembleBlock(stream)
	if err != nil {
		return err
	}
	h, ok := toBlockHash(hdr.GetBlockHash())
	if !ok {
		return status.Error(codes.InvalidArgument, "block_hash must be 32 bytes")
	}
	ver := hdr.GetVersion()
	if ver == 0 {
		// Sentinel guard: an unset version on the Replicate path is a primary-side bug, not
		// silently storable. Surfacing it here keeps the divergence-prevention invariant
		// (primary and replica share a version) executable.
		return status.Error(codes.InvalidArgument, "Replicate requires header.version > 0")
	}
	stored := s.store.PutWithVersion(h, &cache.Entry{
		ModelID:       hdr.GetModelId(),
		KV:            buf,
		TokenIDs:      hdr.GetTokenIds(),
		TenantID:      hdr.GetTenantId(),
		RecomputeCost: hdr.GetRecomputeCost(),
	}, ver)
	// Loop prevention: deliberately do NOT touch s.repl here. A replica receiving Replicate
	// stores locally and stops — re-forwarding would loop forever across N nodes.
	return stream.SendAndClose(&kvcachev1.WriteResponse{Version: stored, Stored: stored == ver})
}

// Evict removes a block and reports whether it was present.
func (s *Server) Evict(ctx context.Context, req *kvcachev1.EvictRequest) (*kvcachev1.EvictResponse, error) {
	h, ok := toBlockHash(req.GetBlockHash())
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "block_hash must be 32 bytes")
	}
	evicted := s.store.Delete(req.GetModelId(), h)
	if evicted {
		s.metrics.Eviction("manual", 1) // an explicit Evict RPC, distinct from pressure/ttl
	}
	return &kvcachev1.EvictResponse{Evicted: evicted}, nil
}

// Health is a trivial liveness/readiness probe.
func (s *Server) Health(ctx context.Context, req *kvcachev1.HealthRequest) (*kvcachev1.HealthResponse, error) {
	return &kvcachev1.HealthResponse{Ok: true}, nil
}
