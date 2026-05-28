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

// Server implements the generated kvcachev1.KVCacheServer. Embedding the
// Unimplemented base makes future RPC additions non-breaking.
type Server struct {
	kvcachev1.UnimplementedKVCacheServer
	store *cache.Store
	repl  replicaEnqueuer // nil on a single node (no replication); set in Phase 3 Sub-stage B
}

// New wires a Server to a Store with NO replication (single-node / tests).
func New(store *cache.Store) *Server { return &Server{store: store} }

// NewWithReplicator wires a Server that, after each successful Write (it is the PRIMARY for
// that block), hands the block to repl for async forwarding to the replica (ADR 0021).
func NewWithReplicator(store *cache.Store, repl replicaEnqueuer) *Server {
	return &Server{store: store, repl: repl}
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
			}
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
		return status.Error(codes.NotFound, "block not cached")
	}
	if v := req.GetVersion(); v != 0 && v != e.Version {
		return status.Error(codes.NotFound, "requested version not available")
	}
	if toks := req.GetTokenIds(); len(toks) > 0 && !slices.Equal(toks, e.TokenIDs) {
		return status.Error(codes.NotFound, "token_ids do not match cached block")
	}

	data := e.KV // immutable snapshot — safe to stream without holding a lock
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
	return &kvcachev1.EvictResponse{Evicted: s.store.Delete(req.GetModelId(), h)}, nil
}

// Health is a trivial liveness/readiness probe.
func (s *Server) Health(ctx context.Context, req *kvcachev1.HealthRequest) (*kvcachev1.HealthResponse, error) {
	return &kvcachev1.HealthResponse{Ok: true}, nil
}
