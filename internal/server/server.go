// Package server implements the gRPC KVCache service over an in-memory Store.
package server

import (
	"context"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kvcachev1 "github.com/haochentSC/distributed-kv-cache/gen/kvcache/v1"
	"github.com/haochentSC/distributed-kv-cache/internal/cache"
)

// fetchChunkBytes bounds each Fetch frame well under gRPC's 4 MB message cap (ADR 0012).
const fetchChunkBytes = 1 << 20 // 1 MiB

// Server implements the generated kvcachev1.KVCacheServer. Embedding the
// Unimplemented base makes future RPC additions non-breaking.
type Server struct {
	kvcachev1.UnimplementedKVCacheServer
	store *cache.Store
}

// New wires a Server to a Store.
func New(store *cache.Store) *Server { return &Server{store: store} }

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
// Last=true. Returns NotFound on a miss or a version mismatch. The store's model check
// upholds the correctness invariant (ADR 0016); the block hash itself binds the tokens.
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

// Write client-streams one block: the FIRST message MUST be a WriteHeader, the rest
// KVChunk data frames. It assembles the bytes, stores the entry, and returns the
// assigned version (ADR 0012).
func (s *Server) Write(stream kvcachev1.KVCache_WriteServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hdr := first.GetHeader()
	if hdr == nil {
		return status.Error(codes.InvalidArgument, "first Write message must be a WriteHeader")
	}
	h, ok := toBlockHash(hdr.GetBlockHash())
	if !ok {
		return status.Error(codes.InvalidArgument, "block_hash must be 32 bytes")
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
			return err
		}
		if msg.GetHeader() != nil {
			return status.Error(codes.InvalidArgument, "unexpected second WriteHeader")
		}
		if c := msg.GetChunk(); c != nil {
			buf = append(buf, c.GetData()...)
		}
	}

	// buf is freshly built and not reused, so Put may take ownership (no copy).
	ver := s.store.Put(h, &cache.Entry{
		ModelID:       hdr.GetModelId(),
		KV:            buf,
		TokenIDs:      hdr.GetTokenIds(),
		TenantID:      hdr.GetTenantId(),
		RecomputeCost: hdr.GetRecomputeCost(),
	})
	return stream.SendAndClose(&kvcachev1.WriteResponse{Version: ver, Stored: true})
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
