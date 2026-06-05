// Package coldtier is the cold storage tier (S3) the hot in-memory cache demotes evicted
// blocks to and reads back through on a miss — the GPU→RAM→S3 tiering of plan §5 (ADR 0027).
//
// It is a LEAF package: it owns the AWS SDK and the on-the-wire framing, so the cache/server
// core stays cloud-free and links the SDK only through this package. The cache hooks it via a
// plain callback (cache.SpillFunc) and the server via a narrow interface (server's coldReader);
// neither imports this package, so both still build and test without AWS.
//
// Correctness (ADR 0016): every block is keyed by (model, block_hash), so a Get can only ever
// return the bytes that were Spilled under that exact key — a cold hit can never be a wrong
// block. A failed/dropped Spill or a storage error on Get degrades to a cache MISS (a future
// recompute, ADR 0013), never to wrong bytes.
package coldtier

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
)

// Tier is a lower storage tier for evicted KV blocks. Implementations must be safe for
// concurrent use.
type Tier interface {
	// Spill asynchronously copies an evicted block to cold storage. It MUST NOT block — it is
	// called on the eviction path while a stripe lock is held — so it enqueues and returns.
	// Best-effort: a dropped or failed spill is just a future recompute (ADR 0013).
	// kv and tokenIDs must not be mutated by the caller after the call (the cache passes
	// immutable Entry snapshots, so this holds).
	Spill(model string, hash [32]byte, version uint64, tokenIDs []int32, kv []byte)

	// Get synchronously fetches a block. ok=false is a clean miss; a non-nil err is a
	// storage/transport failure that the caller also serves as a miss (never as wrong bytes).
	Get(ctx context.Context, model string, hash [32]byte) (kv []byte, version uint64, tokenIDs []int32, ok bool, err error)

	// Close stops background workers, draining queued spills best-effort.
	Close() error
}

// objectKey is the cold-storage key for a block: a fixed prefix, the model, and the hex hash.
// Hashing keeps keys uniform; the model segment keeps two models' identical-prefix blocks apart
// (they already hash differently per ADR 0011, but the path keeps it obvious in the bucket).
func objectKey(model string, hash [32]byte) string {
	return "blocks/" + model + "/" + hex.EncodeToString(hash[:])
}

// --- framing -------------------------------------------------------------------------------
//
// A cold object is self-describing so a Get can reconstruct a SERVABLE entry (the server
// re-verifies version + token_ids on read-through, ADR 0016). Layout, little-endian:
//
//	[4: magic "KVC1"][8: version][4: nTokens][4*nTokens: token ids][rest: kv]

const magic = "KVC1"
const headerMin = 4 + 8 + 4 // magic + version + nTokens

// encode frames an entry into one cold-storage blob. It copies kv (the blob is a fresh buffer),
// which is fine because encode runs off the hot path — in the S3 worker, not under the stripe lock.
func encode(version uint64, tokenIDs []int32, kv []byte) []byte {
	blob := make([]byte, headerMin+4*len(tokenIDs)+len(kv))
	copy(blob[0:4], magic)
	binary.LittleEndian.PutUint64(blob[4:12], version)
	binary.LittleEndian.PutUint32(blob[12:16], uint32(len(tokenIDs)))
	off := 16
	for _, t := range tokenIDs {
		binary.LittleEndian.PutUint32(blob[off:off+4], uint32(t))
		off += 4
	}
	copy(blob[off:], kv)
	return blob
}

// decode parses a blob produced by encode. It validates the magic and lengths so a truncated or
// foreign object surfaces as an error (served as a miss), not as bogus tokens/bytes.
func decode(blob []byte) (version uint64, tokenIDs []int32, kv []byte, err error) {
	if len(blob) < headerMin || string(blob[0:4]) != magic {
		return 0, nil, nil, errors.New("coldtier: bad object header")
	}
	version = binary.LittleEndian.Uint64(blob[4:12])
	n := int(binary.LittleEndian.Uint32(blob[12:16]))
	if n < 0 || 16+4*n > len(blob) {
		return 0, nil, nil, fmt.Errorf("coldtier: token count %d overruns object of %d bytes", n, len(blob))
	}
	tokenIDs = make([]int32, n)
	off := 16
	for i := 0; i < n; i++ {
		tokenIDs[i] = int32(binary.LittleEndian.Uint32(blob[off : off+4]))
		off += 4
	}
	kv = append([]byte(nil), blob[off:]...) // copy out so it doesn't pin the whole blob
	return version, tokenIDs, kv, nil
}

// --- Memory: an in-process Tier for tests and local dev (no AWS) ---------------------------

// Memory is a Tier backed by an in-memory map. It exists for unit tests and local runs without
// S3. Spill is synchronous here (no worker pool) but still satisfies the non-blocking contract
// for the small payloads tests use.
type Memory struct {
	mu sync.Mutex
	m  map[string][]byte // objectKey -> framed blob
}

// NewMemory returns an empty in-memory Tier.
func NewMemory() *Memory { return &Memory{m: make(map[string][]byte)} }

func (t *Memory) Spill(model string, hash [32]byte, version uint64, tokenIDs []int32, kv []byte) {
	blob := encode(version, tokenIDs, kv)
	t.mu.Lock()
	t.m[objectKey(model, hash)] = blob
	t.mu.Unlock()
}

func (t *Memory) Get(_ context.Context, model string, hash [32]byte) ([]byte, uint64, []int32, bool, error) {
	t.mu.Lock()
	blob, ok := t.m[objectKey(model, hash)]
	t.mu.Unlock()
	if !ok {
		return nil, 0, nil, false, nil
	}
	version, tokenIDs, kv, err := decode(blob)
	if err != nil {
		return nil, 0, nil, false, err
	}
	return kv, version, tokenIDs, true, nil
}

func (t *Memory) Close() error { return nil }
