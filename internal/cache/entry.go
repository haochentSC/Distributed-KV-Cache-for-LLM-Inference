// Package cache implements a single in-memory KV-cache shard.
package cache

import (
	"sync/atomic"
	"time"
)

// BlockHash is the opaque 32-byte key for a KV block. It is a chained SHA-256
// computed client-side (ADR 0011); the server treats it as opaque (ADR 0010).
type BlockHash [32]byte

// Entry is a stored KV block plus the metadata the eviction policies and
// hit-verification need.
//
// Concurrency contract (see store.go and the review of HC's first cut):
//   - The VALUE fields below are immutable once an Entry is published into the Store.
//     Put never mutates a live Entry in place — on overwrite it swaps in a fresh one.
//     That lets Get hand back the *Entry under a read lock, and lets Fetch stream
//     KV without copying, with no risk of a concurrent overwrite changing the bytes
//     mid-read (review "Bug 2").
//   - The only mutable state is the access bookkeeping (accessCount/lastAccessed),
//     updated on every Get — which holds only a READ lock. Concurrent readers may
//     therefore update it at once, so it must be atomic, not plain fields
//     (review "Bug 1": mutating a plain field under RLock is a data race).
type Entry struct {
	TokenIDs      []int32   // for hit verification / hash-collision guard
	KV            []byte    // serialized K and V for all layers/heads of this block
	ModelID       string    // KV cache is model-specific
	WireHash      BlockHash // the client's block hash; the map key namespaces it by model (storeKey)
	Version       uint64    // bumped on overwrite
	SizeBytes     int64     // len(KV); cached for accounting
	TenantID      string    // Phase 5 (fairness)
	RecomputeCost float64   // Phase 5 (cost-aware eviction)
	CreatedAt     time.Time

	accessCount  atomic.Uint64 // Phase 5 frequency signal; atomic (updated under RLock)
	lastAccessed atomic.Int64  // unix nanos; 0 = never read
}

// AccessCount returns how many times this entry has been read via Store.Get.
func (e *Entry) AccessCount() uint64 { return e.accessCount.Load() }

// LastAccessed returns the last time this entry was read via Store.Get (zero if never).
func (e *Entry) LastAccessed() time.Time {
	ns := e.lastAccessed.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// recordAccess bumps the access stats. Safe to call while holding only a read lock.
func (e *Entry) recordAccess(now time.Time) {
	e.accessCount.Add(1)
	e.lastAccessed.Store(now.UnixNano())
}
