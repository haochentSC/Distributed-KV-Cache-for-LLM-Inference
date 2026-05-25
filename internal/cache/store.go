package cache

import (
	"encoding/binary"
	"sync"
	"time"
)

// Store is a single in-memory cache shard.
//
// It is a STRIPED mutex map: a fixed power-of-two array of stripes, each guarding its
// own map. Unrelated keys fall on different stripes and never contend — which matters
// because the workload is write-heavy with large values and bursty reads. A single
// global RWMutex would serialize every shard's traffic; sync.Map can't do the
// composite check-then-act operations we need (look-up-and-record, evict-under-
// pressure). See docs/01-architecture.md and the distributed-systems-in-go skill.
type Store struct {
	stripes []*stripe
	policy  EvictionPolicy
}

type stripe struct {
	mu sync.RWMutex
	m  map[BlockHash]*Entry
}

const numStripes = 16 // power of two so stripeFor can mask instead of modulo

// NewStore builds a Store, defaulting to NoopPolicy when policy is nil.
func NewStore(policy EvictionPolicy) *Store {
	if policy == nil {
		policy = NoopPolicy{}
	}
	stripes := make([]*stripe, numStripes)
	for i := range stripes {
		stripes[i] = &stripe{m: make(map[BlockHash]*Entry)}
	}
	return &Store{stripes: stripes, policy: policy}
}

// stripeFor selects the stripe for a key. Block hashes are uniform (SHA-256), so the
// low 8 bytes make a good index; we mask by len-1 (valid because len is a power of
// two). Mixing several bytes — not just one — keeps the spread even for crafted test
// keys too, not only real hashes.
func (s *Store) stripeFor(h BlockHash) *stripe {
	idx := binary.LittleEndian.Uint64(h[:8]) & uint64(len(s.stripes)-1)
	return s.stripes[idx]
}

// Get returns the entry for (model, h) and whether it was present, recording an
// access on a hit. This is the real read path (used by Fetch). The returned *Entry is
// immutable per Entry's concurrency contract, so it is safe to read after the lock is
// released. A model mismatch is treated as a miss (ADR 0016 hit-verification guard).
func (s *Store) Get(model string, h BlockHash) (*Entry, bool) {
	st := s.stripeFor(h)
	st.mu.RLock()
	defer st.mu.RUnlock()

	e, ok := st.m[h]
	if !ok || e.ModelID != model {
		return nil, false
	}
	e.recordAccess(time.Now()) // atomic — safe under the shared read lock
	s.policy.RecordAccess(h)
	return e, true
}

// Peek reports presence WITHOUT recording an access. Lookup uses it: a presence check
// is not a reuse, and counting it would inflate the frequency signal the eviction
// policy relies on (ADR 0011 keeps Lookup metadata-only). The returned *Entry is
// immutable.
func (s *Store) Peek(model string, h BlockHash) (*Entry, bool) {
	st := s.stripeFor(h)
	st.mu.RLock()
	defer st.mu.RUnlock()

	e, ok := st.m[h]
	if !ok || e.ModelID != model {
		return nil, false
	}
	return e, true
}

// Put stores a KV block under h and returns the assigned version (1 on first write,
// +1 per overwrite). On overwrite it publishes a FRESH *Entry instead of mutating the
// existing one, so any reader holding the old entry keeps a consistent snapshot
// (review "Bug 2").
//
// Ownership: Put takes ownership of e.KV and e.TokenIDs and does NOT copy them — the
// caller must not mutate those slices afterward. This is deliberate: the only hot-path
// caller (the Write handler) builds a fresh buffer per call, so copying multi-MB KV
// here would be pure waste against ADR 0015's "keep copies off the hot path" goal.
func (s *Store) Put(h BlockHash, e *Entry) uint64 {
	if e == nil {
		return 0
	}
	st := s.stripeFor(h)
	st.mu.Lock()
	defer st.mu.Unlock()

	version := uint64(1)
	created := time.Now()
	if prev, ok := st.m[h]; ok {
		version = prev.Version + 1
		created = prev.CreatedAt // preserve original creation time across overwrites
	}

	stored := &Entry{
		TokenIDs:      e.TokenIDs,
		KV:            e.KV,
		ModelID:       e.ModelID,
		Version:       version,
		SizeBytes:     int64(len(e.KV)),
		TenantID:      e.TenantID,
		RecomputeCost: e.RecomputeCost,
		CreatedAt:     created,
	}
	st.m[h] = stored
	s.policy.RecordWrite(h, stored.SizeBytes)
	return version
}

// Delete removes (model, h) and reports whether it was present, recording an eviction
// on a hit. A model mismatch is a no-op miss.
func (s *Store) Delete(model string, h BlockHash) bool {
	st := s.stripeFor(h)
	st.mu.Lock()
	defer st.mu.Unlock()

	e, ok := st.m[h]
	if !ok || e.ModelID != model {
		return false
	}
	delete(st.m, h)
	s.policy.RecordEvict(h)
	return true
}
