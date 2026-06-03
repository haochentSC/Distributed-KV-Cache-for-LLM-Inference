package cache

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
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

	// Memory accounting (Phase 4). totalBytes is the sum of every stored Entry.SizeBytes
	// across ALL stripes; it is read by the Evictor without holding any stripe lock, so it
	// MUST be atomic (never a plain int64 guarded by per-stripe mutexes — no single mutex
	// covers the sum). maxBytes == 0 means UNBOUNDED — the pre-Phase-4 behaviour that
	// NoopPolicy and most unit tests rely on, so old callers keep working untouched.
	totalBytes atomic.Int64
	maxBytes   int64 // hard ceiling; 0 = unbounded
	hiWater    int64 // absolute byte level that wakes the Evictor (hiFrac * maxBytes)
	loWater    int64 // the Evictor frees down to this; hiWater-loWater is the hysteresis gap
	hiFrac     float64
	loFrac     float64
	evictSig   chan struct{} // buffered(1): a coalesced "you have work" nudge to the Evictor

	// now is the clock seam. It defaults to time.Now; tests override it so the TTL sweep and
	// access-time bookkeeping are deterministic without time.Sleep (per .claude/rules/go-testing).
	now func() time.Time
}

type stripe struct {
	mu sync.RWMutex
	m  map[BlockHash]*Entry
}

const numStripes = 16 // power of two so stripeFor can mask instead of modulo

// Default watermark fractions of maxBytes. hi wakes the Evictor; lo is what it frees down to.
// They live as consts so NewStore's struct literal and the misconfiguration fallback agree.
const (
	defaultHiFrac = 0.90
	defaultLoFrac = 0.75
)

// StoreOption configures a Store at construction (functional-options pattern). Phase 4 adds
// memory bounds; callers that pass no options get the original unbounded Store, so existing
// call sites — NewStore(NoopPolicy{}) — compile and behave exactly as before.
type StoreOption func(*Store)

// WithMaxBytes bounds the shard's resident size. When usage crosses the high-water mark the
// background Evictor frees entries down to the low-water mark; a write that would breach the
// ceiling outright is rejected by the admission guard (OverHardLimit). n <= 0 = unbounded.
func WithMaxBytes(n int64) StoreOption {
	return func(s *Store) { s.maxBytes = n }
}

// WithWatermarks overrides the high/low-water fractions of maxBytes (defaults 0.90 / 0.75).
// hi is the level that wakes the Evictor; lo is the level it frees down to. Caller must keep
// 0 < lo < hi <= 1. The absolute thresholds are computed in NewStore once maxBytes is known.
func WithWatermarks(hi, lo float64) StoreOption {
	return func(s *Store) { s.hiFrac, s.loFrac = hi, lo }
}

// NewStore builds a Store, defaulting to NoopPolicy when policy is nil. Options are applied
// after the defaults, then the absolute watermarks are derived from the final maxBytes.
func NewStore(policy EvictionPolicy, opts ...StoreOption) *Store {
	if policy == nil {
		policy = NoopPolicy{}
	}
	stripes := make([]*stripe, numStripes)
	for i := range stripes {
		stripes[i] = &stripe{m: make(map[BlockHash]*Entry)}
	}
	s := &Store{
		stripes:  stripes,
		policy:   policy,
		hiFrac:   defaultHiFrac,
		loFrac:   defaultLoFrac,
		evictSig: make(chan struct{}, 1),
		now:      time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	// Guard against operator misconfiguration — the watermark fractions come straight from CLI
	// flags and only make sense as 0 < lo < hi <= 1. A bad pair would either drain the WHOLE shard
	// (lo <= 0) or collapse the hysteresis gap (lo >= hi → evict-one/write-one thrash), so fall
	// back to the defaults rather than honor it. (Silent on purpose: NewStore has no logger and is
	// called from tests; main.go validates the same flags up front for an operator-facing warning.)
	if !(s.hiFrac > 0 && s.hiFrac <= 1 && s.loFrac > 0 && s.loFrac < s.hiFrac) {
		s.hiFrac, s.loFrac = defaultHiFrac, defaultLoFrac
	}
	if s.maxBytes > 0 {
		s.hiWater = int64(float64(s.maxBytes) * s.hiFrac)
		s.loWater = int64(float64(s.maxBytes) * s.loFrac)
	}
	return s
}

// Bytes reports the current resident size (sum of all stored Entry.SizeBytes). Lock-free.
func (s *Store) Bytes() int64 { return s.totalBytes.Load() }

// Len reports the number of stored entries across all stripes. It briefly RLocks each stripe,
// so treat it as a metrics/diagnostic call, not a hot-path one.
func (s *Store) Len() int {
	n := 0
	for _, st := range s.stripes {
		st.mu.RLock()
		n += len(st.m)
		st.mu.RUnlock()
	}
	return n
}

// LowWater returns the byte level the Evictor frees down to (0 when unbounded). The Evictor's
// drain loop reads this; exported so it lives in one place.
func (s *Store) LowWater() int64 { return s.loWater }

// EvictSignal is the channel the Evictor selects on to learn it has pressure work. Receive-only
// to callers; only signalEvict sends.
func (s *Store) EvictSignal() <-chan struct{} { return s.evictSig }

// signalEvict nudges the Evictor when usage is at/over the high-water mark. NON-BLOCKING: the
// channel is buffered(1), so if a nudge is already pending we drop this one (coalescing — the
// Evictor will drain to low-water regardless of how many writes piled up). Cheap enough to call
// while holding the stripe lock because the send never blocks.
func (s *Store) signalEvict() {
	if s.maxBytes <= 0 || s.totalBytes.Load() < s.hiWater {
		return
	}
	select {
	case s.evictSig <- struct{}{}:
	default:
	}
}

// OverHardLimit reports whether accepting addBytes more would push past the hard ceiling
// (maxBytes). Server.Write uses it to reject-fast (ADR 0017) rather than risk OOM when the
// Evictor can't keep up — a rejected write is just a future recompute (ADR 0013). Unbounded
// stores (maxBytes == 0) never reject.
func (s *Store) OverHardLimit(addBytes int64) bool {
	return s.maxBytes > 0 && s.totalBytes.Load()+addBytes > s.maxBytes
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
	e.recordAccess(s.now()) // atomic — safe under the shared read lock
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
	created := s.now()
	var prevSize int64 // bytes the overwritten entry occupied (0 on first write)
	if prev, ok := st.m[h]; ok {
		version = prev.Version + 1
		created = prev.CreatedAt // preserve original creation time across overwrites
		prevSize = prev.SizeBytes
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

	// Account for the DELTA this write moved the resident size by: on a first write prevSize is
	// 0 so we add the whole entry; on an overwrite we add only (new - prev), which is negative
	// when the new entry is smaller. atomic.Add because no single mutex covers the cross-stripe
	// sum. Update the counter BEFORE signalling — signalEvict reads it to decide if we crossed
	// the high-water mark (signal-first would read a stale, pre-write total and miss the trigger).
	s.totalBytes.Add(stored.SizeBytes - prevSize)
	s.signalEvict()
	return version
}

// PutWithVersion stores e under h at an EXPLICIT version — the replica side of replication
// (the Replicate RPC, ADR 0021). The replica must keep the version the PRIMARY assigned,
// not mint its own: a version-pinned Fetch (server.Fetch) compares versions, so if the two
// copies disagreed, a failover read could spuriously miss.
//
// How it differs from Put:
//   - Put MINTS version = prev+1; PutWithVersion is TOLD the version.
//   - Replication can arrive out of order or late. Last-writer-wins is fine for
//     eventually-consistent cache data (ADR 0013), but a STALE delivery must not clobber a
//     newer copy. So if a local entry already exists with Version >= version, KEEP it and
//     drop this delivery.
func (s *Store) PutWithVersion(h BlockHash, e *Entry, version uint64) uint64 {
	if e == nil || version == 0 {
		// version == 0 is reserved for "unset" on the wire (Write path); refuse it here so a
		// missing field doesn't quietly install an entry under a sentinel version.
		return 0
	}
	st := s.stripeFor(h)
	st.mu.Lock()
	defer st.mu.Unlock()

	created := s.now()
	var prevSize int64 // bytes the superseded entry occupied (0 if none)
	if prev, ok := st.m[h]; ok {
		if prev.Version >= version {
			// Stale or duplicate delivery — keep the newer (or equal) local copy. Last-writer-
			// wins is fine for eventually-consistent cache data (ADR 0013), but only when
			// "later" means strictly greater version; an out-of-order replay must not clobber.
			return prev.Version
		}
		created = prev.CreatedAt
		prevSize = prev.SizeBytes
	}

	stored := &Entry{
		TokenIDs:      e.TokenIDs,
		KV:            e.KV,
		ModelID:       e.ModelID,
		Version:       version, // authoritative — assigned by the primary, NOT minted here
		SizeBytes:     int64(len(e.KV)),
		TenantID:      e.TenantID,
		RecomputeCost: e.RecomputeCost,
		CreatedAt:     created,
	}
	st.m[h] = stored
	s.policy.RecordWrite(h, stored.SizeBytes)

	// Same accounting as Put — replication writes count toward the byte budget too, so a replica
	// can hit its own watermark and evict independently of the primary (fine: cache data is
	// eventually consistent, ADR 0013, and a replica miss is a recompute, never a violation).
	s.totalBytes.Add(stored.SizeBytes - prevSize)
	s.signalEvict()
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
	s.totalBytes.Add(-e.SizeBytes) // a deleted entry frees its bytes; no signalEvict — freeing
	return true                    // never creates pressure, only relieves it
}

// evict removes a block by hash REGARDLESS of model and returns the bytes it freed (0 if it
// was already gone). This is the INTERNAL memory-pressure path used only by the Evictor —
// unlike Delete (the RPC path) there is no model check, because the policy named a raw hash
// and within a shard a hash maps to exactly one entry.
//
// Lock order (read this before implementing): stripe lock FIRST, and RecordEvict (called here)
// re-enters the LRU's own lock — so the order is stripe -> lru, matching Get and Put. The
// Evictor MUST have already returned from Victim() (releasing the LRU lock) before calling
// evict; never hold the LRU lock across this call, or two goroutines take {stripe, lru} in
// opposite orders and deadlock. (See lru.go's header.)
func (s *Store) evict(h BlockHash) int64 {
	st := s.stripeFor(h)
	st.mu.Lock()
	defer st.mu.Unlock()

	e, ok := st.m[h]
	if !ok {
		return 0 // a concurrent Delete already removed it; nothing freed
	}
	delete(st.m, h)
	s.totalBytes.Add(-e.SizeBytes)
	s.policy.RecordEvict(h) // re-enters the lru lock: stripe -> lru, the program-wide order
	return e.SizeBytes
}

// evictOne asks the policy for its next victim and evicts it, returning the bytes freed and
// whether the policy HAD a victim to offer. This is the ONE place the "ask policy, then delete
// from store" sequence lives, so the stripe->lru lock order is enforced in a single spot. The
// Evictor's drain loop just calls this until it's freed enough.
//
// Contract precision (the drain loop depends on it): ok==true means the POLICY named a victim, NOT
// that bytes were necessarily freed. freed can be 0 with ok==true when the victim raced away
// between Victim() and evict() — a concurrent Delete/overwrite. That is harmless: Delete calls
// RecordEvict, so the LRU won't re-offer the same hash, and the Evictor breaks only on ok==false
// (policy empty / all pinned) while otherwise re-checking Bytes(). So a 0-freed pass never spins.
func (s *Store) evictOne() (freed int64, ok bool) {
	h, ok := s.policy.Victim() // takes + RELEASES the lru lock before returning
	if !ok {
		return 0, false // policy has nothing to give (empty / all pinned)
	}
	return s.evict(h), true // evict() takes the stripe lock — stripe -> lru order preserved
}

// sweepIdle evicts every entry whose last access (or creation, if never read) is older than
// ttl, returning how many it removed. Called by the Evictor on its TTL ticker. ttl <= 0
// disables the sweep (returns 0).
//
// Implementation note: do NOT delete while ranging a stripe's map under its own lock and also
// calling back into the policy in a way that re-locks the same stripe — collect victim hashes
// first (cheap, just [32]byte keys), release, then evict them via s.evict. Deleting from a Go
// map during range of THAT map is allowed, but going through s.evict (which re-locks the
// stripe) while already holding it is not. Two-pass keeps the lock discipline simple.
func (s *Store) sweepIdle(ttl time.Duration) int {
	if ttl <= 0 {
		return 0
	}
	cutoff := s.now().Add(-ttl)

	// Pass 1: collect victims under each stripe's RLock. We gather just the 32-byte hashes (cheap)
	// rather than evicting in place, because s.evict re-locks the stripe — calling it while we
	// already hold the same stripe lock would deadlock. Collect-then-evict sidesteps that.
	var victims []BlockHash
	for _, st := range s.stripes {
		st.mu.RLock()
		for h, e := range st.m {
			idleSince := e.LastAccessed()
			if idleSince.IsZero() {
				idleSince = e.CreatedAt // never read — age from when it was written
			}
			if idleSince.Before(cutoff) {
				victims = append(victims, h)
			}
		}
		st.mu.RUnlock()
	}

	// Pass 2: evict each collected hash. s.evict takes the stripe lock itself and tolerates a
	// hash that raced away (returns 0) — count only those we actually removed.
	n := 0
	for _, h := range victims {
		if s.evict(h) > 0 {
			n++
		}
	}
	return n
}
