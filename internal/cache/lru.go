package cache

import (
	"container/list"
	"sync"
)

// LRUPolicy is the Phase 4 baseline EvictionPolicy: under pressure it evicts the
// Least-Recently-USED block. "Used" means read (Get) or (over)written (Put) — both push the
// block to the front of a recency list; Victim() returns the block at the back.
//
// Why this is the BASELINE and nothing fancier: Phase 5's cost-aware + fairness engine (GDSF +
// DRF, plan §3.5) is measured AGAINST this number, so LRU must be clean and obviously-correct,
// not clever. It is intentionally size-blind and cost-blind — the Store owns the byte budget;
// the policy owns only the ordering.
//
// CONCURRENCY CONTRACT (this is the part to get right):
//   - This struct has its own mutex. Store.Get/Put call RecordAccess/RecordWrite while holding
//     a STRIPE lock, so the lock order across the program is stripe -> lru. NEVER call back into
//     the Store (or anything that takes a stripe lock) from inside these methods, or you invert
//     the order and risk deadlock.
//   - Victim() must take p.mu, read the back element, and RELEASE p.mu before returning. The
//     caller (Store.evictOne) then takes a stripe lock to actually delete — so the LRU lock is
//     never held across a stripe lock.
//   - The `-race` detector is the proof here; run it (in WSL2) before trusting this.
type LRUPolicy struct {
	mu    sync.Mutex
	ll    *list.List                  // front = most-recently-used, back = least
	items map[BlockHash]*list.Element // hash -> its node in ll, for O(1) move/remove
}

// lruItem is the value parked in each list node. We store the key so Victim() can read it off
// the back element, and RecordEvict can find-and-unlink in O(1) via the items map.
type lruItem struct{ key BlockHash }

// NewLRU builds an empty LRU policy. Wire it into a Store with cache.NewStore(NewLRU(), ...).
func NewLRU() *LRUPolicy {
	return &LRUPolicy{
		ll:    list.New(),
		items: make(map[BlockHash]*list.Element),
	}
}

// RecordAccess marks h as most-recently-used (called on every cache hit). If h isn't tracked
// yet, this is a no-op — a read can't precede the write that inserts it, but be defensive.
func (p *LRUPolicy) RecordAccess(h BlockHash) {
	// TODO(hc): lock p.mu; if e := p.items[h] exists, p.ll.MoveToFront(e).
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.items[h]; ok {
		p.ll.MoveToFront(e)
	}
}

// RecordWrite inserts h (or moves it to front on overwrite) as most-recently-used. size is
// ignored by LRU (the Store tracks bytes); it's in the interface for the cost-aware policy later.
func (p *LRUPolicy) RecordWrite(h BlockHash, size int64) {
	// TODO(hc): lock p.mu.
	//   if e := p.items[h] exists -> MoveToFront(e)
	//   else -> e := p.ll.PushFront(&lruItem{key: h}); p.items[h] = e
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.items[h]; ok {
		p.ll.MoveToFront(e)
	} else {
		e = p.ll.PushFront(&lruItem{key: h})
		p.items[h] = e
	}
}

// RecordEvict drops h from the policy's bookkeeping. Called by Store.Delete and Store.evict
// AFTER the entry is gone from the map, so the two views stay consistent. No-op if untracked.
func (p *LRUPolicy) RecordEvict(h BlockHash) {
	// TODO(hc): lock p.mu; if e := p.items[h] exists -> p.ll.Remove(e); delete(p.items, h).
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.items[h]; ok {
		p.ll.Remove(e)
		delete(p.items, h)
	}
}

// Victim returns the least-recently-used block, or ok=false if the policy is empty. It does
// NOT remove anything — the caller evicts from the Store, which calls RecordEvict to unlink.
// Take p.mu, read p.ll.Back(), and release p.mu before returning (see the concurrency contract).
func (p *LRUPolicy) Victim() (BlockHash, bool) {
	// TODO(hc): lock p.mu; e := p.ll.Back(); if e == nil return BlockHash{}, false;
	//           return e.Value.(*lruItem).key, true.
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.ll.Back()
	if e == nil {
		return BlockHash{}, false
	}
	return e.Value.(*lruItem).key, true
}
