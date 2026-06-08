package cache

import (
	"container/heap"
	"sync"
)

// GDSFPolicy is the Phase 5a headline eviction policy: cost-aware GreedyDual-Size-Frequency value
// plus optional static per-tenant byte quotas (plan §3.5, ADR 0007). It replaces LRU behind the
// same EvictionPolicy seam — swapped in one line at NewStore, nothing else changes.
//
// VALUE FUNCTION (why this beats LRU/LFU here):
//
//		H(block) = L + freq · cost / size
//
//	  - cost  = recompute cost (∝ prefill FLOPs ≈ prefix length). A long, expensive-to-prefill block
//	    is worth keeping even if it's read less often, because MISSING it costs more GPU. LRU ignores
//	    this entirely.
//	  - size  = resident bytes. Cost is valued PER BYTE, so a huge block must earn its footprint —
//	    the dimension that matters most for multi-MB KV tensors.
//	  - freq  = access count. The frequency signal (the "F" in GDSF).
//	  - L     = an aging offset (the classic GreedyDual trick). On eviction we set L to the victim's
//	    priority, so every surviving/new block is measured against the CURRENT floor. This is what
//	    lets a once-valuable-but-now-cold block age out — LFU never forgets, GDSF does. L is
//	    monotonically non-decreasing, so it also tie-breaks toward older inflation epochs.
//
// We evict the LOWEST H under pressure.
//
// FAIRNESS (Phase 5a, static): each tenant has an optional byte quota. A tenant OVER its quota is
// reclaimed first — Victim() returns the lowest-H block among over-quota tenants before touching
// anyone under their floor. With no quotas configured this degenerates to pure global cost-aware
// GDSF (the efficiency baseline for the 5b knob). Quotas here are STATIC: a tenant may transiently
// borrow idle capacity but is capped under contention. Elastic borrowing + reclaim-fairness + the
// fairness_weight knob is Phase 5b (deliberately NOT built here — see ADR 0007 scoping).
//
// CONCURRENCY CONTRACT (same shape as LRUPolicy — read lru.go's header):
//   - One mutex guards everything. Record*/OverQuota are called under a STRIPE lock (Put path) or
//     with no lock (Evictor loop); they take ONLY p.mu, never a stripe lock, so the program-wide
//     order stays stripe -> policy.
//   - Victim() takes p.mu and RELEASES it before returning; the caller (Store.evictOne) then takes
//     a stripe lock to delete. The policy lock is never held across a stripe lock.
//   - The `-race` detector (WSL2) is the proof; run it before trusting this.
type GDSFPolicy struct {
	mu sync.Mutex

	items     map[BlockHash]*gdsfItem // every tracked block, for O(1) access/evict lookup
	tenants   map[string]*tenantState // per-tenant heap + byte accounting, created lazily
	quotas    map[string]int64        // configured per-tenant byte budget; >0 enables it
	inflation float64                 // L: the GreedyDual aging floor, monotonically non-decreasing

	// Phase 5b: elastic (work-conserving) fairness. When elastic, the quotas are FLOORS, not caps:
	// OverQuota() always reports false (no independent cap trigger — eviction is watermark-only, so
	// a tenant freely borrows idle capacity), and Victim() biases victim selection toward tenants
	// ABOVE their floor by the fairnessWeight knob. elastic=false keeps the 5a static-cap behaviour.
	elastic        bool
	fairnessWeight float64 // w in [0,1]: 0 = pure GDSF efficiency, 1 = strong max-min fairness bias
}

// maxOverage is the overage assigned to a tenant with no configured floor in elastic mode: it has
// no fairness guarantee, so its resident blocks are reclaimed before any floored tenant's. Large,
// but finite (a real number keeps Victim()'s arithmetic total-order well-defined).
const maxOverage = 1e9

// gdsfItem is one tracked block. priority is cached so the heap can order without recomputing;
// it is refreshed on every write/access. index is maintained by container/heap (see Swap) so an
// in-place access can heap.Fix in O(log n) without a linear search.
type gdsfItem struct {
	key      BlockHash
	tenant   string
	size     int64
	cost     float64
	freq     uint64
	priority float64
	index    int // position in its tenant's heap; -1 once removed
}

// tenantState is one tenant's slice of the cache: its own min-heap (by priority) and a running
// byte total checked against quota. Keeping a per-tenant heap (rather than one global heap) makes
// "lowest-H block among over-quota tenants" a peek of each over-quota tenant's root — O(#tenants),
// which is tiny (a handful of configured tenants).
type tenantState struct {
	bytes int64
	quota int64 // 0 = unlimited (never "over quota")
	h     gdsfHeap
}

// NewGDSF builds the Phase 5a STATIC-quota GDSF policy. quotas maps tenant_id -> byte quota; a
// tenant absent from the map (or mapped to 0) is unlimited. Pass nil for pure cost-aware GDSF with
// no fairness floor. Here a quota is a hard CAP: an over-quota tenant is reclaimed even with free
// global capacity (OverQuota() is an independent eviction trigger).
func NewGDSF(quotas map[string]int64) *GDSFPolicy {
	return newGDSF(quotas, false, 0)
}

// NewGDSFElastic builds the Phase 5b WORK-CONSERVING GDSF policy. The quotas become FLOORS, not
// caps: a tenant borrows idle capacity freely (OverQuota() is always false, so eviction is
// watermark-only), and under genuine global pressure Victim() steers eviction away from tenants
// below their floor. fairnessWeight w in [0,1] interpolates the bias: 0 = pure global GDSF
// (efficiency), 1 = strong max-min fairness. Out-of-range w is clamped.
func NewGDSFElastic(quotas map[string]int64, fairnessWeight float64) *GDSFPolicy {
	switch {
	case fairnessWeight < 0:
		fairnessWeight = 0
	case fairnessWeight > 1:
		fairnessWeight = 1
	}
	return newGDSF(quotas, true, fairnessWeight)
}

func newGDSF(quotas map[string]int64, elastic bool, fairnessWeight float64) *GDSFPolicy {
	q := make(map[string]int64, len(quotas))
	for k, v := range quotas {
		q[k] = v
	}
	return &GDSFPolicy{
		items:          make(map[BlockHash]*gdsfItem),
		tenants:        make(map[string]*tenantState),
		quotas:         q,
		elastic:        elastic,
		fairnessWeight: fairnessWeight,
	}
}

// priorityOf computes H = L + freq·cost/size, defending against the degenerate inputs a caller
// might not set: cost<=0 falls back to 1 (so an un-costed block still ages by frequency/size
// rather than pinning at L forever), and size<=0 to 1 (no divide-by-zero; a zero-byte block is
// valued purely by freq·cost).
func (p *GDSFPolicy) priorityOf(freq uint64, cost float64, size int64) float64 {
	if cost <= 0 {
		cost = 1
	}
	if size <= 0 {
		size = 1
	}
	return p.inflation + float64(freq)*cost/float64(size)
}

// tenantFor returns the tenant's state, creating it (with its configured quota) on first use.
func (p *GDSFPolicy) tenantFor(id string) *tenantState {
	ts, ok := p.tenants[id]
	if !ok {
		ts = &tenantState{quota: p.quotas[id]}
		p.tenants[id] = ts
	}
	return ts
}

// RecordWrite inserts a new block or updates an existing one (overwrite). On overwrite it adjusts
// the tenant's byte total by the size delta and re-heaps; a (rare) tenant change moves the item
// between heaps. freq is preserved across an overwrite — re-storing a block is not a fresh start.
func (p *GDSFPolicy) RecordWrite(meta WriteMeta) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if it, ok := p.items[meta.Key]; ok {
		// Overwrite. If the tenant changed (defensive — same key normally keeps its tenant), move
		// the item to the new tenant's heap; otherwise adjust in place.
		if it.tenant != meta.TenantID {
			p.removeItem(it)
		} else {
			ts := p.tenants[it.tenant]
			ts.bytes += meta.Size - it.size
			it.size, it.cost = meta.Size, meta.Cost
			it.priority = p.priorityOf(it.freq, it.cost, it.size)
			heap.Fix(&ts.h, it.index)
			return
		}
	}

	ts := p.tenantFor(meta.TenantID)
	it := &gdsfItem{
		key:      meta.Key,
		tenant:   meta.TenantID,
		size:     meta.Size,
		cost:     meta.Cost,
		freq:     1, // the write itself counts as one touch (mirrors LRU treating a write as a use)
		priority: p.priorityOf(1, meta.Cost, meta.Size),
	}
	heap.Push(&ts.h, it)
	p.items[meta.Key] = it
	ts.bytes += meta.Size
}

// RecordAccess bumps a block's frequency and re-prioritises it (a cache hit raises its value).
// No-op for an untracked key.
func (p *GDSFPolicy) RecordAccess(h BlockHash) {
	p.mu.Lock()
	defer p.mu.Unlock()

	it, ok := p.items[h]
	if !ok {
		return
	}
	it.freq++
	it.priority = p.priorityOf(it.freq, it.cost, it.size)
	heap.Fix(&p.tenants[it.tenant].h, it.index)
}

// RecordEvict drops a block from the policy's bookkeeping after the Store removed it. No-op for an
// untracked key (it may have raced away via Delete). Frees the tenant's bytes.
func (p *GDSFPolicy) RecordEvict(h BlockHash) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if it, ok := p.items[h]; ok {
		p.removeItem(it)
	}
}

// removeItem unlinks an item from its tenant heap, the items index, and the tenant byte total.
// Caller holds p.mu.
func (p *GDSFPolicy) removeItem(it *gdsfItem) {
	ts := p.tenants[it.tenant]
	if it.index >= 0 && it.index < ts.h.Len() {
		heap.Remove(&ts.h, it.index)
	}
	ts.bytes -= it.size
	delete(p.items, it.key)
}

// OverQuota reports whether any tenant currently exceeds its configured quota. This is the extra
// eviction trigger that makes static fairness bite: an over-quota tenant is reclaimed even when
// the global watermark is satisfied. O(#tenants).
func (p *GDSFPolicy) OverQuota() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.elastic {
		// Work-conserving: floors are reclaimed only under the global watermark, never as an
		// independent cap, so a tenant may hold borrowed capacity until something else needs it.
		// Fairness is expressed at victim-selection time (effPriority), not here.
		return false
	}
	for _, ts := range p.tenants {
		if ts.quota > 0 && ts.bytes > ts.quota {
			return true
		}
	}
	return false
}

// Victim returns the next block to evict and advances the aging floor L. Selection order:
//  1. If any tenant is over its quota, evict the lowest-H block AMONG over-quota tenants (enforce
//     the static cap — these tenants must shrink first).
//  2. Otherwise (pure global pressure) evict the globally lowest-H block.
//
// It does NOT remove the item — the caller evicts via the Store, which calls RecordEvict. L is set
// to the chosen block's priority (the GreedyDual aging step): future blocks start from this floor.
//
// Elastic (Phase 5b) routes to victimElastic; static (Phase 5a) keeps the over-quota-first rule.
func (p *GDSFPolicy) Victim() (BlockHash, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.elastic {
		return p.victimElastic()
	}
	return p.victimStatic()
}

// victimElastic picks the block minimising the fairness-adjusted priority
//
//	H_eff = H / (1 + w · overage),   overage = max(0, bytes/floor − 1)
//
// A tenant at or below its floor (overage 0) keeps its true H — protected; a tenant above its floor
// is discounted toward 0 — evicted first. The discount is a per-tenant scalar, so it preserves each
// tenant's intra-heap order: the lowest-H_eff block of a tenant is still its heap root. So this is
// the same O(#tenants) root-peek as the static path, just with a fairness-weighted comparison.
//
// The aging floor L advances by the victim's TRUE priority (not the discounted score): the discount
// only steers WHICH tenant pays, it must not corrupt the global GreedyDual value scale. Caller holds p.mu.
func (p *GDSFPolicy) victimElastic() (BlockHash, bool) {
	var best *gdsfItem
	var bestScore float64
	for _, ts := range p.tenants {
		if ts.h.Len() == 0 {
			continue
		}
		root := ts.h[0]
		score := root.priority / (1 + p.fairnessWeight*p.overage(ts))
		if best == nil || score < bestScore {
			best, bestScore = root, score
		}
	}
	if best == nil {
		return BlockHash{}, false
	}
	if best.priority > p.inflation {
		p.inflation = best.priority
	}
	return best.key, true
}

// overage reports how far a tenant is over its floor as a fraction: bytes/floor − 1, clamped at 0
// (a tenant at/below its floor has 0 overage). A tenant with no configured floor in elastic mode
// has no fairness guarantee, so any residency is treated as maximal overage (reclaimed first).
// Caller holds p.mu.
func (p *GDSFPolicy) overage(ts *tenantState) float64 {
	if ts.quota <= 0 {
		if ts.bytes > 0 {
			return maxOverage
		}
		return 0
	}
	o := float64(ts.bytes)/float64(ts.quota) - 1
	if o < 0 {
		return 0
	}
	return o
}

// victimStatic is the Phase 5a rule: over-quota tenants are reclaimed first (the hard cap), else the
// globally lowest-H block. Caller holds p.mu.
func (p *GDSFPolicy) victimStatic() (BlockHash, bool) {
	var best *gdsfItem
	overQuota := false
	for _, ts := range p.tenants {
		if ts.h.Len() == 0 {
			continue
		}
		root := ts.h[0] // this tenant's lowest-priority block
		tenantOver := ts.quota > 0 && ts.bytes > ts.quota

		switch {
		case tenantOver && !overQuota:
			// First over-quota candidate seen — it outranks any global candidate gathered so far.
			best, overQuota = root, true
		case tenantOver == overQuota:
			// Same class (both over-quota, or both global while none over-quota yet): take the
			// lower priority.
			if best == nil || root.priority < best.priority {
				best = root
			}
		}
		// (tenantOver==false while overQuota==true: a non-over-quota tenant is ignored — we only
		// evict from over-quota tenants once one exists.)
	}

	if best == nil {
		return BlockHash{}, false
	}
	if best.priority > p.inflation {
		p.inflation = best.priority // aging floor only ever rises
	}
	return best.key, true
}

// --- gdsfHeap: a min-heap of *gdsfItem ordered by priority, with index tracking -------------
// container/heap gives us O(log n) push/pop/fix/remove. Swap maintains each item's index field so
// heap.Fix/heap.Remove can locate an item in O(1) instead of scanning.

type gdsfHeap []*gdsfItem

func (h gdsfHeap) Len() int           { return len(h) }
func (h gdsfHeap) Less(i, j int) bool { return h[i].priority < h[j].priority }
func (h gdsfHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *gdsfHeap) Push(x any) {
	it := x.(*gdsfItem)
	it.index = len(*h)
	*h = append(*h, it)
}
func (h *gdsfHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil // avoid leaking the pointer
	it.index = -1
	*h = old[:n-1]
	return it
}
