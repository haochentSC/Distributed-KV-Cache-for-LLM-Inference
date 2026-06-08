package cache

import "testing"

// putTenant stores a block of the given size/tenant/cost and returns its key. It drives the real
// Store.Put path (so the policy sees the same WriteMeta production does), keying by a single byte.
func putTenant(s *Store, key byte, tenant string, size int, cost float64) BlockHash {
	h := hashByte(key)
	s.Put(h, &Entry{
		KV:            make([]byte, size),
		ModelID:       "m",
		TenantID:      tenant,
		RecomputeCost: cost,
	})
	return h
}

// TestGDSF_EvictsLowestValue is the cost-aware core: GDSF evicts the lowest value/byte block, NOT
// the least-recently-used one. We write the EXPENSIVE block first and the cheap one second, so LRU
// would evict the expensive (older) block — GDSF must instead drop the cheap one.
func TestGDSF_EvictsLowestValue(t *testing.T) {
	p := NewGDSF(nil)
	expensive, cheap := hashByte(1), hashByte(2)

	// Same size; cost differs 100x. H = freq·cost/size, so expensive has the higher priority.
	p.RecordWrite(WriteMeta{Key: expensive, Size: 100, Cost: 100}) // H = 1·100/100 = 1.0
	p.RecordWrite(WriteMeta{Key: cheap, Size: 100, Cost: 1})       // H = 1·1/100  = 0.01

	if got, ok := p.Victim(); !ok || got != cheap {
		t.Fatalf("Victim = %x ok=%v, want cheap %x (lowest value/byte, recency-blind)", got, ok, cheap)
	}
}

// TestGDSF_SizePenalisesFootprint: equal cost, a bigger block is worth LESS per byte, so it is the
// victim. This is the dimension LRU/LFU ignore that matters most for multi-MB KV tensors.
func TestGDSF_SizePenalisesFootprint(t *testing.T) {
	p := NewGDSF(nil)
	small, big := hashByte(1), hashByte(2)

	p.RecordWrite(WriteMeta{Key: small, Size: 10, Cost: 10}) // H = 1.0
	p.RecordWrite(WriteMeta{Key: big, Size: 1000, Cost: 10}) // H = 0.01

	if got, ok := p.Victim(); !ok || got != big {
		t.Fatalf("Victim = %x, want big %x (lower value per byte)", got, big)
	}
}

// TestGDSF_FrequencyRescues: a cheap block read enough times outvalues a pricier one. Access bumps
// freq, which scales the numerator, so a hot cheap block survives.
func TestGDSF_FrequencyRescues(t *testing.T) {
	p := NewGDSF(nil)
	hot, cold := hashByte(1), hashByte(2)

	p.RecordWrite(WriteMeta{Key: hot, Size: 100, Cost: 1})  // H = 0.01
	p.RecordWrite(WriteMeta{Key: cold, Size: 100, Cost: 5}) // H = 0.05  (currently higher)

	// Read `hot` 10x: H = (1+10)·1/100 = 0.11, now above cold's 0.05.
	for i := 0; i < 10; i++ {
		p.RecordAccess(hot)
	}
	if got, ok := p.Victim(); !ok || got != cold {
		t.Fatalf("Victim = %x, want cold %x (hot block rescued by frequency)", got, cold)
	}
}

// TestGDSF_AgingFloorRises: evicting a victim advances L to that victim's priority (the GreedyDual
// trick), and L only ever rises. White-box check on p.inflation (same package).
func TestGDSF_AgingFloorRises(t *testing.T) {
	p := NewGDSF(nil)
	p.RecordWrite(WriteMeta{Key: hashByte(1), Size: 100, Cost: 30}) // H = 0.30
	p.RecordWrite(WriteMeta{Key: hashByte(2), Size: 100, Cost: 50}) // H = 0.50

	got, ok := p.Victim() // lowest is key 1 at 0.30
	if !ok || got != hashByte(1) {
		t.Fatalf("Victim = %x ok=%v, want key 1", got, ok)
	}
	if p.inflation != 0.30 {
		t.Fatalf("inflation (L) = %v, want 0.30 (advanced to victim priority)", p.inflation)
	}
	// A fresh write now starts from the raised floor: H = L + freq·cost/size = 0.30 + 0.01.
	p.RecordWrite(WriteMeta{Key: hashByte(3), Size: 100, Cost: 1})
	if p.items[hashByte(3)].priority != 0.31 {
		t.Fatalf("new block priority = %v, want 0.31 (L + 0.01)", p.items[hashByte(3)].priority)
	}
}

// TestGDSF_QuotaTargetsOverTenant is the static-fairness core: when tenant A is over its quota, A's
// block is the victim even though tenant B owns a strictly LOWER-value block. The quota floor wins
// over raw global value — that is what stops B (cheap/idle) from being starved by greedy A.
func TestGDSF_QuotaTargetsOverTenant(t *testing.T) {
	p := NewGDSF(map[string]int64{"A": 150}) // A capped at 150 bytes; B unlimited

	// A holds 200 bytes (> 150 quota) of relatively HIGH-value blocks.
	p.RecordWrite(WriteMeta{Key: hashByte(1), TenantID: "A", Size: 100, Cost: 100}) // H = 1.0
	p.RecordWrite(WriteMeta{Key: hashByte(2), TenantID: "A", Size: 100, Cost: 100}) // H = 1.0
	// B holds one very LOW-value block — the global lowest.
	bCheap := hashByte(3)
	p.RecordWrite(WriteMeta{Key: bCheap, TenantID: "B", Size: 100, Cost: 1}) // H = 0.01

	if !p.OverQuota() {
		t.Fatal("OverQuota = false, want true (A is 200 > 150)")
	}
	got, ok := p.Victim()
	if !ok {
		t.Fatal("Victim ok=false, want a victim")
	}
	if got == bCheap {
		t.Fatalf("Victim = B's cheap block %x, want one of A's blocks (A is over quota)", got)
	}
	if it := p.items[got]; it.tenant != "A" {
		t.Fatalf("Victim tenant = %q, want A (the over-quota tenant)", it.tenant)
	}
}

// TestGDSF_NoQuotaPicksGlobalLowest: with no tenant over quota, Victim falls back to the globally
// lowest-value block regardless of tenant.
func TestGDSF_NoQuotaPicksGlobalLowest(t *testing.T) {
	p := NewGDSF(nil)
	p.RecordWrite(WriteMeta{Key: hashByte(1), TenantID: "A", Size: 100, Cost: 100})
	lowest := hashByte(2)
	p.RecordWrite(WriteMeta{Key: lowest, TenantID: "B", Size: 100, Cost: 1})

	if p.OverQuota() {
		t.Fatal("OverQuota = true, want false (no quotas configured)")
	}
	if got, ok := p.Victim(); !ok || got != lowest {
		t.Fatalf("Victim = %x, want global lowest %x", got, lowest)
	}
}

// TestStore_GDSFQuotaDrain drives the real Store + Evictor stop-condition: an UNBOUNDED store
// (maxBytes=0) with a per-tenant quota must still reclaim an over-quota tenant down to its floor,
// and leave a within-quota tenant untouched. We drive evictOne manually (no background goroutine)
// so the test is deterministic, asserting the invariant: A ends at/under quota, B is intact.
func TestStore_GDSFQuotaDrain(t *testing.T) {
	pol := NewGDSF(map[string]int64{"A": 250})
	s := NewStore(pol) // unbounded global; only the quota drives eviction

	for i := byte(1); i <= 4; i++ {
		putTenant(s, i, "A", 100, 10) // A: 4×100 = 400 bytes, over its 250 quota
	}
	bKey := putTenant(s, 100, "B", 100, 10) // B: 100 bytes, no quota — must survive

	if !s.needsEviction() {
		t.Fatal("needsEviction = false, want true (A over quota)")
	}
	for s.needsEviction() {
		if _, ok := s.evictOne(); !ok {
			t.Fatal("evictOne ran dry while still needsEviction")
		}
	}

	if pol.tenants["A"].bytes > 250 {
		t.Fatalf("A resident = %d bytes, want <= 250 (quota enforced)", pol.tenants["A"].bytes)
	}
	if _, ok := s.Get("m", bKey); !ok {
		t.Fatal("B's block was evicted, want it kept (B is within quota)")
	}
}

// --- Phase 5b: elastic (work-conserving) floors + the fairness_weight knob ------------------

// TestGDSFElastic_WorkConservingNeverCaps is the work-conserving core: in elastic mode a tenant
// may sit FAR over its floor and OverQuota stays false — so the Store never evicts on quota alone
// (eviction is watermark-only). Contrast the static policy, which would report over-quota here.
func TestGDSFElastic_WorkConservingNeverCaps(t *testing.T) {
	p := NewGDSFElastic(map[string]int64{"A": 100}, 1.0)
	p.RecordWrite(WriteMeta{Key: hashByte(1), TenantID: "A", Size: 500, Cost: 10}) // 500 >> 100 floor

	if p.OverQuota() {
		t.Fatal("OverQuota = true in elastic mode, want false (floors are not caps — work-conserving)")
	}
	// Sanity: the same residency under the STATIC policy IS over quota — proves the mode is the
	// only difference.
	if !NewGDSFStaticWith(map[string]int64{"A": 100}, 500).OverQuota() {
		t.Fatal("static policy should report over-quota for the same residency")
	}
}

// NewGDSFStaticWith builds a static-cap policy already holding `bytes` for tenant A — a tiny helper
// to contrast the two modes' OverQuota in one place.
func NewGDSFStaticWith(quotas map[string]int64, bytes int) *GDSFPolicy {
	p := NewGDSF(quotas)
	p.RecordWrite(WriteMeta{Key: hashByte(1), TenantID: "A", Size: int64(bytes), Cost: 10})
	return p
}

// TestGDSFElastic_WeightZeroIsGlobalGDSF: at w=0 the fairness discount vanishes, so victim
// selection is exactly pure global GDSF — the lowest-H block wins regardless of floors. This is
// the efficiency endpoint of the knob (must reproduce the no-quota GDSF behaviour).
func TestGDSFElastic_WeightZeroIsGlobalGDSF(t *testing.T) {
	// A is way over its floor but owns the HIGH-value block; B is under floor with the low-value one.
	p := NewGDSFElastic(map[string]int64{"A": 50, "B": 1000}, 0)
	p.RecordWrite(WriteMeta{Key: hashByte(1), TenantID: "A", Size: 100, Cost: 100}) // H = 1.0
	lowest := hashByte(2)
	p.RecordWrite(WriteMeta{Key: lowest, TenantID: "B", Size: 100, Cost: 1}) // H = 0.01

	if got, ok := p.Victim(); !ok || got != lowest {
		t.Fatalf("Victim = %x at w=0, want global lowest-H %x (no fairness bias)", got, lowest)
	}
}

// TestGDSFElastic_WeightFlipsVictimToOverShare is the knob's headline: with the SAME blocks as
// above but w=1, the fairness discount makes over-floor A's high-value block the victim instead of
// under-floor B's low-value one. The block that survives changes purely because the knob turned.
func TestGDSFElastic_WeightFlipsVictimToOverShare(t *testing.T) {
	aBlock, bBlock := hashByte(1), hashByte(2)

	// A sits far over its floor (100 bytes / 25 floor -> overage 3); B is under its floor.
	// True priorities: A's block H = 0.10 (the HIGHER), B's H = 0.05 (the lower).
	// At w=1: A's H_eff = 0.10/(1+1·3) = 0.025; B's H_eff = 0.05/(1+0) = 0.05 -> A is the victim.
	p := NewGDSFElastic(map[string]int64{"A": 25, "B": 1000}, 1.0)
	p.RecordWrite(WriteMeta{Key: aBlock, TenantID: "A", Size: 100, Cost: 10}) // true H = 0.10
	p.RecordWrite(WriteMeta{Key: bBlock, TenantID: "B", Size: 100, Cost: 5})  // true H = 0.05
	if got, ok := p.Victim(); !ok || got != aBlock {
		t.Fatalf("Victim = %x at w=1, want over-floor A's block %x (fairness protects under-floor B)", got, aBlock)
	}
	// Prove it is the knob: the same state at w=0 evicts B (the true lowest H).
	p0 := NewGDSFElastic(map[string]int64{"A": 25, "B": 1000}, 0)
	p0.RecordWrite(WriteMeta{Key: aBlock, TenantID: "A", Size: 100, Cost: 10})
	p0.RecordWrite(WriteMeta{Key: bBlock, TenantID: "B", Size: 100, Cost: 5})
	if got, _ := p0.Victim(); got != bBlock {
		t.Fatalf("Victim = %x at w=0, want B's true-lowest-H block %x", got, bBlock)
	}
}

// TestGDSFElastic_UnflooredTenantReclaimedFirst: a tenant with NO configured floor has no fairness
// guarantee, so under contention its blocks go before a floored tenant's — even if its block is
// more valuable. Guards the overage()==maxOverage branch.
func TestGDSFElastic_UnflooredTenantReclaimedFirst(t *testing.T) {
	p := NewGDSFElastic(map[string]int64{"A": 1000}, 1.0) // A floored generously; Z has no floor
	zBlock := hashByte(1)
	p.RecordWrite(WriteMeta{Key: zBlock, TenantID: "Z", Size: 100, Cost: 100}) // true H = 1.0 (high!)
	p.RecordWrite(WriteMeta{Key: hashByte(2), TenantID: "A", Size: 100, Cost: 1})

	if got, ok := p.Victim(); !ok || got != zBlock {
		t.Fatalf("Victim = %x, want unfloored Z's block %x (no fairness guarantee)", got, zBlock)
	}
}

// TestStore_LRUUnaffectedByQuotaPath proves the optional QuotaPolicy seam is truly opt-in: an
// LRU-backed store never reports quota pressure (LRU doesn't implement QuotaPolicy), so
// needsEviction is governed purely by the byte watermark, exactly as in Phase 4.
func TestStore_LRUUnaffectedByQuotaPath(t *testing.T) {
	s := NewStore(NewLRU()) // unbounded
	putTenant(s, 1, "A", 100, 10)
	if s.needsEviction() {
		t.Fatal("needsEviction = true for unbounded LRU store, want false (no quota path)")
	}
}
