package cache

// EvictionPolicy is the swappable seam mandated by the plan (Phase 4). Phase 1
// ships NoopPolicy; Phase 4 adds LRU+TTL; Phase 5 the GDSF + fairness engine —
// all without touching Store or the gRPC API. See the eviction-policy-gdsf-drf skill.
type EvictionPolicy interface {
	RecordAccess(BlockHash)
	// RecordWrite records a (new or overwritten) block. It takes a WriteMeta rather than a bare
	// (hash, size) so a cost-aware/per-tenant policy (Phase 5 GDSF) gets the tenant and recompute
	// cost it needs; LRU ignores everything but the key. Called under a stripe lock, so it must
	// never call back into the Store (lock order is stripe -> policy).
	RecordWrite(WriteMeta)
	RecordEvict(BlockHash)
	// Victim returns the next block to evict under memory pressure, or false if none.
	Victim() (BlockHash, bool)
}

// WriteMeta is the metadata a policy needs to value a block. It carries only what the policy
// reads — NOT the KV bytes — so the eviction layer never touches payload. Cost and TenantID are
// zero/empty for callers that don't set them (LRU, single-tenant runs), which the policies treat
// as "no cost signal" / "the default tenant".
type WriteMeta struct {
	Key      BlockHash
	TenantID string  // Phase 5 fairness; "" = the unnamed default tenant
	Size     int64   // resident bytes (len(KV)); the GDSF denominator
	Cost     float64 // estimated recompute cost (∝ prefill FLOPs); the GDSF numerator
}

// QuotaPolicy is an OPTIONAL capability a policy may implement to enforce per-tenant byte quotas
// INDEPENDENTLY of the global watermark (Phase 5a static fairness). The Store/Evictor type-assert
// for it: when present, an over-quota tenant is treated as eviction pressure (a tenant can fill
// the cache while others are idle, but is reclaimed first once it exceeds its floor). LRU and
// NoopPolicy do NOT implement it, so their behaviour is unchanged — watermark-only eviction.
type QuotaPolicy interface {
	EvictionPolicy
	// OverQuota reports whether any tenant currently exceeds its configured quota. The Evictor
	// keeps draining while this is true, on top of the global watermark condition. Called without
	// any stripe lock held (from the Evictor loop) AND under a stripe lock (from signalEvict), so
	// it must only take the policy's own lock — never a stripe lock — to keep the stripe->policy
	// order.
	OverQuota() bool
}

// NoopPolicy never evicts. It lets the Store work before real policies exist.
type NoopPolicy struct{}

func (NoopPolicy) RecordAccess(BlockHash)    {}
func (NoopPolicy) RecordWrite(WriteMeta)     {}
func (NoopPolicy) RecordEvict(BlockHash)     {}
func (NoopPolicy) Victim() (BlockHash, bool) { return BlockHash{}, false }
