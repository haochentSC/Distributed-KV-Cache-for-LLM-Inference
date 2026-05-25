package cache

// EvictionPolicy is the swappable seam mandated by the plan (Phase 4). Phase 1
// ships NoopPolicy; Phase 4 adds LRU+TTL; Phase 5 the GDSF + fairness engine —
// all without touching Store or the gRPC API. See the eviction-policy-gdsf-drf skill.
type EvictionPolicy interface {
	RecordAccess(BlockHash)
	RecordWrite(key BlockHash, size int64)
	RecordEvict(BlockHash)
	// Victim returns the next block to evict under memory pressure, or false if none.
	Victim() (BlockHash, bool)
}

// NoopPolicy never evicts. It lets the Store work before real policies exist.
type NoopPolicy struct{}

func (NoopPolicy) RecordAccess(BlockHash)       {}
func (NoopPolicy) RecordWrite(BlockHash, int64) {}
func (NoopPolicy) RecordEvict(BlockHash)        {}
func (NoopPolicy) Victim() (BlockHash, bool)    { return BlockHash{}, false }
