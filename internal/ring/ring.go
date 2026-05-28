// Package ring implements a consistent-hash ring with virtual nodes. It maps an
// opaque routing key (a prefix root — block_hash[0], ADR 0011) to the cache node
// that owns it, under the prefix-affinity sharding decision (ADR 0014).
//
// Why a ring and not hash%N: with hash%N, changing the node count remaps almost
// every key (a cache-wide miss storm on every scale event). A consistent-hash ring
// moves only ~1/N of keys when a node is added or removed — that minimal-movement
// property is what makes scaling and failover cheap, and it is what ring_test.go
// asserts. Virtual nodes (several ring points per physical node) even out the arc
// sizes so load converges to 1/N. See docs/01-architecture-overview.md §7 and the
// distributed-systems-in-go skill's consistent-hashing notes.
package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// Ring is a consistent-hash ring. It is safe for concurrent use: reads
// (Lookup/Nodes) take the read lock, mutations (Add/Remove) take the write lock.
// The ring is rebuilt from the member set in Phase 2c (etcd watch), so Add/Remove
// must leave points sorted and consistent.
type Ring struct {
	mu      sync.RWMutex
	vnodes  int                 // virtual points per physical node
	points  []vpoint            // the hash circle, kept sorted by hash ascending
	members map[string]struct{} // current physical nodes (for Nodes + idempotency)
}

// vpoint is one virtual node: a position on the circle owned by a physical node.
type vpoint struct {
	hash uint64
	node string
}

// New returns an empty ring placing vnodes virtual points per physical node.
// If vnodes <= 0, callers should treat that as a programming error; pick a sane
// floor or panic — your call to make in review.
func New(vnodes int) *Ring {
	// vnodes <= 0 is a programming error (an unparsed flag, a zero config). Fail loud
	// here rather than silently building a ring that owns nothing — a clamped default
	// would hide the misconfiguration. (Constructors panicking on invalid args is fine
	// in Go; it's a bug-in-caller signal, not a runtime condition to handle.)
	if vnodes <= 0 {
		panic(fmt.Sprintf("ring.New: vnodes must be > 0, got %d", vnodes))
	}
	return &Ring{
		vnodes:  vnodes,
		members: make(map[string]struct{}),
	}
}

// Add inserts node and its vnodes virtual points onto the ring, then restores the
// sorted invariant. Adding a node already present is a no-op (idempotent), because
// the etcd watch in 2c may replay membership.
func (r *Ring) Add(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.members[node]; ok {
		return // idempotent: the Phase 3 etcd watch may replay membership
	}
	for i := 0; i < r.vnodes; i++ {
		r.points = append(r.points, vpoint{hash: vnodeHash(node, i), node: node})
	}
	// Re-sort the whole circle so Lookup can binary-search. sort.Slice takes a "less"
	// closure; ascending by hash is the invariant every other method relies on.
	sort.Slice(r.points, func(i, j int) bool { return r.points[i].hash < r.points[j].hash })
	r.members[node] = struct{}{}
}

// Remove deletes node and all of its virtual points, preserving sorted order.
// Removing an absent node is a no-op (idempotent).
func (r *Ring) Remove(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.members[node]; !ok {
		return // idempotent
	}
	// In-place filter: a common Go idiom. kept aliases r.points' backing array, but we
	// only ever write at an index <= the read index, so it's safe. Filtering keeps the
	// surviving points in their existing order, so no re-sort is needed (vpoint is a
	// value type — no dangling pointers left in the truncated tail).
	kept := r.points[:0]
	for _, p := range r.points {
		if p.node != node {
			kept = append(kept, p)
		}
	}
	r.points = kept
	delete(r.members, node)
}

// Lookup returns the node that owns key — the node of the first virtual point at or
// after hash(key), wrapping past the end of the circle back to points[0]. Returns ""
// when the ring is empty. key is an opaque routing root (block_hash[0]); it is
// already a uniform SHA-256, so derive the ring position directly from its bytes
// (e.g. binary.BigEndian.Uint64(key[:8])) rather than re-hashing it.
func (r *Ring) Lookup(key []byte) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 {
		return ""
	}
	// Real keys are 32-byte SHA-256 roots, so key[:8] is uniform. keyHash is shared with
	// LookupN so the two can't drift on a malformed-key edge case.
	h := keyHash(key)
	// sort.Search returns the smallest index i where points[i].hash >= h. If none
	// qualifies (h is past the largest point), it returns len(points) and we wrap to 0 —
	// that's the "clockwise past the top of the circle" step.
	idx := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if idx == len(r.points) {
		idx = 0
	}
	return r.points[idx].node
}

// LookupN returns up to n DISTINCT physical nodes that own key, in clockwise order from
// the owner: index 0 is the primary (== Lookup(key)), index 1 the first replica, and so
// on. It returns fewer than n only when the ring has fewer than n physical nodes. This is
// the placement primitive for RF=2 replication (ADR 0021): primary = result[0], replica =
// result[1]. Every process computes the same result from the same member set, so primary
// and replica agree on placement with no extra coordination.
func (r *Ring) LookupN(key []byte, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 || n <= 0 {
		return nil
	}
	h := keyHash(key)
	idx := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if idx == len(r.points) {
		idx = 0
	}
	// Cap n at the physical node count so n > N callers don't trigger a wasted full lap.
	if n > len(r.members) {
		n = len(r.members)
	}
	out := make([]string, 0, n)
	seen := make(map[string]struct{}, n)
	for step := 0; step < len(r.points) && len(out) < n; step++ {
		node := r.points[(idx+step)%len(r.points)].node
		if _, dup := seen[node]; dup {
			continue // skip the other vnodes of an already-collected physical node
		}
		seen[node] = struct{}{}
		out = append(out, node)
	}
	return out
}

// keyHash maps a routing key to a ring position. Extracted so Lookup and LookupN can't drift
// (a single keying bug would break both placement and read-failover the same way, which is
// at least debuggable; two independent bugs would not be).
func keyHash(key []byte) uint64 {
	if len(key) >= 8 {
		return binary.BigEndian.Uint64(key[:8])
	}
	var b [8]byte
	copy(b[:], key)
	return binary.BigEndian.Uint64(b[:])
}

// Nodes returns the current physical node IDs in no particular order.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.members))
	for n := range r.members { // map iteration order is randomized in Go — hence "no particular order"
		out = append(out, n)
	}
	return out
}

// vnodeHash maps a virtual-node label to a position on the circle. The label must be
// stable and unique per (node, i) so every process builds an identical ring from the
// same member set (clients must agree — see 2c).
//
// vnodeHash must be (a) deterministic across processes — every client builds an identical
// ring from the same member set (ADR 0018 static membership), so maphash's per-process
// random seed is out — and (b) well-distributed, so the vnodes spread evenly and each node
// owns ≈1/N of the circle. We first tried fnv-1a here; TestDistribution caught it clustering
// badly (one node owned ~41%, another ~11% over 4 nodes / 128 vnodes), because fnv has weak
// avalanche on short, near-identical labels like "n3#0".."n3#127". sha256's first 8 bytes
// fix it (it's what the block-hash chain already uses) and the cost is negligible — this
// runs only on Add/Remove, never on the Lookup hot path.
func vnodeHash(node string, i int) uint64 {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s#%d", node, i)))
	return binary.BigEndian.Uint64(sum[:8])
}
