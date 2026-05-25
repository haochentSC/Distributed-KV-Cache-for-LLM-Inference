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

import "sync"

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
	// TODO(hc):
	//   1. if node already in r.members, return.
	//   2. for i in [0, r.vnodes): append vpoint{hash: vnodeHash(node, i), node: node}.
	//   3. re-sort r.points by hash ascending (sort.Slice), and record the member.
	// Keeping points sorted here is what lets Lookup binary-search.
}

// Remove deletes node and all of its virtual points, preserving sorted order.
// Removing an absent node is a no-op (idempotent).
func (r *Ring) Remove(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// TODO(hc):
	//   1. if node not in r.members, return.
	//   2. rebuild r.points keeping only points whose .node != node
	//      (filtering preserves the existing sorted order — no re-sort needed).
	//   3. delete(r.members, node).
}

// Lookup returns the node that owns key — the node of the first virtual point at or
// after hash(key), wrapping past the end of the circle back to points[0]. Returns ""
// when the ring is empty. key is an opaque routing root (block_hash[0]); it is
// already a uniform SHA-256, so derive the ring position directly from its bytes
// (e.g. binary.BigEndian.Uint64(key[:8])) rather than re-hashing it.
func (r *Ring) Lookup(key []byte) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	// TODO(hc):
	//   1. if len(r.points) == 0, return "".
	//   2. h := first 8 bytes of key as a uint64 (guard len(key) < 8).
	//   3. idx := sort.Search(len(r.points), func(i) bool { return r.points[i].hash >= h }).
	//   4. if idx == len(r.points), wrap to 0 (clockwise past the top of the circle).
	//   5. return r.points[idx].node.
	panic("TODO(hc): implement Ring.Lookup")
}

// Nodes returns the current physical node IDs in no particular order.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	// TODO(hc): collect and return the keys of r.members.
	panic("TODO(hc): implement Ring.Nodes")
}

// vnodeHash maps a virtual-node label to a position on the circle. The label must be
// stable and unique per (node, i) so every process builds an identical ring from the
// same member set (clients must agree — see 2c).
//
// TODO(hc): pick a hash. hash/fnv (fnv.New64a over fmt.Sprintf("%s#%d", node, i)) is
// the simplest; crypto/sha256 of the label, first 8 bytes, is also fine. Avoid
// maphash here — its seed is process-random, which would make rings disagree.
func vnodeHash(node string, i int) uint64 {
	panic("TODO(hc): implement vnodeHash")
}
