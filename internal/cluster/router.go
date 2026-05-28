// Package cluster implements client-side ("smart client") shard routing. There is no
// proxy in front of the shards: every client (the Go load generator now, the Python
// vLLM connector later) holds a copy of the consistent-hash ring and resolves the
// owning shard in-process, then talks to that shard directly. This avoids an extra
// network hop on the sub-10 ms lookup path and removes the proxy as a SPOF/bottleneck.
//
// Because sharding is prefix-affinity (ADR 0014 — the ring keys on the prefix root
// block_hash[0]), an entire prompt's blocks belong to ONE shard, so a caller resolves
// the owner once per prompt (OwnerConn on blocks[0].Hash) and sends that prompt's
// Lookup/Fetch/Write all to that single owner. An unreachable owner degrades to a cache
// MISS (the caller recomputes) — never block the read path on a down node (ADR 0016).
//
// Membership seam (ADR 0018): the Router does not know where its member set comes from.
// A driver calls SetMembers — a static flag/config driver in Phase 2, and the etcd watch
// in Phase 3 (Sub-stage A) calling the SAME method on every watch event. That keeps the
// routing logic unchanged when etcd lands.
package cluster

import (
	"log"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/haochentSC/distributed-kv-cache/internal/ring"
)

// Member is one cache shard: a stable node ID (the ring label — must match across all
// clients so they build identical rings) and a dial address.
type Member struct {
	ID   string // ring label, e.g. "shard-0"; stable across restarts
	Addr string // gRPC dial target, e.g. "10.0.1.5:50051"
}

// Router resolves which shard owns a routing key and hands back a connection to it.
//
// Concurrency contract: mu guards the {ring, pool} PAIR as one consistent snapshot.
// SetMembers (write) takes mu.Lock; OwnerConn (read) takes mu.RLock and reads both the
// ring and the pool under it, so a reader never sees a node in the ring that's missing
// from the pool (or vice versa) mid-update. (The ring has its own internal lock too, but
// we coordinate the pair here so routing sees an atomic view.)
type Router struct {
	mu   sync.RWMutex
	ring *ring.Ring
	pool map[string]*grpc.ClientConn // node ID -> connection; parallel to ring members
}

// New returns an empty Router whose ring places vnodes virtual points per node.
// Call SetMembers before routing (an empty ring resolves nothing).
func New(vnodes int) *Router {
	return &Router{
		ring: ring.New(vnodes), // panics on vnodes <= 0 — a misconfig should fail loud
		pool: make(map[string]*grpc.ClientConn),
	}
}

// SetMembers reconciles the Router to exactly the given member set: it updates the ring
// and the connection pool so that, afterward, the ring's members and the pool's keys are
// both exactly {m[i].ID}. Idempotent — calling it with the current set is a no-op. This
// is the single mutation entry point; both the Phase 2 static driver and the Phase 3
// etcd watcher call it.
//
// TODO(hc) [guided — this is the routing core, implement it]:
//   - Take r.mu.Lock (you're mutating the ring/pool pair).
//   - Build a set of the desired IDs from m for quick membership tests.
//   - ADDED nodes (in m, not in r.pool): dial with grpc.NewClient(addr, ...) using
//     insecure transport credentials for now (import google.golang.org/grpc/credentials/
//     insecure — matches loadgen/main.go:48). grpc.NewClient is LAZY: it does not connect
//     until the first RPC, so dialing here can't fail on an unreachable node — good, that
//     defers "is it up?" to the degrade-to-miss path. Store the conn in r.pool and call
//     r.ring.Add(id) (Add is idempotent — ring.go:59).
//   - REMOVED nodes (in r.pool, not in m): r.ring.Remove(id) (idempotent), then
//     conn.Close() and delete(r.pool, id). Closing matters so you don't leak connections
//     as membership churns in Phase 3.
//   - Think about: do you want to close-then-remove or remove-then-close? (A reader could
//     still be mid-RPC on a conn you're closing — gRPC handles a Close under an in-flight
//     call by erroring that call, which your degrade-to-miss path already treats as a
//     miss. Note why that's safe in a comment.)
func (r *Router) SetMembers(m []Member) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Desired ID set for O(1) "should this stay?" tests in the removal pass.
	desired := make(map[string]struct{}, len(m))
	for _, mem := range m {
		desired[mem.ID] = struct{}{}
	}

	// ADD pass: dial + ring.Add any member we don't already pool. Both grpc.NewClient
	// and ring.Add are safe to skip/no-op when already present, so this is idempotent.
	for _, mem := range m {
		if _, ok := r.pool[mem.ID]; ok {
			continue
		}
		// grpc.NewClient is LAZY — it validates the target and sets up the balancer but
		// does NOT connect, so it only errors on a malformed target (a config bug), never
		// on an unreachable host. We skip+log a bad target rather than panic: in Phase 3
		// this runs inside the etcd watch loop, where one fat-fingered member must not take
		// routing down for every other shard.
		conn, err := grpc.NewClient(mem.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("cluster: skipping member %q: bad dial target %q: %v", mem.ID, mem.Addr, err)
			continue
		}
		r.pool[mem.ID] = conn
		r.ring.Add(mem.ID)
	}

	// REMOVE pass: drop anything no longer desired. Order matters — remove from the ring
	// FIRST so OwnerConn stops selecting it for new requests, THEN Close the conn. Any RPC
	// already in flight on that conn is aborted by Close and surfaces as an error the
	// caller already treats as a cache miss (degrade-to-miss, ADR 0016) — so closing under
	// an in-flight call is safe, not a correctness hazard.
	for id, conn := range r.pool {
		if _, ok := desired[id]; ok {
			continue
		}
		r.ring.Remove(id)
		conn.Close()
		delete(r.pool, id)
	}
}

// OwnerConn returns a connection to the shard that owns key (the prefix root
// blocks[0].Hash), and true; or (nil, false) when the ring is empty or the owner has no
// live connection. A false result is the caller's signal to treat the request as a cache
// MISS (recompute) — it must not be a fatal error.
//
// TODO(hc) [guided]:
//   - Take r.mu.RLock / defer RUnlock (read the ring+pool as one snapshot).
//   - node := r.ring.Lookup(key); if node == "" the ring is empty -> return nil, false.
//   - conn, ok := r.pool[node]; return conn, ok.
func (r *Router) OwnerConn(key []byte) (*grpc.ClientConn, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	node := r.ring.Lookup(key)
	if node == "" { // empty ring: no owner
		return nil, false
	}
	conn, ok := r.pool[node] // ok==false would mean ring/pool drifted — the mu pairing prevents it
	return conn, ok
}

// OwnerConns returns connections to the shards that own key, ORDERED primary-first then
// replica(s) — up to n entries (n = replication factor). This is the client's read-failover
// path (ADR 0021): try result[0] (primary); on an unreachable owner or a NotFound, fall
// through to result[1] (the replica) before finally degrading to a cache MISS (ADR 0016).
// Returns a nil/short slice when the ring is empty or has fewer than n live members; the
// caller ranges whatever it gets.
func (r *Router) OwnerConns(key []byte, n int) []*grpc.ClientConn {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := r.ring.LookupN(key, n)
	if len(ids) == 0 {
		return nil
	}
	out := make([]*grpc.ClientConn, 0, len(ids))
	for _, id := range ids {
		if conn, ok := r.pool[id]; ok {
			out = append(out, conn) // skip a ring/pool gap rather than emit a nil
		}
	}
	return out
}

// OwnersN returns the ordered owner IDs for key — [primary, replica, ...], up to n — via
// the ring's clockwise distinct-node walk. The primary's replicator uses it to find which
// peers to forward a write to (it skips its own ID). [scaffold — thin ring wrapper]
func (r *Router) OwnersN(key []byte, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ring.LookupN(key, n)
}

// ConnFor returns the pooled connection for a node ID, and whether one exists. Used by the
// replicator to dial a specific replica it chose via OwnersN. [scaffold]
func (r *Router) ConnFor(id string) (*grpc.ClientConn, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.pool[id]
	return c, ok
}

// Owner returns the node ID that owns key (or "" when the ring is empty). This is a
// diagnostics/metrics helper — the routing hot path uses OwnerConn (which needs the
// connection, not the label). Used by the load generator to report the per-shard
// request distribution, i.e. ADR 0014's deferred hot-shard measurement.
func (r *Router) Owner(key []byte) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	node := r.ring.Lookup(key)
	return node, node != ""
}

// Close tears down every pooled connection. Call on shutdown. Safe to call once.
func (r *Router) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var firstErr error
	for id, conn := range r.pool {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(r.pool, id)
	}
	return firstErr
}
