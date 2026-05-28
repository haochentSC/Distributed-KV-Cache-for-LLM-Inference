# ADR 0021 — RF=2 async replication: ring-placed, primary-driven, by prompt root

- **Status:** accepted
- **Date:** 2026-05-27 (Phase 3, Sub-stage B)
- **Deciders:** HC (+ Claude)

## Context

Phase 3's goal is surviving a node failure (plan §4). Prefix-affinity (ADR 0014) puts every
block of a prompt on exactly one shard — the ring owner — so losing that node erases all the
cache it owned until it re-warms. RF=2 (primary + one replica) keeps each block on two nodes so a
single death degrades gracefully instead of dropping a shard's worth of cache. ADR 0013 already
licenses the cheap design: cache data is *eventually consistent*, so replication can be async and
a lost not-yet-replicated tail is just a recompute, never a correctness violation. Sub-stage A
(ADR 0019/0020) gave us the seam this builds on: the smart-client `cluster.Router` (ring +
connection pool) driven by the etcd membership watch.

## Decision

1. **Replica placement = ring, next distinct node clockwise.** RF=2 = ring owner + the next
   *distinct physical* node walking clockwise (skipping the owner's own vnodes), via a new
   `ring.LookupN(key, n)` → ordered `[primary, replica, …]`. Deterministic from the member set, so
   every process computes the same replica with **zero extra etcd state** — same reasoning ADR 0020
   used to publish membership only (don't add a second source of truth while placement is
   deterministic). Rejected an explicit `/kvcache/replicas/*` map (flexible but redundant now).

2. **Primary-driven, async.** The client writes only to the **primary** (the ring owner it already
   routes to). The primary stores locally, **acks the client immediately**, then a background
   `Replicator` forwards the block to the replica. "Acked" means *durable on the primary*, not on RF
   nodes — safe precisely because the data is regenerable (unlike a DB, an async-lost copy costs a
   recompute, not a lost write). Rejected client dual-write (muddier acks, doubles client cost, and
   the Python connector would also have to replicate).

3. **Dedicated `Replicate` RPC, loop-free.** The primary→replica copy travels over a new
   `rpc Replicate(stream WriteChunk)` — same wire shape as `Write`, two differences in *meaning*:
   the header's `version` is **authoritative** (the replica stores under the primary's version via
   `Store.PutWithVersion`, so the two copies' versions never diverge and a version-pinned `Fetch`
   can't spuriously miss), and the replica **must not re-forward** (that is what prevents an infinite
   replication loop). Rejected an `is_replica` bool on `Write` (overloads one method with two
   meanings and still needs the version field).

4. **Placement is by prompt ROOT; storage is by block hash.** A prompt is routed whole by
   `blocks[0].Hash` (prefix-affinity), and the client's read-failover keys on that same root, so the
   replica must be chosen by the **root**, not by each block's own hash — otherwise the failover read
   would query the wrong replica. The primary server only sees individual block writes, so the client
   passes the root in a new `WriteHeader.routing_key`. The replica still *stores* each block under its
   own `block_hash`; only *placement* uses the root.

5. **Read-side failover, included now.** `Router.OwnerConns(key, n)` returns connections ordered
   `[primary, replica, …]`; the caller tries the primary, then the replica, then degrades to a cache
   MISS (ADR 0016). Without this, RF=2 would write a replica that's never read, so the "kill a node,
   still serves" demo wouldn't work — hence in-scope, not deferred.

6. **Bounded, drop-under-pressure queue.** The `Replicator` has a bounded channel; `Enqueue` is
   non-blocking and **drops** when full (logs it). Blocking would stall the Write ack on a slow
   replica — the opposite of the design. A dropped job is a future recompute (ADR 0017 backpressure).

## Consequences

- New ring primitive `LookupN`; new store path `PutWithVersion` (version-guarded: a stale delivery
  with `version <= local` is dropped, so out-of-order replication can't clobber a newer copy).
- `cache-server` gains `-rf` and, when `-etcd` is set with `rf>1`, watches membership itself (reusing
  the client `cluster.Router` + `coord.DriveRouter`) so server and clients agree on the replica.
- Proto grows `WriteHeader.version` (field 7) and `routing_key` (field 8); additive, Go stubs regen
  clean. Python connector regen + replica-aware routing is a follow-up (it only Writes today).
- Sets up Sub-stage C: the lease TTL (ADR 0020) becomes the failure-detection window, and a promoted
  replica must start *being* a primary (accepting Writes for the dead node's keyspace) — the ring
  already hands the dead owner's keys to the next node, which is the replica, so promotion is mostly
  "the replica was already holding the data."
- **Not yet `-race`-verified** (Windows 32-bit MinGW cgo blocker, per the learning log): the new
  `Replicator` goroutine + the router's RWMutex pairing need a WSL2 `-race` pass before chaos (Phase 4).

## Status of implementation

Skeleton landed (signatures, stubs, proto, wiring); the guided cores — `ring.LookupN`,
`Store.PutWithVersion`, `Router.OwnerConns`, the `Write` enqueue hook, `Server.Replicate`, and the
`Replicator` queue/forward loop — are `TODO(hc)` for HC to implement, then review + a `-race` pass.
