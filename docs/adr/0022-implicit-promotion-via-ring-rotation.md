# ADR 0022 — Implicit replica promotion via ring rotation

- **Status:** accepted
- **Date:** 2026-05-28 (Phase 3, Sub-stage C)
- **Deciders:** HC (+ Claude)

## Context

Sub-stage B (ADR 0021) gave us RF=2 with the replica deterministically placed at
`LookupN(root, 2)[1]`. Sub-stage C asks: when the primary dies, *who* becomes the new primary
and *how* do we say so? The textbook answer is leader election — a separate etcd lease per
shard, a watch that fires when it expires, an explicit "I am now primary" handshake. We can
avoid all of it.

## Decision

**Promotion is implicit.** No per-shard leader lease, no election handshake. When the dead
node's membership lease expires:

1. etcd auto-deletes `/kvcache/members/<dead-id>` (lease expiry, ADR 0020).
2. The membership watch (`coord.WatchMembers`) emits a fresh snapshot without the dead id.
3. Every router applies it via `SetMembers`, which calls `ring.Remove(dead-id)`.
4. For every key the dead node owned, `LookupN(key, 2)` now returns
   `[old-replica, new-replica]` — the old replica is the new primary, with **the data
   already on disk**, because Sub-stage B has been replicating to it all along.

The "promote" step is the ring rotation; the data was pre-positioned. No code runs at
"promotion time" — the next Lookup/Fetch from a client just hits the new primary, and the
next Write enqueues a fresh replica copy onto whoever is now `LookupN[1]`.

## Why this works (and where it bends)

- **Determinism is load-bearing.** ADR 0018's "every process recomputes the same ring" is
  what lets "the old replica becomes the new primary" be a property of the rotation rather
  than a coordination step. The `TestLookupN_RebalanceOnRemove` invariant asserts it.
- **Read failover bridges the detection gap.** The lease TTL (10 s by default) is the
  detection window. In that window, clients still route to the dead primary. `OwnerConns`
  hands them the replica next, so the failover read short-circuits the lag and serves from
  the replica — the same node that will *become* primary moments later, so reads stay
  consistent across the cutover.
- **Writes during the gap.** A Write to the dead primary errors out at gRPC; the loadgen's
  failover path currently does **not** re-issue Writes to the replica (writes must be
  primary-minted; otherwise two primaries could co-exist for one second of skew and assign
  divergent versions). Misses during the gap stay misses — the next request to the new
  primary refills.
- **Two-primary risk = lease TTL.** If a node is partitioned but still alive, its lease
  eventually expires and another node "becomes" primary while the partitioned one still
  thinks it is. Cache data is eventually consistent (ADR 0013), and Writes only ever
  ack-on-primary, so the worst case is two acks-of-different-versions; the higher-version
  one wins on the next replication, and `PutWithVersion`'s stale-drop guard makes the loser
  a no-op. No data corruption, just brief duplicate work.

## What we are NOT doing here

- **No per-shard election.** Every shard rotation falls out of one membership lease.
- **No backfill on promotion.** When the new primary takes over, blocks that the dead node
  held but never replicated to it are gone (the recompute cost paid by the next request,
  per ADR 0013). A scheduled "scan local blocks, ensure their `LookupN[1]` has a copy" sweep
  is a Phase 4 chaos/observability item — useful, not required for correctness.
- **No "I am primary" gossip.** Nodes don't track which shards they own; they just answer
  RPCs and the client decides where to send them.

## Consequences

- Sub-stage C ships almost no new code — the design is "we already did the work in B." The
  new test (`TestLookupN_RebalanceOnRemove`) is the executable spec of the rotation
  invariant.
- The lease TTL (ADR 0020, currently 10 s) **is** the failover window. Shortening it
  shortens the gap at the cost of false-positive removals on GC pauses; we'll measure under
  Phase 4 chaos before tuning.
- Sub-stage D (graceful drain) becomes the *fast* path of this same mechanism: a clean
  shutdown revokes the lease immediately rather than waiting out the TTL, so the rotation
  happens in milliseconds rather than seconds.
