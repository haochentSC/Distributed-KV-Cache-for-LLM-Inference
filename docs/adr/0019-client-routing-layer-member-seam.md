# ADR 0019 — Client-side routing layer + the member-source seam

- **Status:** accepted
- **Date:** 2026-05-27 (Phase 3 start, Step 0 — finishes Phase 2)
- **Deciders:** HC (+ Claude)

## Context

Phase 2 built and tested the consistent-hash ring (`internal/ring`, ADR 0014 prefix-affinity), but
nothing consumed it: the load generator still dialed a single `-addr`, so sharding was never actually
exercised across nodes. The ring is a data structure; routing is the layer that turns it into "which
shard do I talk to, and over which connection." That layer is the seam ADR 0018 said must be "cleanly
replaceable by an etcd watch in Phase 3" — so it has to exist (and be shaped for that swap) before
etcd, replication, or failover have anything to coordinate. See `00-project-plan.md` §4 (Phase 2
deliverable: "cache works across all nodes and reports per-shard distribution") and
`01-architecture-overview.md` §7.

## Decision

Add a **smart-client router** (`internal/cluster`) — no proxy: each client holds the ring and
resolves the owning shard in-process, then talks to it directly (the Redis-Cluster / Cassandra-driver
pattern). Two decisions inside it:

1. **Routing follows prefix-affinity (ADR 0014):** a caller resolves the owner *once per prompt* on
   `blocks[0].Hash` (`OwnerConn`) and sends that prompt's `Lookup`/`Fetch`/`Write` all to that one
   shard. An unreachable owner degrades to a cache **miss** (recompute), never a fatal error (ADR 0016).
2. **The member-source seam is `Router.SetMembers([]Member)`, pushed by an external driver.** The
   router does not know where members come from. A static flag driver calls `SetMembers` once now; the
   Phase 3 etcd watch calls the *same* method on every event. Chosen over a `MemberSource`
   interface-the-router-pulls-from because it keeps the seam to a single method, puts all ring+pool
   mutation (and its locking) in one place, and means zero routing-code change when etcd lands.

## Alternatives considered

- **`MemberSource` interface the router consumes (with a `Watch()` channel)** — more "classically Go,"
  but the router would own a goroutine/channel lifecycle and the seam surface is larger. Rejected for
  the smaller `SetMembers` seam.
- **A proxy/load-balancer in front of the shards** — central place for routing, but adds a network hop
  on the sub-10 ms lookup path and is a SPOF/bottleneck. Rejected: smart clients route in-process.
- **Per-block routing** — already rejected in ADR 0014 (fan-out latency); affinity makes routing a
  single owner lookup per prompt.

## Consequences

- Phase 2 is now genuinely complete: a live 3-shard run routes correctly and the load generator
  reports the **per-shard distribution** — the hot-shard measurement ADR 0014 deferred. Measured at
  `prefix-share=0.8`: one shard takes ~87% of requests (≈ `0.8 + 0.2/N`), confirming affinity
  concentrates a viral prefix on one shard and motivating the deferred hot-prefix-replication mitigation.
- The connection-pool concurrency contract (`mu` guards the `{ring, pool}` pair as one snapshot;
  remove-from-ring-then-`Close` so an in-flight RPC aborts into the degrade-to-miss path) is the model
  Phase 3 reuses when the etcd watch churns membership.
- Sub-stage A replaces the one static `SetMembers` call with an etcd watch loop calling `SetMembers`;
  the routing/pool logic does not change.
- `-race` was not run (Windows 32-bit MinGW cgo blocker, per the learning log); plain `go test` is
  green. The RWMutex pairing still needs a WSL2 `-race` pass before the chaos phase.
