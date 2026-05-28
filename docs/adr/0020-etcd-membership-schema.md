# ADR 0020 — etcd membership schema: lease-bound `/kvcache/members/`, membership-only

- **Status:** accepted
- **Date:** 2026-05-27 (Phase 3, Sub-stage A)
- **Deciders:** HC (+ Claude)

## Context

Phase 3 introduces etcd as the linearizable metadata store (ADR 0013, 0009, 0018). Sub-stage A is the
first slice: dynamic cluster **membership**, replacing the Phase-2 static member list that drove
`Router.SetMembers` (ADR 0019). We need to decide what to store in etcd, how a node announces itself
and how its death is detected, and how clients turn that into a ring. Arch §9 sketched three key
families (`/kvcache/members/*`, `/kvcache/ring/*`, `/kvcache/leader/*`) and left the exact schema open.
See `00-project-plan.md` §4 (Phase 3) and `01-architecture-overview.md` §9.

## Decision

- **Key schema (this sub-stage):** `/kvcache/members/<node-id> -> <advertise-addr>`, **membership
  only**. No `/kvcache/ring/*` is published: the ring is *deterministic* from the member set (ADR 0018),
  so every client recomputes an identical ring (confirmed live — the etcd-driven run reproduced the
  static run's per-shard distribution exactly). Leadership keys are deferred to Sub-stage C.
- **Registration = lease + keepalive.** A node `Grant`s a lease (TTL default 10s — *this TTL is the
  failure-detection window and becomes the split-brain knob in Sub-stage C*), `Put`s its key bound to
  the lease, and keepalives in the background (draining the ack channel, or keepalive stalls). Graceful
  shutdown `Revoke`s the lease so the key is deleted immediately rather than after the TTL; a crash just
  lets the lease lapse. Either way etcd deletes the key — failure detection with no heartbeat code of
  our own.
- **Discovery = prefix watch → full snapshot → `SetMembers`.** `WatchMembers` does a prefix `Get` for
  the initial state, **captures its revision, and starts the watch at `revision+1`** so no event between
  the Get and the Watch is missed or double-applied. It keeps an authoritative `id→addr` map and emits
  the **complete set** on each change (not deltas), because `SetMembers` reconciles to "exactly this
  set." `DriveRouter` feeds each snapshot to the router — the same seam the static driver used.

## Alternatives considered

- **Also publish `/kvcache/ring/<vnode> -> node`** (arch §9 sketch) — lets ownership diverge from pure
  hashing (manual rebalance / weighting), but adds a second thing to keep consistent and is redundant
  while placement is deterministic. Deferred; revisit only if weighted placement is wanted.
- **Heartbeat keys instead of leases** — re-implements what etcd leases give for free; rejected.
- **Emit deltas instead of full snapshots** — fights `SetMembers`' set-reconcile contract and makes the
  client reconstruct state; rejected.

## Consequences

- A node join/leave now propagates to every client's ring automatically; the routing/pool code from
  ADR 0019 is unchanged — only the *driver* calling `SetMembers` changed (static list → etcd watch).
  Verified live: 3 servers self-register, loadgen with only `-etcd` discovers and routes across them.
- `cache-server` gains `-etcd`, `-advertise`, `-node-id`, `-lease-ttl` flags; with `-etcd` empty it
  still runs standalone (local single-node, existing tests unaffected).
- The lease TTL is now a real knob we must tune against the partition-detection window in Sub-stage C
  (too short → false-dead under a GC pause; too long → slow failover). Flagged in `Register`.
- Acceptance is an integration test (`TestEtcdRegisterAndWatch`) that **skips without etcd** and asserts
  register→appear, release→disappear. `-race` on the watch goroutine still needs a WSL2 pass.
- New dependency: `go.etcd.io/etcd/client/v3 v3.5.17` (matches the etcd image tag).
