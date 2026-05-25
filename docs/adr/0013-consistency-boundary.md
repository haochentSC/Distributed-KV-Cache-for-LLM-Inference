# ADR 0013 — Consistency boundary: eventual cache data, linearizable metadata

- **Status:** accepted (conceptual; enforced from Phase 2/3)
- **Date:** 2026-05-24 (architecture session, Session 2)
- **Deciders:** HC

## Context

A distributed cache has two kinds of state with very different correctness needs: the cached KV
data, and the metadata describing who owns which shard. See plan §7 (interview beat: "consistency
model") and `docs/01-architecture.md`.

## Decision

- **Cache data is eventually consistent.** A stale or missing entry just causes a recompute —
  exactly what a no-cache system does. So replication can be async and reads can be stale.
- **Metadata is linearizable.** Shard-ownership and membership go through etcd (Phase 2+). Stale
  metadata sends a write to the *wrong* shard, which silently corrupts the cache — not tolerable.

## Alternatives considered

- **Strong consistency for cache data too** (quorum reads/writes) — needless cost for data whose
  loss is harmless. Rejected.
- **Best-effort metadata** — risks silent corruption; rejected.

## Consequences

- Phase 1 (single node) has no metadata layer yet, but the design assumes this split now so
  Phase 2/3 slot in cleanly.
- Justifies async replication (Phase 3) and etcd leases for ownership/leader election.
- This is a deliberately defensible interview talking point, not an accident.
