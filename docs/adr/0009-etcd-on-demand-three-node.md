# ADR 0009 — etcd runs on-demand (never Spot); 3-node recommended for v1

- **Status:** accepted direction — Revision C, Session 1. **Standup deferred to Phase 3 (ADR 0018):**
  Phase 2 uses static ring membership; the 3-node on-demand topology below stands and is realized in Phase 3.
- **Date:** 2026-05-24
- **Deciders:** HC

## Context

ADR 0006 puts cache nodes on Spot. The plan elsewhere mentioned "a small etcd instance (or a
3-node etcd)." etcd is the coordination ground truth: if it's lost or split, writes can go to the
wrong shard and silently corrupt the cache. The Phase 3 split-brain / leader-election failover
story (and the matching interview beat) only holds with a real quorum.

## Decision

Run **etcd on on-demand instances (never Spot)**, and use a **3-node etcd** as the v1 default. A
single-node etcd is the documented cheap fallback if cost/time forces it. Final topology decided
at the start of Phase 2.

## Alternatives considered

- **etcd on Spot** — reclamation could take out quorum; rejected (it's the one thing that must not
  be interruptible).
- **Single-node etcd for v1** — cheaper and simpler, but no real quorum, so the failover/split-brain
  story is weaker. Kept only as a fallback.

## Consequences

- Slightly higher fixed cost (3 small on-demand instances) — acceptable; the cache tier is the
  expensive-at-scale part and that stays on Spot.
- Enables an authentic leader-election + lease-expiry demonstration in Phase 3.
