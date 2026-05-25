# ADR 0002 — etcd for the metadata layer (not a hand-rolled consensus)

- **Status:** accepted
- **Date:** 2026-05-24 (confirmed with HC in Session 1)
- **Deciders:** HC

## Context

The metadata layer tracks shard ownership, node health, and replication topology, and provides
leader election. This must be strongly consistent (stale metadata silently corrupts the cache).
Building a correct consensus implementation is a multi-week effort. See `00-project-plan.md` §5.

## Decision

Use **etcd** for metadata and leader election. Do not implement our own Raft for this project.

## Alternatives considered

- **Build a small Raft library** — adds 4–6 weeks, pushes the project past 20 weeks, and the
  marginal resume value is low. etcd is itself Raft, so consensus is still interview-defensible.
  Better as a separate side project if HC wants the learning.

## Consequences

- Saves weeks; lets the effort go into the cache layer (the actual differentiator).
- Introduces an operational dependency: etcd must run on-demand (never Spot) and ideally 3-node
  for a real quorum — see ADR 0009.
- Leader election uses etcd **leases** (Phase 3); split-brain handling is tested by the chaos harness.
