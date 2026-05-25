# ADR 0006 — Run cache nodes on EC2 Spot

- **Status:** accepted
- **Date:** 2026-05 (cloud integration)
- **Deciders:** HC

## Context

Cost discipline matters for a multi-week project. Losing a cache entry is cheap (it just causes a
recompute), so cache nodes tolerate interruption. See `00-project-plan.md` §5, §6.

## Decision

Run **cache nodes on EC2 Spot**. (etcd and anything stateful-coordination stays on-demand — see
ADR 0009.)

## Alternatives considered

- **All on-demand** — simpler but ~60–70% more expensive for the continuously-running cache tier,
  and misses a free chaos source.

## Consequences

- ~60–70% cheaper cache tier.
- Spot reclamation (with its ~2-minute warning) doubles as **authentic, unscheduled failure
  events** that exercise the Phase 3 graceful-drain path for free.
- The graceful-drain logic must subscribe to the Spot interruption notice (Phase 3).
