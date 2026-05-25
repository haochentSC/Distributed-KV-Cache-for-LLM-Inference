# ADR 0007 — Multi-objective eviction policy (cost-aware GDSF + DRF-style fairness)

- **Status:** accepted (the headline differentiator; built in Phase 5, gated on Phase 4)
- **Date:** 2026-05 (differentiator)
- **Deciders:** HC

## Context

The distributed KV cache space is crowded (LMCache, NVIDIA Dynamo, Mooncake). A resume/interview
artifact needs one non-obvious, fully-built insight, not feature count. See `00-project-plan.md`
§3.5.

## Decision

Build **one admission/eviction controller** that maximizes total cache value (cost-aware,
GDSF-style: `priority ≈ L + access_count × recompute_cost / size_bytes`) **subject to a per-tenant
max-min fairness floor**, with a `fairness_weight ∈ [0,1]` knob interpolating efficiency ↔
fairness. Fairness is **work-conserving/elastic**, not static partitions.

Scoped to ship partially: **5a** (per-tenant accounting + static quota + GDSF) is must-ship; **5b**
(elastic reclaim + the knob + tradeoff curve) is the stretch story.

## Alternatives considered

- **Plain LRU/LFU** — the Phase 4 baseline we measure against; not the differentiator.
- **Cost-aware only** — globally greedy; starves cheap-to-recompute tenants.
- **Static per-tenant partitions** — wastes idle capacity and removes the tension entirely
  (nothing interesting to defend). Rejected as the target design.
- **A third objective** (prefetching, compression, cross-model sharing) — scope creep; explicitly
  out (belongs in the "what I'd do next" interview answer).

## Consequences

- Adds `tenant_id`, `recompute_cost`, `access_count` to the data model, and a multi-tenant
  workload to the load generator (the most under-estimated cost).
- The payoff is an efficiency-vs-fairness **tradeoff curve** — a stronger interview prop than a
  single speedup number.
- Must sit behind the swappable eviction-policy interface introduced in Phase 4.
