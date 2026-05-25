---
name: eviction-policy-gdsf-drf
description: Deep dive on the project's headline differentiator — the multi-objective cache eviction policy combining cost-aware GDSF value with DRF-style per-tenant fairness, the work-conserving/elastic design, the fairness_weight knob, and the efficiency-vs-fairness tradeoff curve. Use in Phase 4 (the swappable eviction interface + LRU baseline) and Phase 5 (build the policy), not before.
---

# Multi-objective eviction policy (GDSF + DRF) — deep dive

The project's headline intellectual contribution. Full rationale in `docs/00-project-plan.md` §3.5
and `docs/adr/0007`. **Gated on the Phase 4 "core ships" milestone — do not start before it.**
This is **[guided]** work (the policy engine is HC's to implement).

## The core tension (lead with this when teaching)
Cost-aware eviction keeps entries by `recompute_cost × reuse_probability` — it's **globally
greedy**, so in a multi-tenant cluster it **starves** tenants whose prefixes are cheap to
recompute. Pure efficiency and fairness genuinely conflict. The contribution is one policy engine
that maximizes value **subject to** a per-tenant fairness floor, with a knob between the extremes.

## The two inputs
- **GDSF value (cost-aware):** `priority ≈ L + (access_count × recompute_cost) / size_bytes`, where
  `L` is an aging offset so once-valuable stale entries don't pin forever. Evict lowest priority.
- **DRF / max-min fairness:** each tenant has a guaranteed minimum share; above the floor, capacity
  is loaned to the highest-value entries (work-conserving). Under contention, a tenant below its
  floor reclaims by evicting the *globally* lowest-value entry among tenants *above* their floor.
- **The knob:** `fairness_weight ∈ [0,1]` interpolates pure GDSF (0) ↔ strict max-min (1).

## Critical design decision
**Work-conserving/elastic, NOT static partitions.** Static quotas waste idle capacity and remove
the tension entirely (nothing to defend in an interview). The elastic reclaim-under-contention
design is where the engineering and the story live.

## Scoping (so it can't become a scope bomb)
- **5a (must-ship):** per-tenant accounting + static quota + GDSF within each quota.
- **5b (stretch, the real story):** elastic reclaim + the knob + the tradeoff curve.
- **No third objective** (no prefetching/compression/cross-model) — that's scope creep.

## The payoff artifact
Sweep `fairness_weight` 0→1; plot efficiency (recompute saved / aggregate hit rate) vs fairness
(min tenant hit rate, or per-tenant variance). A Pareto frontier with a tunable operating point —
a stronger interview prop than a single speedup number.

## Prereqs from earlier phases
- Phase 4 LRU+TTL baseline behind a **swappable eviction-policy interface** (we measure against it).
- Data model fields `tenant_id`, `recompute_cost`, `access_count` (already in the plan §3 schema).
- Load generator extended to ≥3 tenants with distinct profiles (cheap/frequent, expensive/rare,
  bursty) — the most under-estimated cost; without it there's no fairness result to show.
