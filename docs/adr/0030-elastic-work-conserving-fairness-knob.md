# ADR 0030 — Elastic work-conserving fairness + the `fairness_weight` knob (Phase 5b)

- **Status:** accepted
- **Date:** 2026-06-07 (Phase 5b — the differentiator's second half: the tradeoff curve)
- **Deciders:** HC (+ Claude)
- **Builds on:** ADR 0007 (the differentiator + the `fairness_weight` knob), ADR 0029 (GDSF +
  static quotas, the 5a seam), ADR 0024 (the eviction seam), ADR 0016 (correctness invariant)

## Context

ADR 0029 shipped the two *endpoints* of the differentiator: pure cost-aware GDSF (efficiency) and
static per-tenant caps (fairness), and showed they trade off. But the 5a caps are **not
work-conserving** — `OverQuota()` is an independent eviction trigger, so a tenant over its cap is
reclaimed even when the cache has free space, leaving capacity idle. ADR 0007 promised a single knob
`fairness_weight ∈ [0,1]` sweeping *between* the endpoints, with quotas as elastic **floors** (borrow
spare capacity, reclaim under contention) rather than hard caps. Phase 5b builds exactly that and
sweeps the knob into the efficiency-vs-fairness Pareto frontier.

## Decision

**1. Quotas become elastic floors via a new policy mode, not a rewrite.** A second constructor
`NewGDSFElastic(quotas, fairnessWeight)` sets `elastic=true` on the *same* `GDSFPolicy` (per-tenant
heaps, value function, lock discipline all unchanged). `NewGDSF` (static caps, 5a) is untouched and
still selectable. One concrete type, two modes — selected by `-eviction gdsf | gdsf-elastic`.

**2. Work-conserving = elastic `OverQuota()` returns `false`.** The Store already gates extra
eviction on `QuotaPolicy.OverQuota()` (`signalEvict`/`needsEviction`). Returning `false` in elastic
mode makes eviction **watermark-only** — a tenant freely borrows idle capacity and is squeezed only
under genuine global pressure. No Store change: the *same* optional-capability seam expresses both
"hard cap" (static, OverQuota can fire) and "elastic floor" (work-conserving, it never fires).

**3. Fairness moves into victim selection, governed by one scalar knob.** Under pressure,
`victimElastic` evicts the block minimising a fairness-adjusted priority:

```
H_eff(block) = H(block) / (1 + w · overage_t),   overage_t = max(0, bytes_t/floor_t − 1)
```

A tenant at/below its floor (overage 0) keeps its true GDSF priority — protected; a tenant above its
floor is discounted toward 0 — evicted first. `w=0` ⇒ no discount ⇒ exactly pure global GDSF (the
efficiency endpoint); larger `w` ⇒ stronger max-min bias.

**4. The discount is a per-tenant scalar, deliberately — it preserves the O(#tenants) selection.**
A scalar multiplier preserves each tenant's intra-heap order, so the lowest-`H_eff` block of a
tenant is still its heap root. Victim selection stays a peek of each tenant's root — no global
re-sort, heaps unchanged. The aging floor `L` advances by the victim's **true** priority, not the
discounted score: the discount steers *which tenant pays*, it must not corrupt the global GreedyDual
value scale.

**5. A tenant with no configured floor has no fairness guarantee** (`overage = maxOverage`): its
blocks are reclaimed before any floored tenant's. This keeps the policy total-ordered and makes
"configure a floor to get protection" the explicit contract.

**6. The knob is swept offline to produce the curve** (`scripts/phase5b-sweep.ps1`,
`docs/benchmarks/phase5b-eviction.md`), averaged over 3 seeds to remove concurrency jitter. No
runtime auto-tuning — the operator picks the operating point.

## Alternatives considered

- **Additive / rank-normalised blend** `(1−w)·rank_H + w·rank_overage`. More *linearly* responsive
  (the multiplicative discount saturates — see consequences), but needs a global cross-tenant
  normalisation each eviction (O(n) or a second index), losing the per-tenant root-peek. Rejected
  for 5b; noted as the refinement if the knob's response curve matters.
- **Keep caps, add a separate "soft cap" flag.** Two fairness mechanisms to reason about and
  document. Rejected: one knob with `w=0` reproducing pure GDSF and the static policy retained as a
  reference point is cleaner.
- **Auto-tune `w` from observed unfairness.** A control loop on top of the knob — out of scope and
  premature without the manual curve first. Explicitly deferred.
- **A third objective** (latency/SLO-aware) — out of scope by ADR 0007.

## Consequences

- **The Pareto frontier is drawn** (`phase5b-eviction.md`): `w=0` = efficiency corner (20.0% overall
  / 1.9% min-tenant), and `w>0` trades ~6 pts of overall for a 6× min-tenant gain (→12.3%), pulling
  the hoarding tenant down and rescuing the cheap one. 0 correctness violations throughout.
- **Work-conserving Pareto-dominates static caps.** Elastic `w=0.25` (14.4% / 12.3%) beats the 5a
  static-cap point (12.2% / 10.3%) on *both* axes — not leaving capacity idle is a free lunch. This
  is the concrete payoff of the elastic floor over the hard cap.
- **The knob saturates** — an honest design finding. The whole transition is in `w ∈ [0, 0.25]`;
  `w ∈ [0.25, 1]` is a plateau, because once the discount reorders victims toward over-floor tenants,
  more `w` barely changes the ordering. So in practice this is closer to off/on than a smooth dial.
  The additive-blend alternative above is the fix if finer control is wanted.
- **Seam cost: zero in the Store.** Both fairness regimes ride the ADR 0029 `QuotaPolicy` seam; the
  only new surface is `-eviction gdsf-elastic` + `-fairness-weight`. Defaults (`lru`) reproduce
  Phase 4 exactly; `gdsf` reproduces 5a exactly.
- **Deferred:** a finer sweep of the `[0,0.25]` knee, the additive blend, auto-tuning, floors with
  `Σ < maxBytes` (unclaimed headroom), and the AWS-cluster re-run (batched with the Phase-4 AWS
  window). The GPU/TTFT track (Phase 4.5) is unaffected.
