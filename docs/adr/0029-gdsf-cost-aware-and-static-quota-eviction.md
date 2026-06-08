# ADR 0029 — GDSF cost-aware eviction + static per-tenant quotas (Phase 5a)

- **Status:** accepted
- **Date:** 2026-06-07 (Phase 5a — the headline differentiator, must-ship half)
- **Deciders:** HC (+ Claude)
- **Builds on:** ADR 0007 (the differentiator decision), ADR 0024 (LRU baseline + the eviction seam),
  ADR 0016 (correctness invariant), ADR 0025 (metrics cardinality budget)

## Context

ADR 0007 committed to the project's differentiator: one admission/eviction controller that maximizes
cache value (cost-aware, GDSF-style) **subject to** a per-tenant fairness floor, with a
`fairness_weight` knob. Phase 5a is the must-ship half — per-tenant accounting, **static** quotas,
and GDSF cost-aware eviction — measured against the Phase 4 LRU baseline. ADR 0024 left the
`EvictionPolicy` seam deliberately thin and flagged it: `Victim()` carried no tenant identity and no
free-amount, both of which GDSF + quotas need.

## Decision

**1. GDSF value function.** Each block's priority is `H = L + freq·cost/size`:
- `cost` = `recompute_cost` (∝ prefill FLOPs ≈ prefix length); `size` = resident bytes (so cost is
  valued *per byte* — the dimension that dominates for multi-MB KV tensors); `freq` = access count.
- `L` = a GreedyDual aging floor: on eviction `L` advances to the victim's priority, so survivors and
  new blocks are measured against the current floor. This is what lets a once-valuable-but-cold block
  age out (LFU never forgets; LRU ignores cost/size). `L` is monotonically non-decreasing.
- Evict the **lowest** `H`. Degenerate inputs are guarded (`cost<=0`→1, `size<=0`→1).

**2. Per-tenant accounting lives in the policy, not the Store.** The Store keeps owning the global
byte budget (`totalBytes` + watermarks, untouched). `GDSFPolicy` owns per-tenant byte sums + quotas.
This held `store.go`'s change to a one-line metadata forward (`RecordWrite(h,size)` →
`RecordWrite(WriteMeta{Key,TenantID,Size,Cost})`).

**3. Static quotas enforced through the existing single Evictor, made quota-aware — not a second
signalling path.** A new **optional** `QuotaPolicy` interface (`OverQuota() bool`) is type-asserted
by the Store: `signalEvict` (trigger) and the new `needsEviction` (drain-stop) OR-in `OverQuota()`,
and `Victim()` returns the lowest-`H` block **among over-quota tenants first**, falling back to the
global lowest-`H`. LRU and NoopPolicy do **not** implement `QuotaPolicy`, so their behaviour is
byte-identical to Phase 4 (watermark-only).

**4. Per-tenant data structure: one indexed min-heap per tenant.** `container/heap` with an `index`
field per item gives O(log n) write/access/evict and O(#tenants) victim selection (peek each
over-quota tenant's root). A single global heap was rejected — segmenting victims by quota class
would mean pop-and-skip.

**5. Per-tenant hit rate is measured client-side in `loadgen`, not via server metrics.** ADR 0025
budgets label cardinality and excludes `tenant_id`. The benchmark is loadgen-driven and loadgen knows
tenant + outcome per request, so that is the cardinality-free place to measure it.

**6. Lock discipline unchanged.** `GDSFPolicy` mirrors `LRUPolicy`'s contract: one mutex, methods
called under a stripe lock (or no lock) take only the policy lock — never a stripe lock — so the
program-wide order stays stripe → policy. `Victim()` releases before returning. Proven with `-race`
(WSL2).

## Alternatives considered

- **True hard per-tenant caps with a dedicated per-tenant pressure signal** — cleaner "static
  partition" semantics, but a second signalling path through the Store. Rejected: the optional
  `QuotaPolicy` over the existing Evictor is enough and far less invasive. The consequence is that a
  tenant may transiently borrow idle capacity before being reclaimed under contention (see below).
- **Accounting in the Store** — would have spread tenant logic across `store.go` and the policy.
  Keeping it in the policy localised the whole feature.
- **One global heap** — see decision 4.
- **A `tenant_id` Prometheus label** — see decision 5 and ADR 0025.

## Consequences

- **The tension is demonstrated** (`docs/benchmarks/phase5a-eviction.md`): on an oversubscribed
  16 MiB shard, GDSF lifts aggregate hit rate (17.6%→21.5% vs LRU) while starving the cheap tenant
  (5.6%→3.1%); GDSF + quotas reverses that (min-tenant 3.1%→10.5%, cheap tenant 3.1%→16.9%) at an
  efficiency cost (→12.4%). 0 correctness violations throughout (ADR 0016 holds under eviction).
- **Quotas here are static, and also act as caps** — a tenant can borrow idle capacity but is capped
  under contention; sizing `Σ quota ≈ maxBytes` makes the floors bite. This is *one operating point*,
  not the curve.
- **Deferred to Phase 5b:** elastic/work-conserving borrowing, reclaim-fairness under contention, the
  `fairness_weight ∈ [0,1]` knob, and the swept efficiency-vs-fairness Pareto curve. The 5a seam
  (`QuotaPolicy`, the per-tenant heaps, the value function) is built to extend into it. No third
  objective (ADR 0007).
- **Wiring:** `-eviction lru|gdsf` and `-tenant-quota "A=…,B=…"` on `cache-server`; `-multitenant`
  on `loadgen`. Defaults (`lru`, no quotas, single-tenant) reproduce Phase 4 exactly.
