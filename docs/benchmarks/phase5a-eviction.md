# Phase 5a — Cost-aware (GDSF) + per-tenant-quota eviction benchmark

Status: **complete (local).** Demonstrates the efficiency-vs-fairness tension (plan §3.5, ADR 0007,
ADR 0029) on a single bounded shard driven by the multi-tenant load generator. No GPU / no AWS.

## Environment

- Single `cache-server` on `localhost:50051`, `-max-bytes 16777216` (16 MiB ≈ 256 × 64 KiB blocks),
  default watermarks (hi 0.90 / lo 0.75).
- `loadgen -multitenant` (3 tenants, distinct profiles — see `cmd/loadgen/main.go:buildTenants`):
  - **A** cheap/short/frequent — pool of 40 prefixes × 2 blocks (80 hot blocks), prefix-share 0.95, weight 0.55, cost 32.
  - **B** expensive/long/rare — pool of 20 prefixes × 16 blocks (320 hot blocks), prefix-share 0.80, weight 0.30, cost 256.
  - **C** bursty — pool of 12 prefixes × 6 blocks (72 hot blocks), prefix-share 0.70, weight 0.15, cost 96, active 2 s of every 6 s.
- The union of hot blocks (~472 × 64 KiB ≈ 29 MiB) **oversubscribes** the 16 MiB cache, so the
  policy must choose *whose* reusable blocks to keep — that contention is what makes the policy
  matter (with a single hot prefix per tenant the hot set fits and hit rate is profile-determined).
- `-payload-bytes 65536 -concurrency 8 -requests 800 -tail-blocks 2 -seed 7` (≈6,400 requests).

## Commands

```bash
# LRU baseline
cache-server -addr :50051 -eviction lru  -max-bytes 16777216
# GDSF, no fairness (pure cost-aware)
cache-server -addr :50051 -eviction gdsf -max-bytes 16777216
# GDSF + static per-tenant quotas (A=6 MiB, B=7 MiB, C=3 MiB; Σ = 16 MiB)
cache-server -addr :50051 -eviction gdsf -max-bytes 16777216 \
  -tenant-quota "A=6291456,B=7340032,C=3145728"

# same driver for all three:
loadgen -members localhost:50051 -multitenant -payload-bytes 65536 \
  -concurrency 8 -requests 800 -tail-blocks 2 -seed 7
```

## Results (block hit rate; seed 7, 2026-06-07)

| Config              | Overall (efficiency) | A (cheap) | B (expensive) | C (bursty) | min-tenant (max-min fairness) |
|---------------------|:--:|:--:|:--:|:--:|:--:|
| LRU baseline        | 17.6% | 5.6%  | 23.8% | 2.8%  | 2.8%  |
| GDSF (no quota)     | **21.5%** | 3.1%  | 30.8% | 3.4%  | 3.1%  |
| GDSF + quotas       | 12.4% | **16.9%** | 10.5% | 13.0% | **10.5%** |

0 correctness violations in all three runs (ADR 0016 holds under eviction).

## Reading

- **GDSF is the efficiency endpoint.** Cost-aware valuation keeps B's high-`recompute_cost` blocks,
  lifting aggregate recompute-saved (17.6% → 21.5% vs LRU) — but it is *globally greedy*, so it
  starves the cheap tenant even harder than LRU did (A 5.6% → 3.1%) while B hoards (23.8% → 30.8%).
- **GDSF + quotas is the fairness endpoint.** Static per-tenant floors stop B from borrowing A's and
  C's capacity, so the min tenant's hit rate jumps (3.1% → 10.5%) and the cheap tenant A recovers
  5.5× (3.1% → 16.9%). The price is aggregate efficiency (21.5% → 12.4%) — exactly the tension.
- These are the **two endpoints** of the `fairness_weight` knob. Phase 5b makes the quotas elastic
  (work-conserving) and sweeps the knob to fill in the **efficiency-vs-fairness Pareto frontier**
  between them; this 5a run is the must-ship proof that the tension is real and the policy controls it.

## Caveats / honesty

- Static quotas here are also caps: B's quota (7 MiB) is far below its 20 MiB hot set, so B is held
  down hard. That is a deliberately fairness-favouring operating point, not "the" answer — it is one
  point on the curve 5b will draw.
- Per-tenant hit rate is measured **client-side in loadgen**, not via server metrics: ADR 0025
  budgets label cardinality and excludes `tenant_id`. The tenant set is small and known, so a bounded
  server-side label would be defensible, but the loadgen already knows tenant + outcome per request,
  so that is the cheaper, cardinality-free place to measure it.
- Single shard, local, CPU-only. Re-running across the 3-node AWS cluster is batched with the
  deferred Phase-4 AWS verification window (cold-tier round-trip, AWS chaos, CloudWatch alarms).
