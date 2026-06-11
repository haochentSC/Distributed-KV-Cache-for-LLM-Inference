# Phase 4.5 — distributed GPU TTFT + on-cluster validations (the paid window, executed 2026-06-10/11)

The single paid AWS window from `aws-batch-runbook.md`, executed end-to-end. Total wall time
≈ 4.5 h (≈ 2 h of it debugging real failures — see Findings), total spend ≈ **$2–3**
(GPU ≈ 50 min of g6.2xlarge Spot; the rest t3.small).

## Environment (as actually run — differs from the runbook defaults in two ways)

- **GPU: `g6.2xlarge` (1× NVIDIA L4 24 GB), not `g5.2xlarge` (A10G).** g5.2xlarge Spot had
  **zero capacity in every us-east-1 AZ** at run time (placement score 1/10 region-wide after
  two `InsufficientInstanceCapacity` failures); g6.2xlarge scored 3/10 and launched first try.
  Same VRAM class, same 8-vCPU G/VT Spot quota, same benchmark semantics (pinned host memory,
  un-throttled KV budget: **4,621 blocks ≈ 74k tokens** vs the throttled laptop's fraction).
- **GPU in us-east-1b, cluster in us-east-1a** (its own `aws_subnet.gpu`; Spot capacity is
  AZ-dependent). Cross-AZ adds <1 ms to every cache RPC — a conservative bias *against* the cache.
- Cache cluster: 3× t3.small Spot (`c7i.large` was reclaimed-for-capacity within minutes — see
  Findings), `cache_max_bytes = 1.0 GB`, `GOMEMLIMIT=1400MiB`, RF=2, vLLM 0.22.1,
  Qwen/Qwen2.5-7B-Instruct bf16, `--tensor-parallel-size 1` (`tp_world=1` keys ≡ ADR 0031).

## Headline: TTFT vs shared-prefix length (system_prompt workload, p50 of 8 runs)

| repeats | prompt tokens | baseline p50 | warm p50 | warm vs baseline |
|---:|---:|---:|---:|---:|
| 4  | 269   | 104.4 ms  | 110.0 ms | **-5.3 %** |
| 8  | 525   | 171.6 ms  | 172.5 ms | **-0.5 %** |
| 16 | 1,038 | 282.4 ms  | 283.0 ms | **-0.2 %** |
| 32 | 2,062 | 547.8 ms  | 506.3 ms | **+7.6 %** |
| 64 | 4,110 | 1,070.3 ms | 954.1 ms | **+10.9 %** |

(JSON: `phase45-distributed-qwen7b.json`, `phase45-distributed-qwen7b-long.json`.)

**The crossover ADR 0031 predicted is now measured on real hardware.** Prefill grows
super-linearly with prefix length while the cache fetch grows linearly, so the cache loses a
few ms at trivial prefixes (fixed RPC overhead), breaks even ≈ 1k tokens, and wins double
digits where prefix reuse actually matters — **116 ms saved off a 1,070 ms TTFT at 4k tokens**,
over a cross-AZ link to a t3.small. `[kvc] save/load path active` both observed; zero
correctness warnings.

## Real-workload replay (ShareGPT, the dataset vLLM's own benchmarks use)

2,000 multi-turn conversations → 6,782 requests (avg 825 tokens), tokenized with tiktoken,
replayed via `loadgen -trace` against the live 3-node cluster:
**32.7 % block hit rate** from genuine multi-turn prefix reuse, 58 req/s, 0 errors,
**0 violations**, p50/p95/p99 = 62/151/189 ms, traffic balanced 37/31/32 % across shards.

## On-cluster validations

- **Verify gate:** gentle (conc 2) and full-strength (conc 8, 60 s) runs both **0 violations**
  (4,626 and 8,303 requests).
- **Chaos, with `loadgen -verify` running through each fault — all 0 violations:**
  100 ms egress latency (8,312 req); 30 s etcd partition → lease expiry, ring rotation,
  rejoin (11,888 req); **real node termination** → failover to the 2-node ring (7,165 req).
- **CloudWatch:** `kvcache-cache-2-unhealthy` → **ALARM** on the killed node, after fixing
  `treat_missing_data` (see Findings); healthy nodes stayed OK.
- **5a/5b eviction on the cluster** (same workload/floors/seed as the local 5b sweep, one seed):

  | config | overall hit | min tenant | local equivalent |
  |---|---:|---:|---|
  | LRU baseline | 15.1 % | 10.6 % | — |
  | GDSF static-cap | 12.0 % | 10.1 % | caps tax efficiency ✓ |
  | elastic w=0 | **21.6 %** | 2.5 % | 20.0 % / 1.9 % ✓ |
  | elastic w=0.5 | 13.9 % | **11.1 %** | ~14 % / ~12 % ✓ |

  **The cluster reproduces the local Pareto frontier** — efficiency corner and fairness
  plateau both land within ~1.5 points of the laptop numbers.
- **Cold tier:** spill→S3→read-through round-trip works (S3 grew by ~2,100 objects; reads
  served from S3 after eviction; 0 violations). **But** the bulk-eviction case sheds most
  spills — see Findings.

## Findings (each cost real debugging time; each is now fixed in code or documented)

1. **Container had no AWS region → every cold-tier S3 call failed** (`Invalid region`), and the
   per-eviction error spam through the blocking `awslogs` driver wedged whole nodes (SSH banner
   timeouts included). The wrapper passed `awslogs-region` to the log *driver* but nothing to the
   *app*. Fix: `-e AWS_REGION` in `userdata/cache.sh.tftpl`.
2. **`cache_max_bytes=1.5 GB` on a 2 GB t3.small OOMs under write load** (Go RSS ≈ 1.5–2× the
   store cap during GC). Fix: 1.0 GB + `GOMEMLIMIT=1400MiB` in the unit. After both fixes the
   same node took the full-strength verify that previously wedged it.
3. **Liveness alarms silently ignored terminated nodes**: `StatusCheckFailed` emits *no*
   datapoints once an instance is gone, and the default `treat_missing_data="missing"` keeps the
   alarm OK through a node loss. Fix: `treat_missing_data = "breaching"` (observed flipping the
   killed node's alarm to ALARM within ~2 min).
4. **Spill pipeline is throughput-bound under burst eviction** (by-design drop-over-stall,
   ADR 0013/0027): 4 workers ≈ 40–60 S3 PUT/s on t3.small vs watermark-burst evictions of
   hundreds/s → ~2,000 of 4,300 blocks' spills shed (logged + counted; never a violation).
   Documented limitation; future work: deeper queue / more workers / paced eviction.
5. **Spot capacity is a real failure domain, not a checkbox**: c7i.large fulfilled after 12–13 min
   then reclaimed (`instance-terminated-no-capacity`) within minutes; g5.2xlarge unavailable
   region-wide. Check `get-spot-placement-scores` before picking types/AZs; the GPU subnet is now
   AZ-independent (`gpu_az`).
6. **Operational:** don't kill a slow `terraform apply` — on Windows the child `terraform.exe`
   survives the wrapper and keeps writing state (two orphans raced ours; required
   `state rm` + `import` surgery). Slow Spot fulfillment looks identical to a hang.
