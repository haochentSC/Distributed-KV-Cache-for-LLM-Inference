# ADR 0033 — Single-GPU AWS benchmark default after partial GPU-quota approval; the "why AWS" scope

- **Status:** accepted
- **Date:** 2026-06-10
- **Deciders:** HC (+ Claude)
- **Supersedes the scope of:** ADR 0032 (TP=4 / 30B as the headline) — the TP path is kept in code
  but demoted to a deferred option.
- **Builds on:** ADR 0031 (single-node TTFT; deficit is environmental and closes with model size),
  ADR 0028 (AWS cluster), ADR 0006/0009 (Spot workers, on-demand etcd).

## Context

The paid GPU window (ADR 0032) targeted a 30B model with tensor parallelism (TP=4) on `g5.12xlarge`
(4× A10G, 48 vCPUs). Two AWS account gates surfaced when we tried it (2026-06-09):

1. **Free Plan blocks GPU.** A new account defaults to the Free Plan, which only launches
   free-tier-eligible types and rejected the GPU with `InvalidParameterCombination: not eligible for
   Free Tier`. HC upgraded to a Paid Plan.
2. **G/VT Spot vCPU quota.** New accounts start at 0. HC requested 48 (case 178103325100884); AWS
   **partially approved to 8 vCPUs**, stating 8 is the **standard-process ceiling for GPU** and that
   anything beyond requires AWS sales / account-team engagement. HC chose not to pursue sales.

8 vCPUs = exactly one **single-GPU** instance (`g5.2xlarge`, 1× A10G, 24 GB). TP=4 needs 48 and is
therefore not reachable on the standard path.

Separately, HC asked the sharper question: *why AWS at all, and is it just resume keywords?* The
honest position we settled on (recorded here because it is the project's narrative, not just a config
choice): AWS is **not** justified by traffic scale — this is a *coordination-under-failure* project,
not a high-QPS one. It **is** justified by what the thesis requires and what we actually built:
real independent failure domains for failover, **Spot reclaim as an authentic chaos signal** (cache
on Spot, etcd on-demand — ADR 0009), **S3 as the cold tier** (ADR 0027), and end-to-end
**IaC/IAM/observability operational maturity**. GPU *compute*, by contrast, is not an AWS-shaped
problem; a GPU-specialized cloud is the right tool there. That "right tool per job" split is the
defensible answer, not a retreat.

## Decision

1. **Default the GPU benchmark to single-GPU.** `gpu_instance_type` defaults to `g5.2xlarge`
   (1× A10G); the distributed driver defaults to `--tensor-parallel-size 1` and a 7-8B model
   (`Qwen/Qwen2.5-7B-Instruct`, bf16 fits 24 GB; ~13-14B needs quantization). At `world = 1` the
   connector keys are byte-identical to ADR 0031 (`shard_model_id` returns the bare `model_id`), so
   the result is directly comparable. A real A10G with pinned memory and an un-throttled KV cache is
   exactly the environment ADR 0031 predicted would flip the cache from net-negative to net-positive
   — this single-GPU run is the legitimate headline, not a consolation.

2. **Keep the TP=4 path in code, demoted to a deferred "Option B."** No TP code is removed
   (`shard_model_id`, the probe's `tp_rank/tp_world`, `gpu.tf`'s overridable `gpu_instance_type`).
   TP=4 / 30B is reachable two ways: a 48-vCPU quota via AWS sales, **or** a GPU-specialized cloud
   (Lambda/RunPod/Modal) — the connector's TP keying (ADR 0032) is provider-agnostic, so it runs
   there unchanged. The runbook flags both `g5.12xlarge` + `--tensor-parallel-size 4` variants.

3. **Record the "why AWS" narrative as a project artifact** (Context above) so the deployment reads
   as a deliberate engineering choice — failure domains + Spot-chaos + S3 + IaC — rather than keyword
   collection.

## Alternatives considered

- **Pursue 48 vCPUs via AWS sales.** Rejected: slow, high-friction, and unnecessary — the single-GPU
  result is a valid headline and the TP artifact survives on a GPU cloud.
- **Run TP=4 entirely on a GPU-specialized cloud now.** Viable and still open as Option B, but adds a
  second provider before the simpler single-GPU AWS number is even captured. Sequenced after, not
  instead.
- **Drop the cloud GPU run entirely** (lean on local ADR 0031 + the CPU AWS cluster). Rejected as the
  default: it forgoes the one number — a positive cloud TTFT on pinned memory — that the whole GPU
  thread was for.

## Consequences

- The headline becomes a **single-GPU 7-8B A10G TTFT** number; cheaper (~$2-6 for a 2-3 hr Spot
  window vs. $6-20) and unblocked on the existing 8-vCPU quota.
- The TP=4 / KV-head-sharding engineering (ADR 0032) is **demonstrated-in-code and deferred**, not
  abandoned; it ships if/when a 4-GPU box is available.
- The runbook gains an "Account prerequisites & GPU scope" section (Paid Plan + 8-vCPU cap) plus a
  `t3.small` load-host lesson (run `loadgen` from a peer node, not the node-under-test or etcd; prefer
  `c7i.large`) learned in the 2026-06-09 attempt.
- Server, proto, ring, and Go `blockhash` remain untouched (as in ADR 0032).
- **2026-06-11 update (ADR 0034):** RunPod Option B Session A executed (long-context + demo); Session B
  (TP=4 keying) pending. AWS L4 +10.9% @ 4k remains the resume headline; RunPod refines where the
  cache wins/loses (`phase45-gpu-cloud.md`).
