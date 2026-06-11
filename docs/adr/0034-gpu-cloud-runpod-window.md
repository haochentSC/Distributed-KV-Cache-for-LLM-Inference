# ADR 0034 — GPU-cloud window on RunPod (Option B); Session A results + Session B handoff

- **Status:** accepted (Session A executed; Session B pending)
- **Date:** 2026-06-11
- **Deciders:** HC (+ Claude)
- **Builds on:** ADR 0031 (TTFT inequality / environmental bottlenecks), ADR 0032 (TP shard keying),
  ADR 0033 (AWS single-GPU default; TP=4 deferred to GPU cloud)

## Context

AWS Phase 4.5 (ADR 0033) measured the distributed TTFT crossover on a cost-tier GPU (g6.2xlarge 1×
L4, cross-AZ to t3.small cache): **+10.9% @ 4k tokens**. The 8-vCPU G/VT Spot quota blocks TP=4 /
30B on AWS. HC approved **Option B**: RunPod for GPU compute only (AWS remains the distributed-systems
story), expanded to three deliverables: long-context curve, TP=4 keying validation, serving demo.

Pre-flight (commit `ff0feb3`): runbook, `--deadline-ms` + context guard on the driver, TP flags on the
probe, verified repeats ladder (top rung **504** not 512).

## Decision

**Provider: RunPod (Secure Cloud).** Self-serve 1× A100 80GB and 4× A6000/A40 under one account;
Direct TCP SSH for `scp`; PyTorch template + `vllm==0.22.1` pin. Rejected for this project: Modal
(topology fight), Vast.ai (unreliable), CoreWeave (sales-gated). Lambda = solid runner-up.

**Cache on pod: loopback single `cache-server`, no Docker-in-Docker.** Cross-compiled
`bin/cache-server-linux-amd64` + `scp` connector bundle. Framing: distributed correctness already
proven on AWS; RunPod isolates long-context transfer vs prefill and TP keying — not a second
distributed benchmark.

## Session A results (executed 2026-06-11)

Pod `fu7bdllghlfssu`, A100-SXM4-80GB, ~2 h, ~$3–4.

| Deliverable | Outcome |
|---|---|
| Long-context 7B sweep (1k→32k) | **No crossover** within 32k; deficit shrinks −171% → −91% |
| 14B scaling check @ 4k/16k | **Worse than 7B** at same token count (−270% @ 4k) — KV-bytes/FLOPs |
| Serving demo | Connector works under `vllm serve`; TTFT regression on A100 (283 vs 768 ms p50) |
| Probe gate | `block_axis=1`, `tp_world=1` — unchanged |

**Headline refinement:** The plan’s “20–40% @ 32k on A100” extrapolation **did not hold**. Crossover
requires **compute-bound prefill** (L4-class serving GPU), not flagship throughput. The AWS L4
**+10.9% @ 4k** remains the resume `[measured]` number; RunPod adds honest boundary analysis.

Artifacts: `docs/benchmarks/phase45-gpu-cloud.md`, JSONs under `docs/benchmarks/phase45-longcontext-*`,
`runpod-demo-serve.typescript`.

## Session B (pending)

4× A6000 or A40; Qwen2.5-32B-Instruct bf16, `--tensor-parallel-size 4`. Success = probe gate
(`tp_world=4`, 2 KV heads/rank, distinct `shard_model_id` per rank) + driver run with save/load
active and **0 correctness warnings**. TTFT delta is secondary; engineering completeness is the
deliverable (ADR 0032).

Handoff: `docs/benchmarks/runpod-gpu-window-plan.md` § “Next session handoff”.

## Consequences

- **Positive:** Option B executed without AWS quota sales; provider-agnostic connector/driver/probe
  validated on a second cloud; serving demo artifact; falsifies naive scaling claims with data.
- **Negative:** No stronger TTFT number than AWS; interview story must separate “where it wins”
  (cost-tier GPU, long prefix, distributed) from “where it loses” (flagship GPU, Python hot path).
- **Operational:** Terminate pods in console (not stop); `runpodctl` not configured on pod; Windows
  OpenSSH needs Direct TCP + user PATH fix; PEP 668 → `--break-system-packages`.

## Links

- `docs/benchmarks/runpod-runbook.md`, `docs/benchmarks/runpod-gpu-window-plan.md`
- `docs/benchmarks/phase45-gpu-cloud.md`, `docs/benchmarks/phase45-distributed-gpu.md`
- ADR 0031, ADR 0032, ADR 0033
