# Phase 4.5-B — RunPod GPU-cloud window (Session A executed 2026-06-11; Session B pending)

The RunPod paid window from [`runpod-runbook.md`](runpod-runbook.md) / [`runpod-gpu-window-plan.md`](runpod-gpu-window-plan.md).
**Session A** (1× A100 80 GB, long-context curve + serving demo) ran end-to-end. **Session B**
(4× A6000/A40, TP=4 / Qwen2.5-32B keying validation) is the next session — see the handoff section
at the bottom of the plan doc.

Total Session A wall time ≈ **2 h** on pod `fu7bdllghlfssu`; spend ≈ **$3–4** (A100 secure cloud,
on-demand). **Terminate the pod in the RunPod console when done** — `runpodctl` was not configured
on the box.

## Environment (Session A — as actually run)

- **GPU:** NVIDIA A100-SXM4-80GB (81920 MiB), Secure Cloud, RunPod PyTorch template.
- **Access:** Direct TCP SSH `154.54.102.45:18848` (proxy `ssh.runpod.io` is terminal-only — no
  `scp`). Windows laptop: OpenSSH on user PATH; key `~/.ssh/id_ed25519_runpod`.
- **Cache topology:** single `cache-server` on **loopback** (`127.0.0.1:50051`, `-max-bytes 32 GiB`).
  Deliberate control: the *distributed* story is already proven on AWS (ADR 0028 / `phase45-distributed-gpu.md`);
  loopback isolates long-context transfer vs prefill without cross-AZ noise.
- **Software:** vLLM **0.22.1** (`pip install --break-system-packages` — PEP 668 on the pod image),
  connector `pip install -e /root/connector`, probe gate `block_axis=1`, `tp_world=1`.
- **Driver:** `--max-model-len 32768`, `--deadline-ms 15000` (pre-flight patch — default 2000 ms would
  silently time out ~2 GB warm fetches). Top repeat rung **504** (not 512): verified 32,271 tokens ≤
  32,768 context (`64 tok/repeat + 15-tok suffix`).

## Headline: long-context TTFT curve (Qwen2.5-7B, system_prompt, p50 of 8 runs)

| repeats | prompt tokens | baseline p50 | warm p50 | warm vs baseline |
|---:|---:|---:|---:|---:|
| 16  | 1,038  | 75 ms   | 204 ms  | **−171 %** |
| 32  | 2,062  | 136 ms  | 389 ms  | **−186 %** |
| 64  | 4,110  | 273 ms  | 766 ms  | **−181 %** |
| 128 | 8,207  | 565 ms  | 1,507 ms | **−167 %** |
| 256 | 16,399 | 1,274 ms | 3,015 ms | **−137 %** |
| 504 | 32,271 | 3,039 ms | 5,808 ms | **−91 %** |

(JSON: [`phase45-longcontext-qwen7b.json`](phase45-longcontext-qwen7b.json).)

**No crossover on the A100 within 32k context.** The deficit *monotonically shrinks* with prefix
length (−171% → −91%) but never crosses zero. Compare AWS L4 @ 4k tokens: baseline 1,070 ms,
warm 954 ms (**+10.9%**). On the same 4k rung here: baseline **273 ms**, warm **766 ms** — the A100
prefills ~4× faster while the warm path stays Python/deserialize/copy bound (~300 MB/s effective).

`[kvc] save/load path active` on every sweep; zero correctness warnings.

## Scaling check: Qwen2.5-14B (KV-bytes/FLOPs ratio, not “bigger model helps”)

| repeats | prompt tokens | baseline p50 | warm p50 | warm vs baseline |
|---:|---:|---:|---:|---:|
| 64  | 4,110  | 539 ms  | 1,993 ms | **−270 %** |
| 256 | 16,399 | 2,589 ms | 7,699 ms | **−197 %** |

(JSON: [`phase45-longcontext-qwen14b.json`](phase45-longcontext-qwen14b.json).)

At the **same** 4k-token prefix, 14B is *worse* than 7B (−270% vs −181%) because Qwen2.5-14B
carries ~3.4× the KV bytes per token (48 layers × 8 KV heads vs 28 × 4) for only ~2× the prefill
FLOPs. This **refines ADR 0031**: the crossover is driven by **KV-bytes / recompute-cost**, not
model size alone.

## Serving demo (`vllm serve` + connector, repeats=64 ≈ 4k tokens, p50 TTFT)

Recorded: [`runpod-demo-serve.typescript`](runpod-demo-serve.typescript). Integration artifact —
connector works under the OpenAI-compatible server; TTFT numbers are **not** the resume headline.

| config | TTFT p50 |
|---|---:|
| baseline (no connector) | **283 ms** |
| external-cold | **770 ms** |
| external-warm | **768 ms** |

`[kvc] save/load path active` under serve; byte-identical outputs across requests. Framing: loopback
= transport best case; AWS cross-AZ L4 = conservative bound for where the cache *wins*.

## Resume / interview framing (honest)

- **Measured TTFT win (keep):** AWS distributed run, L4, cross-AZ → **+10.9% @ 4k tokens**
  (`phase45-distributed-gpu.md`). That is the `[measured]` headline.
- **RunPod contribution (keep):** boundary analysis — crossover requires **compute-bound prefill**
  (cost-tier GPU), not flagship throughput; 14B falsifies naive “bigger model ⇒ cache wins”; serving
  demo proves end-to-end connector integration.
- **Do not claim:** “20–40% @ 32k on A100” — the plan’s optimistic extrapolation did not materialize.

## Findings (Session A)

1. **PEP 668 on RunPod PyTorch image:** `pip install vllm` needs `--break-system-packages` (or a venv).
2. **Proxy SSH vs Direct TCP:** `ssh.runpod.io` works for shells only; benchmark bundle upload requires
   Direct TCP Ports + `scp -P <port>`.
3. **Silent failure mode confirmed in the wild:** without `--deadline-ms 15000`, long warm fetches would
   degrade to recompute with zero errors — the pre-flight patch was load-bearing.
4. **A100 prefill is too fast for this connector path** at 7B/14B; the warm bottleneck is the Python
   deserialize + GPU copy hot path (same family as ADR 0031), not gRPC RTT on loopback.

## Next session (Session B — handoff)

See [`runpod-gpu-window-plan.md`](runpod-gpu-window-plan.md) § “Next session handoff”. Deliverable:
`phase45-tp4-qwen32b.json` + TP=4 probe dump proving distinct `shard_model_id` per rank (ADR 0032).
**Terminate the Session B pod immediately after.**
