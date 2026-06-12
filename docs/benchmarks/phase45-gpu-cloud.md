# Phase 4.5-B — RunPod GPU-cloud window (Session A executed 2026-06-11; Session B executed 2026-06-12)

The RunPod paid window from [`runpod-runbook.md`](runpod-runbook.md) / [`runpod-gpu-window-plan.md`](runpod-gpu-window-plan.md).
**Session A** (1× A100 80 GB, long-context curve + serving demo) ran end-to-end. **Session B**
(4× A40, TP=4 / Qwen2.5-32B keying validation) ran 2026-06-12 — and caught a real server-side
keying bug (ADR 0035) before validating clean. See the Session B section at the bottom.

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

## Session B — TP=4 / Qwen2.5-32B keying validation (executed 2026-06-12)

**Environment:** 4× NVIDIA A40 46 GB (PCIe, no NVLink — custom allreduce disabled, NCCL-over-PCIe;
fine, this session validates keying correctness, not TP throughput), Secure Cloud,
**PyTorch 2.9 / CUDA 13.0 template** (see findings — the first pod, on the plain PyTorch template,
had a CUDA 12.7 driver that could not load the cu130 torch that vLLM 0.22.1 pulls in). 100 GB
container disk; conda at `/opt/conda` (no PEP 668 — plain `pip install`). Wall time ≈ 1.5 h
including the bug hunt; ≈ $3–4.

**Probe gate (passed):** `tp_world=4`, ranks 0–3 each dumped once on their own device, per-rank KV
shape `[2, 22900, 16, 2, 128]` = **2 KV heads/rank** (Qwen2.5-32B's 8 ÷ 4), `block_axis=1`
unchanged. Dump: [`runpod-tp4-kv-layout-probe.json`](runpod-tp4-kv-layout-probe.json).

**The gate then caught a real bug.** First benchmark run: save active on all 4 ranks but `load path
active` on rank 0 only; server hit:miss exactly **1:3**; a presence probe showed every block stored
under ONE shard key with versions in multiples of 4. Root cause (server, not connector): the store
map was keyed by **block hash alone** with `model_id` as a guarded metadata field — and ADR 0032
deliberately keeps block hashes rank-agnostic, so all four `model#tpR/4` shard ids collided on the
same map slot, last-writer-wins. Worse than a miss: a rank's fetch could return **another rank's
shard bytes** (the stamped hash matches, so the ADR 0016 guard passes). Fix: namespace the store key
by model (`storeKey = SHA-256(model_id ‖ wire hash)`), keep the wire hash on the entry for
spill/replication. **ADR 0035.** Single-model runs (every prior benchmark) were unaffected.

**Validation on the fixed server (the deliverable):** `[kvc] load path active` on **all four
ranks**; batch_fetch **9,280 hits / 0 misses**; **512 writes = 128 blocks × 4 ranks exactly once**;
the only lookup misses are the cold pass. Zero correctness warnings. The TP keying
(`shard_model_id`, ADR 0032) is validated end-to-end on real tensor-parallel hardware.

TTFT (secondary — A40 prefill is fast and the warm path is the known Python-bound hot path,
ADR 0031): warm vs baseline −40.1% @ 269 tok, −51.6% @ 525, −24.7% @ 1,038, −32.0% @ 2,062.
JSON: [`phase45-tp4-qwen32b.json`](phase45-tp4-qwen32b.json).

### Findings (Session B)

1. **Template CUDA version is a hard gate on RunPod.** vLLM 0.22.1 pulls `torch 2.11.0+cu130`,
   which needs a CUDA ≥ 13.0 *driver* on the host. The plain "RunPod PyTorch" template landed on a
   12.7-driver host (engine init fails: "NVIDIA driver too old"); redeploying with the
   **PyTorch 2.9 / CUDA 13.0** template fixed it. The driver is host-level — not fixable in-container.
2. **SSH key injection is template-dependent**: the CUDA-13 template did not inject the account SSH
   key; it had to be appended to `authorized_keys` via the Web Terminal (which force-wraps pasted
   lines ~80 cols — split long lines into shell variables).
3. **The probe-gate philosophy paid for itself**: the lockstep invariant ("rank-0 presence stands in
   for all ranks") let the scheduler claim hits that three ranks could not serve, silently — only
   the per-rank server metrics (1:3 hit:miss) and a shard-presence probe
   (`connector/tools/diag_shard_presence.py`, kept) exposed it. "Zero warnings" ≠ correct under TP.
