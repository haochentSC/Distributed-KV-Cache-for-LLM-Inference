# Phase 4.5-B handoff — RunPod GPU window (BOTH SESSIONS EXECUTED)

> **Session A: EXECUTED 2026-06-11. Session B: EXECUTED 2026-06-12** — results in
> [`phase45-gpu-cloud.md`](phase45-gpu-cloud.md), ADR 0034; Session B caught + fixed a real
> store-keying bug (ADR 0035). Total spend ≈ $7–8 of the ~$20 cap. This doc is kept as the
> historical plan; nothing below is pending.

## What Session A delivered (do not re-run)

| Item | Status | Artifact |
|---|---|---|
| Pre-flight (local) | ✅ done (commit `ff0feb3`) | runbook, driver/probe patches, demo client |
| Long-context 7B sweep (1k→32k) | ✅ done | `phase45-longcontext-qwen7b.json` |
| 14B scaling check (4k, 16k) | ✅ done | `phase45-longcontext-qwen14b.json` |
| Serving demo (`vllm serve`) | ✅ done | `runpod-demo-serve.typescript` |
| Probe gate (7B, tp=1) | ✅ done | `runpod-a100-kv-layout-probe.json` |
| **TP=4 / 32B validation** | ⬜ **next session** | `phase45-tp4-qwen32b.json` (missing) |

**Honest headline:** no 32k crossover on A100; AWS L4 **+10.9% @ 4k** stays the resume number.
RunPod Session A = boundary analysis + serving integration proof. See results doc for framing.

**Teardown:** Session A pod `fu7bdllghlfssu` must be **terminated** in the RunPod console (HC
action — agent has no API key). Verify zero pods before launching Session B.

## Next session handoff — Session B only (~1–1.5 h, ~$3–5)

### 0. Console (HC)

1. Confirm Session A pod is **terminated** (not stopped).
2. Deploy **Secure Cloud**, **4× RTX A6000** (fallback: 4× A40).
3. Template: **Runpod PyTorch**; **SSH Terminal Access** / expose TCP 22 checked.
4. **Container disk: 100 GB** (Qwen2.5-32B weights ≈ 65 GB bf16 + vLLM + HF cache).
5. Persistent storage: **0 GB** (terminate when done).
6. Paste **Direct TCP** `ip:port` to the agent (not `ssh.runpod.io`).

### 1. Upload + setup (~10 min)

Same as Session A — from repo root on the laptop:

```powershell
scp -i $env:USERPROFILE\.ssh\id_ed25519_runpod -P <port> bin/cache-server-linux-amd64 root@<pod-ip>:/root/cache-server
scp -i $env:USERPROFILE\.ssh\id_ed25519_runpod -P <port> -r connector root@<pod-ip>:/root/connector
```

On the pod:

```bash
pip install --break-system-packages vllm==0.22.1
pip install --break-system-packages -e /root/connector
chmod +x /root/cache-server
/root/cache-server -addr 127.0.0.1:50051 -max-bytes 17179869184 &   # 16 GiB enough for TP run
nvidia-smi   # expect 4 GPUs
```

### 2. TP probe gate (before benchmark spend)

```bash
python /root/connector/tools/probe_kv_layout.py \
  --model Qwen/Qwen2.5-32B-Instruct --tensor-parallel-size 4 \
  --max-model-len 8192 --gpu-mem-util 0.90 --out /root/kv_layout_probe_tp4.json
```

**Pass criteria:** `tp_world=4`; per-rank KV heads = **2** (8 / 4); **distinct `shard_model_id` per
rank** in the dump. Do not proceed if any rank would clobber another (ADR 0032).

### 3. TP=4 benchmark

```bash
python /root/connector/scripts/run_distributed_benchmark.py \
  --models Qwen/Qwen2.5-32B-Instruct --tensor-parallel-size 4 \
  --cache-addr 127.0.0.1:50051 \
  --workload system_prompt --repeats 4,8,16,32 \
  --max-model-len 8192 --deadline-ms 8000 --gpu-mem-util 0.90 \
  --output /root/phase45-tp4-qwen32b.json
```

Success = save/load active on all ranks, **0 correctness warnings**. TTFT delta is bonus.

### 4. Capture + teardown

```powershell
scp -i $env:USERPROFILE\.ssh\id_ed25519_runpod -P <port> root@<pod-ip>:/root/phase45-tp4-qwen32b.json docs/benchmarks/
scp -i $env:USERPROFILE\.ssh\id_ed25519_runpod -P <port> root@<pod-ip>:/root/kv_layout_probe_tp4.json docs/benchmarks/
```

- Update `phase45-gpu-cloud.md` Session B section + mark ADR 0034 Session B complete.
- Add EXECUTED banner to [`runpod-runbook.md`](runpod-runbook.md) Session B block.
- Update `CLAUDE.md` status → Phase 6 polish.
- **TERMINATE the Session B pod.** Console shows zero pods.

Full step-by-step: [`runpod-runbook.md`](runpod-runbook.md) § Session B.

## Original plan context (unchanged)

AWS Phase 4.5 measured distributed TTFT on L4 (+10.9% @ 4k). AWS 8-vCPU quota blocks TP=4/30B
(ADR 0033). Option B = RunPod for GPU compute; AWS for the distributed system.

### Provider evaluation (recorded ADR 0034)

RunPod chosen; Lambda runner-up; Modal/CoreWeave/Vast rejected — see ADR 0034.

### Key design decisions (still valid)

- Loopback cache on pod (not re-proving AWS distributed story).
- Provider-agnostic connector/driver/probe (ADR 0032/0033).
- `--deadline-ms` mandatory for long fetches; repeats top rung **504** for 32k context.

## Verification checklist (Session B)

- [ ] Probe gate before benchmark spend
- [ ] `[kvc] save/load path active` on all TP ranks
- [ ] Zero correctness warnings
- [ ] JSON + probe dump in `docs/benchmarks/`
- [ ] Pod **terminated**; console shows zero pods

## Risks (Session B)

- 4× A6000 availability — A40 equivalent.
- 32B weight download ~65 GB — budget 100 GB container disk.
- First vLLM TP=4 load may need `--max-model-len 8192` cap (already in probe + driver).
