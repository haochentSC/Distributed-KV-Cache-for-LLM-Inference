# RunPod GPU-window runbook — long-context curve + TP=4/30B + serving demo (Phase 4.5-B)

Executes [`runpod-gpu-window-plan.md`](runpod-gpu-window-plan.md) (approved 2026-06-11). Two paid
sessions on RunPod, both **execution-only** — everything below the pre-flight line was prepared and
locally verified beforehand. Budget cap **~$20**; estimate **$7–12**.

> **Pods bill per minute. TERMINATE (not stop) the pod the moment a session ends.** A stopped pod
> still bills for disk. Teardown sanity at the end of each session: the RunPod console shows
> **zero pods**, running or stopped.

## HC console prerequisites (do once, before Session A)

- RunPod account + **~$20 credit** loaded.
- SSH public key registered (Settings → SSH Keys).
- When launching pods, prefer one with a **public IP / direct TCP port 22** ("SSH over exposed
  TCP") — the basic proxy SSH does not support `scp`. Fallback: `runpodctl send/receive`.

## Pre-flight (done locally, free — 2026-06-11)

- Driver gained `--deadline-ms` (the hardcoded 2000 ms would make a ~2 GB warm 32k fetch time out
  and **silently** degrade to recompute — warm ≈ baseline with zero errors) and a fail-fast
  context-overflow guard that runs before the model loads.
- Probe gained `--tensor-parallel-size` (the Session B gate) and `--max-model-len`.
- **Repeats ladder verified with the real Qwen2.5 tokenizer:** the `system_prompt` workload is
  exactly **64 tokens/repeat + a 15-token suffix**, so `--repeats 512` = 32,783 tokens —
  **overflows** the 32,768 context by exactly the suffix. Verified ladder:

  | repeats | 16 | 32 | 64 | 128 | 256 | **504** |
  |---|---|---|---|---|---|---|
  | prompt tokens | 1,038 | 2,062 | 4,110 | 8,207 | 16,399 | **32,271** |

- Demo client scaffolded: `connector/scripts/demo_serve_client.py` (streams completions, prints
  per-request TTFT; reuses the driver's `WORKLOADS` so demo blocks = benchmark blocks).
- Cache server cross-compiled for linux/amd64; connector package staged for `scp`;
  `pytest tests/test_hashing.py` green.

## Cost

A100 80 GB (secure cloud) ≈ $1.6–2.2/hr × ~2 h + 4× A6000 ≈ $2–3/hr × ~1.5 h ≈ **$7–12** total.
The demo rides on the Session A box. HF weight downloads are free/fast on pod NVMe.

## Common setup (both sessions, ~10 min)

```
# from the laptop — binary + connector bundle (cross-compiled in pre-flight)
scp -P <port> bin/cache-server-linux-amd64 root@<pod-ip>:/root/cache-server
scp -P <port> -r connector root@<pod-ip>:/root/connector

# on the pod (RunPod PyTorch template)
pip install vllm==0.22.1            # pin: the connector + probe were written against this
pip install -e /root/connector
nvidia-smi                          # 1× A100 (Session A) / 4× A6000 or A40 (Session B)

# cache server on loopback, generous cap (single-node control — the distributed story is
# already proven on AWS; loopback isolates the long-context transfer question, per the plan)
chmod +x /root/cache-server
/root/cache-server -addr 127.0.0.1:50051 -max-bytes 34359738368 &   # 32 GiB
```

Working-set math (why 32 GiB is comfortable): Qwen2.5-7B ≈ 57 KB KV/token → a full 32k prefix
≈ 1.9 GB (rungs share prefix blocks); 14B ≈ 192 KB/token → ≈ 6.2 GB.

## Session A — long-context curve + demo (1× A100 80 GB, ~2 h)

1. **Probe gate (before any benchmark spend):**

   ```
   python /root/connector/tools/probe_kv_layout.py \
     --model Qwen/Qwen2.5-7B-Instruct --gpu-mem-util 0.90 --out kv_layout_probe.json
   ```
   Confirm `block_axis` unchanged and `tp_world=1` in the dump.

2. **Driver smoke** (`--repeats 4`, one model) — watch for `[kvc] save/load path active` and the
   new `[guard] ... fits` lines; zero correctness warnings.

3. **The sweep** (the headline artifact):

   ```
   python /root/connector/scripts/run_distributed_benchmark.py \
     --models Qwen/Qwen2.5-7B-Instruct \
     --cache-addr 127.0.0.1:50051 \
     --workload system_prompt --repeats 16,32,64,128,256,504 \
     --max-model-len 32768 --deadline-ms 15000 --gpu-mem-util 0.90 \
     --output docs/benchmarks/phase45-longcontext-qwen7b.json
   ```
   The 32k baseline prefill is slow by design (~5–10 s/run); budget ~15–20 min. Optionally
   re-run with `--models Qwen/Qwen2.5-14B-Instruct` (bf16 fits the 80 GB card) for the
   scaling-curve artifact → `phase45-longcontext-qwen14b.json`.

4. **Serving demo** (recorded). Terminal capture via `script demo_serve.typescript` (preinstalled)
   or `pip install asciinema && asciinema rec demo.cast`. Two server runs, same client:

   ```
   # run 1 — baseline (no connector)
   vllm serve Qwen/Qwen2.5-7B-Instruct --port 8000 --max-model-len 8192 \
     --no-enable-prefix-caching --gpu-memory-utilization 0.90
   python /root/connector/scripts/demo_serve_client.py --label baseline --repeats 64

   # run 2 — connector-backed (cache already warm from the sweep at repeats=64)
   vllm serve Qwen/Qwen2.5-7B-Instruct --port 8000 --max-model-len 8192 \
     --no-enable-prefix-caching --gpu-memory-utilization 0.90 \
     --kv-transfer-config '{"kv_connector":"DistributedKVConnector","kv_role":"kv_both",
       "kv_connector_module_path":"kvcache_connector.connector",
       "kv_connector_extra_config":{"cache_addr":"127.0.0.1:50051",
       "model_id":"Qwen/Qwen2.5-7B-Instruct","block_tokens":16,
       "deadline_ms":5000,"tenant_id":"demo"}}'
   python /root/connector/scripts/demo_serve_client.py --label external-cold --repeats 64
   python /root/connector/scripts/demo_serve_client.py --label external-warm --repeats 64
   ```
   (JSON on one line in the real command; flag spelling per `vllm serve --help` — 0.22.1 uses
   `--no-enable-prefix-caching`.) `scp` the recording + JSONs back to the laptop, under
   `docs/benchmarks/`. Framing for the results doc: loopback = transport best case; the
   cross-AZ AWS number is the conservative bound.

5. **TERMINATE the pod.** Console shows zero pods.

## Session B — TP=4 / 32B (4× A6000 or A40, ~1–1.5 h)

1. Common setup as above (16 GiB `-max-bytes` is plenty: repeats ≤ 32 ≈ 2.1k tokens × ~256 KB
   KV/token across ranks ≈ 0.5 GB).
2. **TP gate (the whole point of ADR 0032), before the benchmark:**

   ```
   python /root/connector/tools/probe_kv_layout.py \
     --model Qwen/Qwen2.5-32B-Instruct --tensor-parallel-size 4 \
     --max-model-len 8192 --gpu-mem-util 0.90 --out kv_layout_probe_tp4.json
   ```
   Confirm in the dump/`[kvc-probe]` lines: `tp_world=4`, per-rank KV heads = **2**
   (`num_key_value_heads 8 / 4`), and a **distinct `shard_model_id` per rank**.

3. **The TP=4 run:**

   ```
   python /root/connector/scripts/run_distributed_benchmark.py \
     --models Qwen/Qwen2.5-32B-Instruct --tensor-parallel-size 4 \
     --cache-addr 127.0.0.1:50051 \
     --workload system_prompt --repeats 4,8,16,32 \
     --max-model-len 8192 --deadline-ms 8000 --gpu-mem-util 0.90 \
     --output docs/benchmarks/phase45-tp4-qwen32b.json
   ```
   Success = the keying works end-to-end (save/load active on all ranks, zero correctness
   warnings) + whatever TTFT delta a 32B model shows; engineering completeness is the deliverable.

4. **TERMINATE the pod.** Console shows zero pods.

## Capture (local, free — after both sessions)

- Results doc `docs/benchmarks/phase45-gpu-cloud.md` (headline table, demo pointer, findings —
  same shape as `phase45-distributed-gpu.md`).
- **ADR 0034**: provider evaluation + decision (in the plan doc), loopback-cache framing, results,
  what TP=4 proved.
- Update the `CLAUDE.md` status block; add an EXECUTED banner to this runbook and the plan doc.
- One conventional commit, no co-author trailer.

## Risks (from the plan; checked at the gates)

- A100 80 GB dry → A100 40 GB covers ~16k (note the cut ladder). 4× A6000 dry → 4× A40.
- vLLM 0.22.1 wheel vs pod CUDA: the smoke run is the gate (same wheel worked on AL2023 + DLAMI).
- If warm ≈ baseline at long rungs with no warnings, check the cache server log and per-rung
  `external_warm_ms` vs `external_cold_ms` first — a too-tight `--deadline-ms` is the silent
  failure mode the pre-flight patch exists for.
