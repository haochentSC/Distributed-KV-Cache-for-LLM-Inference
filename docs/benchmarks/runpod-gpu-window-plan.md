# Phase 4.5-B plan — GPU-cloud window on RunPod: long-context curve + TP=4/30B + serving demo

> **Status: PLANNED, not executed.** This is a handoff document — written 2026-06-11, approved by HC,
> to be executed in a later session. Nothing here has been run. Budget cap: **~$20**.

## Context

The AWS paid window (executed 2026-06-10/11, `phase45-distributed-gpu.md`) measured the single-GPU
TTFT crossover (+10.9% @ 4k tokens), but AWS's 8-vCPU G/VT quota ceiling blocks anything bigger.
ADR 0033 deferred the TP=4/30B path to a GPU-specialized cloud ("Option B") with the narrative:
AWS for the *distributed* system, a GPU cloud for GPU *compute* — right tool per job.

HC decided (2026-06-11) to execute Option B, expanded to **three deliverables on RunPod**:

1. **Long-context TTFT curve** (1× A100 80GB): sweep shared-prefix length 4k→32k tokens. The
   crossover math (prefill ~O(n²) vs cache fetch ~O(n)) predicts the +10.9% @ 4k grows to a
   20–40%-class headline — the strongest single number this project can produce.
2. **TP=4 / 30B-class run** (4× A6000 or A40): validates the ADR 0032 KV-head shard keying
   (`shard_model_id`) end-to-end on real tensor parallelism — engineering completeness.
3. **Live serving demo**: `vllm serve` (OpenAI-compatible endpoint) backed by the cache connector,
   exercised with real requests and *recorded* (terminal capture + latency snippet) — a serving
   artifact without a 24/7 bill. Persistent hosting explicitly rejected (cost, no added signal).

### Provider evaluation (record in ADR 0034 when executed)

- **RunPod — chosen.** Best price/availability; both box shapes (1× A100/H100 and 4× A6000/A40)
  self-serve under one account; pods are containers with SSH. Practitioner-standard.
- **Lambda Cloud** — cleanest VMs, most recognized ML-infra brand; multi-GPU availability spottier
  and pricier. Solid runner-up.
- **Modal** — serverless DX is trendy, but it abstracts away exactly the infra control this project
  demonstrates; multi-process topology (cache server + vLLM) fights the platform. Poor fit.
- **CoreWeave** — the most "industry standard" GPU neocloud, but sales/contract-gated; not
  self-serve at this scale.
- **Vast.ai** — cheapest marketplace; unreliable hosts, no signal. Rejected.

The provider name is secondary to the artifacts; AWS already carries the infra credibility.

## Key design decisions

- **Cache topology on the pod: run the Go binaries directly, no Docker.** RunPod pods are
  themselves containers (no docker-in-docker on community pods). Cross-compile
  `GOOS=linux GOARCH=amd64` builds of the cache server and `scp` them up. For the benchmarks a
  **single cache-server on loopback** is the right control (same semantics as ADR 0031; the
  *distributed* story is already proven on AWS — re-proving it here would muddy both numbers).
  State this framing explicitly in the results doc so it reads as deliberate.
- **No server/proto/connector code changes expected.** The TP keying (`shard_model_id`), the probe
  (`connector/tools/probe_kv_layout.py`), and the driver
  (`connector/scripts/run_distributed_benchmark.py`) are provider-agnostic by design
  (ADR 0032/0033). Only likely change: the driver may need a `--max-model-len` passthrough and
  larger `--repeats` values for the 32k sweep — check before the window, patch locally and commit
  first (scaffold-class change).
- **Models.** Long-context: `Qwen/Qwen2.5-7B-Instruct` (directly comparable to the AWS number) and
  optionally `Qwen/Qwen2.5-14B-Instruct` bf16 on the 80GB card. TP=4: `Qwen/Qwen2.5-32B-Instruct`
  bf16 (8 KV heads → 2 per rank, divisible ✓ — re-verify in the probe gate). All support 32k context.
- **Cost control.** RunPod bills per minute; **terminate (not stop) the pod the moment a session
  ends**. Estimate: A100 80GB ≈ $1.6–2.2/hr × ~2h + 4× A6000 ≈ $2–3/hr × ~1.5h ≈ **$7–12** total;
  the demo rides on one of those boxes. Comfortably under $20 with a debugging buffer.

## Execution plan

### Step 0 — pre-flight (free, local; do everything possible before billing starts)

- New runbook: `docs/benchmarks/runpod-runbook.md` (mirror the structure of
  `aws-batch-runbook.md` — it earned its keep). Sessions, gates, teardown sanity.
- RunPod account + ~$20 credit + SSH key registered (**HC does this in the console**).
- Check the driver for 32k support: `--max-model-len`, `--repeats` up to ~512, and that
  workload-prefix × repeats actually reaches 32k tokens. Patch locally if needed; commit first.
- Cross-compile the cache server for linux/amd64; stage the connector package for `scp` (same
  bundle as the AWS window). Local smoke: `cd connector && python -m pytest tests/test_hashing.py -q`.
- Pick the pod template (RunPod PyTorch image); pin `vllm==0.22.1` as before.

### Session A — long-context curve (1× A100 80GB, ~2 h)

1. Launch pod (secure cloud, A100 80GB); `scp` binaries + connector;
   `pip install vllm==0.22.1` + `pip install -e connector`.
2. Start the cache server on loopback (generous `cache_max_bytes` — the pod has 80+ GB RAM).
3. **Gate before any benchmark spend**: `probe_kv_layout.py` on Qwen2.5-7B — layout + `tp_world=1`
   unchanged.
4. Driver smoke (`--repeats 4`), then the sweep:
   `--workload system_prompt --repeats 16,32,64,128,256,512` (≈1k→32k tokens),
   `--cache-addr 127.0.0.1:50051`, output `docs/benchmarks/phase45-longcontext-qwen7b.json`.
   Optionally repeat for 14B (the scaling-curve artifact).
5. **Demo on the same box**: `vllm serve` with the connector env wired; hit the OpenAI-compatible
   endpoint with repeated-prefix requests (`curl`/small script); capture an `asciinema`/`script`
   recording + the TTFT delta with and without the connector. Save under `docs/benchmarks/`.
6. **Terminate the pod.**

### Session B — TP=4 / 30B (4× A6000 or A40, ~1–1.5 h)

1. Launch the 4-GPU pod; same setup.
2. **Gate**: probe with `tensor_parallel_size=4` on Qwen2.5-32B — confirm per-rank KV heads = 2 and
   a distinct `shard_model_id` per rank (the whole point of ADR 0032).
3. Driver: `--models Qwen/Qwen2.5-32B-Instruct --tensor-parallel-size 4 --repeats 4,8,16,32`,
   output `docs/benchmarks/phase45-tp4-qwen32b.json`.
4. **Terminate the pod.** Sanity: RunPod console shows zero running pods.

### Step 3 — capture (local, free)

- Results doc `docs/benchmarks/phase45-gpu-cloud.md` (headline table, demo pointer, findings —
  same shape as `phase45-distributed-gpu.md`).
- **ADR 0034**: GPU-cloud window — the provider evaluation + decision above, the loopback-cache
  framing, results, what TP=4 proved.
- Update the `CLAUDE.md` status block + add an EXECUTED banner to the runbook. One conventional
  commit, **no co-author trailer** (CLAUDE.md rule).

## Verification

- Probe gate before any benchmark spend on every box (the AWS window's discipline).
- Driver smoke run before each long sweep.
- `warm_vs_baseline_pct` present in every JSON; `[kvc] save/load path active` observed; zero
  correctness warnings.
- Teardown sanity: zero running pods at the end of each session.
- Local: `go build ./...`, `gofmt -l .` clean if any Go is touched; pre-commit hook on the commit.

## Risks / honest caveats

- **RunPod availability** varies by region/GPU: if A100 80GB is dry, A100 40GB still covers ~16k
  (fall back; note it). If 4× A6000 is dry, 4× A40 is equivalent for this purpose.
- **vLLM 0.22.1 wheel vs pod CUDA**: the RunPod PyTorch template should match (the same wheel
  worked on AL2023 + DLAMI), but the smoke run is the gate.
- The 32k *baseline* prefill will be slow (~10 s+ TTFT on 7B) — that's the point, but budget the
  runtime: each repeat level runs baseline/cold/warm × several iterations.
- The demo runs on loopback, not cross-AZ — frame it as "transport best case; the cross-AZ AWS
  number is the conservative bound" in the results doc.
