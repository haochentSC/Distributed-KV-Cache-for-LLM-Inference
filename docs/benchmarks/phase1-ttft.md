# Phase 1 / Phase 4.5 single-node TTFT Benchmark

Status: **run + measured 2026-06-08** (vLLM 0.22.1, RTX 3080, WSL2). Save/load tensor-copy works and
is correct; the ADR 0015 inequality is **measured to invert at single-node ≤3B** (env-capped — see
Results + ADR 0031). Full design/finding writeup: **ADR 0031**.

## Environment

- Runtime: WSL2 + local RTX 3080 laptop GPU. **8 GB dedicated VRAM is the real budget** — the
  ~15.7 GB "shared GPU memory" is host RAM via Windows WDDM and is *not* usable by vLLM's CUDA pool
  under WSL2. TinyLlama-1.1B is the comfortable dev target; **Qwen2.5-3B fits but is tight** — weights
  alone are 5.8 GB, so it needs `--gpu-mem-util 0.86 --enforce-eager` and throttles the KV cache to
  ~0.09 GiB (a measured confound, see Results). 7B+ does not fit in bf16.
- Cache server: `go run ./cmd/cache-server -addr :50051`
- Connector package: `pip install -e "connector[dev,vllm]"`
- vLLM version: record exact version from `python -c "import vllm; print(vllm.__version__)"` (pin it).

## Procedure (Phase 4.5)

The connector's lookup path is done; the worker-side tensor copy is not, because it depends on the
installed vLLM's paged-KV layout + connector-API surface. Sequence:

1. **Probe the live layout (Step 1):**
   `python connector/tools/probe_kv_layout.py --model TinyLlama/TinyLlama-1.1B-Chat-v1.0 --out kv_layout_probe.json`
   This loads the model behind our connector with `probe=true` and dumps each KV tensor's
   shape/dtype/stride, the inferred `block_axis`, and the structure of `forward_context` /
   `attn_metadata` (where `slot_mapping` / physical block ids live).
2. **Wire save/load-copy (Steps 2-3):** the tensor mechanics are ready and CPU-tested in
   `connector/src/kvcache_connector/blockio.py` (`serialize_block` / `deserialize_into`, parameterized
   by the `block_axis` the probe reports). Remaining is the version-specific glue: extract physical
   block ids from the probe-revealed structures and call `client.write` (save) / `deserialize_into`
   (load) for full blocks. Confirm `block_tokens` (16) == vLLM block size.
3. **Run the benchmark + clear the ADR 0015 gate (Step 4):** the command below; then fill Results.
4. **Opportunistic:** while in WSL2's Linux toolchain, run `go test ./... -race` to clear the standing
   race debt the Windows box can't (32-bit MinGW cgo).

## Command

```bash
python connector/scripts/run_phase1_benchmark.py \
  --model TinyLlama/TinyLlama-1.1B-Chat-v1.0 \
  --cache-addr localhost:50051 \
  --runs 8 \
  --output docs/benchmarks/phase1-ttft.json
```

## Acceptance

Phase 1 is complete when this document is updated with:

- Baseline vLLM TTFT p50/p95/p99 without the external connector.
- External connector cold and warm TTFT p50/p95/p99.
- `lookup + fetch + decode_copy` compared against recompute for at least one full-block prefix.
- A short conclusion stating whether the external cache improves repeated-prefix TTFT.

## Results

**Environment as run:** vLLM 0.22.1, CUDA 13.0, RTX 3080 8 GB (WSL2), Python 3.12.13, FlashAttention
v2, `enforce_eager`, `block_tokens=16`. TTFT proxy = wall-clock latency of a 1-token generation;
`warm_vs_baseline = (base − warm)/base` so **positive = cache wins**. p50 over 6–8 runs; cold run
populates the cache, warm serves it back. Use p50 — the first warm call eats a one-time Triton JIT
spike (visible in p95). Raw JSON: `phase45-tinyllama-sweep.json`, `phase45-qwen3b-sweep.json`,
`phase45-qwen3b-batched.json`.

| model | prompt tokens | baseline p50 | warm p50 | warm_vs_baseline |
|-------|--------------|-------------|----------|------------------|
| TinyLlama-1.1B | 482 | 40 ms | 106 ms | −165% |
| TinyLlama-1.1B | 962 | 70 ms | 189 ms | −169% |
| TinyLlama-1.1B | 1682 | 119 ms | 321 ms | −170% |
| Qwen2.5-3B | 385 | 92 ms | 136 ms | −47% |
| Qwen2.5-3B | 769 | 165 ms | 235 ms | −42% |
| Qwen2.5-3B | 1345 | 230 ms | 381 ms | −66% |

**`lookup + fetch + decode_copy` vs recompute (the ADR 0015 gate), one full-block-prefix, 769 tok /
48 blocks × 36 layers** — instrumented via `KVC_LOAD_PROFILE=1`:

```
load breakdown: batch_fetch = 89.4 ms   deserialize + copy + sync = 135.5 ms   (= 225 ms total)
```

- External load: **4.7 ms/block** (225 ms ÷ 48).  Recompute (baseline prefill): **3.4 ms/block**
  (165 ms ÷ 48).  → the inequality **inverts: 4.7 > 3.4**.
- Neither half is bandwidth-bound (28 MB over localhost should be single-digit ms). The 89 ms is
  protobuf + Python `bytearray` assembly of 28 MB; the 135 ms is 1,728 small **unpinned** H2D copies
  (`pin_memory=False` under WSL2). Confirmed by a null result: **batching 48 fetches → 1 BatchFetch
  moved TTFT by noise** (3B 769-tok: −42% → −48%), so the per-block RTT was never the cost.
- Confound: 3B weights (5.8 GB) throttled the KV cache to **0.09 GiB / ~2,500 tokens** of the 8 GB.

## Conclusion

On a single node at ≤3B on this WSL2 box, the external cache **does not** improve repeated-prefix
TTFT — recompute is cheaper than fetch + deserialize + unpinned-copy. This is **environment-capped,
not algorithmically lost**: the deficit closes monotonically with model size (−169% @1B → ~−48% @3B),
and the bottleneck is Python-serialization + unpinned PCIe, both of which a real deployment removes
(pinned buffers, zero-copy/RDMA transport, a GPU not full of weights, a larger model and/or real
cross-request prefix sharing). The headline TTFT win is therefore expected on, and deferred to, the
**distributed/cloud GPU path**. Full analysis: **ADR 0031**.
