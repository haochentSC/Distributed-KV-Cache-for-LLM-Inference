# Chat-UI demo — per-turn TTFT with a KV-cache-hit badge

A tiny chat frontend that makes the cache *visible*: every assistant turn shows its measured
TTFT (time to first streamed token) and a badge — green **KV cache hit · n blocks** when the
turn's prefill was served from the distributed cache, amber **cold** when it wasn't. Run a
conversation, press **Replay conversation**: the second pass rebuilds the same context turn by
turn, so every prefill hits the cache written on the first pass.

The badge is ground truth, not a guess: the backend diffs the cache-server's Prometheus
counters (`kvcache_cache_requests_total`, ADR 0025) around each turn.

> Honest framing: on a small model + local GPU the *badge* flips reliably but the TTFT delta is
> modest — at this scale prefill is cheap (that's the measured crossover story,
> [`phase45-distributed-gpu.md`](../../docs/benchmarks/phase45-distributed-gpu.md)). The
> headline numbers come from the AWS L4 benchmark; this demo is about *seeing* the mechanism.

## Run it (WSL2 / Linux box with a GPU and the project's vLLM venv)

Three processes:

```bash
# 1. The cache (Go) — metrics endpoint on :9090 is what feeds the badge
go run ./cmd/cache-server -max-bytes 4294967296

# 2. vLLM with the connector (TinyLlama fits an 8 GB GPU; use Qwen2.5-3B/7B if you have room).
#    --no-enable-prefix-caching disables vLLM's *internal* prefix cache so the badge/TTFT
#    measure the distributed cache, not local APC.
# VLLM_USE_FLASHINFER_SAMPLER=0 + --enforce-eager: no CUDA-toolkit/nvcc JIT needed (matches
# the benchmark drivers); 0.80 leaves headroom on an 8 GB laptop GPU.
VLLM_USE_FLASHINFER_SAMPLER=0 \
vllm serve TinyLlama/TinyLlama-1.1B-Chat-v1.0 --port 8000 --max-model-len 2048 \
  --gpu-memory-utilization 0.80 --enforce-eager --no-enable-prefix-caching \
  --kv-transfer-config '{"kv_connector":"DistributedKVConnector","kv_role":"kv_both","kv_connector_module_path":"kvcache_connector.connector","kv_connector_extra_config":{"cache_addr":"127.0.0.1:50051","model_id":"TinyLlama/TinyLlama-1.1B-Chat-v1.0","block_tokens":16,"deadline_ms":5000,"tenant_id":"demo"}}'

# 3. The UI (stdlib only — no extra pip installs)
python demo/chat-ui/serve.py
# open http://localhost:8001
```

The demo: ask 3–4 questions, then **Replay conversation** — first pass mostly cold (the cache
is being written), second pass every turn badge-lit green.
