# Phase 1 TTFT Benchmark

Status: benchmark harness added; GPU run still required in WSL2.

## Environment

- Runtime: WSL2 + local RTX 3080
- Cache server: `go run ./cmd/cache-server -addr :50051`
- Connector package: `pip install -e "connector[dev,vllm]"`
- vLLM version: record exact version from `python -c "import vllm; print(vllm.__version__)"`

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

Pending WSL2/GPU run.
