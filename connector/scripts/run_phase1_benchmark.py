"""Run the Phase 1 single-node TTFT benchmark.

This script is meant for WSL2 + CUDA. It measures wall-clock latency for one-token
generations as a practical TTFT proxy, with and without the dynamic connector.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path
from typing import Any


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="TinyLlama/TinyLlama-1.1B-Chat-v1.0")
    parser.add_argument("--cache-addr", default="localhost:50051")
    parser.add_argument("--block-tokens", type=int, default=16)
    parser.add_argument("--runs", type=int, default=8)
    parser.add_argument("--output", default="docs/benchmarks/phase1-ttft.json")
    args = parser.parse_args()

    from vllm import LLM, SamplingParams
    from vllm.config import KVTransferConfig

    prompt = (
        "You are a concise technical assistant. Use precise distributed systems terminology. "
        "Explain how a cache miss differs from a correctness violation. "
    ) * 16
    sampling = SamplingParams(max_tokens=1, temperature=0.0)

    baseline = LLM(model=args.model, enable_prefix_caching=False)
    baseline_lat = measure(baseline, prompt, sampling, args.runs)
    del baseline

    ktc = KVTransferConfig(
        kv_connector="DistributedKVConnector",
        kv_role="kv_both",
        kv_connector_module_path="kvcache_connector.connector",
        kv_connector_extra_config={
            "cache_addr": args.cache_addr,
            "model_id": args.model,
            "block_tokens": args.block_tokens,
            "deadline_ms": 500,
            "tenant_id": "phase1",
        },
    )
    external = LLM(model=args.model, enable_prefix_caching=False, kv_transfer_config=ktc)
    cold_lat = measure(external, prompt, sampling, 1)
    warm_lat = measure(external, prompt, sampling, args.runs)

    result = {
        "model": args.model,
        "cache_addr": args.cache_addr,
        "block_tokens": args.block_tokens,
        "runs": args.runs,
        "baseline_ms": summarize(baseline_lat),
        "external_cold_ms": summarize(cold_lat),
        "external_warm_ms": summarize(warm_lat),
        "notes": "Wall-clock one-token generation latency used as TTFT proxy.",
    }
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


def measure(llm: Any, prompt: str, sampling: Any, runs: int) -> list[float]:
    latencies: list[float] = []
    for _ in range(runs):
        start = time.perf_counter()
        llm.generate([prompt], sampling)
        latencies.append((time.perf_counter() - start) * 1000)
    return latencies


def summarize(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)
    return {
        "p50": percentile(ordered, 0.50),
        "p95": percentile(ordered, 0.95),
        "p99": percentile(ordered, 0.99),
        "mean": statistics.fmean(ordered),
    }


def percentile(ordered: list[float], p: float) -> float:
    if not ordered:
        return 0.0
    idx = int(p * len(ordered))
    if idx >= len(ordered):
        idx = len(ordered) - 1
    return ordered[idx]


if __name__ == "__main__":
    main()
