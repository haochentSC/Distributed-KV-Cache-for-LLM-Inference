"""Run the Phase 1 / Phase 4.5 single-node TTFT benchmark.

This script is meant for WSL2 + CUDA. It measures wall-clock latency for one-token
generations as a practical TTFT proxy, with and without the dynamic connector.

It sweeps one or more prompt lengths (``--repeats``) inside a single model load, so
you can hunt for the crossover where the external cache (transfer O(n)) beats local
prefill (attention O(n^2)). Each length is run: baseline (no cache) vs external cold
(populates the cache) vs external warm (serves it back).
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from pathlib import Path
from typing import Any

# Native PyTorch sampler — avoids FlashInfer's startup nvcc JIT (CUDA toolkit not
# installed under the runtime-only wheels). See connector/tools/probe_kv_layout.py.
os.environ.setdefault("VLLM_USE_FLASHINFER_SAMPLER", "0")


BASE = (
    "You are a concise technical assistant. Use precise distributed systems terminology. "
    "Explain how a cache miss differs from a correctness violation. "
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="TinyLlama/TinyLlama-1.1B-Chat-v1.0")
    parser.add_argument("--cache-addr", default="localhost:50051")
    parser.add_argument("--block-tokens", type=int, default=16)
    parser.add_argument("--runs", type=int, default=8)
    parser.add_argument("--repeats", default="16",
                        help="Comma list of prompt repeat factors to sweep, e.g. 16,40,80.")
    parser.add_argument("--max-model-len", type=int, default=None,
                        help="Raise for long-context sweeps / bigger models (e.g. 8192).")
    parser.add_argument("--gpu-mem-util", type=float, default=0.55,
                        help="Conservative on an 8 GB 3080; shared GPU memory is not usable VRAM under WSL2/CUDA. "
                             "Raise toward ~0.92 for a 3B model whose weights alone are ~6 GB.")
    parser.add_argument("--enforce-eager", action="store_true",
                        help="Skip CUDA-graph capture (~0.3-0.4 GB) so a tight 8 GB model fits. Applies equally "
                             "to baseline and external, so the comparison stays fair.")
    parser.add_argument("--output", default="docs/benchmarks/phase1-ttft.json")
    args = parser.parse_args()

    from vllm import LLM, SamplingParams
    from vllm.config import KVTransferConfig

    repeats = [int(x) for x in str(args.repeats).split(",") if x.strip()]
    prompts = {r: BASE * r for r in repeats}
    sampling = SamplingParams(max_tokens=1, temperature=0.0)
    common = dict(model=args.model, enable_prefix_caching=False, gpu_memory_utilization=args.gpu_mem_util)
    if args.max_model_len:
        common["max_model_len"] = args.max_model_len
    if args.enforce_eager:
        common["enforce_eager"] = True

    # --- baseline: no connector ---
    baseline = LLM(**common)
    tok = _tokenizer(baseline)
    prompt_tokens = {r: _count_tokens(tok, prompts[r]) for r in repeats}
    baseline_lat = {r: measure(baseline, prompts[r], sampling, args.runs) for r in repeats}
    del baseline
    _free_gpu()

    # --- external: kv_both connector (saves on miss, loads on hit) ---
    ktc = KVTransferConfig(
        kv_connector="DistributedKVConnector",
        kv_role="kv_both",
        kv_connector_module_path="kvcache_connector.connector",
        kv_connector_extra_config={
            "cache_addr": args.cache_addr,
            "model_id": args.model,
            "block_tokens": args.block_tokens,
            "deadline_ms": 2000,
            "tenant_id": "phase1",
        },
    )
    external = LLM(kv_transfer_config=ktc, **common)
    cold_lat: dict[int, list[float]] = {}
    warm_lat: dict[int, list[float]] = {}
    for r in repeats:
        cold_lat[r] = measure(external, prompts[r], sampling, 1)        # populate (save path)
        warm_lat[r] = measure(external, prompts[r], sampling, args.runs)  # serve back (load path)

    results = []
    for r in repeats:
        base_p50 = summarize(baseline_lat[r])["p50"]
        warm_p50 = summarize(warm_lat[r])["p50"]
        results.append({
            "repeat": r,
            "prompt_tokens": prompt_tokens[r],
            "baseline_ms": summarize(baseline_lat[r]),
            "external_cold_ms": summarize(cold_lat[r]),
            "external_warm_ms": summarize(warm_lat[r]),
            "warm_vs_baseline_pct": None if base_p50 == 0 else round(100.0 * (base_p50 - warm_p50) / base_p50, 1),
        })

    out_obj = {
        "model": args.model,
        "max_model_len": args.max_model_len,
        "block_tokens": args.block_tokens,
        "runs": args.runs,
        "results": results,
        "notes": "Wall-clock one-token generation latency as TTFT proxy. warm_vs_baseline_pct>0 = cache wins.",
    }
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(out_obj, indent=2), encoding="utf-8")
    print(json.dumps(out_obj, indent=2))
    # Compact crossover summary.
    print("\nrepeat  tokens  baseline_p50  warm_p50  warm_vs_baseline")
    for row in results:
        pct = row["warm_vs_baseline_pct"]
        pct_str = "n/a" if pct is None else f"{pct:+.1f}%"
        print(f"{row['repeat']:>6}  {row['prompt_tokens']:>6}  "
              f"{row['baseline_ms']['p50']:>11.1f}  {row['external_warm_ms']['p50']:>8.1f}  {pct_str:>8}")


def _tokenizer(llm: Any) -> Any:
    try:
        return llm.get_tokenizer()
    except Exception:
        return None


def _count_tokens(tok: Any, text: str) -> int:
    if tok is None:
        return 0
    try:
        return len(tok.encode(text))
    except Exception:
        return 0


def _free_gpu() -> None:
    """Release the baseline model's VRAM before loading the external one (8 GB is tight)."""
    import gc

    gc.collect()
    try:
        import torch

        if torch.cuda.is_available():
            torch.cuda.empty_cache()
    except Exception:
        pass


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
