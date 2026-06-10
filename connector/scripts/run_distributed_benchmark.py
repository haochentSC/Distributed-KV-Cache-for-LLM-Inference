"""Phase 4.5 distributed TTFT benchmark — the paid-window headline (ADR 0031/0032).

This runs on a GPU node (AWS g5.2xlarge, 1x A10G — single-GPU, the 8-vCPU G/VT Spot quota
path; TP=4 on g5.12xlarge is the deferred multi-GPU path, ADR 0032/0033) and points vLLM, via our dynamic
connector, at the LIVE multi-node cache cluster over the VPC. Unlike the single-node
laptop run (connector/scripts/run_phase1_benchmark.py), here:

  * the model runs on a real datacenter A10G (single-GPU by default; ``--tensor-parallel-size``
    > 1 shards KV heads, one worker per rank — see connector.shard_model_id),
  * host memory is pinned (real Linux, not WSL2) and the KV cache is not throttled,
  * the workload is prefix-heavy and realistic (a long shared system-prompt / RAG /
    few-shot prefix + a short unique suffix per request), which is where prefix reuse
    actually pays.

It measures wall-clock one-token latency as a TTFT proxy in three conditions per prompt:
baseline (no connector) vs external cold (populates the cache) vs external warm (serves
the shared prefix back). ``warm_vs_baseline_pct > 0`` means the cache wins. Sweeping
``--models`` emits the TTFT-vs-model-size scaling curve that is the real artifact.

Example (from the GPU node, cache reachable at a cluster private IP):

    python connector/scripts/run_distributed_benchmark.py \
        --models Qwen/Qwen2.5-7B-Instruct \
        --tensor-parallel-size 1 \
        --cache-addr 10.0.1.23:50051 \
        --workload system_prompt --repeats 4,8,16 \
        --output docs/benchmarks/phase45-distributed-qwen7b.json
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from pathlib import Path
from typing import Any

# Native PyTorch sampler — avoids FlashInfer's startup nvcc JIT (CUDA toolkit not always
# present on the runtime wheels). Same choice as the single-node driver.
os.environ.setdefault("VLLM_USE_FLASHINFER_SAMPLER", "0")


# A long, realistic SHARED prefix per workload type. Repeating it (``--repeats``) scales the
# cached-prefix length so we can find where transfer (O(n)) beats prefill (O(n^2)) on a real model.
# Each request appends a short UNIQUE suffix, so only the shared prefix is a cache hit — exactly the
# system-prompt / RAG / few-shot reuse pattern that motivates an external KV cache.
WORKLOADS: dict[str, str] = {
    "system_prompt": (
        "You are a meticulous distributed-systems engineer embedded in a production on-call "
        "rotation. Always reason about consistency, partial failure, and backpressure before "
        "proposing a fix. Prefer precise terminology (quorum, lease, watermark, GDSF) and cite "
        "the relevant trade-off. Never serve data that mismatches the requested key. "
    ),
    "rag": (
        "Context document (retrieved): A distributed KV cache for LLM inference shares attention "
        "key/value tensors across a multi-node cluster using consistent hashing with prefix "
        "affinity, RF=2 asynchronous replication, etcd-coordinated failover, and a cost-aware, "
        "fairness-weighted eviction policy. Cold blocks spill to S3. Answer ONLY from this context. "
    ),
    "fewshot": (
        "Q: What is a cache miss? A: A requested key is absent; the value is recomputed. "
        "Q: What is a correctness violation? A: Serving bytes that do not match the requested key. "
        "Q: What is a watermark? A: A high/low byte threshold that drives background eviction. "
        "Q: What is GDSF? A: A cost-aware eviction value H = L + freq*cost/size. "
    ),
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", default="Qwen/Qwen2.5-7B-Instruct",
                        help="Comma list of HF model ids to sweep (the scaling curve). The default "
                             "fits a single A10G (24 GB) in bf16; ~13-14B needs quantization.")
    parser.add_argument("--tensor-parallel-size", type=int, default=1,
                        help="GPU ranks; each owns a KV-head shard (ADR 0032). Default 1 = single-GPU "
                             "(the 8-vCPU-quota path); set 4 for the g5.12xlarge TP path.")
    parser.add_argument("--cache-addr", default="localhost:50051",
                        help="Cache node host:port. On AWS use a cluster PRIVATE IP (same VPC).")
    parser.add_argument("--block-tokens", type=int, default=16)
    parser.add_argument("--runs", type=int, default=8)
    parser.add_argument("--workload", choices=sorted(WORKLOADS), default="system_prompt")
    parser.add_argument("--repeats", default="4,8,16",
                        help="Comma list of prefix repeat factors to sweep (scales the cached prefix).")
    parser.add_argument("--max-model-len", type=int, default=8192)
    parser.add_argument("--gpu-mem-util", type=float, default=0.90,
                        help="No WSL2 throttle here — leave headroom for the KV cache on each A10G.")
    parser.add_argument("--enforce-eager", action="store_true",
                        help="Skip CUDA-graph capture. Applies to baseline AND external, so fair.")
    parser.add_argument("--output", default="docs/benchmarks/phase45-distributed.json")
    args = parser.parse_args()

    models = [m for m in (s.strip() for s in args.models.split(",")) if m]
    repeats = [int(x) for x in str(args.repeats).split(",") if x.strip()]
    prefix = WORKLOADS[args.workload]

    sweeps = [run_model(m, prefix, repeats, args) for m in models]

    out_obj = {
        "phase": "4.5-distributed",
        "tensor_parallel_size": args.tensor_parallel_size,
        "cache_addr": args.cache_addr,
        "workload": args.workload,
        "block_tokens": args.block_tokens,
        "runs": args.runs,
        "models": sweeps,
        "notes": (
            "Wall-clock one-token latency as TTFT proxy. warm_vs_baseline_pct>0 = cache wins. "
            "Shared prefix is cached; each request adds a short unique suffix. Sweep --models for "
            "the TTFT-vs-model-size scaling curve (ADR 0031: the deficit closes as the model grows)."
        ),
    }
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(out_obj, indent=2), encoding="utf-8")
    print(json.dumps(out_obj, indent=2))

    print("\nmodel                              repeat  tokens  baseline_p50  warm_p50  warm_vs_baseline")
    for sweep in sweeps:
        for row in sweep["results"]:
            pct = row["warm_vs_baseline_pct"]
            pct_str = "n/a" if pct is None else f"{pct:+.1f}%"
            print(f"{sweep['model'][:34]:<34}  {row['repeat']:>6}  {row['prompt_tokens']:>6}  "
                  f"{row['baseline_ms']['p50']:>11.1f}  {row['external_warm_ms']['p50']:>8.1f}  {pct_str:>8}")


def run_model(model: str, prefix: str, repeats: list[int], args: Any) -> dict[str, Any]:
    """Baseline vs external-cold vs external-warm for one model, swept over prefix lengths."""
    from vllm import LLM, SamplingParams
    from vllm.config import KVTransferConfig

    # prompt = (shared prefix * r) + a short unique suffix, so only the shared prefix is a cache hit.
    prompts = {r: (prefix * r) + f" Q{r}: summarize the above in one word. A:" for r in repeats}
    sampling = SamplingParams(max_tokens=1, temperature=0.0)
    common: dict[str, Any] = dict(
        model=model,
        enable_prefix_caching=False,  # isolate the EXTERNAL cache; no in-engine prefix reuse.
        gpu_memory_utilization=args.gpu_mem_util,
        tensor_parallel_size=args.tensor_parallel_size,
        max_model_len=args.max_model_len,
    )
    if args.enforce_eager:
        common["enforce_eager"] = True

    # --- baseline: no connector ---
    baseline = LLM(**common)
    tok = _tokenizer(baseline)
    prompt_tokens = {r: _count_tokens(tok, prompts[r]) for r in repeats}
    baseline_lat = {r: measure(baseline, prompts[r], sampling, args.runs) for r in repeats}
    del baseline
    _free_gpu()

    # --- external: kv_both connector (saves on miss, loads on hit), keyed per TP rank ---
    ktc = KVTransferConfig(
        kv_connector="DistributedKVConnector",
        kv_role="kv_both",
        kv_connector_module_path="kvcache_connector.connector",
        kv_connector_extra_config={
            "cache_addr": args.cache_addr,
            "model_id": model,
            "block_tokens": args.block_tokens,
            "deadline_ms": 2000,
            "tenant_id": "phase45-dist",
        },
    )
    external = LLM(kv_transfer_config=ktc, **common)
    cold_lat: dict[int, list[float]] = {}
    warm_lat: dict[int, list[float]] = {}
    for r in repeats:
        cold_lat[r] = measure(external, prompts[r], sampling, 1)         # populate (save path)
        warm_lat[r] = measure(external, prompts[r], sampling, args.runs)  # serve back (load path)
    del external
    _free_gpu()

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
    return {"model": model, "max_model_len": args.max_model_len, "results": results}


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
    """Release a model's VRAM before loading the next (the sweep loads several)."""
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
        "mean": statistics.fmean(ordered) if ordered else 0.0,
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
