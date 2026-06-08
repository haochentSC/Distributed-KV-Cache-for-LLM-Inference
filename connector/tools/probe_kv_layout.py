"""Phase 4.5 Step 1 — dump the live vLLM paged-KV layout + connector-API surface.

Run this once in WSL2 (CUDA) before wiring the save/load-copy hooks. It loads the
model behind our connector with ``probe=true``, runs one short generation so the
worker hooks fire, and writes a JSON dump describing:

  - each per-layer KV cache tensor (shape / dtype / stride / device / contiguity),
  - the inferred block axis + num-blocks source (``kv_cache_config``),
  - the structure of ``forward_context`` (start_load_kv) and ``attn_metadata``
    (save_kv_layer) — i.e. where ``slot_mapping`` / physical block ids live.

The dump (default ``kv_layout_probe.json``, plus stdout ``[kvc-probe]`` lines) is
what drives Steps 2-3. No cache server is required — lookups just miss.

    python connector/tools/probe_kv_layout.py \
        --model TinyLlama/TinyLlama-1.1B-Chat-v1.0 \
        --out kv_layout_probe.json
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

# Use vLLM's native PyTorch top-k/top-p sampler. FlashInfer's sampler JIT-compiles a
# CUDA kernel at startup and needs the full CUDA toolkit (nvcc); the wheels only ship
# the runtime, so on a stock WSL2 box that build fails. The native path needs no nvcc.
os.environ.setdefault("VLLM_USE_FLASHINFER_SAMPLER", "0")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="TinyLlama/TinyLlama-1.1B-Chat-v1.0")
    parser.add_argument("--cache-addr", default="localhost:50051")
    parser.add_argument("--block-tokens", type=int, default=16)
    parser.add_argument("--gpu-mem-util", type=float, default=0.55,
                        help="Conservative on an 8 GB 3080; shared GPU memory is not usable VRAM under WSL2/CUDA.")
    parser.add_argument("--out", default="kv_layout_probe.json")
    args = parser.parse_args()

    # Absolute path: the connector runs in vLLM's EngineCore *subprocess*, whose working
    # directory differs from this shell, so a relative probe_out lands somewhere unexpected.
    out = Path(args.out).resolve()
    if out.exists():
        out.unlink()  # the connector appends; start clean each run

    from vllm import LLM, SamplingParams
    from vllm.config import KVTransferConfig

    ktc = KVTransferConfig(
        kv_connector="DistributedKVConnector",
        kv_role="kv_both",
        kv_connector_module_path="kvcache_connector.connector",
        kv_connector_extra_config={
            "cache_addr": args.cache_addr,
            "model_id": args.model,
            "block_tokens": args.block_tokens,
            "deadline_ms": 200,
            "tenant_id": "probe",
            "probe": True,
            "probe_out": str(out),
        },
    )

    llm = LLM(
        model=args.model,
        enable_prefix_caching=False,
        gpu_memory_utilization=args.gpu_mem_util,
        kv_transfer_config=ktc,
    )
    # One short generation drives register_kv_caches + start_load_kv + save_kv_layer.
    prompt = "Explain how a cache miss differs from a correctness violation. " * 8
    llm.generate([prompt], SamplingParams(max_tokens=1, temperature=0.0))

    print(f"\n[probe] layout dump written to {out.resolve()}")
    print("[probe] inspect it (esp. attn_metadata for slot_mapping / block ids), then wire Steps 2-3.")


if __name__ == "__main__":
    main()
