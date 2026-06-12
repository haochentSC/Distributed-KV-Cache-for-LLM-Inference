"""Diagnose TP shard presence: which ranks' KV shards are actually in the cache?

Rebuilds the benchmark's block hashes (same workload prefix, tokenizer, and
chain_blocks as the driver/connector) and runs a Lookup under every rank's
shard_model_id. Run on the pod next to a populated cache server.
"""

from __future__ import annotations

import argparse
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default="Qwen/Qwen2.5-32B-Instruct")
    parser.add_argument("--cache-addr", default="127.0.0.1:50051")
    parser.add_argument("--tp-world", type=int, default=4)
    parser.add_argument("--repeats", default="4,8,16,32")
    parser.add_argument("--workload", default="system_prompt")
    parser.add_argument("--block-tokens", type=int, default=16)
    parser.add_argument("--scripts-dir", default="/root/connector/scripts")
    args = parser.parse_args()

    sys.path.insert(0, args.scripts_dir)
    from run_distributed_benchmark import WORKLOADS  # noqa: E402

    from transformers import AutoTokenizer  # noqa: E402

    from kvcache_connector.client import KVCacheClient  # noqa: E402
    from kvcache_connector.hashing import chain_blocks, shard_model_id  # noqa: E402

    prefix = WORKLOADS[args.workload]
    tok = AutoTokenizer.from_pretrained(args.model)
    client = KVCacheClient(args.cache_addr, deadline_ms=5000)

    for r in (int(x) for x in args.repeats.split(",")):
        prompt = (prefix * r) + f" Q{r}: summarize the above in one word. A:"
        tokens = tok(prompt).input_ids
        blocks = chain_blocks(args.model, tokens, args.block_tokens)
        print(f"repeats={r} tokens={len(tokens)} blocks={len(blocks)}")
        keys = [
            (f"rank {rank}", shard_model_id(args.model, rank, args.tp_world))
            for rank in range(args.tp_world)
        ]
        # An unsharded write would indicate tp_world resolved to 1 on the save path.
        keys.append(("unsharded", args.model))
        for label, sid in keys:
            pres = client.lookup(sid, blocks)
            n = sum(1 for p in pres if p.has_entry)
            versions = sorted({p.version for p in pres if p.has_entry})
            print(
                f"  {label} ({sid}): {n}/{len(blocks)} present, "
                f"versions={versions[:6]}{'...' if len(versions) > 6 else ''}"
            )


if __name__ == "__main__":
    main()
