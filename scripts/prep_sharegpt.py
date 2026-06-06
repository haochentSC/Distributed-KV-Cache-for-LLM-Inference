#!/usr/bin/env python3
"""Preprocess ShareGPT into a loadgen trace of real, multi-turn LLM traffic.

Why this exists: the synthetic loadgen fakes prefix reuse with a single "hot prefix".
Real reuse comes from MULTI-TURN conversations — turn N+1's prompt re-sends turns 1..N
as its prefix, so those KV blocks are already cached. ShareGPT is real chat logs and is
the same dataset vLLM's benchmark_serving.py uses, so replaying it makes hit-rate and
latency numbers realistic (and comparable to published vLLM benchmarks).

What it emits: JSONL, one line per REQUEST. Each request is the cumulative token context
at a conversation turn (everything the model would see when generating that turn's reply):

    {"conv": <int>, "turn": <int>, "tokens": [<int>, ...]}

We only emit token IDs because the distributed cache is keyed by block hashes derived
from token IDs (internal/blockhash, ADR 0010). The KV payload itself is irrelevant to
cache *behavior* (hit rate, eviction, load balance) — loadgen synthesizes payload bytes,
so no GPU/model is needed to drive a realistic workload. (Real KV tensors only matter for
the Phase 4.5 TTFT benchmark.)

Tokenizer: tiktoken cl100k_base by default — no HF gating, no model download, realistic
token lengths/boundaries. Pass --hf-tokenizer <name> (needs `transformers`) to match the
exact model you'll serve on the GPU later, e.g. Qwen/Qwen2.5-7B-Instruct.

Getting ShareGPT (the cleaned split vLLM uses):
    curl -L -o sharegpt.json \\
      https://huggingface.co/datasets/anon8231489123/ShareGPT_Vicuna_unfiltered/resolve/main/ShareGPT_V3_unfiltered_cleaned_split.json

Usage:
    pip install tiktoken                       # or: pip install transformers
    python scripts/prep_sharegpt.py --in sharegpt.json --out trace.jsonl --max-convs 2000
"""

import argparse
import json
import sys


def build_tokenizer(hf_name):
    """Return an encode(text) -> list[int]. Real tokenizer = realistic block boundaries."""
    if hf_name:
        from transformers import AutoTokenizer  # lazy: only if --hf-tokenizer is used

        tok = AutoTokenizer.from_pretrained(hf_name)
        return lambda text: tok.encode(text, add_special_tokens=False)
    import tiktoken

    enc = tiktoken.get_encoding("cl100k_base")
    return enc.encode


def iter_conversations(path):
    """Yield each conversation's list of {from, value} turns from a ShareGPT JSON file.

    ShareGPT shape: a JSON list of objects, each with a "conversations" list whose items
    are {"from": "human"|"gpt"|"system", "value": "<text>"}. We tolerate minor variants.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    for item in data:
        turns = item.get("conversations") or item.get("conversation") or []
        if turns:
            yield turns


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--in", dest="inp", required=True, help="ShareGPT conversations JSON")
    ap.add_argument("--out", dest="out", required=True, help="output trace JSONL")
    ap.add_argument("--hf-tokenizer", default="", help="HF tokenizer name (else tiktoken cl100k_base)")
    ap.add_argument("--max-convs", type=int, default=2000, help="cap conversations (0 = all)")
    ap.add_argument("--max-tokens", type=int, default=8192, help="truncate context to this many tokens")
    ap.add_argument("--min-tokens", type=int, default=8, help="skip requests shorter than this")
    args = ap.parse_args()

    encode = build_tokenizer(args.hf_tokenizer)

    n_convs = 0
    n_reqs = 0
    n_tokens = 0
    with open(args.out, "w", encoding="utf-8") as out:
        for conv_idx, turns in enumerate(iter_conversations(args.inp)):
            if args.max_convs and n_convs >= args.max_convs:
                break

            # Replay the conversation turn by turn. The "running" text is everything the
            # model has seen so far; at each HUMAN turn we emit a request = tokens(running +
            # this human turn). Then we append the assistant reply so the NEXT turn's prefix
            # includes it — that growing prefix is exactly what the cache reuses.
            running = ""
            turn_idx = 0
            emitted_this_conv = False
            for t in turns:
                role = (t.get("from") or "").lower()
                text = t.get("value") or ""
                if not text:
                    continue
                if role in ("human", "user", "system"):
                    running += text + "\n"
                    toks = encode(running)
                    if len(toks) > args.max_tokens:
                        toks = toks[: args.max_tokens]
                    if len(toks) >= args.min_tokens:
                        out.write(json.dumps({"conv": conv_idx, "turn": turn_idx, "tokens": toks}) + "\n")
                        n_reqs += 1
                        n_tokens += len(toks)
                        turn_idx += 1
                        emitted_this_conv = True
                else:  # gpt / assistant — extends the context for the next turn's prefix
                    running += text + "\n"
            if emitted_this_conv:
                n_convs += 1

    avg = (n_tokens / n_reqs) if n_reqs else 0
    print(
        f"wrote {n_reqs} requests from {n_convs} conversations to {args.out} "
        f"(avg {avg:.0f} tokens/request)",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
