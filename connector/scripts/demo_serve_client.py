"""Phase 4.5-B serving demo client — per-request TTFT against a live ``vllm serve`` endpoint.

This is the demo half of the RunPod window (docs/benchmarks/runpod-runbook.md): vLLM runs as
an OpenAI-compatible server backed by our connector, and this client sends repeated-prefix
requests (the same shared system-prompt prefix as the benchmark workloads + a short unique
suffix per request) while streaming the response, so TTFT = time to the first streamed token.

Run it once against a baseline server (no connector) and once against a connector-backed
server; the recorded terminal capture of the two TTFT columns IS the demo artifact.

    # on the pod, after `vllm serve ...` is up (see the runbook for both server commands):
    python connector/scripts/demo_serve_client.py --label baseline --repeats 64
    python connector/scripts/demo_serve_client.py --label external-warm --repeats 64

Only needs ``requests`` (a vLLM dependency) — no openai package.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path

import requests

# Reuse the exact shared-prefix workloads from the benchmark driver so the demo's cache
# entries are the same blocks the sweep measures (and the texts never diverge).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_distributed_benchmark import WORKLOADS  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000",
                        help="vllm serve address (OpenAI-compatible).")
    parser.add_argument("--model", default="",
                        help="Served model id; default = first entry of GET /v1/models.")
    parser.add_argument("--workload", choices=sorted(WORKLOADS), default="system_prompt")
    parser.add_argument("--repeats", type=int, default=64,
                        help="Shared-prefix repeat factor (64 ~ 4k tokens on Qwen2.5).")
    parser.add_argument("--requests", type=int, default=6, dest="num_requests",
                        help="Requests to send; each gets a unique suffix question.")
    parser.add_argument("--max-tokens", type=int, default=32,
                        help="Tokens to generate per request (enough to see streaming).")
    parser.add_argument("--label", default="run",
                        help="Tag printed with the results (e.g. baseline / external-warm).")
    args = parser.parse_args()

    model = args.model or first_served_model(args.base_url)
    prefix = WORKLOADS[args.workload] * args.repeats

    print(f"[demo] label={args.label}  model={model}  workload={args.workload}  "
          f"repeats={args.repeats}  requests={args.num_requests}")

    ttfts: list[float] = []
    for i in range(args.num_requests):
        # Unique suffix per request: only the shared prefix can be a cache hit (ADR 0016
        # semantics — byte-identical prefix blocks, never fuzzy reuse).
        prompt = prefix + f" Q{i}: in one short sentence, what matters most here? A:"
        ttft_ms, total_ms, text = stream_completion(
            args.base_url, model, prompt, args.max_tokens)
        ttfts.append(ttft_ms)
        snippet = text.strip().replace("\n", " ")[:60]
        print(f"[demo]   req {i}: TTFT {ttft_ms:8.1f} ms   total {total_ms:8.1f} ms   | {snippet}")

    ordered = sorted(ttfts)
    p50 = ordered[len(ordered) // 2]
    print(f"[demo] {args.label}: TTFT p50 {p50:.1f} ms   mean {statistics.fmean(ttfts):.1f} ms   "
          f"min {ordered[0]:.1f} ms   max {ordered[-1]:.1f} ms")


def first_served_model(base_url: str) -> str:
    resp = requests.get(f"{base_url}/v1/models", timeout=10)
    resp.raise_for_status()
    data = resp.json().get("data", [])
    if not data:
        raise SystemExit(f"[demo] no models served at {base_url}/v1/models")
    return data[0]["id"]


def stream_completion(base_url: str, model: str, prompt: str,
                      max_tokens: int) -> tuple[float, float, str]:
    """POST /v1/completions with stream=True; returns (ttft_ms, total_ms, generated_text)."""
    body = {
        "model": model,
        "prompt": prompt,
        "max_tokens": max_tokens,
        "temperature": 0.0,
        "stream": True,
    }
    start = time.perf_counter()
    first: float | None = None
    text_parts: list[str] = []
    with requests.post(f"{base_url}/v1/completions", json=body, stream=True,
                       timeout=600) as resp:
        resp.raise_for_status()
        for raw in resp.iter_lines():
            if not raw:
                continue
            line = raw.decode("utf-8", errors="replace")
            if not line.startswith("data:"):
                continue
            payload = line[len("data:"):].strip()
            if payload == "[DONE]":
                break
            if first is None:
                first = time.perf_counter()
            try:
                chunk = json.loads(payload)
                text_parts.append(chunk["choices"][0].get("text", ""))
            except (json.JSONDecodeError, KeyError, IndexError):
                pass
    end = time.perf_counter()
    ttft_ms = ((first or end) - start) * 1000
    return ttft_ms, (end - start) * 1000, "".join(text_parts)


if __name__ == "__main__":
    main()
