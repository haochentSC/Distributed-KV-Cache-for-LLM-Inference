#!/usr/bin/env python3
"""Chat-UI demo backend: proxy a vLLM OpenAI endpoint and stamp each turn with KV-cache stats.

Serves index.html and one API route:

    POST /api/chat   {"messages": [...], "max_tokens": 96}

which forwards to vLLM ``/v1/chat/completions`` (stream=true) and passes the SSE stream
through unchanged — except that just before the final ``[DONE]`` it injects one extra event:

    data: {"kvcache_demo": {"fetch_hits": 12, "fetch_misses": 0, "lookup_hits": 12, ...}}

computed as the delta of the cache-server's Prometheus counters
(``kvcache_cache_requests_total{op,result}`` on the -metrics-addr endpoint, ADR 0025) scraped
before/after the turn. The frontend lights a "KV cache hit" badge when fetch_hits > 0 and
measures TTFT client-side (time to first streamed chunk).

Single-user demo: the before/after metrics delta is racy under concurrent traffic, fine here.
Stdlib only — no pip installs on top of the vLLM venv.

    python serve.py [--port 8001] [--vllm http://127.0.0.1:8000]
                    [--metrics http://127.0.0.1:9090/metrics]
"""

from __future__ import annotations

import argparse
import json
import re
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ARGS: argparse.Namespace  # set in main()

METRIC_RE = re.compile(
    r'^kvcache_cache_requests_total\{op="(?P<op>[a-z_]+)",result="(?P<result>[a-z]+)"\}\s+(?P<val>[0-9.e+]+)'
)


def scrape_cache_counters() -> dict[str, float]:
    """Return {"<op>_<result>": value} from the cache-server's /metrics, {} if unreachable."""
    try:
        with urllib.request.urlopen(ARGS.metrics, timeout=2) as resp:
            text = resp.read().decode("utf-8", errors="replace")
    except OSError:
        return {}
    out: dict[str, float] = {}
    for line in text.splitlines():
        m = METRIC_RE.match(line)
        if m:
            out[f"{m['op']}_{m['result']}"] = float(m["val"])
    return out


def first_served_model(base_url: str) -> str:
    with urllib.request.urlopen(f"{base_url}/v1/models", timeout=10) as resp:
        data = json.load(resp).get("data", [])
    if not data:
        raise RuntimeError(f"no models served at {base_url}/v1/models")
    return data[0]["id"]


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: object) -> None:  # quieter default log
        print(f"[chat-ui] {self.address_string()} {fmt % args}")

    def do_GET(self) -> None:
        if self.path not in ("/", "/index.html"):
            self.send_error(404)
            return
        body = (Path(__file__).parent / "index.html").read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        if self.path != "/api/chat":
            self.send_error(404)
            return
        try:
            req = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            self._proxy_chat(req)
        except BrokenPipeError:
            pass  # client navigated away mid-stream
        except Exception as exc:  # surface anything else to the UI, not a hung request
            self.send_error(502, f"upstream error: {exc}")

    def _proxy_chat(self, req: dict) -> None:
        before = scrape_cache_counters()
        body = json.dumps(
            {
                "model": ARGS.model,
                "messages": req["messages"],
                "max_tokens": int(req.get("max_tokens", 96)),
                "temperature": 0.0,
                "stream": True,
            }
        ).encode()
        upstream = urllib.request.Request(
            f"{ARGS.vllm}/v1/chat/completions",
            data=body,
            headers={"Content-Type": "application/json"},
        )
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        # SSE has no fixed length; close to delimit (keep HTTP/1.1 elsewhere for the static file).
        self.send_header("Connection", "close")
        self.end_headers()
        with urllib.request.urlopen(upstream, timeout=600) as resp:
            for raw in resp:
                line = raw.decode("utf-8", errors="replace")
                if line.strip() == "data: [DONE]":
                    break
                self.wfile.write(raw)
                self.wfile.flush()
        after = scrape_cache_counters()

        def delta(*keys: str) -> int:
            return int(sum(after.get(k, 0.0) - before.get(k, 0.0) for k in keys))

        stats = {
            "kvcache_demo": {
                # the server records connector loads as op="batch_fetch" (streamed) and
                # op="fetch" (unary); either one means KV bytes were actually served
                "fetch_hits": delta("batch_fetch_hit", "fetch_hit"),
                "fetch_misses": delta("batch_fetch_miss", "fetch_miss"),
                "lookup_hits": delta("lookup_hit"),
                "lookup_misses": delta("lookup_miss"),
                "metrics_reachable": bool(before or after),
            }
        }
        self.wfile.write(f"data: {json.dumps(stats)}\n\n".encode())
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()


def main() -> None:
    global ARGS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--vllm", default="http://127.0.0.1:8000")
    parser.add_argument("--metrics", default="http://127.0.0.1:9090/metrics",
                        help="cache-server Prometheus endpoint (-metrics-addr)")
    parser.add_argument("--model", default="",
                        help="served model id; default = first of GET /v1/models")
    ARGS = parser.parse_args()
    ARGS.model = ARGS.model or first_served_model(ARGS.vllm)
    print(f"[chat-ui] model={ARGS.model}  vllm={ARGS.vllm}  metrics={ARGS.metrics}")
    print(f"[chat-ui] open http://localhost:{ARGS.port}")
    ThreadingHTTPServer(("0.0.0.0", ARGS.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
