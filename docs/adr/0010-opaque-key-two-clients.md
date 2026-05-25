# ADR 0010 — Opaque-bytes `prefix_hash` key; two clients generated from one proto

- **Status:** accepted (Revision D, Session 1)
- **Date:** 2026-05-24
- **Deciders:** HC

## Context

The cache key is the *tokenized* prefix hash (SHA-256 over token IDs), which differs from raw
text. Tokenization requires the model's tokenizer, which lives on the Python/vLLM side. The cache
server is Go (ADR 0001) and the synthetic load generator is also Go, while the vLLM connector is
Python (ADR 0008). The plan read as if a single client library served both.

## Decision

- In the proto, `prefix_hash` is an **opaque 32-byte `bytes`** field. The Go cache server treats
  it as an opaque key and **never tokenizes**.
- Generate **two clients from the one proto**: a **Go** client (synthetic load generator, and the
  shard-routing logic it shares) and a **Python** client (the vLLM connector).

## Alternatives considered

- **Server-side tokenization** — would drag a Python tokenizer / model dependency into the Go
  server; rejected.
- **One client only** — impossible across the Go/Python boundary; the proto-generated split is the
  natural design.

## Consequences

- Clean separation: tokenization stays Python-side; the Go server is a pure byte-keyed store.
- The synthetic load generator can fabricate keys without any tokenizer (just hash random/clustered
  token-id sequences) — which is what makes GPU-free testing possible.
- Shard-routing rules (consistent hashing) must be implemented consistently in both clients;
  factor the ring logic so the two stay in sync (Phase 2).
