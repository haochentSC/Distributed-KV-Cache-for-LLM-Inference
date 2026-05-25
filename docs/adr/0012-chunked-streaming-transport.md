# ADR 0012 — Chunked streaming for KV tensor transport

- **Status:** accepted
- **Date:** 2026-05-24 (architecture session, Session 2)
- **Deciders:** HC

## Context

KV payloads are large — ~2 MB/token, so even one 16-token block is ~32 MB, well over gRPC's
default 4 MB max message size. `Fetch`/`Write` must move these without buffering whole tensors or
hitting the message cap. `Lookup` must stay cheap (metadata only). See `docs/01-architecture.md`.

## Decision

- `Fetch` **server-streams** `KVChunk` frames (bounded size, ~1–4 MB each, `last=true` terminator).
- `Write` **client-streams**: first message is a `WriteHeader`, then `KVChunk` data frames.
- `Lookup` is unary and returns metadata only — it never touches tensor bytes.

## Alternatives considered

- **Unary with a raised max message size (e.g. 64 MB)** — simplest to write, but buffers the whole
  tensor per call, gives no backpressure, and stays fragile as blocks grow. Acceptable only for a
  throwaway first spike; rejected as the design.

## Consequences

- Bounded memory and natural backpressure; no message-size wall.
- Handlers are streaming (slightly more code); the `Store` API can stay simple (hand it assembled
  bytes, or a reader) — keep streaming concerns in the server layer.
- Chunk size becomes a tunable for the Phase 4 throughput/latency benchmark.
