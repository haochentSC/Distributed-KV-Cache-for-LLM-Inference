# ADR 0015 — KV payload as opaque framed bytes; serialization overhead is a Phase 1 gate

- **Status:** accepted
- **Date:** 2026-05-24 (architecture review, Session 2)
- **Deciders:** HC

## Context

The project's entire value rests on one inequality: **cache lookup + transfer < recompute.** KV
payloads are multi-MB (ADR 0012). If we serialize the tensor *through* protobuf, every read/write
pays CPU encode/decode/copy on megabytes — overhead that can erase the cache benefit. The plan
already flags this as a Phase 1 risk ("serialization overhead might wipe out the cache benefit;
measure carefully", plan §Phase 1). We were treating it as a measure-later detail; it is actually a
design decision + a gate. Refines ADR 0012. See `docs/01-architecture-overview.md` §4–5.

## Decision

- **Protobuf for metadata, opaque bytes for the tensor.** `WriteHeader` / `Lookup` / responses use
  protobuf; the KV itself travels as an **opaque, framed byte stream** in `KVChunk.data` — the
  server never parses or re-encodes tensor structure (consistent with the opaque-key principle,
  ADR 0010). Treat the payload as `[]byte`, copied as few times as possible on the hot path.
- **Serialization overhead is a Phase 1 exit gate, not a Phase 4 surprise.** Before declaring
  Phase 1 done, measure per-block encode/decode/copy cost and confirm `lookup + fetch ≪ recompute`
  on the target model. If overhead is material, address it in Phase 1 (reduce copies, tune chunk
  size) rather than discovering it under chaos in Phase 4.

## Alternatives considered

- **Serialize tensors as structured protobuf** (typed fields per layer/head) — schema-pretty, but
  forces CPU (de)serialization of MB-scale data every call and couples the Go server to tensor
  layout. Rejected: cost on the hot path, and it breaks the opaque-server design.
- **Defer the question to the Phase 4 benchmark** — risks finding the project's core premise broken
  late, after sharding/replication are built on top. Rejected: measure the load-bearing assumption
  first.
- **Zero-copy / RDMA transport now** (GPUDirect, NixlConnector-style) — the real long-term fix, but
  needs multi-GPU + RDMA hardware. Out of scope for v1; recorded as the v2 / "what I'd do at 10×"
  answer.

## Consequences

- The Go server stays an opaque byte mover end-to-end (key *and* value), simple and model-agnostic.
- Phase 1 gains an explicit, measurable exit criterion that de-risks the whole project early.
- Chunk size and copy count become the first tuning knobs (pairs with ADR 0012's chunk-size knob).
- If the gate fails and in-process tuning isn't enough, the fallback is the RDMA path — a scope
  change we'd take knowingly, not by accident.
