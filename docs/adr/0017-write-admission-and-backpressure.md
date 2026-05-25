# ADR 0017 — Write admission & backpressure for multi-MB writes

- **Status:** proposed (design in Phase 1, harden in Phase 4)
- **Date:** 2026-05-24 (architecture review, Session 2)
- **Deciders:** HC (+ Claude)

## Context

Writes are large (multi-MB per block, ADR 0012) and bursty. Memory-pressure *eviction* (Phase 4)
reacts *after* bytes are resident — but a burst of concurrent large writes can drive a node to OOM
*before* eviction frees space, especially while streamed chunks accumulate in buffers. The design
has an eviction path but no **admission** path (deciding whether to accept a write at all under
load). This is a genuine reliability gap given the payload sizes. See
`docs/01-architecture-overview.md` §14 and plan §Phase 4.

## Decision (proposed — mechanism to be designed with HC)

Adopt explicit backpressure on the write path, layered:

- **Bound in-flight bytes** per stream and per node (a memory budget), so streamed chunks can't
  accumulate without limit.
- **Admission check before buffering:** above a high-water mark, **reject** new writes fast with a
  retryable status (e.g. `RESOURCE_EXHAUSTED`) rather than accepting and OOMing. A rejected write is
  safe — the client just recomputes, exactly the cache-miss baseline (ADR 0016 allows this).
- **Prefer reject-fast over deep queueing** for v1 (queues add latency and hide pressure); revisit
  bounded queueing if benchmarks show reject churn.

_Pending HC's design (Phase 1 skeleton, Phase 4 hardening):_ exact high/low-water marks, per-stream
vs per-node budgets, and whether eviction can be triggered *eagerly* to admit a high-value write.

## Alternatives considered

- **Rely on eviction alone** — reactive; can lose the race against a write burst and OOM the node.
  Rejected as the sole mechanism.
- **Unbounded accept + OS/Go GC handles it** — multi-MB buffers make OOM a real crash, not a slow
  GC. Rejected.
- **Deep request queue** — smooths bursts but adds tail latency and masks overload signals; not for
  v1.

## Consequences

- A node under pressure degrades by shedding writes (→ client recompute), never by crashing — which
  is exactly the reliability posture the project wants.
- `RESOURCE_EXHAUSTED` becomes part of the client contract; the load generator must handle/measure
  it (admission rejects become a benchmark metric, like eviction rate).
- Interacts with the eviction policy (Phase 5): admission and eviction are two halves of one
  capacity controller — keep them coherent.
