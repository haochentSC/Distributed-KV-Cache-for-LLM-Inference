# ADR 0008 — Integrate via a custom `KVConnectorBase_V1` (no fork)

- **Status:** accepted (Revision A, Session 1)
- **Date:** 2026-05-24
- **Deciders:** HC
- **Refines:** ADR 0003

## Context

The project plan framed Phase 1 vLLM integration as "fork vLLM vs. thin Python proxy" and rated
Risk 1 (vLLM internals) High/High. That framing predates vLLM's KV-transfer subsystem. Verified
in May 2026:

- vLLM exposes a first-class abstract interface **`KVConnectorBase_V1`**
  (`vllm/distributed/kv_transfer/kv_connector/v1/base.py`) for external KV offload, with a clean
  scheduler-process / worker-process method split.
- Since **June 2025**, vLLM supports **dynamic loading** of connectors — referenced by config from
  an external package.
- Production connectors exist to learn from: `NixlConnector` (RDMA), `OffloadingConnector`
  (CPU/disk), `LMCacheConnector`.

## Decision

Implement a **custom `KVConnectorBase_V1`** in our own package and load it via vLLM's dynamic
connector mechanism. **No vLLM fork, no PagedAttention patching.** It calls the Go cache over gRPC.

## Alternatives considered

- **Fork vLLM** — high coupling to internals and release cadence; unnecessary given the connector API.
- **Thin Python proxy** — kept as a fallback if a given vLLM version proves too coupled; pin a
  known-good release first.

## Consequences

- Risk 1 drops from High/High to **Low–Medium**; Phase 1 becomes "implement a connector," not
  "modify vLLM."
- Residual work is understanding the connector contract (method split + KV block layout) — budget
  Phase 0 reading.
- The plan's "v2 NCCL/RDMA" optimization already exists upstream as `NixlConnector` — a reference,
  and a possible later swap.
- Sources: vLLM KV-transfer docs / DeepWiki, vLLM blog "Inside vLLM's New KV Offloading Connector"
  (Jan 2026), RFC vllm-project/vllm#14724. See `00-project-plan.md` §8.
