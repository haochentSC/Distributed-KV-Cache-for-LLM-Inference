# 04 — KV Cache & vLLM

> **Status: skeleton / index.** The deep procedure (how KV tensors are laid out, how to implement
> the connector, how to measure TTFT) lives in the **`vllm-integration` Skill**. This doc is the
> index + the project-specific integration decisions.

## The one-paragraph version

During prefill, a transformer writes per-token **K and V tensors** for every layer/head into the
**KV cache** so it never recomputes attention for tokens it has seen. Requests that **share a
prompt prefix** (system prompts, RAG context, few-shot examples) can **reuse** those tensors —
"prefix caching" — cutting time-to-first-token. vLLM does this per-node; this project makes the
cache **shared across nodes**. See [`00-project-plan.md` §2](./00-project-plan.md).

## Integration decision (Revision A — ADR 0008)

We integrate as a **custom `KVConnectorBase_V1`** in our own package, loaded via vLLM's dynamic
connector mechanism — **no fork**.

- Interface: `vllm/distributed/kv_transfer/kv_connector/v1/base.py` (scheduler-side vs
  worker-side method split).
- Reference connectors to study first: `NixlConnector` (RDMA), `OffloadingConnector` (CPU/disk),
  `LMCacheConnector`.
- `prefix_hash` is computed Python-side (SHA-256 over token IDs) and sent to the Go cache as
  **opaque bytes** (ADR 0010).

<!-- TODO (Phase 0/1): document the actual KV block layout the connector hands us, the exact
     hook methods we override, and how we serialize tensors over gRPC. -->

## Measuring TTFT

<!-- TODO (Phase 1 / 4.5): document the benchmark method — warm vs cold prefix, with/without
     cache, p50/p95/p99 — so the headline number is reproducible. -->

_Filled in Phase 1 and Phase 4.5._

## Reading

[`00-project-plan.md` §8](./00-project-plan.md): vLLM docs, PagedAttention paper, the KV-connector
references and RFC #14724, LMCache, the Illustrated Transformer.
