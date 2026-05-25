---
name: vllm-integration
description: Procedure for integrating the cache with vLLM via a custom KVConnectorBase_V1 (no fork), understanding the KV tensor layout, serializing tensors over gRPC, and measuring TTFT. Use in Phase 0 (study the interface) and Phase 1 (implement the connector), or when working on anything in the Python connector or the GPU benchmark.
---

# vLLM integration — procedure

Deep content for the vLLM side, kept out of always-on context. Pairs with
`docs/04-kv-cache-and-vllm.md` and the plan `docs/00-project-plan.md` §2/§6/§8.

**Integration decision (ADR 0008):** a **custom `KVConnectorBase_V1` in our own package, loaded via
vLLM's dynamic connector mechanism — no fork.** The connector hooks are **[guided]** work.

## Phase 0 — study before coding
1. Read the connector base: `vllm/distributed/kv_transfer/kv_connector/v1/base.py`. Map out which
   methods run in the **scheduler** process vs the **worker** process.
2. Read one reference connector end-to-end — start with `OffloadingConnector` (closest to "store KV
   elsewhere"), then skim `NixlConnector` (RDMA) and `LMCacheConnector`.
3. Run vLLM locally with TinyLlama-1.1B; turn on prefix caching; observe a cache hit. Capture the
   measurement in the Phase 0 notebook.
4. Write down the **KV block layout** vLLM hands the connector (shapes, dtype, per-layer/per-head
   organization) — this drives the proto and the serialization.

## Phase 1 — implement
- Implement the connector calling the **Python client generated from our proto** (don't hand-write
  a gRPC client). `prefix_hash` = SHA-256 over token IDs, sent as opaque bytes (ADR 0010).
- Keep tensor (de)serialization explicit and measured — serialization overhead can wipe out the
  cache benefit (plan Phase 1 risk). Benchmark with and without the cache.
- Fallback if a vLLM version is too coupled: pin a known-good release, or a thin Python coordinator.

## TTFT measurement (Phase 1 + Phase 4.5)
- Distinguish cold (first) vs warm (repeated-prefix) requests; report p50/p95/p99.
- Phase 4.5: GPU instances in the same VPC/AZ as the cache, run, capture the headline number,
  `terraform destroy` immediately.

## Sources
vLLM KV-transfer docs / DeepWiki, vLLM blog "Inside vLLM's New KV Offloading Connector" (Jan 2026),
RFC vllm-project/vllm#14724, LMCache, PagedAttention paper. Links in plan §8.
