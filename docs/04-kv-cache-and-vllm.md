# 04 - KV Cache & vLLM

> **Status: Phase 1 implementation notes.** The deep procedure lives in the
> `vllm-integration` Skill. This doc records the project-specific integration decisions and the
> exact connector/benchmark shape.

## The One-Paragraph Version

During prefill, a transformer writes per-token **K and V tensors** for every layer/head into the
**KV cache** so it never recomputes attention for tokens it has seen. Requests that **share a
prompt prefix** can reuse those tensors, cutting time-to-first-token. vLLM does this per-node; this
project makes the cache shared across nodes. See [`00-project-plan.md`](./00-project-plan.md).

## Integration Decision

We integrate as a **custom `KVConnectorBase_V1`** in our own package, loaded via vLLM's dynamic
connector mechanism. No vLLM fork.

- Interface: `vllm/distributed/kv_transfer/kv_connector/v1/base.py`.
- References to inspect in the installed vLLM version: `OffloadingConnector`, `NixlConnector`,
  `LMCacheConnector`.
- `block_hash` is computed Python-side and sent to the Go cache as opaque bytes.

## Phase 1 Implementation Notes

The repo contains a connector package in `connector/src/kvcache_connector/`.

- **Version policy:** install the latest stable vLLM at implementation time, then record the exact
  version here before calling Phase 1 done. The connector API is experimental, so method signatures
  must be checked against the installed `KVConnectorBase_V1`.
- **Dynamic loading:** use `kv_connector="DistributedKVConnector"` and
  `kv_connector_module_path="kvcache_connector.connector"` in `KVTransferConfig`.
- **Extra config:** `cache_addr`, `model_id`, `block_tokens`, `deadline_ms`, `tenant_id`.
- **Hashing:** `kvcache_connector.hashing.chain_blocks` mirrors Go `internal/blockhash`: chained
  SHA-256, little-endian int32 token IDs, model ID folded into the seed, trailing partial block
  dropped.
- **Payload framing:** `kvcache_connector.codec` writes `KVC1 || header_len || JSON header ||
  tensor bytes`. The Go cache stores opaque bytes; only Python understands tensor layout.
- **Current gap to close during the WSL2/vLLM pass:** wire decoded payload frames into the live
  vLLM paged KV cache layout and save the correct request block ranges after prefill. This is the
  version-sensitive part that must be finalized against the installed vLLM source before the TTFT
  result is valid.

## Measuring TTFT

Use `connector/scripts/run_phase1_benchmark.py` and record results in
[`benchmarks/phase1-ttft.md`](./benchmarks/phase1-ttft.md). Measure baseline, external-cache cold,
and external-cache warm one-token generation latency. Treat it as a TTFT proxy unless the installed
vLLM exposes first-token timestamps directly.

Phase 1 is not complete until the benchmark shows warm repeated-prefix improvement and the
serialization gate from ADR 0015 is recorded. **(Session 3, 2026-05-25: this finalization + the TTFT
run are deferred to the Phase 4.5 GPU window — they need WSL2 + a local GPU. Phase 2 proceeds on the
synthetic load generator without them, per the GPU-decoupling design.)**

## Reading

[`00-project-plan.md`](./00-project-plan.md): vLLM docs, PagedAttention paper, KV-connector
references, RFC #14724, LMCache, and transformer background.
