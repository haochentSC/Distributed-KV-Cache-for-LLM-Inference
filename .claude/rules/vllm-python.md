---
description: vLLM connector (Python) conventions
paths:
  - "**/*.py"
  - "connector/**"
  - "vllm_connector/**"
---

# vLLM connector (Python) conventions

The integration is a **custom `KVConnectorBase_V1` in our own package, loaded via vLLM's dynamic
connector mechanism — no fork** (`docs/adr/0008`). Deep procedure: the `vllm-integration` skill.

- **Never fork vLLM.** Implement the connector against the abstract base at
  `vllm/distributed/kv_transfer/kv_connector/v1/base.py`. Pin a known-good vLLM version.
- Respect the interface's **scheduler-process vs worker-process** method split — don't do tensor
  I/O on the scheduler side or scheduling decisions on the worker side.
- Study `NixlConnector`, `OffloadingConnector`, `LMCacheConnector` before writing ours.
- **Keep the cache layer GPU-free:** the connector is the only GPU-adjacent code; it talks to the
  Go cache over gRPC using the **Python client generated from our proto** (don't hand-write a client).
- Compute `prefix_hash` here (SHA-256 over token IDs) and send opaque bytes to the cache (ADR 0010).
- This is the GPU/critical-path component — implementing the core hooks is **[guided]**.
