# vLLM Connector (Python)

Phase 1 connector package for the Go KV cache. It is loaded through vLLM's dynamic
`KVConnectorBase_V1` mechanism; the project does not fork vLLM.

## Setup

From WSL2:

```bash
python -m pip install -e "connector[dev,vllm]"
make proto-python
```

The generated Python client lives under `connector/src/kvcache_proto/` and is produced from the
same proto as the Go cache server.

## Dynamic Loading

Use:

```python
KVTransferConfig(
    kv_connector="DistributedKVConnector",
    kv_role="kv_both",
    kv_connector_module_path="kvcache_connector.connector",
    kv_connector_extra_config={
        "cache_addr": "localhost:50051",
        "model_id": "TinyLlama/TinyLlama-1.1B-Chat-v1.0",
        "block_tokens": 16,
        "deadline_ms": 500,
        "tenant_id": "phase1",
    },
)
```

## Status

Hashing, gRPC client calls, opaque payload framing, and benchmark wiring are implemented. The final
vLLM pass must adapt `connector.py`'s load/save tensor-copy hooks to the exact paged KV layout of
the installed vLLM release, then run `docs/benchmarks/phase1-ttft.md`.
