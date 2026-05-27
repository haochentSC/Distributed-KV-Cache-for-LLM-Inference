"""vLLM dynamic KV connector.

This module is intentionally imported only by vLLM. Helper modules and tests do not
import vLLM, so the package remains testable on CPU-only machines.
"""

from __future__ import annotations

from typing import Any

from vllm.distributed.kv_transfer.kv_connector.v1.base import (  # type: ignore
    KVConnectorBase_V1,
    KVConnectorMetadata,
    KVConnectorRole,
    KVConnectorWorkerMetadata,
)

from .client import KVCacheClient
from .codec import decode_frames, encode_frames, tensor_to_frame
from .hashing import Block, chain_blocks


class DistributedKVMetadata(KVConnectorMetadata):
    def __init__(self, plans: dict[str, dict[str, Any]] | None = None):
        self.plans = plans or {}


class DistributedKVWorkerMetadata(KVConnectorWorkerMetadata):
    def __init__(self, saved_request_ids: set[str] | None = None):
        self.saved_request_ids = saved_request_ids or set()

    def aggregate(self, other: KVConnectorWorkerMetadata) -> KVConnectorWorkerMetadata:
        if isinstance(other, DistributedKVWorkerMetadata):
            self.saved_request_ids.update(other.saved_request_ids)
        return self


class DistributedKVConnector(KVConnectorBase_V1):
    """Single-node external KV cache connector for Phase 1.

    The scheduler side performs cache lookup and tells vLLM how many full-block
    prompt tokens are externally available. The worker side provides synchronous
    hooks for loading/saving. vLLM's paged KV layout has changed across releases;
    the tensor copy helpers are deliberately small and isolated so the installed
    version can be adapted in one module after inspecting the live layout.
    """

    @classmethod
    def requires_piecewise_for_cudagraph(cls, extra_config: dict[str, Any]) -> bool:
        return bool(extra_config.get("piecewise_cudagraph", True))

    def __init__(self, vllm_config: "VllmConfig", role: KVConnectorRole, kv_cache_config: "KVCacheConfig"):
        super().__init__(vllm_config=vllm_config, role=role, kv_cache_config=kv_cache_config)
        cfg = vllm_config.kv_transfer_config
        extra = getattr(cfg, "kv_connector_extra_config", {}) or {}
        get_extra = getattr(cfg, "get_from_extra_config", None)
        if callable(get_extra):
            extra = {
                "cache_addr": get_extra("cache_addr", extra.get("cache_addr", "localhost:50051")),
                "model_id": get_extra("model_id", extra.get("model_id", "")),
                "block_tokens": get_extra("block_tokens", extra.get("block_tokens", 16)),
                "deadline_ms": get_extra("deadline_ms", extra.get("deadline_ms", 200)),
                "tenant_id": get_extra("tenant_id", extra.get("tenant_id", "")),
            }

        self.cache_addr = str(extra.get("cache_addr", "localhost:50051"))
        self.model_id = str(extra.get("model_id", ""))
        self.block_tokens = int(extra.get("block_tokens", 16))
        self.tenant_id = str(extra.get("tenant_id", ""))
        self.client = KVCacheClient(self.cache_addr, int(extra.get("deadline_ms", 200)))
        self.kv_caches: dict[str, Any] = {}
        self.pending_plans: dict[str, dict[str, Any]] = {}
        self.load_errors: set[int] = set()
        self.saved_request_ids: set[str] = set()

    def register_kv_caches(self, kv_caches: dict[str, Any]):
        self.kv_caches = dict(kv_caches)

    def start_load_kv(self, forward_context: "ForwardContext", **kwargs: Any) -> None:
        if not self.has_connector_metadata():
            return
        metadata = self._get_connector_metadata()
        if not isinstance(metadata, DistributedKVMetadata):
            return
        for plan in metadata.plans.values():
            for raw_block, version in zip(plan["blocks"], plan["versions"]):
                block = Block(hash=raw_block["hash"], token_ids=tuple(raw_block["token_ids"]))
                payload = self.client.fetch(plan["model_id"], block, version=version)
                if payload is None:
                    continue
                # The actual copy into vLLM's paged KV cache is version-specific.
                # Decode here so benchmark instrumentation can account for payload
                # framing; the live vLLM adaptation should map frames to block IDs.
                decode_frames(payload)

    def wait_for_layer_load(self, layer_name: str) -> None:
        return None

    def save_kv_layer(self, layer_name: str, kv_layer: Any, attn_metadata: "AttentionMetadata", **kwargs: Any) -> None:
        self._last_saved_layer = (layer_name, kv_layer)

    def wait_for_save(self):
        return None

    def build_connector_worker_meta(self) -> KVConnectorWorkerMetadata | None:
        if not self.saved_request_ids:
            return None
        out = DistributedKVWorkerMetadata(set(self.saved_request_ids))
        self.saved_request_ids.clear()
        return out

    def get_finished(self, finished_req_ids: set[str]) -> tuple[set[str] | None, set[str] | None]:
        return finished_req_ids & self.saved_request_ids, None

    def get_block_ids_with_load_errors(self) -> set[int]:
        out = set(self.load_errors)
        self.load_errors.clear()
        return out

    def get_num_new_matched_tokens(self, request: "Request", num_computed_tokens: int) -> tuple[int | None, bool]:
        tokens = _request_tokens(request)
        model_id = self.model_id or _request_model_id(request)
        blocks = chain_blocks(model_id, tokens, self.block_tokens)
        if not blocks:
            return 0, False

        presences = self.client.lookup(model_id, blocks)
        first_needed_block = num_computed_tokens // self.block_tokens
        hit_blocks = 0
        versions: list[int] = []
        for presence in presences[first_needed_block:]:
            if not presence.has_entry:
                break
            hit_blocks += 1
            versions.append(presence.version)
        matched_tokens = hit_blocks * self.block_tokens
        if matched_tokens == 0:
            return 0, False

        request_id = _request_id(request)
        hit_slice = blocks[first_needed_block : first_needed_block + hit_blocks]
        self.pending_plans[request_id] = {
            "request_id": request_id,
            "model_id": model_id,
            "blocks": [{"hash": b.hash, "token_ids": list(b.token_ids)} for b in hit_slice],
            "versions": versions,
            "num_external_tokens": matched_tokens,
        }
        return matched_tokens, False

    def update_state_after_alloc(self, request: "Request", blocks: "KVCacheBlocks", num_external_tokens: int):
        plan = self.pending_plans.get(_request_id(request))
        if plan is not None:
            plan["allocated_blocks"] = blocks

    def build_connector_meta(self, scheduler_output: "SchedulerOutput") -> KVConnectorMetadata:
        metadata = DistributedKVMetadata(self.pending_plans)
        self.pending_plans = {}
        return metadata

    def update_connector_output(self, connector_output: "KVConnectorOutput"):
        worker_meta = getattr(connector_output, "worker_metadata", None)
        if isinstance(worker_meta, DistributedKVWorkerMetadata):
            self.saved_request_ids.update(worker_meta.saved_request_ids)

    def request_finished(self, request: "Request", block_ids: list[int]) -> tuple[bool, dict[str, Any] | None]:
        # Phase 1 keeps ownership with vLLM; saving full request blocks requires the
        # live KV layout captured in docs/04. Returning False avoids delaying frees.
        return False, None

    def shutdown(self):
        self.client.close()


def encode_layer_payload(layer_name: str, kv_layer: Any, extra: dict[str, Any] | None = None) -> bytes:
    frame, data = tensor_to_frame(layer_name, kv_layer)
    return encode_frames([frame], [data], extra=extra)


def _request_tokens(request: Any) -> list[int]:
    for name in ("prompt_token_ids", "all_token_ids", "token_ids"):
        value = getattr(request, name, None)
        if value is not None:
            return list(value)
    prompt = getattr(request, "prompt", None)
    if prompt is not None:
        value = getattr(prompt, "token_ids", None)
        if value is not None:
            return list(value)
    return []


def _request_id(request: Any) -> str:
    return str(getattr(request, "request_id", getattr(request, "id", "")))


def _request_model_id(request: Any) -> str:
    return str(getattr(request, "model_id", getattr(request, "model", "default")))
