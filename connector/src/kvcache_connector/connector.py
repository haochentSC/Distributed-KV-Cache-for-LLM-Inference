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

from .blockio import BlockLayout, deserialize_into, infer_block_axis, serialize_block
from .client import KVCacheClient
from .codec import encode_frames, tensor_to_frame
from .hashing import Block, chain_blocks, shard_model_id


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
    """External KV cache connector (vLLM dynamic connector, no fork — ADR 0008).

    The scheduler side performs cache lookup and tells vLLM how many full-block
    prompt tokens are externally available. The worker side provides synchronous
    hooks for loading/saving. vLLM's paged KV layout has changed across releases;
    the tensor copy helpers are deliberately small and isolated so the installed
    version can be adapted in one module after inspecting the live layout.

    Tensor parallelism: vLLM runs one worker per GPU rank, each holding a disjoint
    KV-head shard. Each rank reads/writes its own shard under a rank-namespaced key
    (shard_model_id); the scheduler checks presence under canonical rank 0 and trusts
    the lockstep invariant (all ranks save the same full blocks in the same forward).
    A missing/partial shard degrades to recompute via the ADR 0016 load guard, never
    to a wrong serve. World size 1 leaves the single-GPU key path unchanged.
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
                # Probe/layout keys must survive this rebuild, else self.probe is always False.
                "probe": get_extra("probe", extra.get("probe", False)),
                "probe_out": get_extra("probe_out", extra.get("probe_out", "kv_layout_probe.json")),
                "block_axis": get_extra("block_axis", extra.get("block_axis", None)),
                "num_blocks": get_extra("num_blocks", extra.get("num_blocks", None)),
            }

        self.cache_addr = str(extra.get("cache_addr", "localhost:50051"))
        self.model_id = str(extra.get("model_id", ""))
        self.block_tokens = int(extra.get("block_tokens", 16))
        self.tenant_id = str(extra.get("tenant_id", ""))

        # Tensor-parallel keying (see hashing.shard_model_id). The TP world size is a
        # global config value available on BOTH the scheduler and worker connectors;
        # the per-worker rank is only known inside a worker process (set in
        # register_kv_caches). Scheduler-side lookups use canonical rank 0.
        self.tp_world = _tp_world_size(vllm_config)
        self.tp_rank = 0
        self.client = KVCacheClient(self.cache_addr, int(extra.get("deadline_ms", 200)))
        self.kv_caches: dict[str, Any] = {}
        self.layer_names: list[str] = []
        self.layout: BlockLayout | None = None
        self.pending_plans: dict[str, dict[str, Any]] = {}
        self.load_errors: set[int] = set()
        self.saved_request_ids: set[str] = set()
        # block_table for the current forward, captured worker-side in save_kv_layer and
        # consumed in wait_for_save (which gets no attn_metadata of its own).
        self._save_block_rows: list[list[int]] | None = None
        self._warned: set[str] = set()

        # Layout probe (Phase 4.5 Step 1): set extra_config {"probe": true} to dump the
        # live paged-KV layout + connector-API surface once, then drive the wiring from it.
        # block_axis / num_blocks overrides let us pin the layout when inference is ambiguous.
        self.probe = bool(extra.get("probe", False))
        self.probe_out = str(extra.get("probe_out", "kv_layout_probe.json"))
        self.block_axis_override = extra.get("block_axis", None)
        self.num_blocks_override = extra.get("num_blocks", None)
        self.kv_cache_config = kv_cache_config
        self._probe_done: dict[str, bool] = {"layout": False, "load_ctx": False, "save_ctx": False}

    def register_kv_caches(self, kv_caches: dict[str, Any]):
        self.kv_caches = dict(kv_caches)
        self.layer_names = list(self.kv_caches.keys())
        # This runs inside a worker process, after the TP group is initialized, so the
        # rank is now resolvable. Each rank holds its own KV-head shard (the registered
        # tensors are already this rank's slice), and writes/reads it under its own key.
        self.tp_rank = _tp_rank()
        self._warn_once("rank_resolved", f"register_kv_caches: resolved tp_rank={self.tp_rank}/{self.tp_world}")
        self._init_layout()
        if self.probe:
            self._probe_layout()

    def start_load_kv(self, forward_context: "ForwardContext", **kwargs: Any) -> None:
        if self.probe and not self._probe_done["load_ctx"]:
            self._probe_obj("start_load_kv.forward_context", forward_context)
            self._probe_obj("start_load_kv.kwargs", kwargs)
            self._probe_done["load_ctx"] = True
        if self.layout is None or not self.has_connector_metadata():
            return
        metadata = self._get_connector_metadata()
        if not isinstance(metadata, DistributedKVMetadata):
            return
        # Only plans that actually have external hit blocks to load.
        plans = [p for p in metadata.plans.values() if p.get("blocks")]
        if not plans:
            return
        rows = self._block_table_rows(self._attn_md_from_ctx(forward_context))
        if rows is None:
            return
        # We map plan i -> block_table row i. Unambiguous only when counts match (the
        # single-request batches the benchmark drives). Otherwise skip = miss = recompute.
        if len(plans) != len(rows):
            self._warn_once("load_batch", f"load: plans={len(plans)} != block_table rows={len(rows)}; skipping")
            return
        for plan, row in zip(plans, rows):
            self._load_plan(plan, row)

    def wait_for_layer_load(self, layer_name: str) -> None:
        return None

    def save_kv_layer(self, layer_name: str, kv_layer: Any, attn_metadata: "AttentionMetadata", **kwargs: Any) -> None:
        if self.probe and not self._probe_done["save_ctx"]:
            self._probe_obj("save_kv_layer.layer_name", layer_name)
            self._probe_tensor("save_kv_layer.kv_layer", kv_layer)
            self._probe_obj("save_kv_layer.attn_metadata", attn_metadata)
            self._probe_obj("save_kv_layer.kwargs", kwargs)
            self._probe_done["save_ctx"] = True
        # The block table is per-request (identical across layers); capture it once so
        # wait_for_save can map logical blocks -> physical block ids. kv_layer aliases the
        # registered cache tensor, so all layers are already readable from self.kv_caches.
        if self._save_block_rows is None:
            self._save_block_rows = self._block_table_rows(attn_metadata)

    def wait_for_save(self):
        rows = self._save_block_rows
        self._save_block_rows = None
        if self.layout is None or not self.kv_caches or rows is None:
            return
        if not self.has_connector_metadata():
            return
        metadata = self._get_connector_metadata()
        if not isinstance(metadata, DistributedKVMetadata):
            return
        plans = [p for p in metadata.plans.values() if p.get("save_blocks")]
        if not plans:
            return
        if len(plans) != len(rows):
            self._warn_once("save_batch", f"save: plans={len(plans)} != block_table rows={len(rows)}; skipping")
            return
        for plan, row in zip(plans, rows):
            self._save_plan(plan, row)

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

        first_needed_block = num_computed_tokens // self.block_tokens
        # Presence under canonical rank 0; the lockstep invariant makes it stand in for
        # all shards (see shard_model_id). The block hashes themselves stay rank-agnostic.
        presences = self.client.lookup(shard_model_id(model_id, 0, self.tp_world), blocks)
        hit_blocks = 0
        versions: list[int] = []
        for presence in presences[first_needed_block:]:
            if not presence.has_entry:
                break
            hit_blocks += 1
            versions.append(presence.version)
        matched_tokens = hit_blocks * self.block_tokens

        # vLLM needs at least one token left to compute; never report a 100% external
        # match (only possible when the prompt is an exact multiple of block_tokens).
        if matched_tokens >= len(tokens) and hit_blocks > 0:
            hit_blocks -= 1
            versions = versions[:hit_blocks]
            matched_tokens = hit_blocks * self.block_tokens

        # Record BOTH sides every call: the external hit prefix to load, and the full
        # blocks beyond it (newly computed this request) to save back. Recording the save
        # plan even on a cold miss is what lets the cache get populated at all.
        request_id = _request_id(request)
        hit_start = first_needed_block
        save_start = hit_start + hit_blocks
        hit_slice = blocks[hit_start:save_start]
        save_slice = blocks[save_start:]
        self.pending_plans[request_id] = {
            "request_id": request_id,
            "model_id": model_id,
            "hit_start": hit_start,
            "blocks": [{"hash": b.hash, "token_ids": list(b.token_ids)} for b in hit_slice],
            "versions": versions,
            "num_external_tokens": matched_tokens,
            "save_start": save_start,
            "save_blocks": [{"hash": b.hash, "token_ids": list(b.token_ids)} for b in save_slice],
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

    # ----------------------------------------------------------- save / load

    def _load_plan(self, plan: dict[str, Any], row: list[int]) -> None:
        """Fetch all external hit blocks in ONE round-trip and copy each into its slot.

        We issue a single BatchFetch for the whole plan (not one RPC per block). NOTE
        (Phase 4.5): profiling showed the transport is NOT the single-node bottleneck —
        batching the RPCs barely moved TTFT. The cost is the per-block, per-layer
        deserialize + host->device copy (unpinned under WSL2, ``pin_memory=False``), of
        which there are ``num_layers * num_hit_blocks`` small ones. Set
        ``KVC_LOAD_PROFILE=1`` to print a one-shot fetch-vs-copy breakdown.

        Payloads come back parallel to ``blocks``; a ``None`` entry (miss / mismatch) is
        skipped and that block is recomputed by vLLM.
        """
        import os
        import time

        profile = os.environ.get("KVC_LOAD_PROFILE") == "1"
        hit_start = int(plan.get("hit_start", 0))
        model_id = plan["model_id"]
        blocks = [
            Block(hash=rb["hash"], token_ids=tuple(rb["token_ids"]))
            for rb in plan["blocks"]
        ]
        t0 = time.perf_counter()
        # This rank loads its OWN KV-head shard, keyed by its rank (see shard_model_id).
        shard_id = shard_model_id(model_id, self.tp_rank, self.tp_world)
        self._warn_once("load_key", f"load shard key: {shard_id} (tp_rank={self.tp_rank}/{self.tp_world})")
        payloads = self.client.batch_fetch(shard_id, blocks, plan["versions"])
        t_fetch = time.perf_counter() - t0

        n_copied = 0
        t1 = time.perf_counter()
        for j, (block, payload) in enumerate(zip(blocks, payloads)):
            if payload is None:
                continue
            logical = hit_start + j
            if logical >= len(row):
                continue
            physical_id = int(row[logical])
            try:
                extra = deserialize_into(payload, self.kv_caches, physical_id, self.layout)
            except Exception as exc:
                self.load_errors.add(physical_id)
                self._warn_once("load_copy", f"deserialize failed @block {physical_id}: {exc!r}")
                continue
            n_copied += 1
            self._warn_once("load_active", "load path active: first hit block copied into paged cache")
            # Correctness guard (ADR 0016): the payload must be the block we asked for.
            stamped = extra.get("h")
            if stamped is not None and stamped != block.hash.hex():
                self.load_errors.add(physical_id)
                self._warn_once("load_mismatch", "load: payload hash != requested block hash")

        if profile and n_copied:
            # The H2D copy_ is async; sync so t_copy reflects the real transfer cost, not
            # just the time to enqueue it. Diagnostic only — gated behind KVC_LOAD_PROFILE.
            try:
                import torch

                if torch.cuda.is_available():
                    torch.cuda.synchronize()
            except Exception:
                pass
            t_copy = time.perf_counter() - t1
            self._warn_once(
                "load_profile",
                f"load breakdown: {n_copied} blocks x {len(self.layer_names)} layers | "
                f"batch_fetch={t_fetch * 1000:.1f}ms  deserialize+copy+sync={t_copy * 1000:.1f}ms",
            )

    def _save_plan(self, plan: dict[str, Any], row: list[int]) -> None:
        """Serialize each newly-computed full block from the paged cache and write it back."""
        save_start = int(plan.get("save_start", 0))
        model_id = plan["model_id"]
        # This rank saves its OWN KV-head shard, keyed by its rank (see shard_model_id).
        shard_id = shard_model_id(model_id, self.tp_rank, self.tp_world)
        self._warn_once("save_key", f"save shard key: {shard_id} (tp_rank={self.tp_rank}/{self.tp_world})")
        for j, raw_block in enumerate(plan["save_blocks"]):
            logical = save_start + j
            if logical >= len(row):
                continue
            physical_id = int(row[logical])
            block = Block(hash=raw_block["hash"], token_ids=tuple(raw_block["token_ids"]))
            try:
                payload = serialize_block(
                    self.layer_names, self.kv_caches, physical_id, self.layout,
                    extra={"h": block.hash.hex()},
                )
            except Exception as exc:
                self._warn_once("save_serialize", f"serialize failed @block {physical_id}: {exc!r}")
                continue
            recompute_cost = float(len(block.token_ids))  # GDSF cost model (plan §3.5)
            version = self.client.write(
                shard_id, block, payload, tenant_id=self.tenant_id, recompute_cost=recompute_cost
            )
            if version is not None:
                self.saved_request_ids.add(plan["request_id"])
                self._warn_once("save_active", "save path active: first computed block written to cache")

    @staticmethod
    def _attn_md_from_ctx(forward_context: Any) -> Any:
        """vLLM hands attn metadata as a per-layer dict in start_load_kv; any entry has the
        request block table (it is identical across layers). Return one representative."""
        md = getattr(forward_context, "attn_metadata", None)
        if isinstance(md, dict):
            return next(iter(md.values()), None)
        return md

    @staticmethod
    def _block_table_rows(attn_metadata: Any) -> list[list[int]] | None:
        """block_table[req][i] = physical block id of the request's i-th logical block."""
        if attn_metadata is None:
            return None
        bt = getattr(attn_metadata, "block_table", None)
        if bt is None:
            return None
        try:
            rows = bt.detach().to("cpu").tolist()
        except Exception:
            return None
        if rows and not isinstance(rows[0], list):  # 1-D (single request) -> wrap
            return [rows]
        return rows

    def _warn_once(self, key: str, msg: str) -> None:
        if key not in self._warned:
            self._warned.add(key)
            print(f"[kvc] {msg}", flush=True)

    # ------------------------------------------------------------------ layout

    def _init_layout(self) -> None:
        """Best-effort discovery of the per-layer block axis (Phase 4.5 Step 1).

        ``block_axis`` is the only version-specific value we need to copy block
        slabs (see blockio). Prefer explicit overrides; otherwise infer from the
        tensor shape + a num-blocks hint. Leaves ``self.layout`` None (and logs)
        when ambiguous, so a wrong guess never silently corrupts the cache.
        """
        if not self.kv_caches:
            return
        sample = next(iter(self.kv_caches.values()))
        shape = tuple(int(d) for d in getattr(sample, "shape", ()) or ())
        if not shape:
            return
        if self.block_axis_override is not None:
            axis = int(self.block_axis_override)
            self.layout = BlockLayout(axis, int(shape[axis]), self.block_tokens)
            return
        num_blocks = self._num_blocks_hint(shape)
        if not num_blocks:
            return
        try:
            axis = infer_block_axis(shape, num_blocks)
        except ValueError as exc:
            print(f"[kvc] block-axis inference failed: {exc}")
            return
        self.layout = BlockLayout(axis, int(shape[axis]), self.block_tokens)

    def _num_blocks_hint(self, shape: tuple[int, ...]) -> int | None:
        """Try to learn the physical block count from vLLM's kv_cache_config.

        Field names have drifted across vLLM releases; probe several, fall back to
        the explicit override. The probe dump records the real source for pinning.
        """
        if self.num_blocks_override is not None:
            return int(self.num_blocks_override)
        cfg = self.kv_cache_config
        for name in ("num_blocks", "num_gpu_blocks", "num_kv_blocks"):
            value = getattr(cfg, name, None)
            if isinstance(value, int) and value > 0:
                return value
        return None

    # ------------------------------------------------------------------- probe

    def _probe_layout(self) -> None:
        record: dict[str, Any] = {
            "block_tokens": self.block_tokens,
            # Under TP the per-rank KV tensor should have num_kv_heads/tp_world heads;
            # the dump lets the paid window confirm heads are sharded before any real run.
            "tp_rank": self.tp_rank,
            "tp_world": self.tp_world,
            "layers": {},
        }
        for name, tensor in self.kv_caches.items():
            record["layers"][name] = self._tensor_desc(tensor)
        record["inferred_layout"] = (
            None if self.layout is None
            else {"block_axis": self.layout.block_axis, "num_blocks": self.layout.num_blocks, "block_size": self.layout.block_size}
        )
        record["kv_cache_config"] = self._obj_desc(self.kv_cache_config)
        self._probe_write("register_kv_caches", record)
        self._probe_done["layout"] = True

    def _probe_obj(self, label: str, obj: Any) -> None:
        self._probe_write(label, self._obj_desc(obj))

    def _probe_tensor(self, label: str, tensor: Any) -> None:
        self._probe_write(label, self._tensor_desc(tensor))

    @staticmethod
    def _tensor_desc(tensor: Any) -> dict[str, Any]:
        try:
            return {
                "type": type(tensor).__name__,
                "shape": [int(d) for d in tensor.shape],
                "dtype": str(tensor.dtype),
                "stride": [int(s) for s in tensor.stride()] if hasattr(tensor, "stride") else None,
                "device": str(getattr(tensor, "device", "")),
                "is_contiguous": bool(tensor.is_contiguous()) if hasattr(tensor, "is_contiguous") else None,
            }
        except Exception as exc:  # never let the probe break a run
            return {"type": type(tensor).__name__, "error": repr(exc)}

    @staticmethod
    def _obj_desc(obj: Any, depth: int = 0) -> Any:
        """Defensive introspection: shape/dtype for tensors, attr summary for objects."""
        if obj is None or isinstance(obj, (bool, int, float, str, bytes)):
            return obj
        if hasattr(obj, "shape") and hasattr(obj, "dtype"):
            return DistributedKVConnector._tensor_desc(obj)
        if isinstance(obj, (list, tuple)):
            head = [DistributedKVConnector._obj_desc(x, depth + 1) for x in list(obj)[:4]]
            return {"type": type(obj).__name__, "len": len(obj), "head": head}
        if isinstance(obj, dict):
            return {"type": "dict", "keys": [str(k) for k in list(obj)[:20]]}
        attrs: dict[str, Any] = {}
        if depth < 2:
            for name in sorted(n for n in dir(obj) if not n.startswith("_")):
                try:
                    value = getattr(obj, name)
                except Exception:
                    continue
                if callable(value):
                    continue
                attrs[name] = DistributedKVConnector._obj_desc(value, depth + 1)
        return {"type": type(obj).__name__, "attrs": attrs}

    def _probe_write(self, label: str, record: Any) -> None:
        import json

        line = json.dumps({"probe": label, "data": record}, default=str, indent=2)
        print(f"[kvc-probe] {label}\n{line}", flush=True)
        try:
            with open(self.probe_out, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        except Exception as exc:
            print(f"[kvc-probe] could not write {self.probe_out}: {exc!r}")


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


def _tp_world_size(vllm_config: Any) -> int:
    """Tensor-parallel world size from config (available on scheduler AND worker).

    Defensive: field names have drifted across vLLM releases, and a missing value
    means single-GPU (world 1), which keeps the bare-model_id path unchanged.
    """
    pc = getattr(vllm_config, "parallel_config", None)
    size = getattr(pc, "tensor_parallel_size", None)
    if isinstance(size, int) and size > 0:
        return size
    return 1


def _tp_rank() -> int:
    """This worker's tensor-parallel rank, or 0 when TP is not initialized.

    Only callable inside a worker process once the TP group exists (i.e. from
    register_kv_caches onward). On the scheduler side, or under CPU tests, the
    import/lookup fails and we fall back to canonical rank 0.
    """
    try:
        from vllm.distributed.parallel_state import (  # type: ignore
            get_tensor_model_parallel_rank,
        )

        return int(get_tensor_model_parallel_rank())
    except Exception:
        return 0
