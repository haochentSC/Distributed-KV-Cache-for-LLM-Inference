"""Layout-parameterized paged-KV block I/O.

vLLM's per-layer KV cache tensor enumerates physical blocks along one axis. The
rank and order of that tensor vary by attention backend and vLLM version, so we
parameterize on just that axis (``block_axis``) and copy whole-block *slabs*.
All version-specific knowledge collapses to one integer, discovered by the layout
probe (``tools/probe_kv_layout.py``).

A *block slab* is ``kv_tensor.select(block_axis, physical_block_id)``: the full KV
for one physical block — both K and V, every head, every position in the block.
Serialization concatenates one slab per layer into the existing codec frame
format (:mod:`kvcache_connector.codec`), so a single cache entry holds every
layer's KV for one logical block.

This module depends on ``torch`` but **not** on vLLM, so the block-copy mechanics
are unit-testable on a CPU-only machine with ordinary CPU tensors.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .codec import TensorFrame, decode_frames, encode_frames


@dataclass(frozen=True)
class BlockLayout:
    """Where physical blocks live in a per-layer KV tensor.

    ``block_axis`` is the only version-specific value; ``num_blocks`` and
    ``block_size`` are carried for validation and for inferring the axis.
    """

    block_axis: int
    num_blocks: int
    block_size: int


def infer_block_axis(shape: tuple[int, ...], num_blocks: int) -> int:
    """Return the axis whose extent equals the allocator's block count.

    Best-effort: if zero or more than one axis matches ``num_blocks`` the layout
    is ambiguous, so we raise rather than guess — the caller should fall back to
    an explicit ``block_axis`` from config (the probe prints the right value).
    """
    matches = [i for i, dim in enumerate(shape) if int(dim) == int(num_blocks)]
    if len(matches) != 1:
        raise ValueError(
            f"cannot infer block axis from shape {tuple(shape)} with "
            f"num_blocks={num_blocks}; matching axes={matches}. "
            "Pass block_axis explicitly (see tools/probe_kv_layout.py)."
        )
    return matches[0]


def extract_block(kv_tensor: Any, physical_block_id: int, block_axis: int) -> Any:
    """Contiguous CPU copy of one physical block's slab for a single layer."""
    slab = kv_tensor.select(block_axis, int(physical_block_id))
    return slab.detach().to("cpu").contiguous()


def apply_block(kv_tensor: Any, physical_block_id: int, block_axis: int, slab: Any) -> None:
    """Copy a slab back into the layer's paged KV at ``physical_block_id`` in place.

    ``select`` returns a view that shares storage with ``kv_tensor``, so ``copy_``
    writes through to the real paged cache. Shapes must match exactly — a mismatch
    means the layout/block_size assumption is wrong, and we fail loudly rather than
    corrupt the cache (upholds the correctness invariant, ADR 0016).
    """
    dst = kv_tensor.select(block_axis, int(physical_block_id))
    src = slab.to(device=dst.device, dtype=dst.dtype)
    if tuple(src.shape) != tuple(dst.shape):
        raise ValueError(
            f"slab shape {tuple(src.shape)} != block-slot shape {tuple(dst.shape)} "
            f"at block {physical_block_id}"
        )
    dst.copy_(src)


def serialize_block(
    layer_names: list[str],
    kv_caches: dict[str, Any],
    physical_block_id: int,
    layout: BlockLayout,
    extra: dict[str, Any] | None = None,
) -> bytes:
    """Pack every layer's slab for one physical block into one codec payload.

    Layer order is fixed by ``layer_names`` and recorded per-frame, so the load
    side reassembles by frame name rather than positional order.
    """
    frames = []
    payloads = []
    for name in layer_names:
        slab = extract_block(kv_caches[name], physical_block_id, layout.block_axis)
        data = _slab_bytes(slab)
        frames.append(TensorFrame(name=name, dtype=str(slab.dtype), shape=tuple(slab.shape), offset=0, nbytes=len(data)))
        payloads.append(data)
    return encode_frames(frames, payloads, extra=extra)


def _slab_bytes(slab: Any) -> bytes:
    """Raw little-endian bytes of a tensor, dtype-agnostic.

    vLLM's KV is bfloat16, which numpy cannot represent — so ``tensor.numpy()`` (the
    codec's default path) raises. Reinterpret as uint8 first; that works for fp16/bf16/
    fp8 alike. The frame records the real dtype, so the load side reconstructs exactly.
    """
    import torch

    return slab.detach().to("cpu").contiguous().flatten().view(torch.uint8).numpy().tobytes()


def deserialize_into(
    blob: bytes,
    kv_caches: dict[str, Any],
    physical_block_id: int,
    layout: BlockLayout,
) -> dict[str, Any]:
    """Copy a packed block payload back into the paged KV for one physical block.

    Returns the payload's ``extra`` dict (e.g. block hash / token-id digest) so the
    caller can re-check the correctness guard before trusting the copy.
    """
    import torch  # deferred: keeps pure callers import-light, matches codec contract

    frames, payloads, extra = decode_frames(blob)
    for frame, payload in zip(frames, payloads):
        if frame.name not in kv_caches:
            raise ValueError(f"payload layer {frame.name!r} not in registered kv_caches")
        dtype = _torch_dtype(frame.dtype)
        # frombuffer needs a writable buffer; bytearray copies the (read-only) memoryview.
        src = torch.frombuffer(bytearray(payload), dtype=dtype).reshape(frame.shape)
        apply_block(kv_caches[frame.name], physical_block_id, layout.block_axis, src)
    return extra


def _torch_dtype(dtype_str: str) -> Any:
    """Map a ``str(tensor.dtype)`` like ``"torch.float16"`` back to a torch dtype."""
    import torch

    name = dtype_str.split(".")[-1]
    dtype = getattr(torch, name, None)
    if not isinstance(dtype, torch.dtype):
        raise ValueError(f"unsupported tensor dtype {dtype_str!r}")
    return dtype
