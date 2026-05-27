"""Opaque KV payload framing.

The Go service only stores bytes. This module owns the Python-side frame format so
the vLLM connector can preserve tensor layout without teaching Go about tensors.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
import json
import struct
from typing import Any


MAGIC = b"KVC1"
HEADER_LEN = struct.Struct("<I")


@dataclass(frozen=True)
class TensorFrame:
    name: str
    dtype: str
    shape: tuple[int, ...]
    offset: int
    nbytes: int


def encode_frames(frames: list[TensorFrame], payloads: list[bytes], extra: dict[str, Any] | None = None) -> bytes:
    if len(frames) != len(payloads):
        raise ValueError("frames and payloads must have the same length")

    offset = 0
    fixed_frames: list[TensorFrame] = []
    for frame, payload in zip(frames, payloads):
        fixed_frames.append(
            TensorFrame(
                name=frame.name,
                dtype=frame.dtype,
                shape=tuple(frame.shape),
                offset=offset,
                nbytes=len(payload),
            )
        )
        offset += len(payload)

    header = {
        "version": 1,
        "frames": [asdict(f) for f in fixed_frames],
        "extra": extra or {},
    }
    header_bytes = json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return MAGIC + HEADER_LEN.pack(len(header_bytes)) + header_bytes + b"".join(payloads)


def decode_frames(blob: bytes) -> tuple[list[TensorFrame], list[memoryview], dict[str, Any]]:
    if len(blob) < len(MAGIC) + HEADER_LEN.size or blob[: len(MAGIC)] != MAGIC:
        raise ValueError("invalid KV frame magic")
    header_len = HEADER_LEN.unpack(blob[len(MAGIC) : len(MAGIC) + HEADER_LEN.size])[0]
    header_start = len(MAGIC) + HEADER_LEN.size
    header_end = header_start + header_len
    if header_end > len(blob):
        raise ValueError("truncated KV frame header")
    header = json.loads(blob[header_start:header_end].decode("utf-8"))
    payload = memoryview(blob)[header_end:]

    frames: list[TensorFrame] = []
    payloads: list[memoryview] = []
    for raw in header.get("frames", []):
        frame = TensorFrame(
            name=raw["name"],
            dtype=raw["dtype"],
            shape=tuple(raw["shape"]),
            offset=int(raw["offset"]),
            nbytes=int(raw["nbytes"]),
        )
        end = frame.offset + frame.nbytes
        if frame.offset < 0 or end > len(payload):
            raise ValueError(f"tensor frame {frame.name!r} points outside payload")
        frames.append(frame)
        payloads.append(payload[frame.offset:end])
    return frames, payloads, dict(header.get("extra", {}))


def tensor_to_frame(name: str, tensor: Any) -> tuple[TensorFrame, bytes]:
    """Convert a torch-like tensor to a CPU bytes frame.

    The connector calls this with real torch tensors. Tests can pass any object that
    exposes detach(), cpu(), contiguous(), numpy(), dtype, and shape.
    """

    cpu = tensor.detach().cpu().contiguous()
    arr = cpu.numpy()
    data = arr.tobytes(order="C")
    return TensorFrame(name=name, dtype=str(cpu.dtype), shape=tuple(cpu.shape), offset=0, nbytes=len(data)), data
