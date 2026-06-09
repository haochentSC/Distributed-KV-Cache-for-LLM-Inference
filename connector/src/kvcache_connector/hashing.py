"""Chained block hashing shared with the Go load generator.

The Go server treats block hashes as opaque. The connector owns tokenization-aware
hashing and must match internal/blockhash exactly.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import struct
from typing import Iterable


SEED_PREFIX = b"kvcache/v1\x00"


@dataclass(frozen=True)
class Block:
    hash: bytes
    token_ids: tuple[int, ...]


def chain_blocks(model_id: str, token_ids: Iterable[int], block_tokens: int) -> list[Block]:
    """Return one hash/token pair per full token block.

    block_hash[0] = SHA256(SHA256(seed || model_id) || little-endian int32 tokens)
    block_hash[i] = SHA256(block_hash[i-1] || little-endian int32 tokens)
    """

    tokens = tuple(int(t) for t in token_ids)
    if block_tokens <= 0 or len(tokens) < block_tokens:
        return []

    prev = hashlib.sha256(SEED_PREFIX + model_id.encode("utf-8")).digest()
    out: list[Block] = []
    full_blocks = len(tokens) // block_tokens
    for i in range(full_blocks):
        block_tokens_tuple = tokens[i * block_tokens : (i + 1) * block_tokens]
        payload = bytearray(prev)
        for token_id in block_tokens_tuple:
            payload += struct.pack("<i", token_id)
        prev = hashlib.sha256(payload).digest()
        out.append(Block(hash=prev, token_ids=block_tokens_tuple))
    return out


def chain_hashes(model_id: str, token_ids: Iterable[int], block_tokens: int) -> list[bytes]:
    return [b.hash for b in chain_blocks(model_id, token_ids, block_tokens)]


def shard_model_id(model_id: str, tp_rank: int, tp_world: int) -> str:
    """Namespace the cache key by tensor-parallel rank.

    Under tensor parallelism vLLM runs one worker per GPU rank, each holding a
    DISJOINT shard of the KV heads. The block hash, however, is seeded only from
    token_ids + the bare model_id (see chain_blocks), so it is identical across
    ranks — every rank would otherwise key the same (model_id, hash) entry on the
    server and clobber each other's shard, then serve one rank's bytes to all of
    them. We keep the hash rank-independent (it must match across scheduler/worker)
    and instead fold the rank into the OPAQUE model_id the connector sends, so each
    rank owns a distinct server entry. The Go server is untouched (ADR 0010): it
    only ever sees an opaque string.

    World size 1 (single GPU) returns the bare model_id unchanged, so the
    single-GPU path stays byte-identical to the Phase 4.5 results.

    The scheduler side (presence/lookup) has no rank; it queries canonical rank 0
    and relies on the lockstep invariant — all ranks save the same full blocks in
    the same forward, so rank 0's presence implies every shard exists. A partial
    write degrades to recompute via the ADR 0016 load guard, never to a wrong serve.
    """
    if tp_world <= 1:
        return model_id
    return f"{model_id}#tp{tp_rank}/{tp_world}"
