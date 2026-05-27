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
