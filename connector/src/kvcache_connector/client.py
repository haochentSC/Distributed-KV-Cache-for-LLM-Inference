"""Small gRPC wrapper for the Go KV cache service."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
import time

import grpc

from kvcache.v1 import kvcache_pb2, kvcache_pb2_grpc

from .hashing import Block


CHUNK_BYTES = 1 << 20


@dataclass(frozen=True)
class Presence:
    has_entry: bool
    version: int
    size_bytes: int


@dataclass(frozen=True)
class ClientStats:
    lookup_ms: float = 0.0
    fetch_ms: float = 0.0
    write_ms: float = 0.0
    bytes_fetched: int = 0
    bytes_written: int = 0


class KVCacheClient:
    def __init__(self, addr: str, deadline_ms: int = 200):
        self.addr = addr
        self.deadline_s = deadline_ms / 1000
        self.channel = grpc.insecure_channel(addr)
        self.stub = kvcache_pb2_grpc.KVCacheStub(self.channel)
        self.stats = ClientStats()

    def close(self) -> None:
        self.channel.close()

    def lookup(self, model_id: str, blocks: Iterable[Block]) -> list[Presence]:
        block_list = list(blocks)
        start = time.perf_counter()
        try:
            resp = self.stub.Lookup(
                kvcache_pb2.LookupRequest(
                    model_id=model_id,
                    block_hashes=[b.hash for b in block_list],
                ),
                timeout=self.deadline_s,
            )
        except grpc.RpcError:
            return [Presence(False, 0, 0) for _ in block_list]
        finally:
            self.stats = ClientStats(
                lookup_ms=self.stats.lookup_ms + elapsed_ms(start),
                fetch_ms=self.stats.fetch_ms,
                write_ms=self.stats.write_ms,
                bytes_fetched=self.stats.bytes_fetched,
                bytes_written=self.stats.bytes_written,
            )
        return [
            Presence(b.has_entry, b.version, b.size_bytes)
            for b in resp.blocks
        ]

    def fetch(self, model_id: str, block: Block, version: int = 0) -> bytes | None:
        start = time.perf_counter()
        out = bytearray()
        seen_last = False
        try:
            stream = self.stub.Fetch(
                kvcache_pb2.FetchRequest(
                    model_id=model_id,
                    block_hash=block.hash,
                    version=version,
                    token_ids=list(block.token_ids),
                ),
                timeout=self.deadline_s,
            )
            for chunk in stream:
                out.extend(chunk.data)
                seen_last = chunk.last
        except grpc.RpcError:
            return None
        finally:
            self.stats = ClientStats(
                lookup_ms=self.stats.lookup_ms,
                fetch_ms=self.stats.fetch_ms + elapsed_ms(start),
                write_ms=self.stats.write_ms,
                bytes_fetched=self.stats.bytes_fetched + len(out),
                bytes_written=self.stats.bytes_written,
            )
        if not seen_last:
            return None
        return bytes(out)

    def batch_fetch(
        self,
        model_id: str,
        blocks: Iterable[Block],
        versions: Iterable[int],
    ) -> list[bytes | None]:
        """Fetch many blocks in ONE round-trip; returns payloads parallel to ``blocks``.

        Collapses N sequential ``fetch`` RPCs into a single BatchFetch stream — the load
        path is latency-bound on the per-block RTT, so this is the dominant speedup
        (Phase 4.5). Each element is the block's bytes, or ``None`` for a miss / version
        or token mismatch / truncated stream — the caller recomputes those blocks. A
        whole-RPC failure returns all ``None`` (degrade to recompute, never wrong bytes).
        """
        block_list = list(blocks)
        ver_list = list(versions)
        start = time.perf_counter()
        buffers = [bytearray() for _ in block_list]
        found = [False] * len(block_list)
        done = [False] * len(block_list)
        total = 0
        try:
            stream = self.stub.BatchFetch(
                kvcache_pb2.BatchFetchRequest(
                    model_id=model_id,
                    blocks=[
                        kvcache_pb2.FetchBlock(
                            block_hash=b.hash,
                            version=v,
                            token_ids=list(b.token_ids),
                        )
                        for b, v in zip(block_list, ver_list)
                    ],
                ),
                timeout=self.deadline_s,
            )
            for chunk in stream:
                i = chunk.index
                if i >= len(block_list):
                    continue  # defensive: ignore an out-of-range index
                if not chunk.found:
                    done[i] = True
                    continue
                found[i] = True
                buffers[i].extend(chunk.data)
                total += len(chunk.data)
                if chunk.last:
                    done[i] = True
        except grpc.RpcError:
            return [None for _ in block_list]
        finally:
            self.stats = ClientStats(
                lookup_ms=self.stats.lookup_ms,
                fetch_ms=self.stats.fetch_ms + elapsed_ms(start),
                write_ms=self.stats.write_ms,
                bytes_fetched=self.stats.bytes_fetched + total,
                bytes_written=self.stats.bytes_written,
            )
        return [
            bytes(buffers[i]) if (found[i] and done[i]) else None
            for i in range(len(block_list))
        ]

    def write(
        self,
        model_id: str,
        block: Block,
        payload: bytes,
        tenant_id: str = "",
        recompute_cost: float = 0.0,
    ) -> int | None:
        start = time.perf_counter()

        def chunks():
            yield kvcache_pb2.WriteChunk(
                header=kvcache_pb2.WriteHeader(
                    model_id=model_id,
                    block_hash=block.hash,
                    token_ids=list(block.token_ids),
                    tenant_id=tenant_id,
                    recompute_cost=recompute_cost,
                    total_size=len(payload),
                )
            )
            for off in range(0, len(payload), CHUNK_BYTES):
                end = min(off + CHUNK_BYTES, len(payload))
                yield kvcache_pb2.WriteChunk(
                    chunk=kvcache_pb2.KVChunk(data=payload[off:end], last=end == len(payload))
                )

        try:
            resp = self.stub.Write(chunks(), timeout=self.deadline_s)
            return resp.version
        except grpc.RpcError:
            return None
        finally:
            self.stats = ClientStats(
                lookup_ms=self.stats.lookup_ms,
                fetch_ms=self.stats.fetch_ms,
                write_ms=self.stats.write_ms + elapsed_ms(start),
                bytes_fetched=self.stats.bytes_fetched,
                bytes_written=self.stats.bytes_written + len(payload),
            )


def elapsed_ms(start: float) -> float:
    return (time.perf_counter() - start) * 1000
