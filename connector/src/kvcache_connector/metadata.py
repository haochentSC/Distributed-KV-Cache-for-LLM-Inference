"""Connector metadata exchanged between vLLM scheduler and worker processes."""

from __future__ import annotations

from dataclasses import dataclass, field

from .hashing import Block


@dataclass
class RequestPlan:
    request_id: str
    model_id: str
    blocks: list[Block]
    hit_versions: list[int]
    num_external_tokens: int
    block_tokens: int


@dataclass
class DistributedKVConnectorMetadata:
    plans: dict[str, RequestPlan] = field(default_factory=dict)


@dataclass
class DistributedKVConnectorWorkerMetadata:
    saved_request_ids: set[str] = field(default_factory=set)

    def aggregate(self, other: "DistributedKVConnectorWorkerMetadata") -> "DistributedKVConnectorWorkerMetadata":
        self.saved_request_ids.update(other.saved_request_ids)
        return self
