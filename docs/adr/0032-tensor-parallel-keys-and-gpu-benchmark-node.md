# ADR 0032 — Tensor-parallel KV cache keys + a GPU benchmark node (prep for the distributed TTFT window)

- **Status:** accepted (code prepped). **Scope update (2026-06-10, ADR 0033):** AWS partially approved
  the G/VT Spot quota to **8 vCPUs** (standard-process GPU ceiling), so the AWS default benchmark is now
  **single-GPU** (`g5.2xlarge`, `--tensor-parallel-size 1`, 7-8B). The TP=4 / 30B path below is kept in
  code but **deferred** to an AWS-sales quota or a GPU-specialized cloud (the keying here is
  provider-agnostic). At `world = 1` everything below reduces to the bare `model_id`, so single-GPU is unaffected.
- **Date:** 2026-06-09 (Phase 4.5 — distributed prep)
- **Deciders:** HC (+ Claude)
- **Builds on:** ADR 0031 (single-node GPU TTFT; the deficit closes with model size, headline deferred
  to the distributed/cloud path), ADR 0008/0010 (no-fork `KVConnectorBase_V1`, opaque block bytes),
  ADR 0016 (correctness invariant), ADR 0028 (AWS cluster Terraform layout), ADR 0006/0009 (Spot for
  workers, on-demand for etcd), ADR 0027 (S3 cold tier)

## Context

ADR 0031 measured the single-node TTFT honestly: the external cache *loses* on a laptop at ≤3B because
of WSL2 unpinned memory + a Python/protobuf hot path + a KV cache throttled to 0.09 GiB — all
environmental, with the deficit shrinking sharply as the model grows (−169% at 1.1B → ~−48% at 3B).
The headline `[measured]` number lives on the **distributed/cloud GPU path**, which is the one
remaining paid window (bundled with cold-tier verify, AWS chaos, and the 5a/5b eviction re-run).

HC chose the most ambitious headline: a **30B-class model with multi-GPU tensor parallelism (TP)** on
`g5.12xlarge` (4× A10G, 96 GB). TP is what forces new engineering, because under it vLLM runs **one
worker process per GPU rank, each holding a disjoint shard of the KV heads** (`num_kv_heads / tp`).
Our connector already fans out per rank (vLLM constructs one connector instance per worker), but the
block hash is seeded only from `(token_ids, model_id)` and is therefore **identical across ranks**
(`internal/blockhash` ≡ `hashing.chain_blocks`). Without a change, every rank would key the same
`(model_id, block_hash)` server entry and clobber the others' shard — then serve one rank's bytes to
all of them. That is a correctness landmine (a wrong serve, the one thing ADR 0016 forbids).

This stage is **prep only — no spend**: write and locally verify everything, so the paid window is
short. The TP behavior and the TTFT number are validated *in* that window (see the runbook).

## Decision

**1. Rank-namespace the cache key, in the connector, leaving the Go server untouched.** The block hash
stays rank-independent (it must match across the scheduler that computes it and every worker that uses
it). Instead we fold the rank into the **opaque `model_id`** the connector sends —
`hashing.shard_model_id(model_id, rank, world)` returns `"<model_id>#tp{rank}/{world}"`, or the bare
`model_id` when `world ≤ 1` (so the single-GPU path is byte-identical to ADR 0031). Each rank thus
owns a distinct server entry for its shard. Per ADR 0010 the server only ever sees an opaque string;
no server, proto, or routing change.

**2. Presence/lookup is rank-agnostic; rely on a lockstep invariant.** `get_num_new_matched_tokens`
runs scheduler-side, which has **no rank** (it is not a GPU worker). It checks presence under
**canonical rank 0** (`shard_model_id(model_id, 0, world)`; `world` is global config, available on
both sides via `parallel_config.tensor_parallel_size`). The invariant that makes this sound: **all
ranks compute and save the same full blocks in the same forward**, so rank 0's presence implies every
shard exists.

**3. The safety net is the existing ADR 0016 guard, not a distributed transaction.** If presence and
the per-rank payloads ever disagree (a partial/failed write on one rank), the load path's hash guard
plus `get_block_ids_with_load_errors()` make vLLM **recompute** that block. A missing or mismatched
shard degrades to recompute — never to a wrong serve. We keep the cheap optimistic path and let
correctness fall back, rather than paying for cross-rank coordination.

**4. A cost-guarded GPU benchmark node in Terraform.** New `terraform/cluster/gpu.tf`, `count =
var.gpu_count` with **`gpu_count` defaulting to 0**, so a normal `apply` creates nothing and the only
hourly-billed GPU resource never appears by accident. `g5.12xlarge` Spot, in the existing public
subnet; its own SG (SSH from the operator + all egress). **No cache-side rule is needed** —
`aws_security_group.cache` already allows gRPC 50051 from the whole `vpc_cidr`, and the node shares
the subnet, so it reaches a cache node's private IP directly. The AMI is a Deep Learning AMI (NVIDIA
driver + PyTorch) resolved from SSM, with a `gpu_ami_id` override for when the SSM path drifts. No
IAM: vLLM + the connector are pip-installed in the runbook, so the node needs neither ECR nor S3.

**5. A distributed benchmark driver + a cold-tier verify, reusing what exists.**
`connector/scripts/run_distributed_benchmark.py` mirrors the single-node driver but adds
`--tensor-parallel-size`, a cross-VPC `--cache-addr`, a high `--gpu-mem-util` (no WSL2 throttle), and
prefix-heavy workloads (shared system-prompt / RAG / few-shot prefix + short unique suffix), and can
sweep `--models` to emit the TTFT-vs-model-size scaling curve. Cold-tier verify is a `loadgen
-verify-coldtier` mode (not new infra): it writes a working set larger than a node's budget, lets it
spill, then **directly Fetches** every block — because `Lookup` is hot-only, a Lookup-gated client
never touches cold, so a direct Fetch that returns an evicted block proves the S3 read-through, with
the existing `payload=f(hash)` oracle (ADR 0016) checking correctness.

## Alternatives considered

- **Mix the rank into the block hash** (so each rank's shard hashes differently). This would *spread*
  a hot prefix's shards across cache nodes instead of piling all `world` shards onto the one node that
  owns the (rank-independent) hash — a real load-balancing win. Rejected for v1: it changes
  `chain_blocks`/`blockio` and would have to stay in lockstep with the Go `internal/blockhash`, and it
  breaks the clean "scheduler checks presence once under rank 0" story. The hot-shard amplification
  (already ~87% on one shard per ADR 0014, now ×`world`) is **documented and accepted**, like the
  original hot-shard call; hot-prefix/shard spreading is the same deferred v2 mitigation.
- **Cross-rank write barrier / 2-phase presence** so a lookup hit strictly implies all shards. Rejected
  as over-engineering: the ADR 0016 recompute fallback already converts any shard gap into a miss, at
  zero coordination cost.
- **Quantized 30B on a single A10G (`g5.2xlarge`)** to dodge TP entirely. Cheaper and simpler, but TP
  with KV-head sharding is the more interesting distributed-systems artifact and what HC chose; the
  single-GPU path still works unchanged (`world = 1`) and remains available as a fallback in the window.

## Consequences

- **The server, proto, ring, and Go `blockhash` are untouched.** TP correctness is entirely in the
  connector's keying + the existing load guard. `world = 1` leaves ADR 0031's single-GPU results
  comparable byte-for-byte.
- **Local verification only (no spend):** `shard_model_id` is unit-tested on CPU (world 1 ⇒ unchanged;
  world > 1 ⇒ distinct per-rank keys; scheduler-rank-0 == rank-0 worker); the connector compiles; the
  benchmark driver compiles; `loadgen -verify-coldtier` builds + `go test ./...` green; `terraform fmt`
  + `validate` pass and the GPU resources are all `count`-gated to 0 by default.
- **Flagged for the window** (can't be confirmed without the GPU): the exact Deep Learning AMI SSM
  parameter name (set `gpu_ami_id` if it has drifted); that `get_tensor_model_parallel_rank()` is
  callable at `register_kv_caches` under the pinned vLLM 0.22.1 (the probe now dumps `tp_rank`/
  `tp_world` + per-rank KV shape to confirm heads are sharded before any real run); and the exact 30B
  model + that its GQA head count divides cleanly by `tp=4`.
- **Hot-shard concentration grows by `world`** for a hot prefix (all shards share the rank-independent
  hash → same owning node). Accepted for v1; spreading is the deferred mitigation.
- **The paid window is now a runbook** (`docs/benchmarks/aws-batch-runbook.md`), with a cost estimate
  and an emphasized teardown — `terraform destroy` / `-var gpu_count=0`, since the GPU bills hourly.
