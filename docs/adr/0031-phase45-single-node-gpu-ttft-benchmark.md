# ADR 0031 — Phase 4.5 single-node GPU TTFT benchmark (the cache loses on a laptop, and exactly why)

- **Status:** accepted
- **Date:** 2026-06-08 (Phase 4.5 — the one number that needs a real GPU)
- **Deciders:** HC (+ Claude)
- **Builds on:** ADR 0015 (the core inequality `lookup + fetch + decode_copy < recompute`, made a
  Phase 1 exit gate), ADR 0016 (correctness invariant), ADR 0008/0010 (no-fork `KVConnectorBase_V1`,
  opaque block bytes), ADR 0012 (chunked tensor streaming)

## Context

Every distributed property of the cache (sharding, replication, failover, eviction, the Phase 5
differentiator) has been exercised with the synthetic load generator. The one thing it cannot
produce is the **end-to-end TTFT number from a real vLLM + GPU run** — the `[measured]` placeholder in
the resume bullet, and the ADR 0015 exit gate deferred since Phase 1. Phase 4.5 closes the gate on
HC's local **RTX 3080 (8 GB, WSL2)**, single-node (local vLLM worker → local Go cache).

The gap was narrow in surface: the Go service, the Python gRPC client, and the framing codec were all
done. Only the **worker-side tensor-copy hooks** in the connector were stubbed, because they depend on
the installed vLLM's paged-KV memory layout, which had to be inspected live on the GPU.

## What was built

**1. Live paged-KV layout, captured by a probe** (`connector/tools/probe_kv_layout.py`). vLLM v1
(0.22.1, FlashAttention v2) lays each layer's KV out as one tensor
`[2, num_blocks, block_size, num_kv_heads, head_dim]`, bf16, with **`block_axis = 1`** and
`block_size = 16` (== our `block_tokens`). Measured shapes:
  - TinyLlama-1.1B: 22 layers, `[2, 4988, 16, 4, 64]`.
  - Qwen2.5-3B-Instruct: 36 layers, GQA (2 KV heads), `head_dim 128` → `[2, num_blocks, 16, 2, 128]`.
  All version-specific knowledge collapses to that one integer `block_axis`; the block-copy mechanics
  are parameterized on it and CPU-unit-tested in `blockio.py` (incl. a bf16 round-trip — numpy can't
  represent bf16, so the byte path is uint8-view-based).

**2. Save path** — `save_kv_layer`/`wait_for_save` slice each newly-computed **full** block's slab out
of the paged tensor (`serialize_block`, all layers into one codec payload) and `client.write` it with
`recompute_cost = token count` (the GDSF cost model, ADR 0029).

**3. Load path** — `start_load_kv` fetches the externally-present prefix blocks and `deserialize_into`
copies each back into its allocated paged slot, re-checking the ADR 0016 hash guard before trusting
the copy. `get_num_new_matched_tokens` tells vLLM how many prefix tokens are external so it skips
recomputing them.

**4. BatchFetch RPC** (proto + Go server + Python client) — added mid-investigation to collapse the
per-block fetch into one round-trip (see findings). One request lists all hit blocks; the response
stream tags each frame with its block index; an absent/mismatched block is isolated to a single
`found=false` frame so it never fails the batch (ADR 0013/0016). The cold read-through was refactored
into a shared `coldHit` helper so Fetch and BatchFetch apply identical guards.

## Findings (the measured result)

TTFT proxy = wall-clock latency of a one-token generation; `warm_vs_baseline = (base − warm)/base`,
so **positive = cache wins**. p50 over 6–8 runs (the cold run populates the cache; warm serves it
back). The save/load round-trip works and is correct — both `[kvc] save path active` and
`[kvc] load path active` fire, zero correctness warnings across all runs.

**The cache does not beat recompute on a single node at ≤3B — but the deficit closes sharply with
model size:**

| model | prompt tokens | baseline p50 | warm p50 | warm_vs_baseline |
|-------|--------------|-------------|----------|------------------|
| TinyLlama-1.1B | 962 | 70 ms | 189 ms | **−169%** |
| TinyLlama-1.1B | 1682 | 119 ms | 321 ms | −170% |
| Qwen2.5-3B | 385 | 92 ms | 136 ms | −47% |
| Qwen2.5-3B | 769 | 165 ms | 235 ms | −42% |
| Qwen2.5-3B | 1345 | 230 ms | 381 ms | −66% |

Tripling the model (1.1B → 3B) **quartered** the deficit (−169% → ~−48%), exactly as the physics
predicts: prefill is O(n²)·(model size) so recompute-per-block rises with the model, while the
external load cost per block is roughly model-independent (GQA even *shrank* the 3B payload, 590 KB vs
TinyLlama's 720 KB/block). The curves converge as the model grows.

**Two bottleneck hypotheses were raised and *falsified with data*:**

1. *"It's the per-block RPC round-trip latency."* Batching 48 sequential fetches into one BatchFetch
   moved the number by **noise** (3B: −42% → −48%, within variance). Falsified — on localhost the RTT
   was never the cost.
2. The instrumented breakdown (`KVC_LOAD_PROFILE=1`) settled it. For a 769-token / 48-block prompt
   (48 blocks × 36 layers):

   ```
   load breakdown: batch_fetch = 89.4 ms   deserialize + copy + sync = 135.5 ms
   ```

   **225 ms total load overhead = 4.7 ms/block, vs recompute 3.4 ms/block** (165 ms ÷ 48). That 1.4×
   ratio *is* the −42%. Neither half is bandwidth-bound (28 MB over localhost should be single-digit
   ms): the **89 ms transport** is protobuf encode/decode + Python `bytearray` assembly of 28 MB
   (identical whether 1 stream or 48 — which is *why* batching didn't help), and the **135 ms copy** is
   1,728 small `torch.frombuffer`/reshape + **unpinned** H2D `copy_` ops (vLLM forces
   `pin_memory=False` under WSL2).

**The ADR 0015 inequality therefore inverts at single-node ≤3B on this WSL2 box: 4.7 ms > 3.4 ms per
block.** It is environment-capped, not algorithmically lost — three constraints a real deployment
removes: (a) WSL2 forbids pinned host memory (the H2D copy floor); (b) the Python hot path serializes
28 MB through protobuf; (c) the KV cache was throttled to **0.09 GiB / ~2,500 tokens** because 3B
weights ate 5.8 of the 8 GB. The crossover lives where those lift: pinned buffers + zero-copy/RDMA
transport (NIXL-style), a GPU not 86%-full of weights, and a larger model and/or real cross-request
prefix sharing — i.e. **the distributed/cloud GPU path, deferred to a paid window.**

## Alternatives considered

- **Push to a 7B model locally** (where prefill/block clearly exceeds load/block, so warm would flip
  positive). Doesn't fit 8 GB in bf16 (14 GB weights); 4-bit adds quant complexity and was out of the
  Phase 4.5 scope (ADR Session-2 decision). The scaling trend already establishes the direction.
- **Optimize the WSL2 copy path** (single `frombuffer` per block, coalesce the 36 per-layer ops, a
  pinned staging buffer). Low ceiling — unpinned PCIe is the floor and the 89 ms transport stays —
  and the real fix (pinned + zero-copy transport) belongs on the cloud path. Deferred, not pursued.
- **Report a cherry-picked positive number.** Rejected: the honest, fully-decomposed negative result
  (two hypotheses falsified, the bottleneck named and measured) is the stronger interview artifact and
  correctly motivates the distributed run.

## Consequences

- **ADR 0015 gate is *resolved* (measured), not *passed*.** At single-node ≤3B WSL2 the inequality
  fails by 1.4×, for fully-understood environment reasons; the scaling trend + the load decomposition
  predict it passes on the distributed/cloud path. The gate moves there.
- **The save/load + BatchFetch path is real, correct, and committed** — the distributed GPU run reuses
  it unchanged. BatchFetch is a permanent API improvement regardless of the perf finding.
- **`-race` debt cleared** while in the WSL2 Linux toolchain: `go test ./... -race` green across all
  packages (the Windows box can't — 32-bit MinGW cgo).
- **Deferred to the paid cloud window:** the distributed vLLM → multi-node cache TTFT run with pinned
  memory + a non-throttled KV cache; that is where the headline number is produced. Bundled with the
  existing AWS batch (cold-tier verify, chaos, CloudWatch, 5a/5b re-run).
- **Resume `[measured]`:** stays unfilled with a *positive single-node* number by design — the bullet
  reads "built + verified end-to-end on GPU; the TTFT win is measured to be transport/copy-bound on a
  single laptop node and lands on the distributed path," with the scaling curve as evidence.
