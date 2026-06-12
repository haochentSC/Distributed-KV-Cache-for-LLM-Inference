# ADR 0035 — Namespace store keys by model_id (TP=4 shard-clobbering fix)

- **Status:** accepted (implemented + validated on hardware 2026-06-12)
- **Date:** 2026-06-12
- **Deciders:** HC (+ Claude)
- **Builds on:** ADR 0010 (opaque block hashes), ADR 0016 (hit-verification guard),
  ADR 0032 (TP shard keying — rank-agnostic block hashes, per-rank `shard_model_id`)

## Context

The Session B TP=4 / Qwen2.5-32B run (ADR 0034) failed its correctness gate in the wild: all four
ranks logged an active save path, but only rank 0 ever loaded a hit, the server's batch_fetch
hit:miss was exactly 1:3, and a shard-presence probe showed every block stored under **one** shard
key with versions in multiples of 4 — four writers clobbering one slot.

Root cause: `internal/cache/store.go` keyed its stripe maps by **`BlockHash` alone**
(`map[BlockHash]*Entry`), treating `model_id` as a *guard* — `Get`/`Peek` returned a miss when the
stored entry's `ModelID` differed, but `Put` overwrote whatever lived under the hash and restamped
the `ModelID`. That guard semantics assumed one model per deployment. ADR 0032 broke the
assumption *by design*: every TP rank hashes the same token chain (the lockstep invariant — the
scheduler checks presence under canonical rank 0 on behalf of all ranks), so the four shard ids
`model#tp{0..3}/4` necessarily share block hashes and collided last-writer-wins.

Two failure modes, one worse than the other:

1. **Silent degradation:** three ranks' saves vanish (overwritten), their fetches miss, and the
   blocks get recomputed and re-saved every pass — warm ≈ baseline with zero errors.
2. **Silent corruption:** the surviving entry's stamped hash matches every rank's request (hashes
   are rank-agnostic), so the ADR 0016 hash guard PASSES while serving **another rank's KV-head
   shard bytes**. The scheduler skipped prefill for those tokens, so nothing recomputes the bad KV.

The same collision exists for any two models sharing a block hash — TP just made it deterministic.

## Decision

**Derive the in-memory map key by mixing the model into the hash at the store boundary:**
`storeKey = SHA-256(model_id ‖ wire_hash)`, applied inside `Get`/`Peek`/`Put`/`PutWithVersion`/
`Delete`. The eviction policy sees the namespaced key too (its `items` map had the same collision).

**The wire hash stays the outward identity.** The entry carries `WireHash`; the spill path passes
`(ModelID, WireHash)` so cold-tier S3 objects, replication (`ReplicaJob.Hash`), and client routing
all still speak `(model_id, wire hash)` — a replica receiving Replicate re-derives the same
namespaced key locally. No proto, client, or policy-interface change.

The `ModelID` guard in `Get`/`Peek`/`Delete` stays as defense-in-depth (it is now provably
redundant: a namespaced key only ever maps to entries of its own model).

### Alternatives rejected

- **Composite struct key `{model, hash}`:** cleaner types but ripples through the
  `EvictionPolicy` interface, LRU/GDSF internals, and every test that uses `BlockHash` keys.
- **Derive the composite at the gRPC boundary (server.go):** one choke point, but Write and
  Replicate would then disagree about what `block_hash` means on the wire (Replicate carries an
  already-derived key), a landmine for replication.
- **Make the connector send pre-namespaced hashes:** breaks ADR 0032's deliberate design — the
  scheduler must check presence under rank 0 for blocks whose hashes all ranks share.

## Validation (4× A40, Qwen2.5-32B, TP=4)

Before: load active on rank 0 only; hit:miss 1:3; one shard key held all blocks, versions ×4.
After: `load path active` on **all four ranks**; **9,280 hits / 0 misses**; **512 writes =
128 blocks × 4 ranks exactly once**; only the cold pass misses lookups; zero correctness warnings.
(`docs/benchmarks/phase45-gpu-cloud.md` § Session B; `phase45-tp4-qwen32b.json`.)

## Consequences

- **Positive:** TP shard keying (ADR 0032) holds end-to-end on real hardware; multi-model
  deployments are now correct in general; the diagnostic shard-presence probe
  (`connector/tools/diag_shard_presence.py`) and per-path shard-key logs are kept.
- **Negative:** one extra SHA-256 per store operation (negligible next to multi-MB payload I/O);
  in-memory keys no longer equal wire hashes when debugging map contents — read `Entry.WireHash`.
- **Lesson (the gate philosophy):** "zero warnings" is not correctness under TP. The failure was
  invisible to the driver (it does not diff outputs) and to the ADR 0016 guard (hash matches);
  only server-side per-key metrics and a presence probe exposed it. Chaos/validation gates must
  observe the *server's* view, not just the client's.
