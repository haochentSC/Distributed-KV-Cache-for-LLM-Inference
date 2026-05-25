# ADR 0014 — Sharding granularity: prefix-affinity vs. per-block hashing

- **Status:** proposed (decide at Phase 2 start, with HC)
- **Date:** 2026-05-24 (architecture review, Session 2)
- **Deciders:** HC (+ Claude)

## Context

Phase 2 shards the keyspace across nodes. ADR 0011 gives us independent, uniformly-distributed
`block_hash` values. The unexamined assumption was "put each block on the ring by its own hash."
But block hashes of the *same prompt* are unrelated, so **consecutive blocks of one prefix scatter
across different shards**. A 500-token shared prefix (≈31 blocks) can land on many shards, which
means:

- one logical `Lookup` splits into **multiple per-shard RPCs**, and
- `Fetch` fans out the same way, so end-to-end latency is the **max over all touched shards**.

This quietly contradicts the plan's "sub-10 ms lookup, faster than recompute" premise (plan §2, §3).
This is cheap to decide now and painful to change after Phase 2 ships, so we capture the fork here
rather than defaulting silently. See `docs/01-architecture-overview.md` §7.

## Decision (proposed — not yet ratified)

**Recommended:** shard with **prefix affinity** — route by a coarse prefix root (e.g. the first
block hash, or the first K tokens' hash), so all blocks of prompts sharing that root live on **one
shard**. A prefix lookup becomes one shard / one RPC. Pair it with **hot-prefix replication** to
contain the load-imbalance cost (see Alternatives). Final call deferred to Phase 2 start with HC,
since this is core distributed-systems design (guided), not a scaffold decision.

## Alternatives considered

- **Per-block consistent hashing (current implied default)** — perfect load balance; blocks spread
  evenly. But scatters a single prefix across shards → multi-RPC fan-out and max-latency reads.
  Optimizes balance at the cost of the latency goal.
- **Prefix-affinity (recommended)** — one prompt's blocks co-locate → one-RPC, low-latency lookups
  and locality for `Fetch`. Cost: a viral prefix (a popular system prompt) creates a **hot shard**
  (load imbalance). This is the classic *locality vs. balance* tension.
- **Affinity + hot-prefix replication / prefix-aware routing** — co-locate by default, but detect
  hot prefixes and replicate them across shards so routing can spread their load. This is what
  production systems (SGLang, Mooncake) converge on. More moving parts; the strongest end-state.
- **Radix-tree (RadixAttention-style) distributed index** — also enables late-divergence sharing,
  but is much harder to distribute. Already a Phase-6 "what I'd redesign" talking point (ADR 0011).

## Consequences

- If affinity is chosen: routing keys on a *prefix root*, not per-block; the client still does
  per-block presence + run assembly (ADR 0011) within the owning shard. The ring (ADR 0009 etcd)
  publishes ownership of prefix-root ranges, not per-block ranges.
- We accept hot-shard risk and must measure it under the multi-tenant load generator; hot-prefix
  replication is the mitigation lever and the interview beat.
- Revisit if measured fan-out latency under per-block hashing turns out acceptable, or if a radix
  index is adopted later. Until ratified, Phase 2 design treats this as the open decision in
  `docs/01-architecture-overview.md` §16.
