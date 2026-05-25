# ADR 0011 — Block-wise chained hashing; per-block presence + client-side run assembly

- **Status:** accepted
- **Date:** 2026-05-24 (architecture session, Session 2)
- **Deciders:** HC

## Context

We need to reuse the *longest shared prefix* between requests, not just byte-identical full
prefixes. A single SHA-256 over the whole prefix only supports exact-key lookup — the hash of a
500-token prefix reveals nothing about its 300-token sub-prefix, so partial overlap can't be
discovered. See `docs/01-architecture.md` and plan §3 "Key data structures".

## Decision

- **Block-wise chained hashing:** split tokens into fixed-size blocks (default 16);
  `block_hash[i] = SHA256(block_hash[i-1] || tokenIDs(block i))`. Shared prefixes produce identical
  hashes until divergence — "longest cached prefix" = "longest run of present block hashes".
- **Staged implementation:** Phase 1 ships **exact-key (single-block) lookup**; the `Lookup` API
  takes a *repeated* `block_hashes` so growing to multi-block longest-prefix is non-breaking.
- **Per-block presence + client-side assembly:** the server answers "do I have block `h`?" per
  block; the **client** computes the longest contiguous run from index 0.

## Alternatives considered

- **Exact full-prefix hash only** — simplest, but no longest-common-prefix discovery; weak prefix
  reuse and would need rework. Rejected as the target.
- **Server-side longest-match** — can't work once blocks shard independently (Phase 2): one shard
  doesn't see the global run. Rejected.
- **Radix/trie (SGLang RadixAttention-style)** — shares cache across late-diverging prefixes too,
  but is much harder to distribute. Noted as a Phase-6 "what I'd redesign" talking point, not v1.

## Consequences

- The Phase 1 API is already the distributed API — Phase 2 sharding needs no API change.
- Client (connector + load generator) owns block splitting, hashing, and run assembly; the Go
  server stays an opaque-keyed store (pairs with ADR 0010).
- The exact vLLM block-hash formula is confirmed against vLLM source when the connector is built.
