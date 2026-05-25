# ADR 0016 — The cache correctness invariant (what "no correctness violation" means)

- **Status:** accepted
- **Date:** 2026-05-24 (architecture review, Session 2)
- **Deciders:** HC

## Context

The Phase 4 deliverable claims "zero correctness violations under chaos." For an **eventually
consistent** cache (ADR 0013), that phrase is meaningless until we define the invariant: a stale or
missing read is *explicitly allowed* (it costs a recompute — the no-cache baseline). So "correct"
can't mean "always returns the freshest entry." We need to state precisely what the cache must
*never* do, so the chaos tests have a real assertion to check. See plan §Phase 4 and
`docs/01-architecture-overview.md` §14.

## Decision

**The invariant:** the cache may return a miss for an entry that exists, and may serve a slightly
stale version, but it must **never serve KV bytes that do not match the requested `(block_hash,
model_id, token_ids)`.** Serving wrong-but-plausible KV would corrupt model output silently; that
is the one outcome ruled out.

- **Allowed (not violations):** miss on a present entry; reading a lagging replica; losing an
  un-replicated write after a primary crash; eviction of a live entry.
- **Forbidden (violations the chaos suite asserts against):** returning data for the wrong key;
  returning data for a different model; returning data whose stored `token_ids` don't match the
  request; partial/truncated payload reported as complete.
- **Mechanism:** the existing **hit-verification** on `Fetch` — compare stored `token_ids` /
  `model_id` against the request and treat any mismatch as a miss — is the guard that upholds this
  (already noted in `docs/01-architecture.md`). Chunk streams carry an explicit `last=true` so a
  truncated transfer is detectable, never silently "complete."

## Alternatives considered

- **Stronger invariant (always-fresh reads)** — would require synchronous replication / read
  quorums, defeating the eventual-consistency choice (ADR 0013) for data whose loss is just a
  recompute. Rejected as over-strong for a cache.
- **Leave it implicit** — keeps "no correctness violations" as an unfalsifiable slogan; the chaos
  tests would have nothing concrete to assert. Rejected.

## Consequences

- The Phase 4 chaos harness gets a concrete, testable property: inject partitions/kills, then verify
  every served entry matches its requested key/model/tokens (and reconstructs correctly), tolerating
  misses and staleness.
- Pairs with ADR 0013 (consistency boundary) and ADR 0010 (opaque keys): correctness lives in the
  client-verifiable key↔value binding, not in freshness.
- Sets the bar for Phase 3 reconciliation: a returning ex-primary may hold stale/divergent entries —
  acceptable as long as none violate the invariant (worst case they self-heal via miss→recompute).
