# ADR 0027 — S3 cold tier: spill-on-evict + Fetch read-through

- **Status:** accepted
- **Date:** 2026-06-05 (Phase 4 / AWS Sub-stage E)
- **Deciders:** HC (+ Claude)

## Context

The hot store is RAM-bounded (ADR 0024): under pressure or the TTL sweep it drops the
lowest-value block, and that block is *gone* — the next request for it recomputes on the GPU. That
is correct but wasteful for a block that was expensive to prefill and is merely cold, not dead. The
plan's tiered-storage story (GPU VRAM → CPU RAM → S3, §5) wants a **cold tier**: evicted blocks
spill to cheap object storage and a later miss reads through to it, trading a slow S3 round-trip
for an even slower GPU recompute. This also gives the AWS deployment a real use for S3 + IAM beyond
remote state.

Constraints: (1) the core must stay **cloud-free** — `internal/cache` and `internal/server` are
heavily unit-tested and must build and test without the AWS SDK; (2) it must not slow the hot path
or the eviction path; (3) it must not weaken the correctness invariant (ADR 0016).

## Decision

**A leaf `internal/coldtier` package owns the AWS SDK and the framing; the core hooks it through
two narrow seams it defines itself.**

1. **`coldtier.Tier` interface** with `Spill` (async demote), `Get` (sync read), `Close`. Two
   impls: an in-memory `Memory` for tests/dev, and `S3Tier` (`aws-sdk-go-v2`, region+creds from the
   instance role — no static credentials, ADR 0004). Neither `cache` nor `server` imports this
   package, so both still compile and test SDK-free.

2. **Spill-on-evict, hooked in `Store.evict`.** `evict` is the single chokepoint both
   memory-pressure eviction (`evictOne`) and the TTL sweep (`sweepIdle`) funnel through, so one
   hook demotes exactly the right blocks. The seam is a plain `cache.SpillFunc` callback (no SDK
   import); `main` adapts it to `Tier.Spill`. **The explicit `Evict` RPC goes through `Delete`,
   which does NOT spill** — a client deleting a block means "remove it," not "move it to cold."

3. **Spill is async + best-effort.** `Tier.Spill` enqueues to a bounded worker pool and returns;
   the worker frames + `PutObject`s off-thread. It is called **under the stripe lock**, so the
   contract is *must not block* — a full queue **drops** the spill (and counts it). A dropped or
   failed spill is just a future recompute (ADR 0013), never a violation. Framing (the byte copy)
   happens in the worker, not under the lock.

4. **Read-through in `Server.Fetch`, behind a `coldReader` seam.** On a hot miss, Fetch asks the
   cold tier; on a hit it streams the bytes and **re-admits** them to the hot store (at the
   *original* version, via `PutWithVersion`, so a replica's copy still agrees) so repeat fetches
   stay fast. Re-admit is skipped if it would breach the hard ceiling (avoiding admit→evict→spill
   thrash). A storage error or a miss both degrade to `NotFound` (a recompute), never to wrong
   bytes.

5. **Self-describing cold objects.** A cold object is `["KVC1"][version][nTokens][tokens][kv]`,
   keyed by `blocks/<model>/<hex(block_hash)>`. Storing version + token_ids (not just KV) lets the
   read-through re-apply the **same** version/token guards the hot path applies, so ADR 0016 holds
   across the tier boundary: a cold hit can only return the bytes stored under that exact
   `(model, block_hash)`, and a version/token mismatch is served as a miss.

6. **Off by default.** Selected by `-cold-bucket`; empty disables the tier entirely (no SDK calls,
   no behaviour change). All local and chaos runs stay exactly as before.

## Why not these alternatives

- **Spill synchronously inside `evict`.** Couples eviction latency to S3 (hundreds of ms) while a
  stripe lock is held — it would stall every writer hashing to that stripe. The async pool keeps
  the lock hold to a channel send.
- **Spill from `Delete` too (one path for all removals).** Conflates "client asked to remove this"
  with "we ran out of RAM." A deleted block should stay deleted, not resurrect from cold on the
  next Fetch. Keeping `Delete` spill-free preserves the RPC's meaning.
- **Store only KV bytes in the cold object.** Then read-through couldn't verify version/token_ids
  and would have to serve unverified bytes — a direct ADR 0016 hole. The few extra header bytes buy
  the correctness guard.
- **Put the S3 client in `cache`/`server` directly.** Drags the AWS SDK into the most-tested
  packages and their test binaries. The leaf package + callback/interface seams keep the core
  cloud-free, matching how replication and metrics are already decoupled.
- **Synchronous re-admit always.** Under a working set larger than RAM this thrashes
  (admit→evict→spill→re-upload). Skipping re-admit over the hard ceiling bounds the churn; full
  elasticity is a Phase 5 concern.

## Consequences

- A bounded node now demotes evicted blocks to S3 instead of losing them; a later Fetch for a cold
  block is a recovered hit (slower than RAM, far faster than a GPU recompute). The tiering story
  (VRAM→RAM→S3) is now real.
- **Cloud-free core preserved:** `go test ./internal/cache ./internal/server` build and pass with
  no AWS SDK linked; only the `coldtier` package and the `cache-server` binary pull it in.
- **Verified locally:** framing round-trip + key isolation (`internal/coldtier`), spill-on-evict and
  Delete-doesn't-spill (`internal/cache`), and Fetch read-through + re-admit + version-mismatch-is-a-
  miss (`internal/server`) all pass; `go vet` + `gofmt` clean. The S3-specific paths (NoSuchKey →
  miss, real `PutObject`/`GetObject`) are exercised on the cluster (Stage 5 verify): force eviction
  with a low `-max-bytes`, confirm objects in the bucket, then Fetch an evicted block from cold with
  zero violations.
- **`-race` debt** carries over (Windows toolchain): the spill enqueue + worker pool and the
  read-through re-admit want one WSL2 `-race` pass.
- New dependency: `aws-sdk-go-v2` (config, s3). Confined to `internal/coldtier`.
- Cold objects currently have **no lifecycle/expiry** — they accumulate. An S3 lifecycle rule
  (expire after N days) is a cheap follow-up, noted for the Terraform bucket (Stage 2).
