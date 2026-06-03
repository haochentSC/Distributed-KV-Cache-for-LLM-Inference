# ADR 0024 — LRU baseline + watermark-driven eviction

- **Status:** accepted
- **Date:** 2026-06-03 (Phase 4, Sub-stages A–C)
- **Deciders:** HC (+ Claude)

## Context

Phase 4 makes the cache production-shaped: a node must stay within a memory budget instead of
growing until the kernel OOM-kills it. Two things are needed — a *policy* (which block to drop)
and a *mechanism* (when to drop, and how to refuse work we can't hold). The policy must be the
**baseline** the Phase 5 cost-aware + fairness engine (GDSF + DRF, ADR 0007) is measured against,
so it has to be clean and obviously-correct, not clever. The mechanism has to coexist with the
existing striped-map concurrency (one RWMutex per stripe) and the async replication path.

## Decision

**1. LRU as the baseline, behind the existing `EvictionPolicy` seam.** A doubly-linked recency
list (`container/list`) plus a `map[BlockHash]*list.Element` gives O(1) touch/insert/remove and
O(1) victim selection. Front = most-recently-used, back = least. The policy is deliberately
**size-blind and cost-blind**: the Store owns the byte budget, the policy owns only ordering.
Swapping in Phase 5's policy touches one line (`NewStore(NewLRU(), …)`), nothing else.

**2. Byte accounting via an atomic delta, not a locked sum.** `Store.totalBytes` is an
`atomic.Int64`. Every write adjusts it by `(newSize − prevSize)` — negative on a shrinking
overwrite, the whole size on a first write. It is atomic because **no single mutex covers the
cross-stripe sum**, and the evictor reads it lock-free. The counter is updated *before* signalling
so the high-water check reads a fresh total.

**3. One background evictor, two triggers, one goroutine.**
- **Pressure (watermark):** a write that crosses the **high-water** mark (default 0.90 × max)
  nudges the evictor over a buffered(1) channel (coalescing — extra nudges are dropped). The
  evictor then frees down to the **low-water** mark (default 0.75 × max). The hi/lo gap is
  hysteresis: it prevents evict-one/write-one thrashing on every byte over the line.
- **TTL (ticker):** every `sweepEvery`, drop entries idle longer than `ttl`, regardless of
  pressure. A two-pass sweep (collect hashes under each stripe's RLock, then evict) keeps the
  lock discipline simple.

**4. Reject-fast admission at the hard ceiling (ADR 0017).** A write that would push past
`maxBytes` is refused with `ResourceExhausted` rather than risking OOM while the evictor catches
up. The hi-water..max headroom is exactly the cushion that absorbs the check-then-act race in the
(non-atomic) admission guard. A rejected write is just a future recompute (ADR 0013) — never a
correctness violation, so shedding load here is safe.

**5. Lock order is always stripe → lru.** `Get`/`Put` hold a stripe lock and call into the policy
(lru lock). The eviction path mirrors this: `evictOne` calls `Victim()` (which takes *and releases*
the lru lock) and only then calls `evict()` (stripe lock, which re-enters the lru lock via
`RecordEvict`). The lru lock is **never** held across a stripe lock, so the two orders can't invert.

**6. A clock seam (`Store.now`) for deterministic TTL tests.** `now` defaults to `time.Now`;
tests override it to age entries without `time.Sleep` (per `.claude/rules/go-testing`). It is
write-once outside tests, so it needs no synchronisation.

## Why not these alternatives

- **A global lock + plain `int64` counter.** Would serialise the whole shard's writes and defeat
  the striped-map design; the atomic counter is the only thing correct under N stripe-locked
  writers and one lock-free reader.
- **Evict synchronously inside `Put` when over budget.** Couples write latency to eviction work
  and would hold a stripe lock while asking the policy for a victim — the exact lock-order
  inversion we designed out. The background drain keeps eviction off the write path.
- **No hysteresis (evict to exactly max).** Produces thrash: every write at the boundary triggers
  one eviction. The lo-water gap amortises drains.
- **A fancier baseline (LRU-K, segmented LRU).** Defeats the purpose — the baseline exists to be a
  clean reference number for the Phase 5 policy, not to compete with it.

## Consequences

- A bounded node now stays under `maxBytes` and sheds load gracefully under burst rather than
  OOM-ing. Unbounded stays the default (`-max-bytes 0`), so nothing changes for callers who don't
  opt in.
- Replicas account and evict independently of their primary (eventually consistent, ADR 0013), so
  a replica under its own pressure may drop a block the primary keeps — a recompute, never a
  violation.
- The `EvictionPolicy` interface (`RecordAccess/RecordWrite/RecordEvict/Victim`) is proven against
  LRU but is **thin for Phase 5**: `Victim()` carries no tenant identity or free-amount, which
  GDSF + DRF will need. Extending the seam is deferred to Phase 5 by design.
- Verified by `go test ./internal/cache` (build/vet/gofmt clean); the race-detector proof
  (`-race`) must be run in a WSL2 Go env — the Windows toolchain lacks 64-bit cgo.
