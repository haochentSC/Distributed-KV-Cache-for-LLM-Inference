# Learning Log

> A running, dated record of what HC learned, what broke, and what HC would redo per phase.
> Newest entries at the top within each phase.

## Template

```md
### YYYY-MM-DD - <short title>
**Phase:** <n>
**What I was doing:** ...
**What I learned / what broke:** ...
**Why it matters / what I'd redo:** ...
**Links:** ADR/PR/commit, docs
```

---

## Phase 4 - Eviction, observability & chaos

### 2026-06-04 - Prometheus instrumentation: keeping infra out of the core, and the cardinality trap
**Phase:** 4 (Sub-stage E)
**What I was doing:** Instrumented the node for Prometheus — a new `internal/metrics` package with
its own registry, a pair of gRPC interceptors for latency/per-code counts, hit/miss + eviction +
replication counters, polled resident/queue gauges, and a `/metrics` endpoint. Captured in ADR 0025.
**What I learned / what broke:**
- **Label cardinality is the failure mode, not CPU.** Every distinct label-value combination is a
  separate time series held in the scraper's RAM. Labelling a high-volume counter with `model_id`
  or `block_hash` lets traffic mint unbounded series and OOM the target — the exact thing eviction
  exists to prevent, recreated in the metrics layer. So labels are *only* bounded sets (method,
  code, op, result, reason). The discipline is counterintuitive: fewer label dimensions is safer.
- **The same seam trick as eviction, again.** To keep `internal/cache` free of Prometheus, the
  Evictor takes an injected `onEvict(reason, n)` *func* (not even an interface), and the server
  defines its own narrow `metricsSink`. `*metrics.Metrics` satisfies both — `m.Eviction` is passed
  straight to `NewEvictor`. Same lesson as ADR 0024's policy seam: push infra to the edge, keep the
  core a pure library.
- **Private registry > global.** promauto's global registry is process-wide mutable state; a second
  `New()` or a test re-registering a name panics. A `prometheus.NewRegistry()` per `*Metrics` makes
  it an ordinary value and lets the tests read series with `testutil.ToFloat64` in isolation.
- **One interceptor beats N instrumented handlers.** A unary + a stream interceptor record
  `(method, code, latency)` for every RPC centrally — no handler bookkeeping, and new RPCs are
  covered for free. `status.Code(nil) == OK`, so successes count without a special case.
- **Gauges are polled, counters are pushed.** Levels (resident bytes/entries, queue depth) are
  sampled on a 5 s ticker so the hot write path stays metrics-free; events (hit/evict/replica) fire
  inline. Matching the metric *type* to push-vs-poll kept the write path clean.
- **"Replication lag" was the wrong noun.** This replicator drops under pressure, so there's no
  apply-time to measure without wire timestamps. Queue depth + dropped/error counts are the honest
  health signal; I named the gauge for what it is and deferred true lag.
**Why it matters / what I'd redo:** The cardinality rule is the transferable one — it's the single
most common way real Prometheus deployments fall over. Recurring friction to fix properly: every
Windows commit trips the pre-commit `gofmt -l` because `core.autocrlf` rewrites the working tree to
CRLF while the index is LF. The durable fix is a `.gitattributes` (`*.go text eol=lf`) +
`git add --renormalize`, raised as a separate change.
**Links:** ADR 0025 (+ 0007, 0013, 0017, 0021); `internal/metrics/`, `internal/cache/evictor.go`,
`internal/server/{server,replicator}.go`, `cmd/cache-server/main.go`.

---

### 2026-06-03 - LRU baseline + watermark eviction: policy vs. mechanism, and the lock order that ties them
**Phase:** 4 (Sub-stages A-C)
**What I was doing:** Built the full eviction core behind the existing `EvictionPolicy` seam — the
LRU baseline (`LRUPolicy`, list+map), byte accounting on the Store (atomic delta), the background
`Evictor` (watermark pressure drain + TTL sweep), and the reject-fast write-admission guard
(ADR 0017). Captured in ADR 0024.
**What I learned / what broke:**
- **Policy and mechanism are different jobs, and keeping them apart is the whole point.** The
  policy answers "which block?" (ordering only — size-blind, cost-blind); the Store answers "are
  we over budget?" (bytes) and the Evictor answers "when?" (watermarks). LRU stays a clean
  reference number because it owns *only* ordering. The Phase 5 GDSF/DRF policy slots into the
  same seam without touching the Store or the gRPC API.
- **The byte counter MUST be atomic, not a locked int.** There is no single mutex over the
  cross-stripe sum, and the evictor reads it without any stripe lock. A plain `int64` guarded by
  per-stripe mutexes would be a data race on the total. `atomic.Int64` + a delta (`new - prev`,
  negative on a shrinking overwrite) is the correct shape.
- **Lock order is the load-bearing invariant.** Everything takes stripe -> lru, never the reverse.
  `Victim()` takes *and releases* the lru lock before `evictOne` calls `evict` (which re-takes the
  lru lock via `RecordEvict` under the stripe lock). Holding the lru lock across a stripe lock
  would invert the order and deadlock. Concentrating the "ask policy then delete" sequence in
  `evictOne` is what makes the order auditable in one place.
- **Hysteresis is not optional.** Evicting to exactly max means every boundary write triggers a
  drain (thrash). The hi(0.90)/lo(0.75) gap amortises it; the buffered(1) signal coalesces a burst
  of nudges into one drain.
- **Testing time needs a seam.** TTL eviction depends on `time.Now`, which is non-deterministic
  and the testing rules forbid `time.Sleep` for sync. Added a `Store.now` clock (default
  `time.Now`, overridden in tests) — a textbook "mock the external boundary" move that made
  `TestStore_SweepIdle` deterministic.
- **Couldn't run `-race` here.** Windows lacks 64-bit cgo and the only WSL distro is
  `docker-desktop` (no Go), so the formal concurrency proof still has to be run in a real WSL2 Go
  env: `go test ./internal/cache -race`.
**Why it matters / what I'd redo:** The split (policy/budget/timing) is the reusable lesson — it's
what lets the headline Phase 5 policy be a drop-in. The one thing to revisit early in Phase 5: the
`Victim()` signature is too thin (no tenant, no free-amount) for DRF fairness; extend the seam
before building GDSF rather than after.
**Links:** ADR 0024 (+ 0007, 0013, 0017); `internal/cache/{lru,evictor,store}.go`,
`internal/cache/eviction_test.go`, `internal/server/server.go`.

---

## Phase 3 - Replication, failover & etcd coordination

### 2026-05-28 - RF=2 + implicit promotion + Spot drain: Phase 3 collapses into one mechanism
**Phase:** 3 (Sub-stages B, C, D)
**What I was doing:** Closed out Phase 3 in one pass — RF=2 async replication (ADR 0021),
implicit promotion via ring rotation (ADR 0022), graceful drain wired to Spot (ADR 0023). The
guided cores landed: `ring.LookupN` (distinct-node clockwise walk), `Store.PutWithVersion`
(version-guarded, stale-drop), `Router.OwnerConns` (ordered read failover), the `Replicator`
(bounded queue, drop-under-pressure, primary→replica `Replicate` RPC), the `Write` enqueue
hook, loadgen routing_key + read failover, `internal/spot` IMDS watcher.
**What I learned / what broke:**
- **The big insight: Sub-stage C is "no code."** Classical distributed-systems training points
  at leader election for failover — a per-shard etcd lease, an "I am primary now" handshake. We
  didn't need any of it. The deterministic ring (ADR 0018) + replica deterministically placed at
  `LookupN[1]` (ADR 0021) + lease-bound membership (ADR 0020) make promotion a *property* of
  the ring rotation: when the dead node's membership key disappears, every router recomputes,
  and the old replica IS the new primary, with the data already on disk because we'd been
  replicating to it. The test `TestLookupN_RebalanceOnRemove` is the executable spec.
- **Placement key ≠ storage key.** The replication design hit a wall when I realized the primary
  only sees individual block writes, but replica placement has to use the prompt root (because
  read-failover keys on the root). Fix: add `routing_key` to `WriteHeader`; primary stores by
  `block_hash` but places by `routing_key`. This is the kind of asymmetry I would not have caught
  without thinking the failover read path through end-to-end first.
- **Version preservation is the safety invariant.** `PutWithVersion`'s `prev.Version >= version
  → drop` guard is what makes async replication safe under out-of-order delivery. Without it,
  a late v2 arriving after a fresh v3 silently rolls back. Test added.
- **Dedicated `Replicate` RPC > `is_replica` bool.** Two-meaning fields turn into Stockholm
  syndrome in 3 months. Separate RPC also makes loop prevention a one-line "do not call
  `s.repl.Enqueue` in Replicate" rather than a runtime flag check.
- **One drain path, two triggers (Sub-stage D).** Spot interruption pushes onto the SAME signal
  channel SIGTERM does, so the shutdown sequence has one implementation. Order matters:
  revoke-lease-then-GracefulStop turns a Spot reclaim from a 10-second outage into milliseconds.
- **Still pending:** `-race` (Windows MinGW blocker) on cluster/coord/server/spot in WSL2;
  multi-node kill-and-verify integration test (Phase 4 chaos territory).
**Why it matters / what I'd redo:** "What if we didn't need election?" was the question that
saved the most code. It only worked because three earlier ADRs (0018 determinism, 0020 membership
leases, 0021 deterministic replica placement) had each made a small choice that, combined, made
election unnecessary. Determinism keeps paying.
**Links:** ADRs 0021/0022/0023, `internal/ring/lookup_n_test.go`,
`internal/cache/put_with_version_test.go`, `internal/spot/`, `internal/server/replicator.go`.

### 2026-05-27 - etcd membership: leases for failure detection, watch for discovery
**Phase:** 3 (Sub-stage A)
**What I was doing:** Added `internal/coord` — etcd-backed cluster membership. `Register` (grant lease
→ put key under it → keepalive → revoke on release) and `WatchMembers` (prefix Get + watch from
revision+1, emit full snapshots) feed the existing `Router.SetMembers` seam via `DriveRouter`. Wired
`cache-server` to self-register (`-etcd`/`-advertise`/`-node-id`/`-lease-ttl`) and the loadgen to
discover via the watch (`-etcd`).
**What I learned / what broke:**
- **A lease IS the failure detector.** A node puts its membership key bound to a lease and keepalives
  it; crash → keepalives stop → lease expires → etcd deletes the key → the watch removes it from the
  ring. No heartbeat code of our own. Graceful shutdown `Revoke`s so the node leaves immediately
  instead of after the TTL. (ADR 0020.)
- **You MUST drain the KeepAlive ack channel** in a goroutine, or it back-pressures and the lease
  silently lapses — a classic etcd-client footgun.
- **The Get/Watch revision gap is real.** `WatchMembers` captures `resp.Header.Revision` from the
  initial prefix Get and starts the watch at `revision+1`, so no event between the snapshot and the
  watch is missed or replayed. Emitting the FULL member set (not deltas) matches `SetMembers`'
  "reconcile to exactly this set" contract.
- **The seam held.** Swapping the static `-members` driver for the etcd watch changed zero routing
  code, and the live etcd-driven run reproduced the static run's per-shard distribution *exactly*
  (86.8% hot shard) — proof the recomputed ring is byte-identical across clients (ADR 0018 determinism).
- Docker Desktop's daemon wasn't running; had to start it before the integration test (which skips
  cleanly when etcd is unreachable). `-race` on the new watch goroutine still pending WSL2.
**Why it matters / what I'd redo:** The lease TTL I picked (10s) is not arbitrary — it's the
failure-detection window, and I'll have to weigh it against the partition-detection window in Sub-stage
C (the split-brain knob). Membership-only schema kept this slice small; ownership/leader keys come with
failover. Next: RF=2 async replication (Sub-stage B).
**Links:** ADR 0020 (membership schema), ADR 0013 (consistency boundary), ADR 0018/0019 (determinism +
the seam), `internal/coord/`, `cmd/cache-server/`, `cmd/loadgen/`.

## Phase 2 - Two-node distributed cache

### 2026-05-27 - Smart-client routing layer; affinity makes the hot shard visible
**Phase:** 3 (Step 0 — finishes Phase 2)
**What I was doing:** Built the client-side routing layer (`internal/cluster`) that finally *consumes*
the ring: a `Router` wrapping the ring + a node→gRPC-conn pool, with `SetMembers` as the single
membership-mutation seam (static driver now, etcd watch in Sub-stage A). Wired it into the load
generator (`-members` instead of `-addr`, degrade-to-miss, per-shard distribution report) and ran a
live 3-shard cluster.
**What I learned / what broke:**
- **The seam shape is a real design choice.** Chose `Router.SetMembers([]Member)` pushed by an
  external driver over a `MemberSource` interface the router pulls from — it keeps all ring+pool
  mutation (and locking) in one method, so swapping the static driver for an etcd watch in Phase 3
  changes *zero* routing code. (ADR 0019.)
- **Connection-pool concurrency:** one `RWMutex` guards the `{ring, pool}` *pair* so a reader never
  sees a node in the ring that's missing from the pool. On member removal, remove-from-ring **then**
  `Close()` — an in-flight RPC on the closed conn aborts into the degrade-to-miss path, which is safe
  because a miss just recomputes (ADR 0016). `grpc.NewClient` is lazy, so dialing can't fail on an
  unreachable host — "is it up?" is deferred to that same miss path.
- **Affinity's hot-shard cost is now measured, not hypothetical.** At `prefix-share=0.8` one shard
  took 86.8% of requests; the arithmetic `0.8 + 0.2/3 ≈ 0.867` matches, so the concentration is
  exactly prefix-affinity routing the viral prefix to a single owner (ADR 0014). This is the number
  that justifies hot-prefix replication later.
- `-race` still blocked on this box (32-bit MinGW cgo); 6 new `cluster` tests pass under plain
  `go test`. Needs a WSL2 `-race` pass before chaos testing.
**Why it matters / what I'd redo:** This closes Phase 2 — sharding is provably exercised across nodes
with a reported distribution. The routing layer is deliberately shaped so Phase 3's etcd watch is a
driver swap, not a rewrite. Next: stand up local etcd and replace the static `SetMembers` with a
membership watch (Sub-stage A).
**Links:** ADR 0019 (routing layer + seam), ADR 0014 (affinity), ADR 0018 (static membership),
`internal/cluster/`, `cmd/loadgen/`.

### 2026-05-25 - Consistent-hash ring + the hash-distribution trap
**Phase:** 2
**What I was doing:** Implemented the consistent-hash ring (`internal/ring/ring.go`): `Add`/`Remove`
(sorted vpoint slice), `Lookup` (binary search + wrap), `Nodes`, and `vnodeHash`. Enabled the four
ring tests (empty, determinism, distribution, minimal-movement). HC had the concept down; the friction
was Go, so this one was auto-completed with the idioms annotated.
**What I learned / what broke:**
- **The hash function for vnode placement matters as much as the ring algorithm.** First cut used
  `fnv-1a` over `"node#i"`. `TestDistribution` failed hard — one node owned ~41% of keys, another
  ~11% (over 4 nodes / 128 vnodes, where the spread should be ~±9%). fnv has weak avalanche on short,
  near-identical labels, so the 128 points-per-node clustered instead of spreading. Switching to
  `sha256(label)[:8]` fixed it immediately (tight ±~10%). The minimal-movement property held either
  way — that's about *which* hash, not *how good* the hash is.
- The determinism requirement (ADR 0018: every client builds an identical ring) rules out `maphash`
  (process-random seed). `TestDeterministic` (same key, different add order, two rings) is the guard.
- `go test -race` still can't run on this Windows box (32-bit MinGW); ran plain `go test` here, which
  is enough to catch the distribution bug. The RWMutex correctness check still needs a WSL2 `-race` run.
**Why it matters / what I'd redo:** "Pick a hash" is not a throwaway step — measure the distribution,
don't assume. The statistical test paid for itself on the first run. Next: 2c routing layer
(ring + gRPC client pool, degrade-to-miss), then the multi-process local harness.
**Links:** ADR 0014 (prefix-affinity), ADR 0018 (static membership), `internal/ring/`.

---

## Phase 1 - Single-node external cache

### 2026-05-25 - Connector support package and stronger fetch verification
**Phase:** 1
**What I was doing:** Added the Python connector package structure, generated Python protobuf stubs,
and tightened the Go fetch path so a request can verify the token IDs bound to a block hash.
**What I learned / what broke:**
- The cache API needed `FetchRequest.token_ids` to match ADR 0016 literally; model+hash is strong,
  but token verification makes the invariant executable in tests.
- Python `grpcio-tools` was not installed in the active environment, so Python codegen failed until
  it was installed. Installing it upgraded `protobuf` and may conflict with other local Python
  packages such as Streamlit; WSL2 should use a project virtualenv.
- The version-sensitive part is still the live vLLM paged-KV copy: hashing, gRPC calls, framing,
  and benchmark scaffolding are in place, but the tensor load/save hooks must be finished against
  the installed vLLM release before claiming a TTFT win.
**Why it matters / what I'd redo:** Keep generated clients and connector dependencies isolated in a
WSL2 virtualenv. Treat the vLLM connector as guided work: the public hooks are small, but the real
learning is mapping vLLM's live KV layout into the opaque byte frame safely.
**Links:** `proto/kvcache/v1/kvcache.proto`, `connector/`, `docs/benchmarks/phase1-ttft.md`.

### 2026-05-24 - Architecture decided + Go project scaffolded
**Phase:** 1 (scaffolding ahead of Phase 0 completion; connector/GPU work still waits on Phase 0)
**What I was doing:** Designed the Phase 1 architecture together and scaffolded the Go project.
**What I learned / what broke:**
- Settled the keystone design: block-wise chained hashing, per-block presence with client-side run
  assembly, and chunked streaming for multi-MB tensors. Captured in ADRs 0011-0013.
- Scaffolding built, vetted, and formatted cleanly; plain tests passed. `go test -race` failed in
  this Windows environment because the installed gcc was 32-bit MinGW and the race detector needs
  64-bit cgo.
**Why it matters / what I'd redo:** Verify the `-race` path early on Windows, since the testing
convention leans on it. WSL2 should be the default Phase 1 environment.
**Links:** ADRs 0011-0013, `docs/01-architecture.md`, `proto/kvcache/v1/kvcache.proto`.

---

## Phase 0 - Foundation

### 2026-05-24 - Local RTX 3080 reshapes GPU logistics
**Phase:** 0
**What I was doing:** Re-checked the GPU plan against local hardware: RTX 3080 8 GB, 32 GB RAM,
Ryzen 9 5900HX.
**What I learned / what broke:**
- The cache stays CPU-only because it stores and ships opaque KV bytes. VRAM is the scarce tier it
  offloads from, not a better place to put the external cache.
- The local 3080 replaces Colab/rental for Phase 0-1 vLLM work; cloud GPU remains optional for the
  later distributed headline benchmark.
- WSL2 is the better default because it supports the vLLM/CUDA path and fixes Go race-test tooling.
**Why it matters / what I'd redo:** Separate hardware availability from component responsibility.
The cache is not a compute component.
**Links:** `docs/00-project-plan.md`, decisions log Session 2.

### 2026-05-24 - Project setup + a design correction before code
**Phase:** 0
**What I was doing:** Consolidated docs into `docs/`, added Claude config, and reviewed the plan.
**What I learned / what broke:** The "fork vLLM vs thin Python proxy" framing was outdated; vLLM has
a first-class `KVConnectorBase_V1` interface and dynamic connector loading, so integration can live
in our package with no fork.
**Why it matters / what I'd redo:** Re-check fast-moving OSS assumptions against current docs at the
start of each phase.
**Links:** ADRs 0008-0010, `docs/00-project-plan.md`.
