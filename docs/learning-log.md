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

## Phase 2 - Two-node distributed cache

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
