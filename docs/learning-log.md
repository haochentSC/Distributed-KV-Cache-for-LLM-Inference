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

## Phase 4.5 - End-to-end GPU TTFT benchmark

### 2026-06-12 - RunPod Session B: the TP=4 gate caught a real keying bug (ADR 0035)
**Phase:** 4.5-B (RunPod Option B, Session B; ADRs 0034/0035)
**What I was doing:** TP=4 / Qwen2.5-32B keying validation on 4× A40 — probe gate, then the
distributed driver against a loopback cache-server.
**What I learned / what broke:**
- **The probe gate passed but the run failed silently:** save active on all 4 ranks, load active on
  rank 0 only; server batch_fetch hit:miss EXACTLY 1:3. A shard-presence probe
  (`connector/tools/diag_shard_presence.py`) showed every block under ONE shard key, versions in
  multiples of 4 — four writers clobbering one slot.
- **Root cause was the server, not the connector:** the store mapped `BlockHash -> Entry` with
  `model_id` as a read guard, not part of the key. ADR 0032 keeps block hashes rank-agnostic *by
  design* (the scheduler checks presence under rank 0 for everyone), so the four `#tpR/4` shard ids
  necessarily collide. Worse than degradation: the stamped hash matches across ranks, so the
  ADR 0016 guard would pass while serving ANOTHER rank's KV shard — silent corruption.
- **Fix:** namespace the store key — `storeKey = SHA-256(model_id ‖ wire_hash)` inside the store;
  `Entry.WireHash` keeps the wire identity for spill/replication. Re-validated on hardware: all 4
  ranks load, 9,280 hits / 0 misses, 512 writes = 128 blocks × 4 ranks exactly once.
- **"Zero warnings" ≠ correct.** The driver doesn't diff outputs and the client saw no errors;
  only the SERVER's per-key metrics exposed the 1:3 ratio. Validation gates must observe the
  server's view.
- **RunPod ops:** template CUDA version is a hard gate (vLLM 0.22.1 pins cu130 torch → needs a
  CUDA ≥ 13.0 host driver; the plain PyTorch template landed on 12.7); the CUDA-13 template doesn't
  inject the account SSH key (Web Terminal + authorized_keys, split long pastes); conda lives at
  /opt/conda (no PEP 668); `pkill -f cache-server` over SSH matches its own command line.
**Why it matters / what I'd redo:** This is exactly what the ADR 0032 gate exists for — and the bug
was one layer deeper than the gate was aimed at. The single-model assumption hid in the store since
Phase 1 and every earlier benchmark was blind to it. I'd add a multi-model store test from day one
whenever a key has a "guard" field — a guard on read with last-writer-wins on write is a collision
waiting for a workload that shares hashes.
**Links:** ADR 0035, ADR 0032/0016, `docs/benchmarks/phase45-gpu-cloud.md` § Session B,
`phase45-tp4-qwen32b.json`, `runpod-tp4-kv-layout-probe.json`.

### 2026-06-11 - RunPod Session A: no 32k crossover on A100; crossover is GPU-class dependent
**Phase:** 4.5-B (RunPod Option B, Session A; ADR 0034)
**What I was doing:** Executed the RunPod long-context sweep (7B, 1k→32k), 14B scaling check, and
`vllm serve` demo on 1× A100 80GB with loopback cache-server (Session B TP=4 still pending).
**What I learned / what broke:**
- **The plan's 20–40% @ 32k extrapolation did not land.** Warm path stayed Python/deserialize/copy
  bound (~300 MB/s); A100 baseline prefill at 4k is **273 ms** vs AWS L4 **1,070 ms** — same connector,
  opposite TTFT sign. Crossover needs *compute-bound* prefill (cost-tier serving GPU), not flagship
  throughput.
- **14B is worse than 7B at the same token count** (−270% vs −181% @ 4k) because KV bytes/token
  scales faster than prefill FLOPs — refines ADR 0031's "deficit closes with model size" to
  **KV-bytes / recompute-cost ratio**.
- **RunPod ops:** Direct TCP SSH for `scp`; proxy `ssh.runpod.io` terminal-only; PEP 668 →
  `--break-system-packages`; `--deadline-ms 15000` load-bearing (2s silently degrades to recompute).
  Top repeat rung **504** not 512 (32,783 tokens overflows context).
- **Serving demo:** connector works under OpenAI-compatible serve; TTFT regression on A100 is honest
  (baseline 283 ms p50 vs warm 768 ms) — integration artifact, not headline.
**Why it matters / what I'd redo:** A measured negative on flagship hardware is as valuable as a
 cherry-picked positive — it defines *where* the cache pays (AWS L4 + long prefix + distributed).
 Session B still owes TP=4 keying proof (ADR 0032). Terminate pods in console every time.
**Links:** ADR 0034, `docs/benchmarks/phase45-gpu-cloud.md`, `runpod-gpu-window-plan.md` (Session B
handoff), JSONs under `docs/benchmarks/phase45-longcontext-*`, `runpod-demo-serve.typescript`.

### 2026-06-09 - Tensor parallelism shards the KV cache, so the cache KEY has to shard too
**Phase:** 4.5 (distributed prep, no spend; ADR 0032)
**What I was doing:** Prepping the paid AWS window for a 30B-class TP=4 headline run — GPU-node
Terraform, a distributed benchmark driver, a cold-tier verify, and the connector changes TP needs.
**What I learned / what broke:**
- **Under tensor parallelism, each GPU rank holds only a SLICE of the KV heads** (`num_kv_heads/tp`),
  and vLLM runs one worker (one connector instance) per rank. But our block hash is seeded only from
  `(token_ids, model_id)`, so it's *identical across ranks* — every rank would write its different
  shard to the same `(model_id, hash)` server entry and overwrite the others, then all ranks would
  load one rank's bytes. That's not a miss; it's a *wrong serve*, the one thing ADR 0016 forbids.
- **The fix doesn't touch the server.** Since the server treats `model_id` as opaque (ADR 0010), I
  fold the rank into it (`shard_model_id` → `model#tp{r}/{w}`). The hash stays rank-independent (it
  must — the scheduler computes it and the workers reuse it). World size 1 returns the bare id, so the
  single-GPU path is byte-identical to ADR 0031.
- **Scheduler vs worker is the subtle part.** `get_num_new_matched_tokens` runs scheduler-side, which
  has no rank. So presence is checked under *canonical rank 0*, trusting a lockstep invariant (all
  ranks save the same blocks in the same forward). I didn't need a distributed transaction to make
  that safe — the existing load hash-guard + `get_block_ids_with_load_errors` already turn any
  shard gap into a recompute. Optimistic path, correctness fallback.
- **`Lookup` is hot-only**, so a Lookup-gated client never exercises the cold tier — which is why the
  cold-tier verify has to *directly* Fetch evicted blocks to prove the S3 read-through.
**Why it matters / what I'd redo:** The instinct "the cache is keyed by content, so it's
parallelism-agnostic" is exactly wrong once the *content itself* is sharded across workers. Keying is
a distributed-systems decision, not a hashing detail. I'd consider mixing the rank into the hash later
(spreads a hot prefix's shards across nodes instead of piling them on one) — deferred, since it
couples the Python hash to Go `internal/blockhash`.
**Links:** ADR 0032, `connector/src/kvcache_connector/hashing.py` (`shard_model_id`), `connector.py`,
`terraform/cluster/gpu.tf`, `docs/benchmarks/aws-batch-runbook.md`.

### 2026-06-08 - The cache loses on a laptop — and the *why* is the whole lesson
**Phase:** 4.5 (the one number that needs a real GPU; single-node, RTX 3080 + WSL2)
**What I was doing:** Wired the stubbed vLLM worker-side tensor-copy hooks (probe the live paged-KV
layout → save full blocks → load them back into paged slots), then benchmarked TTFT with/without the
external cache, hunting for the crossover where caching wins (ADR 0031).
**What I learned / what broke:**
- **The live KV layout is one integer.** vLLM v1 / FlashAttention v2 stores each layer's KV as
  `[2, num_blocks, block_size, num_kv_heads, head_dim]` bf16, `block_axis=1`. Parameterizing the
  block-copy on just that axis kept the mechanics CPU-unit-testable (no GPU, no vLLM). bf16 bit me:
  numpy can't represent it, so `tensor.numpy()` crashes — reinterpret as uint8 bytes instead.
- **The cache did not beat recompute at ≤3B — but the deficit quartered from 1B→3B** (−169% → ~−48%).
  Prefill is O(n²)·model-size; external load cost per block is ~model-independent. The curves
  converge as the model grows — the trend, not just one point, is the result.
- **I was wrong twice about the bottleneck, and disproving each with data was the point.** Hypothesis
  1 (per-block RPC latency) → built a BatchFetch RPC to collapse 48 round-trips into 1 → TTFT moved by
  *noise*. That null result *is* evidence: on localhost the RTT was never the cost. Then a one-shot
  profiler split the load: `batch_fetch 89 ms + deserialize/copy 135 ms = 225 ms` for 48 blocks =
  4.7 ms/block vs recompute 3.4 ms/block. Both halves are Python-serialization + **unpinned** PCIe
  copy (WSL2 forces `pin_memory=False`), not bandwidth — 28 MB should cross localhost in single-digit
  ms. The async H2D copy hid behind the queue until I added a `torch.cuda.synchronize()` to measure it.
- **The deck is stacked by the environment, not the algorithm:** unpinned memory, a Python hot path
  serializing 28 MB, and a KV cache throttled to 0.09 GiB because 3B weights ate 5.8/8 GB.
**Why it matters / what I'd redo:** A measured, fully-decomposed *negative* result (two hypotheses
falsified, the cost named to the millisecond) is a stronger artifact than a cherry-picked positive
one — and it correctly motivates the distributed/cloud run with pinned memory + zero-copy transport.
I'd reach for the profiler *before* building BatchFetch next time: measure the split, then optimize
the dominant term. (BatchFetch is still a keeper — one round-trip for a multi-block load is right.)
**Links:** ADR 0031, `docs/benchmarks/phase1-ttft.md`, `connector/.../blockio.py` + `connector.py`
(`_load_plan`, `KVC_LOAD_PROFILE`), `proto/.../kvcache.proto` (BatchFetch), `internal/server/server.go`.

---

## Phase 5 - The differentiator: cost-aware + fair eviction

### 2026-06-07 - 5b: the work-conserving floor + the fairness knob (and a knob that saturates)
**Phase:** 5b (the stretch half — the actual tradeoff curve)
**What I was doing:** Turned 5a's static caps into elastic **floors** and added the
`fairness_weight ∈ [0,1]` knob (`-eviction gdsf-elastic`, ADR 0030), then swept it to draw the
efficiency-vs-fairness Pareto frontier (`scripts/phase5b-sweep.ps1`, `phase5b-eviction.md`).
**What I learned / what broke:**
- **"Work-conserving" is a one-line idea with a big payoff.** Making `OverQuota()` return false in
  elastic mode means a tenant never gets reclaimed just for exceeding its floor — only the global
  watermark evicts, so idle capacity is always lent out. That single change made elastic `w=0.25`
  (14.4% overall / 12.3% min-tenant) **Pareto-dominate** the 5a static caps (12.2% / 10.3%) — better
  on *both* axes. The hard cap was leaving capacity on the floor; not doing that is free.
- **Fairness as a victim-selection discount, not a separate mechanism.** `H_eff = H/(1+w·overage)`
  protects under-floor tenants and exposes over-floor ones, with `w=0` collapsing to pure GDSF. The
  trick that kept it cheap: a *per-tenant scalar* discount preserves each tenant's heap order, so
  victim selection is still an O(#tenants) root-peek — no global re-sort.
- **Keep the value scale clean.** `L` must advance by the victim's *true* priority, not the discounted
  score — the knob decides *who* pays, it mustn't corrupt the global GreedyDual aging.
- **The knob saturates — and saying so is the point.** The entire transition is in `w ∈ [0, 0.25]`;
  `w ∈ [0.25, 1]` is a flat plateau. A multiplicative discount is hypersensitive near 0: once it
  reorders victims toward over-floor tenants, more `w` changes nothing. So this dial is closer to
  off/on than smooth. The honest write-up names the fix (an additive rank-blend) without building it.
- **Concurrency-8 eviction order is nondeterministic**, so a single seed gave a non-monotonic
  mid-curve. Averaging 3 seeds in the sweep script removed the jitter and the frontier read cleanly.
**Why it matters / what I'd redo:** This *is* the differentiator's headline — "here is my cache's
efficiency/fairness frontier, here's why the work-conserving floor beats a hard cap, and here's where
the knob actually has leverage." The redo: a finer sweep of the `[0, 0.25]` knee and the additive
blend to get a dial with usable mid-range.
**Links:** ADR 0030, ADR 0029, ADR 0007, `docs/benchmarks/phase5b-eviction.md`,
`internal/cache/gdsf.go`, `scripts/phase5b-sweep.ps1`

### 2026-06-07 - GDSF + static quotas: the efficiency-vs-fairness tension, made visible
**Phase:** 5a (the must-ship half of the headline differentiator)
**What I was doing:** Built the cost-aware GDSF eviction policy (`H = L + freq·cost/size`) plus
static per-tenant byte quotas behind the Phase 4 `EvictionPolicy` seam, wired `-eviction gdsf` /
`-tenant-quota`, extended loadgen with a 3-tenant workload (`-multitenant`), and benchmarked
LRU vs GDSF vs GDSF+quotas on one bounded shard (ADR 0029).
**What I learned / what broke:**
- **A benchmark only shows what the workload lets it.** The first multi-tenant run had GDSF ≈ LRU
  (A 47.5% vs 47.3%) — useless. Cause: the cache easily held every tenant's *single* hot prefix, so
  hit rate was profile-determined, not policy-determined. The fix was a **pool of distinct hot
  prefixes per tenant** whose union *oversubscribes* the cache. Only when the policy must choose
  *whose* reusable blocks to evict does cost-awareness (and a quota) have anything to do. This is the
  "multi-tenant load generator is the single most under-estimated cost" warning from plan §3.5, felt
  first-hand.
- **The tension is real and the policy controls it.** GDSF maximises aggregate hit rate (17.6%→21.5%)
  by hoarding the expensive tenant's high-`cost` blocks — and *starves the cheap tenant harder than
  LRU* (5.6%→3.1%). Static quotas flip it: min-tenant 3.1%→10.5%, cheap tenant 3.1%→16.9%, at an
  efficiency cost (→12.4%). These are the two endpoints of the `fairness_weight` knob 5b will sweep.
- **The optional-capability interface pattern kept the blast radius tiny.** Quota enforcement rides
  the *existing* single Evictor via an optional `QuotaPolicy` (`OverQuota()`) the Store type-asserts
  — so LRU/Noop are byte-identical to Phase 4 and `store.go` changed by ~one metadata-forward line.
  Adding a second signalling path would have been the obvious-but-wrong move.
- **GreedyDual aging (`L`) is the non-obvious bit:** advancing `L` to each victim's priority is what
  makes a once-hot block age out without an explicit timestamp — LFU can't, LRU ignores cost. Tested
  white-box (`p.inflation`) since it has no external surface.
- **`-race` still needs WSL2** (Windows toolchain lacks 64-bit cgo, per Phase 4). The default WSL
  distro was `docker-desktop` (no Go); had to target `wsl -d Ubuntu` and call `/usr/local/go/bin/go`
  by full path (Git-bash mangles `/mnt/...` args; PowerShell→wsl avoids it). Race-clean on the new
  GDSF code.
**Why it matters / what I'd redo:** This is the interview centrepiece — "here is the
efficiency/fairness frontier of my cache and where I'd set the knob," not "X% faster." The honest
caveat to carry forward: 5a's quotas are also caps, so the GDSF+quota point is deliberately
fairness-favouring — one point, not the curve. 5b draws the curve.
**Links:** ADR 0029, ADR 0007, `docs/benchmarks/phase5a-eviction.md`,
`internal/cache/{gdsf,eviction,store,evictor}.go`, `cmd/loadgen/main.go`

## Phase 4 - Eviction, observability & chaos

### 2026-06-06 - First AWS apply: the cluster goes live, and t3 burst credits bite
**Phase:** 4 (Sub-stage E — first `terraform apply` against a real account)
**What I was doing:** Ran the authored Terraform end-to-end for the first time, stage by stage with a
verify gate at each step: Stage 0 toolchain/identity/budget, Stage 1 `bootstrap` (S3 state + DynamoDB
lock), Stage 2-4 `cluster` (VPC, 3-node etcd, 3 Spot cache nodes, ECR, S3 cold tier, IAM, CloudWatch).
Built + pushed the image, then drove a `loadgen -verify` run inside the VPC.
**What I learned / what broke:**
- **`templatefile()` parses the WHOLE file, comments included.** A literal `${...}` inside a bash
  comment in `cache.sh.tftpl` was read as a Terraform interpolation and failed `validate` (`...` is
  not a valid expression). Lesson: in a `.tftpl`, even comment text must escape `$${...}` or avoid the
  token. Caught at `validate`, before any apply.
- **The image push necessarily races first boot — so user-data must not die on it.** The ECR repo only
  exists *after* the cluster apply, so a fresh cache node always boots before the image is pushable.
  The original user-data ran `docker pull` under `set -euo pipefail`, so the missing image **aborted
  user-data before the systemd unit was even written** — the node then couldn't self-heal at all
  (no unit to retry). Fix: make the boot pull non-fatal and let `cache-server.service`
  (`Restart=always`) retry `docker run` until the image lands; add `user_data_replace_on_change` so a
  template fix actually recreates the node.
- **`t3.*` burst credits are a correctness hazard, not just a perf one.** Driving `loadgen` *on* a
  cache node depleted its CPU-credit balance to 0; AWS then throttled the box to its ~0.2 vCPU
  baseline. At baseline the cache-server couldn't reliably send its 10s etcd **lease keepalive**, so
  every node's lease lapsed and `/kvcache/members/` went **empty** — and `sshd` couldn't even complete
  a banner exchange. A throughput problem cascaded into a membership/availability outage. Fix:
  `credit_specification { cpu_credits = "unlimited" }` on the cache nodes (set live via
  `modify-instance-credit-specification` to recover the running fleet, then committed to Terraform).
  For sustained benchmark/chaos load a **non-burstable** type (`c7i.large`) is the right substrate;
  `t3.small`'s 2 GiB RAM is also tight against the 1.5 GB `cache_max_bytes` default.
- **Never run the load generator on a shard.** loadgen competes with the cache-server it's hammering
  for the same 2 vCPUs. Run it from an etcd node or a bastion (still inside the VPC, since nodes
  advertise private IPs). After moving it to an etcd node with a gentle payload, the verify run was
  clean: **6,596 req, 0 errors, 0 violations**, and reproduced the ADR 0014 ~87% hot-shard
  concentration *on AWS* — matching the local number exactly.
- **The locked-down etcd SG makes `etcdctl endpoint health --cluster` lie.** It probes each node's
  *client* port (2379) cross-node, but the SG only allows 2379 from cache nodes + operator; etcd peers
  talk on 2380. So 2/3 endpoints show "unhealthy" while quorum is actually fine — confirm with a
  `put`/`get` (needs quorum) instead.
- **Windows/PowerShell friction:** `terraform init -backend-config=backend.hcl` needs the arg quoted
  (`"-backend-config=..."`) or PowerShell splits on `=`; `$(cat <<'EOF')` heredocs don't exist (use
  `git commit -F file`); `~/.ssh/config` with bad perms blocks ssh (use `-F NUL`); winget-installed
  tools need a PATH refresh from the registry in already-open shells; `stash@{0}` needs quoting.
**Why it matters / what I'd redo:** Two of these (the boot-pull race and the credit throttle) are the
kind of bug you only find by actually applying to a real account — `validate`/`plan` can't surface
them. I'd default cache nodes to a small non-burstable type for any run that drives real load, and
always drive load from a non-shard node. **Open question for next session:** does a cache-server
re-register on its own after an etcd lease *lapses* (vs needing a process restart)? Recovery here took
an instance reboot, but that was confounded by the credit throttle — isolate it under Sub-stage C
failover testing.
**Links:** ADR 0028 (First-apply findings), commit `fix(terraform): unblock first AWS apply...`,
`terraform/cluster/{cache.tf,s3.tf,userdata/cache.sh.tftpl}`, `terraform/README.md`.

### 2026-06-06 - Real workloads: replaying ShareGPT instead of synthetic traffic
**Phase:** 4 (making the benchmark practical, not theoretical)
**What I was doing:** Added a `-trace` mode to `loadgen` that replays a real multi-turn chat
dataset (ShareGPT, the same one vLLM's `benchmark_serving.py` uses) instead of the synthetic
hot-prefix model. An offline Python step (`scripts/prep_sharegpt.py`) tokenizes the
conversations; `loadgen` replays the token IDs through the existing Lookup/Fetch/Write path.
**What I learned / what broke:**
- **For a cache, the access pattern IS the real data — payload bytes are not.** Hit rate,
  eviction, and load balance depend only on *which blocks are requested and when*, so a real
  trace makes the numbers real with zero GPU/model needed. Real KV tensors only matter for the
  separate Phase 4.5 TTFT benchmark.
- **Real prefix reuse comes from multi-turn conversations, not a knob.** Turn N+1 re-sends turns
  1..N as its prefix, so those blocks are already cached. Verified in the trace: a conversation's
  turns 0/1/2 share an identical token head. First real run: **31.9% block hit rate, 0
  correctness violations** on 6.5k requests.
- **Tokenization belongs offline, in Python, with a real tokenizer.** Go has no good HF tokenizer;
  splitting tokenize (Python, tiktoken `cl100k_base`) from replay (Go) keeps block lengths
  realistic while the Go side still does the real chained block-hashing (ADR 0010). Same
  separation-of-concerns seam idea as the cold tier.
- **Hit rate is a function of cache size vs working set.** With `-max-bytes` below the working
  set the evictor drops blocks before reuse; raising it lifts the hit rate. Sweeping `-max-bytes`
  and plotting the curve is the portfolio artifact, not a single number.
- **Windows gotcha:** PowerShell *drops* an empty-string arg (`-metrics-addr ""`) passed to a
  native exe, so Go's flag parser grabbed the next token. Use a real port or the `-flag=` form.
**Why it matters / what I'd redo:** Moves the project from "synthetic load proves it works" to
"real workload, real reuse, still correct." Next: replay this against the AWS cluster under
`aws-chaos.sh` and capture Grafana panels.
**Links:** `scripts/prep_sharegpt.py`, `cmd/loadgen/trace.go`, `cmd/loadgen/main.go`

### 2026-06-05 - S3 cold tier: keeping the cloud out of a cloud-free core
**Phase:** 4 (AWS Sub-stage E — the cold tier, ahead of the Terraform)
**What I was doing:** Built spill-on-evict + Fetch read-through to an S3 cold tier
(`internal/coldtier`), the one real code change for the AWS deployment. Captured in ADR 0027.
**What I learned / what broke:**
- **One eviction chokepoint = one hook.** Both memory-pressure eviction and the TTL sweep funnel
  through `Store.evict`, so a single `SpillFunc` call there demotes exactly the right blocks — and
  because the explicit `Evict` RPC goes through `Delete` (a *different* method), a client deletion
  correctly does NOT resurrect from cold. Finding the one place all the paths converge beat
  sprinkling hooks across `evictOne`/`sweepIdle`.
- **"Cloud-free core" is a dependency-graph property, enforced by seams.** The trick is that
  `cache` exposes a plain `func` callback and `server` a tiny `coldReader` interface — *they define
  the seam, the leaf `coldtier` package implements it* — so neither imports the AWS SDK and both
  still `go test` without it. Same shape as how replication and metrics were already decoupled.
  Importing the SDK into a widely-used package would have forced it into every test binary.
- **Async under a lock means "enqueue and return," nothing more.** Spill is called while a stripe
  lock is held, so it must not do S3 I/O (hundreds of ms) there. It pushes to a bounded worker pool
  and *drops* on a full queue — best-effort, because a lost spill is a recompute, not a violation
  (ADR 0013). The expensive part (framing the multi-MB blob, the PutObject) happens in the worker.
- **The correctness invariant has to survive the tier boundary.** Storing only KV bytes in S3 would
  force read-through to serve unverified bytes — an ADR 0016 hole. So the cold object is
  self-describing (`version + token_ids + kv`), and read-through re-applies the *same* version/token
  guards the hot path does. A cold hit can only return bytes stored under that exact
  `(model, block_hash)`; a mismatch is a miss.
- **Re-admit can thrash.** Re-admitting a cold hit to RAM keeps repeats fast, but under a working
  set larger than RAM it loops admit→evict→spill→re-upload. Skipping re-admit when it would breach
  the hard ceiling bounds the churn — a reminder that tiering's win depends on the hot tier being
  big enough for the *hot* set.
**Why it matters / what I'd redo:** The transferable idea is decoupling an optional cloud dependency
behind a seam the core owns, so the core stays testable offline. Next: a WSL2 `-race` pass over the
spill pool + re-admit, an S3 lifecycle rule so cold objects expire, and the live cluster verify
(force eviction → objects in the bucket → recovered cold hit, zero violations).
**Links:** ADR 0027; `internal/coldtier/{coldtier,s3}.go`, `internal/cache/store.go` (SpillFunc),
`internal/server/server.go` (read-through), `cmd/cache-server/main.go` (`-cold-bucket`).

---

### 2026-06-05 - Chaos harness: an invariant isn't tested until a wrong byte fails the build
**Phase:** 4 (chaos sub-stage)
**What I was doing:** Stood up `cmd/chaos` — a harness that builds the binaries, launches a 3-node
etcd-backed cluster, drives verifying load through it, and hard-kills random nodes on a schedule —
then asserts zero correctness violations. Added a `-verify` correctness oracle to the load generator
and chaos-cluster scrape targets to `prometheus.yml`. Captured in ADR 0026.
**What I learned / what broke:**
- **A correctness invariant is just prose until it's an executable assertion.** ADR 0016 says "never
  serve KV that mismatches the requested key." The way to *test* that under chaos is to make each
  block's payload a deterministic function of its hash (`payload = f(hash)`), then regenerate and
  compare on every Fetch. Now a corrupt byte *or* a mis-served block both surface as a mismatch, the
  process exits non-zero, and the chaos run becomes a CI gate. A throughput graph could never catch
  this — it can't tell a correct miss from a silently-wrong hit.
- **A miss is not a violation — that distinction is the whole design.** Under a kill, blocks vanish
  (eviction, owner moved). The oracle treats `NotFound` as a legitimate miss and only a *byte
  mismatch* as fatal. Conflating the two would make every failover look like corruption.
- **`go run` can't be chaos-killed; build the binary.** `go run` spawns a compiler+child, so
  `Process.Kill` hits the wrapper and orphans the real server — the lease never lapses and there's
  nothing to fail over. Building once and exec'ing the binary is what makes the kill a real crash.
- **SIGKILL ≠ SIGTERM here, on purpose.** A hard kill is the *unplanned* loss the lease TTL exists
  for; SIGTERM would trigger the graceful drain (ADR 0023), which deregisters first — a different
  story. The crash path exercises lease-expiry → ring-removal → read-failover with zero cooperation.
- **Recovery is bounded by the lease TTL, and you can watch it.** With a 5s lease, the per-2s `req/s`
  line dips after a kill and climbs back within ~the TTL; a killed node's `:910x` Prometheus target
  goes DOWN and its series stop — the failure made visible. Live result: 3 kills / 2 restarts, 4815
  requests, **0 violations, 0 errors, 0 degraded** — failover was seamless enough the client never
  even degraded to a miss.
- **The replicator log floods under a kill.** When the dead node was someone's replica, the primary
  logs one "connection refused" per block until the ring drops it. Harmless (the primary already
  acked; a lost replica update is a future miss), but loud enough to bury a real VIOLATION line — a
  follow-up should dedupe that log.
**Why it matters / what I'd redo:** The transferable idea is the **oracle**: design the workload so
the invariant you care about is *checkable from the data itself*, not inferred from metrics. That's
how you chaos-test correctness rather than just availability. Next: one WSL2 `-race` pass over the
fault loop + oracle, and partition/latency faults when the cluster moves to Linux/EC2 (Sub-stage E).
**Links:** ADR 0026; `cmd/chaos/main.go`, `cmd/loadgen/main.go` (`-verify`/`fillVerifiable`),
`cmd/loadgen/main_test.go`, `deploy/observability/prometheus/prometheus.yml`.

---

### 2026-06-04 - Grafana dashboards: PromQL is where raw counters become a story
**Phase:** 4 (Sub-stage F)
**What I was doing:** Built a local-first observability stack under `deploy/observability/` — a
docker-compose of Prometheus + Grafana with an auto-provisioned datasource and a 10-panel
"KV Cache — Overview" dashboard driven by the Sub-stage E metrics.
**What I learned / what broke:**
- **A histogram is useless without `histogram_quantile` + `rate` over `le`.** The server exposes
  `..._bucket` cumulative counters; a latency percentile is
  `histogram_quantile(0.99, sum by (le, method) (rate(..._bucket[$__rate_interval])))`. The `sum by
  (le)` is what aggregates buckets across instances before the quantile — quantiles don't average,
  so you must combine the raw buckets first, not the per-node p99s.
- **Rates, not raw counters, on a dashboard.** Counters only ever climb; `rate(...[window])`
  turns them into the per-second view a human reads. `$__rate_interval` (a Grafana built-in) sizes
  the window to the scrape interval automatically, so the same JSON is correct at any scrape rate.
- **Provisioning = no clicks, survives a fresh container.** A fixed datasource `uid` ("prometheus")
  referenced by the dashboard JSON is what makes the wiring reproducible; a file provider loads the
  dashboard on boot. This is the difference between a demo I rebuild each time and infra-as-code.
- **A real port collision.** The cache-server's metrics endpoint and Prometheus both want 9090;
  mapped Prometheus to host 9091 and scraped the host via `host.docker.internal` (with a
  `host-gateway` extra_hosts so it also works on Linux, not just Docker Desktop).
- **Local-first is a deliberate deviation.** The plan deploys on AWS from Phase 2 (ADR 0006), but
  iterating PromQL/panels against a live local node beats a `terraform apply` loop. Same dashboard
  JSON ships to the cloud later — the artifact is portable, only the scrape targets change.
**Why it matters / what I'd redo:** The transferable skill is reading a histogram correctly in
PromQL — it's the single most-flubbed thing in Prometheus interviews. Next time I'd add a couple of
recording rules for the hit-rate ratio so the dashboard query is cheaper, but it's premature at one
node. Sub-stage G (CloudWatch logs + alarms) is the remaining observability bullet.
**Links:** ADR 0025; `deploy/observability/` (compose, prometheus.yml, grafana provisioning +
`kvcache-overview.json`).

---

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
