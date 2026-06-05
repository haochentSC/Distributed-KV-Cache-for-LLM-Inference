# ADR 0026 — Chaos harness: process-kill fault + correctness oracle

- **Status:** accepted
- **Date:** 2026-06-05 (Phase 4, chaos sub-stage)
- **Deciders:** HC (+ Claude)

## Context

Phase 4's last deliverable is the one that turns "we believe the cache is correct under failure"
into a *measured, repeatable* claim — the "core v1 ships" gate (plan §Phase 4). Phases 2–3 built
the machinery (consistent-hash ring, RF=2 async replication, etcd lease-expiry failover); none of
it has been exercised while a node is actually dying underneath live traffic.

Two things were missing:

1. **A fault injector** that kills nodes on a schedule against a real running cluster.
2. **An executable assertion.** ADR 0016 pinned the correctness invariant — *never serve KV that
   mismatches the requested `(block_hash, model_id, token_ids)`; misses are fine* — but until now
   that invariant was prose. A chaos run that only watches throughput can't tell a correct miss
   from a silently-corrupt hit. We need an oracle that makes a wrong byte **fail loudly**.

The constraint is the dev box: a Windows laptop with no `tc`/`iptables`, so network partition and
latency injection (the other classic chaos faults) aren't portable here.

## Decision

**1. A standalone harness (`cmd/chaos`) that owns the whole experiment.** It builds the
`cache-server` + `loadgen` binaries, launches N nodes against a real local etcd, waits until all N
have *registered* (reusing the same `WatchMembers` seam clients use, so "up" means what the ring
sees), runs verifying load for a wall-clock window, and kills/restarts random nodes on a schedule.
Its **exit code is the verdict** — non-zero iff the load reported ≥1 correctness violation — so it
doubles as a CI gate.

**2. The fault is a hard process kill (`Process.Kill` ≈ SIGKILL), and only that.** A SIGKILL is a
*crash* — the unplanned-loss case the etcd lease TTL exists for. It exercises the entire
lease-expiry → ring-removal → read-failover path with zero cooperation from the dying node (unlike
the graceful SIGTERM drain of ADR 0023, which is a different story). Network partition + latency
injection need `tc`/`iptables` on Linux and are **deferred to the AWS infra stage** (Phase 3
tracker Sub-stage E), where the nodes are real Linux EC2 instances. A clean kill is the
highest-value *portable* fault, so it ships first.

**3. The correctness oracle: `payload = f(block_hash)`, verified on every Fetch (`loadgen
-verify`).** Each block's bytes are a deterministic, non-cryptographic function of its hash
(`fillVerifiable`, an xorshift64\* PRNG seeded from the 32-byte hash). Two properties make it an
oracle: it's **deterministic** (a reader can regenerate the expected bytes from the hash alone) and
**injective enough** (block A's content can never equal block B's). So *both* failure shapes the
invariant forbids — corrupt bytes, and serving the wrong block — surface as a byte mismatch. A
mismatch increments a violation counter and the process exits non-zero at the end. A `NotFound` is
*not* a violation: it's a legitimate miss (eviction, or a failover that moved the owner).

**4. Build binaries, don't `go run` them.** `go run` spawns a compiler+child and the kill must hit
the *server*, not a wrapper — kill the wrapper and the real server is orphaned, the lease never
lapses, and there is nothing to fail over. Building once into a temp dir and exec'ing the binary
directly is what makes the kill real.

**5. An `aliveAboveRF` guard keeps ≥ `rf` nodes up.** The fault loop skips a kill that would drop
the live count below the replication factor, so a key's primary *and* replica are never both down
at once. This keeps the **availability** story clean (load recovers within ~the lease TTL); note
**correctness holds either way** — both-down just yields misses, never wrong bytes.

**6. Live stats + `prometheus.yml` targets make recovery visible.** `loadgen -stats-every` prints a
per-interval `req/s` line so the dip-and-recovery after each kill is readable in the terminal; the
chaos cluster's metrics ports (`:9100–:9102`) are added to `prometheus.yml` so a killed node shows
as a DOWN target and its series stop in Grafana — the failure, made visible.

## Why not these alternatives

- **Kill via SIGTERM.** That triggers the *graceful* drain path (ADR 0023), which deregisters from
  etcd first — the opposite of a crash. It tests the planned-shutdown story, not the lease-expiry
  failover story. We want the unplanned one; the Spot-drain path already covers graceful.
- **A separate verifier process that reads back every block.** Doubles the traffic and adds its own
  routing. Folding the oracle into the load generator means the *same* request that could be served
  wrong is the one that checks — no observability gap between write and verify.
- **A cryptographic checksum per block.** Overkill. The oracle only needs "A's bytes ≠ B's bytes and
  are reproducible from the key"; a fast xorshift fill clears that bar at 2 MiB without hashing cost
  on the hot path.
- **Network partition now, via a userspace proxy.** A TCP proxy that drops packets would approximate
  a partition on Windows, but it's a second moving part to trust and it doesn't match how the AWS
  chaos will actually be done (`tc`/`iptables`). Defer partition to where it's native rather than
  build a throwaway.
- **Let kills drop below `rf`.** Tempting (it tests the both-down case), but it muddies the headline
  availability number with expected misses. Correctness is asserted unconditionally regardless; the
  guard is only about keeping the *recovery* measurement legible. Running with `-nodes <= -rf` is
  still allowed (the harness warns), just not the default.

## Consequences

- **Verified live (3 nodes, rf=2, lease-ttl=5s, 40s window):** 3 kills / 2 restarts, 4815 requests,
  **0 correctness violations, 0 errors, 0 degraded-to-miss**, 63.8% block hit rate. Read-side
  failover was seamless enough that the client never even degraded to a miss. Per-shard distribution
  reproduced the ADR 0014 hot shard (83.8% on the prefix-affinity owner). ADR 0016 holds under
  crash-kill. This is the executable form of the phase's exit invariant.
- The harness is **portable** (no Linux-only tooling) so it runs in CI on any platform and locally
  on the dev laptop. Partition + latency faults are explicitly carried over to Sub-stage E (AWS).
- **Known rough edge:** when a primary's replica is the just-killed node, the server's async
  replicator logs one `forward to <node> ... connection refused` line *per block* until the dead
  node leaves the ring — a harmless flood (the primary already acked the client; a lost replica
  update is a future miss, ADR 0021), but it's loud enough to bury a real `VIOLATION` line in the
  scrollback. A follow-up should rate-limit/dedupe that replicator log; left as-is here to keep this
  sub-stage to the harness itself.
- **`-race` debt carries over.** The chaos harness and the `-verify` path spawn goroutines and share
  counters; the Windows box still can't run `go test -race` (32-bit MinGW cgo). The fault loop +
  oracle want one WSL2 `-race` pass before this is called fully done (same carryover as the routing
  and etcd-watch code).
- CloudWatch logs + alarms (the cloud-native half of the observability bullet) remain deferred to
  the AWS deployment — they need a live AWS account and are tracked with Sub-stage E.
