# 02 - Roadmap & Working Agreement

> The phased plan lives in full in [`00-project-plan.md`](./00-project-plan.md). This doc is the
> working layer: the milestone checklist, definition of done, and working agreement.

## Working Agreement

This is an **educational** project. Prefer the path that teaches more, even if slower. For each
non-trivial component we follow the teaching loop:

**Teach -> Design -> Skeleton -> HC implements -> Review -> Capture.**

If HC is stuck, Claude gives a hint or leading question before the answer.

### Guided vs. Scaffold

- **HC implements [guided]:** consistent-hashing ring, replication/failover, leader-election /
  etcd integration, eviction policy, gRPC handler logic, interesting load-generator logic,
  chaos-test logic, and vLLM connector tensor-copy hooks.
- **Claude scaffolds [scaffold]:** project structure, build/config/boilerplate, interface/type
  stubs after design agreement, repetitive/mechanical code, test scaffolding.
- When unsure which side a task is on, ask.

## Definition of Done

- [ ] Builds: `go build ./...`
- [ ] Tests pass with the race detector: `go test ./... -race`
- [ ] Formatted + vetted: `gofmt`, `go vet ./...`
- [ ] Docs + ADRs updated in the same change
- [ ] Learning-log entry if something was learned/broke
- [ ] Small, reviewable commit with a Conventional Commits message

## Milestone Checklist

- [ ] **Phase 0 - Foundation:** local vLLM + measured single-node prefix-cache hits; verified AWS +
      Terraform toolchain.
- [~] **Phase 1 - Single-node external cache:** Go cache server, gRPC proto (Go + Python clients),
      in-memory shard, vLLM `KVConnectorBase_V1` integration, synthetic load generator, TTFT
      benchmark. **Current (verified 2026-05-25):** CPU-only core done + clean (server, striped
      store, block-hash, load generator, eviction seam, Python support libs; build/vet/gofmt/plain
      test pass). **Resolved in Phase 4.5 (2026-06-08, ADR 0031):** the vLLM tensor-copy hooks are
      wired and the TTFT exit gate (ADR 0015) is *measured* — it inverts at single-node ≤3B (load
      4.7 ms/block > recompute 3.4 ms/block, env-capped), so the headline win moves to the distributed
      cloud run. `-race` now passes in WSL2 (this Windows box has 32-bit MinGW).
- [~] **Phase 2 - Two-node distributed cache (local-first, then AWS):** consistent-hash ring
      (prefix-affinity, ADR 0014), client-side routing with degrade-to-miss, multi-process local
      harness, then Terraform cluster on AWS. **etcd deferred to Phase 3 (ADR 0018):** static ring
      membership for now. **Started 2026-05-25.**
- [~] **Phase 3 - Replication & failover:** RF=2, async replication, replica promotion, etcd lease
      membership, graceful drain wired to Spot interruption. **Status 2026-05-28:** Sub-stages
      A (etcd membership, ADR 0020), B (RF=2 async replication, ADR 0021), C (implicit promotion
      via ring rotation, ADR 0022), and D (graceful drain + Spot, ADR 0023) all landed locally.
      Per-shard leader-election leases were *not* needed — the deterministic ring + replica-at-
      LookupN[1] makes promotion implicit (see ADR 0022). **Remaining for the Phase 3 box-check:**
      WSL2 `-race` pass on cluster/coord/server/spot; multi-node chaos test (kill + verify served
      from replica); IAM + S3 cold tier (deferred — slot belongs with Phase 4 AWS work).
- [x] **Phase 4 - Eviction, observability, chaos:** LRU+watermark eviction (ADR 0024), Prometheus
      metrics (ADR 0025), process-kill chaos harness (ADR 0026), S3 cold tier (ADR 0027), and the
      AWS cluster live + verified (ADR 0028, first `terraform apply` 2026-06-06). **Deferred AWS
      batch (one paid window):** cold-tier round-trip verify, AWS chaos (`tc`/`iptables`), CloudWatch
      alarm wiring, and re-running the Phase-5 benchmarks on the 3-node cluster.
- [x] **Phase 4.5 - GPU benchmark (single-node, done 2026-06-08; distributed run deferred to cloud).**
      Wired the vLLM worker-side tensor-copy hooks (probe live paged-KV layout → save full blocks →
      load back into paged slots) + a BatchFetch RPC; ran TTFT on TinyLlama-1.1B and Qwen2.5-3B
      (RTX 3080, WSL2, vLLM 0.22.1). **Finding (ADR 0031):** save/load works + is correct, but the
      ADR 0015 inequality *inverts* at single-node ≤3B — external load 4.7 ms/block vs recompute
      3.4 ms/block — bottlenecked by Python serialization + **unpinned** H2D copy (WSL2
      `pin_memory=False`), proven by a null BatchFetch result. Deficit closes with model size
      (−169% @1B → ~−48% @3B). **The headline TTFT win is deferred to the distributed/cloud GPU run**
      (pinned memory + zero-copy transport + non-throttled KV cache) — bundled with the AWS paid batch.
- [x] **Phase 5 - Differentiator: cost-aware + fairness eviction (complete, local, 2026-06-07).**
      5a (ADR 0029): GDSF cost-aware value `H = L + freq·cost/size` + static per-tenant caps. 5b
      (ADR 0030): elastic work-conserving floors + the `fairness_weight ∈ [0,1]` knob
      (`H_eff = H/(1+w·overage)`), swept into the efficiency-vs-fairness Pareto frontier —
      elastic Pareto-dominates the static caps (`docs/benchmarks/phase5{a,b}-eviction.md`).
- [ ] **Phase 6 - Polish & story:** README + prior-art, demo video, blog post, resume bullets.

## Git Workflow

- Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`).
- Small commits over big batches. Branch off `main`; do not commit straight to `main` for
  non-trivial work.
- The pre-commit hook must pass; do not bypass it.
