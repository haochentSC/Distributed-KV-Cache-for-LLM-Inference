# CLAUDE.md

Distributed KV cache for LLM inference: a CPU-only Go service that shares attention KV tensors
across a multi-node cluster (consistent hashing, replication, etcd-coordinated failover,
cost-aware + fair eviction), integrated with vLLM and deployed on AWS via Terraform.

**Source of truth:** [`docs/00-project-plan.md`](docs/00-project-plan.md) — strategy, phases,
decisions log. Don't duplicate it here. Decisions are recorded as ADRs in `docs/adr/`.
**Current phase: Phase 5 (the differentiator — cost-aware + fair eviction).**
Phases 1–4 are done: the CPU-only core (server, store, block-hash, load generator, Python connector
libs), the consistent-hash ring + client routing, etcd-coordinated RF=2 async replication with
implicit promotion and graceful/Spot drain (ADRs 0021–0023), and the Phase 4 LRU+watermark eviction,
Prometheus, and process-kill chaos harness (ADRs 0024–0026). The **AWS cluster went live + verified**
(Sub-stage E, ADR 0028): first `terraform apply` succeeded 2026-06-06 — 3-node etcd quorum, 3 Spot
cache nodes, ECR, S3 cold tier, IAM, CloudWatch; `loadgen -verify` on AWS = 0 violations.
**Phase 5 COMPLETE (local, the differentiator).** 5a (ADR 0029): GDSF cost-aware eviction
(`H = L + freq·cost/size`) + static per-tenant quotas behind the Phase 4 seam; `-eviction gdsf` /
`-tenant-quota`, loadgen `-multitenant` (`docs/benchmarks/phase5a-eviction.md`). 5b (2026-06-07,
ADR 0030): elastic work-conserving floors + the `fairness_weight ∈ [0,1]` knob
(`-eviction gdsf-elastic -fairness-weight w`, `H_eff = H/(1+w·overage)`, `OverQuota()`=false =
watermark-only). Swept the efficiency-vs-fairness Pareto frontier (`scripts/phase5b-sweep.ps1`,
`docs/benchmarks/phase5b-eviction.md`): `w=0` efficiency corner (20.0%/1.9% min), `w≥0.25` fairness
plateau (~14%/~12% min); **elastic Pareto-dominates the 5a static caps**; the knob saturates fast.
**Phase 4.5 GPU path:** single-node TTFT measured + decomposed (ADR 0031) — the cache loses at ≤3B for
*environmental* reasons (WSL2 unpinned memory, Python/protobuf hot path, throttled KV cache), deficit
closing with model size. **Distributed run prepped, no spend (2026-06-09, ADR 0032):** headline is a
30B-class model with tensor parallelism (TP=4) on a `g5.12xlarge`; the connector keys each rank's
KV-head shard distinctly (`shard_model_id`; server untouched), a cost-guarded GPU node lives in
Terraform (`gpu_count` defaults to 0), plus a TP-aware distributed driver and `loadgen -verify-coldtier`.
**Deferred to one paid window — see the runbook `docs/benchmarks/aws-batch-runbook.md`:** the distributed
TTFT run, cold-tier round-trip verify, AWS chaos (`aws-chaos.sh`, `tc`/`iptables`), CloudWatch alarms,
and the 5a/5b cluster re-run.
**Remember to `terraform destroy` the AWS cluster when not actively testing — it bills hourly (the GPU node especially).**

<!-- Keep this file < ~200 lines: it loads every session. Always-true rules only.
     Topic/path-specific guidance → .claude/rules/. Deep procedures → Skills (.claude/skills/). -->

## This is an educational project (the most important rule)

The goal is HC *deeply learning* distributed systems, Go, LLM/vLLM internals, and AWS/Terraform —
**not** shipping fastest. Prefer the path that teaches more, even if slower. HC is new to Go,
distributed systems, and LLM serving — when a concept is likely unfamiliar, **teach it briefly
inline** (a couple of sentences + a pointer), then continue.

## Working agreement: guided implementation, NOT autocomplete

For each non-trivial component, follow this loop — **do not write the core logic for HC**:

1. **Teach** — the concept and why it matters *here*.
2. **Design** — lay out options + tradeoffs; decide together.
3. **Skeleton** — give interfaces, signatures, struct defs, and stubs with `TODO`s + guidance
   comments — **not** the filled-in logic.
4. **HC implements** the logic.
5. **Review** — point out bugs/improvements; ask 1–2 questions to check understanding.
6. **Capture** — record an ADR + a learning-log entry.

If HC is stuck, give a hint or leading question *before* the answer.

### The dividing line

- **HC implements [guided]:** consistent-hashing ring, replication & failover, leader-election /
  etcd integration, the eviction policy (esp. the cost-aware + fairness engine, plan §3.5), gRPC
  handler logic, the interesting parts of the load generator, chaos-test logic.
- **Claude may scaffold directly [scaffold]:** project structure, build/config/boilerplate
  (`go.mod`, Makefile, Dockerfile, Terraform boilerplate, CI), interface/type stubs *after the
  design is agreed*, repetitive/mechanical code, test scaffolding. Briefly say what you generated
  and why.
- **When unsure which side a task falls on, ASK.** Don't assume.

## Commands

Most don't exist yet (Phase 0). As code lands, these are the verifiable gates:

- Build: `go build ./...`
- Test (always with the race detector): `go test ./... -race`
- Format / vet: `gofmt -l .` (must print nothing) and `go vet ./...`
- Terraform: `terraform fmt -check`, `terraform validate`, `terraform plan` (never auto-`apply`).

The pre-commit hook (`.githooks/pre-commit`, wired via `core.hooksPath`) runs gofmt/vet/test on
commit. Don't bypass it with `--no-verify`.

## Layout

```
docs/        # 00-project-plan (source of truth) + 00-initial-prompt; 01-05 working docs; adr/; learning-log
.claude/     # rules/ (path-scoped) + skills/ (load-on-demand) + settings
.githooks/   # pre-commit (gofmt/vet/test)
```
Go code (`cmd/`, `internal/`, `proto/`), Python connector, and `terraform/` are added from
Phase 1 on — update this section as they appear.

## Conventions

- **Go:** standard `gofmt`; exported identifiers documented; small interfaces; errors wrapped with
  `fmt.Errorf("...: %w", err)`; no naked `panic` in library code; concurrency reasoned about
  explicitly (the race detector is mandatory). The eviction policy lives behind a swappable
  interface (plan Phase 4).
- **Proto / keys:** `prefix_hash` is **opaque bytes** server-side (ADR 0010); one proto generates
  both a Go and a Python client.
- **vLLM:** integrate via a custom `KVConnectorBase_V1`, **no fork** (ADR 0008).
- **Terraform:** remote state in S3 + DynamoDB lock; no static credentials (IAM roles/instance
  profiles); cache nodes on Spot, etcd on-demand (ADRs 0006, 0009).

## Git workflow

- **Conventional Commits** (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`).
- **No AI co-authorship.** Never add `Co-Authored-By` / AI-attribution trailers (Claude, Cursor,
  etc.) to commit messages. Commits are authored by HC.
- Small, reviewable commits over big batches. Branch off `main` for non-trivial work.
- Update docs + ADRs **in the same change** as the code — they're part of "done".
- Close the loop: run build/tests/linter yourself and report results before calling a task done.

## Hygiene

- Plan before non-trivial code; show the plan and wait for approval.
- **Surface uncertainty.** If guessing about an API, a Go idiom, or an AWS detail, say so and
  verify — a checked answer beats a confident wrong one.
- Tell HC when you've written to memory so it can be audited via `/memory`.
