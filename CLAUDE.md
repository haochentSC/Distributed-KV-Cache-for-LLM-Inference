# CLAUDE.md

Distributed KV cache for LLM inference: a CPU-only Go service that shares attention KV tensors
across a multi-node cluster (consistent hashing, replication, etcd-coordinated failover,
cost-aware + fair eviction), integrated with vLLM and deployed on AWS via Terraform.

**Source of truth:** [`docs/00-project-plan.md`](docs/00-project-plan.md) — strategy, phases,
decisions log. Don't duplicate it here. Decisions are recorded as ADRs in `docs/adr/`.
**Current phase: Phase 6 (polish & story) — all benchmark phases COMPLETE.**
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
closing with model size. **Distributed prep (2026-06-09, ADR 0032):** connector keys each TP rank's
KV-head shard distinctly (`shard_model_id`; server untouched), a cost-guarded GPU node in Terraform
(`gpu_count` defaults to 0), a TP-aware distributed driver, and `loadgen -verify-coldtier`.
**Rescoped 2026-06-10 (ADR 0033):** AWS approved the G/VT Spot quota only to **8 vCPUs** (standard GPU
ceiling), so the AWS headline is now **single-GPU** — `g5.2xlarge` (1× A10G), `--tensor-parallel-size 1`,
a 7-8B model (the pinned-memory env ADR 0031 said would flip the cache net-positive). The **TP=4 / 30B**
path stays in code but is **deferred** to an AWS-sales quota or a GPU-specialized cloud (Lambda/RunPod/Modal;
the TP keying is provider-agnostic). The "why AWS" story: AWS for the *distributed* system (real failure
domains, Spot-as-chaos, S3 cold tier, IaC) — not for scale; GPU compute belongs on a GPU cloud.
**The paid window EXECUTED 2026-06-10/11 (`docs/benchmarks/phase45-distributed-gpu.md`) — ~$2-3 total.**
The headline is **measured**: TTFT crossover on a real GPU (g6.2xlarge 1× L4 — g5/A10G had zero Spot
capacity region-wide) — the cache loses 5% at trivial prefixes, breaks even ~1k tokens, **wins +7.6% @ 2k
/ +10.9% @ 4k tokens** (116 ms off a 1,070 ms TTFT, cross-AZ). ShareGPT replay on the live cluster:
**32.7% hit rate**, 0 violations, p50 62 ms. Chaos (latency / etcd partition / real node kill): all
**0 violations**; the node-loss alarm fires after the `treat_missing_data="breaching"` fix. The 5a/5b
cluster re-run **reproduces the local Pareto frontier**. Fixes landed: container `AWS_REGION` (the cold
tier was dead without it) + `GOMEMLIMIT` (t3.small OOM), `cache_max_bytes` 1.0 GB, AZ-independent GPU
subnet (`gpu_az`). Known limitation: the spill pipeline sheds under burst eviction (drop-over-stall by
design; ~40-60 S3 PUT/s vs hundreds/s bursts). **RunPod Option B (ADR 0034, 2026-06-11): Session A
EXECUTED** — long-context curve + serving demo on 1× A100 80GB; **no 32k crossover** (A100 prefills
too fast; warm path Python-bound); 14B *worse* than 7B at same tokens (KV-bytes/FLOPs). AWS L4
**+10.9% @ 4k** stays the resume headline (`phase45-gpu-cloud.md`). **Session B EXECUTED 2026-06-12**
(4× A40, TP=4 / Qwen2.5-32B): probe gate passed, then the benchmark **caught a real server bug** — the
store keyed entries by block hash alone, so the four rank-agnostic shard ids clobbered one map slot
last-writer-wins (silent-corruption risk: the stamped hash matches across ranks, so the ADR 0016 guard
passes while serving another rank's shard). **Fixed (ADR 0035):** store keys namespaced by model
(`storeKey = SHA-256(model_id ‖ wire hash)`; `Entry.WireHash` kept for spill/replication), re-validated
on hardware — load/save active on all 4 ranks, 9,280 hits / 0 misses, 512 writes = 128 blocks × 4 ranks
exactly once. TP keying (ADR 0032) validated end-to-end. **Next: Phase 6 (polish & story).**
**AWS cluster DESTROYED; all RunPod pods TERMINATED.**

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
