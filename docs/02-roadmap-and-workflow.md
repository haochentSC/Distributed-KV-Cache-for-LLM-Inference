# 02 — Roadmap & Working Agreement

> The phased plan lives in full in [`00-project-plan.md` §4](./00-project-plan.md). This doc is
> the **working layer**: the milestone checklist, the definition of done, and our working
> agreement. Update the checkboxes as we go.

## Working agreement (the core behavioral rule)

This is an **educational** project. Prefer the path that teaches more, even if slower. For each
non-trivial component we follow the teaching loop:

**Teach → Design (decide together) → Skeleton (interfaces/stubs + `TODO`s, no filled logic) →
HC implements → Review (bugs + 1–2 understanding-check questions) → Capture (ADR + learning log).**

If HC is stuck, Claude gives a hint or leading question *before* the answer.

### Guided vs. scaffold dividing line

- **HC implements [guided]:** consistent-hashing ring, replication/failover, leader-election /
  etcd integration, the eviction policy (esp. the §3.5 cost-aware + fairness engine), gRPC
  handler logic, the interesting parts of the load generator, chaos-test logic.
- **Claude scaffolds [scaffold]:** project structure, build/config/boilerplate (`go.mod`,
  Makefile, Dockerfile, Terraform boilerplate, CI), interface/type stubs *after design is
  agreed*, repetitive/mechanical code, test scaffolding.
- When unsure which side a task is on, **ask**.

(The same agreement is mirrored in `CLAUDE.md` so it loads every session.)

## Definition of done (per change)

- [ ] Builds: `go build ./...`
- [ ] Tests pass with the race detector: `go test ./... -race`
- [ ] Formatted + vetted: `gofmt`, `go vet ./...` (enforced by the pre-commit hook)
- [ ] Docs + ADRs updated in the same change (not batched at the end)
- [ ] Learning-log entry if something was learned/broke
- [ ] Small, reviewable commit with a Conventional Commits message

## Milestone checklist

- [ ] **Phase 0 — Foundation:** local vLLM + measured single-node prefix-cache hits; verified
      AWS + Terraform toolchain (billing alarm, IAM admin user, throwaway `t3.micro`). **← current**
- [ ] **Phase 1 — Single-node external cache:** Go cache server, gRPC proto (Go + Python clients),
      in-memory shard, vLLM `KVConnectorBase_V1` integration, synthetic load generator, TTFT benchmark.
- [ ] **Phase 2 — Two-node distributed cache on AWS:** Terraform cluster (VPC/SG/EC2/etcd/ECR/S3
      state), consistent hashing, shard routing.
- [ ] **Phase 3 — Replication & failover:** RF=2, async replication, replica promotion, etcd
      lease leader election, graceful drain wired to Spot interruption, IAM + S3 cold tier.
- [ ] **Phase 4 — Eviction, observability, chaos (CORE SHIPS GATE):** swappable LRU+TTL eviction,
      memory-pressure eviction, Prometheus/Grafana + CloudWatch, chaos harness, benchmark report.
- [ ] **Phase 4.5 — GPU benchmark:** real vLLM + GPU TTFT number, then `terraform destroy`.
- [ ] **Phase 5 — Differentiator:** 5a cost-aware + static fairness (must-ship); 5b elastic +
      `fairness_weight` knob + tradeoff curve (stretch).
- [ ] **Phase 6 — Polish & story:** README + prior-art, demo video, blog post, resume bullets.

## Git workflow

- Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`).
- Small commits over big batches. Branch off `main`; don't commit straight to `main` for
  non-trivial work.
- The pre-commit hook (`gofmt`/`go vet`/`go test -race`) must pass — don't `--no-verify`.
