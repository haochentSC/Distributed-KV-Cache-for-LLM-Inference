# 05 — Cloud Deployment (AWS + Terraform)

> **Status: skeleton / index.** The runbook (Terraform module layout, `apply`/`destroy` flow,
> chaos with `tc`/`iptables`, Spot handling, cost discipline) lives in the **`cloud-deploy-aws`
> Skill**. This doc is the index + the project-specific topology decisions.

## Topology (compute split by role)

| Role | Compute | Pricing | Notes |
|---|---|---|---|
| Cache nodes (the artifact) | EC2 CPU (`t3`/`c7g`) | **Spot** OK | size for RAM (≈1 GB per 500-token prefix — Revision E) |
| etcd | EC2 CPU | **on-demand, never Spot**; 3-node recommended | Revision C / ADR 0009 |
| vLLM workers | EC2 GPU (`g5`/`g4dn`) | Spot, benchmark windows only | brought up Phase 4.5, then `terraform destroy` |

AWS service map (see [`00-project-plan.md` §3 cloud topology](./00-project-plan.md)): VPC/subnets/SG,
IAM roles + instance profiles (no static creds), S3 (cold tier + Terraform remote state),
DynamoDB (state lock), ECR (image), CloudWatch (logs + alarms).

## Cost discipline

- Billing alarm set in Phase 0.
- Cache cluster is pennies/hour; the only expensive window is the Phase 4.5 GPU benchmark —
  always `terraform destroy` GPU nodes immediately after.
- *Verify current instance pricing before budgeting — GPU prices drift.*

## IaC hygiene

Realized in Phase 3/4 Sub-stage E (ADR 0028). Full runbook: [`../terraform/README.md`](../terraform/README.md).

**Module layout** (`terraform/`):
- `bootstrap/` — S3 state bucket + DynamoDB lock table on **local state** (it can't store the
  backend that defines itself). Apply once.
- `cluster/` — the VPC, security groups, IAM, ECR, S3 cold tier, CloudWatch, the 3-node etcd
  quorum, and the Spot cache nodes — on the S3 **remote backend**. A flat root of small files
  (`network/security/iam/ecr/s3/cloudwatch/etcd/cache.tf`), not nested modules (premature at this
  size). user-data templates live in `cluster/userdata/`.

**Remote state:** S3 backend (versioned, encrypted, private) + DynamoDB lock, supplied at
`terraform init -backend-config=backend.hcl` (backends can't read variables). State and `*.tfvars`
are gitignored; `*.example` files are committed.

**Tagging:** provider `default_tags` stamps `project=kvcache, phase, owner, module` on everything,
so spend is attributable and `destroy` is auditable.

**Reproducible chaos:** `terraform destroy` + `apply` rebuilds a clean cluster between experiments —
that reproducibility (not just "infra as code") is why chaos runs live behind Terraform. Cache
nodes are discrete instances, not an ASG, so a chaos kill isn't auto-healed (ADR 0005/0028).

**Key decisions:** static-IP etcd bootstrap (no discovery service); ECR image under
systemd-managed Docker with SIGTERM→drain on stop (ADR 0023); IMDSv2 hop-limit 2 so the
in-container Spot poller reaches metadata; least-privilege IAM scoped to the cold-bucket ARN, no
static credentials (ADR 0004). Single public subnet, no NAT gateway (cost), SSH/metrics locked to
the operator IP.

## Orchestration decision

EC2 + Docker for v1 (raw instances make node-kill/partition chaos clean — a managed k8s control
plane would fight the chaos harness). EKS is a **post-v1 stretch** bullet, not a v1 dependency.
See [`00-project-plan.md` §5](./00-project-plan.md) and ADR 0005.
