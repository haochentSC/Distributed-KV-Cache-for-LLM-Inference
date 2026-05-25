---
name: cloud-deploy-aws
description: Runbook for the AWS + Terraform deployment — VPC/subnets/SG, EC2 (Spot cache nodes, on-demand etcd, GPU benchmark nodes), IAM roles/instance profiles, S3 (cold tier + remote state) + DynamoDB lock, ECR, CloudWatch, and reproducible chaos runs. Use in Phase 0 (toolchain) and Phases 2–4.5 (cluster), or when touching Terraform/AWS.
---

# Cloud deploy (AWS + Terraform) — runbook

Deep IaC/ops content, kept out of always-on context. Pairs with `docs/05-cloud-deployment-aws.md`
and the plan `docs/00-project-plan.md` §3/§5. Path-scoped specifics also in
`.claude/rules/terraform.md`.

Designing modules is [guided] for the interesting parts; boilerplate is [scaffold]. **Never
auto-`apply`** — show `plan`, HC runs `apply`/`destroy`.

## Phase 0 — toolchain smoke test
- AWS account, **billing alarm**, IAM admin user (not root), AWS CLI + Terraform installed.
- `terraform apply` a throwaway `t3.micro`, confirm, `terraform destroy`. Goal is to clear the
  toolchain hurdle, not build anything real.

## Phase 2 — the cluster
- VPC + subnets + security groups, single AZ for low-latency gRPC.
- Cache EC2 on **Spot**; **etcd on-demand (3-node recommended)** — never Spot (ADR 0009).
- **Remote state:** S3 backend + DynamoDB lock (set this up first).
- ECR for the cache image; IAM roles + instance profiles so nodes use **no static credentials**.
- Size cache nodes for RAM (Revision E), not just price.

## Phase 3 — failure handling on real infra
- Subscribe graceful drain to the **EC2 Spot interruption notice** (~2-min warning) → free chaos.
- S3 cold tier for evicted entries; IAM scoped to exactly what each node needs.

## Phase 4 — observability + chaos
- CloudWatch logs + a couple of alarms alongside Prometheus/Grafana.
- Chaos on raw EC2 with `tc` (latency/loss) and `iptables` (partitions). `terraform destroy` +
  `apply` rebuilds a clean cluster between experiments — that reproducibility is the point of IaC here.

## Phase 4.5 — GPU window
- GPU (`g5`/`g4dn`) Spot in the same VPC/AZ, run the benchmark, **`terraform destroy` immediately**.
  This is the only expensive window — verify current pricing first.

## Cost discipline
Billing alarm always on; cache tier is pennies/hour; confine GPU to the benchmark window; tag
everything for attribution.
