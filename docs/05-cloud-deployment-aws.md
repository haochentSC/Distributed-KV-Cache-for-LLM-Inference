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

<!-- TODO (Phase 2): document the module layout, remote-state backend (S3 + DynamoDB lock),
     tagging convention, and how `apply`/`destroy` makes chaos runs reproducible. -->

_Filled in Phase 2._

## Orchestration decision

EC2 + Docker for v1 (raw instances make node-kill/partition chaos clean — a managed k8s control
plane would fight the chaos harness). EKS is a **post-v1 stretch** bullet, not a v1 dependency.
See [`00-project-plan.md` §5](./00-project-plan.md) and ADR 0005.
