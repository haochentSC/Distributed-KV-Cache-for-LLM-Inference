---
description: Terraform / AWS IaC conventions for this project
paths:
  - "**/*.tf"
  - "**/*.tfvars"
  - "terraform/**"
---

# Terraform conventions

This is [guided] territory for the interesting bits but [scaffold] for boilerplate — confirm
which when unsure. Cloud topology and the AWS service map are in
`docs/00-project-plan.md` §3/§5; deep runbook is the `cloud-deploy-aws` skill.

- **Remote state:** S3 backend + DynamoDB lock. No local state committed. (`docs/adr/0004`.)
- **No static credentials anywhere.** Nodes authenticate via IAM roles + instance profiles.
- **Pricing tier (ADRs 0006, 0009):** cache nodes on **Spot**; **etcd on-demand, never Spot**,
  3-node recommended for v1.
- **Instance sizing (Revision E):** size cache nodes for RAM (≈1 GB per 500-token prefix at
  2 MB/token). `t3.micro` is only for the Phase 0 toolchain smoke test.
- **Tag everything** (project, phase, owner) so cost is attributable and `destroy` is safe.
- **Workflow:** `terraform fmt` + `terraform validate` before commit; show `terraform plan` and
  wait for approval — **never auto-`apply`**. `apply`/`destroy` are HC-run actions.
- GPU nodes (Phase 4.5) are brought up only for the benchmark window and `terraform destroy`'d
  immediately after — the only expensive window in the project.
- Keep modules small and reproducible so chaos runs can rebuild a clean cluster.
