# ADR 0004 — Deploy on AWS, provisioned via Terraform

- **Status:** accepted
- **Date:** 2026-05 (cloud integration)
- **Deciders:** HC

## Context

The project needs a real multi-node substrate from Phase 2. The cloud + IaC signal is a preferred
qualification at most surveyed firms ("preferably AWS" at Amazon). See `00-project-plan.md` §5.

## Decision

Deploy on **AWS**, with the entire cluster (VPC, instances, IAM, S3, ECR) managed as
**Terraform**. Remote state in S3 with a DynamoDB lock.

## Alternatives considered

- **GCP / Azure** — fewer posting mentions; concepts transfer, so AWS by default (swap to GCP only
  if specifically targeting Google).
- **CloudFormation / CDK** — Terraform is cloud-agnostic and the most asked-for; strongest IaC tell.
- **Local Docker Compose** — no cloud signal, and doesn't exercise IAM/VPC/Spot.

## Consequences

- Doubles coverage (distributed systems + cloud) for near-lateral effort.
- `terraform apply`/`destroy` makes chaos runs reproducible (rebuild a clean cluster between
  experiments).
- Requires Phase 0 cloud onboarding (account, billing alarm, IAM admin user, toolchain).
