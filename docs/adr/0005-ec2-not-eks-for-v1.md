# ADR 0005 — EC2 + Docker for v1; EKS as a post-v1 stretch

- **Status:** accepted
- **Date:** 2026-05 (cloud integration)
- **Deciders:** HC

## Context

The Phase 4 chaos harness kills nodes and partitions networks with `tc`/`iptables`. We need to
measure correctness violations and recovery time cleanly. See `00-project-plan.md` §5.

## Decision

Run v1 on **raw EC2 + Docker**. Capture **EKS** migration as a post-v1 stretch bullet.

## Alternatives considered

- **EKS / managed Kubernetes for v1** — a managed control plane reschedules pods and self-heals,
  which *fights* the chaos harness and contaminates the measurements. Higher learning surface too.
  Rejected for v1.

## Consequences

- We own failures cleanly on raw instances — the correct substrate for chaos testing.
- Lower learning surface for v1.
- EKS remains available later as its own resume bullet + blog post; it must not block v1.
