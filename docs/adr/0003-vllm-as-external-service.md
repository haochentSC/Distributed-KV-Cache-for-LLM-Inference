# ADR 0003 — Integrate with vLLM as an external service, not a fork

- **Status:** accepted (refined by ADR 0008)
- **Date:** 2026-05-24
- **Deciders:** HC

## Context

The cache must integrate with vLLM to be useful. Forking vLLM couples us to its internals and
its release cadence, and keeps a GPU in the critical path of our work. See `00-project-plan.md` §1, §6.

## Decision

Keep the cache an **external service**; vLLM talks to it over gRPC. Do not fork vLLM.

## Alternatives considered

- **Fork vLLM / patch PagedAttention** — highest coupling and integration risk; rejected.
- **Thin Python proxy in front of vLLM** — viable fallback, sacrifices some performance.

## Consequences

- Cache layer stays GPU-free and decoupled from vLLM internals; all distributed-systems work runs
  on CPU.
- **Refined by ADR 0008:** the concrete mechanism is a custom `KVConnectorBase_V1` loaded
  dynamically — which delivers "external, no fork" via a maintained extension point rather than a
  bespoke proxy.
