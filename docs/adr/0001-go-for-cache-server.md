# ADR 0001 — Go for the cache server

- **Status:** accepted
- **Date:** 2026-05-24 (confirmed with HC in Session 1)
- **Deciders:** HC

## Context

The cache server is the project's core artifact. HC is new to both Go and Rust. The project is
educational and already ambitious; adding "learn Rust" as a parallel goal is a real risk. See
`00-project-plan.md` §5.

## Decision

Implement the cache server in **Go**.

## Alternatives considered

- **Rust** — marginally better performance and strong recruiter signal (Cloudflare, Discord,
  Firecracker), but ~6–10 weeks to productive and the borrow checker on shared cache state is a
  genuine stuck-risk. Rejected for v1; fine as a later rewrite if HC wants it.

## Consequences

- Faster path to productive (~3–4 weeks), mature gRPC ecosystem, goroutines map cleanly to the
  concurrency we need, strong signal (Uber/Stripe/Microsoft/Amazon).
- The vLLM-side connector remains Python; the proto therefore generates two clients (see ADR 0010).
- Conventions and tooling (`gofmt`, `go vet`, `go test -race`) are enforced via the pre-commit hook.
