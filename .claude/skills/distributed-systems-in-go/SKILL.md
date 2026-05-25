---
name: distributed-systems-in-go
description: Implementation playbook for the distributed-systems parts of the KV cache in Go — sharding, consistent hashing, replication, etcd-based leader election, failover/graceful-drain, consistency, backpressure, and chaos testing. Use when designing or implementing any of these (mainly Phases 2–4), or when teaching HC the concept behind them.
---

# Distributed Systems in Go — playbook

This skill holds the deep teaching content kept out of the always-on context. It pairs with
`docs/03-distributed-systems-in-go.md` (the index) and the source-of-truth plan
`docs/00-project-plan.md`.

**Remember the working agreement:** these topics are mostly **[guided]** — Teach → Design →
Skeleton (stubs + TODOs, no filled logic) → HC implements → Review → Capture. Don't write the
core logic for HC.

## When to use
- Designing/implementing sharding or the consistent-hashing ring (Phase 2)
- Replication, replica promotion, failover, graceful drain (Phase 3)
- etcd leases / leader election / membership (Phase 3)
- Consistency model decisions; backpressure / admission; chaos testing (Phase 4)

## How to teach each topic (the loop)
1. State the *problem in this system* (e.g. "two requests for the same prefix must hit the same
   shard even as nodes join/leave") before any Go.
2. Lay out 2–3 design options with tradeoffs; pick together; record an ADR.
3. Hand HC interfaces + struct stubs + `TODO`s, not the algorithm body.
4. Review HC's implementation for the classic bugs (see `pitfalls.md`); ask an understanding-check
   question.

## Topic map (expand into sibling files as we build)
- `consistent-hashing.md` — ring, virtual nodes, why mod-N rehashing is wrong, key→shard lookup
- `replication.md` — RF=2 primary→replica async, version vectors, what "acked" means
- `etcd-coordination.md` — leases, watches, leader election, membership; why not hand-roll Raft
- `failover.md` — promotion, split-brain avoidance, Spot-interrupt-triggered drain
- `consistency.md` — eventual cache reads vs linearizable metadata, and why the split is safe
- `pitfalls.md` — Go concurrency traps (goroutine leaks, map races, ctx cancellation)
- `chaos.md` — `tc`/`iptables` fault injection on raw EC2; asserting zero correctness violations

> Only `pitfalls.md` is seeded now; create the others on demand when the matching phase starts so
> the content reflects what we actually build.
