# 03 — Distributed Systems in Go

> **Status: skeleton / index.** The deep teaching content (patterns, code idioms, worked
> examples) lives in the **`distributed-systems-in-go` Skill** so it loads on demand and doesn't
> spend context every turn. This doc is the index + the project-specific decisions; the skill is
> the playbook.

## What we'll implement, and where the teaching lives

| Concept | Phase | Project-specific decision | Deep dive |
|---|---|---|---|
| Sharding | 2 | shard = slice of prefix-hash space | skill: `sharding.md` |
| Consistent hashing | 2 | **built** (`internal/ring`): prefix-affinity on `block_hash[0]` (ADR 0014); 128 virtual nodes; sha256 placement | skill: `consistent-hashing.md` |
| Replication | 3 | RF=2, primary→replica async | skill: `replication.md` |
| Leader election / coordination | 3 | etcd leases (don't hand-roll Raft — ADR 0002) | skill: `etcd-coordination.md` |
| Failover & graceful drain | 3 | replica promotion; Spot-interrupt-triggered drain | skill: `failover.md` |
| Consistency | 3 | eventual for cache reads, linearizable metadata | skill: `consistency.md` |
| Backpressure | 4 | memory-pressure eviction + admission control | skill: `backpressure.md` |
| Chaos testing | 4 | `tc`/`iptables` on raw EC2; zero correctness violations | skill: `chaos.md` |

## Go idioms we lean on

<!-- TODO (Phase 1+): fill as we hit them — goroutines/channels for fan-out, context cancellation,
     sharded mutex maps vs sync.Map, errgroup, table-driven tests, the race detector. Keep
     examples short; the skill holds the long ones. -->

- **Consistent-hash ring** (`internal/ring`, Phase 2): `sort.Slice` to keep the circle sorted,
  `sort.Search` for the clockwise lookup + wrap, in-place slice filtering on `Remove`, and
  deterministic vnode placement via `sha256` — **not** `maphash` (its seed is process-random, so
  rings would disagree across clients). Lesson: `fnv-1a` clustered the vnodes badly (one node owned
  ~41% over 4 nodes / 128 vnodes); the statistical distribution test caught it.
- **Striped mutex map** (`internal/cache`): per-stripe `RWMutex` + atomic access counters under the
  read lock; immutable published `Entry` so `Fetch` streams without holding a lock.

## Background reading

See [`00-project-plan.md` §8](./00-project-plan.md): the Raft paper (to understand what etcd
does), DDIA chapters 5/6/7/9, MIT 6.824 lectures 1–8.
