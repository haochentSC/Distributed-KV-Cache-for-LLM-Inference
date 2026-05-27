# ADR 0018 — Static ring membership in Phase 2; etcd deferred to Phase 3

- **Status:** accepted
- **Date:** 2026-05-25 (Phase 2 start)
- **Deciders:** HC (+ Claude)

## Context

`00-project-plan.md` §4 (Phase 2) and `01-architecture-overview.md` §9 tag **etcd** as a Phase 2
item: stand up a 3-node etcd, publish the consistent-hash ring to it, and have clients watch it.
But Phase 2's actual goal is narrower — prove **sharding + client-side routing** works across a
small, fixed set of nodes. A 2–3 node cluster with no membership churn does not yet need
linearizable coordination: every client can build an **identical** ring from the same static member
list (the ring is deterministic given the member set — see `ring.go`'s `vnodeHash` note that warns
against process-random seeds). etcd's leases/watches only start earning their keep in Phase 3, where
**failover and leader election** introduce real, dynamic membership changes that must be agreed
linearizably (ADR 0013 consistency boundary, ADR 0009 etcd topology).

This honors the plan's "ship the minimum end-to-end first" guardrail (§6 Risk 3): bolting on etcd
before sharding is proven adds learning surface (watches, leases, quorum) ahead of the thing it's
supposed to coordinate.

## Decision

In **Phase 2, ring membership is static** — the member list is passed by flag/config to every client
(the Go load generator now; the Python connector when it resumes), and each builds an identical ring
locally. **etcd is introduced in Phase 3**, where it becomes the linearizable source of truth for
membership, ownership, and leader leases (ADR 0009), replacing the static list.

## Alternatives considered

- **etcd in Phase 2 (as originally tagged)** — matches the architecture doc's phasing, but front-loads
  consensus machinery before sharding/routing is demonstrated; more to debug, no failover yet to
  justify it.
- **Static membership forever** — rejected: it cannot survive a node dying or a rebalance, which is
  exactly the Phase 3 story. Static membership is explicitly a Phase 2-only simplification.

## Consequences

- Phase 2 stays focused: ring + routing + degrade-to-miss, proven locally then on AWS, with no etcd
  dependency on the critical path.
- The routing layer must take its member list from config in a way that's **cleanly replaceable** by
  an etcd watch in Phase 3 (keep the "where does the member set come from" seam small and explicit).
- A node added/removed in Phase 2 means re-running clients with a new list (acceptable — no churn is
  expected at this stage). Dynamic membership is Phase 3 work.
- Revisit immediately at Phase 3 start: the static list is replaced by `/kvcache/members/*` +
  `/kvcache/ring/*` in etcd (illustrative schema in `01-architecture-overview.md` §9).
