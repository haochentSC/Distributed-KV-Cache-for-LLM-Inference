# ADR 0023 — Graceful drain wired to Spot interruption

- **Status:** accepted
- **Date:** 2026-05-28 (Phase 3, Sub-stage D)
- **Deciders:** HC (+ Claude)

## Context

Cache nodes run on EC2 Spot (ADR 0006) for cost. AWS gives a ~2 minute interruption notice
via the instance-metadata service before reclaiming a Spot instance. Without wiring this
notice into a planned drain, every reclaim looks exactly like a crash — the node disappears,
its membership lease takes up to TTL seconds to expire (ADR 0020), and during that window
clients keep routing requests to a soon-to-be-gone primary. We want the planned case to be
*faster* and *cleaner* than the crash case.

## Decision

**One drain path, two triggers.** SIGTERM and the Spot interruption notice both push onto
the same `stop` channel, so the shutdown sequence is identical:

1. **Deregister first.** Call the lease's `release()` (ADR 0020) — etcd revokes the lease
   *immediately*, so the membership key disappears in milliseconds rather than TTL seconds.
   Every router sees the new snapshot via the existing watch and stops routing to us.
2. **Then drain in-flight RPCs.** `grpc.Server.GracefulStop()` lets accepted RPCs finish
   while rejecting new ones. By this point new traffic shouldn't arrive anyway — step 1
   already removed us from the ring.

Order matters: dropping out of the ring *before* draining is what makes a Spot reclaim
silent. If we drained first, the lease would still be live, clients would still route to us,
and they'd hit refusals. (The crash path skips step 1 entirely — that's the gap the lease
TTL exists to close.)

**Spot listener: own package, IMDSv2 poll, off-EC2 = no-op.** `internal/spot` polls
`http://169.254.169.254/latest/meta-data/spot/instance-action` every 5 s. A 200 means the
notice fired; the watcher then calls a callback that sends `SIGTERM` to the same `stop`
channel `signal.Notify` uses, so the shutdown sequence has one implementation and one set
of bugs. Off EC2 the link-local IMDS is unreachable; the watcher logs once and continues
polling, costing nothing.

## Why not separate code paths

A "Spot-aware shutdown" parallel to the SIGTERM shutdown is two implementations to keep in
sync. Splitting them invites subtle drift (what if Spot drains but doesn't revoke the
lease?). Funnelling Spot through the existing signal channel reuses the tested path.

## Why poll IMDS instead of using AWS SDK / events

The interruption notice is plain HTTP on a link-local address; pulling in the AWS SDK
(credentials, region config, retries, ~MB of dependencies) for one URL is overkill. The
SDK would also need IAM to do anything useful, and IMDS doesn't. The package stays
zero-dependency Go stdlib — easy to test, easy to run anywhere.

## Why 5 s poll interval

The notice window is 120 s. At 5 s we get the notice within 5 s of issuance, leaving
≥115 s for drain — enough for a generous `GracefulStop` deadline even with multi-MB
in-flight Fetches. Faster polling burns CPU/network for no measurable improvement.

## Consequences

- The `-spot` flag is opt-in (defaults to false) so local/dev runs aren't polling IMDS for
  no reason. In Terraform-deployed AMIs, the systemd unit sets `-spot`.
- The IAM role on cache nodes does **not** need any new permissions — IMDS is unauthenticated
  beyond the IMDSv2 token (which is also IAM-free).
- Crash path (kernel panic, OOM, instance termination without notice) still relies on the
  lease TTL — the worst case is unchanged from ADR 0020. Spot is the *common* case and now
  beats the lease TTL by ~10x in latency.
- Sub-stage D closes Phase 3. Combined with B (replication) and C (implicit promotion),
  the cluster now survives planned and unplanned node loss at correctness, with planned loss
  fast enough to be invisible in the latency tail.
