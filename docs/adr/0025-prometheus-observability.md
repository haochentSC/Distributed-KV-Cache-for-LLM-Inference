# ADR 0025 — Prometheus instrumentation behind a metrics-free core

- **Status:** accepted
- **Date:** 2026-06-04 (Phase 4, Sub-stage E)
- **Deciders:** HC (+ Claude)

## Context

Phase 4's deliverable is a benchmark report under normal and failure conditions, which is only
possible once the node emits the Section-3 metrics: cache hit rate, Lookup/Fetch latency
percentiles, eviction rate by reason, per-shard memory, and replication health. The plan calls
for Prometheus (scrape) + Grafana (dashboards) and, later, CloudWatch (the cloud-native half).
This ADR covers the instrumentation layer — the metrics themselves and how they attach to a
codebase that has so far kept `internal/cache` free of any transport or infra dependency (the
`EvictionPolicy` seam, the `replicaEnqueuer` interface).

## Decision

**1. One `internal/metrics` package is the only place that imports Prometheus.** It owns a
`*Metrics` value holding every collector. The core stays clean: the `Evictor` takes an injected
`onEvict func(reason string, n int)` callback (a plain func, not an interface — zero Prometheus
in `internal/cache`); the gRPC layer talks to metrics through a narrow `metricsSink` interface it
defines itself (`Hit/Miss/Eviction/Replica`), defaulting to a `noopMetrics` so handlers call it
unconditionally. `main` constructs the one `*Metrics` and wires it to all three (evictor callback,
server sink, replicator sink). `*metrics.Metrics` happens to satisfy both the callback and the
interface — `m.Eviction` feeds the evictor *and* the manual Evict RPC.

**2. A private registry, not the global default.** Collectors register into a
`prometheus.NewRegistry()` owned by the `*Metrics`, not promauto's process-global one. The global
registry is shared mutable state: a second `New()` (or a unit test) registering the same metric
name panics with "duplicate collector registration". A private registry makes `*Metrics` an
ordinary value — constructible in prod and in each test with no global side effects — and the
white-box tests read individual series with `testutil.ToFloat64`.

**3. Label cardinality is budgeted to bounded sets only.** Every distinct label-value combo is a
separate in-memory time series, so labels are restricted to small fixed sets: gRPC `method` (5),
status `code` (~17), `op` (lookup|fetch), `result` (hit|miss), eviction `reason`
(pressure|ttl|manual), replication `result` (forwarded|dropped|error). High-cardinality
identifiers — `model_id`, `tenant_id`, `block_hash` — are **deliberately never labels**: they
would let traffic mint unbounded series and OOM the scrape target, the very failure Phase 4
exists to prevent. (Per-prefix-length / per-model breakdowns from Section 3 are deferred to
bucketed histograms rather than raw labels.)

**4. Latency + per-code counts come from one pair of gRPC interceptors**, not per-handler code.
`UnaryInterceptor` (Lookup/Evict/Health) and `StreamInterceptor` (Fetch/Write/Replicate) time the
handler and record `(method, code)` once, centrally — no handler has to remember to instrument
itself, and `status.Code(nil) == OK` means successes are counted too.

**5. Gauges are polled, events are pushed.** Counters (`Hit`, `Eviction`, `Replica`) fire inline
at the event. Levels — `resident_bytes`, `resident_entries`, `replication_queue_depth` — are
sampled by a 5 s poller in `main` reading `Store.Bytes/Len` and `Replicator.QueueDepth`. Polling
keeps the hot write path metrics-free, and a gauge is a point-in-time level anyway (Prometheus
only reads it at scrape).

**6. `/metrics` is a separate HTTP server** (`-metrics-addr`, default `:9090`). The gRPC port
speaks HTTP/2 framing a Prometheus scraper can't share. The endpoint is non-fatal (a failed
metrics listener logs, never kills the cache) and can be disabled with an empty flag.

**7. "Replication lag" is approximated by queue depth + outcome counts.** True wire-time lag
(primary-write → replica-apply) would need timestamps on the Replicate header; deferred. For the
async-drop replicator, a rising `replication_queue_depth` plus the dropped/error split is the
meaningful health signal and costs nothing.

## Why not these alternatives

- **`internal/cache` imports Prometheus directly.** Simpler by a few lines, but it puts an infra
  dependency in the unit-testable core and breaks the seam discipline the rest of the codebase
  holds. The injected callback keeps the core a pure library.
- **promauto + the global registry.** Convenient until the second constructor or test panics on
  duplicate registration; the private registry is the testable choice.
- **A fat single metrics interface shared everywhere.** Violated "small interfaces"; instead the
  consumer (server package) owns a 4-method `metricsSink`, and `*Metrics` is a concrete superset.
- **Instrument each handler for latency.** Repetitive and easy to forget on a new RPC; the
  interceptor pair covers every current and future method for free.

## Consequences

- A scraped node now exposes hit rate, p50/95/99 latency (per method), eviction rate by reason,
  resident size, and replication health — enough for the Phase 4 Grafana dashboards (Sub-stage F)
  and the benchmark report. TTFT stays out (GPU path, Phase 4.5).
- The histogram **buckets** (`DefBuckets`, 5 ms…10 s) are the one tuning point: percentiles are
  only resolvable to a bucket edge, so if the real tail clusters off-grid, refine buckets there.
- `prometheus/client_golang` moves from an indirect (etcd) dep to a direct one in `go.mod`.
- CloudWatch logs + alarms (the cloud-native half, Sub-stage G) and the dashboards (F) build on
  these metrics but are separate changes.
