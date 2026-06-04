# Local observability stack (Phase 4, Sub-stage F)

Prometheus + Grafana that scrape a locally-running `cache-server` and render the
**KV Cache — Overview** dashboard from the metrics added in Sub-stage E (ADR 0025).

This is a **local development harness**. The production cluster ships the same dashboard on AWS
(the project deploys via Terraform from Phase 2 on, ADR 0006), but iterating on PromQL and panel
layout against a real running node is far faster on a laptop than through a `terraform apply`.

## Run it

```sh
# 1. Start a cache-server on the host (metrics are on :9090 by default).
#    Any bound is fine; these flags just make eviction panels do something.
go run ./cmd/cache-server -max-bytes 64000000 -ttl 5m

# 2. Bring up Prometheus + Grafana.
docker compose -f deploy/observability/docker-compose.yml up

# 3. Open the dashboards.
#    Grafana:    http://localhost:3000   (anonymous admin; "KV Cache — Overview")
#    Prometheus: http://localhost:9091   (targets page: Status -> Targets)

# 4. Drive traffic so the panels move.
go run ./cmd/loadgen
```

Tear down with `docker compose -f deploy/observability/docker-compose.yml down`.

## Why the ports look the way they do

The cache-server's own metrics endpoint is `:9090` on the host. Prometheus *also* defaults to
9090, so the container is mapped to host **9091** to avoid the collision. Inside the compose
network Prometheus still listens on 9090 and Grafana reaches it at `http://prometheus:9090`.

Prometheus scrapes the host via `host.docker.internal:9090`. On Docker Desktop that name resolves
automatically; the `extra_hosts: host-gateway` mapping in the compose file makes it work on Linux
too. For a **multi-node** local cluster, add each node's `host:metrics-port` to the `targets` list
in [`prometheus/prometheus.yml`](prometheus/prometheus.yml).

## Layout

```
docker-compose.yml                         Prometheus + Grafana services
prometheus/prometheus.yml                  5s scrape of the cache-server /metrics
grafana/provisioning/datasources/          auto-adds the Prometheus datasource (uid "prometheus")
grafana/provisioning/dashboards/           tells Grafana to load dashboards/*.json on boot
grafana/dashboards/kvcache-overview.json   the dashboard (10 panels)
```

## Panels → metrics (all `kvcache_*`, ADR 0025)

| Panel | Query source |
|-------|--------------|
| Cache hit rate (overall) | `cache_requests_total{result}` |
| Resident bytes / entries (per node) | `resident_bytes`, `resident_entries` gauges |
| Cache requests by op + result | `cache_requests_total{op,result}` |
| gRPC p99 latency by method | `grpc_request_duration_seconds_bucket` |
| gRPC latency p50/p95/p99 (all methods) | same histogram |
| gRPC request rate by method + code | `grpc_requests_total{method,code}` |
| Eviction rate by reason | `evictions_total{reason}` |
| Replication outcomes | `replication_total{result}` |
| Replication queue depth (lag proxy) | `replication_queue_depth` gauge |

Time-to-first-token (TTFT) is intentionally absent — it needs a GPU and lands in Phase 4.5.

## Editing the dashboard

The dashboard is provisioned read-from-disk but `allowUiUpdates: true`, so you can tweak panels
live in Grafana. To persist a change: **Dashboard settings → JSON Model → copy**, then paste over
`grafana/dashboards/kvcache-overview.json` (strip any `"id"` at the top level so it re-imports
cleanly). The `$__rate_interval` in every query auto-sizes the `rate()` window to the scrape
interval, so the JSON stays correct at any scrape rate.
