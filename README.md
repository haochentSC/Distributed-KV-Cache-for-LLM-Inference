# Distributed KV Cache for LLM Inference

A distributed, CPU-only cache that shares attention **KV tensors** across a multi-node LLM serving
cluster, so any GPU can reuse a prompt prefix another GPU already computed — cutting time-to-first-
token on shared-prefix workloads (system prompts, RAG context, few-shot, agent loops).

Built in **Go** (consistent-hashing sharding, primary-replica replication, etcd-coordinated
failover, a cost-aware + fair eviction policy), integrated with **vLLM** via a custom KV connector
(no fork), and deployed on **AWS via Terraform**. The headline differentiator is a multi-objective
eviction policy that trades cache *efficiency* against per-tenant *fairness* along a tunable curve.

> **Status: Phase 1 (single-node cache).** A local gRPC cache server and synthetic load generator
> are runnable without a GPU; cluster features (etcd, sharding, eviction policy) come in later
> phases. This is an **educational project** — built deliberately, to learn distributed systems,
> Go, LLM/vLLM internals, and cloud/IaC, not to ship fastest.

## Navigate

| If you want… | Read |
|---|---|
| The strategy, scope, phases, decisions (**source of truth**) | [`docs/00-project-plan.md`](docs/00-project-plan.md) |
| How this project is run (the working agreement) | [`docs/02-roadmap-and-workflow.md`](docs/02-roadmap-and-workflow.md) |
| Full-scope target architecture (all phases) | [`docs/01-architecture-overview.md`](docs/01-architecture-overview.md) |
| System design + diagrams (Phase-1 detail) | [`docs/01-architecture.md`](docs/01-architecture.md) |
| Distributed-systems concepts in Go | [`docs/03-distributed-systems-in-go.md`](docs/03-distributed-systems-in-go.md) |
| KV cache + vLLM integration | [`docs/04-kv-cache-and-vllm.md`](docs/04-kv-cache-and-vllm.md) |
| AWS + Terraform deployment | [`docs/05-cloud-deployment-aws.md`](docs/05-cloud-deployment-aws.md) |
| Why each significant decision was made | [`docs/adr/`](docs/adr/) |
| What was learned along the way | [`docs/learning-log.md`](docs/learning-log.md) |

## Quick start (local demo)

Two terminals from the repo root. Terminal 1 starts the **cache server** (Go, listens on
`:50051`). Terminal 2 runs **loadgen**, which mimics a vLLM connector: random tokens with a
shared hot prefix, then `Lookup` / `Fetch` / `Write` over gRPC and prints hit rate and latency.

```bash
# Terminal 1
go run ./cmd/cache-server

# Terminal 2 (defaults: 8 workers × 200 requests, 2 MiB per block — can take ~10–20s)
go run ./cmd/loadgen
```

Example report line to expect: `block hit rate: ~60–80%` with `0 errors` once the hot prefix
warms. Lighter run on a laptop:

```bash
go run ./cmd/loadgen -payload-bytes 65536 -requests 50 -concurrency 4
```

If the server fails with `bind: ... port ... already permitted`, something is already on
`:50051` — use the existing server and only run loadgen, or stop that process and retry.

## Commands

Requires [Go 1.22+](https://go.dev/dl/) (see `go.mod`). On Windows PowerShell, run each command
from the repo root; use `;` instead of `&&` between commands if chaining.

### Run locally

| Command | What it does |
|---|---|
| `go run ./cmd/cache-server` | Start one in-memory cache shard over gRPC (`-addr` default `:50051`). |
| `go run ./cmd/loadgen` | Synthetic client traffic (shared-prefix pattern); prints throughput, block hit rate, p50/p95/p99. |
| `make run-server` | Same as `go run ./cmd/cache-server`. |
| `make run-loadgen` | Same as `go run ./cmd/loadgen`. |

Useful **loadgen** flags: `-addr localhost:50051`, `-payload-bytes` (KV size per block),
`-prefix-share` (fraction reusing the hot prefix), `-concurrency`, `-requests`. Server and
client addresses must match (`-addr` on both if you change the port).

### Build and quality gates

| Command | What it does |
|---|---|
| `go build ./...` | Compile all packages and binaries. |
| `go test ./... -race` | Unit tests with the race detector (required before merge). |
| `go vet ./...` | Static analysis. |
| `gofmt -l .` | List files that need formatting (should print nothing). |
| `make build` / `make test` / `make vet` / `make fmt` | Makefile wrappers for the above. |

Pre-commit (optional): `git config core.hooksPath .githooks` then commits run fmt/vet/test.

### Protobuf / codegen

| Command | What it does |
|---|---|
| `make tools` | Install `protoc-gen-go` and `protoc-gen-go-grpc` plugins. |
| `make proto` | Regenerate `gen/kvcache/v1/` from `proto/kvcache/v1/kvcache.proto` (needs `protoc` on PATH). |
| `make tidy` | `go mod tidy` — sync module dependencies. |

## Roadmap (high level)

Phase 0 Foundation → 1 Single-node external cache → 2 Two-node distributed cache on AWS →
3 Replication & failover → 4 Eviction, observability, chaos *(core ships gate)* → 4.5 GPU TTFT
benchmark → 5 Cost-aware + fairness eviction policy *(the differentiator)* → 6 Polish & write-up.
Full detail in [`docs/00-project-plan.md` §4](docs/00-project-plan.md).

<!-- The Phase 6 README will add: a quickstart (terraform apply), architecture diagram,
     "prior art and how this differs" (vs LMCache / NVIDIA Dynamo / Mooncake), measured TTFT and
     throughput numbers, and a demo. -->

## License

See [`LICENSE`](LICENSE).
