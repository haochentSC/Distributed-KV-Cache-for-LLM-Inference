# Distributed KV Cache for LLM Inference — Project Plan

> **Purpose of this document.** This is a self-contained briefing for the project owner (HC) *and* for any future Claude session that needs to pick up planning, scoping, debugging help, or interview prep work on this project. Future sessions should treat this document as ground truth for project context, scope, and decisions already made.

---

## Section 1 — Context for Future Sessions

### Who this is for

HC is a CS Master's student at UIUC (MCS program, Jan 2026 – present), with an undergrad CS degree from USC. Previous internship at Stringer News (Flask/SendGrid/PostgreSQL/Docker). Current portfolio includes a live SaaS (AgentVeris — FastAPI/Celery/PG/Redis/Next.js), an Android app (BeachApp), and an ML research reproduction (World Models — PyTorch/Gymnasium). HC is comfortable with Python, Flask/FastAPI, PostgreSQL, Redis, Docker, basic ML/PyTorch. HC has **not** previously worked in Go or Rust, and has **not** previously worked on transformer internals or LLM inference systems (the World Models project used VAE + PPO, not attention/transformers).

### Why this project was chosen

From a comprehensive analysis of preferred qualifications across ~20 SWE postings at 11 big tech companies (FAANG + AI labs + high-bar firms — Google, Meta, Amazon, Apple, Microsoft, Stripe, Databricks, Uber, DoorDash, Anthropic, OpenAI), the biggest gap in HC's portfolio is **distributed systems**, which appeared as a preferred qualification at 6 of 11 surveyed companies. The second-largest gap is **GenAI infrastructure**, which Amazon's 2026 SDE-I posting calls out explicitly and which Anthropic and OpenAI weight heavily.

A plain Raft KV store was considered but rejected — the use case is contrived ("I rebuilt etcd but slower") and the story doesn't differentiate against the many other candidates who do MIT 6.824. The LLM KV cache project hits the same distributed-systems signal *plus* the GenAI infrastructure signal *plus* solves a real, current production pain point in LLM serving.

### What's already been decided

- **Project chosen:** Distributed KV cache for LLM inference, integrated with vLLM.
- **Target timeline:** 15–20 weeks part-time (alongside MCS coursework). *(Revised up from 14–18 to absorb the cloud/IaC work added below.)*
- **Primary purpose:** Resume artifact + interview anchor story for big tech / AI lab SWE roles.
- **Secondary purpose:** Real learning of distributed systems, LLM inference internals, **and cloud-native infrastructure (AWS + Terraform)**.
- **Companies this targets:** Google, Amazon, Databricks, Microsoft, Uber (distributed systems signal); Anthropic, OpenAI, Together AI, Fireworks, Anyscale, Perplexity (GenAI infra signal); **broad cloud-native SWE market** (the AWS/Terraform/observability signal — see the cloud analysis in the SWE projects doc).
- **Deployment substrate:** **AWS, provisioned via Terraform.** The cache cluster runs on real cloud from Phase 2 onward, not local Docker Compose. Rationale and the CPU/GPU decoupling that makes this cheap are in the new Section 3 "Cloud deployment topology" and Section 5.

### What's NOT decided yet (open questions for future sessions to resolve with HC)

- Whether to migrate the cluster from **EC2 + Docker to EKS** as a post-v1 stretch (see Section 5 — EC2 is the deliberate v1 choice because raw instances make node-kill/partition chaos testing clean; EKS is an optional later bullet)
- v1 etcd topology: **3-node (recommended)** vs single-node cheap fallback (resolved direction this session — see Section 3 cloud topology; final call at Phase 2 start)

### What's now decided (Go + etcd confirmed in Session 1, 2026-05-24)

- **Cache server language: Go.** Confirmed with HC (was "lean Go"). Lower learning curve, mature gRPC, strong recruiter signal. See Section 5 and ADR 0001.
- **Metadata layer: etcd, not a hand-rolled consensus.** Confirmed with HC. Saves 4–6 weeks; etcd is itself Raft so consensus is still interview-defensible. See Section 5 and ADR 0002.

### What's now decided re: infrastructure (resolved this session)

- **Cloud provider: AWS.** Most posting mentions, Amazon's explicit "preferably AWS," largest market share, concepts transfer to GCP/Azure.
- **GPU situation: resolved by decoupling compute by role.** The cache layer (the artifact) is CPU-only; only the vLLM workers need a GPU. So the distributed cache runs continuously on cheap CPU instances, and GPU spend is confined to short end-to-end benchmark windows. See Section 6, Risk 2. *(Refined 2026-05-24, Session 2: HC has a local **RTX 3080 (8 GB) + 32 GB RAM** dev laptop, so the Phase 0–1 vLLM/connector work runs locally with no Colab/rental — the cloud GPU is now only an optional convenience for the Phase 4.5 distributed headline benchmark. Note this does **not** change the cache design: it stores/ships opaque KV bytes and computes nothing, so it stays CPU-only regardless of GPU access — VRAM is the scarce tier the cache offloads *from*, not *to*.)*
- **vLLM integration: external service via a custom KV connector, not a fork.** *(Refined 2026-05-24, Session 1 — Revision A.)* vLLM exposes a first-class `KVConnectorBase_V1` extension point and supports **dynamic loading** of connectors, so our connector lives in our own package and references the Go cache over gRPC — no vLLM fork, no PagedAttention patching. Keeps the cache layer GPU-free and decoupled from vLLM internals. See Phase 1, Section 6 Risk 1, Section 8, and ADR 0008.

---

## Section 2 — The Problem in Plain English

Modern LLM serving works like this: a user sends a prompt, the model "prefills" by computing attention over every token in the prompt (this is the slow part — quadratic in prompt length), then generates output tokens one at a time. During prefill and generation, the model writes **K and V tensors** (key and value matrices from the attention mechanism) into a buffer called the **KV cache**. This cache lets the model avoid recomputing attention for tokens it has already seen.

Here's the critical observation: **many real-world LLM requests share prompt prefixes.** Examples:

- A chatbot with a system prompt — every conversation starts with the same 500 tokens
- A RAG application — the same context document is included in every query about that document
- Few-shot prompting — the same examples appear in every prompt
- Agent loops — long conversation history with mostly-stable earlier turns

When two requests share a prefix, you can **reuse** the KV cache tensors computed for the first request instead of recomputing them for the second. This is called **prefix caching**, and it can cut time-to-first-token by 30–70%.

vLLM already does this on a single node. But in a multi-GPU cluster, each GPU only knows its own cache. If the load balancer routes user B's request to a different GPU than user A's, user B has to recompute the shared prefix. **A distributed KV cache solves this** — all GPUs in the cluster can look up cached prefix KV tensors from a shared store, regardless of which GPU served the original request.

This is a real, active area of work in 2025–2026. vLLM has an open project called LMCache exploring exactly this. SGLang has RadixAttention. NVIDIA's TensorRT-LLM has its own approach. Anyone serving LLMs at scale has thought about this problem.

### Why this is interesting from a distributed-systems perspective

Building a distributed KV cache touches almost every classical distributed systems concept:

- **Sharding** — KV entries are too big to fit on one node, so they must be partitioned
- **Consistent hashing** — choose how to map prefixes to shards stably as the cluster scales
- **Replication** — a cache node failure should not lose data (or at least should degrade gracefully)
- **Consistency** — cache reads can be eventually consistent, but metadata (which shard owns which prefix) cannot
- **Failure handling** — node crashes, network partitions, slow nodes
- **Backpressure** — what happens when the cache is full or under heavy write load
- **Observability** — hit/miss rates, latency, eviction stats — all critical
- **Coordination** — how do nodes agree on cluster membership, shard ownership?

It's also interesting because the workload has unusual properties: **writes are large** (each cache entry is megabytes of tensor data), **reads are bursty** (a popular prefix gets hit thousands of times), and **latency matters** (the whole point is to be faster than recomputing, so cache lookup must be sub-10ms).

---

## Section 3 — System Architecture

### High-level diagram

```
                    ┌─────────────────────┐
                    │   Client / API GW   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │  Inference Router   │  ← routes by load + cache locality
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
        ┌──────────┐     ┌──────────┐     ┌──────────┐
        │  vLLM    │     │  vLLM    │     │  vLLM    │
        │  GPU 0   │     │  GPU 1   │     │  GPU 2   │
        └─────┬────┘     └─────┬────┘     └─────┬────┘
              │                │                │
              └────────────────┼────────────────┘
                               │  gRPC: lookup, fetch, write
                               ▼
        ┌─────────────────────────────────────────┐
        │       Distributed KV Cache Layer        │
        │                                         │
        │   ┌────────┐  ┌────────┐  ┌────────┐    │
        │   │Shard 0 │  │Shard 1 │  │Shard 2 │    │
        │   │ (P+R)  │  │ (P+R)  │  │ (P+R)  │    │
        │   └────────┘  └────────┘  └────────┘    │
        └─────────────────────┬───────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  etcd / metadata │  ← shard ownership, health, config
                    └──────────────────┘

                    ┌──────────────────┐
                    │  Prometheus +    │  ← hit/miss rates, latency, eviction
                    │  Grafana         │
                    └──────────────────┘
```

P+R = Primary + Replica per shard for fault tolerance.

### Components

**1. Inference workers (vLLM)**

Existing software. You're not building these. Each runs on a GPU. Integrated via a **custom `KVConnectorBase_V1`** (our own package, loaded by vLLM's dynamic connector mechanism — no fork) that queries the distributed cache before a prefill and writes computed KV entries back. *(Revision A — see Section 6 Risk 1.)*

**2. Cache shards (the thing you're building)**

A horizontally-scaled service. Each shard owns a slice of the prefix-hash space. Stores KV tensors in memory (and possibly spills to NVMe for older entries). Exposes a gRPC API. *(Revision D: `prefix_hash` is an **opaque 32-byte value** in the proto — the SHA-256 is computed Python-side from token IDs; the Go server never tokenizes. One proto generates two clients: Go for the load generator, Python for the vLLM connector.)*

- `Lookup(prefix_hash) → {has_entry: bool, version: int}`
- `Fetch(prefix_hash, version) → KV tensors`
- `Write(prefix_hash, KV tensors, metadata)`
- `Evict(prefix_hash)`
- `Health()`

**3. Cache coordinator / metadata layer**

Tracks: which shards are alive, which shard owns which hash range, replication topology. **Strong recommendation: use etcd here, don't build your own consensus.** The resume win is the cache layer you built on top, not the consensus library. Etcd is itself a Raft-based system, so you can talk about consensus knowledgeably without re-implementing it. Future sessions: if HC pushes back on this and wants to implement Raft, that's an additional 4–6 weeks and pushes the project to 20+ weeks total.

**4. Inference router**

Lightweight — sits in front of vLLM workers. Routes requests based on (a) which worker is least loaded, and (b) cache locality (if a worker already has the relevant prefix in local GPU memory, prefer it). For v1, locality-aware routing can be skipped; round-robin is fine.

**5. Observability stack**

Prometheus for metrics scraping, Grafana for dashboards. Required metrics:

- Cache hit rate (overall, per shard, per prefix length bucket)
- Lookup latency p50/p95/p99
- Fetch latency p50/p95/p99
- Eviction rate, eviction reason (TTL, LRU, memory pressure)
- Time-to-first-token end-to-end (with and without cache hit)
- Per-shard memory usage
- Replication lag

**6. Synthetic load generator (build this early — it de-risks everything)**

A standalone client that emits `Write`/`Lookup`/`Fetch` traffic against the cache with realistic-sized payloads (~2 MB per simulated token by default, with a **configurable per-token payload size** so soak tests run cheap on small instances — Revision E; configurable prefix-sharing distribution) **without needing a GPU or vLLM at all.** This matters more than it looks: every distributed property of the cache — consistent hashing, replication, failover, eviction, throughput, latency under load, chaos behavior — is fully exercisable with synthetic blobs. Only the single end-to-end "real TTFT reduction" headline number requires actual vLLM + GPU. So the bulk of the project (and the bulk of the interview story) is built and benchmarked on cheap CPU nodes, decoupled from the highest-risk component (vLLM internals). Build it in Phase 1 and lean on it through Phase 4.

### Cloud deployment topology (AWS)

The cluster runs on AWS from Phase 2 onward, provisioned entirely with Terraform. The decisive insight is **compute is split by role**:

- **Cache nodes (the artifact) — CPU only.** Each shard is RAM + network + (optionally) NVMe spill. Runs on general-purpose instances (`t3`/`c7g`-class). *(Revision E: size for RAM — a single 500-token prefix ≈ 1 GB at 2 MB/token, so a `t3.micro` (1 GB) is only adequate for the Phase 0 toolchain smoke test, not for holding a meaningful cache. Pick the instance by the working-set RAM you need; use the load generator's configurable payload size to keep continuous soak tests cheap.)* *All* the distributed-systems work, IaC, IAM, and chaos testing lives here.
- **etcd — on-demand (never Spot), 3-node recommended for v1.** *(Revision C: etcd is the coordination ground truth, so it must not run on interruptible Spot. A 3-node quorum is what makes the Phase 3 leader-election / split-brain failover story real; a single node is the cheap fallback if cost/time forces it. Final call at Phase 2 start.)*
- **vLLM workers — GPU, benchmark windows only.** Only needed to produce the end-to-end TTFT number. Brought up in the *same VPC / same AZ* as the cache (so lookups stay sub-10ms), run for a few hours, then torn down. Confining GPU to benchmark sessions is what keeps the whole project affordable.

AWS service map (each maps to a defensible interview answer, not a keyword):

| Concern | AWS service | Note |
|---|---|---|
| Compute | EC2 (cache + benchmark GPU workers) | EC2 not EKS for v1 — see Section 5 |
| Networking | VPC, subnets, security groups, placement group | Node-to-node gRPC; same-AZ for low-latency lookups |
| Identity | IAM roles + instance profiles | No static credentials anywhere |
| Object storage | S3 | Cold-tier KV spill, benchmark artifacts, **Terraform remote state** (+ DynamoDB state lock) |
| Image registry | ECR | Cache server container image |
| Observability | CloudWatch (logs + alarms) | Runs *alongside* Prometheus/Grafana — claim both open-source and cloud-native observability |
| Infra-as-code | Terraform | Whole cluster as code; also makes chaos runs reproducible via `apply`/`destroy` |
| Cost + chaos | EC2 Spot | Spot reclamation = free, realistic node-failure events with a ~2-min drain warning |

### Key data structures

**Prefix hash key.** Cache entries are keyed by the *tokenized* prefix hash, not the raw text. Two prompts that differ in whitespace but tokenize identically should share cache. Use a rolling hash (e.g., SHA-256 over the token ID sequence) computed incrementally per token, so you can look up the longest cached prefix in one query rather than checking every prefix length.

**Cache entry.** Stored as:
```
{
  prefix_hash: bytes (32 bytes for SHA-256),
  token_ids: list[int],  // for verification on hit
  kv_tensors: bytes,     // serialized K and V for all layers, all heads
  model_id: str,         // KV cache is model-specific
  created_at: timestamp,
  last_accessed: timestamp,
  size_bytes: int,
  ref_count: int,
  tenant_id: str,        // for multi-tenant fairness (see Section 3.5)
  recompute_cost: float, // est. prefill cost to regenerate; drives cost-aware eviction (Section 3.5)
  access_count: int      // frequency signal for the GDSF-style policy (Section 3.5)
}
```

KV tensor size depends on the model. For Llama-3-8B with bfloat16: roughly 64KB per token per layer, 32 layers → 2MB per token. For a 500-token system prompt, that's 1GB. **This is why caching matters so much** — and why memory is the dominant constraint.

---

## Section 3.5 — The Differentiator: Multi-Objective Cache Policy (Cost-Aware + Fair)

> **Why this section exists.** The distributed KV cache space is crowded with strong open-source projects (LMCache, NVIDIA Dynamo, Mooncake). For a *resume/interview* artifact, the goal is not to out-engineer NVIDIA — it's to show one non-obvious engineering insight, fully built and benchmarked, that demonstrates judgment the big perf-focused projects underinvest in. This section is that differentiator. It is the project's headline intellectual contribution. **It is built only after the core v1 (Phases 1–4) ships and benchmarks clean — it is a layered addition, not a Phase 1 concern.**
>
> **Honest framing (Revision B, 2026-05-24).** vLLM + LMCache already offload KV to CPU/disk/remote, and **cross-engine/cross-node KV reuse is an active vLLM RFC (#14724)** — so "build an external KV store and integrate it" is *not* the novel part anymore, and the project should not claim it as such. The defensible contribution is twofold and should be stated that way in the README and in interviews: (1) the **distributed layer** — consistent-hashing sharding, replication, and etcd-coordinated failover, built and chaos-tested end to end; and (2) **this section's multi-objective cost-aware + fairness eviction policy**, which the incumbents underinvest in. The eviction policy is the headline; the integration is table stakes.

### The core insight: caching is a multi-objective optimization, and the objectives fight

Almost every cache (including the incumbents' default policies) evicts on **recency/frequency** (LRU/LFU). Two better objectives exist, and the interesting part is that they conflict:

1. **Cost-awareness.** A cached KV entry's *value* is not how recently it was used — it is `recompute_cost × reuse_probability`. A long, expensive-to-prefill prefix accessed occasionally can be worth more to keep than a short, cheap one accessed slightly more often, because missing the expensive one costs more GPU. This is **exactly the GreedyDual-Size-Frequency (GDSF)** idea from the web-caching literature, adapted to KV tensors (where "size" and "cost" are both first-class because tensors are large *and* expensive to regenerate).

2. **Fairness across tenants.** In any shared/multi-tenant serving cluster, a pure cost-aware policy is **globally greedy** — it keeps the highest-value entries regardless of *whose* they are. A tenant whose workloads are cheap-to-recompute gets systematically starved, because the optimizer always finds someone else's entry more valuable. This maps to **max-min fairness / Dominant Resource Fairness (DRF)** from multi-tenant resource scheduling.

**The tension is the whole point.** The globally cheapest (most cost-efficient) cache allocation can be grossly unfair; a perfectly fair allocation leaves global efficiency on the table. The engineering contribution is a single policy engine that maximizes total cache value *subject to* a per-tenant fairness guarantee, with a tunable knob between the two extremes. This is one policy problem in one subsystem (the admission/eviction controller), not two bolted-on features — which is precisely why combining them is coherent rather than scope creep.

### Design: one admission/eviction controller, two inputs

- **Value function (cost-aware, GDSF-style):** each entry carries an estimated `recompute_cost` (proportional to prefill FLOPs ≈ prefix token count, refined if you want by measured prefill time) and an `access_count`. The GDSF priority is roughly `priority = L + (access_count × recompute_cost) / size_bytes`, where `L` is an aging offset that prevents stale-but-once-valuable entries from pinning forever. Evict the lowest-priority entry under memory pressure.
- **Fairness constraint (DRF/max-min-style):** each tenant has a guaranteed minimum share of cache capacity. Above that floor, capacity is loaned out to whoever has the highest-value entries (work-conserving). Under contention, a tenant below its floor can reclaim capacity by evicting the *globally* lowest-value entry among tenants that are *above* their floor.
- **The knob:** a single parameter (call it `fairness_weight ∈ [0,1]`) interpolates between pure GDSF (0, max efficiency) and strict per-tenant max-min (1, max fairness). This knob is what you sweep to produce the benchmark curve below.

### Critical design decision: work-conserving, NOT static partitions

The trivial version — give each tenant a fixed slice and run cost-aware *within* the slice — is a trap. It is easy to build, but: (a) it wastes an idle tenant's reserved capacity (no work-conservation), and (b) **it removes the tension entirely**, so there is nothing interesting to defend in an interview. The version worth building is **elastic / work-conserving**: tenants borrow spare capacity freely but each has a guaranteed floor that is reclaimed fairly under contention. That is where cost-awareness and fairness genuinely interact, and where the engineering — and the story — lives.

### Scoping: static-first, then elastic (so the feature can't become a scope bomb)

This is the most ambitious part of the project for someone new to distributed systems, so it is deliberately split so a partial result is still shippable:

- **Feature v1 (must-ship):** per-tenant accounting + static quota + GDSF cost-aware eviction *within* each tenant's quota. Proves the accounting and the cost-aware policy. Demonstrates both ideas even if elasticity is cut for time.
- **Feature v2 (the real story, stretch):** make it work-conserving/elastic with reclaim-under-contention and the `fairness_weight` knob. This is the version that produces the tradeoff curve and the strong interview answer.

### What this costs (be honest about it)

Two concrete additions beyond the core:

1. **Per-tenant identity and accounting** in the cache data model (the `tenant_id` field, per-tenant usage counters, per-tenant capacity tracking). Bounded, but real.
2. **A multi-tenant workload in the load generator.** The single biggest under-estimated cost. To make starvation *visible* and the curve meaningful, the Section 3 synthetic load generator must be extended to emit traffic from several tenants with deliberately different profiles — e.g., Tenant A: many cheap/short prefixes; Tenant B: few expensive/long prefixes; Tenant C: bursty. Without this, there is no fairness result to show.

### The payoff: a tradeoff curve, not a single number

The benchmark artifact is unusually strong precisely because it visualizes a fundamental tension. Sweep `fairness_weight` from 0→1 and plot:

- **x-axis:** total recompute saved (or aggregate hit rate) — *efficiency*
- **y-axis:** per-tenant hit-rate variance, or the min tenant's hit rate (max-min) — *fairness*

The result is a Pareto-style frontier with a tunable operating point. "Here is the efficiency/fairness frontier of my cache, here is where I'd set the knob for a latency-SLA tenant vs. a batch tenant, and here is why" is a far stronger interview prop than "60% faster." It directly arms three of the four story beats (Section 7).

---

## Section 4 — Phased Implementation Plan

Total: **core v1 is 15–20 weeks** at ~15 hours/week (Phases 0–4, the shippable, fully-benchmarked distributed cache on AWS). The Section 3.5 differentiator (Phase 5) adds ~4 weeks, and polish (Phase 6) ~2–4, so the **full project with the differentiator lands around 21–26 weeks**. Treat Phase 4 as the hard "core ships" gate: if time runs out, a benchmarked core v1 without the differentiator is still a strong artifact. The differentiator is high-upside, not load-bearing. Adjust if HC's available hours differ.

### Phase 0 — Foundation (Weeks 1–2)

**Goal:** Understand vLLM well enough to integrate with it.

- Read the vLLM architecture docs end-to-end
- Read the PagedAttention paper (the algorithm vLLM uses for KV cache management)
- Run vLLM locally with a small model (TinyLlama-1.1B is good — fits comfortably in the local RTX 3080's 8 GB). **Environment note:** vLLM is NVIDIA/CUDA — use the discrete 3080, not the 5900HX's integrated Radeon (the AMD ROCm path is immature). As of 2026 vLLM runs both **natively on Windows 11** and under **WSL2**; WSL2 is the more battle-tested path and also gives a Linux toolchain that fixes the `go test -race` 32-bit-mingw blocker from the learning log — so one environment serves both the GPU work and Go race testing. Confirm the exact vLLM build/version + CUDA combo when setting up.
- Send some requests, watch the cache work, examine the existing prefix-caching behavior
- Read the LMCache project to understand the state of the art
- **Cloud onboarding (light, ~half a day):** create an AWS account, set a billing alarm, create an IAM admin user (not root), install the AWS CLI + Terraform, and `terraform apply` a throwaway `t3.micro` to confirm the toolchain works end to end. Don't build anything real yet — just clear the setup hurdle now so Phase 2 isn't blocked on it.
- **Deliverable:** A working local vLLM setup with a Jupyter notebook that demonstrates measured prefix-cache hits on a single node, **plus** a verified AWS + Terraform toolchain

### Phase 1 — Single-Node External Cache (Weeks 3–5)

**Goal:** Prove you can pull KV tensors out of vLLM, store them externally, and put them back.

- Set up a Go project for the cache server *(Go confirmed Session 1 — ADR 0001)*
- Define the gRPC service and protobuf schemas; generate **both** a Go and a Python client from the one proto, with `prefix_hash` as opaque bytes *(Revision D — ADR 0010)*
- Implement an in-memory single-shard cache (just a hash map for now)
- Uphold the **cache correctness invariant** — never serve KV that mismatches the requested
  `(block_hash, model_id, token_ids)`; misses/staleness are allowed — via hit-verification on
  `Fetch` *(ADR 0016)*
- **Integrate with vLLM via a custom `KVConnectorBase_V1`** *(Revision A — ADR 0008)*: implement the connector in our own package and load it through vLLM's dynamic-connector mechanism (no fork). On each request the scheduler-side hooks check the external cache for prefix matches; on a miss, the worker-side hooks write the computed KV back. Study `NixlConnector`, `OffloadingConnector`, and `LMCacheConnector` as references first.
- **Build the synthetic load generator** (Section 3, component 6): a client that drives the cache with realistic ~2 MB/token blobs (payload size configurable — Revision E) and a configurable prefix-sharing distribution, no GPU required. This becomes the primary test harness for Phases 2–4 and decouples all distributed-systems work from the vLLM/GPU critical path.
- Benchmark: TTFT with and without external cache, single node
- **Deliverable:** Single-node setup where external cache demonstrates measurable TTFT improvement on repeated prefixes, **plus a synthetic load generator that can saturate the cache without a GPU**

**Risks in this phase:**
- *(Lowered by Revision A.)* The `KVConnectorBase_V1` interface is the supported extension point, so we implement against a maintained API rather than patching PagedAttention. Residual risk: understanding the scheduler/worker method split and the KV layout the connector hands us — budget reading time, but this is no longer High/High (see Risk 1).
- Serialization overhead might wipe out the cache benefit. **Mitigation is now a decision + a gate
  (ADR 0015):** the KV payload moves as **opaque framed bytes** (protobuf for metadata only, never
  the tensor), and **`lookup + fetch ≪ recompute` is a Phase 1 exit gate** — measure it before
  declaring Phase 1 done, not under chaos in Phase 4.

### Phase 2 — Two-Node Distributed Cache (local-first, then AWS) (Weeks 6–9)

**Goal:** Add a second cache node and shard between them with a consistent-hash ring + client-side
routing — proven locally first, then deployed to AWS.

> **Revision F (2026-05-25, Session 3).** Three refinements ratified at Phase 2 start: **(1) Sequence
> — local-first.** Build and test the ring + routing across **local multi-process** cache nodes
> first, *then* `terraform apply` the same thing to AWS; this tightens the learning loop and makes
> AWS a deployment step, not a debugging surface. **(2) etcd deferred to Phase 3 (ADR 0018).** A fixed
> 2–3 node cluster has no membership churn, so every client builds an identical ring from a **static**
> member list; etcd's leases/watches only earn their keep at Phase 3 failover. **(3) Sharding
> granularity decided — prefix-affinity (ADR 0014 accepted),** keyed on `block_hash[0]`.

- **Local first:** run 2–3 `cache-server` processes; route between them with the ring (below) and the
  synthetic load generator. Prove sharding works before any cloud spend.
- Implement consistent hashing to route between the shards — **decided: prefix-affinity, keyed on a
  prefix root (`block_hash[0]`) (ADR 0014).** Affinity co-locates a prompt's blocks on one shard
  (one-RPC, sub-10 ms lookups) at the cost of hot-shard risk for viral prefixes; per-block hashing
  was rejected because it scatters a prefix across shards (fan-out latency). Phase 2 *measures* the
  hot-shard effect (load-gen per-shard distribution); hot-prefix replication is the deferred mitigation.
- Implement the shard-routing logic in the shared client (recall there are **two** generated clients — Go for the load generator, Python for the vLLM connector — both consuming the same routing rules; Revision D) so a caller can look up which shard owns a given prefix root. The member set comes from static config in Phase 2, behind a small seam an etcd watch replaces in Phase 3.
- Handle the case where a prefix should be on shard A but A is unreachable (return cache miss, log it)
- **Then to AWS:** write the cluster as Terraform — a VPC with subnets and security groups, two CPU
  cache instances (EC2 Spot), all in one AZ for low-latency gRPC. Push the cache image to ECR. Use an
  S3 bucket (+ DynamoDB lock) as the Terraform remote-state backend. **(etcd is *not* provisioned
  here — it lands in Phase 3 per ADR 0018.)**
- Drive the whole thing with the synthetic load generator — **no GPU needed for this phase**
- **Deliverable:** A 2–3 node distributed cache with consistent hashing — proven locally, then running
  on AWS via `terraform apply`; benchmark (synthetic load) shows the cache works across all nodes and
  reports per-shard distribution.

### Phase 3 — Replication and Failure Handling (Weeks 10–12)

**Goal:** Make the cache survive a node failure — on real cloud infrastructure.

- Add primary + replica per shard (replication factor 2)
- Implement async replication from primary to replica
- Implement replica promotion when primary fails
- Handle split-brain scenarios via etcd lease-based leader election
- Implement graceful shutdown (drain in-flight requests, transfer ownership) — **and wire it to AWS EC2 Spot interruption notices**, so a real ~2-minute spot reclamation triggers a clean drain. This gives you authentic, unscheduled failure events for free.
- Add IAM roles/instance profiles so nodes authenticate to S3/etcd with no static credentials; add an S3 cold tier for evicted entries (sets up Phase 4 tiering)
- **Deliverable:** Kill any single cache node (or let Spot reclaim it); system continues serving requests with degraded performance but no correctness violations

### Phase 4 — Eviction, Observability, Chaos (Weeks 13–15)

**Goal:** Make it production-shaped.

- Implement LRU eviction with TTL fallback — **this is the baseline the Section 3.5 policy will be measured against, so keep it cleanly swappable behind an eviction-policy interface**
- Implement memory pressure detection and proactive eviction
- Add Prometheus metrics for everything listed in Section 3
- Build Grafana dashboards; **also ship logs and a couple of alarms to CloudWatch** so the story covers both open-source and cloud-native observability
- Write a chaos test harness that randomly kills nodes, partitions the network (using `tc` or `iptables` on the raw EC2 instances), and adds latency
- Run benchmarks (synthetic load) under chaos: measure correctness violations (there should be zero), latency degradation, recovery time
- **Deliverable:** A benchmark report showing performance under normal conditions and under various failure modes, all on AWS. **This is the "core v1 ships" milestone — the gate that must be cleared before starting the Section 3.5 differentiator.**

### Phase 4.5 — End-to-End GPU Benchmark (Week 16)

**Goal:** Produce the one number that needs a real GPU — the headline TTFT reduction.

> **Local vs. cloud GPU (decided 2026-05-24, Session 2).** HC's local RTX 3080 can run this benchmark, but with a tradeoff. *Local:* zero GPU cost and fine for the **single-node** "external cache cuts TTFT" number — but the vLLM worker talks to a local cache, so it doesn't exercise the **distributed** cluster (the actual story). *Cloud:* GPU + the multi-node cache cluster in one VPC/AZ shows the real distributed headline. **Recommended:** prototype/iterate the benchmark locally on the 3080, then do one short cloud run against the live cluster for the resume number. A pragmatic cheaper alternative if cloud GPU is a hassle: local 3080 + a local multi-process cache cluster for a "good enough" distributed-ish number.

> **Status (2026-06-11): AWS distributed window EXECUTED** (`phase45-distributed-gpu.md`) — L4 cross-AZ
> **+10.9% @ 4k tokens** (resume headline). **RunPod Session A EXECUTED** (ADR 0034) — long-context
> curve to 32k + serving demo; no crossover on A100 (prefill too fast); 14B worse than 7B at same
> tokens. Details: `phase45-gpu-cloud.md`. **RunPod Session B PENDING** — TP=4 / Qwen2.5-32B on 4×
> A6000; handoff `runpod-gpu-window-plan.md`. Pre-flight commit `ff0feb3`; runbook
> `runpod-runbook.md`.
>
> Earlier: single-node done (ADR 0031); distributed prep (ADR 0032); AWS rescope to single-GPU
> (ADR 0033). TP=4 path validated on GPU cloud, not AWS quota.

- `terraform apply` one or two GPU instances (e.g., `g5.xlarge`, ideally Spot) into the *same VPC/AZ* as the running cache cluster, so lookups stay sub-10ms
- Point real vLLM workers at the distributed cache; run repeated-prefix workloads (system-prompt, RAG-context, few-shot patterns)
- Capture end-to-end TTFT with and without the distributed cache; this is the number that fills the `[measured]` in the resume bullet
- `terraform destroy` the GPU nodes immediately after — this is the only expensive window in the whole project
- **Deliverable:** A measured end-to-end TTFT improvement number from a real vLLM + GPU run, with a documented, reproducible benchmark setup

### Phase 5 — Differentiator: Multi-Objective Cache Policy (Weeks 17–20)

**Goal:** Build the headline differentiator from Section 3.5. Gated on core v1 (Phase 4) being shipped and benchmarked.

This phase is split so a partial result still ships (see Section 3.5 scoping):

> **Status (2026-06-07): 5a SHIPPED (local).** GDSF cost-aware value (`H = L + freq·cost/size`) +
> static per-tenant quotas behind the Phase 4 seam (ADR 0029); `-eviction gdsf` / `-tenant-quota`;
> loadgen `-multitenant` (3 tenants). Benchmarked LRU vs GDSF vs GDSF+quotas — the
> efficiency-vs-fairness tension is visible (GDSF: 21.5% aggregate but starves the cheap tenant to
> 3.1%; GDSF+quotas: cheap tenant 16.9%, min-tenant 3.1%→10.5%), 0 correctness violations
> (`docs/benchmarks/phase5a-eviction.md`). Re-running 5a on the AWS cluster is batched with the
> deferred Phase-4 AWS verification window.
>
> **Status (2026-06-07): 5b SHIPPED (local).** Elastic work-conserving floors + the
> `fairness_weight ∈ [0,1]` knob (ADR 0030); `-eviction gdsf-elastic -fairness-weight w`. Quotas
> become floors: `OverQuota()` returns false (watermark-only eviction = work-conserving), and
> victim selection uses `H_eff = H/(1 + w·overage)`. Swept (`scripts/phase5b-sweep.ps1`, 3-seed
> mean): the Pareto frontier is drawn — `w=0` = efficiency corner (20.0% overall / 1.9% min-tenant),
> `w≥0.25` = fairness plateau (~14% / ~12% min). **Elastic Pareto-dominates the 5a static caps on
> both axes** (w=0.25: 14.4%/12.3% vs static 12.2%/10.3%); the knob saturates fast (honest finding).
> 0 violations (`docs/benchmarks/phase5b-eviction.md`). **Phase 5 (the differentiator) is complete.**

**5a — Cost-aware + static fairness (must-ship, ~weeks 17–18):**
- Extend the cache data model with `tenant_id`, `recompute_cost`, `access_count` (already in the Section 3 entry schema)
- Implement the GDSF-style value function behind the eviction-policy interface from Phase 4
- Add per-tenant accounting and static per-tenant quotas; cost-aware eviction within each quota
- Extend the synthetic load generator to emit multi-tenant traffic (≥3 tenants with distinct profiles: cheap/frequent, expensive/rare, bursty)
- **Deliverable:** A working cost-aware policy with per-tenant isolation, benchmarked against the Phase 4 LRU baseline (show hit-rate / recompute-saved improvement)

**5b — Elastic work-conserving fairness + the knob (the real story, stretch, ~weeks 19–20):**
- Make quotas elastic: tenants borrow spare capacity; floors reclaimed fairly under contention by evicting the globally lowest-value entry among tenants above their floor
- Implement the `fairness_weight` knob interpolating GDSF ↔ max-min
- Sweep the knob; produce the efficiency-vs-fairness tradeoff curve (Section 3.5)
- **Deliverable:** The tradeoff-curve benchmark and a short write-up of where you'd set the operating point and why

> If time runs short, 5a alone is a legitimate, defensible result. Do not let 5b's ambition jeopardize a shipped 5a. And do **not** add a third differentiator — two objectives composing into one policy engine is the coherent sweet spot.

### Phase 6 — Polish and Story (Weeks 21–22, optional 23–24)

**Goal:** Convert this into a resume artifact and interview anchor.

- Write a README that explains the project, the architecture, and the results, with diagrams (include the cloud topology + a `terraform apply` quickstart)
- **Add a "Prior art and how this differs" section** to the README comparing the design to LMCache, NVIDIA Dynamo, and Mooncake — turns the crowded landscape from a liability into a credibility signal, and pre-loads the interview beats
- Record a 3-minute demo video showing the system handling failure
- Write a blog post (1500–2500 words) on a non-obvious thing you learned. Suggested topics:
  - "Why I chose async replication for an LLM KV cache" (consistency tradeoffs)
  - "What pulling KV tensors out of vLLM taught me about PagedAttention"
  - "Chaos-testing a distributed cache: what broke first and why"
  - "Running a distributed cache on AWS Spot: turning interruptions into a chaos-testing feature" (cloud + chaos angle in one post)
  - "The efficiency-vs-fairness frontier of a multi-tenant KV cache" (the Section 3.5 differentiator — likely the strongest post)
- Update resume bullets with the actual measured numbers
- Prepare interview talking points (see Section 7)
- **Deliverable:** Public GitHub repo with README, demo, blog post, and a portfolio entry

---

## Section 5 — Tech Stack Decisions

### Cache server language: Go (recommended)

| Criterion | Go | Rust |
|---|---|---|
| Learning curve | ~3–4 weeks to productive | ~6–10 weeks to productive |
| Performance | Excellent (sub-ms latency easy) | Marginally better |
| gRPC ecosystem | Mature, widely used | Mature, slightly more painful |
| Concurrency model | Goroutines, easy to reason about | Async/await + ownership, harder |
| Recruiter signal | Strong (Uber, Stripe, Microsoft, Amazon all use Go) | Strong (Cloudflare, Discord, AWS Firecracker) |
| Risk of getting stuck | Low | Higher (borrow checker on shared state is real) |

**Recommendation: Go for the first version.** The project is ambitious enough without adding "learn Rust" as a parallel goal. If HC already knows Rust, fine — but don't learn it for this project.

### Metadata store: etcd (recommended)

Strong recommendation. Building your own consensus adds 4–6 weeks and the marginal resume value is low (interviewers will know you used etcd, and that's fine — etcd is itself Raft, so you can speak knowledgeably about consensus). The interesting work is the cache layer on top.

If HC really wants the consensus learning, do it as a separate side project (a small Raft library in Go, ~3 weeks) before or after this project, not bundled in.

### Inference server: vLLM (only realistic choice)

vLLM is the de facto standard for open-source LLM serving and the one most companies use internally. Other options (TGI, MLC-LLM, TensorRT-LLM) are either less common or harder to modify.

### Model: TinyLlama-1.1B for dev, Llama-3-8B for benchmarking

TinyLlama fits comfortably in the local RTX 3080's **8 GB** — use it for dev and connector iteration. Llama-3-8B is what people actually deploy and gives more realistic benchmarks; at ~16 GB for fp16 weights it does **not** fit in 8 GB unquantized. On the 3080, Llama-3-8B is viable only **4-bit quantized** (~5 GB weights) with short sequences and small batches — tight, fine for spot checks, but not the realistic-benchmark config. For the headline Llama-3-8B numbers, use a larger cloud GPU (Phase 4.5) or UIUC A100s if accessible. *(Refined 2026-05-24, Session 2 for HC's 8 GB 3080.)*

### Transport: gRPC

Standard for inter-service RPC. Protobuf gives you schema versioning, generated clients in any language, and HTTP/2 multiplexing.

**Optimization for later:** If serialization overhead becomes the bottleneck (likely with multi-MB tensors), look at NCCL or RDMA for GPU-to-GPU transfer of KV tensors, bypassing the cache server's CPU entirely. This is a v2 feature.

### Observability: Prometheus + Grafana + OpenTelemetry, plus CloudWatch

Standard stack. OpenTelemetry for traces, Prometheus for metrics scraping, Grafana for dashboards. All free, all what companies actually use. On AWS, also ship logs and a couple of alarms to **CloudWatch** — running both lets you legitimately claim open-source *and* cloud-native observability, and the contrast (when you'd reach for which) is a good interview talking point.

### Storage backend per shard

In-memory hash map (Go `sync.Map` or a sharded mutex-protected map) for v1. Once you have working v1, consider:

- A B-tree (for ordered access) — probably not needed
- LMDB or BoltDB for disk spillover — useful if you want to demonstrate memory-tier handling
- An S3 cold tier for evicted-but-not-dead entries — pairs naturally with the AWS deployment and gives a clean tiered-storage story (GPU mem → CPU RAM → NVMe → S3)

### Cloud provider: AWS (recommended)

AWS over GCP/Azure: the most job-posting mentions, Amazon's explicit "preferably AWS," the largest market share, and the core concepts (VMs, VPC, IAM, object storage, managed metrics) transfer directly to the other two. If HC is specifically targeting Google, GCP is a reasonable swap, but default to AWS for breadth.

### Orchestration: EC2 + Docker for v1, EKS as a post-v1 stretch (recommended)

This is a deliberate choice, and the reasoning is *not* just "simpler." The Phase 4 chaos harness kills nodes and partitions networks with `tc`/`iptables`. A managed Kubernetes control plane actively fights that — it reschedules pods and self-heals, which contaminates the correctness and recovery-time measurements you're trying to take. On raw EC2 instances you own the failure cleanly. So EC2 is both lower learning-surface *and* the more correct substrate for this specific project.

Kubernetes still carries strong recruiter signal, so capture it as a **post-v1 stretch**: once the EC2 version ships and is benchmarked, "migrated the cluster to EKS" is its own resume bullet and a strong blog post. Don't let it block v1.

### Infra-as-code: Terraform (recommended)

Terraform over CloudFormation/CDK: cloud-agnostic, the most asked-for in postings, and the single strongest "production engineer" tell for a new grad. Manage the entire cluster (VPC, instances, IAM, S3, ECR) as code. Bonus: it makes chaos runs reproducible — `terraform destroy` + `apply` rebuilds a clean cluster between experiments. Use an S3 backend with a DynamoDB lock for remote state (a small detail that signals real-world IaC hygiene).

### Cost control: Spot + teardown discipline + billing alarm

The cache cluster is CPU-only and runs for pennies/hour; the only expensive component is the GPU benchmark window (Phase 4.5), scoped to a few hours. Run cache nodes on Spot (≈60–70% cheaper) — and note Spot interruptions double as free chaos events (Phase 3). Always `terraform destroy` GPU nodes after benchmarking. Set an AWS billing alarm in Phase 0. *Verify current instance pricing before budgeting — GPU instance prices drift.*

---

## Section 6 — Risk Register and De-risking

### Risk 1: vLLM internals are harder than expected

**Likelihood:** Low–Medium *(downgraded from High — Revision A, 2026-05-24)*. **Impact:** Medium.

The original concern was that vLLM's KV cache is tightly coupled to its PagedAttention scheduler, so pulling tensors out and back would need deep, fragile modifications. That framing predates vLLM's KV-transfer subsystem.

**Why this is now smaller (verified May 2026):**
- vLLM exposes a first-class abstract interface, **`KVConnectorBase_V1`** (`vllm/distributed/kv_transfer/kv_connector/v1/base.py`), purpose-built for external KV offload, with a clean split between scheduler-process and worker-process methods. We implement against this maintained API instead of patching internals.
- Since **June 2025**, vLLM supports **dynamic loading** of connectors — our connector ships in our own package and is selected by config, so **no fork** of vLLM is needed.
- Production connectors already exist to learn from: **`NixlConnector`** (RDMA — note this is also the plan's "v2 NCCL/RDMA" optimization, already implemented upstream), **`OffloadingConnector`** (CPU/disk offload), and **`LMCacheConnector`**.

**Residual risk + mitigation:** the remaining work is *understanding* the connector contract — the scheduler/worker method split and the exact KV-block layout vLLM hands us — not inventing the integration. Spend Phase 0 reading the connector interface and one reference implementation. Fallback if even that proves too coupled for a given vLLM version: pin a known-good vLLM release, or run a thin Python coordinator in front of vLLM. See ADR 0008 and Section 8 resources.

### Risk 2: GPU access and cost

**Likelihood:** Low (after decoupling). **Impact:** Low–Medium.

This risk is largely *designed out* by the compute-by-role split (Section 3, Cloud deployment topology). The cache layer — the artifact and the bulk of the work — is CPU-only and needs no GPU at all. The synthetic load generator (Section 3, component 6) exercises every distributed property of the cache without a GPU. A GPU is only required for the single end-to-end TTFT number in Phase 4.5.

**Mitigation:** Run the Phase 0–1 vLLM integration work on HC's **local RTX 3080 (8 GB)** with TinyLlama-1.1B — no Colab or rental needed (better than Colab: always-on, no session limits, instant connector iteration). *(Updated 2026-05-24, Session 2 — was "free Colab GPU or a cheap rental.")* Do *all* of Phases 2–4 on CPU-only AWS instances with the synthetic generator. The Phase 4.5 paid GPU window is now **optional** — see Phase 4.5 for the local-vs-cloud tradeoff; if used, it's one or two `g5`/`g4dn` Spot instances in the same VPC for a few hours, then `terraform destroy`. Total GPU spend should land in the single-to-low-double-digit dollars per benchmark session rather than the hundreds a continuously-running GPU cluster would cost. UIUC research compute, if accessible, is another option. *Verify current pricing before budgeting.*

### Risk 3: Scope creep

**Likelihood:** Very high. **Impact:** Project doesn't finish.

Every interesting feature (locality-aware routing, NCCL transport, multi-tier storage, prefix tree indexing instead of hashing, **migrating EC2 → EKS**) is tempting and will eat weeks.

**Mitigation:** Ship the minimum end-to-end pipeline in Phase 1–3 (on AWS, CPU-only cache) before considering any new feature, and treat the Phase 4 "core ships" gate as a hard commitment. The EKS migration is explicitly a *post-v1* stretch — do not start it until the EC2 version is benchmarked.

**On the Section 3.5 differentiator specifically:** the cost-aware + fairness policy (Phase 5) is the *one sanctioned* scope addition, and it has its own internal guardrails — it starts only after Phase 4 ships, it's split into a must-ship 5a and a stretch 5b, and the rule is **no third differentiator**. The danger is treating "add features" as open season because one addition was blessed. It isn't: cost-aware + fairness compose into a single coherent policy engine; anything beyond that (predictive prefetching, compression, cross-model sharing, semantic matching) is creep and belongs in the interview "what I'd do next" answer, not the build.

### Risk 4: Hard to demo

**Likelihood:** Medium. **Impact:** Medium.

If the demo is "look at this Grafana dashboard," it won't be visceral.

**Mitigation:** Build a tiny chatbot UI that shows TTFT for each turn, with a "cache hit" badge. Demo it by running the same conversation twice — the second time, every turn lights up as a cache hit and is visibly faster. This is the kind of demo that lands in recruiter screens.

### Risk 5: Hard to talk about coherently in interviews

**Likelihood:** Medium. **Impact:** Medium-High.

The project touches a lot of areas. Without preparation, HC might ramble.

**Mitigation:** See Section 7. Practice the four story beats. The blog post in Phase 6 forces the kind of crisp narrative needed for interviews — and the Section 3.5 differentiator gives you the single best beat (the efficiency/fairness tension), since a tradeoff you can explain from both ends signals more than any raw speedup number.

---

## Section 7 — Interview Preparation

Future sessions: when HC asks for interview prep, use this as the canonical set of talking points.

### The 30-second elevator version

> "I built a distributed cache for LLM inference that shares attention KV state across a multi-node serving cluster. When you have multiple GPUs serving LLM requests with shared prompt prefixes — system prompts, RAG contexts — recomputing the prefix attention on every request is wasteful. My system caches those KV tensors across nodes with consistent hashing and replication, so any GPU in the cluster can reuse them. I measured a 60% reduction in p95 time-to-first-token on repeated-prefix workloads, with the system staying correct under randomized node-kill chaos tests. The part I'm most proud of is the eviction policy: it's cost-aware — keeping entries by how expensive they'd be to recompute, not just recency — with a per-tenant fairness constraint, because a purely cost-optimal cache starves cheap tenants. I can show the efficiency-versus-fairness tradeoff curve."

### The four story beats (practice these out loud)

**1. Hardest engineering decision.**

Candidate answers (pick one based on what actually happened):

- *Consistency model.* "I chose eventual consistency for the cache reads but linearizable consistency for the metadata. The justification: a stale cache hit is fine — at worst, you recompute, which is exactly what no-cache does. But stale metadata means writes go to the wrong shard, which silently corrupts the cache. So I used etcd with leases for metadata and async replication with version vectors for the cache itself."

- *Replication factor.* "I considered RF=3 with quorum reads but settled on RF=2 with primary-replica. RF=3 is overkill for cache data — losing an entry just causes a recompute. RF=2 cuts replication cost in half and still survives any single failure."

- *The efficiency/fairness tradeoff in the cache policy (the differentiator).* "Cost-aware eviction — keeping entries by `recompute_cost × reuse_probability`, basically GreedyDual-Size-Frequency adapted to KV tensors — is globally greedy. In a multi-tenant cluster it systematically starves any tenant whose prefixes are cheap to recompute, because the optimizer always finds someone else's entry more valuable. So pure efficiency and fairness directly conflict. I built one policy engine that maximizes cache value subject to a per-tenant max-min floor, work-conserving so idle capacity is still loaned out, with a knob between the two extremes. The hard part wasn't either policy alone — it was that they fight, and making the fair version still work-conserving rather than just statically partitioning capacity, which would've wasted it."

**2. What broke and how you found it.**

Strong answer: name a specific bug. Examples that might come up:

- *Split-brain on etcd lease expiry.* "Under a network partition, both halves of the cluster thought they were leader for the same shard, and both accepted writes. I found it via a chaos test that injected a 30-second partition and then validated cache contents — the two halves had divergent state. Fixed by making leases shorter than the partition detection window and requiring a fresh lease before any write."

- *Goroutine leak in connection handling.* "Memory grew unbounded under load. pprof showed thousands of goroutines stuck in a select. I'd forgotten to close a channel on client disconnect. The fix was one line, but finding it took a day of memory profiling."

**3. What you'd redesign.**

Strong answer:

- "The serialization is doing too much copying. v1 uses Protobuf, which means CPU-side encode/decode on multi-MB tensors on every read. I'd move to NCCL or RDMA for tensor transfer between GPU nodes, with Protobuf only for the metadata. The infrastructure to do that is heavier — you need device-aware networking — but it would probably double the throughput."

- "I'm using consistent hashing on prefix hash, which means similar prefixes that differ in one early token get hashed to completely different shards. A radix tree (like SGLang's RadixAttention) would let me share cache across prefixes that diverge late, not just prefixes that are byte-identical. The tradeoff is the radix tree is harder to distribute."

- *(Considered and rejected — a good signal to volunteer.)* "I considered semantic/fuzzy prefix matching — reuse cache for *similar* prompts via embedding similarity. I rejected it because it's a correctness landmine: KV tensors are position- and token-exact, so reusing them for a different token sequence produces wrong outputs, not just a worse cache hit. LMCache's CacheBlend tackles non-prefix sharing and it's genuinely hard. Knowing *why* the obvious idea is wrong was more useful than building it badly."

**4. Next 10x scale problem.**

Strong answer:

- "Today the bottleneck is cache memory — at scale, you can't hold every prefix in RAM. The 10x version needs tiered storage: hot prefixes in GPU memory, warm in CPU RAM, cold in NVMe. That changes the API — lookups become async with variable latency — and changes the routing — you want to prefer GPUs that have the relevant prefix in *their local* memory, not just any GPU."

- "Network becomes the bottleneck before compute. Today I send raw tensors over gRPC. At 10x, I'd compress (KV tensors are highly compressible — there's research showing 4-bit or even 2-bit quantization of KV cache is viable), and I'd consider locality-aware request routing so requests prefer the GPU that already has the prefix local."

- *(The research frontier — good to gesture at.)* "The next frontier is cross-model KV sharing — reusing cache across different fine-tunes of the same base model, which means 'translating' KV between models. There's recent research on this (DroidSpeak). It's well beyond what I built, but it's where the field is heading, and my fairness work would matter even more there because you'd be sharing an even scarcer, more valuable cache across more tenants and models."

### System design interviews

If asked to design a system, this project gives you authentic context for:

- Designing distributed caches (Redis Cluster, Memcached at scale)
- Designing LLM serving systems
- Designing systems with very large per-entry payloads (similar problems: distributed video transcode caches, distributed model weight serving)

---

## Section 8 — Resources

### Required reading (do in Phase 0)

- vLLM documentation: https://docs.vllm.ai
- PagedAttention paper (Kwon et al., 2023): https://arxiv.org/abs/2309.06180
- vLLM source code, focus on `vllm/core/block_manager.py` and `vllm/worker/cache_engine.py`
- LMCache project: https://github.com/LMCache/LMCache (this is roughly what you're building, but distributed)

**KV connector / KV-transfer integration (the Phase 1 integration path — added 2026-05-24, Revision A):**
- `KVConnectorBase_V1` interface: `vllm/distributed/kv_transfer/kv_connector/v1/base.py` — the abstract base our connector implements (scheduler-side vs worker-side method split)
- Reference connectors to study: `NixlConnector` (RDMA), `OffloadingConnector` (CPU/disk), `LMCacheConnector` — all under `vllm/distributed/kv_transfer/kv_connector/v1/`
- vLLM blog, "Inside vLLM's New KV Offloading Connector" (Jan 2026): https://vllm.ai/blog/2026-01-08-kv-offloading-connector
- RFC — KV Cache Offloading for Cross-Engine KV Reuse: https://github.com/vllm-project/vllm/issues/14724 (the cross-node reuse problem this project sits next to)
- KV cache transfer / disaggregated serving overview: https://deepwiki.com/vllm-project/vllm/9.4-kv-cache-transfer-and-disaggregated-serving

### Distributed systems background (if not familiar)

- Raft paper (Ongaro & Ousterhout, 2014): https://raft.github.io/raft.pdf — read once to understand what etcd is doing
- "Designing Data-Intensive Applications" by Martin Kleppmann — Chapters 5 (Replication), 6 (Partitioning), 7 (Transactions), 9 (Consistency). This is the single best background reading.
- MIT 6.824 lecture notes (free online) — Lectures 1–8 cover the relevant material

### Go learning (if needed)

- "Tour of Go" (official, free, ~1 day)
- "Effective Go" (official, ~3 hours)
- Write a small CLI tool in Go before starting Phase 1 — anything, just to get past the syntax wall

### LLM internals

- The Illustrated Transformer (Jay Alammar): https://jalammar.github.io/illustrated-transformer/
- Andrej Karpathy's "Let's build GPT" video — the part on KV caching is in the followup videos
- If unfamiliar with attention, watch this before reading PagedAttention

### Adjacent prior art (read for context, don't copy)

- SGLang RadixAttention paper
- DeepSeek's MLA (Multi-head Latent Attention) — different approach but interesting reading
- Together AI's blog posts on inference optimization
- NVIDIA TensorRT-LLM KV cache docs

---

## Section 9 — Resume Integration

Once finished, the project goes near the top of Technical Projects. Draft entry:

> **Distributed KV Cache for LLM Inference** *(Repo link)* — 2026
> *Go, gRPC, etcd, vLLM, AWS (EC2, S3, IAM, VPC), Terraform, Prometheus, Grafana, CloudWatch, OpenTelemetry*
> - Designed and shipped a distributed cache for LLM inference KV tensors that shares prefix attention state across a multi-node serving cluster, reducing p95 time-to-first-token by [measured]% on repeated-prefix workloads.
> - Implemented consistent-hashing shard placement, primary-replica replication with async log shipping, and etcd-coordinated graceful failover on a multi-node AWS cluster (EC2 across a VPC, provisioned with Terraform); sustained [measured] req/s with zero correctness violations under randomized node-kill and Spot-interruption chaos tests.
> - Built a custom gRPC protocol for KV-tensor transfer, persisted a cold tier and benchmark artifacts to S3, and instrumented end-to-end OpenTelemetry traces with cache hit/miss, eviction, and replication-lag metrics surfaced in Grafana and CloudWatch.

Replace `[measured]` with actual benchmark numbers. Don't ship the bullet without numbers.

**Note the deliberate design:** cloud is folded into bullets 2 and 3 (which describe the cluster and the transport/observability layers — the natural homes for AWS), *not* added as a fourth bullet. This keeps the entry to three bullets for the one-page rule while making the project double-cover the distributed-systems *and* cloud preferred quals. The single project now hits: distributed systems, consistent hashing, replication, fault tolerance, gRPC, Go, AWS, EC2/VPC/S3/IAM, Terraform (IaC), CloudWatch, Prometheus/Grafana, OpenTelemetry, vLLM, LLM serving, KV cache, prefix sharing, and chaos testing.

If applying to AI-lab infra roles specifically, lead with the GenAI framing. If applying to general SWE/distributed-systems roles, lead with the distributed systems framing. If applying to cloud-native/platform/infra-heavy roles, lead with the AWS + Terraform framing — same project, three emphases:

- **AI infra framing:** "...for LLM inference that shares prefix attention state across a multi-GPU serving cluster..."
- **Distributed systems framing:** "...high-throughput distributed cache with consistent-hashing sharding, async replication, and etcd-coordinated leader election..."
- **Cloud/platform framing:** "...a fault-tolerant distributed cache deployed on AWS via Terraform — EC2 across a VPC, S3-backed tiering, IAM-scoped nodes, CloudWatch + Prometheus observability — chaos-tested against node-kill and Spot reclamation..."

---

## Section 10 — Decisions Log

When future sessions help HC with this project, append decisions here so the next session has the full context.

| Date | Decision | Rationale |
|---|---|---|
| Initial scoping | Build distributed KV cache for LLM inference instead of plain Raft KV store | Hits both distributed systems AND GenAI infrastructure preferred quals; stronger story; real use case |
| Initial scoping | Use Go for cache server | Lower learning curve than Rust; strong recruiter signal; mature gRPC ecosystem |
| Initial scoping | Use etcd for metadata, not build own consensus | Saves 4–6 weeks; marginal resume value of implementing Raft inside this project is low |
| Initial scoping | Integrate with vLLM as external service, not fork | Lower coupling, easier to demo, lower risk of integration getting stuck |
| Initial scoping | Target 14–18 weeks, RF=2, single-cluster scope | Forces shipping over feature creep |
| Cloud integration (2026-05) | Deploy on AWS via Terraform from Phase 2 onward instead of local Docker Compose | Doubles the project's coverage to include the cloud preferred qual (AWS appears as a preferred qual at most surveyed firms; "preferably AWS" at Amazon) for near-lateral effort; IaC partly substitutes for local multi-node setup |
| Cloud integration (2026-05) | Decouple compute by role: cache layer CPU-only, GPU confined to Phase 4.5 benchmark | Removes GPU from the critical path, collapses cloud cost, and decouples the distributed work from the highest-risk component (vLLM internals) |
| Cloud integration (2026-05) | Add a synthetic load generator in Phase 1 | Lets all distributed-systems testing (sharding, replication, failover, chaos) run without a GPU or vLLM, de-risking the project |
| Cloud integration (2026-05) | EC2 + Docker for v1, EKS as a post-v1 stretch | Raw EC2 makes node-kill/partition chaos testing clean; a managed k8s control plane would fight the chaos harness. EKS captured as a later bullet, not a v1 dependency |
| Cloud integration (2026-05) | Run cache nodes on EC2 Spot | ≈60–70% cost cut and Spot reclamation doubles as realistic, free chaos events |
| Cloud integration (2026-05) | Revise timeline 14–18 → 15–20 weeks | Absorbs cloud onboarding, Terraform, and the dedicated GPU benchmark phase |
| Differentiator (2026-05) | Add a multi-objective cache policy (cost-aware GDSF + DRF-style fairness) as the headline differentiator (Section 3.5, Phase 5) | Crowded space (LMCache, Dynamo, Mooncake); a defensible non-obvious insight beats feature count for a resume/interview artifact. The two objectives compose into one policy engine via a real efficiency/fairness tension |
| Differentiator (2026-05) | Combine cost-aware + fairness rather than either alone | They share the eviction/admission subsystem and create a productive tension (cost-greedy starves cheap tenants); combining is coherent, not scope creep. Capped at these two — no third differentiator |
| Differentiator (2026-05) | Work-conserving/elastic fairness, not static partitions | Static quotas waste idle capacity and remove the tension entirely (nothing to defend). Elastic reclaim-under-contention is where the engineering and the story live |
| Differentiator (2026-05) | Split into must-ship 5a (static) + stretch 5b (elastic + knob) | Prevents the ambitious feature from becoming a scope bomb; 5a alone is still defensible |
| Differentiator (2026-05) | Gate Phase 5 behind the Phase 4 "core ships" milestone | Differentiator is high-upside, not load-bearing; a benchmarked core v1 must exist first |
| Differentiator (2026-05) | Full-project timeline ~21–26 weeks (core 15–20 + diff ~4 + polish ~2–4) | Honest accounting of the added phases |
| Session 1 (2026-05-24) | **Confirm Go** for the cache server (was "lean Go") | HC confirmed; resolves the open language question. ADR 0001 |
| Session 1 (2026-05-24) | **Confirm etcd** for metadata (not a custom Raft) | HC confirmed; resolves the open metadata question. ADR 0002 |
| Session 1 (2026-05-24) | **Revision A — integrate via `KVConnectorBase_V1`, no fork** | vLLM's KV-transfer subsystem + dynamic connector loading make a fork unnecessary; downgrades Risk 1 from High/High to Low–Medium. ADR 0008 |
| Session 1 (2026-05-24) | **Revision B — re-anchor novelty** on the distributed layer + the cost-aware/fairness policy, not on "building the integration" | vLLM+LMCache already offload KV and cross-engine reuse is an open RFC; honest framing keeps the interview story defensible |
| Session 1 (2026-05-24) | **Revision C — etcd on-demand (never Spot), 3-node recommended for v1** | etcd is coordination ground truth; Spot would risk quorum. 3 nodes make the split-brain/failover story real. ADR 0009 |
| Session 1 (2026-05-24) | **Revision D — opaque-bytes `prefix_hash` key; two generated clients (Go + Python)** | Keeps tokenization Python-side and the Go server tokenizer-free; the proto serves both the load generator and the vLLM connector. ADR 0010 |
| Session 1 (2026-05-24) | **Revision E — size cache nodes for RAM; configurable load-gen payload** | `t3.micro` only suits the Phase 0 smoke test; a 500-token prefix ≈ 1 GB. Configurable payload keeps soak tests cheap |
| Session 1 (2026-05-24) | Project docs consolidated into `docs/`; Claude Code config added (CLAUDE.md, rules, skills, pre-commit hook) | Persist working rules and keep the context budget lean per the initial prompt's side note |
| Session 2 (2026-05-24) | Authored `docs/01-architecture-overview.md` (full-scope target architecture) alongside the Phase-1 `01-architecture.md` | HC wanted one design-level doc covering the whole system (sharded/replicated/multi-tenant/cloud) to reevaluate before later phases; decided vs. proposed parts are tagged |
| Session 2 (2026-05-24) | Use HC's **local RTX 3080 (8 GB)** for Phase 0–1 vLLM work; cloud GPU now optional (Phase 4.5 only) | Removes Colab/rental from the critical path and de-risks the connector. **Cache design unchanged — stays CPU-only:** it stores/ships opaque KV bytes and computes nothing; VRAM is the scarce tier it offloads *from*. 8 GB ⇒ TinyLlama for dev, Llama-3-8B only 4-bit/tight. vLLM = NVIDIA/CUDA (use the 3080, not the iGPU); native-Win11 or WSL2, WSL2 also fixes the `-race` toolchain |
| Session 2 (2026-05-24) | **ADR 0014 — sharding granularity (prefix-affinity vs per-block), Proposed** | Design review surfaced that per-block hashing scatters a prefix across shards (fan-out latency vs the sub-10 ms goal). Affinity co-locates but risks hot shards. Captured as a Phase-2 open decision while it's cheap to change |
| Session 2 (2026-05-24) | **ADR 0015 — KV payload as opaque framed bytes; serialization overhead a Phase 1 gate** | The project's core inequality (lookup+transfer < recompute) was an unmeasured assumption; make it a Phase 1 exit gate and keep tensor bytes out of protobuf |
| Session 2 (2026-05-24) | **ADR 0016 — cache correctness invariant** | "Zero correctness violations" was undefined for an eventually-consistent cache; pinned it to "never serve KV mismatching the requested key/model/tokens" so chaos tests have a real assertion |
| Session 2 (2026-05-24) | **ADR 0017 — write admission / backpressure, Proposed** | Multi-MB writes can OOM a node before eviction reacts; reject-fast above a high-water mark (a rejected write just recomputes) closes the gap |
| Session 3 (2026-05-25) | **Phase 1 verified ~80% done; proceed to Phase 2 now** | CPU-only core (server, store, block-hash, load generator, Python support libs) complete + clean (build/vet/gofmt/plain-test pass). vLLM tensor-copy hooks + TTFT gate remain stubs but are GPU-path work, deliberately decoupled — they fold into Phase 4.5. The synthetic load generator drives all of Phase 2 without a GPU |
| Session 3 (2026-05-25) | **Phase 2 sequence: local multi-process first, then AWS** | Build + test the ring/routing across local cache-server processes to tighten the learning loop; AWS becomes a deployment step, not a debugging surface |
| Session 3 (2026-05-25) | **ADR 0014 ratified — prefix-affinity sharding (key on `block_hash[0]`)** | One prompt's blocks co-locate → one-RPC sub-10 ms lookups, upholding the plan's latency premise. Accept hot-shard risk; Phase 2 *measures* it via per-shard distribution; hot-prefix replication is the deferred mitigation |
| Session 3 (2026-05-25) | **ADR 0018 — static ring membership in Phase 2; etcd deferred to Phase 3** | A fixed 2–3 node cluster has no churn, so clients build an identical ring from a static config; etcd's leases/watches only earn their keep at Phase 3 failover. Keeps Phase 2 focused (ship sharding before adding consensus) |
| Session 4 (2026-05-27) | **ADR 0019 — client-side routing layer (`internal/cluster`); `SetMembers` member seam. Phase 2 complete** | The ring had no consumer; added a smart-client router (ring + conn pool) wired into the load generator. Chose `Router.SetMembers([]Member)` pushed by a driver (static now, etcd watch in Phase 3) over a pull interface — keeps all mutation/locking in one place so the etcd swap changes no routing code. Live 3-shard run reports per-shard distribution (hot shard ~87% at prefix-share 0.8), taking ADR 0014's deferred measurement |
| Session 4 (2026-05-27) | **ADR 0020 — etcd membership (Sub-stage A): lease-bound `/kvcache/members/`, membership-only** | First etcd slice. Nodes self-register under a lease+keepalive (crash → lease lapses → ring removal; graceful → revoke); clients watch the prefix (Get revision+1 to avoid gaps) and feed full snapshots to the same `SetMembers` seam. No `/kvcache/ring/*` — ring is deterministic from members (ADR 0018). Verified live: 3 servers self-register, loadgen with only `-etcd` discovers them and reproduces the static distribution exactly. Lease TTL is the split-brain knob for Sub-stage C |
| Session N (2026-06-05) | **ADR 0026 — chaos harness: hard process-kill fault + `payload=f(hash)` correctness oracle. Phase 4 core ships** | `cmd/chaos` builds the binaries, launches an etcd-backed N-node cluster, drives `loadgen -verify` load, and SIGKILLs random nodes on a schedule; exit code is the verdict (CI gate). Made ADR 0016 *executable*: each block's bytes are a deterministic function of its hash, so corruption or a mis-served block fails loudly while a `NotFound` stays a legitimate miss. Build (not `go run`) so the kill is a real crash; `aliveAboveRF` guard keeps the recovery story legible. Verified live (3 nodes, rf=2, 5s lease, 40s): 3 kills/2 restarts, 4815 reqs, **0 violations / 0 errors / 0 degraded**. Partition + latency faults deferred to AWS (need `tc`/`iptables`); CloudWatch alarms likewise. This is the Phase 4 "core v1 ships" gate |
| Session N (2026-06-05) | **ADR 0027 — S3 cold tier (spill-on-evict + Fetch read-through)** | Evicted blocks (pressure + TTL, via the single `Store.evict` chokepoint) demote to S3 instead of being lost; a hot miss reads through + re-admits. Kept behind `-cold-bucket` (off = cloud-free core). Leaf `internal/coldtier` owns the AWS SDK + framing; `cache` exposes a `SpillFunc` and `server` a `coldReader` interface, so both still test without the SDK. Correctness holds across the tier (cold objects carry version+token_ids; read-through re-applies the ADR 0016 guards). Tested locally; vet/gofmt clean |
| Session N (2026-06-05) | **ADR 0028 — AWS cluster Terraform: authored, pending first `apply`** | Two roots (`bootstrap` local-state backend; `cluster` flat root on S3 backend). Single-AZ VPC, no NAT; **3-node static-IP etcd** (initial-cluster templated before instances exist); Spot cache nodes run the ECR image under systemd Docker (`ExecStop`→SIGTERM→drain, ADR 0023; IMDSv2 **hop-limit 2** for the in-container Spot poller); discrete instances not an ASG (chaos-clean, ADR 0005); least-priv IAM scoped to the cold-bucket ARN, no static creds. `.gitattributes` pins LF for `*.sh`/`*.tftpl` (must be LF on Linux) + `*.go` (fixes the gofmt-on-Windows friction). Not yet validated against a live account — `fmt`/`validate`/`apply` are Stage-0-gated HC actions |
| Session N (2026-06-06) | **First AWS `apply` — cluster live + verified; ADR 0028 → accepted** | Staged apply (bootstrap → cluster) against a real account succeeded. Verified: etcd 3-node quorum, 3 Spot cache nodes registered, image in ECR, `loadgen -verify` 0 violations (6.6k req), ~87% hot-shard reproduced on AWS. Three fixes found *only* at apply (not at `validate`/`plan`): (1) `templatefile()` parses comments → a literal `${...}` in a `.tftpl` comment broke validate; (2) first-boot image race — fatal `docker pull` in user-data bricked nodes before the systemd unit was written → made non-fatal + `user_data_replace_on_change`; (3) `t3.small` burst-credit exhaustion under load throttled nodes to baseline, starving the etcd lease keepalive → empty membership → `cpu_credits=unlimited` (+ recommend non-burstable `c7i.large` for sustained load). Operational rules learned: never run loadgen on a shard; drive it from an etcd node/bastion inside the VPC. Findings recorded in ADR 0028 + learning-log |
| Session N (2026-06-06) | **Commit hygiene: no AI co-authorship** | Per HC: commit messages must never include `Co-Authored-By`/AI-attribution trailers; commits are authored by HC. Added to CLAUDE.md Git workflow |
| Session N (2026-06-08) | **ADR 0031 — Phase 4.5 single-node GPU TTFT: the cache loses on a laptop, measured & decomposed** | Wired the stubbed vLLM tensor-copy hooks (probe live paged-KV layout `[2,num_blocks,16,kv_heads,head_dim]` bf16 `block_axis=1` → save full blocks → load back into paged slots), added a BatchFetch RPC, and ran TTFT on TinyLlama-1.1B + Qwen2.5-3B (RTX 3080, WSL2, vLLM 0.22.1). Save/load works + correct (0 violations). **The ADR 0015 inequality inverts at single-node ≤3B: external load 4.7 ms/block > recompute 3.4 ms/block.** Two bottleneck hypotheses falsified with data: batching 48 fetches→1 moved TTFT by noise (RTT wasn't the cost), then a profiler split the load `batch_fetch 89 ms + deserialize/unpinned-copy 135 ms` — both Python-serialization + WSL2 `pin_memory=False`, not bandwidth. Deficit closes with model size (−169%@1B → ~−48%@3B). Env-capped, not algorithmic → headline TTFT win deferred to the distributed/cloud run (pinned mem + zero-copy transport + non-throttled KV cache), bundled with the AWS paid batch. `-race` cleared in WSL2. Resume `[measured]` stays unfilled by a positive single-node number by design |
| Session N (2026-06-09) | **ADR 0032 — distributed-TTFT prep: tensor-parallel KV keys + a cost-guarded GPU node (no spend)** | HC picked the ambitious headline — a 30B-class model with **tensor parallelism (TP=4)** on a `g5.12xlarge`. TP runs one vLLM worker per GPU rank, each holding a disjoint KV-head shard, but the block hash is rank-independent → ranks would clobber each other's shard on the same server key (a wrong serve, ADR 0016). Fix, entirely in the connector: fold the rank into the **opaque** `model_id` (`shard_model_id`, server untouched per ADR 0010); scheduler checks presence under canonical rank 0 and trusts the lockstep invariant, with the existing hash-guard → recompute as the safety net. Also: `terraform/cluster/gpu.tf` (`gpu_count` default 0 = no accidental GPU bill; cache SG already allows VPC gRPC so no new rule); a TP-aware distributed driver + `loadgen -verify-coldtier`; and the paid-window runbook. All locally verified (CPU unit test, build/vet/`go test`, `terraform fmt`/`validate`); the TTFT number + TP behavior are validated *in* the window. Rejected mixing rank into the hash (would spread shards but couples to Go `blockhash`); hot-shard concentration now ×world, accepted for v1 |
| Session N (2026-06-11) | **ADR 0034 — RunPod GPU-cloud window (Option B); Session A executed, Session B pending** | RunPod Secure Cloud for GPU compute (AWS remains distributed story). Session A: 1× A100 80GB, long-context sweep to 32k + 14B scaling + `vllm serve` demo (~$3–4). **No 32k crossover** on A100 (prefill too fast; warm Python-bound); 14B worse than 7B at same tokens (KV-bytes/FLOPs). AWS L4 **+10.9% @ 4k** stays resume headline. Session B handoff: TP=4 / Qwen2.5-32B on 4× A6000 — `runpod-gpu-window-plan.md`. Pre-flight `ff0feb3`; results `phase45-gpu-cloud.md`. Terminate pods in console after each session |

---

## Section 11 — Pickup Checklist for Future Sessions

If you're a Claude session being asked to help with this project, before answering questions:

1. **Confirm what phase HC is in.** Ask: "Which phase are you currently working on?" Phase context changes which advice is relevant.
2. **Check the decisions log.** If HC has changed any of the original decisions (e.g., switched to Rust, decided to implement Raft), get the rationale before working from outdated assumptions.
3. **Check if HC is stuck.** If HC is debugging something specific, lean into that rather than re-planning. The plan above is for the macro project; debugging help is local.
4. **Don't re-design what's been built.** If HC has shipped Phase 2 and is working on Phase 3, don't suggest a different architecture for Phase 1 unless explicitly asked.
5. **Honor HC's preferences from the user memory:** direct engagement, pressure-test reasoning, don't offer unsolicited recommendations, no excessive hedging.
6. **When suggesting code, default to Go** unless HC has stated otherwise.

Future sessions can append new sections (a Section 12, 13, etc.) for ongoing work — implementation notes, debugging stories, post-mortems. Keep this document as the living source of truth.

---

## Section 12 — Phase 3 Sub-stage Tracker (live status)

Phase 3 (§4 "Replication and Failure Handling") was broken into sub-stages in Session 4 (2026-05-27).
A blocking finding opened it: Phase 2's ring had **no consumer** (loadgen dialed a single addr), so
routing had to be finished first as "Step 0". Sequencing decisions this session: **local-first then
AWS**, and **distributed core first, infra (IAM + S3) second**. Each sub-stage follows the CLAUDE.md
guided loop and ends with an ADR. Detailed plan: `~/.claude/plans/` (this machine) — summarized here so
it survives a session switch.

| # | Sub-stage | Status | ADR | Notes |
|---|---|---|---|---|
| 0 | **Finish Phase 2 client routing** | ✅ done | 0019 | `internal/cluster` smart-client router (ring + conn pool); `SetMembers` seam; degrade-to-miss; loadgen `-members` + per-shard distribution. Live 3-shard run: hot shard ~87% at prefix-share 0.8 (ADR 0014 measurement). |
| A | **etcd membership + ownership** | ✅ done | 0020 | `internal/coord`: `Register` (lease+keepalive+revoke), `WatchMembers` (Get rev+1 → full snapshots → `SetMembers`). `cache-server` self-registers (`-etcd/-advertise/-node-id/-lease-ttl`); loadgen discovers via `-etcd`. Membership-only schema (ring recomputed, ADR 0018). Verified live vs Docker etcd. |
| B | **RF=2 async replication** | ⬜ not started | (0021 planned) | Extend ring with `LookupN(key,n)` → primary + replica. Additive `Replicate` RPC. Primary acks client, then async ships to replica; replica applies via `Store.Put`; replica-lag backpressure. Open: log format, version reconciliation, may `Fetch` serve from a replica. |
| C | **Leader election + failover/promotion** | ⬜ not started | (0022 planned) | etcd lease on `/kvcache/leader/shard-<k>`; replica promotes on lease lapse (data already present via B, so **no `Handoff` RPC** — re-warm gaps on miss). Split-brain knob: **lease TTL < partition-detection window** (the §A 10s TTL feeds this); fresh-lease-before-write guard; old-primary reconciliation by version. |
| D | **Graceful drain + Spot interruption** | ⬜ not started | — | On SIGTERM: deregister from etcd first (release lease so clients stop routing + replica promotes), drain in-flight, then `GracefulStop`. The release-before-stop ordering is already in `cmd/cache-server/main.go`; add an AWS Spot-interruption poller (`169.254.169.254/.../spot/instance-action`) triggering the same path. |
| E | **AWS infra: etcd 3-node + Spot nodes + IAM + S3 cold tier** | ✅ **live + verified** (2026-06-06) | 0027, 0028 | First `apply` succeeded against a real account. **Verified live:** etcd 3-node quorum (elected leader, write/read), all 3 Spot cache nodes self-registered in `/kvcache/members/`, image built+pushed to ECR, `loadgen -verify` inside the VPC = **0 violations** over 6.6k req, ADR 0014 ~87% hot-shard reproduced on AWS. **Fixes found at apply (committed):** `templatefile()` comment bug; non-fatal boot pull + `user_data_replace_on_change` (first-boot image race); `t3` `cpu_credits=unlimited` (burst-credit exhaustion starved the etcd keepalive → empty membership). Findings in ADR 0028. **Still pending:** cold-tier round-trip verify (force eviction → S3 objects → recovered cold hit); AWS chaos (`aws-chaos.sh` instance-kill + `tc`/`iptables`); CloudWatch *alarm* wiring check. Never auto-`apply`. |

**Cross-cutting carryover:**
- **`-race` debt:** the Windows box can't run `go test -race` (32-bit MinGW cgo). Routing + the etcd
  watch goroutine still need one WSL2 `-race` pass before the Phase-4 chaos suite.
- **Local etcd:** single-node `kvc-etcd` Docker container on `localhost:2379` (image
  `quay.io/coreos/etcd:v3.5.17`, matches the Go client dep). Sub-stages B/C reuse it.
- **Exit invariant for the phase (ADR 0016):** under node kill / Spot reclaim, never serve KV that
  mismatches the requested `(block_hash, model_id, token_ids)`; misses are fine. This is the assertion
  the failover and (Phase 4) chaos tests must hold.
