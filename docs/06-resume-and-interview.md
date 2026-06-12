# 06 — Resume bullets & interview prep (final, with measured numbers)

> Supersedes the *hypothetical* numbers in plan §7 (written before execution — e.g. the "60 %
> p95" placeholder). **Everything below is measured and traceable** to
> [`docs/benchmarks/`](benchmarks/). Use only these numbers.

## Resume bullets

Pick one block per resume flavor; trim to taste. Every number has a source.

**Systems / backend flavor**

- Built a distributed KV cache for LLM inference in Go — consistent-hash sharding, RF=2 async
  replication, etcd-coordinated failover — that cut time-to-first-token by **10.9 %** on 4k-token
  shared prefixes against a live vLLM GPU node, measured cross-AZ on AWS.
- Designed a chaos-test harness with an end-to-end integrity oracle (every fetched block
  re-hashed client-side); sustained **0 correctness violations** across injected latency, etcd
  partitions, hard node kills, and real AWS Spot interruptions (10k+ verified requests per run).
- Devised a work-conserving multi-tenant eviction policy (GDSF cost-awareness + an elastic
  max-min fairness knob) that **Pareto-dominates static quotas** — simultaneously +2.2 pts global
  hit rate and +2 pts worst-tenant hit rate — and swept the full efficiency↔fairness frontier.

**Infra / cloud flavor**

- Deployed and benchmarked the system on AWS via Terraform (Spot cache nodes, 3-node etcd, S3
  cold tier, ECR, CloudWatch alarms); used **Spot interruptions as free chaos testing**; total
  benchmark spend ≈ $5–7 across two GPU windows.
- Replayed 2,000 ShareGPT conversations (6,782 requests) against the live 3-node cluster:
  **32.7 % block hit rate**, 58 req/s, p50 62 ms, balanced 37/31/32 % across shards, 0 errors.

**ML-serving flavor**

- Integrated with vLLM via a custom `KVConnectorBase_V1` connector (no fork) that pages
  attention KV tensors GPU↔host↔gRPC, with tensor-parallel-aware keying validated end-to-end at
  **TP=4 / Qwen2.5-32B** on 4× A40.
- Characterized when remote KV caching pays: measured the TTFT crossover (~1k tokens on an L4;
  **no crossover ≤32k on an A100** — prefill speed vs transfer cost), and published the negative
  result alongside the win.

## The 30-second elevator version (updated to measured reality)

> "I built a distributed cache for LLM inference that shares attention KV tensors across a
> serving cluster — when requests share a prompt prefix (system prompts, RAG context), any GPU
> can reuse the prefill another GPU already did. It's Go: consistent hashing, RF=2 async
> replication, etcd failover, deployed on AWS with Terraform. On a real GPU it cuts
> time-to-first-token about 11 % at 4k-token prefixes — and I can tell you exactly where it
> *stops* helping and why, because I measured the crossover. It survived chaos testing — real
> node kills, etcd partitions, Spot reclaims — with zero integrity violations, verified
> end-to-end. The part I'm proudest of is the eviction policy: cost-aware caching starves your
> cheapest tenant by construction, so I built a work-conserving fairness knob into victim
> selection and swept the whole efficiency-versus-fairness frontier. I can show you the curve."

## The four story beats

### 1. Hardest engineering decision — the efficiency/fairness tension

GDSF (`H = L + freq·cost/size`) is globally greedy: in a multi-tenant cache it hands everything
to whoever's blocks are most expensive — measured: best-ever 20 % global hit rate with the cheap
tenant starved to **1.9 %**. Static quotas fix fairness but aren't work-conserving — they tax
efficiency even with zero contention (measured: −2.3 pts vs LRU). The design that resolved it:
quotas become *elastic floors*, eviction stays watermark-only, and fairness lives inside victim
selection as a per-tenant discount `H_eff = H/(1 + w·overage)`. One knob, `w ∈ [0,1]`, sweeps
pure GDSF → max-min fairness. Two findings worth volunteering: (a) elastic `w=0.25`
**Pareto-dominates** static caps (14.4 %/12.3 % vs 12.2 %/10.3 %); (b) **the knob saturates** —
the entire transition happens in `w ∈ (0, 0.25]` because a multiplicative discount only needs to
*reorder* victims once. A knob that's secretly a switch is a design finding, not noise.

### 2. What broke and how you found it — the TP=4 silent-corruption bug (real, ADR 0035)

This is the strongest possible answer because it actually happened:

> "My store keyed entries by content hash of the token block. Under tensor parallelism that's
> wrong in a subtle way: all 4 ranks hash the *same tokens* but hold *different* KV-head shards.
> So four writes landed on one map slot, last writer wins. The vicious part: my integrity guard
> re-verified the stamped content hash on every fetch — and it *passed*, because the hash
> matches across ranks. Rank 0 could be served rank 3's tensor with a green checkmark. The
> benchmark caught it instead: TP=4 write counts were 4× too low. Fix: namespace store keys by
> model/shard identity — `SHA-256(model_id ‖ wire_hash)` — keep the wire hash for
> replication/spill, re-validate on hardware: 512 writes = 128 blocks × 4 ranks, exactly once."

Lesson to state out loud: *an integrity check is only as good as the identity it checks* — the
guard verified "is this the right bytes for this hash," not "is this the right shard for this
rank." Backup stories (also real): the cold tier was silently dead on AWS because the container
lacked `AWS_REGION` (found via S3 object counts, not errors); CloudWatch alarms never fired on
node loss until `treat_missing_data="breaching"` (a dead node doesn't *send* metrics); a 2 s gRPC
deadline silently timed out ~2 GB warm fetches at 32k context — every one was found by a
benchmark asserting on *outcomes*, not logs.

### 3. What you'd redesign

- **Transport.** Protobuf/gRPC copies multi-GB tensors through CPU on every fetch — the A100
  result proves this is the binding constraint (warm path Python/transfer-bound; no crossover
  ≤32k). The redesign is the production answer: RDMA/NIXL for tensor payloads, gRPC only for
  control. That's exactly the LMCache/Dynamo/Mooncake architecture — I now understand *why* from
  my own measurements.
- **Prefix granularity.** Consistent hashing on exact block hashes shares only byte-identical
  prefixes; a distributed radix structure (à la SGLang RadixAttention) would share
  late-diverging prefixes. Harder to distribute — the tree wants locality, the hash ring
  destroys it.
- **Considered and rejected (good signal):** semantic/fuzzy prefix reuse — rejected because KV
  is position- and token-exact; reusing across different token sequences produces *wrong
  outputs*, not just misses. Knowing why the obvious idea is incorrect was worth more than
  building it.

### 4. Next 10× scale problem

- **Tiering.** At scale the cache can't live in RAM: GPU-local hot tier, CPU warm, NVMe/S3 cold.
  I have the S3 cold tier built and a measured failure mode to discuss: bursty eviction sheds
  spills (~40–60 PUT/s vs hundreds/s bursts; drop-over-stall by design — a lost spill is a
  recompute, never an error).
- **Network before compute.** At 10× I'd quantize KV in flight (4-bit KV is viable per research)
  and route requests *to* the data — locality-aware scheduling instead of data movement. My
  cross-AZ <1 ms penalty was tolerable at 7B/4k; it won't be at 70B/128k.
- **Frontier gesture:** cross-model KV reuse (different fine-tunes of one base; e.g.
  DroidSpeak). Multi-tenant fairness matters *more* there — scarcer cache, more claimants.

## Likely follow-ups, with grounded answers

| Question | The answer, in one breath |
|---|---|
| Why does the cache *lose* at small prefixes / fast GPUs? | The hit saves prefill (super-linear in tokens) but costs a fixed RPC + linear transfer + deserialize. L4: break-even ~1k tokens. A100: prefill so fast the warm path never wins ≤32k — and 14B loses *worse* than 7B at equal tokens because KV bytes grow faster than saved FLOPs. |
| Why RF=2, not RF=3/quorum? | It's a cache: losing an entry is a recompute, not data loss. RF=2 survives any single failure at half RF=3's write amplification. Metadata (ring membership) is where consistency matters — that's etcd/Raft, linearizable. |
| Why eventual consistency on reads? | A stale read is a *miss or a verified-correct hit* — the content hash makes wrong bytes impossible to serve silently (ADR 0016), and a miss just recomputes, which is the no-cache baseline anyway. |
| Why Go and not Rust/C++? | Concurrency model fit (goroutine-per-conn + channels for drain/failover), mature gRPC, and speed-to-correctness: the race detector ran on every commit. Latency budget is network-dominated; language wasn't the bottleneck. |
| Why not just use LMCache? | The goal was learning the distributed layer LMCache delegates. Same vLLM integration point; I own sharding/replication/failover/eviction — and the fairness policy doesn't exist there. |
| Is 32.7 % hit rate good? | It's *organic* — real ShareGPT multi-turn structure, no synthetic prefix injection. Every point is prefill skipped; at 4k-token prefixes each hit is ~11 % of TTFT. |
| What does `-verify` actually verify? | Payload = f(key) end-to-end: the client recomputes the expected bytes for every fetched block. Any mismatch = violation = non-zero exit. Every benchmark and chaos number in the repo ran under it. |

## System-design interview leverage

Authentic context for: distributed caches (huge values, skewed value-of-entry), LLM serving
(TTFT decomposition, PagedAttention block management, TP sharding), multi-tenant resource
policies (DRF-adjacent, work conservation), failure-domain design on Spot, and "design a
benchmark that can't lie to you" (integrity oracles, negative results).
