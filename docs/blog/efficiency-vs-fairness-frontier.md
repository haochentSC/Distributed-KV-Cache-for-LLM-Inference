# The efficiency-vs-fairness frontier of a multi-tenant LLM KV cache

> **Draft for HC to edit and publish.** Plain markdown + one image
> (`docs/img/fairness-frontier.png`) so it ports to a personal site, dev.to, or Medium as-is.
> Word count ≈ 1,900. Voice is first-person; revise freely — especially the intro and the
> closing section, which should sound like you.

---

I spent the last few months building a distributed KV cache for LLM inference — a Go service
that stores transformer attention KV tensors so that any vLLM worker in a cluster can reuse a
prompt prefix some other worker already prefilled. Most of the project is classic distributed
systems (consistent hashing, replication, etcd-coordinated failover, chaos testing on AWS Spot),
and most write-ups about caches like this lead with a latency number.

This post is about something else: the part of the project where two reasonable goals turned out
to be genuinely, measurably in conflict — and what the shape of that conflict looks like when you
sweep it end to end. The question:

**When several tenants share one cache, who do you evict?**

## Why multi-tenant caching has a fairness problem

An LLM KV cache is unusual among caches in two ways.

First, the entries are *huge*. One 4k-token prefix for a 7B model is on the order of a gigabyte
of KV tensors at fp16. You don't cache millions of small objects; you cache thousands of large
blocks, and a single tenant's workload can plausibly fill the whole tier.

Second, the *value* of an entry varies enormously and predictably. A cache hit saves you the
prefill compute for that prefix — and prefill cost grows super-linearly with prefix length. A hit
on a 4k-token RAG context is worth far more than a hit on a 200-token system prompt. So unlike a
CDN cache, where LRU is usually close enough, a KV cache really wants to be **cost-aware**: keep
the entries whose recomputation is expensive.

There's a well-studied policy for exactly this: **GDSF** (Greedy-Dual-Size-Frequency). Each entry
gets a priority

```
H = L + freq · cost / size
```

where `cost` is the measured time to recreate the entry, `size` is its bytes, `freq` is its hit
count, and `L` is a global aging floor that rises whenever something is evicted (so cold
high-value entries eventually age out). Evict the lowest `H`. It's simple, it's O(log n), and it
demonstrably beats LRU when entry values are skewed — which in a KV cache they always are.

And here's the problem: GDSF is *globally* greedy. It doesn't know what a tenant is. If tenant B
runs long-context requests (huge, expensive, valuable blocks) and tenant A runs short
cheap-to-recompute prompts, GDSF will — correctly, by its own objective — hand the entire cache
to B. A's entries are always the cheapest to recreate, so A's entries are always the victims.

I measured exactly this. Three synthetic tenants share one shard whose working sets oversubscribe
it roughly 2:1 — A is cheap and frequent, B is expensive and rare, C is bursty. Under pure GDSF,
the cache posts its best-ever global hit rate… and tenant A's hit rate collapses to **1.9 %**
while B sits at 28.7 %. If A and B are two products of the same company — or two paying
customers — that's not a cache, that's a denial of service with extra steps.

## The obvious fix fails in an interesting way

The textbook answer is per-tenant quotas: give each tenant a byte budget, and when a tenant
exceeds it, evict from that tenant first. I built that (it's the `gdsf` policy with
`-tenant-quota` in the repo), and it does protect the starved tenant. The min-tenant hit rate
recovers to roughly LRU levels.

But hard caps have a structural flaw: they're **not work-conserving**. A tenant over its quota
gets its blocks reclaimed *even when the cache has free space* — capacity that nobody else wants
goes idle on principle. In the measurements this shows up directly: static caps cost about two
points of *global* hit rate versus plain LRU (12.2 % vs 14.5 % overall) while only matching LRU's
fairness. You pay the fairness tax even when there's no contention to be fair about.

That bothered me more than the number itself. A policy that throws away capacity in the
uncontended case is solving the wrong problem: fairness only *matters* under contention, so the
mechanism should only *bind* under contention.

## Elastic floors and one knob

The 5b design keeps GDSF's value model but moves fairness into victim selection, governed by a
single parameter:

- Quotas become **floors**, not caps. Nothing is ever evicted just for being over its floor;
  eviction still only triggers on global memory watermarks. A tenant can borrow every idle byte
  in the cache. (Work-conserving by construction.)
- When the cache *is* under pressure and must choose a victim, each entry's GDSF priority is
  discounted by how far its tenant is over its floor:

```
H_eff = H / (1 + w · overage)        overage = max(0, tenant_bytes / floor − 1)
```

- `w ∈ [0, 1]` is the **fairness weight**. At `w = 0` the discount vanishes and you have pure
  GDSF. As `w` grows, entries belonging to over-floor tenants look progressively cheaper to the
  evictor, so the squeeze lands on whoever is borrowing the most — which is max-min fairness,
  approached continuously.

One property worth calling out because it kept the implementation honest: the discount is a
*per-tenant scalar*, so it never reorders entries within a tenant — each tenant's own heap order
is preserved, and victim selection stays a cheap root-peek across tenants rather than a global
re-sort. The fairness mechanism costs O(#tenants), not O(#entries).

## Sweeping the knob

Five values of `w`, three RNG seeds each, same oversubscribed three-tenant workload, plotted as
efficiency (overall hit rate) versus fairness (the worst-off tenant's hit rate):

![Efficiency vs fairness frontier](../img/fairness-frontier.png)

| Config | Overall | Min tenant |
|---|---:|---:|
| LRU baseline | 14.5 % | 10.0 % |
| GDSF + static caps | 12.2 % | 10.3 % |
| Elastic, w=0 | **20.0 %** | 1.9 % |
| Elastic, w=0.25 | 14.4 % | **12.3 %** |
| Elastic, w=0.5–1 | ~14 % | ~11–12 % |

Three findings, in increasing order of how much they surprised me.

**1. The frontier is real and you buy fairness with efficiency.** Turning the knob from 0 to 0.25
costs about six points of global hit rate and buys a 6× improvement for the worst-off tenant
(1.9 % → 12.3 %). Neither endpoint is "correct" — it genuinely depends on what the cache is for.
A batch-throughput cluster where all tenants are one team should run `w = 0` and let GDSF be
greedy. A cache with a latency SLA per tenant should run `w ≈ 0.25` and treat the six points as
the price of predictability. The interesting deliverable isn't a number, it's the *curve* — and
the argument for where your operating point belongs.

**2. Work-conserving floors Pareto-dominate static caps.** The elastic policy at `w = 0.25` is
strictly better than hard quotas on *both* axes — more efficient (14.4 % vs 12.2 %) *and* fairer
(12.3 % vs 10.3 %). This was the satisfying result: not leaving capacity idle is a free lunch.
The floor gives the same protection as a cap, but only charges for it when someone actually needs
the protection. (This result reproduced within ~1.5 points when I re-ran the same sweep on the
live 3-node AWS cluster, which was a relief — single-shard local results don't always survive
contact with a distributed deployment.)

**3. The knob saturates — fast.** I expected a smooth dial from efficiency to fairness across
`[0, 1]`. What I measured is closer to a switch: the entire transition happens between `w = 0`
and `w = 0.25`, and everything from 0.25 to 1.0 sits on a plateau. In hindsight the math says
why: the discount is multiplicative, so the moment `w · overage` is large enough to *reorder*
victims — to make an over-floor tenant's median entry rank below an at-floor tenant's worst entry
— the eviction order barely changes with more `w`. The knob's useful range is a narrow band near
zero, and `w ∈ [0.25, 1]` is mostly cosmetic. If I iterate on this, the candidate fix is an
additive rank blend instead of a multiplicative discount, which should spread the response across
the range. I noted it and didn't build it; knowing *why* the knob saturates felt like the actual
lesson.

## What I'd tell someone building one of these

- **Cost-awareness is worth it, but it weaponizes the cache against your cheapest tenant.** Any
  value-based policy starves whoever is least valuable *by construction*. If tenants matter,
  fairness can't be an afterthought bolted on as quotas; it has to live inside victim selection.
- **Make the fairness mechanism work-conserving or it will tax you in the common case.**
  Contention is the exception; design the mechanism so it's invisible until then. The
  Pareto-dominance result is the whole argument in one data point.
- **Sweep your knob before you ship it.** I almost documented `fairness_weight` as a continuous
  dial. It isn't one — it's off/on with a narrow transition band, and an operator who sets 0.7
  "for extra fairness" would be turning a knob connected to nothing. The sweep cost an afternoon
  and changed what the configuration docs should honestly say.
- **Report the corner cases as results.** The 1.9 % starvation number and the plateau are more
  useful — and frankly more interesting in interviews — than the headline 20 %.

Everything here is reproducible from the repo: the policy is ~one file behind a swappable
eviction interface, the sweep is a script, and the numbers above are committed JSON/CSV with the
plot generated from them. Links: [project repo](https://github.com/haochentSC/Distributed-KV-Cache-for-LLM-Inference),
[the 5b write-up](../benchmarks/phase5b-eviction.md), and the design ADRs
([0007](../adr/0007-multi-objective-eviction-policy.md),
[0029](../adr/0029-gdsf-cost-aware-and-static-quota-eviction.md),
[0030](../adr/0030-elastic-work-conserving-fairness-knob.md)).

---

*Caveats, because benchmarks without caveats are advertising: synthetic three-tenant workload on
an oversubscribed single shard (the distributed re-run is one seed); hit rates are block-level;
the frontier is five points coarse, deliberately — the knee between 0 and 0.25 deserves a finer
sweep I haven't done. The local table is a 3-seed mean.*
