# Learning Log

> A running, dated record of what HC learned, what broke, and what HC would redo — per phase.
> This is raw material for the eventual blog post and interview prep, so favor honesty over
> polish. Newest entries at the top within each phase.

## Template (copy for each entry)

```
### YYYY-MM-DD — <short title>
**Phase:** <n>
**What I was doing:** …
**What I learned / what broke:** …
**Why it matters / what I'd redo:** …
**Links:** ADR/PR/commit, docs
```

---

## Phase 1 — Single-node external cache

### 2026-05-24 — Architecture decided + Go project scaffolded (logic left to implement)
**Phase:** 1 (scaffolding ahead of Phase 0 completion — connector/GPU work still waits on Phase 0)
**What I was doing:** Designed the Phase 1 architecture together and scaffolded the Go project.
**What I learned / what broke:**
- Settled the keystone design: **block-wise chained hashing** (exact-key first, grows to
  longest-prefix), **per-block presence with client-side run assembly** (so the same API survives
  Phase 2 sharding), and **chunked streaming** for the multi-MB tensors (gRPC's default cap is
  4 MB). Captured in ADRs 0011–0013.
- Scaffolding builds, vets, and `gofmt`s clean; the placeholder test passes. **But `go test -race`
  fails in this environment** — the installed gcc is 32-bit `mingw32` (GCC 6.3.0) and the race
  detector needs 64-bit cgo. Plain `go test` is fine. Need a 64-bit toolchain (mingw-w64) for race
  coverage, or run race tests in WSL/Linux/CI. The pre-commit hook uses `-race`, so this must be
  resolved before the first Go commit.
**Why it matters / what I'd redo:** The per-block-presence insight is the non-obvious one — it
makes "exact-key now" and "distributed longest-prefix later" literally the same API. Toolchain
lesson: verify the `-race` path on Windows early, since the testing convention leans on it.
**Links:** ADRs 0011–0013, `docs/01-architecture.md`, `proto/kvcache/v1/kvcache.proto`.

---

## Phase 0 — Foundation

### 2026-05-24 — Local RTX 3080 reshapes GPU logistics (but not the cache design)
**Phase:** 0
**What I was doing:** Re-checked the GPU plan against my actual hardware — RTX 3080 (8 GB),
32 GB RAM, Ryzen 9 5900HX.
**What I learned / what broke:**
- The "CPU-only" rule is about the **cache's nature**, not GPU availability. The cache stores and
  ships opaque KV bytes and computes nothing — there's nothing to put on a GPU. VRAM is the scarce
  tier the cache offloads *from*; a GPU-resident cache would hold ~4× less (8 GB VRAM vs 32 GB RAM)
  and defeat the point. LMCache/Mooncake/Dynamo all build this tier on CPU too. So the design is
  unchanged.
- What *does* change: the local 3080 replaces Colab/rental for Phase 0–1 vLLM work, and makes the
  Phase 4.5 cloud GPU optional (local is fine for the single-node number; cloud still wins for the
  *distributed* headline).
- Environment: vLLM is NVIDIA/CUDA (the 5900HX's integrated Radeon is irrelevant). As of 2026 vLLM
  runs natively on Win11 *and* under WSL2; WSL2 is more battle-tested and, as a bonus, its Linux
  toolchain fixes the `go test -race` 32-bit-mingw blocker — one environment for both.
**Why it matters / what I'd redo:** Good reminder to separate "what hardware do I have" from "what
does this component actually do." The GPU question felt like it might unlock a faster cache design;
it doesn't, because the cache isn't a compute component.
**Links:** `00-project-plan.md` §1, Phase 0, Phase 4.5, Risk 2, §5 model; decisions log (Session 2).

### 2026-05-24 — Project setup + a design correction before writing any code
**Phase:** 0
**What I was doing:** Standing up the project: consolidated docs into `docs/`, added the Claude
Code config (CLAUDE.md, rules, skills, pre-commit hook), and reviewed the project plan.
**What I learned / what broke:** Reading the plan surfaced that the "fork vLLM vs. thin Python
proxy" framing is outdated — vLLM has a first-class `KVConnectorBase_V1` interface and supports
dynamic connector loading (since June 2025), so the integration can be a connector in our own
package with **no fork**. That downgrades the project's biggest risk (Risk 1) from High/High to
Low–Medium. Also confirmed Go + etcd, and re-anchored the project's novelty honestly (the
distributed layer + the cost-aware/fairness eviction policy, not "building the integration").
**Why it matters / what I'd redo:** Cheap, high-leverage to verify the integration surface
*before* committing to a Phase 1 design. Lesson: re-check fast-moving OSS assumptions in the plan
against current docs at the start of each phase.
**Links:** ADRs 0008–0010, decisions log (Session 1 rows), `00-project-plan.md` Risk 1.

<!-- Add Phase 0 reading notes (PagedAttention, LMCache, vLLM connector interface) and AWS
     onboarding notes below as you go. -->
