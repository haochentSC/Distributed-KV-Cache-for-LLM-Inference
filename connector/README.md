# vLLM connector (Python) — placeholder

Built in **Phase 1, after Phase 0** (a local vLLM running + reading the `KVConnectorBase_V1`
interface). It is a custom connector in this package, loaded via vLLM's dynamic connector
mechanism — **no fork** (ADR 0008). It will:

1. Split request token IDs into fixed-size blocks and compute chained block hashes (ADR 0011).
2. `Lookup` per-block presence, assemble the longest contiguous run, `Fetch` the hit blocks, and
   `Write` missed blocks after vLLM prefills them.
3. Talk to the Go cache via the **Python** client generated from
   [`proto/kvcache/v1/kvcache.proto`](../proto/kvcache/v1/kvcache.proto).

Requires a GPU, so it is **not** built during the CPU-only phases. See
`.claude/skills/vllm-integration/SKILL.md` and `docs/04-kv-cache-and-vllm.md`.
