---
description: Protobuf / gRPC schema conventions
paths:
  - "**/*.proto"
---

# Proto / gRPC conventions

The cache gRPC API is sketched in `docs/00-project-plan.md` §3 (component 2). Designing the API
is **[guided]** (decide the shape together); generating stubs after agreement is **[scaffold]**.

- **`prefix_hash` is opaque `bytes` (32 bytes, SHA-256).** The Go server never tokenizes; the hash
  is computed Python-side. (`docs/adr/0010`.)
- **One proto, two generated clients:** Go (synthetic load generator + shard routing) and Python
  (the vLLM `KVConnectorBase_V1` connector). Keep the proto language-neutral.
- Version explicitly: a package version in the proto path; additive changes only on a stable
  service (reserve removed field numbers).
- Methods follow the plan's surface: `Lookup`, `Fetch`, `Write`, `Evict`, `Health`. KV tensor
  payloads are large (multi-MB) — keep metadata and tensor bytes in separate messages/fields so a
  `Lookup` doesn't drag tensor data.
- Document each message/field; generated code is committed or generated via the Makefile — be
  consistent and note which in the proto's README.
