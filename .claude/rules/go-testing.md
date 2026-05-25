---
description: Go test conventions
paths:
  - "**/*_test.go"
---

# Go testing conventions

- **Always run with the race detector:** `go test ./... -race`. Concurrency is the core of this
  project — a passing test without `-race` proves little.
- **Table-driven tests** for anything with multiple cases; name subtests with `t.Run`.
- **No `time.Sleep` for synchronization.** Use channels, `sync.WaitGroup`, `context` deadlines, or
  `testing/synctest`-style techniques. Sleeps make tests flaky and hide races.
- Prefer real in-process components over heavy mocks for the cache logic; mock only external
  boundaries (etcd, AWS) behind small interfaces.
- For distributed behavior (replication, failover, eviction), assert the **invariant** (e.g. "zero
  correctness violations", "every acked write is fetchable from primary or replica"), not just a
  happy path.
- Keep tests deterministic: seed any randomness and log the seed.
- Writing the interesting test *logic* is often [guided]; test scaffolding/boilerplate is [scaffold].
