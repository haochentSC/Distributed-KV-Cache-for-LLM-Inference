# Go concurrency pitfalls (review checklist)

Use this when reviewing HC's implementation of any concurrent component. These are the bugs that
actually show up in a cache like this.

- **Goroutine leaks.** Every goroutine needs a way to exit — a `context` that gets cancelled or a
  channel that gets closed on client disconnect. Symptom: memory grows under load; `pprof` shows
  thousands of goroutines parked in `select`. (This is a named interview beat in the plan §7.)
- **Map races.** A plain `map` under concurrent read/write panics or corrupts. Use a sharded
  mutex-protected map or `sync.Map` (sharded mutex usually wins for this workload). `-race` catches it.
- **Lost cancellation.** Pass `context.Context` down; check `ctx.Err()` in loops; don't start work
  you can't cancel. RPC handlers must respect the client's deadline.
- **Unbounded queues / no backpressure.** A write buffer with no bound turns a load spike into an
  OOM. Bound channels; shed or block deliberately.
- **`time.Sleep` as synchronization** (in code or tests) — replace with channels/WaitGroup/ctx.
- **Replication races.** "Acked" must mean a defined thing (written to primary? replica too?).
  Define it, then test the invariant under node kill.
- **Partial writes on shutdown.** Graceful drain must finish or reject in-flight requests, not drop
  them silently.

Always: `go test ./... -race`, and profile with `pprof` when memory or latency looks wrong.
