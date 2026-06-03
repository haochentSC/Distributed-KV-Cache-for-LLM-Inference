package cache

import (
	"context"
	"time"
)

// Evictor is the background memory-pressure + TTL loop for one Store. There is exactly one per
// cache-server; start it once with `go ev.Run(ctx)` and it stops when ctx is cancelled (the
// same shutdown ctx main already threads through the replicator).
//
// Two triggers, one goroutine:
//   - PRESSURE (watermark): Store.signalEvict nudges EvictSignal when a write crosses the
//     high-water mark. On a nudge, free entries down to the low-water mark. The high/low gap is
//     hysteresis — it stops us from evict-one/write-one thrashing on every byte over the line.
//   - TTL (ticker): every sweepEvery, drop entries idle longer than ttl, regardless of pressure.
type Evictor struct {
	store      *Store
	ttl        time.Duration // idle TTL; 0 disables TTL sweeping
	sweepEvery time.Duration // how often the TTL sweep runs
}

// NewEvictor builds an Evictor. ttl <= 0 disables the TTL sweep (pressure eviction still runs).
// sweepEvery should be > 0 when ttl is set; a sane default is 30s.
func NewEvictor(store *Store, ttl, sweepEvery time.Duration) *Evictor {
	return &Evictor{store: store, ttl: ttl, sweepEvery: sweepEvery}
}

// Run is the loop. It blocks until ctx is cancelled. Start it in a goroutine.
func (e *Evictor) Run(ctx context.Context) {
	// A ticker for the TTL sweep. When ttl is disabled we still want a valid ticker to select
	// on (simplest is to create one and just not act, or guard with sweepEvery); keep it simple.
	ticker := time.NewTicker(e.sweepInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-e.store.EvictSignal():
			// PRESSURE drain: a write crossed the high-water mark. Free down to the low-water
			// mark — the hi/lo gap is the hysteresis that stops evict-one/write-one thrashing.
			for e.store.Bytes() > e.store.LowWater() {
				if _, ok := e.store.evictOne(); !ok {
					break // policy ran dry (empty / all pinned); break or we spin this goroutine hot
				}
			}

		case <-ticker.C:
			// TTL sweep: drop entries idle longer than ttl, regardless of pressure. No-op when
			// ttl <= 0 (sweepIdle guards it), so this case is harmless even with TTL disabled.
			e.store.sweepIdle(e.ttl)
		}
	}
}

// sweepInterval returns a safe ticker period even when TTL sweeping is off, so Run can always
// build a valid ticker. When disabled we pick a long, harmless interval.
func (e *Evictor) sweepInterval() time.Duration {
	if e.ttl <= 0 || e.sweepEvery <= 0 {
		return time.Hour // effectively idle; the ticker case is a no-op sweepIdle anyway
	}
	return e.sweepEvery
}
