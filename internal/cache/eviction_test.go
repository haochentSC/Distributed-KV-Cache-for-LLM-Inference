package cache

import (
	"testing"
	"time"
)

// These tests are the red→green target for the Sub-stage A TODO(hc) bodies. They will FAIL
// against the current stubs (which return zero values / don't account bytes) and pass once the
// LRU ops, byte accounting, evict, and evictOne are implemented. Run with -race in WSL2.

// TestLRU_Order pins the recency contract of LRUPolicy in isolation (no Store):
// least-recently-USED is the victim; a read (RecordAccess) rescues an entry from the back.
func TestLRU_Order(t *testing.T) {
	p := NewLRU()
	a, b, c := hashByte(1), hashByte(2), hashByte(3)

	p.RecordWrite(a, 10)
	p.RecordWrite(b, 10)
	p.RecordWrite(c, 10) // recency front→back: c, b, a  (a is oldest)

	if got, ok := p.Victim(); !ok || got != a {
		t.Fatalf("Victim after 3 writes = %x ok=%v, want %x (oldest)", got, ok, a)
	}

	p.RecordAccess(a) // a is now most-recently-used → b becomes the victim
	if got, ok := p.Victim(); !ok || got != b {
		t.Fatalf("Victim after touching a = %x ok=%v, want %x", got, ok, b)
	}

	p.RecordEvict(b)
	if got, ok := p.Victim(); !ok || got != c {
		t.Fatalf("Victim after evicting b = %x ok=%v, want %x", got, ok, c)
	}

	p.RecordEvict(c)
	p.RecordEvict(a)
	if _, ok := p.Victim(); ok {
		t.Fatal("Victim on empty policy should report ok=false")
	}
}

// TestStore_ByteAccounting checks totalBytes tracks the live resident size across the write
// (first + overwrite, both directions) and delete paths.
func TestStore_ByteAccounting(t *testing.T) {
	s := NewStore(NewLRU(), WithMaxBytes(1<<20))
	h := hashByte(1)

	s.Put(h, &Entry{ModelID: "m", KV: make([]byte, 100)})
	if got := s.Bytes(); got != 100 {
		t.Fatalf("Bytes after 100B write = %d, want 100", got)
	}
	s.Put(h, &Entry{ModelID: "m", KV: make([]byte, 250)}) // overwrite larger: delta +150
	if got := s.Bytes(); got != 250 {
		t.Fatalf("Bytes after overwrite to 250B = %d, want 250", got)
	}
	s.Put(h, &Entry{ModelID: "m", KV: make([]byte, 50)}) // overwrite smaller: delta -200
	if got := s.Bytes(); got != 50 {
		t.Fatalf("Bytes after overwrite to 50B = %d, want 50", got)
	}
	if !s.Delete("m", h) {
		t.Fatal("Delete should report true")
	}
	if got := s.Bytes(); got != 0 {
		t.Fatalf("Bytes after delete = %d, want 0", got)
	}
}

// TestStore_EvictToLowWater is the watermark invariant: after filling past the bound and
// draining, resident size is ≤ low-water and the evicted blocks are exactly the
// least-recently-used ones (here, the first written, since none were re-accessed).
//
// It drives eviction by calling evictOne directly in a loop — the same thing the background
// Evictor does on a pressure signal, but deterministic (no goroutine/timing in the test).
func TestStore_EvictToLowWater(t *testing.T) {
	// max 1000B, low-water 500B. 10 × 100B writes fill to exactly max.
	s := NewStore(NewLRU(), WithMaxBytes(1000), WithWatermarks(0.90, 0.50))

	keys := make([]BlockHash, 10)
	for i := range keys {
		keys[i] = uniqueHash(0, i)
		s.Put(keys[i], &Entry{ModelID: "m", KV: make([]byte, 100)})
	}
	if got := s.Bytes(); got != 1000 {
		t.Fatalf("Bytes after filling = %d, want 1000", got)
	}

	// Drain to low-water, exactly as Evictor.Run's pressure case will.
	for s.Bytes() > s.LowWater() {
		if _, ok := s.evictOne(); !ok {
			t.Fatal("evictOne reported nothing to evict while still over low-water")
		}
	}

	if got := s.Bytes(); got > s.LowWater() {
		t.Fatalf("Bytes after drain = %d, want ≤ low-water %d", got, s.LowWater())
	}
	if got := s.Len(); got != 5 {
		t.Fatalf("entries after drain = %d, want 5", got)
	}
	// The 5 oldest (keys[0..4]) should be gone; the 5 newest (keys[5..9]) should remain.
	for i := 0; i < 5; i++ {
		if _, ok := s.Peek("m", keys[i]); ok {
			t.Errorf("key[%d] (LRU) should have been evicted", i)
		}
	}
	for i := 5; i < 10; i++ {
		if _, ok := s.Peek("m", keys[i]); !ok {
			t.Errorf("key[%d] (recent) should have survived", i)
		}
	}
}

// TestStore_SignalsOnPressure pins the pressure TRIGGER (signalEvict) deterministically, without
// the background goroutine: a write below the high-water mark must NOT nudge the Evictor, and the
// write that crosses it MUST. (The drain that follows the nudge is proven by EvictToLowWater.)
func TestStore_SignalsOnPressure(t *testing.T) {
	// max 1000B, hi-water 900B. 100B writes: the 9th brings us to exactly 900 → first nudge.
	s := NewStore(NewLRU(), WithMaxBytes(1000), WithWatermarks(0.90, 0.50))

	for i := 0; i < 8; i++ { // 800B total, below the 900B high-water mark
		s.Put(uniqueHash(0, i), &Entry{ModelID: "m", KV: make([]byte, 100)})
	}
	select {
	case <-s.EvictSignal():
		t.Fatal("Evictor was signalled below the high-water mark")
	default: // expected: no pending nudge
	}

	s.Put(uniqueHash(0, 8), &Entry{ModelID: "m", KV: make([]byte, 100)}) // 900B == high-water
	select {
	case <-s.EvictSignal(): // expected: exactly one coalesced nudge is pending
	default:
		t.Fatal("crossing the high-water mark did not signal the Evictor")
	}
}

// TestStore_SweepIdle is the TTL invariant: after a sweep, no entry idle longer than ttl survives,
// and a recently-touched entry does. The Store's clock seam (s.now) makes this deterministic — no
// time.Sleep, per .claude/rules/go-testing.
func TestStore_SweepIdle(t *testing.T) {
	s := NewStore(NewLRU())
	base := time.Unix(1_700_000_000, 0) // fixed instant; all three writes are stamped here
	s.now = func() time.Time { return base }

	old1, old2, fresh := hashByte(1), hashByte(2), hashByte(3)
	for _, h := range []BlockHash{old1, old2, fresh} {
		s.Put(h, &Entry{ModelID: "m", KV: make([]byte, 100)})
	}

	if n := s.sweepIdle(0); n != 0 { // ttl <= 0 is always a no-op
		t.Fatalf("sweepIdle(0) evicted %d, want 0 (disabled)", n)
	}

	// Advance the clock 10m and record a read on `fresh` at the new now, so its last-access is
	// recent while old1/old2 remain stamped at base.
	s.now = func() time.Time { return base.Add(10 * time.Minute) }
	if _, ok := s.Get("m", fresh); !ok {
		t.Fatal("Get(fresh) missed")
	}

	// Evict everything idle > 5m: old1/old2 (idle 10m) go; fresh (idle 0m) stays.
	if n := s.sweepIdle(5 * time.Minute); n != 2 {
		t.Fatalf("sweepIdle evicted %d, want 2", n)
	}
	if _, ok := s.Peek("m", fresh); !ok {
		t.Error("fresh entry (recently read) should have survived the sweep")
	}
	for _, h := range []BlockHash{old1, old2} {
		if _, ok := s.Peek("m", h); ok {
			t.Errorf("idle entry %x should have been swept", h[0])
		}
	}
	if got := s.Bytes(); got != 100 {
		t.Fatalf("Bytes after sweep = %d, want 100 (only fresh remains)", got)
	}
}
