package cache

import "testing"

// TestPutWithVersion_NewKey: the happy path — explicit version on a fresh key installs.
func TestPutWithVersion_NewKey(t *testing.T) {
	s := NewStore(nil)
	h := hashByte(1)
	got := s.PutWithVersion(h, &Entry{ModelID: "m", KV: []byte("v")}, 7)
	if got != 7 {
		t.Fatalf("PutWithVersion fresh key = %d, want 7", got)
	}
	e, ok := s.Peek("m", h)
	if !ok || e.Version != 7 || string(e.KV) != "v" {
		t.Fatalf("after PutWithVersion: e=%+v ok=%v", e, ok)
	}
}

// TestPutWithVersion_StaleDropped: the safety invariant. A delivery whose version is
// <= the locally-stored version must be DROPPED, never installed — otherwise async
// out-of-order replication could roll an entry back to an older copy.
func TestPutWithVersion_StaleDropped(t *testing.T) {
	s := NewStore(nil)
	h := hashByte(2)
	s.PutWithVersion(h, &Entry{ModelID: "m", KV: []byte("v3")}, 3)

	// A late delivery of v2 (older) must keep v3.
	got := s.PutWithVersion(h, &Entry{ModelID: "m", KV: []byte("v2-late")}, 2)
	if got != 3 {
		t.Fatalf("stale v2 should return existing v3, got %d", got)
	}
	e, _ := s.Peek("m", h)
	if string(e.KV) != "v3" {
		t.Fatalf("stale delivery clobbered live entry: KV=%q", e.KV)
	}

	// An equal-version re-delivery (idempotent retry) is also a no-op.
	got = s.PutWithVersion(h, &Entry{ModelID: "m", KV: []byte("v3-dup")}, 3)
	if got != 3 {
		t.Fatalf("duplicate v3 should return existing v3, got %d", got)
	}
	e, _ = s.Peek("m", h)
	if string(e.KV) != "v3" {
		t.Fatalf("duplicate delivery clobbered live entry: KV=%q", e.KV)
	}

	// A strictly newer version DOES install.
	got = s.PutWithVersion(h, &Entry{ModelID: "m", KV: []byte("v4")}, 4)
	if got != 4 {
		t.Fatalf("fresh v4 should install, got %d", got)
	}
	e, _ = s.Peek("m", h)
	if string(e.KV) != "v4" || e.Version != 4 {
		t.Fatalf("after v4 install: e=%+v", e)
	}
}

// TestPutWithVersion_ZeroVersionRefused: version 0 is the wire sentinel for "unset" and
// must not silently install — that would mask a primary-side bug where Version wasn't set.
func TestPutWithVersion_ZeroVersionRefused(t *testing.T) {
	s := NewStore(nil)
	h := hashByte(3)
	if v := s.PutWithVersion(h, &Entry{ModelID: "m", KV: []byte("x")}, 0); v != 0 {
		t.Fatalf("PutWithVersion(version=0) = %d, want 0", v)
	}
	if _, ok := s.Peek("m", h); ok {
		t.Fatal("version=0 must not install an entry")
	}
}
