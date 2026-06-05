package cache

import (
	"bytes"
	"testing"
)

// spillRec records the SpillFunc calls so a test can assert what was demoted to the cold tier.
type spillRec struct {
	model   string
	hash    BlockHash
	version uint64
	tokens  []int32
	kv      []byte
}

// TestSpillOnEvict: a pressure/TTL eviction demotes the victim to the spiller with its exact
// bytes/version/tokens, but an explicit Delete (the Evict RPC path) does NOT spill — deleting a
// block means "remove it", not "move it to cold" (ADR 0027).
func TestSpillOnEvict(t *testing.T) {
	var spills []spillRec
	s := NewStore(NewLRU(), WithSpiller(func(model string, h BlockHash, v uint64, toks []int32, kv []byte) {
		spills = append(spills, spillRec{model, h, v, toks, append([]byte(nil), kv...)})
	}))

	var h1 BlockHash
	h1[0] = 1
	kv1 := bytes.Repeat([]byte{0xAA}, 1024)
	s.Put(h1, &Entry{ModelID: "m", KV: kv1, TokenIDs: []int32{10, 20}})

	// Force an eviction through the pressure path (evictOne -> evict).
	freed, ok := s.evictOne()
	if !ok || freed != int64(len(kv1)) {
		t.Fatalf("evictOne: ok=%v freed=%d want ok=true freed=%d", ok, freed, len(kv1))
	}
	if len(spills) != 1 {
		t.Fatalf("expected exactly 1 spill, got %d", len(spills))
	}
	got := spills[0]
	if got.model != "m" || got.hash != h1 || got.version != 1 ||
		len(got.tokens) != 2 || !bytes.Equal(got.kv, kv1) {
		t.Errorf("spill payload mismatch: %+v", got)
	}

	// Delete must NOT spill.
	spills = nil
	var h2 BlockHash
	h2[0] = 2
	s.Put(h2, &Entry{ModelID: "m", KV: []byte("xyz"), TokenIDs: []int32{1}})
	if !s.Delete("m", h2) {
		t.Fatal("Delete reported the block absent")
	}
	if len(spills) != 0 {
		t.Errorf("Delete should not spill, but recorded %d spill(s)", len(spills))
	}
}

// TestNoSpiller_EvictStillWorks: with no spiller installed (the default, cloud-free path), eviction
// behaves exactly as before — nothing to demote, the entry is just dropped.
func TestNoSpiller_EvictStillWorks(t *testing.T) {
	s := NewStore(NewLRU())
	var h BlockHash
	h[0] = 7
	s.Put(h, &Entry{ModelID: "m", KV: []byte("data"), TokenIDs: []int32{1}})
	freed, ok := s.evictOne()
	if !ok || freed != 4 {
		t.Fatalf("evictOne without spiller: ok=%v freed=%d want true/4", ok, freed)
	}
	if _, present := s.Peek("m", h); present {
		t.Error("entry should be gone after eviction")
	}
}
