package main

import "testing"

// The correctness oracle is only as trustworthy as fillVerifiable: if it weren't deterministic,
// the chaos run would flag false violations; if two different hashes produced the same bytes, a
// mis-served block could slip through. These tests pin both properties.

// TestFillVerifiable_Deterministic: the same hash always yields the same bytes (so a reader can
// regenerate and compare).
func TestFillVerifiable_Deterministic(t *testing.T) {
	var h [32]byte
	for i := range h {
		h[i] = byte(i * 7)
	}
	a := make([]byte, 4096+5) // include a non-8-multiple tail to exercise the trailing loop
	b := make([]byte, len(a))
	fillVerifiable(a, h)
	fillVerifiable(b, h)
	if !equalBytes(a, b) {
		t.Fatal("fillVerifiable is not deterministic for a fixed hash")
	}
	// Not all-zero (the seed=0 guard and the multiply ensure real content).
	allZero := true
	for _, x := range a {
		if x != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		t.Fatal("fillVerifiable produced all zeros")
	}
}

// TestFillVerifiable_DistinctHashes: different hashes produce different content, so "served the
// wrong block" surfaces as a byte mismatch.
func TestFillVerifiable_DistinctHashes(t *testing.T) {
	var h1, h2 [32]byte
	h1[0], h2[0] = 1, 2 // differ in a single byte
	a := make([]byte, 1024)
	b := make([]byte, 1024)
	fillVerifiable(a, h1)
	fillVerifiable(b, h2)
	if equalBytes(a, b) {
		t.Fatal("two different hashes produced identical content — the oracle could miss a mis-served block")
	}
}

// TestFillVerifiable_ZeroHash: the all-zero hash must not hit the xorshift fixed point (which
// would emit all zeros and weaken the oracle).
func TestFillVerifiable_ZeroHash(t *testing.T) {
	var zero [32]byte
	buf := make([]byte, 256)
	fillVerifiable(buf, zero)
	for _, x := range buf {
		if x != 0 {
			return // good: produced non-zero content despite the zero seed
		}
	}
	t.Fatal("zero hash produced all-zero content (xorshift fixed point not guarded)")
}
