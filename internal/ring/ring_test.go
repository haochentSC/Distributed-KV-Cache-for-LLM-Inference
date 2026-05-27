package ring

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

// key makes a deterministic 32-byte routing key from an int, standing in for a real
// block_hash[0] (a SHA-256). Deterministic so tests are reproducible.
func key(i int) []byte {
	h := sha256.Sum256([]byte(fmt.Sprintf("key-%d", i)))
	return h[:]
}

// TestEmptyRing: Lookup on an empty ring returns "".
func TestEmptyRing(t *testing.T) {
	r := New(128)
	if got := r.Lookup(key(0)); got != "" {
		t.Fatalf("Lookup on empty ring = %q, want %q", got, "")
	}
}

// TestDeterministic: the same key maps to the same node on repeated lookups, and the
// mapping does not depend on the order nodes were added. This is what the fixed-seed
// vnodeHash buys us — and what a process-random seed (maphash) would break.
func TestDeterministic(t *testing.T) {
	a := New(128)
	for _, n := range []string{"n1", "n2", "n3"} {
		a.Add(n)
	}
	b := New(128)
	for _, n := range []string{"n3", "n1", "n2"} { // same set, different add order
		b.Add(n)
	}
	for i := 0; i < 1000; i++ {
		k := key(i)
		na, nb := a.Lookup(k), b.Lookup(k)
		if na != nb {
			t.Fatalf("key %d: ring A → %q, ring B → %q (add order must not matter)", i, na, nb)
		}
		if again := a.Lookup(k); again != na {
			t.Fatalf("key %d: repeated Lookup unstable (%q then %q)", i, na, again)
		}
	}
}

// TestDistribution: with enough virtual nodes, each physical node owns roughly 1/N of
// the keyspace. This is what vnodes buy us — tune the vnode count if the spread is
// too wide.
func TestDistribution(t *testing.T) {
	const nodes, samples, vnodes = 4, 100_000, 128
	r := New(vnodes)
	for i := 0; i < nodes; i++ {
		r.Add(fmt.Sprintf("n%d", i))
	}
	counts := map[string]int{}
	for i := 0; i < samples; i++ {
		counts[r.Lookup(key(i))]++
	}
	if len(counts) != nodes {
		t.Fatalf("only %d/%d nodes own any keys", len(counts), nodes)
	}
	// Calibration lesson: with 128 vnodes over 4 nodes the spread sits within roughly
	// ±15%; ±25% is a comfortable, non-flaky tolerance. Fewer vnodes ⇒ wider spread.
	ideal := float64(samples) / nodes
	lo, hi := int(0.75*ideal), int(1.25*ideal)
	for node, c := range counts {
		if c < lo || c > hi {
			t.Errorf("node %s owns %d keys, want in [%d,%d] (ideal %.0f)", node, c, lo, hi, ideal)
		}
	}
}

// TestMinimalMovement: THE property. Map K keys over N nodes, add one node, and assert
// only ~1/(N+1) of keys changed owner — versus hash%N, which would move almost all.
func TestMinimalMovement(t *testing.T) {
	const nodes, samples = 4, 100_000
	r := New(128)
	for i := 0; i < nodes; i++ {
		r.Add(fmt.Sprintf("n%d", i))
	}
	before := make([]string, samples)
	for i := 0; i < samples; i++ {
		before[i] = r.Lookup(key(i))
	}

	r.Add(fmt.Sprintf("n%d", nodes)) // grow the cluster by one

	moved := 0
	for i := 0; i < samples; i++ {
		if r.Lookup(key(i)) != before[i] {
			moved++
		}
	}
	frac := float64(moved) / float64(samples)
	ideal := 1.0 / float64(nodes+1) // ≈ 0.20: a new node should claim ~1/(N+1) of the circle
	// The teaching point: this is FAR below 1.0. hash%N would remap almost everything.
	if frac > 2*ideal {
		t.Errorf("add moved %.1f%% of keys, want near %.1f%% (≪ 100%% that hash%%N forces)", frac*100, ideal*100)
	}

	// Remove is symmetric: taking the node back out should restore the original owners
	// exactly (its keys revert to where they were before it joined).
	r.Remove(fmt.Sprintf("n%d", nodes))
	for i := 0; i < samples; i++ {
		if got := r.Lookup(key(i)); got != before[i] {
			t.Fatalf("key %d after add+remove = %q, want original %q", i, got, before[i])
		}
	}
}
