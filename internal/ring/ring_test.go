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
	_ = r // TODO(hc): assert r.Lookup(key(0)) == "" (then drop this line).
}

// TestDeterministic: the same key maps to the same node on repeated lookups, and the
// mapping does not depend on the order nodes were added.
func TestDeterministic(t *testing.T) {
	// TODO(hc):
	//   - build ring A by Add("n1"),Add("n2"),Add("n3"); build ring B in a different
	//     add order; assert Lookup(key(i)) agrees between A and B for many i.
	//   - assert repeated Lookup of the same key is stable.
}

// TestDistribution: with enough virtual nodes, each physical node owns roughly 1/N of
// the keyspace. This is what vnodes buy us — tune the vnode count if the spread is
// too wide.
func TestDistribution(t *testing.T) {
	t.Skip("TODO(hc): enable after Ring.Add/Lookup are implemented")
	const nodes, samples = 4, 100_000
	r := New(128)
	for i := 0; i < nodes; i++ {
		r.Add(fmt.Sprintf("n%d", i))
	}
	counts := map[string]int{}
	for i := 0; i < samples; i++ {
		counts[r.Lookup(key(i))]++
	}
	_ = counts
	// TODO(hc): assert every node's share is within a tolerance of 1/nodes
	//   (e.g. each count in [0.8, 1.2] * samples/nodes). Loosen/tighten the tolerance
	//   against what 128 vnodes actually delivers; that calibration is the lesson.
}

// TestMinimalMovement: THE property. Map K keys over N nodes, add one node, and assert
// only ~1/(N+1) of keys changed owner — versus hash%N, which would move almost all.
func TestMinimalMovement(t *testing.T) {
	t.Skip("TODO(hc): enable after Ring.Add/Lookup are implemented")
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

	// TODO(hc):
	//   - recompute owners after the Add; count how many differ from `before`.
	//   - assert the moved fraction is near 1/(nodes+1) (allow generous slack, e.g.
	//     < 2x the ideal). The teaching point: this is FAR below 1.0, which is what
	//     hash%N would force. Optionally also test Remove symmetrically.
}
