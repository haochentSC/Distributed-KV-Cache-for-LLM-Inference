package ring

import (
	"fmt"
	"testing"
)

// TestLookupN_DistinctOrderedAndPrimaryAgrees: LookupN's contract has two halves and both
// must hold for replication to work: (1) the returned IDs are DISTINCT physical nodes (not
// repeated vnodes of the same machine — that would replicate to ourselves and defeat the
// point), and (2) result[0] equals Lookup(key) so the primary every client computes for
// reads is the same primary the primary's replicator chose for placement.
func TestLookupN_DistinctOrderedAndPrimaryAgrees(t *testing.T) {
	r := New(128)
	for _, n := range []string{"n1", "n2", "n3", "n4"} {
		r.Add(n)
	}
	for i := 0; i < 1000; i++ {
		k := key(i)
		got := r.LookupN(k, 2)
		if len(got) != 2 {
			t.Fatalf("key %d: LookupN n=2 returned %d ids", i, len(got))
		}
		if got[0] == got[1] {
			t.Fatalf("key %d: replica must be a DIFFERENT physical node, got [%q,%q]", i, got[0], got[1])
		}
		if primary := r.Lookup(k); got[0] != primary {
			t.Fatalf("key %d: LookupN[0]=%q must equal Lookup=%q", i, got[0], primary)
		}
	}
}

// TestLookupN_CapsAtNodeCount: asking for more replicas than nodes returns N entries, not a
// loop. This is what saves callers that pass rf=2 against a still-warming single-node ring.
func TestLookupN_CapsAtNodeCount(t *testing.T) {
	r := New(64)
	r.Add("only-node")
	got := r.LookupN(key(42), 5)
	if len(got) != 1 || got[0] != "only-node" {
		t.Fatalf("LookupN n=5 over 1 node = %v, want [\"only-node\"]", got)
	}
}

// TestLookupN_EmptyAndZero: defensive — empty ring or n<=0 must not panic, must return nil.
func TestLookupN_EmptyAndZero(t *testing.T) {
	r := New(16)
	if got := r.LookupN(key(0), 2); got != nil {
		t.Fatalf("empty ring LookupN = %v, want nil", got)
	}
	r.Add("a")
	if got := r.LookupN(key(0), 0); got != nil {
		t.Fatalf("n=0 LookupN = %v, want nil", got)
	}
}

// TestLookupN_RebalanceOnRemove: the invariant Sub-stage C relies on. When a node leaves
// the ring, the keys it owned must hand off to its REPLICA (the previous LookupN[1]) —
// i.e. the new primary == the old replica. Without this, "implicit promotion" wouldn't be
// implicit at all.
func TestLookupN_RebalanceOnRemove(t *testing.T) {
	r := New(128)
	for i := 0; i < 4; i++ {
		r.Add(fmt.Sprintf("n%d", i))
	}
	checked := 0
	for i := 0; i < 500 && checked < 100; i++ { // sample 100 keys
		k := key(i)
		before := r.LookupN(k, 2)
		if len(before) != 2 {
			continue
		}
		// Remove the primary; the new primary should be the old replica.
		r.Remove(before[0])
		after := r.LookupN(k, 1)
		if len(after) != 1 || after[0] != before[1] {
			t.Fatalf("key %d: after removing primary %q, new primary = %v, want old replica %q",
				i, before[0], after, before[1])
		}
		r.Add(before[0]) // restore for the next iteration
		checked++
	}
	if checked == 0 {
		t.Fatal("no keys exercised")
	}
}
