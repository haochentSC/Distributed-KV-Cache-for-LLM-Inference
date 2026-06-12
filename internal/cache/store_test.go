package cache

import (
	"fmt"
	"sync"
	"testing"
)

// hashByte builds a key that varies only in the high byte (handy for single-key tests).
func hashByte(b byte) BlockHash {
	var h BlockHash
	h[31] = b
	return h
}

// uniqueHash builds a distinct key per (id, i). It varies the LOW bytes, which is what
// stripeFor reads — so these keys spread across stripes (a real concurrency test).
func uniqueHash(id, i int) BlockHash {
	var h BlockHash
	h[0] = byte(id)
	h[1] = byte(id >> 8)
	h[2] = byte(i)
	h[3] = byte(i >> 8)
	return h
}

func TestStore_PutGet(t *testing.T) {
	s := NewStore(nil)
	h := hashByte(1)
	kv := []byte("tensor-bytes")

	ver := s.Put(h, &Entry{ModelID: "llama", KV: kv, TokenIDs: []int32{1, 2, 3}})
	if ver != 1 {
		t.Fatalf("first Put version = %d, want 1", ver)
	}

	got, ok := s.Get("llama", h)
	if !ok {
		t.Fatal("Get miss after Put")
	}
	if got.Version != 1 || string(got.KV) != string(kv) {
		t.Fatalf("Get = %+v, want version 1 and KV %q", got, kv)
	}
	if got.AccessCount() != 1 {
		t.Fatalf("AccessCount after one Get = %d, want 1", got.AccessCount())
	}

	ver2 := s.Put(h, &Entry{ModelID: "llama", KV: []byte("updated")})
	if ver2 != 2 {
		t.Fatalf("second Put version = %d, want 2", ver2)
	}
	got2, ok := s.Get("llama", h)
	if !ok || string(got2.KV) != "updated" || got2.Version != 2 {
		t.Fatalf("after overwrite: got %+v, ok=%v", got2, ok)
	}
}

func TestStore_GetWrongModel(t *testing.T) {
	s := NewStore(nil)
	h := hashByte(2)
	s.Put(h, &Entry{ModelID: "model-a", KV: []byte("x")})

	if _, ok := s.Get("model-b", h); ok {
		t.Fatal("Get should miss when model_id does not match stored entry")
	}
}

// TestStore_SameHashDistinctModels is the ADR 0035 regression: tensor-parallel shard ids
// ("model#tpR/W", ADR 0032) deliberately share rank-agnostic block hashes, so entries for
// the SAME hash under DIFFERENT models must coexist — not clobber last-writer-wins. The
// Session B TP=4 run caught exactly this in the wild: only the last rank's shard survived.
func TestStore_SameHashDistinctModels(t *testing.T) {
	s := NewStore(nil)
	h := hashByte(4)
	models := []string{"qwen#tp0/4", "qwen#tp1/4", "qwen#tp2/4", "qwen#tp3/4"}

	// Every rank writes its own shard under the SAME wire hash.
	for _, m := range models {
		if ver := s.Put(h, &Entry{ModelID: m, KV: []byte("shard-of-" + m)}); ver != 1 {
			t.Fatalf("Put(%s) version = %d, want 1 (a fresh namespace, not an overwrite)", m, ver)
		}
	}

	// Every rank reads back its OWN bytes — no clobbering, no cross-shard serve.
	for _, m := range models {
		e, ok := s.Get(m, h)
		if !ok {
			t.Fatalf("Get(%s) missed; another model's write clobbered the entry", m)
		}
		if want := "shard-of-" + m; string(e.KV) != want {
			t.Fatalf("Get(%s) returned %q, want %q (another rank's shard bytes!)", m, e.KV, want)
		}
		if e.WireHash != h {
			t.Fatalf("Get(%s).WireHash = %x, want the wire hash %x", m, e.WireHash, h)
		}
	}

	// Overwriting one model's entry leaves the others' versions and bytes untouched.
	if ver := s.Put(h, &Entry{ModelID: models[0], KV: []byte("updated")}); ver != 2 {
		t.Fatalf("overwrite Put version = %d, want 2", ver)
	}
	for _, m := range models[1:] {
		if e, ok := s.Get(m, h); !ok || e.Version != 1 {
			t.Fatalf("Get(%s) after sibling overwrite: ok=%v version=%d, want ok v1", m, ok, e.Version)
		}
	}

	// Deleting one model's entry leaves the others present.
	if !s.Delete(models[1], h) {
		t.Fatal("Delete(tp1) should remove tp1's entry")
	}
	if _, ok := s.Get(models[1], h); ok {
		t.Fatal("Get(tp1) after Delete should miss")
	}
	for _, m := range []string{models[0], models[2], models[3]} {
		if _, ok := s.Get(m, h); !ok {
			t.Fatalf("Get(%s) should survive a sibling Delete", m)
		}
	}
}

func TestStore_Delete(t *testing.T) {
	s := NewStore(nil)
	h := hashByte(3)
	s.Put(h, &Entry{ModelID: "m", KV: []byte("v")})

	if s.Delete("other", h) {
		t.Fatal("Delete with wrong model should return false (entry stays)")
	}
	if !s.Delete("m", h) {
		t.Fatal("Delete existing entry should return true")
	}
	if _, ok := s.Get("m", h); ok {
		t.Fatal("Get after Delete should miss")
	}
	if s.Delete("m", h) {
		t.Fatal("Delete again should return false")
	}
}

func TestStore_PutNil(t *testing.T) {
	if v := NewStore(nil).Put(hashByte(4), nil); v != 0 {
		t.Fatalf("Put(nil) version = %d, want 0", v)
	}
}

// TestStore_PeekNoAccessRecord checks Peek reports presence without counting a reuse.
func TestStore_PeekNoAccessRecord(t *testing.T) {
	s := NewStore(nil)
	h := hashByte(5)
	s.Put(h, &Entry{ModelID: "m", KV: []byte("v")})

	e, ok := s.Peek("m", h)
	if !ok {
		t.Fatal("Peek miss on present key")
	}
	if e.AccessCount() != 0 {
		t.Fatalf("Peek must not record an access; AccessCount = %d", e.AccessCount())
	}
	if _, ok := s.Peek("other", h); ok {
		t.Fatal("Peek must miss on model mismatch")
	}
}

// TestStore_ConcurrentPutGet exercises many keys across stripes: every acked Put must be
// immediately Get-able with the right bytes. Run under -race to prove the locking.
func TestStore_ConcurrentPutGet(t *testing.T) {
	s := NewStore(nil)
	const goroutines = 32
	const perG = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				h := uniqueHash(id, i)
				model := fmt.Sprintf("model-%d", id%4)
				kv := []byte(fmt.Sprintf("kv-%d-%d", id, i))
				if ver := s.Put(h, &Entry{ModelID: model, KV: kv}); ver == 0 {
					t.Errorf("Put returned 0 for id=%d i=%d", id, i)
					return
				}
				got, ok := s.Get(model, h)
				if !ok || string(got.KV) != string(kv) {
					t.Errorf("Get miss or wrong KV id=%d i=%d ok=%v", id, i, ok)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

// TestStore_ConcurrentGetSameKey hammers ONE key from many goroutines. With a plain
// (non-atomic) counter mutated under the read lock this loses updates (and races under
// -race); the atomic counter must land exactly on the number of Gets.
func TestStore_ConcurrentGetSameKey(t *testing.T) {
	s := NewStore(nil)
	h := hashByte(7)
	s.Put(h, &Entry{ModelID: "m", KV: []byte("v")})

	const goroutines = 50
	const perG = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if _, ok := s.Get("m", h); !ok {
					t.Error("Get miss on present key")
					return
				}
			}
		}()
	}
	wg.Wait()

	got, _ := s.Get("m", h)             // one more read
	want := uint64(goroutines*perG) + 1 // + this final Get
	if got.AccessCount() != want {
		t.Fatalf("AccessCount = %d, want %d (lost updates ⇒ non-atomic counter)", got.AccessCount(), want)
	}
}
