package coldtier

import (
	"bytes"
	"context"
	"testing"
)

// The cold tier is only correct if a Spilled block comes back BYTE-IDENTICAL and under the same
// version/tokens — otherwise read-through would serve a mangled block and break ADR 0016. These
// tests pin the framing round-trip and the Memory tier's key isolation.

func TestEncodeDecode_RoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		version uint64
		tokens  []int32
		kv      []byte
	}{
		{"typical", 7, []int32{1, 2, 3, 4}, bytes.Repeat([]byte{0xAB}, 4096)},
		{"empty kv", 1, []int32{9}, nil},
		{"no tokens", 42, nil, []byte("hello")},
		{"empty both", 0, nil, nil},
		{"negative-looking token", 3, []int32{-1, 2147483647}, []byte{0x00, 0xFF}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			blob := encode(c.version, c.tokens, c.kv)
			gotV, gotToks, gotKV, err := decode(blob)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if gotV != c.version {
				t.Errorf("version: got %d want %d", gotV, c.version)
			}
			if len(gotToks) != len(c.tokens) {
				t.Fatalf("tokens len: got %d want %d", len(gotToks), len(c.tokens))
			}
			for i := range c.tokens {
				if gotToks[i] != c.tokens[i] {
					t.Errorf("token[%d]: got %d want %d", i, gotToks[i], c.tokens[i])
				}
			}
			if !bytes.Equal(gotKV, c.kv) {
				t.Errorf("kv mismatch: got %d bytes want %d", len(gotKV), len(c.kv))
			}
		})
	}
}

// A truncated or foreign blob must error (so it's served as a miss), never decode to bogus
// tokens/bytes — that's the line between a miss and a correctness violation.
func TestDecode_RejectsGarbage(t *testing.T) {
	if _, _, _, err := decode([]byte("XX")); err == nil {
		t.Error("decode accepted a too-short blob")
	}
	if _, _, _, err := decode([]byte("NOPExxxxxxxxxxxx")); err == nil {
		t.Error("decode accepted a bad magic")
	}
	// Valid header claiming 100 tokens but no token bytes following.
	bad := encode(1, nil, nil)
	bad[12] = 100 // overwrite nTokens to 100
	if _, _, _, err := decode(bad); err == nil {
		t.Error("decode accepted a token count overrunning the object")
	}
}

func TestMemoryTier_SpillGetMiss(t *testing.T) {
	tier := NewMemory()
	defer tier.Close()
	var h1, h2 [32]byte
	h1[0], h2[0] = 1, 2
	kv := []byte("kv-bytes-for-h1")

	// Miss before spill.
	if _, _, _, ok, _ := tier.Get(context.Background(), "m", h1); ok {
		t.Fatal("expected miss before spill")
	}
	tier.Spill("m", h1, 5, []int32{1, 2}, kv)

	gotKV, gotV, gotToks, ok, err := tier.Get(context.Background(), "m", h1)
	if err != nil || !ok {
		t.Fatalf("expected cold hit, got ok=%v err=%v", ok, err)
	}
	if gotV != 5 || len(gotToks) != 2 || !bytes.Equal(gotKV, kv) {
		t.Errorf("round-trip mismatch: v=%d toks=%v kv=%q", gotV, gotToks, gotKV)
	}
	// A different hash must NOT collide with h1's object.
	if _, _, _, ok, _ := tier.Get(context.Background(), "m", h2); ok {
		t.Error("h2 should be a miss — keys must be isolated by hash")
	}
	// A different model under the same hash is also a distinct key.
	if _, _, _, ok, _ := tier.Get(context.Background(), "other", h1); ok {
		t.Error("(other,h1) should be a miss — keys must be isolated by model")
	}
}
