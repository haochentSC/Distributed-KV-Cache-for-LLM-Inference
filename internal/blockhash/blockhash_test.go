package blockhash

import "testing"

const bt = 4 // small block size for readable tests

func TestChain_SharedPrefixMatchesUntilDivergence(t *testing.T) {
	base := []int32{1, 2, 3, 4, 5, 6, 7, 8} // 2 blocks
	a := append(append([]int32(nil), base...), 9, 9, 9, 9)
	b := append(append([]int32(nil), base...), 7, 7, 7, 7)

	ha := Chain("m", a, bt)
	hb := Chain("m", b, bt)
	if len(ha) != 3 || len(hb) != 3 {
		t.Fatalf("want 3 blocks each, got %d and %d", len(ha), len(hb))
	}
	if ha[0] != hb[0] || ha[1] != hb[1] {
		t.Fatal("shared first two blocks must hash identically")
	}
	if ha[2] == hb[2] {
		t.Fatal("diverging third block must hash differently")
	}
}

func TestChain_ModelSeparation(t *testing.T) {
	toks := []int32{1, 2, 3, 4, 5, 6, 7, 8}
	a := Chain("model-a", toks, bt)
	b := Chain("model-b", toks, bt)
	if len(a) != 2 || len(b) != 2 {
		t.Fatalf("want 2 blocks each, got %d and %d", len(a), len(b))
	}
	for i := range a {
		if a[i] == b[i] {
			t.Fatalf("block %d must differ across models (model not folded into seed)", i)
		}
	}
}

func TestChain_PartialBlockDropped(t *testing.T) {
	full := Chain("m", []int32{1, 2, 3, 4}, bt)           // exactly 1 block
	withTail := Chain("m", []int32{1, 2, 3, 4, 5, 6}, bt) // 1 block + partial
	if len(full) != 1 || len(withTail) != 1 {
		t.Fatalf("partial trailing block must be dropped: got %d and %d", len(full), len(withTail))
	}
	if full[0] != withTail[0] {
		t.Fatal("the one full block must hash the same regardless of the dropped tail")
	}
}

func TestChain_Deterministic(t *testing.T) {
	toks := []int32{10, 20, 30, 40, 50, 60, 70, 80}
	if Chain("m", toks, bt)[0] != Chain("m", toks, bt)[0] {
		t.Fatal("Chain must be deterministic for identical inputs")
	}
}

func TestChainBlocks_BindsHashToCopiedTokenBlock(t *testing.T) {
	toks := []int32{10, 20, 30, 40, 50, 60, 70, 80}
	blocks := ChainBlocks("m", toks, bt)
	hashes := Chain("m", toks, bt)
	if len(blocks) != len(hashes) {
		t.Fatalf("ChainBlocks length = %d, Chain length = %d", len(blocks), len(hashes))
	}
	for i := range blocks {
		if blocks[i].Hash != hashes[i] {
			t.Fatalf("block %d hash mismatch", i)
		}
		want := toks[i*bt : (i+1)*bt]
		for j := range want {
			if blocks[i].TokenIDs[j] != want[j] {
				t.Fatalf("block %d token %d = %d, want %d", i, j, blocks[i].TokenIDs[j], want[j])
			}
		}
	}

	toks[0] = 999
	if blocks[0].TokenIDs[0] != 10 {
		t.Fatal("ChainBlocks must copy token IDs instead of aliasing caller input")
	}
}

func TestChain_TooFewTokens(t *testing.T) {
	if Chain("m", []int32{1, 2, 3}, bt) != nil {
		t.Fatal("fewer than one full block must return nil")
	}
	if Chain("m", []int32{1, 2, 3, 4}, 0) != nil {
		t.Fatal("blockTokens <= 0 must return nil")
	}
}
