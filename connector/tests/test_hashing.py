from kvcache_connector.hashing import chain_blocks, chain_hashes


def test_chain_blocks_shared_prefix_matches_until_divergence():
    base = [1, 2, 3, 4, 5, 6, 7, 8]
    a = base + [9, 9, 9, 9]
    b = base + [7, 7, 7, 7]

    ha = chain_hashes("m", a, 4)
    hb = chain_hashes("m", b, 4)

    assert len(ha) == 3
    assert len(hb) == 3
    assert ha[0] == hb[0]
    assert ha[1] == hb[1]
    assert ha[2] != hb[2]


def test_chain_blocks_binds_copied_tokens():
    toks = [10, 20, 30, 40, 50, 60, 70, 80]
    blocks = chain_blocks("m", toks, 4)

    assert [b.token_ids for b in blocks] == [(10, 20, 30, 40), (50, 60, 70, 80)]
    toks[0] = 999
    assert blocks[0].token_ids[0] == 10


def test_partial_block_dropped_and_model_separated():
    assert len(chain_blocks("m", [1, 2, 3, 4, 5], 4)) == 1
    assert chain_hashes("model-a", [1, 2, 3, 4], 4)[0] != chain_hashes("model-b", [1, 2, 3, 4], 4)[0]
    assert chain_blocks("m", [1, 2, 3], 4) == []
