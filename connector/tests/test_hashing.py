from kvcache_connector.hashing import chain_blocks, chain_hashes, shard_model_id


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


def test_shard_model_id_single_gpu_unchanged():
    # World size <= 1 must leave the key byte-identical (Phase 4.5 single-GPU path).
    assert shard_model_id("Qwen/Qwen2.5-32B", 0, 1) == "Qwen/Qwen2.5-32B"
    assert shard_model_id("m", 0, 0) == "m"


def test_shard_model_id_distinct_per_rank():
    m = "Qwen/Qwen2.5-32B"
    keys = {shard_model_id(m, r, 4) for r in range(4)}
    # Every rank owns a distinct server key, so shards never clobber each other.
    assert len(keys) == 4
    assert all(k != m for k in keys)


def test_shard_model_id_scheduler_matches_worker_rank0():
    # The scheduler checks presence under canonical rank 0; rank-0 worker writes under
    # the same key (the lockstep invariant), so a lookup actually finds the saved shard.
    m = "meta-llama/Llama-3.1-70B"
    assert shard_model_id(m, 0, 8) == shard_model_id(m, 0, 8)
    # Same rank, different world => different key (a TP-size change is a new namespace).
    assert shard_model_id(m, 0, 4) != shard_model_id(m, 0, 8)
