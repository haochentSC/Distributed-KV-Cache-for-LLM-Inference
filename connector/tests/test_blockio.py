"""CPU tests for the paged-KV block I/O mechanics.

These use ordinary CPU torch tensors (no GPU, no vLLM) to lock the slice /
reassemble / round-trip semantics that the connector relies on. If torch is not
installed the module is skipped.
"""

import pytest

torch = pytest.importorskip("torch")

from kvcache_connector.blockio import (
    BlockLayout,
    apply_block,
    deserialize_into,
    extract_block,
    infer_block_axis,
    serialize_block,
)


# Stand-in for vLLM's FlashAttention V1 per-layer KV cache:
# (2 = [K, V], num_blocks, block_size, num_kv_heads, head_size).
NUM_BLOCKS = 8
BLOCK_SIZE = 4
KV_HEADS = 2
HEAD_SIZE = 3
BLOCK_AXIS = 1


def _make_kv_caches(names=("layers.0.kv", "layers.1.kv")):
    return {
        name: torch.randn(2, NUM_BLOCKS, BLOCK_SIZE, KV_HEADS, HEAD_SIZE, dtype=torch.float16)
        for name in names
    }


def test_infer_block_axis_picks_unique_match():
    shape = (2, NUM_BLOCKS, BLOCK_SIZE, KV_HEADS, HEAD_SIZE)
    assert infer_block_axis(shape, NUM_BLOCKS) == BLOCK_AXIS


def test_infer_block_axis_raises_on_ambiguity():
    # Two axes share the extent -> ambiguous, must raise rather than guess.
    with pytest.raises(ValueError):
        infer_block_axis((4, 4, 7), 4)


def test_infer_block_axis_raises_on_no_match():
    with pytest.raises(ValueError):
        infer_block_axis((2, 5, 3), NUM_BLOCKS)


def test_extract_then_apply_round_trips_one_layer():
    kv = _make_kv_caches(("only.kv",))["only.kv"]
    pid = 3
    original = kv.select(BLOCK_AXIS, pid).clone()

    slab = extract_block(kv, pid, BLOCK_AXIS)

    dst = torch.zeros_like(kv)
    apply_block(dst, pid, BLOCK_AXIS, slab)
    assert torch.equal(dst.select(BLOCK_AXIS, pid), original)
    # Only the targeted block was written.
    other = (pid + 1) % NUM_BLOCKS
    assert torch.count_nonzero(dst.select(BLOCK_AXIS, other)) == 0


def test_apply_block_rejects_shape_mismatch():
    kv = _make_kv_caches(("only.kv",))["only.kv"]
    bad = torch.zeros(2, BLOCK_SIZE, KV_HEADS, HEAD_SIZE + 1, dtype=torch.float16)
    with pytest.raises(ValueError):
        apply_block(kv, 0, BLOCK_AXIS, bad)


def test_serialize_deserialize_block_round_trips_all_layers():
    layout = BlockLayout(block_axis=BLOCK_AXIS, num_blocks=NUM_BLOCKS, block_size=BLOCK_SIZE)
    src_caches = _make_kv_caches()
    names = list(src_caches)
    pid = 5

    originals = {n: src_caches[n].select(BLOCK_AXIS, pid).clone() for n in names}
    blob = serialize_block(names, src_caches, pid, layout, extra={"hash": "deadbeef"})

    # Fresh, zeroed caches: deserialize must reconstruct exactly the one block.
    dst_caches = {n: torch.zeros_like(t) for n, t in src_caches.items()}
    extra = deserialize_into(blob, dst_caches, pid, layout)

    assert extra == {"hash": "deadbeef"}
    for n in names:
        assert torch.equal(dst_caches[n].select(BLOCK_AXIS, pid), originals[n])
        # blocks other than pid stay untouched (still zero).
        mask = torch.ones(NUM_BLOCKS, dtype=torch.bool)
        mask[pid] = False
        assert torch.count_nonzero(dst_caches[n].index_select(BLOCK_AXIS, torch.nonzero(mask).flatten())) == 0


def test_serialize_deserialize_round_trips_bfloat16():
    # vLLM's real KV dtype. numpy can't represent bf16, so this guards the dtype-agnostic
    # byte path (regression: tensor_to_frame's .numpy() crashes on bf16).
    layout = BlockLayout(block_axis=BLOCK_AXIS, num_blocks=NUM_BLOCKS, block_size=BLOCK_SIZE)
    src = {
        name: torch.randn(2, NUM_BLOCKS, BLOCK_SIZE, KV_HEADS, HEAD_SIZE).to(torch.bfloat16)
        for name in ("model.layers.0.self_attn.attn", "model.layers.1.self_attn.attn")
    }
    names = list(src)
    pid = 2
    originals = {n: src[n].select(BLOCK_AXIS, pid).clone() for n in names}

    blob = serialize_block(names, src, pid, layout)
    dst = {n: torch.zeros_like(t) for n, t in src.items()}
    deserialize_into(blob, dst, pid, layout)

    for n in names:
        assert dst[n].select(BLOCK_AXIS, pid).dtype == torch.bfloat16
        assert torch.equal(dst[n].select(BLOCK_AXIS, pid), originals[n])


def test_deserialize_rejects_unknown_layer():
    layout = BlockLayout(block_axis=BLOCK_AXIS, num_blocks=NUM_BLOCKS, block_size=BLOCK_SIZE)
    src_caches = _make_kv_caches(("layers.0.kv",))
    blob = serialize_block(["layers.0.kv"], src_caches, 0, layout)
    with pytest.raises(ValueError):
        deserialize_into(blob, {"different.name": next(iter(src_caches.values()))}, 0, layout)
