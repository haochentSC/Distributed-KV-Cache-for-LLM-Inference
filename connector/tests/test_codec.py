from kvcache_connector.codec import TensorFrame, decode_frames, encode_frames


def test_encode_decode_frames_round_trip():
    frames = [
        TensorFrame("layer.0", "torch.float16", (2, 3), 0, 0),
        TensorFrame("layer.1", "torch.float16", (4,), 0, 0),
    ]
    payloads = [b"abcdef", b"wxyz"]
    blob = encode_frames(frames, payloads, extra={"block": 3})

    got_frames, got_payloads, extra = decode_frames(blob)

    assert extra == {"block": 3}
    assert [f.name for f in got_frames] == ["layer.0", "layer.1"]
    assert [f.offset for f in got_frames] == [0, 6]
    assert [f.nbytes for f in got_frames] == [6, 4]
    assert [bytes(p) for p in got_payloads] == payloads


def test_decode_rejects_bad_magic():
    try:
        decode_frames(b"NOPE")
    except ValueError as exc:
        assert "magic" in str(exc)
    else:
        raise AssertionError("decode_frames should reject bad magic")
