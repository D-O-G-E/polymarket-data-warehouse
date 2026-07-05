import json

from ingestion.state import StateStore


def test_round_trip(tmp_path):
    path = tmp_path / "state.json"
    s = StateStore(path)
    s.set_watermark("tok1", 1_700_000_000)
    s.mark_backfilled("tok2", 720)
    s.mark_backfilled("tok3", None)  # nothing survived, still recorded
    s.save()

    s2 = StateStore(path)
    assert s2.get_watermark("tok1") == 1_700_000_000
    assert s2.get_watermark("missing") is None
    assert s2.is_backfilled("tok2")
    assert s2.is_backfilled("tok3")
    assert not s2.is_backfilled("tok1")


def test_save_is_atomic_no_temp_left_behind(tmp_path):
    path = tmp_path / "state.json"
    s = StateStore(path)
    s.set_watermark("tok", 1)
    s.save()
    s.set_watermark("tok", 2)
    s.save()

    assert json.loads(path.read_text())["price_watermarks"]["tok"] == 2
    assert [p.name for p in tmp_path.iterdir()] == ["state.json"]


def test_fresh_store_when_file_missing(tmp_path):
    s = StateStore(tmp_path / "nope.json")
    assert s.get_watermark("x") is None
    assert not s.is_backfilled("x")
