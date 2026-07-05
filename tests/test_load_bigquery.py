from pathlib import Path

from ingestion.jobs.load_bigquery import discover_pending, loaded_destination


def _touch(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("{}\n")


def test_discovers_pending_files_oldest_first_and_skips_events(tmp_path):
    _touch(tmp_path / "raw/raw_markets/dt=2026-07-05/run-b.jsonl")
    _touch(tmp_path / "raw/raw_markets/dt=2026-07-04/run-a.jsonl")
    _touch(tmp_path / "raw/raw_events/dt=2026-07-05/run-c.jsonl")
    _touch(tmp_path / "raw/raw_price_history/dt=2026-07-05/run-d.jsonl")

    pending = discover_pending(tmp_path)
    tables = [t for t, _ in pending]
    assert tables == ["raw_markets", "raw_markets", "raw_price_history"]
    # within a table: date-ordered (older partition first)
    assert pending[0][1].name == "run-a.jsonl"

    with_events = discover_pending(tmp_path, include_events=True)
    assert ("raw_events", tmp_path / "raw/raw_events/dt=2026-07-05/run-c.jsonl") in with_events


def test_loaded_destination_mirrors_raw_tree(tmp_path):
    src = tmp_path / "raw/raw_markets/dt=2026-07-05/run-a.jsonl"
    dest = loaded_destination(tmp_path, src)
    assert dest == tmp_path / "loaded/raw_markets/dt=2026-07-05/run-a.jsonl"
