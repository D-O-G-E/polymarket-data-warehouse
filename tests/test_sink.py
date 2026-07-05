import json

from ingestion.sink import JsonlWriter, new_run_id


def test_writes_partitioned_jsonl_with_metadata(tmp_path):
    run_id = new_run_id("testjob")
    with JsonlWriter(tmp_path, "raw_markets", run_id, "gamma:/markets/keyset") as w:
        w.write({"payload": {"id": "1", "question": "A?"}})
        w.write({"payload": {"id": "2", "question": "B?"}})
    assert w.rows_written == 2

    files = list(tmp_path.glob("raw/raw_markets/dt=*/*.jsonl"))
    assert len(files) == 1
    assert run_id in files[0].name

    rows = [json.loads(line) for line in files[0].read_text("utf-8").splitlines()]
    assert len(rows) == 2
    for row in rows:
        assert row["_run_id"] == run_id
        assert row["_source"] == "gamma:/markets/keyset"
        assert row["_ingested_at"].endswith("+00:00")
    assert rows[0]["payload"]["id"] == "1"


def test_empty_run_leaves_no_file(tmp_path):
    with JsonlWriter(tmp_path, "raw_markets", new_run_id("t"), "src"):
        pass
    assert list(tmp_path.rglob("*.jsonl")) == []


def test_run_ids_are_unique_and_job_tagged():
    a, b = new_run_id("harvest-prices"), new_run_id("harvest-prices")
    assert a != b
    assert a.startswith("harvest-prices-")
