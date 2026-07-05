"""Append-only JSONL landing zone — the raw/bronze layer.

Layout mirrors a date-partitioned warehouse table so the eventual BigQuery
load is a straight `bq load --source_format=NEWLINE_DELIMITED_JSON`:

    data/raw/<table>/dt=<YYYY-MM-DD>/<run_id>.jsonl

Records are never mutated or deleted; every row carries `_ingested_at`,
`_run_id` and `_source` so any run can be traced or replayed. Re-running a
job lands duplicates by design — deduplication is dbt's job, on the
natural keys (e.g. (token_id, t) for prices).
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import secrets
from pathlib import Path
from typing import IO, Any

log = logging.getLogger(__name__)


def new_run_id(job: str) -> str:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{job}-{stamp}-{secrets.token_hex(3)}"


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


class JsonlWriter:
    """Writes one run's rows for one table. Creates the file lazily so an
    empty run leaves no empty files behind."""

    def __init__(self, data_dir: Path, table: str, run_id: str, source: str) -> None:
        self._dir = (
            data_dir / "raw" / table
            / f"dt={dt.datetime.now(dt.timezone.utc):%Y-%m-%d}"
        )
        self._path = self._dir / f"{run_id}.jsonl"
        self._run_id = run_id
        self._source = source
        self._file: IO[str] | None = None
        self.rows_written = 0

    def write(self, record: dict[str, Any]) -> None:
        if self._file is None:
            self._dir.mkdir(parents=True, exist_ok=True)
            self._file = self._path.open("a", encoding="utf-8", newline="\n")
        row = {
            "_ingested_at": utc_now_iso(),
            "_run_id": self._run_id,
            "_source": self._source,
            **record,
        }
        self._file.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
        self._file.write("\n")
        self.rows_written += 1

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None
            log.info("landed %d rows -> %s", self.rows_written, self._path)

    def __enter__(self) -> "JsonlWriter":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
