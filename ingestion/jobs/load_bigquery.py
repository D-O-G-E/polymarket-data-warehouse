"""Job 4 — load landed JSONL files into BigQuery raw tables.

Load-state lives in the filesystem: a file under data/raw/ is pending; on
successful load it moves to data/loaded/ (same relative path). No
bookkeeping database, obvious at a glance, resumable after any crash. If
a crash lands between load and move, the rerun loads the file again —
duplicates, as everywhere in this pipeline, are dbt's problem, not ours.

Schema strategy: catalog payloads go into a native JSON column instead of
hundreds of autodetected columns. Gamma adds/renames fields freely;
autodetect would eventually break a 3am load over a field we don't even
use. A JSON column cannot break, and dbt parses out the ~20 fields the
models need. Price rows are flat and stable, so they get real columns.

raw_events is opt-in (--include-events): event payloads embed full copies
of their markets, so the table is ~4x the markets table for mostly
redundant bytes — a real cost against the BigQuery sandbox's 10 GiB
storage cap and not needed by any current model.
"""

from __future__ import annotations

import logging
from pathlib import Path

from ingestion.config import Settings

log = logging.getLogger(__name__)

# (table, field list) — metadata columns first, mirroring the JSONL rows.
_META = [
    ("_ingested_at", "TIMESTAMP"),
    ("_run_id", "STRING"),
    ("_source", "STRING"),
]
TABLE_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "raw_markets": _META + [("payload", "JSON")],
    "raw_events": _META + [("payload", "JSON")],
    "raw_price_history": _META
    + [
        ("token_id", "STRING"),
        ("market_id", "STRING"),
        ("condition_id", "STRING"),
        ("t", "INTEGER"),
        ("p", "FLOAT"),
        ("fidelity_minutes", "INTEGER"),
    ],
}


def discover_pending(
    data_dir: Path, *, include_events: bool = False
) -> list[tuple[str, Path]]:
    """(table, file) pairs awaiting load, oldest first for stable ordering."""
    pending: list[tuple[str, Path]] = []
    for table in TABLE_SCHEMAS:
        if table == "raw_events" and not include_events:
            continue
        for f in sorted((data_dir / "raw" / table).glob("dt=*/*.jsonl")):
            pending.append((table, f))
    return pending


def loaded_destination(data_dir: Path, file: Path) -> Path:
    """Mirror data/raw/<x> to data/loaded/<x>."""
    return data_dir / "loaded" / file.relative_to(data_dir / "raw")


def run(
    settings: Settings,
    *,
    project: str | None = None,
    dataset: str | None = None,
    include_events: bool = False,
    max_files: int | None = None,
) -> dict:
    # Imported here so the base package works without the bigquery extra.
    from google.cloud import bigquery

    project = project or settings.bq_project
    if not project:
        raise SystemExit(
            "BigQuery project required: pass --project or set PDW_BQ_PROJECT"
        )
    dataset_id = dataset or settings.bq_dataset
    client = bigquery.Client(project=project)

    ds_ref = bigquery.Dataset(f"{project}.{dataset_id}")
    ds_ref.location = settings.bq_location
    client.create_dataset(ds_ref, exists_ok=True)

    pending = discover_pending(settings.data_dir, include_events=include_events)
    if max_files is not None:
        pending = pending[:max_files]
    log.info("%d files pending load into %s.%s", len(pending), project, dataset_id)

    loaded = failed = rows = 0
    for table, file in pending:
        table_ref = f"{project}.{dataset_id}.{table}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=[
                bigquery.SchemaField(name, type_) for name, type_ in TABLE_SCHEMAS[table]
            ],
            # New metadata fields in future rows must not break loads.
            ignore_unknown_values=True,
            time_partitioning=bigquery.TimePartitioning(field="_ingested_at"),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        try:
            with file.open("rb") as fh:
                job = client.load_table_from_file(fh, table_ref, job_config=job_config)
            job.result()  # wait; raises on failure
        except Exception as exc:  # keep going; summary + exit code report it
            failed += 1
            log.error("load failed for %s: %s", file, exc)
            continue

        dest = loaded_destination(settings.data_dir, file)
        dest.parent.mkdir(parents=True, exist_ok=True)
        file.rename(dest)
        loaded += 1
        rows += job.output_rows or 0
        log.info("loaded %s rows from %s -> %s", job.output_rows, file.name, table)

    summary = {"files_loaded": loaded, "files_failed": failed, "rows_loaded": rows}
    log.info("load-bigquery done: %s", summary)
    if failed:
        raise RuntimeError(f"{failed} file(s) failed to load; see error logs")
    return summary
