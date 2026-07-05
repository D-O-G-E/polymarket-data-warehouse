"""Command-line entrypoint: python -m ingestion <job> [options]."""

from __future__ import annotations

import argparse
import dataclasses
import logging
from pathlib import Path

from ingestion.config import Settings
from ingestion.jobs import backfill_prices, harvest_prices, load_bigquery, sync_catalog


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ingestion",
        description="Polymarket ingestion jobs: land raw catalog and price data as JSONL.",
    )
    parser.add_argument("--data-dir", type=Path, help="landing zone root (default: ./data)")
    parser.add_argument("--state-dir", type=Path, help="watermark/state root (default: ./state)")
    parser.add_argument("--rate-delay", type=float, help="min seconds between API requests")
    parser.add_argument("-v", "--verbose", action="store_true")

    sub = parser.add_subparsers(dest="job", required=True)

    p = sub.add_parser(
        "sync-catalog",
        help="land raw market/event rows: everything open + recently closed",
    )
    p.add_argument(
        "--full",
        action="store_true",
        help="sweep the ENTIRE catalog incl. all closed markets (initial load)",
    )
    p.add_argument(
        "--lookback-days",
        type=int,
        help="re-land markets/events closed within this many days (default 14)",
    )
    p.add_argument("--max-rows", type=int, help="cap rows per sweep (smoke tests)")
    p.add_argument(
        "--volume-floor",
        type=float,
        help="only markets/events with at least this lifetime volume USD "
        "(recommended 10000 with --full: the complete catalog is ~2M rows, "
        "mostly zero-volume)",
    )

    p = sub.add_parser(
        "harvest-prices",
        help="incremental hourly prices for active markets above the volume floor",
    )
    p.add_argument("--volume-floor", type=float, help="min lifetime volume USD (default 10000)")
    p.add_argument("--max-markets", type=int, help="cap number of markets (smoke tests)")

    p = sub.add_parser(
        "backfill-prices",
        help="one-off, rerunnable coarse history for resolved markets",
    )
    p.add_argument("--volume-floor", type=float, help="min lifetime volume USD (default 10000)")
    p.add_argument("--max-markets", type=int, help="process at most N markets this run")
    p.add_argument("--end-date-min", help="only markets that ended on/after this date (YYYY-MM-DD)")

    p = sub.add_parser(
        "load-bigquery",
        help="load pending JSONL files into BigQuery raw tables, then archive them to data/loaded/",
    )
    p.add_argument("--project", help="GCP project id (or set PDW_BQ_PROJECT)")
    p.add_argument("--dataset", help="raw dataset name (default polymarket_raw)")
    p.add_argument(
        "--include-events",
        action="store_true",
        help="also load raw_events (large and redundant; off by default for the sandbox storage cap)",
    )
    p.add_argument("--max-files", type=int, help="load at most N files this run")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Quiet the retry chatter unless -v.
    if not args.verbose:
        logging.getLogger("urllib3").setLevel(logging.WARNING)

    settings = Settings.from_env()
    overrides = {}
    if args.data_dir is not None:
        overrides["data_dir"] = args.data_dir
    if args.state_dir is not None:
        overrides["state_dir"] = args.state_dir
    if args.rate_delay is not None:
        overrides["rate_delay"] = args.rate_delay
    if overrides:
        settings = dataclasses.replace(settings, **overrides)

    if args.job == "sync-catalog":
        sync_catalog.run(
            settings,
            full=args.full,
            lookback_days=args.lookback_days,
            max_rows=args.max_rows,
            volume_floor=args.volume_floor,
        )
    elif args.job == "harvest-prices":
        harvest_prices.run(
            settings,
            volume_floor=args.volume_floor,
            max_markets=args.max_markets,
        )
    elif args.job == "backfill-prices":
        backfill_prices.run(
            settings,
            volume_floor=args.volume_floor,
            max_markets=args.max_markets,
            end_date_min=args.end_date_min,
        )
    elif args.job == "load-bigquery":
        load_bigquery.run(
            settings,
            project=args.project,
            dataset=args.dataset,
            include_events=args.include_events,
            max_files=args.max_files,
        )
    return 0
