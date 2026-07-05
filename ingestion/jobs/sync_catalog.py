"""Job 1 — catalog sync: land raw market and event rows from Gamma.

Each run sweeps (a) everything currently open and (b) everything that
closed within the lookback window, so resolutions are captured shortly
after they happen. Rows land append-only with full payloads; successive
runs give the dbt snapshot the metadata history (questions get edited,
end dates move — real slowly-changing dimensions).

--full sweeps the entire catalog including all closed markets/events; run
it once at project start (and again only if you need to re-land history).
"""

from __future__ import annotations

import datetime as dt
import itertools
import logging

from ingestion.config import Settings
from ingestion.gamma import GammaClient
from ingestion.http_client import HttpClient
from ingestion.sink import JsonlWriter, new_run_id

log = logging.getLogger(__name__)


def run(
    settings: Settings,
    *,
    full: bool = False,
    lookback_days: int | None = None,
    max_rows: int | None = None,
    volume_floor: float | None = None,
) -> dict:
    http = HttpClient(
        timeout=settings.request_timeout,
        rate_delay=settings.rate_delay,
        max_retries=settings.max_retries,
    )
    gamma = GammaClient(http, settings.gamma_base_url, settings.page_limit)
    run_id = new_run_id("sync-catalog")
    lookback = (
        lookback_days if lookback_days is not None else settings.closed_lookback_days
    )
    closed_since = (
        dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=lookback)
    ).strftime("%Y-%m-%d")

    # Optional scope cut for --full: the complete catalog is ~2M markets,
    # ~70% of which never traded even $10k — a floor shrinks the sweep ~5x
    # while keeping everything the analyses use. Note the differing Gamma
    # param names: markets filter on volume_num_min, events on volume_min.
    market_floor = {"volume_num_min": volume_floor} if volume_floor else {}
    event_floor = {"volume_min": volume_floor} if volume_floor else {}

    if full:
        market_sweeps = [("all", {**market_floor})]
        event_sweeps = [("all", {**event_floor})]
    else:
        market_sweeps = [
            ("open", {"closed": "false", **market_floor}),
            (
                "recently-closed",
                {"closed": "true", "end_date_min": closed_since, **market_floor},
            ),
        ]
        event_sweeps = [
            ("open", {"closed": "false", **event_floor}),
            (
                "recently-closed",
                {"closed": "true", "end_date_min": closed_since, **event_floor},
            ),
        ]

    summary: dict = {"run_id": run_id}

    with JsonlWriter(
        settings.data_dir, "raw_markets", run_id, "gamma:/markets/keyset"
    ) as mw:
        for name, filters in market_sweeps:
            rows = gamma.iter_markets(**filters)
            n = 0
            for market in itertools.islice(rows, max_rows):
                mw.write({"payload": market})
                n += 1
            log.info("markets sweep %r: %d rows", name, n)
            summary[f"markets_{name}"] = n

    with JsonlWriter(
        settings.data_dir, "raw_events", run_id, "gamma:/events/keyset"
    ) as ew:
        for name, filters in event_sweeps:
            rows = gamma.iter_events(**filters)
            n = 0
            for event in itertools.islice(rows, max_rows):
                ew.write({"payload": event})
                n += 1
            log.info("events sweep %r: %d rows", name, n)
            summary[f"events_{name}"] = n

    log.info("sync-catalog done: %s", summary)
    return summary
