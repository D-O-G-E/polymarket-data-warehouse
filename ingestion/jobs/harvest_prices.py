"""Job 2 — live price harvester: hourly prices for active markets.

This is the job the warehouse exists for. The CLOB API prunes fine-grained
history once markets resolve, so hourly points must be captured while
markets are live — the raw layer becomes the system of record for data the
source later discards.

Incremental via a per-token watermark (last landed point's timestamp).
Each fetch starts overlap_seconds before the watermark: duplicate points
are harmless (append-only raw, dbt dedupes on (token_id, t)); gaps are not.
The watermark only advances after rows are landed, so a failed run simply
refetches — the job is safe to rerun any time.
"""

from __future__ import annotations

import logging
import time
from typing import Iterator

import requests

from ingestion.clob import ClobClient
from ingestion.config import Settings
from ingestion.gamma import GammaClient, yes_token_id
from ingestion.http_client import HttpClient
from ingestion.sink import JsonlWriter, new_run_id
from ingestion.state import StateStore

log = logging.getLogger(__name__)

STATE_SAVE_EVERY = 25  # tokens between checkpoint saves


def compute_window(
    now: int,
    watermark: int | None,
    *,
    initial_lookback_s: int,
    overlap_s: int,
    max_window_s: int,
) -> tuple[int, int]:
    """(start_ts, end_ts) for the next fetch of one token."""
    if watermark is None:
        start = now - initial_lookback_s
    else:
        start = watermark - overlap_s
    start = max(start, now - max_window_s)
    return start, now


def iter_chunks(start: int, end: int, span_s: int) -> Iterator[tuple[int, int]]:
    """Split [start, end] into consecutive spans of at most span_s seconds.

    Needed because the API silently returns empty for windows over 15 days
    (see clob.py); a first fetch with a 30-day lookback must be chunked.
    """
    cursor = start
    while cursor < end:
        yield cursor, min(cursor + span_s, end)
        cursor += span_s


def run(
    settings: Settings,
    *,
    volume_floor: float | None = None,
    max_markets: int | None = None,
) -> dict:
    http = HttpClient(
        timeout=settings.request_timeout,
        rate_delay=settings.rate_delay,
        max_retries=settings.max_retries,
    )
    gamma = GammaClient(http, settings.gamma_base_url, settings.page_limit)
    clob = ClobClient(http, settings.clob_base_url)
    state = StateStore(settings.state_dir / "ingestion_state.json")
    run_id = new_run_id("harvest-prices")
    floor = volume_floor if volume_floor is not None else settings.volume_floor

    # Materialize the target list first (highest volume first) so the long
    # CLOB loop doesn't hold a Gamma cursor open.
    markets = list(
        gamma.iter_markets(
            closed="false",
            volume_num_min=floor,
            order="volumeNum",
            ascending="false",
        )
    )
    if max_markets is not None:
        markets = markets[:max_markets]
    log.info("harvesting %d active markets (volume >= %s)", len(markets), floor)

    fetched = skipped_fresh = skipped_no_token = failures = points = 0
    with JsonlWriter(
        settings.data_dir, "raw_price_history", run_id, "clob:/prices-history"
    ) as writer:
        for i, market in enumerate(markets, 1):
            token = yes_token_id(market)
            if token is None:
                skipped_no_token += 1
                log.warning(
                    "market %s (%s) has no parseable clobTokenIds; skipped",
                    market.get("id"),
                    market.get("slug"),
                )
                continue

            now = int(time.time())
            watermark = state.get_watermark(token)
            if watermark is not None and now - watermark < settings.min_refetch_seconds:
                skipped_fresh += 1
                continue

            start_ts, end_ts = compute_window(
                now,
                watermark,
                initial_lookback_s=settings.initial_lookback_days * 86_400,
                overlap_s=settings.overlap_seconds,
                max_window_s=settings.max_window_days * 86_400,
            )
            fetched += 1
            try:
                # Chunked because spans over 15 days come back silently
                # empty. Watermark advances per landed chunk, so an
                # interruption resumes where it left off.
                for chunk_start, chunk_end in iter_chunks(
                    start_ts, end_ts, settings.max_span_days * 86_400
                ):
                    history = clob.price_history(
                        token,
                        start_ts=chunk_start,
                        end_ts=chunk_end,
                        fidelity=settings.harvest_fidelity,
                    )
                    if not history:
                        # No trades in this span (or nothing retained);
                        # the watermark stays put so nothing is skipped.
                        continue
                    for point in history:
                        writer.write(
                            {
                                "token_id": token,
                                "market_id": market.get("id"),
                                "condition_id": market.get("conditionId"),
                                "t": point["t"],
                                "p": point["p"],
                                "fidelity_minutes": settings.harvest_fidelity,
                            }
                        )
                    points += len(history)
                    state.set_watermark(token, max(p["t"] for p in history))
            except requests.RequestException as exc:
                # One bad token must not kill the whole run; the watermark
                # only moved for chunks that actually landed.
                failures += 1
                log.error("market %s token %s...: %s", market.get("id"), token[:16], exc)
                continue

            if fetched % STATE_SAVE_EVERY == 0:
                state.save()
            if fetched % 100 == 0:
                log.info("progress: %d/%d markets, %d points", i, len(markets), points)

    state.save()
    summary = {
        "run_id": run_id,
        "markets_targeted": len(markets),
        "tokens_fetched": fetched,
        "skipped_fresh_watermark": skipped_fresh,
        "skipped_no_token": skipped_no_token,
        "token_failures": failures,
        "points_landed": points,
    }
    log.info("harvest-prices done: %s", summary)
    if fetched and failures > 0.1 * fetched:
        # Loud failure for the orchestrator: something systemic is wrong.
        raise RuntimeError(
            f"{failures}/{fetched} tokens failed; see error logs (run {run_id})"
        )
    return summary
