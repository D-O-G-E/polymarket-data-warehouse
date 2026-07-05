"""Job 3 — price backfill for resolved markets (rerunnable one-off).

Resolved markets have had their fine-grained history pruned server-side,
so this job walks a fidelity ladder finest-first and lands whatever
survives, tagging every row with the fidelity actually used so analyses
can filter honestly. Completed tokens are recorded in the state file;
rerunning skips them, so the sweep can be chunked with --max-markets and
resumed after any interruption.

Markets are processed in descending volume order: a bounded run covers the
most liquid (analytically most valuable) markets first.
"""

from __future__ import annotations

import itertools
import logging

import requests

from ingestion.clob import ClobClient
from ingestion.config import Settings
from ingestion.gamma import GammaClient, yes_token_id
from ingestion.http_client import HttpClient
from ingestion.sink import JsonlWriter, new_run_id
from ingestion.state import StateStore

log = logging.getLogger(__name__)

STATE_SAVE_EVERY = 10  # tokens between checkpoint saves


def run(
    settings: Settings,
    *,
    volume_floor: float | None = None,
    max_markets: int | None = None,
    end_date_min: str | None = None,
) -> dict:
    http = HttpClient(
        timeout=settings.request_timeout,
        rate_delay=settings.rate_delay,
        max_retries=settings.max_retries,
    )
    gamma = GammaClient(http, settings.gamma_base_url, settings.page_limit)
    clob = ClobClient(http, settings.clob_base_url)
    state = StateStore(settings.state_dir / "ingestion_state.json")
    run_id = new_run_id("backfill-prices")
    floor = volume_floor if volume_floor is not None else settings.volume_floor

    # Slice BEFORE materializing: --max-markets 500 fetches 5 catalog
    # pages and starts landing prices within seconds, instead of paging
    # the entire closed catalog up front.
    market_iter = gamma.iter_markets(
        closed="true",
        volume_num_min=floor,
        end_date_min=end_date_min,
        order="volumeNum",
        ascending="false",
    )
    if max_markets is not None:
        market_iter = itertools.islice(market_iter, max_markets)
    markets = list(market_iter)
    log.info(
        "backfilling %d resolved markets (volume >= %s%s)",
        len(markets),
        floor,
        f", ended >= {end_date_min}" if end_date_min else "",
    )

    done_already = attempted = empty = failures = points = 0
    fidelity_counts: dict[int, int] = {}
    with JsonlWriter(
        settings.data_dir, "raw_price_history", run_id, "clob:/prices-history"
    ) as writer:
        for i, market in enumerate(markets, 1):
            token = yes_token_id(market)
            if token is None:
                log.warning(
                    "market %s (%s) has no parseable clobTokenIds; skipped",
                    market.get("id"),
                    market.get("slug"),
                )
                continue
            if state.is_backfilled(token):
                done_already += 1
                continue

            attempted += 1
            history: list[dict] = []
            used_fidelity: int | None = None
            try:
                for fidelity in settings.fidelity_ladder:
                    history = clob.price_history(
                        token, interval="max", fidelity=fidelity
                    )
                    if history:
                        used_fidelity = fidelity
                        break
            except requests.RequestException as exc:
                # Not marked done: the next rerun retries this token.
                failures += 1
                log.error("market %s token %s...: %s", market.get("id"), token[:16], exc)
                continue

            if used_fidelity is None:
                empty += 1
                log.info(
                    "market %s (%s): no history at any fidelity",
                    market.get("id"),
                    market.get("slug"),
                )
            else:
                for point in history:
                    writer.write(
                        {
                            "token_id": token,
                            "market_id": market.get("id"),
                            "condition_id": market.get("conditionId"),
                            "t": point["t"],
                            "p": point["p"],
                            "fidelity_minutes": used_fidelity,
                        }
                    )
                points += len(history)
                fidelity_counts[used_fidelity] = fidelity_counts.get(used_fidelity, 0) + 1

            # Mark even the empty ones so reruns don't hammer dead tokens.
            state.mark_backfilled(token, used_fidelity)
            if attempted % STATE_SAVE_EVERY == 0:
                state.save()
            if attempted % 50 == 0:
                log.info("progress: %d/%d markets, %d points", i, len(markets), points)

    state.save()
    summary = {
        "run_id": run_id,
        "markets_targeted": len(markets),
        "already_done": done_already,
        "attempted": attempted,
        "no_history": empty,
        "token_failures": failures,
        "points_landed": points,
        "fidelity_used_counts": fidelity_counts,
    }
    log.info("backfill-prices done: %s", summary)
    if attempted and failures > 0.1 * attempted:
        raise RuntimeError(
            f"{failures}/{attempted} tokens failed; see error logs (run {run_id})"
        )
    return summary
