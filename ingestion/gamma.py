"""Client for the Gamma API — Polymarket's market/event catalog.

Pagination: the plain /markets and /events endpoints cap pages at 100 rows
AND reject offsets past a few thousand ("offset too large, use
/markets/keyset for deeper pagination"), so all sweeps here use the keyset
endpoints, which paginate with an opaque after_cursor/next_cursor pair and
have no depth limit.

Payload quirk worth knowing downstream: `outcomes`, `outcomePrices` and
`clobTokenIds` arrive as JSON *strings* (e.g. '["Yes", "No"]'), not arrays.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Iterator

from ingestion.http_client import HttpClient

log = logging.getLogger(__name__)

# Hard stop against a misbehaving cursor that never terminates.
MAX_PAGES = 50_000


class GammaClient:
    def __init__(self, http: HttpClient, base_url: str, page_limit: int = 100) -> None:
        self._http = http
        self._base_url = base_url.rstrip("/")
        self._page_limit = page_limit

    def iter_markets(self, **filters: Any) -> Iterator[dict]:
        """Yield market rows matching the given Gamma query params.

        Common filters: closed="true"/"false", volume_num_min=<usd>,
        end_date_min="YYYY-MM-DD", order="volumeNum", ascending="false".
        """
        return self._iter_keyset("/markets/keyset", "markets", filters)

    def iter_events(self, **filters: Any) -> Iterator[dict]:
        return self._iter_keyset("/events/keyset", "events", filters)

    def _iter_keyset(
        self, path: str, rows_key: str, filters: dict[str, Any]
    ) -> Iterator[dict]:
        params = {k: v for k, v in filters.items() if v is not None}
        params["limit"] = self._page_limit
        url = self._base_url + path

        cursor: str | None = None
        for page_no in range(MAX_PAGES):
            if cursor is not None:
                params["after_cursor"] = cursor
            data = self._http.get_json(url, params)
            rows = data.get(rows_key) or []
            yield from rows

            next_cursor = data.get("next_cursor")
            if not rows or not next_cursor or next_cursor == cursor:
                return
            cursor = next_cursor
        log.warning("%s: stopped after MAX_PAGES=%d pages", path, MAX_PAGES)


def parse_stringified_list(value: Any) -> list | None:
    """Parse Gamma's JSON-in-a-string fields ('["Yes", "No"]') tolerantly.

    Returns None for missing/empty/malformed values instead of raising —
    old or misconfigured markets do ship broken fields, and one bad row
    must not kill a sweep.
    """
    if value is None:
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, list) else None
    return None


def yes_token_id(market: dict) -> str | None:
    """The CLOB token id of the market's first ('Yes') outcome.

    We only harvest the Yes side: for a binary market No = 1 - Yes, so
    storing both would double the API calls for zero information.
    """
    tokens = parse_stringified_list(market.get("clobTokenIds"))
    if not tokens or not tokens[0]:
        return None
    return str(tokens[0])
