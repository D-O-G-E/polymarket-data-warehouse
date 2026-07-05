"""Client for the CLOB API — historical price curves.

/prices-history takes the CLOB *token id* (not the condition id) as its
`market` param, plus either startTs/endTs unix seconds or an interval
("1h".."max") — the two are mutually exclusive — and `fidelity`, the
resolution in minutes.

Two behaviors that shape the jobs (both verified empirically):

1. Retention: for resolved markets the API prunes fine-grained history
   (fidelity=60 returns nothing where fidelity=720 still works), so live
   markets must be harvested before their data decays and backfills must
   accept coarser fidelity.
2. Span cap: startTs/endTs windows longer than 15 days return silently
   EMPTY at any fidelity, and ~30+ days is a hard 400 ("interval is too
   long"). Callers must chunk windowed fetches. interval=max is exempt.
"""

from __future__ import annotations

import logging
from typing import Any

from ingestion.http_client import HttpClient

log = logging.getLogger(__name__)


class ClobClient:
    def __init__(self, http: HttpClient, base_url: str) -> None:
        self._http = http
        self._base_url = base_url.rstrip("/")

    def price_history(
        self,
        token_id: str,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
        interval: str | None = None,
        fidelity: int | None = None,
    ) -> list[dict]:
        """Return [{"t": unix_seconds, "p": price}, ...] for one token."""
        if interval is not None and (start_ts is not None or end_ts is not None):
            raise ValueError("interval and startTs/endTs are mutually exclusive")
        if interval is None and start_ts is None:
            raise ValueError("provide either interval or start_ts")

        params: dict[str, Any] = {"market": token_id}
        if interval is not None:
            params["interval"] = interval
        else:
            params["startTs"] = start_ts
            if end_ts is not None:
                params["endTs"] = end_ts
        if fidelity is not None:
            params["fidelity"] = fidelity

        data = self._http.get_json(self._base_url + "/prices-history", params)
        return data.get("history") or []
