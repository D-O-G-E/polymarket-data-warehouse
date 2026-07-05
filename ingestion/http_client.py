"""One shared HTTP client: retries, backoff, timeouts, and a global throttle.

Both Polymarket services (Gamma and CLOB) are called through a single
instance so the politeness delay applies across the whole job, not per host.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

USER_AGENT = "polymarket-data-warehouse/0.1 (research ingestion; contact via github)"


class HttpClient:
    def __init__(
        self,
        *,
        timeout: float = 30.0,
        rate_delay: float = 0.25,
        max_retries: int = 5,
    ) -> None:
        self._timeout = timeout
        self._rate_delay = rate_delay
        self._last_request_at = 0.0

        retry = Retry(
            total=max_retries,
            backoff_factor=1.0,  # 0s, 1s, 2s, 4s, 8s between attempts
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session = requests.Session()
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)
        self._session.headers["User-Agent"] = USER_AGENT

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        self._throttle()
        resp = self._session.get(url, params=params, timeout=self._timeout)
        resp.raise_for_status()
        return resp.json()

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self._rate_delay:
            time.sleep(self._rate_delay - elapsed)
        self._last_request_at = time.monotonic()
