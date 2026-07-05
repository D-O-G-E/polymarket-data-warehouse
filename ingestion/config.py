"""Runtime settings for the ingestion jobs.

Every knob can be overridden with a PDW_* environment variable so the same
code runs unchanged on a laptop, in Docker, and in CI. CLI flags override
environment values where a flag exists.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    clob_base_url: str = "https://clob.polymarket.com"

    data_dir: Path = field(default_factory=lambda: Path("data"))
    state_dir: Path = field(default_factory=lambda: Path("state"))

    # Gamma hard-caps pages at 100 rows regardless of the limit param.
    page_limit: int = 100
    request_timeout: float = 30.0
    # Minimum seconds between HTTP requests; the endpoints are public and
    # unauthenticated, so throttle politely.
    rate_delay: float = 0.25
    max_retries: int = 5

    # --- harvest-prices ---
    # Only harvest markets with at least this much lifetime volume (USD).
    # Deliberate scope choice: thin markets add API load but too few price
    # points to support the calibration analysis.
    volume_floor: float = 10_000.0
    # Price resolution in minutes. 60 = hourly, the finest grain the API
    # retains reliably for live markets.
    harvest_fidelity: int = 60
    # How far back to fetch the first time a token is seen.
    initial_lookback_days: int = 30
    # Refetch this much overlap before the watermark; duplicates are fine
    # (raw layer is append-only, dbt dedupes on (token_id, t)), gaps are not.
    overlap_seconds: int = 3_600
    # Skip a token whose watermark is fresher than this — avoids pointless
    # calls when the job reruns quickly.
    min_refetch_seconds: int = 1_800
    # Cap the fetch window so a long-idle token can't request years of data.
    max_window_days: int = 30
    # The API silently returns EMPTY for startTs/endTs spans over 15 days
    # (and 400s past ~30). Verified empirically at every fidelity. All
    # windowed fetches are therefore chunked into spans of at most this.
    max_span_days: int = 14

    # --- backfill-prices ---
    # Try fidelities (minutes) finest-first; resolved markets have their
    # fine-grained history pruned server-side, so coarse is often all that
    # survives. Verified live: a 2024 market returns nothing at 60 but
    # 614 points at 720.
    fidelity_ladder: tuple[int, ...] = (60, 180, 360, 720, 1440)

    # --- sync-catalog ---
    # Recently-closed lookback: each run re-lands markets/events that ended
    # within this many days, so resolutions are captured.
    closed_lookback_days: int = 14

    # --- load-bigquery ---
    bq_project: str | None = None
    bq_dataset: str = "polymarket_raw"
    bq_location: str = "US"

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            gamma_base_url=_env("PDW_GAMMA_BASE_URL", cls.gamma_base_url),
            clob_base_url=_env("PDW_CLOB_BASE_URL", cls.clob_base_url),
            data_dir=Path(_env("PDW_DATA_DIR", "data")),
            state_dir=Path(_env("PDW_STATE_DIR", "state")),
            rate_delay=float(_env("PDW_RATE_DELAY", "0.25")),
            request_timeout=float(_env("PDW_REQUEST_TIMEOUT", "30")),
            volume_floor=float(_env("PDW_VOLUME_FLOOR", "10000")),
            initial_lookback_days=int(_env("PDW_INITIAL_LOOKBACK_DAYS", "30")),
            closed_lookback_days=int(_env("PDW_CLOSED_LOOKBACK_DAYS", "14")),
            bq_project=os.environ.get("PDW_BQ_PROJECT"),
            bq_dataset=_env("PDW_BQ_DATASET", "polymarket_raw"),
            bq_location=_env("PDW_BQ_LOCATION", "US"),
        )
