"""Ingestion state: per-token price watermarks and backfill completion.

A single JSON file, written atomically (temp file + os.replace) so a crash
mid-save can't corrupt it. Losing this file is safe — the raw layer is
append-only and dbt dedupes — it would only cause refetching.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path

log = logging.getLogger(__name__)


class StateStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._data: dict = {"price_watermarks": {}, "backfill_done": {}}
        if path.exists():
            self._data.update(json.loads(path.read_text(encoding="utf-8")))

    # --- price watermarks: last landed point's unix ts, per token ---

    def get_watermark(self, token_id: str) -> int | None:
        return self._data["price_watermarks"].get(token_id)

    def set_watermark(self, token_id: str, ts: int) -> None:
        self._data["price_watermarks"][token_id] = ts

    # --- backfill completion: token -> fidelity landed (None = nothing
    #     survived at any fidelity; still marked so reruns skip it) ---

    def is_backfilled(self, token_id: str) -> bool:
        return token_id in self._data["backfill_done"]

    def mark_backfilled(self, token_id: str, fidelity: int | None) -> None:
        self._data["backfill_done"][token_id] = fidelity

    def save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            dir=self._path.parent, prefix=self._path.name, suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=1)
            os.replace(tmp, self._path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
