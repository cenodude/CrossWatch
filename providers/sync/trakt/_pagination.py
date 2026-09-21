# /providers/sync/trakt/_pagination.py
# Trakt pagination validation and completion tracking
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, NoReturn

from .._log import log as cw_log


class TraktPaginationError(RuntimeError):
    pass


class TraktPager:
    def __init__(self, feature: str, max_pages: int = 100000) -> None:
        self.feature = feature
        self.max_pages = max_pages
        self.seen: set[bytes] = set()

    def _fail(self, reason: str, page: int, **fields: Any) -> NoReturn:
        cw_log("TRAKT", self.feature, "warn", "pagination_incomplete", reason=reason, page=page, **fields)
        raise TraktPaginationError(f"Trakt {self.feature} pagination incomplete: {reason} on page {page}")

    def read(self, response: Any, page: int, *, bucket: str | None = None) -> list[Any]:
        if response.status_code != 200:
            self._fail("http_failed", page, status=response.status_code)
        try:
            rows = response.json()
        except Exception:
            self._fail("invalid_json", page)
        if bucket and isinstance(rows, Mapping):
            rows = rows.get(bucket)
        if not isinstance(rows, list):
            self._fail("invalid_response", page)
        headers = {str(k).lower(): v for k, v in (getattr(response, "headers", None) or {}).items()}
        cw_log(
            "TRAKT", self.feature, "debug", "index_page", page=page, rows=len(rows),
            reported_pages=headers.get("x-pagination-page-count"),
            reported_items=headers.get("x-pagination-item-count"),
            applied_limit=headers.get("x-pagination-limit"),
        )
        if not rows:
            return []
        if self.max_pages > 0 and page > self.max_pages:
            self._fail("safety_cap_hit", page, max_pages=self.max_pages)
        fingerprint = hashlib.sha256(
            json.dumps(sorted(json.dumps(row, sort_keys=True) for row in rows)).encode("utf-8")
        ).digest()
        if fingerprint in self.seen:
            self._fail("repeated_page", page)
        self.seen.add(fingerprint)
        return rows
