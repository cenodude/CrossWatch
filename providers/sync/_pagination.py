# /providers/sync/_pagination.py
# Cancellation and repeated-page detection for provider reads
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Sequence
from typing import Any

from cw_platform.run_control import raise_if_cancelled


def page_numbers(start: int = 1, step: int = 1, *, check_cancel: bool = True) -> Iterator[int]:
    page = start
    while True:
        if check_cancel:
            raise_if_cancelled()
        yield page
        page += step


class PageGuard:
    def __init__(self, error: type[Exception], message: str, *, check_cancel: bool = True) -> None:
        self.error = error
        self.message = message
        self.check_cancel = check_cancel
        self.seen: set[bytes] = set()

    def check(self, rows: Sequence[Any]) -> None:
        if self.check_cancel:
            raise_if_cancelled()
        if not rows:
            return
        fingerprint = hashlib.sha256(
            json.dumps(sorted(json.dumps(row, sort_keys=True, default=str) for row in rows)).encode()
        ).digest()
        if fingerprint in self.seen:
            raise self.error(self.message)
        self.seen.add(fingerprint)
