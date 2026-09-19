# /providers/sync/tracearr/_common.py
# Tracearr Module shared helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from typing import Any, Callable

from .._log import log as cw_log


def make_logger(tag: str) -> Callable[..., None]:
    def _log(msg: str, *, level: str = "debug", **fields: Any) -> None:
        cw_log("TRACEARR", str(tag), str(level), str(msg), **fields)

    return _log
