# cw_platform/orchestration/_specials.py
# Season 0 (specials) pair feature gate for the orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from collections.abc import Mapping
from typing import Any
from ..value_coercion import coerce_bool

SPECIALS_FEATURES = frozenset({"history", "progress", "ratings"})


def specials_excluded(feature: str, fcfg: Mapping[str, Any] | None) -> bool:
    if str(feature) not in SPECIALS_FEATURES or not isinstance(fcfg, Mapping):
        return False
    return not coerce_bool(fcfg.get("include_specials", True), True)


def is_special(item: Any) -> bool:
    if not isinstance(item, Mapping):
        return False
    typ = str(item.get("type") or "").strip().lower().rstrip("s")
    if typ not in ("episode", "season"):
        return False
    raw = item.get("season") if item.get("season") is not None else item.get("season_number")
    if raw is None or isinstance(raw, bool):
        return False
    try:
        return int(raw) == 0
    except (TypeError, ValueError):
        return False


def filter_specials_index(idx: Mapping[str, Any] | None) -> tuple[dict[str, Any], dict[str, Any]]:
    out: dict[str, Any] = {}
    dropped: dict[str, Any] = {}
    for key, item in (idx or {}).items():
        if is_special(item):
            dropped[key] = item
            continue
        out[key] = item
    return out, dropped
