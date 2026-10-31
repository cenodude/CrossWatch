# cw_platform/orchestrator/_history_copies.py
# Update all copies support for one-way history pairs.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from ..history_events import has_history_watch
from ..value_coercion import coerce_bool

COPIES_FLAG = "_cw_history_all_copies"
COPIES_FIELD = "_cw_unplayed_copies"
COPIES_PROVIDERS = frozenset({"JELLYFIN"})


def history_copies_requested(feature: str, fcfg: Mapping[str, Any] | None, dst: str) -> bool:
    return (
        str(feature or "").strip().lower() == "history"
        and str(dst or "").strip().upper() in COPIES_PROVIDERS
        and coerce_bool((fcfg or {}).get("update_all_copies", False))
    )


def config_with_history_copies(config: Mapping[str, Any]) -> dict[str, Any]:
    return {**dict(config or {}), COPIES_FLAG: True}


def take_copy_gaps(index: Mapping[str, Any] | None) -> tuple[dict[str, Any], dict[str, Any]]:
    stripped: dict[str, Any] = {}
    gaps: dict[str, Any] = {}
    for key, row in (index or {}).items():
        if isinstance(row, Mapping) and COPIES_FIELD in row:
            clean = {k: v for k, v in row.items() if k != COPIES_FIELD}
            if row.get(COPIES_FIELD):
                gaps[key] = clean
            stripped[key] = clean
        else:
            stripped[key] = row
    return stripped, gaps


def copy_gap_adds(
    source: Mapping[str, Any],
    gaps: Mapping[str, Any],
    adds: list[dict[str, Any]],
    *,
    alias: Callable[[dict[str, Any]], dict[str, str]],
    find: Callable[[dict[str, Any], dict[str, str], Mapping[str, Any]], Mapping[str, Any] | None],
    minimal: Callable[[Mapping[str, Any]], dict[str, Any]],
    key_of: Callable[[Mapping[str, Any]], str],
) -> list[dict[str, Any]]:
    if not gaps:
        return []
    gap_index = dict(gaps)
    gap_alias = alias(gap_index)
    planned = {key_of(it) for it in adds}
    out: list[dict[str, Any]] = []
    for item in (source or {}).values():
        if not isinstance(item, Mapping) or not has_history_watch(item):
            continue
        if find(gap_index, gap_alias, item) is None:
            continue
        add = minimal(item)
        key = key_of(add)
        if key in planned:
            continue
        planned.add(key)
        out.append(add)
    return out
