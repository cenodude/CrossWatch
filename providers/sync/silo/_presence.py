# /providers/sync/silo/_presence.py
# Silo destination library presence
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from cw_platform import library_presence as lp

from ._common import library, resolve_item, _dbg

FEATURES = ("history", "ratings", "progress")
_TYPES = {"history": ("movie", "episode"), "progress": ("movie", "episode"), "ratings": ("movie", "show")}


def presence(adapter: Any, items: Iterable[Mapping[str, Any]], *, feature: str = "history") -> list[dict[str, Any]]:
    rows = [dict(item or {}) for item in (items or [])]
    if not rows:
        return []
    kinds = _TYPES.get(str(feature or "").strip().lower())
    if not kinds:
        return lp.all_unknown(rows, "unsupported_feature")
    try:
        lib = library(adapter, feature=feature)
    except Exception as exc:
        _dbg(feature, "presence_index_failed", error_type=type(exc).__name__)
        return lp.all_unknown(rows, "index_unavailable")

    out: list[dict[str, Any]] = []
    present = absent = 0
    for row in rows:
        try:
            cid, kind = resolve_item(adapter, lib, row, feature=feature)
        except Exception as exc:
            out.append(lp.unknown(type(exc).__name__))
            continue
        if kind not in kinds:
            out.append(lp.unknown("unsupported_type"))
        elif cid:
            present += 1
            out.append(lp.verdict(lp.PRESENT, item_id=cid))
        else:
            absent += 1
            out.append(lp.verdict(lp.ABSENT, reason=lp.REASON))
    _dbg(feature, "presence_done", items=len(rows), present=present, absent=absent, unknown=len(rows) - present - absent)
    return out
