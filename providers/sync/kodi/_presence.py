# providers/sync/kodi/_presence.py
# CrossWatch Kodi library presence
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform import library_presence as lp

from ._common import EXTERNAL_ID_KEYS, library_index, log

_NATIVE_KEYS = ("_kodi_id",)
_TYPES = frozenset({"movie", "episode"})


def _has_ids(ids: Any) -> bool:
    return bool(lp.external_ids(ids, EXTERNAL_ID_KEYS))


def presence(adapter: Any, items: Iterable[Mapping[str, Any]], *, feature: str = "history") -> list[dict[str, Any]]:
    rows = [dict(item or {}) for item in (items or [])]
    if not rows:
        return []
    usable = [
        str(row.get("type") or "").strip().lower() in _TYPES
        and lp.checkable(row, native_keys=_NATIVE_KEYS, has_ids=_has_ids)
        for row in rows
    ]
    if not any(usable):
        return lp.all_unknown(rows, "not_checkable")
    try:
        idx = library_index(adapter, feature)
    except Exception as exc:
        log(feature, "debug", "presence_index_failed", error_type=type(exc).__name__)
        return lp.all_unknown(rows, "index_unavailable")

    out: list[dict[str, Any]] = []
    present = absent = 0
    for row, ok in zip(rows, usable):
        if not ok:
            out.append(lp.unknown("not_checkable"))
            continue
        try:
            target, reason = idx.resolve(row)
        except Exception as exc:
            out.append(lp.unknown(type(exc).__name__))
            continue
        if target is not None:
            present += 1
            out.append(lp.verdict(lp.PRESENT, item_id=target.get("_kodi_id")))
        elif reason == "not_found":
            absent += 1
            out.append(lp.verdict(lp.ABSENT, reason=lp.REASON))
        else:
            out.append(lp.unknown(reason))
    log(feature, "debug", "presence_done", items=len(rows), present=present, absent=absent,
        unknown=len(rows) - present - absent)
    return out
