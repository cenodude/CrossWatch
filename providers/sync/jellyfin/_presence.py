# providers/sync/jellyfin/_presence.py
# CrossWatch - Jellyfin library presence
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
from collections.abc import Iterable, Mapping
from dataclasses import replace
from typing import Any

from cw_platform import library_presence as lp

from .._log import quiet as quiet_log
from . import _common as common
from ._history import _want_item

_NATIVE_KEYS = ("jellyfin_item_id", "_jellyfin_item_id", "jellyfin")
_TYPES = frozenset({"movie", "show", "series", "episode"})
_PROGRESS_TYPES = frozenset({"movie", "episode"})
_WATCHLIST_RAW_TYPES = frozenset({"movie", "movies", "show", "shows", "series", "tv", "anime"})
_WATCHLIST_TYPES = frozenset({"movie", "show"})


def _ids_only(adapter: Any) -> Any:
    probe = copy.copy(adapter)
    probe.cfg = replace(adapter.cfg, strict_id_matching=True)
    return probe


def _index_ready(adapter: Any, feature: str) -> bool:
    if common._targeted_enabled(adapter):
        from ._id_lookup import _catalogue

        return _catalogue(adapter, feature) is not None
    if feature == "history":
        common.build_provider_index(adapter)
    else:
        common.build_provider_index(adapter, feature=feature)
    return True


def presence(adapter: Any, items: Iterable[Mapping[str, Any]], *, feature: str = "history") -> list[dict[str, Any]]:
    rows = [dict(item or {}) for item in (items or [])]
    if not rows:
        return []
    probe = _ids_only(adapter)
    priority = common._merged_guid_priority(probe)

    def has_ids(ids: Any) -> bool:
        return bool(common.all_ext_pairs(ids or {}, priority)) if isinstance(ids, Mapping) else False

    progress = feature == "progress"
    watchlist = feature == "watchlist"
    lookup_feature = "history" if watchlist else feature
    types = _PROGRESS_TYPES if progress else _WATCHLIST_TYPES if watchlist else _TYPES
    strict = bool(getattr(adapter.cfg, "strict_id_matching", False))
    wants: list[dict[str, Any] | None] = []
    skipped: list[dict[str, Any]] = []
    for row in rows:
        raw_iid = str(row.get("jellyfin_item_id") or row.get("_jellyfin_item_id") or "").strip() or None
        want = dict(row) if progress or watchlist else _want_item(row, raw_iid=raw_iid)
        known_type = common._lookup_type(want) in types
        if watchlist and str(row.get("type") or "").strip().lower() not in _WATCHLIST_RAW_TYPES:
            known_type = False
        usable = known_type and lp.checkable(want, native_keys=_NATIVE_KEYS, has_ids=has_ids)
        wants.append(want if usable else None)
        unmatchable = strict and known_type and lp.without_ids(want, native_keys=_NATIVE_KEYS, has_ids=has_ids)
        skipped.append(lp.verdict(lp.ABSENT, reason=lp.REASON) if unmatchable else lp.unknown("not_checkable"))

    pending = [want for want in wants if want is not None]
    if not pending:
        return skipped
    try:
        if not _index_ready(probe, lookup_feature):
            return lp.all_unknown(rows, "index_unavailable")
        from ._id_lookup import prepare

        prepare(probe, lookup_feature, pending)
    except Exception as exc:
        common._dbg("presence_index_failed", lookup_feature=feature, error_type=type(exc).__name__)
        return lp.all_unknown(rows, "index_unavailable")

    out: list[dict[str, Any]] = []
    present = absent = 0
    with quiet_log():
        for want, fallback in zip(wants, skipped):
            if want is None:
                absent += fallback["status"] == lp.ABSENT
                out.append(fallback)
                continue
            try:
                if progress:
                    found = common.resolve_item_ids(probe, want, feature=feature)
                    iid = found[0] if found else None
                else:
                    iid = common.resolve_item_id(probe, want, feature=lookup_feature)
            except Exception as exc:
                out.append(lp.unknown(type(exc).__name__))
                continue
            if iid:
                present += 1
                out.append(lp.verdict(lp.PRESENT, item_id=iid))
            else:
                absent += 1
                out.append(lp.verdict(lp.ABSENT, reason=lp.REASON))
    common._dbg("presence_done", lookup_feature=feature, items=len(rows), present=present, absent=absent,
                unknown=len(rows) - present - absent)
    return out
