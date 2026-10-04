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

    wants: list[dict[str, Any] | None] = []
    for row in rows:
        raw_iid = str(row.get("jellyfin_item_id") or row.get("_jellyfin_item_id") or "").strip() or None
        want = _want_item(row, raw_iid=raw_iid)
        usable = common._lookup_type(want) in _TYPES and lp.checkable(want, native_keys=_NATIVE_KEYS, has_ids=has_ids)
        wants.append(want if usable else None)

    pending = [want for want in wants if want is not None]
    if not pending:
        return lp.all_unknown(rows, "not_checkable")
    try:
        if not _index_ready(probe, feature):
            return lp.all_unknown(rows, "index_unavailable")
        from ._id_lookup import prepare

        prepare(probe, feature, pending)
    except Exception as exc:
        common._dbg("presence_index_failed", lookup_feature=feature, error_type=type(exc).__name__)
        return lp.all_unknown(rows, "index_unavailable")

    out: list[dict[str, Any]] = []
    present = absent = 0
    with quiet_log():
        for want in wants:
            if want is None:
                out.append(lp.unknown("not_checkable"))
                continue
            try:
                iid = common.resolve_item_id(probe, want, feature=feature)
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
