# providers/sync/emby/_presence.py
# CrossWatch - Emby library presence
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import json
from collections.abc import Iterable, Mapping
from dataclasses import replace
from typing import Any

from cw_platform import library_presence as lp

from .._log import quiet as quiet_log
from . import _common as common
from ._history import _normalize_for_write

_NATIVE_KEYS = ("emby",)
_TYPES = frozenset({"movie", "show", "series", "episode"})
_EXACT_EPISODE_PROVIDERS = ("tmdb", "imdb", "tvdb")
_BATCH = 200


def _ids_only(adapter: Any) -> Any:
    probe = copy.copy(adapter)
    probe.cfg = replace(adapter.cfg, strict_id_matching=True)
    return probe


def _scope(adapter: Any, item: Mapping[str, Any], feature: str, allowed: list[str]) -> dict[str, Any]:
    hint = str(item.get("library_id") or item.get("libraryId") or item.get("source_library_id") or "").strip()
    if hint and hint in allowed:
        return common._emby_scope_from_list([hint])
    if allowed:
        return common._emby_scope_from_list(allowed)
    return dict(common.emby_library_scope(adapter.cfg, feature))


def _query_keys(item: Mapping[str, Any], priority: list[str]) -> list[tuple[tuple[str, ...], str]]:
    kind = common._lookup_type(item)
    ids = dict(item.get("ids") or {})
    show_ids = dict(item.get("show_ids") or {})
    pairs = common.all_ext_pairs(ids, priority)
    series_pairs = common.all_ext_pairs(show_ids, priority) if show_ids else []
    if kind == "movie":
        return [(tuple(pairs), "Movie")] if pairs else []
    if kind in ("show", "series"):
        chosen = pairs or series_pairs
        return [(tuple(chosen), "Series")] if chosen else []
    keys: list[tuple[tuple[str, ...], str]] = []
    for pair in pairs:
        if pair.partition(".")[0] in _EXACT_EPISODE_PROVIDERS:
            keys.append(((pair,), "Episode,Series"))
    if item.get("season") is not None and item.get("episode") is not None:
        keys.extend(((pair,), "Series") for pair in series_pairs)
    return keys


def _prime(adapter: Any, feature: str, wants: list[dict[str, Any]]) -> bool:
    priority = common._merged_guid_priority(adapter)
    allowed = sorted(common.emby_selected_library_ids(adapter.cfg, feature))
    cache = common._lookup_cache(adapter, feature).setdefault("queries", {})
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for want in wants:
        scope = _scope(adapter, want, feature, allowed)
        scope_key = json.dumps(dict(scope), sort_keys=True)
        for pairs, include_types in _query_keys(want, priority):
            if (pairs, include_types, scope_key) in cache:
                continue
            group = groups.setdefault((include_types, scope_key), {"scope": scope, "keys": set(), "pairs": {}})
            group["keys"].add(pairs)
            for pair in pairs:
                group["pairs"][pair] = None

    http, uid = adapter.client, adapter.cfg.user_id
    requests_made = 0
    for (include_types, scope_key), group in groups.items():
        wanted = list(group["pairs"])
        by_pair: dict[str, list[Mapping[str, Any]]] = {}
        for offset in range(0, len(wanted), _BATCH):
            rows = common._direct_query_by_pairs(http, uid, wanted[offset:offset + _BATCH], include_types, group["scope"])
            requests_made += 1
            if rows is None:
                return False
            for row in rows:
                provider_ids = common._ids_from_provider_ids(row.get("ProviderIds"))
                for name, value in provider_ids.items():
                    pair = common.format_provider_pair(name, value)
                    if pair:
                        by_pair.setdefault(pair, []).append(row)
        for pairs in group["keys"]:
            found: dict[str, Mapping[str, Any]] = {}
            for pair in pairs:
                for row in by_pair.get(pair, []):
                    found.setdefault(str(row.get("Id") or ""), row)
            cache[(pairs, include_types, scope_key)] = list(found.values())
    common.cw_log("EMBY", "common", "debug", "presence_primed", lookup_feature=feature,
                  groups=len(groups), requests=requests_made)
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
        want, _base = _normalize_for_write(row)
        usable = common._lookup_type(want) in _TYPES and lp.checkable(want, native_keys=_NATIVE_KEYS, has_ids=has_ids)
        wants.append(want if usable else None)

    pending = [want for want in wants if want is not None]
    if not pending:
        return lp.all_unknown(rows, "not_checkable")
    try:
        if not _prime(probe, feature, pending):
            return lp.all_unknown(rows, "index_unavailable")
    except Exception as exc:
        common.cw_log("EMBY", "common", "debug", "presence_index_failed", lookup_feature=feature,
                      error_type=type(exc).__name__)
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
    common.cw_log("EMBY", "common", "debug", "presence_done", lookup_feature=feature, items=len(rows),
                  present=present, absent=absent, unknown=len(rows) - present - absent)
    return out
