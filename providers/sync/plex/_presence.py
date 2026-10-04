# providers/sync/plex/_presence.py
# CrossWatch - Plex library presence
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform import library_presence as lp
from cw_platform.id_map import ids_from, ids_from_guid

from . import _history as history
from ._common import (
    _as_base_url,
    _xml_to_container,
    extract_show_ids,
    home_scope_enter,
    home_scope_exit,
    item_guid_candidates,
    plex_feature_library_ids,
    plex_headers,
)

_EPISODE_TYPES = frozenset({"episode", "anime"})
_PRESENT = frozenset({history.CLASS_IN_CATALOG_WATCHED, history.CLASS_IN_CATALOG_UNWATCHED})
_MAX_TARGETED_SHOWS = 40
_PAGE_SIZE = 1000
_RATING_KINDS = {"movies": "movie", "shows": "show", "series": "show", "anime": "show", "tv": "show",
                 "tv_shows": "show", "tvshows": "show"}
_GUID_FEATURES: dict[str, tuple[dict[str, str], frozenset[str]]] = {
    "ratings": (_RATING_KINDS, frozenset({"movie", "show", "season", "episode"})),
    "progress": ({"anime": "episode"}, frozenset({"movie", "episode"})),
}


def _external(tokens: set[str]) -> set[str]:
    return {tok for tok in tokens if not tok.startswith(("plex:", "guid:"))}


def _checkable(item: Mapping[str, Any]) -> bool:
    kind = str(item.get("type") or "movie").strip().lower()
    if kind in _EPISODE_TYPES:
        season, episode = history._item_se(item)
        if season is None or episode is None:
            return False
        return bool(_external(history._id_tokens(extract_show_ids(item))))
    if kind != "movie":
        return False
    tokens = history._id_tokens(ids_from(item))
    return bool(tokens) and len(_external(tokens)) == len(tokens)


def _session(adapter: Any) -> tuple[Any, str, dict[str, Any], str] | None:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    base = _as_base_url(srv) if srv else None
    ses = getattr(srv, "_session", None)
    token = getattr(srv, "token", None) or getattr(srv, "_token", None) or ""
    if not (base and ses and token):
        return None
    headers = dict(getattr(ses, "headers", {}) or {})
    headers.update(plex_headers(token))
    headers["Accept"] = "application/json"
    return ses, base, headers, token


def _show_copies(adapter: Any, allow: set[str]) -> dict[str, set[str]]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    out: dict[str, set[str]] = {}
    for sec in adapter.libraries(types=("show",)) or []:
        section_id = str(getattr(sec, "key", "") or "").strip()
        if not section_id or (allow and section_id not in allow):
            continue
        rows, _requests = history._fetch_section_guid_rows(srv, section_id, 2, strict=True)
        for row in rows:
            rk = str(row.get("ratingKey") or "").strip()
            if not rk:
                continue
            for guid in history._row_guids(row):
                for tok in history._id_tokens(ids_from_guid(str(guid)) or {}):
                    out.setdefault(tok, set()).add(rk)
    return out


def _paged_rows(ses: Any, url: str, headers: Mapping[str, Any], params: Mapping[str, Any]) -> list[Mapping[str, Any]] | None:
    out: list[Mapping[str, Any]] = []
    start = 0
    seen: set[tuple[str, ...]] = set()
    while True:
        query = {**params, "X-Plex-Container-Start": start, "X-Plex-Container-Size": _PAGE_SIZE}
        r = ses.get(url, params=query, headers=headers, timeout=20)
        if not getattr(r, "ok", False):
            return None
        ctype = (r.headers.get("content-type") or "").lower()
        data = (r.json() or {}) if "application/json" in ctype else _xml_to_container(r.text or "")
        container = data.get("MediaContainer") or {}
        rows = [row for row in (container.get("Metadata") or []) if isinstance(row, Mapping)]
        if not rows:
            return out
        signature = tuple(str(row.get("ratingKey") or "") for row in rows)
        if signature in seen:
            return None
        seen.add(signature)
        out.extend(rows)
        start += len(rows)
        total = container.get("totalSize")
        if total is not None and start >= int(total):
            return out
        if total is None and len(rows) < _PAGE_SIZE:
            return out


def _load_show_leaves(adapter: Any, cat: Any, show_rks: Iterable[str]) -> int | None:
    opened = _session(adapter)
    if opened is None:
        return None
    ses, base, headers, token = opened
    added = 0
    for rk in sorted(set(show_rks)):
        rows = _paged_rows(ses, f"{base}/library/metadata/{rk}/allLeaves", headers, {"includeGuids": 1})
        if rows is None:
            return None
        for row in rows:
            entry = history._catalog_episode_entry(cat, row, token)
            if entry is not None:
                cat.add(entry)
                added += 1
    return added


def _load_missing_episodes(adapter: Any, allow: set[str], cat: Any, items: list[Mapping[str, Any]]) -> str:
    started = time.monotonic()
    mode = "library"
    try:
        copies = _show_copies(adapter, allow)
        wanted: set[str] = set()
        covered = True
        for item in items:
            rks = set().union(*(copies.get(tok, set()) for tok in history._item_show_tokens(item)))
            if not rks:
                covered = False
                break
            wanted |= rks
        if covered and wanted and len(wanted) <= _MAX_TARGETED_SHOWS and _load_show_leaves(adapter, cat, wanted) is not None:
            mode = "shows"
    except Exception as exc:
        history._dbg("presence_show_lookup_failed", error_type=type(exc).__name__)
    if mode == "library":
        history._populate_catalog_episode_leaves(adapter, allow, cat)
    history._store_history_catalog(adapter, allow, cat)
    history._dbg("presence_episodes_loaded", mode=mode, items=len(items),
                 duration_ms=int((time.monotonic() - started) * 1000))
    return mode


def _verdict(rk: Any, klass: str) -> dict[str, Any]:
    if rk and klass in _PRESENT:
        return lp.verdict(lp.PRESENT, item_id=rk)
    if klass in (history.CLASS_NOT_IN_PLEX_CATALOG, history.CLASS_SHOW_MATCHED_EPISODE_MISSING):
        return lp.verdict(lp.ABSENT, reason=lp.REASON)
    return lp.unknown(klass)


def _history_presence(adapter: Any, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    usable = [_checkable(row) for row in rows]
    if not any(usable):
        return lp.all_unknown(rows, "not_checkable")

    need_home_scope, did_home_switch, _sel_aid, _sel_uname = home_scope_enter(adapter)
    try:
        if not getattr(getattr(adapter, "client", None), "server", None):
            return lp.all_unknown(rows, "no_plex_server")
        if need_home_scope and not did_home_switch:
            return lp.all_unknown(rows, "home_scope_not_applied")
        try:
            allow = plex_feature_library_ids(adapter, "history")
            cat = history._get_history_catalog(adapter, allow)
        except Exception as exc:
            history._dbg("presence_index_failed", error_type=type(exc).__name__)
            return lp.all_unknown(rows, "index_unavailable")

        out: list[dict[str, Any] | None] = [None] * len(rows)
        pending: list[int] = []
        for index, (row, ok) in enumerate(zip(rows, usable)):
            if not ok:
                out[index] = lp.unknown("not_checkable")
                continue
            try:
                rk, klass = cat.resolve(row, strict=True)
            except Exception as exc:
                out[index] = lp.unknown(type(exc).__name__)
                continue
            if not rk and klass == history.CLASS_SHOW_MATCHED_EPISODE_MISSING:
                pending.append(index)
            else:
                out[index] = _verdict(rk, klass)

        if pending:
            try:
                _load_missing_episodes(adapter, allow, cat, [rows[index] for index in pending])
            except Exception as exc:
                history._dbg("presence_episodes_failed", error_type=type(exc).__name__)
                for index in pending:
                    out[index] = lp.unknown("episodes_unavailable")
                pending = []
            for index in pending:
                try:
                    rk, klass = cat.resolve(rows[index], strict=True)
                    out[index] = _verdict(rk, klass)
                except Exception as exc:
                    out[index] = lp.unknown(type(exc).__name__)

        result = [row if row is not None else lp.unknown("not_checked") for row in out]
        present = sum(1 for row in result if row["status"] == lp.PRESENT)
        absent = sum(1 for row in result if row["status"] == lp.ABSENT)
        history._dbg("presence_done", items=len(rows), present=present, absent=absent,
                     unknown=len(rows) - present - absent)
        return result
    finally:
        home_scope_exit(adapter, did_home_switch)


def _number(value: Any) -> int | None:
    try:
        return int(value) if value is not None and str(value).strip() != "" else None
    except (TypeError, ValueError):
        return None


def _show_coordinates(adapter: Any, allow: set[str], show_rks: set[str]) -> dict[str, set[tuple[int | None, int | None]]] | None:
    opened = _session(adapter)
    if opened is None:
        return None
    ses, base, headers, _token = opened
    out: dict[str, set[tuple[int | None, int | None]]] = {rk: set() for rk in show_rks}
    if len(show_rks) <= _MAX_TARGETED_SHOWS:
        for rk in sorted(show_rks):
            rows = _paged_rows(ses, f"{base}/library/metadata/{rk}/allLeaves", headers, {})
            if rows is None:
                return None
            out[rk].update((_number(row.get("parentIndex")), _number(row.get("index"))) for row in rows)
        return out
    for sec in adapter.libraries(types=("show",)) or []:
        section_id = str(getattr(sec, "key", "") or "").strip()
        if not section_id or (allow and section_id not in allow):
            continue
        rows = _paged_rows(ses, f"{base}/library/sections/{section_id}/all", headers, {"type": 4})
        if rows is None:
            return None
        for row in rows:
            rk = str(row.get("grandparentRatingKey") or "").strip()
            if rk in out:
                out[rk].add((_number(row.get("parentIndex")), _number(row.get("index"))))
    return out


def _guid_presence(adapter: Any, rows: list[dict[str, Any]], feature: str) -> list[dict[str, Any]]:
    kind_map, kinds = _GUID_FEATURES[feature]
    need_home_scope, did_home_switch, _sel_aid, _sel_uname = home_scope_enter(adapter)
    try:
        if not getattr(getattr(adapter, "client", None), "server", None):
            return lp.all_unknown(rows, "no_plex_server")
        if need_home_scope and not did_home_switch:
            return lp.all_unknown(rows, "home_scope_not_applied")
        if feature == "ratings":
            from . import _ratings as ratings

            if ratings._shared_user_scope_active(adapter):
                return lp.all_unknown(rows, "shared_user_ratings_unsupported")
        try:
            allow = plex_feature_library_ids(adapter, feature)
            history._build_guid_index(adapter, allow, feature=feature)
        except Exception as exc:
            history._dbg("presence_index_failed", lookup_feature=feature, error_type=type(exc).__name__)
            return lp.all_unknown(rows, "index_unavailable")

        out: list[dict[str, Any] | None] = [None] * len(rows)
        pending: list[tuple[int, str, str, int | None, int | None]] = []
        for index, row in enumerate(rows):
            raw_kind = str(row.get("type") or "").strip().lower()
            kind = kind_map.get(raw_kind, raw_kind or "movie")
            ids = ids_from(row)
            if kind not in kinds or ids.get("plex"):
                out[index] = lp.unknown("not_checkable")
                continue
            if kind in ("movie", "show"):
                if not lp.external_ids(ids):
                    out[index] = lp.unknown("not_checkable")
                    continue
                rk = history._pms_find_in_guid_index(kind, item_guid_candidates(ids, {}, row))
                out[index] = lp.verdict(lp.PRESENT, item_id=rk) if rk else lp.verdict(lp.ABSENT, reason=lp.REASON)
                continue
            raw_show_ids = row.get("show_ids")
            show_ids = ids_from({"ids": dict(raw_show_ids)}) if isinstance(raw_show_ids, Mapping) else {}
            season = _number(row.get("season") if row.get("season") is not None else row.get("season_number"))
            episode = _number(row.get("episode") if row.get("episode") is not None else row.get("episode_number"))
            if not lp.external_ids(show_ids) or season is None or (kind == "episode" and episode is None):
                out[index] = lp.unknown("not_checkable")
                continue
            show_rk = history._pms_find_in_guid_index("show", item_guid_candidates(ids, show_ids, row))
            if not show_rk:
                out[index] = lp.verdict(lp.ABSENT, reason=lp.REASON)
            else:
                pending.append((index, str(show_rk), kind, season, episode))

        if pending:
            started = time.monotonic()
            show_rks = {show_rk for _i, show_rk, _k, _s, _e in pending}
            try:
                coordinates = _show_coordinates(adapter, allow, show_rks)
            except Exception as exc:
                history._dbg("presence_episodes_failed", lookup_feature=feature, error_type=type(exc).__name__)
                coordinates = None
            history._dbg("presence_episodes_loaded", lookup_feature=feature, items=len(pending), shows=len(show_rks),
                         duration_ms=int((time.monotonic() - started) * 1000))
            for index, show_rk, kind, season, episode in pending:
                if coordinates is None:
                    out[index] = lp.unknown("episodes_unavailable")
                    continue
                found = coordinates.get(show_rk) or set()
                hit = (season, episode) in found if kind == "episode" else any(s == season for s, _e in found)
                out[index] = lp.verdict(lp.PRESENT) if hit else lp.verdict(lp.ABSENT, reason=lp.REASON)

        result = [row if row is not None else lp.unknown("not_checked") for row in out]
        present = sum(1 for row in result if row["status"] == lp.PRESENT)
        absent = sum(1 for row in result if row["status"] == lp.ABSENT)
        history._dbg("presence_done", lookup_feature=feature, items=len(rows), present=present, absent=absent,
                     unknown=len(rows) - present - absent)
        return result
    finally:
        home_scope_exit(adapter, did_home_switch)


def presence(adapter: Any, items: Iterable[Mapping[str, Any]], *, feature: str = "history") -> list[dict[str, Any]]:
    rows = [dict(item or {}) for item in (items or [])]
    if not rows:
        return []
    name = str(feature or "").strip().lower()
    if name in _GUID_FEATURES:
        return _guid_presence(adapter, rows, name)
    return _history_presence(adapter, rows)
