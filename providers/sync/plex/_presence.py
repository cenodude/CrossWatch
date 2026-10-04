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
    plex_feature_library_ids,
    plex_headers,
)

_EPISODE_TYPES = frozenset({"episode", "anime"})
_PRESENT = frozenset({history.CLASS_IN_CATALOG_WATCHED, history.CLASS_IN_CATALOG_UNWATCHED})
_MAX_TARGETED_SHOWS = 40
_PAGE_SIZE = 1000


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


def _load_show_leaves(adapter: Any, cat: Any, show_rks: Iterable[str]) -> int | None:
    opened = _session(adapter)
    if opened is None:
        return None
    ses, base, headers, token = opened
    added = 0
    for rk in sorted(set(show_rks)):
        start = 0
        seen: set[tuple[str, ...]] = set()
        while True:
            params = {"includeGuids": 1, "X-Plex-Container-Start": start, "X-Plex-Container-Size": _PAGE_SIZE}
            r = ses.get(f"{base}/library/metadata/{rk}/allLeaves", params=params, headers=headers, timeout=20)
            if not getattr(r, "ok", False):
                return None
            ctype = (r.headers.get("content-type") or "").lower()
            data = (r.json() or {}) if "application/json" in ctype else _xml_to_container(r.text or "")
            container = data.get("MediaContainer") or {}
            rows = [row for row in (container.get("Metadata") or []) if isinstance(row, Mapping)]
            if not rows:
                break
            signature = tuple(str(row.get("ratingKey") or "") for row in rows)
            if signature in seen:
                return None
            seen.add(signature)
            for row in rows:
                entry = history._catalog_episode_entry(cat, row, token)
                if entry is not None:
                    cat.add(entry)
                    added += 1
            start += len(rows)
            total = container.get("totalSize")
            if total is not None and start >= int(total):
                break
            if total is None and len(rows) < _PAGE_SIZE:
                break
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


def presence(adapter: Any, items: Iterable[Mapping[str, Any]], *, feature: str = "history") -> list[dict[str, Any]]:
    rows = [dict(item or {}) for item in (items or [])]
    if not rows:
        return []
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
