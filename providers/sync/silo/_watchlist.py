# /providers/sync/silo/_watchlist.py
# Silo watchlist sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping
from urllib.parse import quote

from ._common import (
    SiloSyncError,
    account_scope,
    as_int,
    key_of,
    library,
    minimal_for,
    pages,
    problem_of,
    read_state,
    resolve_item,
    safe_json,
    silo_request,
    unresolved_entry,
    write_state,
    _dbg,
    _info,
    _warn,
)

FEATURE = "watchlist"
STATE_NAME = "watchlist_series"
TITLES_STATE = "watchlist_titles"


def _title_minimal(row: Mapping[str, Any]) -> dict[str, Any] | None:
    tmdb = as_int(row.get("tmdb_id"))
    if not tmdb or tmdb <= 0:
        return None
    media = str(row.get("media_type") or "").strip().lower()
    out: dict[str, Any] = {"type": "movie" if media == "movie" else "show", "ids": {"tmdb": str(tmdb)}}
    title = str(row.get("title") or "").strip()
    if title:
        out["title"] = title
    year = as_int(row.get("year"))
    if year:
        out["year"] = year
    return out


def _is_member(adapter: Any, content_id: str) -> bool:
    resp = silo_request(adapter, "GET", f"/watchlist/{quote(content_id, safe='')}/membership")
    if resp.status_code == 404:
        return False
    if resp.status_code != 200:
        raise SiloSyncError(f"silo_watchlist_membership_http_{resp.status_code}")
    data = safe_json(resp)
    if not isinstance(data, Mapping) or "is_member" not in data:
        raise SiloSyncError("silo_watchlist_membership_invalid_response")
    return bool(data.get("is_member"))


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    lib = library(adapter)
    rows = pages(adapter, "/watchlist", feature=FEATURE) or []
    collected: dict[str, dict[str, Any]] = {}
    seen_series: set[str] = set()
    unmatched = 0
    for row in rows:
        cid = str(row.get("content_id") or "")
        item = minimal_for(lib, cid)
        if not item:
            unmatched += 1
            continue
        if cid.startswith("series-"):
            seen_series.add(cid)
        key = key_of(item)
        if key:
            collected[key] = item

    scope = account_scope(adapter)
    known = read_state(STATE_NAME, scope)
    kept = 0
    for cid in sorted(set(known.get("series") or []) - seen_series):
        if cid not in lib.series or not _is_member(adapter, cid):
            continue
        item = minimal_for(lib, cid)
        key = key_of(item) if item else ""
        if item and key:
            collected[key] = item
            seen_series.add(cid)
            kept += 1
    write_state(STATE_NAME, scope, {"series": sorted(seen_series)})

    titles = pages(adapter, "/watchlist/titles", feature=FEATURE, allow_disabled=True)
    if titles is None:
        title_items = dict(read_state(TITLES_STATE, scope).get("items") or {})
        if title_items:
            _warn(FEATURE, "titles_unavailable_kept_last_known", count=len(title_items))
    else:
        title_items = {}
        for row in titles:
            item = _title_minimal(row)
            key = key_of(item) if item else ""
            if item and key:
                title_items[key] = item
        write_state(TITLES_STATE, scope, {"items": title_items})
    for key, item in title_items.items():
        collected.setdefault(key, dict(item))

    _info(FEATURE, "index_done", count=len(collected), library=len(rows), titles=len(titles or []), titles_enabled=titles is not None, kept_finished_series=kept, unmatched=unmatched)
    return collected


def _remember_series(adapter: Any, added: Iterable[str], removed: Iterable[str]) -> None:
    scope = account_scope(adapter)
    known = set(read_state(STATE_NAME, scope).get("series") or [])
    known |= {c for c in added if c.startswith("series-")}
    known -= set(removed)
    write_state(STATE_NAME, scope, {"series": sorted(known)})


def _write(adapter: Any, items: Iterable[Mapping[str, Any]], *, remove: bool) -> dict[str, Any]:
    lib = library(adapter)
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []
    touched: list[str] = []
    titles_enabled = True
    method = "DELETE" if remove else "PUT"

    for item in items or []:
        key = key_of(item)
        if not key:
            continue
        cid, kind = resolve_item(adapter, lib, item, feature=FEATURE)
        if kind in ("episode", "season"):
            unresolved.append(unresolved_entry(key, "unsupported_type", item))
            continue
        if cid:
            resp = silo_request(adapter, method, f"/watchlist/{quote(cid, safe='')}")
            if resp.status_code in (200, 204):
                confirmed.append(key)
                touched.append(cid)
                continue
            if resp.status_code == 401:
                raise SiloSyncError("silo_watchlist_unauthorized")
            _warn(FEATURE, "write_failed", action=method.lower(), status=resp.status_code, problem=problem_of(resp) or None)
            unresolved.append(unresolved_entry(key, f"http_{resp.status_code}", item))
            continue
        ids = item.get("ids") if isinstance(item.get("ids"), Mapping) else {}
        tmdb = as_int((ids or {}).get("tmdb"))
        if not tmdb or not titles_enabled:
            _dbg(FEATURE, "item_unresolved_before_write", key=key, reason="not_in_library")
            unresolved.append(unresolved_entry(key, "not_in_library", item))
            continue
        media = "movie" if kind == "movie" else "series"
        resp = silo_request(adapter, method, f"/watchlist/titles/{media}/{tmdb}")
        if resp.status_code in (200, 204):
            confirmed.append(key)
            continue
        if resp.status_code == 409 and problem_of(resp) in ("capability_disabled", "capability_not_configured"):
            titles_enabled = False
            _info(FEATURE, "titles_unavailable", reason=problem_of(resp))
            unresolved.append(unresolved_entry(key, "not_in_library", item))
            continue
        if resp.status_code == 401:
            raise SiloSyncError("silo_watchlist_unauthorized")
        _warn(FEATURE, "write_failed", action=method.lower(), target="title", status=resp.status_code, problem=problem_of(resp) or None)
        unresolved.append(unresolved_entry(key, f"http_{resp.status_code}", item))

    if remove:
        _remember_series(adapter, [], touched)
    else:
        _remember_series(adapter, touched, [])
    _info(FEATURE, "write_done", action="remove" if remove else "add", confirmed=len(confirmed), unresolved=len(unresolved))
    return {"ok": True, "confirmed_keys": confirmed, "unresolved_keys": [u["key"] for u in unresolved], "unresolved": unresolved}


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, items, remove=False)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, items, remove=True)
