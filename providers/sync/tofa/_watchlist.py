# /providers/sync/tofa/_watchlist.py
# Tofa watchlist sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from ._common import (
    PAGE_GUARD,
    WATCHLIST_PAGE,
    TofaSyncError,
    as_int,
    chunks,
    error_of,
    feed_head,
    ids_of_value,
    iso_z,
    key_of,
    library,
    resolve_item,
    safe_json,
    tofa_request,
    unresolved_entry,
    watchlist_key,
    watchlist_state,
    _dbg,
    _info,
    _warn,
)

FEATURE = "watchlist"


def _rows(adapter: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for page in range(1, PAGE_GUARD + 1):
        resp = tofa_request(adapter, "GET", "/users/me/watchlist", params={"page": page, "per_page": WATCHLIST_PAGE})
        if resp.status_code != 200:
            _warn(FEATURE, "read_failed", status=resp.status_code, code=error_of(resp) or None)
            raise TofaSyncError(f"tofa_watchlist_http_{resp.status_code}")
        data = safe_json(resp)
        items = data.get("items") if isinstance(data, Mapping) else None
        total_pages = as_int(data.get("total_pages")) if isinstance(data, Mapping) else None
        if not isinstance(items, list) or not all(isinstance(r, Mapping) for r in items) or total_pages is None or total_pages < 1:
            raise TofaSyncError("tofa_watchlist_invalid_response")
        out.extend(dict(row) for row in items)
        if page >= total_pages:
            return out
        if not items:
            raise TofaSyncError("tofa_watchlist_invalid_continuation")
    raise TofaSyncError("tofa_watchlist_page_guard_tripped")


def _full(adapter: Any) -> tuple[dict[str, Any], str]:
    head = feed_head(adapter, feature=FEATURE)
    rows: dict[str, Any] = {}
    for row in _rows(adapter):
        tmdb = as_int(row.get("tmdb_id"))
        if not tmdb or tmdb <= 0:
            continue
        rows[watchlist_key(row.get("media_type"), tmdb)] = {
            "media_type": "movie" if str(row.get("media_type") or "").lower() == "movie" else "tv", "tmdb_id": tmdb,
            "media_id": row.get("media_id"), "title": str(row.get("title") or "").strip(), "year": as_int(row.get("year")),
            "added_at": row.get("added_at"),
        }
    return rows, head


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    lib = library(adapter)
    rows = watchlist_state(adapter, feature=FEATURE, full=lambda: _full(adapter))
    collected: dict[str, dict[str, Any]] = {}
    for row in rows.values():
        tmdb = as_int(row.get("tmdb_id"))
        if not tmdb:
            continue
        kind = "movie" if row.get("media_type") == "movie" else "show"
        ids: dict[str, str] = {"tmdb": str(tmdb)}
        table = lib.movies if kind == "movie" else lib.shows
        mid = str(row.get("media_id") or "")
        owned = table.get(mid) if mid else None
        if owned is None:
            found = (lib.movie_by_id if kind == "movie" else lib.show_by_id).get(f"tmdb:{tmdb}")
            owned = table.get(found) if found else None
        if owned:
            for k, v in owned["ids"].items():
                ids.setdefault(k, v)
        item: dict[str, Any] = {"type": kind, "ids": ids}
        title = str(row.get("title") or "").strip() or (owned or {}).get("title") or ""
        if title:
            item["title"] = title
        year = as_int(row.get("year"))
        if year:
            item["year"] = year
        added = iso_z(row.get("added_at"))
        if added:
            item["added_at"] = added
        key = key_of(item)
        if key:
            collected[key] = item
    _info(FEATURE, "index_done", count=len(collected), rows=len(rows))
    return collected


def _entry(lib: Any, item: Mapping[str, Any], *, on: bool) -> tuple[dict[str, Any] | None, str]:
    target, kind = resolve_item(lib, item)
    if kind not in ("movie", "show"):
        return None, "unsupported_type"
    ids = ids_of_value(item.get("ids") if isinstance(item.get("ids"), Mapping) else {})
    if not ids.get("tmdb") and target:
        ids = {**(lib.movies if kind == "movie" else lib.shows)[target]["ids"], **ids}
    tmdb = as_int(ids.get("tmdb"))
    if not tmdb:
        return None, "missing_tmdb_id"
    entry: dict[str, Any] = {"on_watchlist": on, "media_type": "movie" if kind == "movie" else "tv", "tmdb_id": tmdb}
    if not on:
        return entry, ""
    title = str(item.get("title") or "").strip()
    if title:
        entry["title"] = title
    year = as_int(item.get("year"))
    if year:
        entry["year"] = year
    tvdb = as_int(ids.get("tvdb"))
    if kind == "show" and tvdb:
        entry["tvdb_id"] = tvdb
    added = iso_z(item.get("added_at") or item.get("listed_at"))
    if added:
        entry["added_at"] = added
    return entry, ""


def _write(adapter: Any, items: Iterable[Mapping[str, Any]], *, on: bool) -> dict[str, Any]:
    lib = library(adapter)
    action = "add" if on else "remove"
    unresolved: list[dict[str, Any]] = []
    entries: list[tuple[str, dict[str, Any], Mapping[str, Any]]] = []
    for item in items or []:
        key = key_of(item)
        if not key:
            continue
        entry, reason = _entry(lib, item, on=on)
        if entry is None:
            _dbg(FEATURE, "item_unresolved_before_write", key=key, reason=reason)
            unresolved.append(unresolved_entry(key, reason, item))
            continue
        entries.append((key, entry, item))
    _dbg(FEATURE, "write_prepare", action=action, items=len(entries), unresolved=len(unresolved))
    confirmed: list[str] = []
    for batch in chunks(entries):
        resp = tofa_request(adapter, "POST", "/users/me/watchlist/batch", json_body={"items": [e for _k, e, _i in batch]})
        if resp.status_code in (200, 204):
            confirmed.extend(k for k, _e, _i in batch)
            continue
        if resp.status_code == 401:
            raise TofaSyncError("tofa_watchlist_unauthorized")
        _warn(FEATURE, "write_failed", action=action, status=resp.status_code, code=error_of(resp) or None)
        unresolved.extend(unresolved_entry(k, f"http_{resp.status_code}", i) for k, _e, i in batch)
    _info(FEATURE, "write_done", action=action, confirmed=len(confirmed), unresolved=len(unresolved))
    return {"ok": True, "confirmed_keys": confirmed, "unresolved_keys": [u["key"] for u in unresolved], "unresolved": unresolved}


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, items, on=True)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, items, on=False)
