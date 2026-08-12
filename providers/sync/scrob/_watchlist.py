# /providers/sync/scrob/_watchlist.py
# Scrob watchlist sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal

from ._common import (
    MEDIA_TYPE_MOVIE,
    MEDIA_TYPE_SERIES,
    PATH_LIST,
    PATH_LIST_ITEM,
    PATH_LIST_ITEMS,
    PATH_LISTS,
    cfg_section,
    error_of,
    ids_for_scrob,
    iso_z,
    mapping,
    ok_status,
    positive_int,
    safe_json,
    scrob_request,
    show_ids_for_scrob,
    year_of,
    _dbg,
    _info,
    _warn,
)

FEATURE = "watchlist"

LIST_ITEM_ID_FIELD = "_scrob_list_item_id"
LIST_ID_FIELD = "_scrob_list_id"


def _key_of(obj: Mapping[str, Any]) -> str:
    try:
        return str(canonical_key(id_minimal(obj)) or "").strip()
    except Exception:
        return ""


def watchlist_name(adapter: Any) -> str:
    section = cfg_section(adapter) or {}
    return str(section.get("watchlist_name") or "Watchlist").strip() or "Watchlist"


def _find_list(adapter: Any, name: str) -> Mapping[str, Any] | None:
    resp = scrob_request(adapter, "GET", PATH_LISTS)
    if not ok_status(resp):
        _warn(FEATURE, "lists_fetch_failed", status=int(resp.status_code), error=error_of(resp))
        return None
    data = safe_json(resp)
    rows = data.get("lists") if isinstance(data, Mapping) else None
    rows = [r for r in rows if isinstance(r, Mapping)] if isinstance(rows, list) else []
    wanted = name.strip().lower()
    for row in rows:
        if str(row.get("name") or "").strip().lower() == wanted:
            return row
    return None


def _create_list(adapter: Any, name: str) -> Mapping[str, Any] | None:
    resp = scrob_request(adapter, "POST", PATH_LISTS, json={"name": name, "privacy_level": "private"})
    if not ok_status(resp):
        _warn(FEATURE, "list_create_failed", status=int(resp.status_code), error=error_of(resp))
        return None
    data = safe_json(resp)
    return data if isinstance(data, Mapping) else None


def resolve_list_id(adapter: Any, *, create: bool = False) -> int | None:
    name = watchlist_name(adapter)
    found = _find_list(adapter, name)
    if found is not None:
        return positive_int(found.get("id"))
    if not create:
        _dbg(FEATURE, "watchlist_missing", name=name)
        return None
    created = _create_list(adapter, name)
    list_id = positive_int((created or {}).get("id"))
    if list_id:
        _info(FEATURE, "watchlist_created", name=name, list_id=list_id)
    return list_id


def _row_to_minimal(row: Mapping[str, Any], list_id: int) -> dict[str, Any] | None:
    media = mapping(row.get("media"))
    tmdb = positive_int(media.get("tmdb_id"))
    if not tmdb:
        return None
    media_type = str(media.get("type") or "").strip().lower()
    if media_type == MEDIA_TYPE_SERIES:
        out: dict[str, Any] = {"type": "show", "ids": {"tmdb": str(tmdb)}}
    elif media_type == MEDIA_TYPE_MOVIE:
        out = {"type": "movie", "ids": {"tmdb": str(tmdb)}}
    else:
        return None

    title = str(media.get("title") or "").strip()
    if title:
        out["title"] = title
    year = year_of(media)
    if year:
        out["year"] = year
    added = iso_z(row.get("added_at"))
    if added:
        out["added_at"] = added

    item_id = positive_int(row.get("id"))
    if item_id:
        out[LIST_ITEM_ID_FIELD] = str(item_id)
    out[LIST_ID_FIELD] = str(list_id)
    return out


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    list_id = resolve_list_id(adapter)
    if not list_id:
        _info(FEATURE, "index_done", count=0, reason="watchlist_missing")
        return {}

    resp = scrob_request(adapter, "GET", PATH_LIST.format(list_id=list_id))
    if not ok_status(resp):
        _warn(FEATURE, "fetch_failed", status=int(resp.status_code), error=error_of(resp))
        return {}
    data = safe_json(resp)
    rows = data.get("items") if isinstance(data, Mapping) else None
    rows = [r for r in rows if isinstance(r, Mapping)] if isinstance(rows, list) else []

    out: dict[str, dict[str, Any]] = {}
    skipped = 0
    for row in rows:
        minimal = _row_to_minimal(row, list_id)
        if not minimal:
            skipped += 1
            continue
        key = _key_of(minimal)
        if not key:
            skipped += 1
            continue
        out[key] = minimal

    _info(FEATURE, "index_done", count=len(out), rows=len(rows), skipped=skipped)
    return out


def _payload_for(item: Mapping[str, Any]) -> dict[str, Any] | None:
    typ = str(item.get("type") or "").strip().lower()
    if typ in ("show", "series", "tv"):
        tmdb = positive_int(ids_for_scrob(item).get("tmdb_id")) or positive_int(show_ids_for_scrob(item).get("tmdb_id"))
        if not tmdb:
            return None
        return {"tmdb_id": tmdb, "media_type": MEDIA_TYPE_SERIES}
    if typ in ("", "movie", "film"):
        tmdb = positive_int(ids_for_scrob(item).get("tmdb_id"))
        if not tmdb:
            return None
        return {"tmdb_id": tmdb, "media_type": MEDIA_TYPE_MOVIE}
    return None


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    pending: list[tuple[str, dict[str, Any]]] = []

    for item in items or []:
        key = _key_of(item)
        if not key:
            continue
        payload = _payload_for(item)
        if payload is None:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "missing_supported_id_or_type"})
            _dbg(FEATURE, "item_unresolved_before_write", key=key)
            continue
        pending.append((key, payload))

    if not pending:
        return {
            "ok": True,
            "count": 0,
            "confirmed_keys": [],
            "unresolved_keys": unresolved_keys,
            "deferred_keys": [],
            "unresolved": unresolved,
        }

    _dbg(FEATURE, "write_prepare", action="add", items=len(pending), skipped=len(unresolved_keys))

    if dry_run:
        return {
            "ok": True,
            "count": len(pending),
            "confirmed_keys": [key for key, _ in pending],
            "unresolved_keys": unresolved_keys,
            "deferred_keys": [],
            "unresolved": unresolved,
        }

    list_id = resolve_list_id(adapter, create=True)
    if not list_id:
        for key, _ in pending:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "watchlist_unavailable"})
        return {
            "ok": False,
            "count": 0,
            "confirmed_keys": [],
            "unresolved_keys": unresolved_keys,
            "deferred_keys": [],
            "unresolved": unresolved,
        }

    ok = True
    for key, payload in pending:
        resp = scrob_request(adapter, "POST", PATH_LIST_ITEMS.format(list_id=list_id), json=payload)
        code = int(resp.status_code)
        if ok_status(resp) or code == 409:
            confirmed.append(key)
            continue
        ok = False
        unresolved_keys.append(key)
        unresolved.append({"key": key, "status": f"http:{code}", "error": error_of(resp)})
        _warn(FEATURE, "watchlist_add_failed", key=key, status=code, error=error_of(resp))

    _info(FEATURE, "write_done", action="add", confirmed=len(confirmed), unresolved=len(unresolved_keys))
    return {
        "ok": ok,
        "count": len(confirmed),
        "confirmed_keys": confirmed,
        "unresolved_keys": unresolved_keys,
        "deferred_keys": [],
        "unresolved": unresolved,
    }


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    ok = True

    pending = [(key, item) for item in (items or []) if (key := _key_of(item))]
    if not pending:
        return {"ok": True, "count": 0, "confirmed_keys": [], "unresolved_keys": [], "deferred_keys": [], "unresolved": []}

    if dry_run:
        return {
            "ok": True,
            "count": len(pending),
            "confirmed_keys": [key for key, _ in pending],
            "unresolved_keys": [],
            "deferred_keys": [],
            "unresolved": [],
        }

    lookup: dict[str, dict[str, Any]] | None = None
    for key, item in pending:
        list_id = positive_int(item.get(LIST_ID_FIELD))
        item_id = positive_int(item.get(LIST_ITEM_ID_FIELD))
        if not (list_id and item_id):
            if lookup is None:
                lookup = build_index(adapter)
            known = lookup.get(key) or {}
            list_id = list_id or positive_int(known.get(LIST_ID_FIELD))
            item_id = item_id or positive_int(known.get(LIST_ITEM_ID_FIELD))
        if not (list_id and item_id):
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "missing_list_item_id"})
            _dbg(FEATURE, "item_unresolved_before_write", key=key, reason="missing_list_item_id")
            continue

        resp = scrob_request(adapter, "DELETE", PATH_LIST_ITEM.format(list_id=list_id, item_id=item_id))
        code = int(resp.status_code)
        if ok_status(resp) or code == 404:
            confirmed.append(key)
            continue
        ok = False
        unresolved_keys.append(key)
        unresolved.append({"key": key, "status": f"http:{code}", "error": error_of(resp)})
        _warn(FEATURE, "watchlist_remove_failed", key=key, status=code, error=error_of(resp))

    _info(FEATURE, "write_done", action="remove", confirmed=len(confirmed), unresolved=len(unresolved_keys))
    return {
        "ok": ok,
        "count": len(confirmed),
        "confirmed_keys": confirmed,
        "unresolved_keys": unresolved_keys,
        "deferred_keys": [],
        "unresolved": unresolved,
    }
