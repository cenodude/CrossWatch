# providers/sync/publicmetadb/_watchlist.py
# PUBLICMETADB Module for watchlist functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal

from .._log import log as cw_log
from ._common import cfg_section, media_type_for_item, read_json, state_file, tmdb_id_for_item, write_json


def _dbg(event: str, **fields: Any) -> None:
    cw_log("PUBLICMETADB", "watchlist", "debug", event, **fields)


def _info(event: str, **fields: Any) -> None:
    cw_log("PUBLICMETADB", "watchlist", "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log("PUBLICMETADB", "watchlist", "warn", event, **fields)


def _shadow_path():
    return state_file("publicmetadb_watchlist.shadow.json")


def _shadow_load() -> dict[str, Any]:
    doc = read_json(_shadow_path())
    if not isinstance(doc.get("items"), dict):
        doc["items"] = {}
    return doc


def _shadow_save(items: Mapping[str, Any], *, list_id: str | None = None) -> None:
    doc: dict[str, Any] = {"items": dict(items)}
    if list_id:
        doc["list_id"] = list_id
    write_json(_shadow_path(), doc)


def _item_key(item: Mapping[str, Any]) -> str:
    return canonical_key(id_minimal(item))


def _to_minimal(row: Mapping[str, Any]) -> dict[str, Any] | None:
    tmdb = tmdb_id_for_item(row)
    if tmdb is None:
        return None
    media = str(row.get("media_type") or row.get("type") or "").strip().lower()
    typ = "show" if media == "tv" else "movie"
    out: dict[str, Any] = {"type": typ, "ids": {"tmdb": str(tmdb)}}
    title = str(row.get("title") or row.get("name") or "").strip()
    if title:
        out["title"] = title
    year = row.get("year")
    if year is not None:
        out["year"] = year
    return id_minimal(out)


def _find_watchlist(adapter: Any) -> str | None:
    cfg = cfg_section(adapter)
    configured = str(cfg.get("watchlist_list_id") or "").strip()
    if configured:
        return configured
    configured_name = str(
        cfg.get("watchlist_name")
        or getattr(getattr(adapter, "cfg", None), "watchlist_name", "")
        or "Watchlist"
    ).strip() or "Watchlist"
    configured_name_l = configured_name.lower()

    data = adapter.client.get_json("/api/external/lists", params={"page": 1, "perPage": 500})
    rows = data.get("items") if isinstance(data, Mapping) else None
    if not isinstance(rows, list):
        return None

    typed_watchlist: str | None = None
    fallback: str | None = None
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        rid = str(row.get("id") or "").strip()
        if not rid:
            continue
        typ = str(row.get("type") or "").strip().lower()
        name = str(row.get("name") or "").strip().lower()
        if name == configured_name_l:
            return rid
        if typ == "watchlist":
            typed_watchlist = typed_watchlist or rid
        if name in ("crosswatch", "crosswatch watchlist", "watchlist"):
            fallback = fallback or rid
    return typed_watchlist or fallback


def _ensure_watchlist(adapter: Any) -> str | None:
    found = _find_watchlist(adapter)
    if found:
        return found
    cfg = cfg_section(adapter)
    if cfg.get("watchlist_auto_create") is False:
        return None
    name = str(
        cfg.get("watchlist_name")
        or getattr(getattr(adapter, "cfg", None), "watchlist_name", "")
        or "Watchlist"
    ).strip() or "Watchlist"
    data = adapter.client.post_json(
        "/api/external/lists",
        json={"name": name, "description": "Managed by CrossWatch", "is_public": False, "type": "watchlist"},
    )
    item = data.get("item") if isinstance(data, Mapping) else None
    if isinstance(item, Mapping):
        return str(item.get("id") or "").strip() or None
    return None


def _fetch_all_items(adapter: Any, list_id: str) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    out: dict[str, dict[str, Any]] = {}
    remote_ids: dict[str, str] = {}
    page = 1
    per_page = int(getattr(adapter.cfg, "watchlist_page_size", 100) or 100)
    per_page = max(1, min(per_page, 500))
    while True:
        data = adapter.client.get_json(
            f"/api/external/lists/{list_id}/items",
            params={"page": page, "perPage": per_page},
        )
        rows = data.get("items") if isinstance(data, Mapping) else None
        if not isinstance(rows, list) or not rows:
            break
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            mini = _to_minimal(row)
            if not mini:
                continue
            key = _item_key(mini)
            out[key] = mini
            rid = str(row.get("id") or "").strip()
            if rid:
                remote_ids[key] = rid
        total_pages = int(data.get("totalPages") or data.get("total_pages") or page)
        if page >= total_pages:
            break
        page += 1
    return out, remote_ids


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    list_id = _ensure_watchlist(adapter)
    if not list_id:
        _warn("index_skipped", reason="missing_watchlist_list")
        return {}
    items, remote_ids = _fetch_all_items(adapter, list_id)
    _shadow_save(remote_ids, list_id=list_id)
    _info("index_done", count=len(items), list_id=list_id)
    return items


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    list_id = _ensure_watchlist(adapter)
    items_list = list(items or [])
    if not list_id:
        return 0, [{"item": id_minimal(it), "hint": "missing_watchlist_list"} for it in items_list]

    shadow = _shadow_load()
    remote_ids: dict[str, str] = dict(shadow.get("items") or {})
    unresolved: list[dict[str, Any]] = []
    ok = 0
    for it in items_list:
        mini = id_minimal(it)
        key = _item_key(mini)
        tmdb = tmdb_id_for_item(mini)
        if tmdb is None:
            unresolved.append({"item": mini, "hint": "missing_tmdb_id"})
            continue
        if key in remote_ids:
            continue
        media = media_type_for_item(mini)
        r = adapter.client.post(
            f"/api/external/lists/{list_id}/items",
            json={"tmdb_id": int(tmdb), "media_type": media},
        )
        if 200 <= r.status_code < 300:
            data = adapter.client.safe_json(r)
            item = data.get("item") if isinstance(data, Mapping) else None
            rid = str(item.get("id") or "").strip() if isinstance(item, Mapping) else ""
            if rid:
                remote_ids[key] = rid
            ok += 1
        else:
            _warn("write_failed", op="add", status=r.status_code, body=(r.text or "")[:200])
            unresolved.append({"item": mini, "hint": f"http:{r.status_code}"})
    _shadow_save(remote_ids, list_id=list_id)
    _info("write_done", op="add", applied=ok, unresolved=len(unresolved))
    return ok, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    list_id = _ensure_watchlist(adapter)
    items_list = list(items or [])
    if not list_id:
        return 0, [{"item": id_minimal(it), "hint": "missing_watchlist_list"} for it in items_list]

    shadow = _shadow_load()
    remote_ids: dict[str, str] = dict(shadow.get("items") or {})
    if not remote_ids:
        _, remote_ids = _fetch_all_items(adapter, list_id)

    unresolved: list[dict[str, Any]] = []
    ok = 0
    for it in items_list:
        mini = id_minimal(it)
        key = _item_key(mini)
        item_id = str(remote_ids.get(key) or "").strip()
        if not item_id:
            unresolved.append({"item": mini, "hint": "missing_remote_item_id"})
            continue
        r = adapter.client.delete(f"/api/external/lists/{list_id}/items/{item_id}")
        if 200 <= r.status_code < 300:
            remote_ids.pop(key, None)
            ok += 1
        else:
            _warn("write_failed", op="remove", status=r.status_code, body=(r.text or "")[:200])
            unresolved.append({"item": mini, "hint": f"http:{r.status_code}"})
    _shadow_save(remote_ids, list_id=list_id)
    _info("write_done", op="remove", applied=ok, unresolved=len(unresolved))
    return ok, unresolved
