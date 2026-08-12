# /providers/sync/scrob/_history.py
# Scrob history sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal

from ._common import (
    HISTORY_PAGE_MAX,
    MEDIA_TYPE_EPISODE,
    PATH_HISTORY,
    PATH_HISTORY_EVENT,
    PATH_HISTORY_ITEM,
    as_int,
    cfg_int,
    cfg_section,
    epoch_of,
    error_of,
    ids_for_scrob,
    iso_z,
    item_from_media,
    mapping,
    media_type_of,
    not_future,
    ok_status,
    paged_get,
    positive_int,
    scrob_request,
    show_ids_for_scrob,
    _dbg,
    _info,
    _warn,
)

FEATURE = "history"

HISTORY_ID_FIELD = "_scrob_history_id"
MEDIA_ID_FIELD = "_scrob_media_id"


def _key_of(obj: Mapping[str, Any]) -> str:
    try:
        return str(canonical_key(id_minimal(obj)) or "").strip()
    except Exception:
        return ""


def _base_key(item: Mapping[str, Any]) -> str:
    if media_type_of(item) == MEDIA_TYPE_EPISODE and isinstance(item.get("show_ids"), Mapping):
        season = as_int(item.get("season"))
        episode = as_int(item.get("episode"))
        if item.get("show_ids") and season is not None and episode is not None:
            return _key_of(
                {
                    "type": MEDIA_TYPE_EPISODE,
                    "show_ids": item.get("show_ids"),
                    "season": season,
                    "episode": episode,
                }
            )
    return _key_of(item)


def _row_to_minimal(row: Mapping[str, Any]) -> dict[str, Any] | None:
    if not bool(row.get("completed", True)):
        return None
    media = mapping(row.get("media"))
    out = item_from_media(media)
    if not out:
        return None

    watched = iso_z(row.get("watched_at"))
    if watched:
        out["watched_at"] = watched

    event_id = positive_int(row.get("id"))
    if event_id:
        out[HISTORY_ID_FIELD] = str(event_id)
    media_id = positive_int(media.get("id"))
    if media_id:
        out[MEDIA_ID_FIELD] = str(media_id)
    return out


def _event_key(item: Mapping[str, Any]) -> str:
    base = _base_key(item)
    if not base:
        return ""
    ts = epoch_of(item.get("watched_at"))
    if ts is not None:
        return f"{base}@{ts}"
    event_id = str(item.get(HISTORY_ID_FIELD) or "").strip()
    return f"{base}@id:{event_id}" if event_id else base


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    section = cfg_section(adapter) or {}
    per_page = max(1, min(cfg_int(section, "history_per_page", HISTORY_PAGE_MAX), HISTORY_PAGE_MAX))
    max_pages = cfg_int(section, "history_max_pages", 500)

    rows = paged_get(adapter, PATH_HISTORY, feature=FEATURE, page_size=per_page, max_pages=max_pages)

    collected: dict[str, dict[str, Any]] = {}
    skipped = 0
    for row in rows:
        minimal = _row_to_minimal(row)
        if not minimal:
            skipped += 1
            continue
        key = _event_key(minimal)
        if not key:
            skipped += 1
            continue
        if key in collected:
            event_id = str(minimal.get(HISTORY_ID_FIELD) or "").strip()
            key = f"{key}~h{event_id}" if event_id else f"{key}~dup"
            suffix = 2
            while key in collected:
                key = f"{key}{suffix}"
                suffix += 1
        collected[key] = minimal

    _info(FEATURE, "index_done", count=len(collected), rows=len(rows), skipped=skipped)
    return collected


def _payload_for(item: Mapping[str, Any]) -> dict[str, Any] | None:
    kind = media_type_of(item)
    payload: dict[str, Any] = {"completed": True}

    watched = not_future(iso_z(item.get("watched_at")))
    if watched:
        payload["watched_at"] = watched

    if kind == MEDIA_TYPE_EPISODE:
        season = as_int(item.get("season"))
        episode = as_int(item.get("episode"))
        show_ids = show_ids_for_scrob(item)
        series_tmdb = positive_int(show_ids.get("tmdb_id"))
        if season is None or episode is None or episode < 1 or not series_tmdb:
            return None
        payload["media_type"] = MEDIA_TYPE_EPISODE
        payload["series_tmdb_id"] = series_tmdb
        payload["season_number"] = season
        payload["episode_number"] = episode
        series_tvdb = positive_int(show_ids.get("tvdb_id"))
        if series_tvdb:
            payload["series_tvdb_id"] = series_tvdb
        payload["tmdb_id"] = positive_int(ids_for_scrob(item).get("tmdb_id")) or 0
        return payload

    tmdb = positive_int(ids_for_scrob(item).get("tmdb_id"))
    if not tmdb:
        return None
    payload["media_type"] = "movie"
    payload["tmdb_id"] = tmdb
    return payload


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    planned = 0
    ok = True

    for item in items or []:
        key = str(item.get("_cw_event_key") or "").strip() or _event_key(item)
        payload = _payload_for(item)
        if payload is None:
            if key:
                unresolved_keys.append(key)
                unresolved.append({"key": key, "status": "missing_supported_id"})
                _dbg(FEATURE, "item_unresolved_before_write", key=key)
            continue

        planned += 1
        if dry_run:
            confirmed.append(key)
            continue

        resp = scrob_request(adapter, "POST", PATH_HISTORY, json=payload)
        if ok_status(resp):
            confirmed.append(key)
            continue
        ok = False
        unresolved_keys.append(key)
        unresolved.append({"key": key, "status": f"http:{int(resp.status_code)}", "error": error_of(resp)})
        _warn(FEATURE, "history_add_failed", key=key, status=int(resp.status_code), error=error_of(resp))

    _dbg(FEATURE, "write_prepare", action="add", items=planned, skipped=len(unresolved_keys))
    _info(FEATURE, "write_done", action="add", confirmed=len(confirmed), unresolved=len(unresolved_keys), dry_run=bool(dry_run))
    return {
        "ok": ok,
        "count": len(confirmed),
        "confirmed_keys": confirmed,
        "unresolved_keys": unresolved_keys,
        "deferred_keys": [],
        "unresolved": unresolved,
    }


def _delete_event(adapter: Any, event_id: str) -> tuple[bool, str]:
    resp = scrob_request(adapter, "DELETE", PATH_HISTORY_EVENT.format(event_id=event_id))
    code = int(resp.status_code)
    if ok_status(resp) or code == 404:
        return True, ""
    return False, error_of(resp) or f"http:{code}"


def _delete_item(adapter: Any, item: Mapping[str, Any]) -> tuple[bool, str]:
    kind = media_type_of(item)
    params: dict[str, Any] = {"media_type": kind}
    media_id = positive_int(item.get(MEDIA_ID_FIELD))
    if media_id:
        params["id"] = media_id
    else:
        tmdb = positive_int(ids_for_scrob(item).get("tmdb_id"))
        if not tmdb:
            return False, "missing_supported_id"
        params["tmdb_id"] = tmdb
    resp = scrob_request(adapter, "DELETE", PATH_HISTORY_ITEM, params=params)
    code = int(resp.status_code)
    if ok_status(resp) or code == 404:
        return True, ""
    return False, error_of(resp) or f"http:{code}"


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    ok = True

    for item in items or []:
        key = str(item.get("_cw_event_key") or "").strip() or _event_key(item)
        if not key:
            continue
        if dry_run:
            confirmed.append(key)
            continue

        event_id = str(item.get(HISTORY_ID_FIELD) or "").strip()
        if event_id:
            done, reason = _delete_event(adapter, event_id)
        else:
            done, reason = _delete_item(adapter, item)

        if done:
            confirmed.append(key)
            continue
        ok = False
        unresolved_keys.append(key)
        unresolved.append({"key": key, "status": reason or "delete_failed"})
        _warn(FEATURE, "history_delete_failed", key=key, reason=reason)

    _info(FEATURE, "write_done", action="remove", confirmed=len(confirmed), unresolved=len(unresolved_keys), dry_run=bool(dry_run))
    return {
        "ok": ok,
        "count": len(confirmed),
        "confirmed_keys": confirmed,
        "unresolved_keys": unresolved_keys,
        "deferred_keys": [],
        "unresolved": unresolved,
    }
