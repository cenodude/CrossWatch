# /providers/sync/publicmetadb/_history.py
# PUBLICMETADB Module for history functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal

from .._log import log as cw_log
from ._common import as_int, read_json, state_file, tmdb_id_for_item, write_json


def _dbg(event: str, **fields: Any) -> None:
    cw_log("PUBLICMETADB", "history", "debug", event, **fields)


def _info(event: str, **fields: Any) -> None:
    cw_log("PUBLICMETADB", "history", "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log("PUBLICMETADB", "history", "warn", event, **fields)


def _shadow_path():
    return state_file("publicmetadb_history.shadow.json")


def _shadow_load() -> dict[str, Any]:
    doc = read_json(_shadow_path())
    if not isinstance(doc.get("items"), dict):
        doc["items"] = {}
    return doc


def _shadow_save(items: Mapping[str, Any]) -> None:
    write_json(_shadow_path(), {"items": dict(items)})


def _iso_epoch(value: Any) -> int | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        if s.isdigit():
            n = int(s)
            return n // 1000 if len(s) >= 13 else n
        iso = s.replace("Z", "+00:00")
        tail = iso[10:] if len(iso) > 10 else ""
        if "T" in iso and "+" not in tail and "-" not in tail:
            iso = iso + "+00:00"
        return int(datetime.fromisoformat(iso).timestamp())
    except Exception:
        return None


def _iso_z(value: Any) -> str | None:
    epoch = _iso_epoch(value)
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _event_key(item: Mapping[str, Any]) -> str:
    mini = id_minimal(item)
    base = canonical_key(mini)
    watched_at = _iso_z(mini.get("watched_at") or item.get("watched_at") or item.get("last_watched_at"))
    if watched_at:
        return f"{base}@{_iso_epoch(watched_at)}"
    hist_id = str(item.get("history_id") or item.get("_publicmetadb_history_id") or "").strip()
    return f"{base}@id:{hist_id}" if hist_id else base


def _show_tmdb_for_item(item: Mapping[str, Any]) -> int | None:
    show_ids_obj = item.get("show_ids")
    show_ids: Mapping[str, Any] = show_ids_obj if isinstance(show_ids_obj, Mapping) else {}
    return as_int(show_ids.get("tmdb")) or tmdb_id_for_item(item)


def _history_id(item: Mapping[str, Any]) -> str | None:
    hid = str(item.get("_publicmetadb_history_id") or item.get("history_id") or "").strip()
    return hid or None


def _rows(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    if not isinstance(data, Mapping):
        return []
    for key in ("items", "results", "data"):
        val = data.get(key)
        if isinstance(val, list):
            return val
    return []


def _to_minimal(row: Mapping[str, Any]) -> dict[str, Any] | None:
    tmdb = tmdb_id_for_item(row)
    if tmdb is None:
        return None
    media = str(row.get("media_type") or row.get("type") or "").strip().lower()
    watched_at = _iso_z(row.get("watched_at") or row.get("watchedAt") or row.get("last_watched_at"))
    hist_id = str(row.get("id") or row.get("history_id") or "").strip()

    if media in ("tv", "show", "series", "episode"):
        season_raw = row.get("season") if row.get("season") is not None else row.get("season_number")
        episode_raw = row.get("episode") if row.get("episode") is not None else row.get("episode_number")
        season = as_int(season_raw)
        episode = as_int(episode_raw)
        if season is None or episode is None:
            return None
        out: dict[str, Any] = {
            "type": "episode",
            "show_ids": {"tmdb": str(tmdb)},
            "season": season,
            "episode": episode,
        }
        title = str(row.get("series_title") or row.get("show_title") or row.get("name") or "").strip()
        if title:
            out["series_title"] = title
    else:
        out = {"type": "movie", "ids": {"tmdb": str(tmdb)}}
        title = str(row.get("title") or row.get("name") or "").strip()
        if title:
            out["title"] = title
        year = row.get("year")
        if year is not None:
            out["year"] = year

    if watched_at:
        out["watched"] = True
        out["watched_at"] = watched_at
    if hist_id:
        out["history_id"] = hist_id
        out["_publicmetadb_history_id"] = hist_id
    return id_minimal(out)


def _fetch_all_items(adapter: Any) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    out: dict[str, dict[str, Any]] = {}
    remote_ids: dict[str, str] = {}
    page = 1
    per_page = int(getattr(adapter.cfg, "history_per_page", 100) or 100)
    per_page = max(1, min(per_page, 500))
    max_pages = int(getattr(adapter.cfg, "history_max_pages", 1000) or 1000)
    max_pages = max(1, max_pages)

    while page <= max_pages:
        data = adapter.client.get_json(
            "/api/external/watched",
            params={"page": page, "perPage": per_page},
        )
        rows = _rows(data)
        if not rows:
            break
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            mini = _to_minimal(row)
            if not mini:
                continue
            key = _event_key(mini)
            out[key] = mini
            rid = _history_id(mini) or str(row.get("id") or "").strip()
            if rid:
                remote_ids[key] = rid
        total_pages = int(data.get("totalPages") or data.get("total_pages") or page) if isinstance(data, Mapping) else page
        if page >= total_pages:
            break
        page += 1
    if page > max_pages:
        _warn("index_limited", max_pages=max_pages, per_page=per_page)
    return out, remote_ids


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    items, remote_ids = _fetch_all_items(adapter)
    _shadow_save(remote_ids)
    _info("index_done", count=len(items))
    return items


def _payload_for_item(item: Mapping[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    mini = id_minimal(item)
    watched_at = _iso_z(mini.get("watched_at") or item.get("watched_at") or item.get("last_watched_at"))
    if not watched_at:
        return None, "missing_watched_at"
    typ = str(mini.get("type") or item.get("type") or "").strip().lower()
    if typ == "episode":
        tmdb = _show_tmdb_for_item(mini) or _show_tmdb_for_item(item)
        season_raw = mini.get("season") if mini.get("season") is not None else item.get("season")
        episode_raw = mini.get("episode") if mini.get("episode") is not None else item.get("episode")
        season = as_int(season_raw)
        episode = as_int(episode_raw)
        if tmdb is None or season is None or episode is None:
            return None, "missing_show_tmdb_or_episode_numbers"
        return {
            "tmdb_id": int(tmdb),
            "media_type": "tv",
            "season": int(season),
            "episode": int(episode),
            "watched_at": watched_at,
        }, None

    tmdb = tmdb_id_for_item(mini) or tmdb_id_for_item(item)
    if tmdb is None:
        return None, "missing_tmdb_id"
    return {"tmdb_id": int(tmdb), "media_type": "movie", "watched_at": watched_at}, None


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    items_list = list(items or [])
    shadow = _shadow_load()
    remote_ids: dict[str, str] = dict(shadow.get("items") or {})
    unresolved: list[dict[str, Any]] = []
    ok = 0

    for it in items_list:
        mini = id_minimal(it)
        key = _event_key(mini)
        if key in remote_ids:
            continue
        payload, hint = _payload_for_item(it)
        if payload is None:
            unresolved.append({"item": mini, "hint": hint or "unsupported_history_item"})
            continue
        r = adapter.client.post("/api/external/watched", json=payload)
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
    _shadow_save(remote_ids)
    _info("write_done", op="add", applied=ok, unresolved=len(unresolved))
    return ok, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    items_list = list(items or [])
    shadow = _shadow_load()
    remote_ids: dict[str, str] = dict(shadow.get("items") or {})
    if not remote_ids:
        _, remote_ids = _fetch_all_items(adapter)

    unresolved: list[dict[str, Any]] = []
    ok = 0
    for it in items_list:
        mini = id_minimal(it)
        key = _event_key(mini)
        history_id = _history_id(it) or _history_id(mini) or str(remote_ids.get(key) or "").strip()
        if not history_id:
            unresolved.append({"item": mini, "hint": "missing_remote_history_id"})
            continue
        r = adapter.client.delete(f"/api/external/watched/{history_id}")
        if 200 <= r.status_code < 300:
            remote_ids.pop(key, None)
            ok += 1
        else:
            _warn("write_failed", op="remove", status=r.status_code, body=(r.text or "")[:200])
            unresolved.append({"item": mini, "hint": f"http:{r.status_code}"})
    _shadow_save(remote_ids)
    _info("write_done", op="remove", applied=ok, unresolved=len(unresolved))
    return ok, unresolved
