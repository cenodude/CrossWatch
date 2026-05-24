# /providers/sync/publicmetadb/_progress.py
# PUBLICMETADB Module for progress functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal

from .._log import log as cw_log
from ._common import as_int, read_json, state_file, tmdb_id_for_item, write_json


def _dbg(event: str, **fields: Any) -> None:
    cw_log("PUBLICMETADB", "progress", "debug", event, **fields)


def _info(event: str, **fields: Any) -> None:
    cw_log("PUBLICMETADB", "progress", "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log("PUBLICMETADB", "progress", "warn", event, **fields)


def _shadow_path():
    return state_file("publicmetadb_progress.shadow.json")


def _shadow_load() -> dict[str, Any]:
    doc = read_json(_shadow_path())
    if not isinstance(doc.get("items"), dict):
        doc["items"] = {}
    return doc


def _shadow_save(items: Mapping[str, Any]) -> None:
    write_json(_shadow_path(), {"items": dict(items)})


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


def _show_tmdb_for_item(item: Mapping[str, Any]) -> int | None:
    show_ids_obj = item.get("show_ids")
    show_ids: Mapping[str, Any] = show_ids_obj if isinstance(show_ids_obj, Mapping) else {}
    return as_int(show_ids.get("tmdb")) or tmdb_id_for_item(item)


def _progress_ms(item: Mapping[str, Any]) -> int | None:
    for key in ("progress_ms", "progressMs", "position_ms", "positionMs", "viewOffset"):
        n = as_int(item.get(key))
        if n is not None:
            return n
    return None


def _duration_ms(item: Mapping[str, Any]) -> int | None:
    for key in ("duration_ms", "durationMs", "runtime_ms", "runtimeMs", "duration", "runtime"):
        n = as_int(item.get(key))
        if n is not None and n > 0:
            return n
    return None


def _resume_id(item: Mapping[str, Any]) -> str | None:
    rid = str(item.get("_publicmetadb_resume_id") or item.get("resume_id") or item.get("id") or "").strip()
    return rid or None


def _item_key(item: Mapping[str, Any]) -> str:
    return canonical_key(id_minimal(item))


def _to_minimal(row: Mapping[str, Any]) -> dict[str, Any] | None:
    tmdb = tmdb_id_for_item(row)
    if tmdb is None:
        return None

    pos_ms = _progress_ms(row)
    dur_ms = _duration_ms(row)
    if pos_ms is None and dur_ms:
        try:
            pct = float(str(row.get("progress")).strip())
            if pct > 0:
                pos_ms = int((pct / 100.0) * float(dur_ms))
        except Exception:
            pos_ms = None
    if pos_ms is None or pos_ms <= 0:
        return None

    media = str(row.get("media_type") or row.get("type") or "").strip().lower()
    if media in ("tv", "show", "series", "episode"):
        season = as_int(row.get("season") or row.get("season_number"))
        episode = as_int(row.get("episode") or row.get("episode_number"))
        if season is None or episode is None:
            return None
        out: dict[str, Any] = {
            "type": "episode",
            "show_ids": {"tmdb": str(tmdb)},
            "season": season,
            "episode": episode,
            "progress_ms": int(pos_ms),
        }
        title = str(row.get("series_title") or row.get("show_title") or row.get("name") or "").strip()
        if title:
            out["series_title"] = title
    else:
        out = {"type": "movie", "ids": {"tmdb": str(tmdb)}, "progress_ms": int(pos_ms)}
        title = str(row.get("title") or row.get("name") or "").strip()
        if title:
            out["title"] = title
        year = row.get("year")
        if year is not None:
            out["year"] = year

    if dur_ms is not None:
        out["duration_ms"] = int(dur_ms)

    updated = row.get("updated") or row.get("updated_at") or row.get("progress_at") or row.get("last_played")
    if updated:
        out["progress_at"] = str(updated)

    rid = _resume_id(row)
    mini = id_minimal(out)
    if rid:
        mini["resume_id"] = rid
        mini["_publicmetadb_resume_id"] = rid
    return mini


def _fetch_all_items(adapter: Any) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    out: dict[str, dict[str, Any]] = {}
    remote_ids: dict[str, str] = {}
    page = 1
    per_page = int(getattr(adapter.cfg, "progress_per_page", 100) or 100)
    per_page = max(1, min(per_page, 500))
    max_pages = int(getattr(adapter.cfg, "progress_max_pages", 1000) or 1000)
    max_pages = max(1, max_pages)

    while page <= max_pages:
        data = adapter.client.get_json(
            "/api/external/resume",
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
            key = _item_key(mini)
            out[key] = mini
            rid = _resume_id(mini) or str(row.get("id") or "").strip()
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
    pos_ms = _progress_ms(mini) or _progress_ms(item) or as_int(item.get("progress"))
    dur_ms = _duration_ms(mini) or _duration_ms(item)
    if pos_ms is None:
        return None, "missing_progress"
    if pos_ms <= 0:
        return None, "zero_progress"
    if dur_ms is None:
        return None, "missing_duration"

    typ = str(mini.get("type") or item.get("type") or "").strip().lower()
    if typ == "episode":
        tmdb = _show_tmdb_for_item(mini) or _show_tmdb_for_item(item)
        season = as_int(mini.get("season") or item.get("season"))
        episode = as_int(mini.get("episode") or item.get("episode"))
        if tmdb is None or season is None or episode is None:
            return None, "missing_show_tmdb_or_episode_numbers"
        return {
            "tmdb_id": int(tmdb),
            "media_type": "tv",
            "season": int(season),
            "episode": int(episode),
            "position_ms": int(pos_ms),
            "runtime_ms": int(dur_ms),
        }, None

    if typ not in ("movie", "movies"):
        return None, "unsupported_progress_type"

    tmdb = tmdb_id_for_item(mini) or tmdb_id_for_item(item)
    if tmdb is None:
        return None, "missing_tmdb_id"
    return {
        "tmdb_id": int(tmdb),
        "media_type": "movie",
        "position_ms": int(pos_ms),
        "runtime_ms": int(dur_ms),
    }, None


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    items_list = list(items or [])
    shadow = _shadow_load()
    remote_ids: dict[str, str] = dict(shadow.get("items") or {})
    unresolved: list[dict[str, Any]] = []
    ok = 0

    for it in items_list:
        mini = id_minimal(it)
        payload, hint = _payload_for_item(it)
        if payload is None:
            unresolved.append({"item": mini, "hint": hint or "unsupported_progress_item"})
            continue

        r = adapter.client.post("/api/external/resume", json=payload)
        if 200 <= r.status_code < 300:
            data = adapter.client.safe_json(r)
            item = data.get("item") if isinstance(data, Mapping) else None
            action = str(data.get("action") or "").strip().lower() if isinstance(data, Mapping) else ""
            key = _item_key(mini)
            if isinstance(item, Mapping):
                rid = str(item.get("id") or "").strip()
                if rid:
                    remote_ids[key] = rid
            elif action in ("completed", "ignored"):
                remote_ids.pop(key, None)
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
        key = _item_key(mini)
        resume_id = _resume_id(it) or _resume_id(mini) or str(remote_ids.get(key) or "").strip()
        if not resume_id:
            unresolved.append({"item": mini, "hint": "missing_remote_resume_id"})
            continue
        r = adapter.client.delete(f"/api/external/resume/{resume_id}")
        if 200 <= r.status_code < 300:
            remote_ids.pop(key, None)
            ok += 1
        else:
            _warn("write_failed", op="remove", status=r.status_code, body=(r.text or "")[:200])
            unresolved.append({"item": mini, "hint": f"http:{r.status_code}"})

    _shadow_save(remote_ids)
    _info("write_done", op="remove", applied=ok, unresolved=len(unresolved))
    return ok, unresolved
