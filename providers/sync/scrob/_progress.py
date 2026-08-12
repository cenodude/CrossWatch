# /providers/sync/scrob/_progress.py
# Scrob playback progress sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal

from ._common import (
    MEDIA_TYPE_EPISODE,
    PATH_CONTINUE_WATCHING,
    PATH_WEBHOOK_KODI,
    as_int,
    error_of,
    ids_for_scrob,
    iso_z,
    item_from_media,
    mapping,
    media_type_of,
    ok_status,
    positive_int,
    safe_json,
    scrob_request,
    show_ids_for_scrob,
    webhook_post,
    _dbg,
    _info,
    _warn,
)

FEATURE = "progress"

MEDIA_ID_FIELD = "_scrob_media_id"

PROGRESS_MIN_PERCENT = 5.0
PROGRESS_MAX_PERCENT = 90.0


def _key_of(obj: Mapping[str, Any]) -> str:
    try:
        return str(canonical_key(id_minimal(obj)) or "").strip()
    except Exception:
        return ""


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _first_number(item: Mapping[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = _as_float(item.get(key))
        if value is not None:
            return value
    return None


def _percent_of(item: Mapping[str, Any]) -> float | None:
    percent = _first_number(item, ("progress_percent", "percent"))
    if percent is not None:
        return max(0.0, min(100.0, percent))
    position = _first_number(item, ("progress_ms", "viewOffset"))
    duration = _first_number(item, ("duration_ms",))
    if position is not None and duration and duration > 0:
        return max(0.0, min(100.0, (position / duration) * 100.0))
    return None


def _row_to_minimal(row: Mapping[str, Any]) -> dict[str, Any] | None:
    media = mapping(row.get("media"))
    out = item_from_media(media)
    if not out:
        return None

    fraction = _as_float(row.get("progress_percent"))
    if fraction is None:
        return None
    percent = max(0.0, min(100.0, fraction * 100.0))
    out["progress_percent"] = round(percent, 3)

    seconds = _as_float(row.get("progress_seconds"))
    if seconds is not None and seconds >= 0:
        out["progress_ms"] = int(round(seconds * 1000.0))
        if percent > 0:
            out["duration_ms"] = int(round(seconds * 1000.0 / (percent / 100.0)))

    runtime = _as_float(media.get("runtime"))
    if runtime and runtime > 0 and not out.get("duration_ms"):
        out["duration_ms"] = int(round(runtime * 60_000.0))

    updated = iso_z(row.get("watched_at"))
    if updated:
        out["progress_at"] = updated
        out["progress_at_source"] = "scrob"

    media_id = positive_int(media.get("id"))
    if media_id:
        out[MEDIA_ID_FIELD] = str(media_id)
    return out


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    resp = scrob_request(adapter, "GET", PATH_CONTINUE_WATCHING)
    if not ok_status(resp):
        _warn(FEATURE, "fetch_failed", status=int(resp.status_code), error=error_of(resp))
        return {}
    data = safe_json(resp)
    rows = data.get("continue_watching") if isinstance(data, Mapping) else None
    rows = [r for r in rows if isinstance(r, Mapping)] if isinstance(rows, list) else []

    out: dict[str, dict[str, Any]] = {}
    skipped = 0
    for row in rows:
        minimal = _row_to_minimal(row)
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


def _kodi_item(item: Mapping[str, Any]) -> dict[str, Any] | None:
    kind = media_type_of(item)
    if kind == MEDIA_TYPE_EPISODE:
        season = as_int(item.get("season"))
        episode = as_int(item.get("episode"))
        show_ids = show_ids_for_scrob(item)
        if season is None or episode is None or not show_ids:
            return None
        unique: dict[str, Any] = {}
        if show_ids.get("tmdb_id"):
            unique["tmdb"] = str(show_ids["tmdb_id"])
        if show_ids.get("tvdb_id"):
            unique["tvdb"] = str(show_ids["tvdb_id"])
        if show_ids.get("imdb_id"):
            unique["imdb"] = str(show_ids["imdb_id"])
        out: dict[str, Any] = {
            "type": "episode",
            "season": season,
            "episode": episode,
            "uniqueid": unique,
        }
        series_title = str(item.get("series_title") or "").strip()
        if series_title:
            out["showtitle"] = series_title
        title = str(item.get("title") or "").strip()
        if title:
            out["title"] = title
        return out

    ids = ids_for_scrob(item)
    unique = {}
    if ids.get("tmdb_id"):
        unique["tmdb"] = str(ids["tmdb_id"])
    if ids.get("imdb_id"):
        unique["imdb"] = str(ids["imdb_id"])
    if not unique:
        return None
    out = {"type": "movie", "uniqueid": unique, "title": str(item.get("title") or "").strip()}
    year = as_int(item.get("year"))
    if year:
        out["year"] = year
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    planned = 0
    ok = True

    for item in items or []:
        key = _key_of(item)
        if not key:
            continue

        percent = _percent_of(item)
        duration_ms = _first_number(item, ("duration_ms",))
        kodi_item = _kodi_item(item)

        if kodi_item is None:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "missing_supported_id"})
            _dbg(FEATURE, "item_unresolved_before_write", key=key)
            continue
        if percent is None or not duration_ms or duration_ms <= 0:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "missing_duration_or_percent"})
            _dbg(FEATURE, "item_unresolved_before_write", key=key, reason="missing_duration_or_percent")
            continue
        if percent <= PROGRESS_MIN_PERCENT or percent >= PROGRESS_MAX_PERCENT:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "outside_supported_progress_window"})
            _dbg(FEATURE, "item_unresolved_before_write", key=key, reason="outside_supported_progress_window", percent=round(percent, 2))
            continue

        total_seconds = int(round(duration_ms / 1000.0))
        position_seconds = int(round(total_seconds * (percent / 100.0)))
        payload = {
            "event": "playback_stopped",
            "item": kodi_item,
            "position_seconds": position_seconds,
            "total_seconds": total_seconds,
            "session_id": f"crosswatch-progress-{key}",
        }

        planned += 1
        if dry_run:
            confirmed.append(key)
            continue

        try:
            resp = webhook_post(adapter, PATH_WEBHOOK_KODI, payload)
        except Exception as exc:
            ok = False
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": exc.__class__.__name__})
            _warn(FEATURE, "progress_write_failed", key=key, error=exc.__class__.__name__)
            continue

        body = safe_json(resp)
        status = str((body or {}).get("status") or "").lower() if isinstance(body, Mapping) else ""
        if ok_status(resp) and status == "ok":
            confirmed.append(key)
            continue
        ok = False
        unresolved_keys.append(key)
        unresolved.append({"key": key, "status": f"http:{int(resp.status_code)}", "error": error_of(resp) or status})
        _warn(FEATURE, "progress_write_failed", key=key, status=int(resp.status_code), error=error_of(resp) or status)

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


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    ok = True

    for item in items or []:
        key = _key_of(item)
        if not key:
            continue
        media_id = positive_int(item.get(MEDIA_ID_FIELD))
        if not media_id:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "missing_scrob_media_id"})
            _dbg(FEATURE, "item_unresolved_before_write", key=key, reason="missing_scrob_media_id")
            continue
        if dry_run:
            confirmed.append(key)
            continue

        resp = scrob_request(adapter, "DELETE", PATH_CONTINUE_WATCHING, params={"media_id": media_id})
        code = int(resp.status_code)
        if ok_status(resp) or code == 404:
            confirmed.append(key)
            continue
        ok = False
        unresolved_keys.append(key)
        unresolved.append({"key": key, "status": f"http:{code}", "error": error_of(resp)})
        _warn(FEATURE, "progress_delete_failed", key=key, status=code, error=error_of(resp))

    _info(FEATURE, "write_done", action="remove", confirmed=len(confirmed), unresolved=len(unresolved_keys), dry_run=bool(dry_run))
    return {
        "ok": ok,
        "count": len(confirmed),
        "confirmed_keys": confirmed,
        "unresolved_keys": unresolved_keys,
        "deferred_keys": [],
        "unresolved": unresolved,
    }
