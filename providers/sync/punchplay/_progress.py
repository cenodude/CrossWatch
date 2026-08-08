# /providers/sync/punchplay/_progress.py
# PunchPlay playback progress sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal

from ._common import (
    URL_IN_PROGRESS,
    URL_IN_PROGRESS_ITEM,
    URL_PLAYBACK,
    error_of,
    has_write_id,
    ids_for_punchplay,
    iso_z,
    punchplay_request,
    request_id_of,
    safe_json,
    _dbg,
    _info,
    _warn,
)

FEATURE = "progress"

PROGRESS_ID_FIELD = "_punchplay_progress_id"


def _key_of(obj: Mapping[str, Any]) -> str:
    try:
        return str(canonical_key(id_minimal(obj)) or "").strip()
    except Exception:
        return ""


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _row_to_minimal(row: Mapping[str, Any]) -> dict[str, Any] | None:
    typ = str(row.get("type") or "").strip().lower()

    if typ == "episode":
        show_tmdb = _as_int(row.get("showTmdbId"))
        season = _as_int(row.get("season"))
        episode = _as_int(row.get("episode"))
        if not show_tmdb or season is None or episode is None:
            return None
        out: dict[str, Any] = {
            "type": "episode",
            "show_ids": {"tmdb": str(show_tmdb)},
            "ids": {},
            "season": season,
            "episode": episode,
        }
        ep_tmdb = _as_int(row.get("tmdbId"))
        if ep_tmdb:
            out["ids"] = {"tmdb": str(ep_tmdb)}
        show_title = str(row.get("showTitle") or "").strip()
        if show_title:
            out["series_title"] = show_title
        ep_title = str(row.get("episodeTitle") or "").strip()
        if ep_title:
            out["title"] = ep_title
    else:
        tmdb = _as_int(row.get("tmdbId"))
        if not tmdb:
            return None
        out = {"type": "movie", "ids": {"tmdb": str(tmdb)}}
        title = str(row.get("title") or "").strip()
        if title:
            out["title"] = title
        year = _as_int(row.get("year"))
        if year:
            out["year"] = year

    percent = _as_float(row.get("progressPercent"))
    if percent is not None:
        out["progress"] = max(0.0, min(100.0, percent))
    pos = _as_float(row.get("progressSeconds"))
    if pos is not None:
        out["position_seconds"] = pos
    dur = _as_float(row.get("durationSeconds"))
    if dur:
        out["duration_seconds"] = dur
    updated = iso_z(row.get("updatedAt"))
    if updated:
        out["updated_at"] = updated
    state = str(row.get("playbackState") or "").strip().lower()
    if state:
        out["playback_state"] = state
    if row.get("nowPlaying") is True:
        out["now_playing"] = True

    entry_id = _as_int(row.get("id"))
    if entry_id:
        out[PROGRESS_ID_FIELD] = str(entry_id)
    return out


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    resp = punchplay_request(adapter, "GET", URL_IN_PROGRESS)
    if resp.status_code != 200:
        _warn(FEATURE, "in_progress_fetch_failed", status=resp.status_code, error=error_of(resp), request_id=request_id_of(resp))
        return {}

    data = safe_json(resp)
    rows: list[Mapping[str, Any]] = []
    if isinstance(data, Mapping):
        raw = data.get("items")
        rows = [r for r in raw if isinstance(r, Mapping)] if isinstance(raw, list) else []
    elif isinstance(data, list):
        rows = [r for r in data if isinstance(r, Mapping)]

    collected: dict[str, dict[str, Any]] = {}
    for row in rows:
        minimal = _row_to_minimal(row)
        if not minimal:
            continue
        key = _key_of(minimal)
        if key:
            collected[key] = minimal

    _info(FEATURE, "index_done", count=len(collected))
    return collected


def _playback_payload(item: Mapping[str, Any]) -> dict[str, Any] | None:
    typ = str(item.get("type") or "").strip().lower()
    is_episode = typ == "episode"

    source = item
    if is_episode and isinstance(item.get("show_ids"), Mapping) and item.get("show_ids"):
        source = {"ids": item.get("show_ids")}
    ids = ids_for_punchplay(source)
    if not has_write_id(ids):
        return None

    payload: dict[str, Any] = {"media_type": "episode" if is_episode else "movie"}
    if ids.get("tmdb_id"):
        payload["tmdb_id"] = ids["tmdb_id"]
    if ids.get("imdb_id"):
        payload["imdb_id"] = ids["imdb_id"]
    if ids.get("tvdb_id"):
        payload["tvdb_id"] = ids["tvdb_id"]

    if is_episode:
        season = _as_int(item.get("season"))
        episode = _as_int(item.get("episode"))
        if season is None or episode is None:
            return None
        payload["season"] = season
        payload["episode"] = episode
        series_title = str(item.get("series_title") or "").strip()
        if series_title:
            payload["title"] = series_title
        ep_title = str(item.get("title") or "").strip()
        if ep_title:
            payload["episode_title"] = ep_title
    else:
        title = str(item.get("title") or "").strip()
        if title:
            payload["title"] = title
        year = _as_int(item.get("year"))
        if year:
            payload["year"] = year

    pos = _as_float(item.get("position_seconds"))
    if pos is not None:
        payload["position_seconds"] = pos
    dur = _as_float(item.get("duration_seconds"))
    if dur:
        payload["duration_seconds"] = dur

    percent = _as_float(item.get("progress"))
    if percent is not None:
        frac = percent / 100.0 if percent > 1.0 else percent
        payload["progress"] = max(0.0, min(1.0, frac))
    elif pos is not None and dur:
        payload["progress"] = max(0.0, min(1.0, pos / dur))

    if item.get("anime") is True:
        payload["anime"] = True
    return payload


def _send(adapter: Any, action: str, payload: Mapping[str, Any]) -> Any:
    return punchplay_request(adapter, "POST", URL_PLAYBACK.format(action=action), json=dict(payload))


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    ok = True

    for item in items or []:
        key = _key_of(item)
        if not key:
            continue
        payload = _playback_payload(item)
        if payload is None:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "missing_supported_id_or_position"})
            _dbg(FEATURE, "item_unresolved_before_write", key=key)
            continue

        resp = _send(adapter, "progress", payload)
        if 200 <= resp.status_code < 300:
            confirmed.append(key)
        else:
            ok = False
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": f"http:{resp.status_code}", "error": error_of(resp)})
            _warn(FEATURE, "progress_write_failed", key=key, status=resp.status_code, error=error_of(resp), request_id=request_id_of(resp))

    _info(FEATURE, "write_done", action="add", confirmed=len(confirmed), unresolved=len(unresolved_keys))
    return {
        "ok": ok,
        "confirmed_keys": confirmed,
        "unresolved_keys": unresolved_keys,
        "deferred_keys": [],
        "unresolved": unresolved,
    }


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    ok = True

    for item in items or []:
        key = _key_of(item)
        if not key:
            continue
        entry_id = str(item.get(PROGRESS_ID_FIELD) or "").strip()
        if not entry_id:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "missing_in_progress_id"})
            _dbg(FEATURE, "item_unresolved_before_write", key=key, reason="dismiss_needs_entry_id")
            continue

        resp = punchplay_request(adapter, "DELETE", URL_IN_PROGRESS_ITEM.format(entry_id=entry_id))
        if 200 <= resp.status_code < 300 or resp.status_code == 404:
            confirmed.append(key)
        else:
            ok = False
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": f"http:{resp.status_code}", "error": error_of(resp)})
            _warn(FEATURE, "progress_dismiss_failed", key=key, status=resp.status_code, error=error_of(resp), request_id=request_id_of(resp))

    _info(FEATURE, "write_done", action="remove", confirmed=len(confirmed), unresolved=len(unresolved_keys))
    return {
        "ok": ok,
        "confirmed_keys": confirmed,
        "unresolved_keys": unresolved_keys,
        "deferred_keys": [],
        "unresolved": unresolved,
    }
