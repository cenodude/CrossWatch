# /providers/sync/punchplay/_progress.py
# PunchPlay playback progress sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
import secrets
from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal

from ._common import (
    URL_IN_PROGRESS,
    cfg_section,
    instance_id,
    URL_IN_PROGRESS_ITEM,
    URL_PLAYBACK,
    error_of,
    has_write_id,
    ids_for_punchplay,
    iso_z,
    punchplay_request,
    request_id_of,
    safe_json,
    snapshot_pages,
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


def _first_number(item: Mapping[str, Any], keys: tuple[str, ...]) -> float | None:
    for k in keys:
        v = _as_float(item.get(k))
        if v is not None:
            return v
    return None


def _position_ms(item: Mapping[str, Any]) -> float | None:
    ms = _first_number(item, ("progress_ms", "progressMs", "viewOffset"))
    if ms is not None:
        return ms
    secs = _first_number(item, ("position_seconds", "positionSeconds"))
    return secs * 1000.0 if secs is not None else None


def _duration_ms_of(item: Mapping[str, Any]) -> float | None:
    ms = _first_number(item, ("duration_ms", "durationMs"))
    if ms is not None and ms > 0:
        return ms
    secs = _first_number(item, ("duration_seconds", "durationSeconds"))
    return secs * 1000.0 if secs is not None and secs > 0 else None


def _percent_of(item: Mapping[str, Any]) -> float | None:
    pct = _first_number(item, ("progress_percent", "progressPercent", "percent", "position_percent", "resume_percent"))
    if pct is not None:
        return max(0.0, min(100.0, pct))
    pos = _position_ms(item)
    dur = _duration_ms_of(item)
    if pos is not None and dur:
        return max(0.0, min(100.0, (pos / dur) * 100.0))
    return None


def _drop_reason(row: Mapping[str, Any]) -> str:
    typ = str(row.get("type") or "").strip().lower()
    if not typ:
        return "show_row_not_a_playback_item" if _as_int(row.get("showTmdbId")) else "missing_type"
    if typ == "episode":
        if not _as_int(row.get("showTmdbId")):
            return "episode_missing_show_tmdb_id"
        if _as_int(row.get("season")) is None:
            return "episode_missing_season"
        if _as_int(row.get("episode")) is None:
            return "episode_missing_episode"
        return "episode_unknown"
    if not _as_int(row.get("tmdbId")):
        return "movie_missing_tmdb_id"
    return f"unhandled_type:{typ or 'empty'}"


def _tmdb_metadata_provider(adapter: Any) -> Any | None:
    cached = getattr(adapter, "_punchplay_progress_tmdb_metadata_provider", None)
    if cached is not None:
        return cached
    cfg = getattr(adapter, "config", None) or getattr(adapter, "raw_cfg", None) or {}
    if not isinstance(cfg, Mapping):
        return None
    tmdb_obj = cfg.get("tmdb")
    tmdb: Mapping[str, Any] = tmdb_obj if isinstance(tmdb_obj, Mapping) else {}
    metadata_obj = cfg.get("metadata")
    metadata: Mapping[str, Any] = metadata_obj if isinstance(metadata_obj, Mapping) else {}
    if not str(tmdb.get("api_key") or metadata.get("tmdb_api_key") or "").strip():
        return None
    try:
        from providers.metadata._meta_TMDB import TmdbProvider

        provider = TmdbProvider(lambda: dict(cfg), lambda _cfg: None)
    except Exception:
        return None
    try:
        setattr(adapter, "_punchplay_progress_tmdb_metadata_provider", provider)
    except Exception:
        pass
    return provider


def _metadata_show_detail(adapter: Any, show_ids: Mapping[str, Any]) -> dict[str, Any]:
    provider = _tmdb_metadata_provider(adapter)
    fetch_ids = {
        key: str(show_ids.get(key) or "").strip()
        for key in ("tmdb", "imdb", "tvdb")
        if str(show_ids.get(key) or "").strip()
    }
    if provider is None or not fetch_ids:
        return {}
    try:
        detail = provider.fetch(entity="tv", ids=fetch_ids, need={"poster": False, "backdrop": False, "ids": False})
    except Exception:
        return {}
    if not isinstance(detail, Mapping):
        return {}
    out: dict[str, Any] = {}
    title = str(detail.get("title") or "").strip()
    if title:
        out["title"] = title
    year = _as_int(detail.get("year"))
    if year is not None:
        out["year"] = year
    return out


def _row_to_minimal(row: Mapping[str, Any], adapter: Any | None = None) -> dict[str, Any] | None:
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
        elif adapter is not None:
            detail = _metadata_show_detail(adapter, out["show_ids"])
            if detail.get("title"):
                out["series_title"] = detail["title"]
            if detail.get("year") is not None:
                out["series_year"] = detail["year"]
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

    pos = _as_float(row.get("progressSeconds"))
    if pos is not None:
        out["progress_ms"] = int(round(pos * 1000.0))
    dur = _as_float(row.get("durationSeconds"))
    if dur:
        out["duration_ms"] = int(round(dur * 1000.0))
    percent = _as_float(row.get("progressPercent"))
    if percent is None and pos is not None and dur:
        percent = (pos / dur) * 100.0
    if percent is not None:
        out["progress_percent"] = round(max(0.0, min(100.0, percent)), 3)
    updated = iso_z(row.get("updatedAt"))
    if updated:
        out["progress_at"] = updated
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
    collected: dict[str, dict[str, Any]] = {}
    dropped: list[str] = []
    rows_seen = 0

    def collect(row: Mapping[str, Any], *, source: str) -> None:
        nonlocal rows_seen
        rows_seen += 1
        minimal = _row_to_minimal(row, adapter)
        if not minimal:
            reason = _drop_reason(row)
            dropped.append(reason)
            _warn(
                FEATURE,
                "index_row_dropped",
                source=source,
                reason=reason,
                row_type=str(row.get("type") or ""),
                entry_id=row.get("id"),
                tmdb_id=row.get("tmdbId"),
                show_tmdb_id=row.get("showTmdbId"),
                season=row.get("season"),
                episode=row.get("episode"),
                title=str(row.get("showTitle") or row.get("title") or "")[:80],
            )
            return
        key = _key_of(minimal)
        if not key:
            dropped.append("no_canonical_key")
            _warn(FEATURE, "index_row_dropped", source=source, reason="no_canonical_key", entry_id=row.get("id"))
            return
        collected[key] = minimal

    resp = punchplay_request(adapter, "GET", URL_IN_PROGRESS)
    if resp.status_code != 200:
        _warn(FEATURE, "in_progress_fetch_failed", status=resp.status_code, error=error_of(resp), request_id=request_id_of(resp))
    else:
        data = safe_json(resp)
        rows: list[Mapping[str, Any]] = []
        if isinstance(data, Mapping):
            raw = data.get("items")
            rows = [r for r in raw if isinstance(r, Mapping)] if isinstance(raw, list) else []
        elif isinstance(data, list):
            rows = [r for r in data if isinstance(r, Mapping)]
        for row in rows:
            collect(row, source="in_progress")

    snapshot_rows = 0
    for page in snapshot_pages(adapter, "playback", feature=FEATURE):
        for row in page:
            snapshot_rows += 1
            collect(row, source="snapshot")

    _info(
        FEATURE,
        "index_done",
        count=len(collected),
        rows=rows_seen,
        snapshot_rows=snapshot_rows,
        dropped=len(dropped),
        drop_reasons=",".join(sorted(set(dropped))) or None,
    )
    return collected


def _session_ids(item: Mapping[str, Any], key: str, *, device_id: str, instance: str, position: float | None) -> dict[str, Any]:
    scope = f"cw-{instance}-{key}"
    created = int(time.time() * 1000)
    out: dict[str, Any] = {
        "playback_session_id": scope,
        "event_id": f"{scope}@{int(position or 0)}:{created}:{secrets.token_hex(4)}",
        "event_created_at": created,
    }
    if device_id:
        out["device_id"] = device_id
    return out


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

    pos_ms = _position_ms(item)
    dur_ms = _duration_ms_of(item)
    if pos_ms is not None:
        payload["position_seconds"] = round(pos_ms / 1000.0, 3)
    if dur_ms:
        payload["duration_seconds"] = round(dur_ms / 1000.0, 3)

    percent = _percent_of(item)
    if percent is not None:
        payload["progress"] = max(0.0, min(1.0, percent / 100.0))

    if item.get("anime") is True:
        payload["anime"] = True
    return payload


def _send(adapter: Any, action: str, payload: Mapping[str, Any]) -> Any:
    return punchplay_request(adapter, "POST", URL_PLAYBACK.format(action=action), json=dict(payload))


def _write_action(item: Mapping[str, Any]) -> str:
    state = str(item.get("playback_state") or "").strip().lower()
    if item.get("now_playing") is True or state in {"watching", "playing"}:
        return "progress"
    return "stop"


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    ok = True

    section = cfg_section(adapter) or {}
    device_id = str(section.get("device_id") or "").strip()
    instance = str(instance_id(adapter) or "default")

    for item in items or []:
        key = _key_of(item)
        if not key:
            continue
        payload = _playback_payload(item)
        if payload is not None:
            payload.update(
                _session_ids(
                    item,
                    key,
                    device_id=device_id,
                    instance=instance,
                    position=payload.get("position_seconds"),
                )
            )
        if payload is None:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "missing_supported_id_or_position"})
            _dbg(FEATURE, "item_unresolved_before_write", key=key)
            continue

        action = _write_action(item)
        if action == "stop":
            payload.setdefault("watched", False)
            payload.setdefault("watched_threshold", 1.0)
        resp = _send(adapter, action, payload)
        if 200 <= resp.status_code < 300:
            body = safe_json(resp) or {}
            ignored = str(body.get("ignored") or "").strip() if isinstance(body, Mapping) else ""
            ok_body = bool(body.get("ok", True)) if isinstance(body, Mapping) else True
            if ignored or not ok_body:
                ok = False
                unresolved_keys.append(key)
                unresolved.append({"key": key, "status": "ignored" if ignored else "not_ok", "error": ignored or error_of(resp)})
                _warn(FEATURE, "progress_write_ignored", key=key, ignored=ignored or None, request_id=request_id_of(resp))
            else:
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
