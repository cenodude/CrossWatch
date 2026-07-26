# /providers/sync/mdblist/_progress.py
# CrossWatch MDBList progress functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import math
import os
import sys
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.id_map import canonical_key, minimal as id_minimal
from providers.sync._log import log as cw_log
from providers.sync._progress_policy import decide_progress_write, progress_materially_equal, select_progress_record


_PROVIDER = "MDBLIST"
_FEATURE = "progress"


def _dbg(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "debug", event, **fields)


def _info(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "warn", event, **fields)


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        number = float(value)
        return number if math.isfinite(number) else None
    except Exception:
        return None


def _int(value: Any) -> int | None:
    try:
        if value is None or value == "" or isinstance(value, bool):
            return None
        return int(float(value))
    except Exception:
        return None


def _positive_int(value: Any) -> int | None:
    number = _int(value)
    return number if number is not None and number > 0 else None


def _imdb(value: Any) -> str | None:
    if value is None or value == "":
        return None
    text = str(value).strip()
    if text and text.isdigit():
        text = f"tt{text}"
    return text if text.startswith("tt") and text[2:].isdigit() else None


def _ids_from_response(source: Any) -> dict[str, Any]:
    raw = dict(source or {}) if isinstance(source, Mapping) else {}
    out: dict[str, Any] = {}
    imdb = _imdb(raw.get("imdb") or raw.get("imdbid") or raw.get("imdb_id"))
    if imdb:
        out["imdb"] = imdb
    for target, keys in {
        "tmdb": ("tmdb", "tmdbid", "tmdb_id"),
        "tvdb": ("tvdb", "tvdbid", "tvdb_id"),
        "trakt": ("trakt", "traktid", "trakt_id"),
    }.items():
        for key in keys:
            number = _positive_int(raw.get(key))
            if number is not None:
                out[target] = number
                break
    mdblist = str(raw.get("mdblist") or raw.get("mdblist_id") or "").strip()
    if mdblist:
        out["mdblist"] = mdblist
    return out


def _imdb_ids(source: Any) -> dict[str, str]:
    raw = dict(source or {}) if isinstance(source, Mapping) else {}
    imdb = _imdb(raw.get("imdb") or raw.get("imdbid") or raw.get("imdb_id"))
    return {"imdb": imdb} if imdb else {}


def _duration_ms(*sources: Mapping[str, Any]) -> int | None:
    for source in sources:
        for key in ("duration_ms", "durationMs", "runtime_ms", "runtimeMs"):
            value = _positive_int(source.get(key))
            if value is not None:
                return value
        for key in ("duration_seconds", "durationSeconds", "runtime_seconds", "runtimeSeconds"):
            value = _positive_int(source.get(key))
            if value is not None:
                return value * 1000
        runtime = _positive_int(source.get("runtime"))
        if runtime is not None:
            return runtime * 60_000
    return None


def _progress_ms(progress_percent: Any, duration_ms: int | None) -> int | None:
    progress = _float(progress_percent)
    if progress is None or progress <= 0 or duration_ms is None or duration_ms <= 0:
        return None
    return int(round(float(duration_ms) * max(0.0, min(progress, 100.0)) / 100.0))


def _item_from_row(row: Mapping[str, Any]) -> tuple[str | None, dict[str, Any] | None, str | None]:
    playback_id = _positive_int(row.get("id"))
    progress = _float(row.get("progress"))
    if playback_id is None or progress is None or progress <= 0:
        return None, None, "mdblist_progress_invalid"

    row_type = str(row.get("type") or "").strip().lower()
    movie_raw = row.get("movie")
    movie = movie_raw if isinstance(movie_raw, Mapping) else None
    show_raw = row.get("show")
    show: Mapping[str, Any] = show_raw if isinstance(show_raw, Mapping) else {}
    episode_raw = row.get("episode")
    episode: Mapping[str, Any] = episode_raw if isinstance(episode_raw, Mapping) else {}

    if movie is not None or row_type == "movie":
        payload: Mapping[str, Any] = movie if movie is not None else row
        duration = _duration_ms(payload, row)
        position = _progress_ms(progress, duration)
        item = id_minimal(
            {
                "type": "movie",
                "title": payload.get("title"),
                "year": payload.get("year"),
                "ids": _ids_from_response(payload.get("ids")),
            }
        )
    else:
        duration = _duration_ms(episode, show, row)
        position = _progress_ms(progress, duration)
        item = id_minimal(
            {
                "type": "episode",
                "title": episode.get("title") or show.get("title"),
                "series_title": show.get("title"),
                "year": show.get("year"),
                "season": episode.get("season"),
                "episode": episode.get("number") or episode.get("episode"),
                "ids": _ids_from_response(episode.get("ids")),
                "show_ids": _ids_from_response(show.get("ids")),
            }
        )

    key = canonical_key(item)
    if not key:
        return None, None, "mdblist_id_missing"

    item["progress_percent"] = round(progress, 3)
    if position is not None and duration is not None:
        item["progress_ms"] = int(position)
        item["duration_ms"] = int(duration)
    paused_at = str(row.get("paused_at") or row.get("progress_at") or "").strip()
    if paused_at:
        item["progress_at"] = paused_at
    item["_mdblist_playback_id"] = playback_id
    return key, item, None


def _request(adapter: Any, method: str, path: str, **kwargs: Any) -> Any:
    url = f"{adapter.client.BASE}{path}"
    fn = getattr(adapter.client, method.lower())
    return fn(url, **kwargs)


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    response = _request(adapter, "GET", "/sync/playback", params={"apikey": adapter.cfg.api_key})
    if not (200 <= int(getattr(response, "status_code", 0) or 0) < 300):
        _warn("index_http_failed", status=getattr(response, "status_code", None))
        return {}
    data = response.json() if (getattr(response, "text", "") or "").strip() else []
    rows: list[Mapping[str, Any]] = []
    if isinstance(data, list):
        rows = [row for row in data if isinstance(row, Mapping)]
    elif isinstance(data, Mapping):
        for key in ("playback", "items", "sessions", "movies", "episodes"):
            bucket = data.get(key)
            if isinstance(bucket, list):
                rows.extend(row for row in bucket if isinstance(row, Mapping))

    out: dict[str, dict[str, Any]] = {}
    skipped: dict[str, int] = {}
    for row in rows:
        key, item, reason = _item_from_row(row)
        if not key or item is None:
            if reason:
                skipped[reason] = skipped.get(reason, 0) + 1
            continue
        selected, action = select_progress_record(out.get(key), item)
        out[key] = selected
        _dbg("item", canonical_key=key, media_type=item.get("type"), progress_percent=item.get("progress_percent"), action=action)
    _info("index_done", count=len(out), rows=len(rows), skipped=sum(skipped.values()), skipped_reasons=skipped)
    return out


def _progress_values(item: Mapping[str, Any]) -> tuple[int | None, int | None, float | None]:
    progress = None
    for key in ("progress_ms", "progressMs", "viewOffset", "progress"):
        progress = _positive_int(item.get(key))
        if progress is not None:
            break
    duration = None
    for key in ("duration_ms", "durationMs", "duration", "runtime_ms", "runtimeMs"):
        duration = _positive_int(item.get(key))
        if duration is not None:
            break
    if progress is not None and duration is not None and duration > 0:
        return progress, duration, round((float(progress) / float(duration)) * 100.0, 2)
    for key in ("progress_percent", "progressPercent", "percent", "position_percent", "resume_percent"):
        percent = _float(item.get(key))
        if percent is not None:
            return progress, duration, round(max(0.0, min(percent, 100.0)), 2)
    return progress, duration, None


def _app_version() -> str:
    env_version = str(os.getenv("APP_VERSION") or "").strip()
    if env_version:
        return env_version
    mod = sys.modules.get("providers.sync._mod_MDBLIST") or sys.modules.get("sync._mod_MDBLIST")
    version = getattr(mod, "__VERSION__", None) if mod is not None else None
    return str(version or "1.0.0").strip() or "1.0.0"


def _movie_body(item: Mapping[str, Any], progress_percent: float) -> tuple[dict[str, Any] | None, str | None]:
    ids = _imdb_ids(item.get("ids"))
    if not ids:
        return None, "mdblist_movie_imdb_missing"
    return {"movie": {"ids": ids}, "progress": progress_percent, "app_version": _app_version()}, None


def _episode_body(item: Mapping[str, Any], progress_percent: float) -> tuple[dict[str, Any] | None, str | None]:
    show_ids = _imdb_ids(item.get("show_ids"))
    if not show_ids:
        return None, "mdblist_show_imdb_missing"
    season = _int(item.get("season") if item.get("season") is not None else item.get("season_number"))
    episode = _positive_int(item.get("episode") if item.get("episode") is not None else item.get("number"))
    if season is None or season < 0 or episode is None:
        return None, "mdblist_episode_number_missing"
    return {
        "show": {"ids": show_ids, "season": {"number": season, "episode": {"number": episode}}},
        "progress": progress_percent,
        "app_version": _app_version(),
    }, None


def _scrobble_body(item: Mapping[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    progress_ms, duration_ms, percent = _progress_values(item)
    if percent is None:
        return None, "mdblist_progress_missing" if progress_ms is None else "mdblist_duration_missing"
    if percent <= 0 or percent >= 100:
        return None, "mdblist_progress_invalid"
    media_type = str(item.get("type") or "").strip().lower()
    if media_type == "movie":
        return _movie_body(item, percent)
    if media_type == "episode":
        return _episode_body(item, percent)
    return None, "mdblist_media_type_unsupported"


def _unresolved(item: Mapping[str, Any], reason: str, status: str = "unresolved", **extra: Any) -> dict[str, Any]:
    out = {"status": status, "reason": reason, "item": id_minimal(item)}
    out.update(extra)
    key = canonical_key(item)
    if key:
        out["canonical_key"] = key
    return out


def _result(ok: bool, count: int, attempted: int, confirmed: list[str], unresolved: list[dict[str, Any]], results: list[dict[str, Any]], skipped: int, **extra: Any) -> dict[str, Any]:
    unresolved_keys: list[str] = []
    for row in unresolved:
        key = str(row.get("canonical_key") or "")
        if key:
            unresolved_keys.append(key)
            continue
        item = row.get("item")
        if isinstance(item, Mapping):
            key = canonical_key(item)
            if key:
                unresolved_keys.append(key)
    out = {
        "ok": bool(ok),
        "count": int(count),
        "attempted": int(attempted),
        "confirmed_keys": list(dict.fromkeys(k for k in confirmed if k)),
        "unresolved": unresolved,
        "unresolved_keys": list(dict.fromkeys(unresolved_keys)),
        "results": results,
        "skipped": int(skipped),
        "errors": sum(1 for row in unresolved if str(row.get("status") or "") == "failed"),
    }
    out.update(extra)
    return out


def _same_mdblist_endpoint() -> bool:
    src = str(os.getenv("CW_PAIR_SRC") or "").upper().strip()
    dst = str(os.getenv("CW_PAIR_DST") or "").upper().strip()
    src_instance = str(os.getenv("CW_PAIR_SRC_INSTANCE") or "default").strip().lower() or "default"
    dst_instance = str(os.getenv("CW_PAIR_DST_INSTANCE") or "default").strip().lower() or "default"
    return src == dst == "MDBLIST" and src_instance == dst_instance


def _percent_equal(source: Mapping[str, Any], target: Mapping[str, Any]) -> bool:
    _sp, _sd, source_percent = _progress_values(source)
    target_percent = _float(target.get("progress_percent"))
    if source_percent is None or target_percent is None:
        return False
    return abs(float(source_percent) - float(target_percent)) <= 0.1


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    src = [dict(item or {}) for item in items or [] if isinstance(item, Mapping)]
    current = build_index(adapter)
    unresolved: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    pending: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    skipped = 0

    for item in src:
        key = canonical_key(item) or ""
        body, reason = _scrobble_body(item)
        if body is None:
            row = _unresolved(item, reason or "mdblist_progress_invalid")
            unresolved.append(row)
            results.append(row)
            continue
        target = current.get(key)
        progress_ms, duration_ms, source_percent = _progress_values(item)
        if isinstance(target, Mapping) and _percent_equal(item, target):
            skipped += 1
            results.append({"status": "skipped", "reason": "progress_unchanged", "canonical_key": key, "item": id_minimal(item)})
            continue
        decision = decide_progress_write(
            active_session=False,
            source_timestamp=item.get("progress_at") or item.get("progressAt") or item.get("last_played") or item.get("lastViewedAt"),
            target_timestamp=(target or {}).get("progress_at") if isinstance(target, Mapping) else None,
            source_progress_ms=progress_ms,
            source_duration_ms=duration_ms,
            target_progress_ms=(target or {}).get("progress_ms") if isinstance(target, Mapping) else None,
            target_duration_ms=(target or {}).get("duration_ms") if isinstance(target, Mapping) else None,
            target_watched=False,
            same_origin=_same_mdblist_endpoint(),
            replay_enabled=True,
        )
        if not decision.apply:
            skipped += 1
            results.append({"status": "skipped", "reason": decision.reason, "canonical_key": key, "item": id_minimal(item), "progress_percent": source_percent})
            continue
        pending.append((key, item, body))
        results.append({"status": "pending" if not dry_run else "dry_run", "canonical_key": key, "item": id_minimal(item)})

    if dry_run:
        return _result(len(unresolved) == 0, len(pending), len(pending), [], unresolved, results, skipped, dry_run=True)

    failed = False
    for key, item, body in pending:
        response = _request(adapter, "POST", "/scrobble/pause", params={"apikey": adapter.cfg.api_key}, json=body)
        status = int(getattr(response, "status_code", 0) or 0)
        if status != 200:
            failed = True
            row = _unresolved(item, f"http:{status}", status="failed", remote_status=status)
            unresolved.append(row)
            results.append(row)
            _warn("write_failed", op="add", canonical_key=key, status=status)

    after = build_index(adapter) if pending and not failed else current
    confirmed: list[str] = []
    for key, item, _body in ([] if failed else pending):
        target = after.get(key)
        if isinstance(target, Mapping) and (
            progress_materially_equal(item.get("progress_ms"), item.get("duration_ms"), target.get("progress_ms"), target.get("duration_ms"))
            or _percent_equal(item, target)
        ):
            confirmed.append(key)
        else:
            row = _unresolved(item, "mdblist_progress_verification_failed", status="failed")
            unresolved.append(row)
            results.append(row)

    ok = len(unresolved) == 0
    _info("write_done", op="add", ok=ok, attempted=len(pending), confirmed=len(confirmed), skipped=skipped, unresolved=len(unresolved))
    return _result(ok, len(confirmed), len(pending), confirmed, unresolved, results, skipped)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    src = [dict(item or {}) for item in items or [] if isinstance(item, Mapping)]
    current = build_index(adapter)
    unresolved: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    pending: list[tuple[str, int, dict[str, Any]]] = []
    skipped = 0

    for item in src:
        key = canonical_key(item) or ""
        target = current.get(key)
        if not isinstance(target, Mapping):
            skipped += 1
            results.append({"status": "skipped", "reason": "already_absent", "canonical_key": key, "item": id_minimal(item)})
            continue
        playback_id = _positive_int(target.get("_mdblist_playback_id") or item.get("_mdblist_playback_id"))
        if playback_id is None:
            row = _unresolved(item, "mdblist_playback_id_missing")
            unresolved.append(row)
            results.append(row)
            continue
        pending.append((key, playback_id, item))
        results.append({"status": "pending" if not dry_run else "dry_run", "canonical_key": key, "remote_id": playback_id, "item": id_minimal(item)})

    if dry_run:
        return _result(len(unresolved) == 0, len(pending), len(pending), [], unresolved, results, skipped, dry_run=True)

    failed = False
    for key, playback_id, item in pending:
        response = _request(adapter, "POST", "/scrobble/clear", params={"apikey": adapter.cfg.api_key}, json={"id": playback_id})
        status = int(getattr(response, "status_code", 0) or 0)
        if status != 200:
            failed = True
            row = _unresolved(item, f"http:{status}", status="failed", remote_status=status)
            unresolved.append(row)
            results.append(row)
            _warn("write_failed", op="remove", canonical_key=key, playback_id=playback_id, status=status)

    after = build_index(adapter) if pending and not failed else current
    verified: list[str] = []
    for key, _playback_id, item in ([] if failed else pending):
        if key not in after:
            verified.append(key)
        else:
            row = _unresolved(item, "mdblist_progress_clear_unconfirmed", status="failed")
            unresolved.append(row)
            results.append(row)

    ok = len(unresolved) == 0
    _info("write_done", op="remove", ok=ok, attempted=len(pending), confirmed=len(verified), skipped=skipped, unresolved=len(unresolved))
    return _result(ok, len(verified), len(pending), verified, unresolved, results, skipped)
