# /providers/sync/simkl/_progress.py
# CrossWatch SIMKL progress functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import math
import os
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.id_map import canonical_key, minimal as id_minimal
from providers.sync._log import log as cw_log
from providers.sync._progress_policy import decide_progress_write, progress_materially_equal, select_progress_record

from ._common import _fix_imdb


_PROVIDER = "SIMKL"
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


def _ids(source: Any, allowed: Iterable[str], *, integer_keys: Iterable[str] = ()) -> dict[str, Any]:
    raw = _fix_imdb(dict(source or {}) if isinstance(source, Mapping) else {})
    integer = {str(k) for k in integer_keys}
    out: dict[str, Any] = {}
    for key in allowed:
        value = raw.get(key)
        if value in (None, ""):
            continue
        if key in integer:
            number = _positive_int(value)
            if number is not None:
                out[key] = number
            continue
        text = str(value).strip()
        if text:
            out[key] = text
    return out


def _movie_ids(source: Any) -> dict[str, Any]:
    return _ids(
        source,
        ("simkl", "imdb", "tmdb", "netflix", "traktslug", "letterboxd", "boxd"),
        integer_keys=("simkl", "tmdb", "netflix"),
    )


def _show_ids(source: Any) -> dict[str, Any]:
    return _ids(
        source,
        ("simkl", "imdb", "tmdb", "tvdb", "netflix", "hulu", "traktslug", "zap2it", "tvcom", "mdl"),
        integer_keys=("simkl", "tmdb", "netflix", "hulu"),
    )


def _anime_ids(source: Any) -> dict[str, Any]:
    return _ids(
        source,
        ("simkl", "imdb", "tmdb", "tvdb", "mal", "anidb", "anilist", "kitsu", "anisearch", "animeplanet", "livechart", "anfo", "ann"),
        integer_keys=("simkl", "tmdb", "mal", "anidb", "anilist", "kitsu", "anisearch", "livechart", "anfo", "ann"),
    )


def _episode_ids(source: Any) -> dict[str, Any]:
    return _ids(source, ("tvdb", "anidb"), integer_keys=("tvdb", "anidb"))


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


def _container(row: Mapping[str, Any]) -> tuple[str, Mapping[str, Any], Mapping[str, Any]]:
    row_type = str(row.get("type") or "").strip().lower()
    movie_raw = row.get("movie")
    movie = movie_raw if isinstance(movie_raw, Mapping) else None
    show_raw = row.get("show")
    show = show_raw if isinstance(show_raw, Mapping) else None
    anime_raw = row.get("anime")
    anime = anime_raw if isinstance(anime_raw, Mapping) else None
    episode_raw = row.get("episode")
    episode: Mapping[str, Any] = episode_raw if isinstance(episode_raw, Mapping) else {}
    if movie is not None or row_type == "movie":
        return "movie", movie or row, {}
    if anime is not None:
        return "anime", anime, episode
    return "show", show or row, episode


def _episode_number(episode: Mapping[str, Any], row: Mapping[str, Any]) -> int | None:
    return _positive_int(episode.get("number") or episode.get("episode") or row.get("episode") or row.get("episode_number"))


def _item_from_row(row: Mapping[str, Any]) -> tuple[str | None, dict[str, Any] | None, str | None]:
    playback_id = _positive_int(row.get("id"))
    progress = _float(row.get("progress"))
    if playback_id is None or progress is None or progress <= 0:
        return None, None, "simkl_progress_invalid"

    media_kind, payload, episode = _container(row)
    duration = _duration_ms(payload, episode, row)
    position = _progress_ms(progress, duration)
    progress_at = str(row.get("paused_at") or row.get("watched_at") or "").strip()

    if media_kind == "movie":
        item = id_minimal(
            {
                "type": "movie",
                "title": payload.get("title"),
                "year": payload.get("year"),
                "ids": _movie_ids(payload.get("ids")),
            }
        )
    else:
        parent_ids = _anime_ids(payload.get("ids")) if media_kind == "anime" else _show_ids(payload.get("ids"))
        item = id_minimal(
            {
                "type": "episode",
                "title": episode.get("title") or payload.get("title"),
                "series_title": payload.get("title"),
                "year": payload.get("year"),
                "season": episode.get("season") or row.get("season"),
                "episode": _episode_number(episode, row),
                "ids": _episode_ids(episode.get("ids")),
                "show_ids": parent_ids,
            }
        )
        if media_kind == "anime":
            item["simkl_bucket"] = "anime"
            if payload.get("anime_type"):
                item["anime_type"] = payload.get("anime_type")

    key = canonical_key(item)
    if not key:
        return None, None, "simkl_id_missing"

    item["progress_percent"] = round(progress, 3)
    if position is not None and duration is not None:
        item["progress_ms"] = int(position)
        item["duration_ms"] = int(duration)
    if progress_at:
        item["progress_at"] = progress_at
    item["_simkl_playback_id"] = playback_id
    return key, item, None


def _request(adapter: Any, method: str, path: str, **kwargs: Any) -> Any:
    url = f"{adapter.client.BASE}{path}"
    fn = getattr(adapter.client, "_request", None)
    if callable(fn):
        return fn(method, url, **kwargs)
    method_fn = getattr(adapter.client, method.lower())
    return method_fn(url, **kwargs)


def _playback_rows(adapter: Any) -> list[Mapping[str, Any]] | None:
    try:
        limit = max(1, min(10000, int(getattr(adapter.cfg, "progress_limit", 10000) or 10000)))
    except Exception:
        limit = 10000
    params: dict[str, Any] = {"limit": limit, "hide_watched": "true"}
    date_from = str(getattr(adapter.cfg, "date_from", "") or "").strip()
    if date_from:
        params["date_from"] = date_from
    response = _request(adapter, "GET", "/sync/playback", params=params)
    if not (200 <= int(getattr(response, "status_code", 0) or 0) < 300):
        _warn("index_http_failed", status=getattr(response, "status_code", None))
        return None
    data = response.json() if (getattr(response, "text", "") or "").strip() else []
    if isinstance(data, list):
        return [row for row in data if isinstance(row, Mapping)]
    if isinstance(data, Mapping):
        rows: list[Mapping[str, Any]] = []
        for key in ("playback", "items", "movies", "episodes", "shows", "anime"):
            bucket = data.get(key)
            if isinstance(bucket, list):
                rows.extend([row for row in bucket if isinstance(row, Mapping)])
        return rows
    return []


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    rows = _playback_rows(adapter)
    if rows is None:
        return {}
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


def _movie_body(item: Mapping[str, Any], progress_percent: float) -> tuple[dict[str, Any] | None, str | None]:
    ids = _movie_ids(item.get("ids"))
    if not ids:
        return None, "simkl_movie_id_missing"
    movie: dict[str, Any] = {"ids": ids}
    title = str(item.get("title") or "").strip()
    if title:
        movie["title"] = title
    year = _int(item.get("year"))
    if year is not None:
        movie["year"] = year
    return {"progress": progress_percent, "movie": movie}, None


def _episode_body(item: Mapping[str, Any], progress_percent: float) -> tuple[dict[str, Any] | None, str | None]:
    episode_ids = _episode_ids(item.get("ids"))
    season = _int(item.get("season") if item.get("season") is not None else item.get("season_number"))
    number = _positive_int(item.get("episode") if item.get("episode") is not None else item.get("number"))

    show_ids_raw = item.get("show_ids")
    show_ids_map = dict(show_ids_raw) if isinstance(show_ids_raw, Mapping) else {}
    wants_anime = str(item.get("simkl_bucket") or "").strip().lower() == "anime" or bool(
        _anime_ids(show_ids_map)
        and any(str(k) in show_ids_map for k in ("mal", "anidb", "anilist", "kitsu", "anisearch", "animeplanet", "livechart"))
    )
    parent_ids = _anime_ids(item.get("show_ids")) if wants_anime else _show_ids(item.get("show_ids"))
    if not parent_ids:
        return None, "simkl_parent_id_missing"

    episode: dict[str, Any]
    if season is not None and season >= 0 and number is not None:
        episode = {"season": season, "number": number}
    elif episode_ids:
        episode = {"ids": episode_ids}
    else:
        return None, "simkl_episode_id_missing"

    parent_key = "anime" if wants_anime else "show"
    parent: dict[str, Any] = {"ids": parent_ids}
    series_title = str(item.get("series_title") or item.get("title") or "").strip()
    if series_title:
        parent["title"] = series_title
    year = _int(item.get("series_year") or item.get("year"))
    if year is not None:
        parent["year"] = year
    return {"progress": progress_percent, parent_key: parent, "episode": episode}, None


def _scrobble_body(item: Mapping[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    progress_ms, duration_ms, percent = _progress_values(item)
    if percent is None:
        return None, "simkl_progress_missing" if progress_ms is None else "simkl_duration_missing"
    if percent <= 0 or percent >= 100:
        return None, "simkl_progress_invalid"
    media_type = str(item.get("type") or "").strip().lower()
    if media_type == "movie":
        return _movie_body(item, percent)
    if media_type == "episode":
        return _episode_body(item, percent)
    return None, "simkl_media_type_unsupported"


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


def _same_simkl_endpoint() -> bool:
    src = str(os.getenv("CW_PAIR_SRC") or "").upper().strip()
    dst = str(os.getenv("CW_PAIR_DST") or "").upper().strip()
    src_instance = str(os.getenv("CW_PAIR_SRC_INSTANCE") or "default").strip().lower() or "default"
    dst_instance = str(os.getenv("CW_PAIR_DST_INSTANCE") or "default").strip().lower() or "default"
    return src == dst == "SIMKL" and src_instance == dst_instance


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
            row = _unresolved(item, reason or "simkl_progress_invalid")
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
            same_origin=_same_simkl_endpoint(),
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
        response = _request(adapter, "POST", "/scrobble/pause", json=body)
        status = int(getattr(response, "status_code", 0) or 0)
        if status != 201:
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
            row = _unresolved(item, "simkl_progress_verification_failed", status="failed")
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
        playback_id = _positive_int(target.get("_simkl_playback_id") or item.get("_simkl_playback_id"))
        if playback_id is None:
            row = _unresolved(item, "simkl_playback_id_missing")
            unresolved.append(row)
            results.append(row)
            continue
        pending.append((key, playback_id, item))
        results.append({"status": "pending" if not dry_run else "dry_run", "canonical_key": key, "remote_id": playback_id, "item": id_minimal(item)})

    if dry_run:
        return _result(len(unresolved) == 0, len(pending), len(pending), [], unresolved, results, skipped, dry_run=True)

    failed = False
    for key, playback_id, item in pending:
        response = _request(adapter, "DELETE", f"/sync/playback/{playback_id}")
        status = int(getattr(response, "status_code", 0) or 0)
        if status != 204:
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
            row = _unresolved(item, "simkl_progress_delete_unconfirmed", status="failed")
            unresolved.append(row)
            results.append(row)

    ok = len(unresolved) == 0
    _info("write_done", op="remove", ok=ok, attempted=len(pending), confirmed=len(verified), skipped=skipped, unresolved=len(unresolved))
    return _result(ok, len(verified), len(pending), verified, unresolved, results, skipped)
