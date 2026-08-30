# providers/sync/stremio/_progress.py
# CrossWatch Stremio progress functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.id_map import minimal as id_minimal
from providers.auth._auth_STREMIO import StremioAuthError
from providers.sync._mod_common import build_op_result, unresolved_keys
from providers.sync._progress_policy import select_progress_record

from ._common import (
    canonical_item_key,
    imdb_ids_from_item,
    imdb_id,
    ids_from_stremio_id,
    iso_from_epoch_ms,
    item_from_episode,
    item_from_movie_record,
    library_records,
    now_iso,
    positive_int,
    native_record_ids,
    read_drop_summary,
    read_merge_write,
    record_read_drop,
    reset_read_drop_report,
    state_of,
    stremio_id_for_item,
    stremio_write_id_for_item,
    poster_url_from_item,
    tmdb_metadata_provider,
    to_int,
    video_id_for_episode,
)
from providers.sync._log import log as cw_log

MAX_STREMIO_PROGRESS_DURATION_MS = 24 * 60 * 60 * 1000


def _progress_percent(position: int, duration: int) -> float | None:
    return round((position / duration) * 100.0, 3) if duration > 0 and position >= 0 else None


def _bounded_duration_ms(value: Any, scale: int = 1) -> int | None:
    number = positive_int(value)
    if number is None:
        return None
    duration = number * scale
    return duration if duration <= MAX_STREMIO_PROGRESS_DURATION_MS else None


def _state_duration_ms(state: Mapping[str, Any]) -> tuple[int | None, int | None]:
    raw = positive_int(state.get("duration"))
    if raw is None:
        return None, None
    duration = _bounded_duration_ms(raw)
    if duration is not None:
        return duration, None
    return None, raw


def _duration_ms(item: Mapping[str, Any]) -> int | None:
    for key in ("duration_ms", "durationMs", "duration", "runtime_ms", "runtimeMs"):
        duration = _bounded_duration_ms(item.get(key))
        if duration is not None:
            return duration
    for key in ("duration_seconds", "durationSeconds", "runtime_seconds", "runtimeSeconds"):
        duration = _bounded_duration_ms(item.get(key), 1000)
        if duration is not None:
            return duration
    for key in ("runtime_minutes", "runtimeMinutes", "duration_minutes", "durationMinutes", "runtime"):
        duration = _bounded_duration_ms(item.get(key), 60_000)
        if duration is not None:
            return duration
    return None


def _progress_percent_value(item: Mapping[str, Any]) -> float | None:
    for key in ("progress_percent", "progressPercent", "percent", "position_percent", "resume_percent"):
        value = item.get(key)
        if value is None or isinstance(value, bool):
            continue
        try:
            percent = float(value)
            if percent == percent:
                return max(0.0, min(100.0, percent))
        except Exception:
            continue
    return None


def _progress_ms(item: Mapping[str, Any], duration: int | None = None) -> int | None:
    for key in ("progress_ms", "progressMs", "position", "position_ms", "positionMs", "viewOffset", "timeOffset", "progress"):
        number = to_int(item.get(key))
        if number is not None:
            return max(0, number)
    percent = _progress_percent_value(item)
    if percent is not None and duration:
        return max(0, int(round((percent / 100.0) * duration)))
    return None


def _image_url(detail: Mapping[str, Any], kind: str) -> str:
    images = detail.get("images")
    image_map = images if isinstance(images, Mapping) else {}
    rows = image_map.get(kind)
    if not isinstance(rows, list):
        return ""
    for row in rows:
        if isinstance(row, Mapping):
            url = str(row.get("url") or "").strip()
            if url:
                return url
    return ""


def _metadata_enriched(adapter: Any, item: Mapping[str, Any], typ: str) -> dict[str, Any]:
    out = dict(item)
    stremio_id = stremio_id_for_item(out)
    if stremio_id:
        if not str(out.get("poster") or out.get("poster_url") or "").strip():
            out["poster"] = poster_url_from_item(out, stremio_id)
        ids_source = out.get("show_ids") if typ in {"episode", "episodes"} else out.get("ids")
        ids_map = ids_source if isinstance(ids_source, Mapping) else {}
        if _duration_ms(out) and str(ids_map.get("tmdb") or "").strip():
            return out
    provider = tmdb_metadata_provider(adapter)
    if provider is None:
        return out
    show_ids_raw = out.get("show_ids")
    show_ids: Mapping[str, Any] = show_ids_raw if isinstance(show_ids_raw, Mapping) else {}
    source: Mapping[str, Any] = show_ids if typ in {"episode", "episodes"} and show_ids else imdb_ids_from_item(out)
    lookup = {k: str(source.get(k) or "").strip() for k in ("tmdb", "imdb", "tvdb") if str(source.get(k) or "").strip()}
    title = str(out.get("series_title") or out.get("show_title") or out.get("title") or "").strip()
    if title:
        lookup["title"] = title
    if out.get("year"):
        lookup["year"] = str(out.get("year"))
    if not lookup:
        return out
    try:
        detail = provider.fetch(entity="tv" if typ in {"episode", "episodes"} else "movie", ids=lookup, need={"poster": True, "backdrop": False, "ids": True, "runtime_minutes": True})
    except Exception:
        return out
    if not isinstance(detail, Mapping):
        return out
    ids_raw = detail.get("ids")
    ids: Mapping[str, Any] = ids_raw if isinstance(ids_raw, Mapping) else {}
    portable_ids: dict[str, Any] = {}
    tmdb = str(ids.get("tmdb") or "").strip()
    if tmdb:
        portable_ids["tmdb"] = tmdb
    imdb = imdb_id(ids.get("imdb"))
    if imdb:
        portable_ids["imdb"] = imdb
    tvdb = str(ids.get("tvdb") or "").strip()
    if tvdb:
        portable_ids["tvdb"] = tvdb
    if portable_ids:
        if typ in {"episode", "episodes"}:
            merged: dict[str, Any] = dict(show_ids)
            for key, value in portable_ids.items():
                merged.setdefault(key, value)
            out["show_ids"] = merged
        else:
            out_ids_raw = out.get("ids")
            out_ids: Mapping[str, Any] = out_ids_raw if isinstance(out_ids_raw, Mapping) else {}
            merged = dict(out_ids)
            for key, value in portable_ids.items():
                merged.setdefault(key, value)
            out["ids"] = merged
    duration = positive_int(detail.get("runtime_minutes"))
    if duration is None and typ in {"episode", "episodes"}:
        tmdb = str(ids.get("tmdb") or lookup.get("tmdb") or "").strip()
        fetch = getattr(provider, "_get", None)
        season = positive_int(out.get("season"))
        episode = positive_int(out.get("episode"))
        if callable(fetch) and tmdb and season and episode:
            try:
                ep = fetch(f"https://api.themoviedb.org/3/tv/{tmdb}/season/{season}/episode/{episode}")
            except Exception:
                ep = {}
            if isinstance(ep, Mapping):
                duration = positive_int(ep.get("runtime"))
    duration_ms = _bounded_duration_ms(duration, 60_000) if duration else None
    if duration_ms is not None:
        if out.get("_stremio_duration_invalid") or _duration_ms(out) is None:
            out["duration_ms"] = duration_ms
            position = _progress_ms(out, duration_ms)
            if position is not None:
                out["progress_ms"] = position
                out["progress_percent"] = _progress_percent(position, duration_ms)
            out.pop("_stremio_duration_invalid", None)
    if not str(out.get("poster") or out.get("poster_url") or "").strip():
        poster = poster_url_from_item(out, imdb) or _image_url(detail, "poster")
        if poster:
            out["poster"] = poster
    return out


def parse_movie_progress_record(record: Mapping[str, Any]) -> dict[str, Any] | None:
    state = state_of(record)
    position = to_int(state.get("timeOffset"))
    duration, invalid_duration = _state_duration_ms(state)
    if position is None or position <= 0 or (duration is None and invalid_duration is None):
        return None
    item = item_from_movie_record(record)
    if not item:
        return None
    item["progress_ms"] = position
    if duration:
        item["duration_ms"] = duration
        item["progress_percent"] = _progress_percent(position, duration)
    elif invalid_duration is not None:
        item["_stremio_duration_invalid"] = invalid_duration
    ts = iso_from_epoch_ms(state.get("lastWatched")) or iso_from_epoch_ms(record.get("_mtime"))
    if ts:
        item["progress_at"] = ts
    return item


def parse_episode_progress_record(record: Mapping[str, Any]) -> dict[str, Any] | None:
    state = state_of(record)
    position = to_int(state.get("timeOffset"))
    duration, invalid_duration = _state_duration_ms(state)
    if position is None or position <= 0 or (duration is None and invalid_duration is None):
        return None
    show_id = str(record.get("_id") or "").strip()
    video_id = str(state.get("video_id") or state.get("videoId") or "").strip()
    season = positive_int(state.get("season"))
    episode = positive_int(state.get("episode"))
    if video_id:
        parts = video_id.split(":")
        if len(parts) >= 3:
            candidate = ":".join(parts[:-2])
            if ids_from_stremio_id(candidate, "series"):
                show_id = candidate
            season = positive_int(parts[-2])
            episode = positive_int(parts[-1])
    elif show_id and season and episode:
        video_id = f"{show_id}:{season}:{episode}"
    if not show_id or not ids_from_stremio_id(show_id, "series") or not season or not episode:
        return None
    item = item_from_episode(show_id, season, episode, record, {"id": video_id, "season": season, "episode": episode})
    if not item:
        return None
    item["progress_ms"] = position
    if duration:
        item["duration_ms"] = duration
        item["progress_percent"] = _progress_percent(position, duration)
    elif invalid_duration is not None:
        item["_stremio_duration_invalid"] = invalid_duration
    ts = iso_from_epoch_ms(state.get("lastWatched")) or iso_from_epoch_ms(record.get("_mtime"))
    if ts:
        item["progress_at"] = ts
    return item


def build_index(adapter: Any, **kwargs: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    reset_read_drop_report(adapter, "progress")
    for record in library_records(adapter, incremental=bool(kwargs.get("incremental"))):
        typ = str(record.get("type") or "").strip().lower()
        if typ not in {"movie", "series", "show"}:
            continue
        state = state_of(record)
        position = to_int(state.get("timeOffset"))
        duration = positive_int(state.get("duration"))
        has_progress = position is not None and position > 0 and bool(duration)
        ids, drop_reason = native_record_ids(adapter, record)
        if not ids:
            if has_progress:
                record_read_drop(adapter, "progress", record, drop_reason or "unsupported_stremio_id", requires_id="tmdb_api_key" if drop_reason == "bare_numeric_id_unverified" else "known_native_id")
            continue
        item = parse_movie_progress_record(record) if typ == "movie" else parse_episode_progress_record(record)
        if not item:
            continue
        item = _metadata_enriched(adapter, item, "episode" if str(item.get("type") or "").lower() == "episode" else "movie")
        if _duration_ms(item) is None:
            invalid_duration = item.get("_stremio_duration_invalid")
            if invalid_duration is not None:
                record_read_drop(adapter, "progress", record, "stremio_duration_invalid", duration_ms=invalid_duration)
            continue
        key = canonical_item_key(item)
        selected, _reason = select_progress_record(out.get(key), item)
        out[key] = selected
    summary = read_drop_summary(adapter, "progress")
    if int(summary.get("dropped") or 0):
        cw_log("STREMIO", "progress", "warn", "index_rows_dropped", indexed=len(out), **summary)
    return out


def _unresolved(item: Mapping[str, Any], reason: str, key: str | None = None) -> dict[str, Any]:
    out = {"status": "unresolved", "reason": reason, "item": id_minimal(item)}
    if key:
        out["key"] = key
        out["canonical_key"] = key
    return out


def _api_failure(item: Mapping[str, Any], key: str, exc: StremioAuthError, fallback: str) -> dict[str, Any]:
    entry = {"status": "failed", "reason": str(getattr(exc, "reason", "") or fallback), "item": id_minimal(item), "key": key, "canonical_key": key}
    detail = str(getattr(exc, "detail", "") or "").replace("\n", " ").replace("\r", " ").strip()
    if detail:
        for token in ("authKey", "auth_key", "password"):
            detail = detail.replace(token, f"{token[0]}***")
        entry["detail"] = detail[:180]
    return entry


def _apply_progress(record: dict[str, Any], item: Mapping[str, Any], clear: bool) -> str | None:
    state = record.setdefault("state", {})
    poster = poster_url_from_item(item, record.get("_id"))
    if poster and not str(record.get("poster") or "").strip():
        record["poster"] = poster
    duration = _duration_ms(item) or positive_int(state.get("duration"))
    position = _progress_ms(item, duration)
    if clear:
        position = 0
    if position is None:
        return "stremio_duration_missing" if _progress_percent_value(item) is not None and not duration else "stremio_progress_missing"
    if not duration and not clear:
        return "stremio_duration_missing"
    typ = str(item.get("type") or id_minimal(item).get("type") or "").strip().lower()
    if typ in {"episode", "episodes"}:
        show_id = str(record.get("_id") or "").strip()
        if not show_id or not ids_from_stremio_id(show_id, "series"):
            return "stremio_id_missing"
        video_id = video_id_for_episode(item, show_id)
        season = positive_int(item.get("season"))
        episode = positive_int(item.get("episode"))
        if not video_id or not season or not episode:
            return "stremio_episode_unresolved"
        state["video_id"] = video_id
        state["season"] = season
        state["episode"] = episode
    state["timeOffset"] = int(position)
    if duration:
        state["duration"] = int(duration)
    record["_mtime"] = now_iso()
    return None


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, items, False, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, items, True, dry_run=dry_run)


def _write(adapter: Any, items: Iterable[Mapping[str, Any]], clear: bool, *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    attempted = 0
    for item in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        key = canonical_item_key(item)
        typ = str(item.get("type") or id_minimal(item).get("type") or "").strip().lower()
        item = _metadata_enriched(adapter, item, typ)
        stremio_id = stremio_write_id_for_item(item)
        if not stremio_id or typ not in {"movie", "episode", "episodes"}:
            entry = _unresolved(item, "stremio_id_missing", key)
            unresolved.append(entry)
            results.append(entry)
            continue
        attempted += 1
        if dry_run:
            confirmed.append(key)
            results.append({"status": "dry_run", "item": id_minimal(item), "canonical_key": key})
            continue
        try:
            def mutate(record: dict[str, Any]) -> None:
                reason = _apply_progress(record, item, clear)
                if reason:
                    raise ValueError(reason)

            read_merge_write(adapter, stremio_id, "movie" if typ == "movie" else "series", item, mutate)
        except ValueError as exc:
            entry = _unresolved(item, str(exc) or "stremio_progress_write_failed", key)
            unresolved.append(entry)
            results.append(entry)
            continue
        except StremioAuthError as exc:
            entry = _api_failure(item, key, exc, "stremio_progress_write_failed")
            unresolved.append(entry)
            results.append(entry)
            continue
        except Exception:
            entry = {"status": "failed", "reason": "stremio_progress_write_failed", "item": id_minimal(item), "key": key, "canonical_key": key}
            unresolved.append(entry)
            results.append(entry)
            continue
        confirmed.append(key)
        results.append({"status": "applied", "item": id_minimal(item), "canonical_key": key})
    return build_op_result(ok=not unresolved, count=len(confirmed), confirmed_keys=confirmed, unresolved_keys=unresolved_keys(unresolved, canonical_item_key), unresolved=unresolved, results=results, attempted=attempted)
