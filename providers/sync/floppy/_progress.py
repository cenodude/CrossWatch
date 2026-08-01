# providers/sync/floppy/_progress.py
# CrossWatch - Floppy progress sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.id_map import minimal as id_minimal
from providers.sync._mod_common import build_op_result, unresolved_keys
from providers.sync._progress_policy import select_progress_record

from ._common import api_put, canonical_item_key, failure_reason, paged, tmdb_id_for_item, unresolved


def _int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(float(str(value).strip()))
    except Exception:
        return None


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).strip())
    except Exception:
        return None


def _ms_from_seconds(value: Any) -> int | None:
    number = _float(value)
    return max(0, round(number * 1000.0)) if number is not None else None


def _seconds_from_ms(value: Any) -> int | None:
    number = _float(value)
    return max(0, round(number / 1000.0)) if number is not None else None


def _duration_ms(item: Mapping[str, Any]) -> int | None:
    for key in ("duration_ms", "durationMs", "duration", "runtime_ms", "runtimeMs"):
        number = _int(item.get(key))
        if number and number > 0:
            return number
    for key in ("duration_seconds", "durationSeconds", "runtime_seconds", "runtimeSeconds"):
        number = _int(item.get(key))
        if number and number > 0:
            return number * 1000
    return None


def _progress_percent(item: Mapping[str, Any]) -> float | None:
    for key in ("progress_percent", "progressPercent", "percent", "resume_percent"):
        number = _float(item.get(key))
        if number is not None:
            return max(0.0, min(100.0, number))
    return None


def _progress_ms(item: Mapping[str, Any], duration: int | None = None) -> int | None:
    for key in ("progress_ms", "progressMs", "position_ms", "positionMs", "viewOffset", "position", "timeOffset"):
        number = _int(item.get(key))
        if number is not None:
            return max(0, number)
    percent = _progress_percent(item)
    if percent is not None and duration:
        return max(0, round((percent / 100.0) * float(duration)))
    return None


def _row_item(row: Mapping[str, Any]) -> dict[str, Any] | None:
    if row.get("completed") is True:
        return None
    typ = str(row.get("media_type") or "").strip().lower()
    ids_raw = row.get("ids")
    ids = ids_raw if isinstance(ids_raw, Mapping) else {}
    tmdb = str(ids.get("tmdb") or (row.get("media_id") if str(row.get("source") or "").strip() == "tmdb" else "") or "").strip()
    if not tmdb:
        return None
    if typ == "episode":
        season = _int(row.get("season_number"))
        episode = _int(row.get("episode_number"))
        if season is None or episode is None:
            return None
        show_ids = {str(key): value for key, value in ids.items() if key in {"tmdb", "imdb", "tvdb"} and value}
        show_ids["tmdb"] = tmdb
        item: dict[str, Any] = {"type": "episode", "show_ids": show_ids, "season": season, "episode": episode}
        if str(row.get("series_title") or "").strip():
            item["series_title"] = str(row.get("series_title") or "").strip()
    elif typ == "movie":
        item = {"type": "movie", "ids": {"tmdb": tmdb}}
    else:
        return None
    title = str(row.get("title") or "").strip()
    if title:
        item["title"] = title
    position = _ms_from_seconds(row.get("position_seconds"))
    duration = _ms_from_seconds(row.get("duration_seconds"))
    if not position:
        return None
    item["progress_ms"] = position
    if duration:
        item["duration_ms"] = duration
        item["progress_percent"] = round((position / duration) * 100.0, 3)
    if str(row.get("updated_at") or "").strip():
        item["progress_at"] = str(row.get("updated_at") or "").strip()
    return item


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in paged(adapter, "playback/progress", params={"media_type": "movie,episode", "completed": "false"}):
        item = _row_item(row)
        if not item:
            continue
        key = canonical_item_key(item)
        selected, _reason = select_progress_record(out.get(key), item)
        out[key] = selected
    return out


def _payload(item: Mapping[str, Any], *, clear: bool = False) -> tuple[dict[str, Any] | None, str]:
    typ = str(id_minimal(item).get("type") or "").strip().lower()
    if typ == "movie":
        tmdb = tmdb_id_for_item(item)
        body: dict[str, Any] = {"media_type": "movie", "ids": {"tmdb": str(tmdb or "")}}
    elif typ == "episode":
        tmdb = tmdb_id_for_item(item, episode_show=True)
        season = _int(item.get("season") if item.get("season") is not None else item.get("season_number"))
        episode = _int(item.get("episode") if item.get("episode") is not None else item.get("episode_number"))
        body = {"media_type": "episode", "ids": {"tmdb": str(tmdb or "")}, "season_number": season, "episode_number": episode}
    else:
        return None, "floppy_progress_type_unsupported"
    if not str(body["ids"].get("tmdb") or "").strip():
        return None, "floppy_tmdb_id_missing"
    if typ == "episode" and (body.get("season_number") is None or body.get("episode_number") is None):
        return None, "floppy_episode_id_missing"
    if clear:
        return body, ""
    duration = _duration_ms(item)
    position = _progress_ms(item, duration)
    if position is None or position <= 0:
        return None, "floppy_progress_missing"
    if not duration:
        return None, "floppy_duration_missing"
    body["position_seconds"] = _seconds_from_ms(position)
    body["duration_seconds"] = max(1, _seconds_from_ms(duration) or 0)
    return body, ""


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, items, clear=False, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, items, clear=True, dry_run=dry_run)


def _write(adapter: Any, items: Iterable[Mapping[str, Any]], *, clear: bool, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_rows: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    for raw in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        key = canonical_item_key(raw)
        body, reason = _payload(raw, clear=clear)
        if body is None:
            entry = unresolved(raw, reason or "floppy_progress_unresolved")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        if dry_run:
            confirmed.append(key)
            results.append({"status": "dry_run", "item": id_minimal(raw), "canonical_key": key})
            continue
        try:
            if clear:
                api_put(adapter, "playback/progress", json={**body, "position_seconds": None})
            else:
                api_put(adapter, "playback/progress", json=body)
        except Exception as exc:
            entry = unresolved(raw, failure_reason(exc))
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        confirmed.append(key)
        results.append({"status": "applied", "item": id_minimal(raw), "canonical_key": key})
    return build_op_result(ok=not unresolved_rows, count=len(confirmed), confirmed_keys=confirmed, unresolved_keys=unresolved_keys(unresolved_rows, canonical_item_key), unresolved=unresolved_rows, results=results, attempted=len(confirmed) + len(unresolved_rows))
