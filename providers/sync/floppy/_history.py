# providers/sync/floppy/_history.py
# CrossWatch - Floppy history sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from typing import Any

from cw_platform.id_map import minimal as id_minimal
from providers.sync._mod_common import build_op_result, unresolved_keys

from ._common import COMPLETED, api_delete, api_patch, api_post, canonical_item_key, failure_reason, item_from_row, paged, tmdb_id_for_item, track_media, unresolved


def _watched_at(item: Mapping[str, Any]) -> str:
    value = str(item.get("watched_at") or item.get("last_watched_at") or item.get("collected_at") or "").strip()
    return value or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _episode_numbers(item: Mapping[str, Any]) -> tuple[int | None, int | None]:
    season_raw = item.get("season") if item.get("season") is not None else item.get("season_number")
    episode_raw = item.get("episode") if item.get("episode") is not None else item.get("episode_number")
    if season_raw is None or episode_raw is None:
        return None, None
    try:
        season = int(season_raw)
        episode = int(episode_raw)
    except Exception:
        return None, None
    return season, episode


def _history_id(item: Mapping[str, Any]) -> str | None:
    for key in ("_floppy_consumption_id", "consumption_id", "history_id", "_history_id"):
        value = str(item.get(key) or "").strip()
        if value:
            return value
    return None


def _movie_history(adapter: Any, tmdb_id: str) -> list[Mapping[str, Any]]:
    try:
        return paged(adapter, f"media/movie/tmdb/{tmdb_id}/history")
    except Exception:
        return []


def _episode_history(adapter: Any, tmdb_id: str, season: int, episode: int) -> list[Mapping[str, Any]]:
    try:
        return paged(adapter, f"media/tv/tmdb/{tmdb_id}/{season}/{episode}/history")
    except Exception:
        return []


def _put_latest(out: dict[str, dict[str, Any]], item: dict[str, Any]) -> None:
    key = canonical_item_key(item)
    current = out.get(key)
    if not current or str(item.get("watched_at") or "") >= str(current.get("watched_at") or ""):
        out[key] = item


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in paged(adapter, "media/movie", params={"status": COMPLETED}):
        item = item_from_row(row, force_type="movie")
        if not item:
            continue
        item["watched"] = True
        item["watched_at"] = row.get("end_date") or row.get("progressed_at") or row.get("created_at")
        item["_floppy_consumption_id"] = row.get("consumption_id")
        _put_latest(out, item)
    for row in paged(adapter, "media/episode"):
        if not row.get("end_date"):
            continue
        item = item_from_row(row, force_type="episode")
        if not item:
            continue
        item["watched"] = True
        item["watched_at"] = row.get("end_date") or row.get("progressed_at") or row.get("created_at")
        item["_floppy_consumption_id"] = row.get("consumption_id")
        _put_latest(out, item)
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_rows: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    for raw in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        key = canonical_item_key(raw)
        typ = str(id_minimal(raw).get("type") or "").lower()
        if typ == "movie":
            tmdb_id = tmdb_id_for_item(raw)
            if not tmdb_id:
                entry = unresolved(raw, "floppy_tmdb_id_missing")
                unresolved_rows.append(entry)
                results.append(entry)
                continue
            if dry_run:
                confirmed.append(key)
                results.append({"status": "dry_run", "item": id_minimal(raw), "canonical_key": key})
                continue
            try:
                _write_movie_history(adapter, tmdb_id, _watched_at(raw))
            except Exception as exc:
                entry = unresolved(raw, failure_reason(exc))
                unresolved_rows.append(entry)
                results.append(entry)
                continue
            confirmed.append(key)
            results.append({"status": "applied", "item": id_minimal(raw), "canonical_key": key})
            continue
        if typ == "episode":
            tmdb_id = tmdb_id_for_item(raw, episode_show=True)
            season, episode = _episode_numbers(raw)
            if not tmdb_id or season is None or episode is None:
                entry = unresolved(raw, "floppy_episode_id_missing")
                unresolved_rows.append(entry)
                results.append(entry)
                continue
            if dry_run:
                confirmed.append(key)
                results.append({"status": "dry_run", "item": id_minimal(raw), "canonical_key": key})
                continue
            try:
                if not _episode_history(adapter, tmdb_id, season, episode):
                    api_post(adapter, f"media/tv/tmdb/{tmdb_id}/{season}/episodes/{episode}/watch", json={"end_date": _watched_at(raw)})
            except Exception as exc:
                entry = unresolved(raw, failure_reason(exc))
                unresolved_rows.append(entry)
                results.append(entry)
                continue
            confirmed.append(key)
            results.append({"status": "applied", "item": id_minimal(raw), "canonical_key": key})
            continue
        entry = unresolved(raw, "floppy_history_type_unsupported")
        unresolved_rows.append(entry)
        results.append(entry)
    return build_op_result(ok=not unresolved_rows, count=len(confirmed), confirmed_keys=confirmed, unresolved_keys=unresolved_keys(unresolved_rows, canonical_item_key), unresolved=unresolved_rows, results=results, attempted=len(confirmed) + len(unresolved_rows))


def _write_movie_history(adapter: Any, tmdb_id: str, watched_at: str) -> None:
    track_media(adapter, "movie", tmdb_id, payload={"status": COMPLETED, "end_date": watched_at})


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_rows: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    for raw in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        key = canonical_item_key(raw)
        typ = str(id_minimal(raw).get("type") or "").lower()
        if dry_run:
            confirmed.append(key)
            results.append({"status": "dry_run", "item": id_minimal(raw), "canonical_key": key})
            continue
        try:
            if typ == "movie":
                tmdb_id = tmdb_id_for_item(raw)
                if not tmdb_id:
                    raise ValueError("missing tmdb")
                history_id = _history_id(raw)
                if not history_id:
                    rows = _movie_history(adapter, tmdb_id)
                    history_id = str((rows[0] if rows else {}).get("consumption_id") or "").strip()
                if history_id:
                    api_delete(adapter, f"media/movie/tmdb/{tmdb_id}/history/{history_id}")
                else:
                    api_patch(adapter, f"media/movie/tmdb/{tmdb_id}", json={"status": 0, "end_date": None})
            elif typ == "episode":
                tmdb_id = tmdb_id_for_item(raw, episode_show=True)
                season, episode = _episode_numbers(raw)
                if not tmdb_id or season is None or episode is None:
                    raise ValueError("missing episode ids")
                history_id = _history_id(raw)
                if not history_id:
                    rows = _episode_history(adapter, tmdb_id, season, episode)
                    history_id = str((rows[0] if rows else {}).get("consumption_id") or "").strip()
                if history_id:
                    api_delete(adapter, f"media/tv/tmdb/{tmdb_id}/{season}/{episode}/history/{history_id}")
                else:
                    api_delete(adapter, f"media/tv/tmdb/{tmdb_id}/{season}/episodes/{episode}/watch")
            else:
                raise ValueError("unsupported")
        except Exception:
            entry = unresolved(raw, "floppy_history_remove_failed")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        confirmed.append(key)
        results.append({"status": "applied", "item": id_minimal(raw), "canonical_key": key})
    return build_op_result(ok=not unresolved_rows, count=len(confirmed), confirmed_keys=confirmed, unresolved_keys=unresolved_keys(unresolved_rows, canonical_item_key), unresolved=unresolved_rows, results=results, attempted=len(confirmed) + len(unresolved_rows))
