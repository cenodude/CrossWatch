# providers/sync/floppy/_history.py
# CrossWatch - Floppy history sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from typing import Any

from cw_platform.history_events import history_sync_key, minimal_history_item
from cw_platform.id_map import minimal as id_minimal
from providers.sync._mod_common import build_op_result, unresolved_keys

from ._common import COMPLETED, absolute_to_coord, api_delete, api_patch, api_post, canonical_item_key, failure_reason, has_coord, int_or_none, item_from_row, paged, reset_layout_cache, show_layout, tmdb_id_for_item, track_media, unresolved

_SRC_SNAPSHOT: dict[str, Any] = {"scope": None, "shows": {}}


def _rewatches_enabled(adapter: Any) -> bool:
    cfg = getattr(adapter, "config", None)
    return bool(isinstance(cfg, Mapping) and cfg.get("_cw_history_rewatches"))


def _history_key(adapter: Any, item: Mapping[str, Any]) -> str:
    return history_sync_key(item, event_mode=_rewatches_enabled(adapter))


def _history_minimal(adapter: Any, item: Mapping[str, Any]) -> dict[str, Any]:
    return minimal_history_item(item, event_mode=_rewatches_enabled(adapter))


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


def _explicit_absolute(item: Mapping[str, Any]) -> int | None:
    for key in ("_simkl_episode_number", "_trakt_number_abs"):
        number = int_or_none(item.get(key))
        if number is not None and number > 0:
            return number
    return None


def _absolute_hint(item: Mapping[str, Any], season: int, episode: int) -> int | None:
    explicit = _explicit_absolute(item)
    if explicit is not None:
        return explicit
    return episode if season == 1 and episode > 0 else None


def _floppy_coord(item: Mapping[str, Any]) -> tuple[int, int] | None:
    season = int_or_none(item.get("_floppy_season"))
    episode = int_or_none(item.get("_floppy_episode"))
    if season is None or episode is None or season < 0 or episode <= 0:
        return None
    return season, episode


def _pair_scope() -> str:
    for name in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        value = str(os.getenv(name) or "").strip()
        if value:
            return value
    return ""


def prepare_source_snapshot(items: Iterable[Mapping[str, Any]]) -> int:
    shows: dict[str, dict[str, Any]] = {}
    reset_layout_cache()
    count = 0
    for item in items or []:
        if not isinstance(item, Mapping) or str(item.get("type") or "").strip().lower() != "episode":
            continue
        tmdb_id = tmdb_id_for_item(item, episode_show=True)
        if not tmdb_id:
            continue
        season, episode = _episode_numbers(item)
        if season is None or episode is None or season < 1 or episode < 1:
            continue
        record = shows.setdefault(tmdb_id, {"coords": set(), "abs": {}})
        record["coords"].add((season, episode))
        hint = _absolute_hint(item, season, episode)
        if hint is not None:
            record["abs"][(season, episode)] = hint
        count += 1
    _SRC_SNAPSHOT["scope"] = _pair_scope()
    _SRC_SNAPSHOT["shows"] = shows
    return count


def _rekey_to_source_numbering(adapter: Any, out: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    shows: dict[str, dict[str, Any]] = _SRC_SNAPSHOT.get("shows") or {}
    if not shows or not out or _SRC_SNAPSHOT.get("scope") != _pair_scope():
        return out
    owned_by_show: dict[str, dict[tuple[int, int], str]] = {}
    for key, item in out.items():
        if str(item.get("type") or "").strip().lower() != "episode":
            continue
        raw_show_ids = item.get("show_ids")
        show_ids: Mapping[str, Any] = raw_show_ids if isinstance(raw_show_ids, Mapping) else {}
        show = str(show_ids.get("tmdb") or "").strip()
        season, episode = _episode_numbers(item)
        if not show or season is None or episode is None:
            continue
        owned_by_show.setdefault(show, {})[(season, episode)] = key

    for show, record in shows.items():
        owned = owned_by_show.get(show)
        if not owned:
            continue
        wanted: set[tuple[int, int]] = record["coords"]
        missing = [coord for coord in wanted if coord not in owned]
        if not missing:
            continue
        layout = show_layout(adapter, show)
        if not layout:
            continue
        for coord in sorted(missing):
            absolute = record["abs"].get(coord)
            if absolute is None:
                continue
            real = absolute_to_coord(layout, absolute)
            if real is None or real == coord or real in wanted:
                continue
            key = owned.get(real)
            if not key:
                continue
            item = out.get(key)
            if not isinstance(item, Mapping):
                continue
            rekeyed = dict(item)
            rekeyed["_floppy_season"], rekeyed["_floppy_episode"] = real
            rekeyed["season"], rekeyed["episode"] = coord
            new_key = _history_key(adapter, rekeyed) if _rewatches_enabled(adapter) else canonical_item_key(rekeyed)
            if new_key in out:
                continue
            out.pop(key, None)
            out[new_key] = rekeyed
            owned.pop(real, None)
            owned[coord] = new_key
    return out


def _write_coord(adapter: Any, tmdb_id: str, item: Mapping[str, Any], season: int, episode: int) -> tuple[int, int]:
    stored = _floppy_coord(item)
    if stored is not None:
        return stored
    absolute = _explicit_absolute(item)
    if absolute is None:
        return season, episode
    layout = show_layout(adapter, tmdb_id)
    if not layout or has_coord(layout, season, episode):
        return season, episode
    real = absolute_to_coord(layout, absolute)
    return real if real is not None else (season, episode)


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


def _movie_history_item(tmdb_id: str, row: Mapping[str, Any]) -> dict[str, Any] | None:
    merged = {
        "item_id": f"movie/tmdb/{tmdb_id}",
        "media_type": "movie",
        "source": "tmdb",
        "media_id": tmdb_id,
        **dict(row or {}),
    }
    item = item_from_row(merged, force_type="movie")
    if not item:
        return None
    watched_at = row.get("end_date") or row.get("progressed_at") or row.get("created_at")
    if not watched_at:
        return None
    item["watched"] = True
    item["watched_at"] = watched_at
    item["_floppy_consumption_id"] = row.get("consumption_id")
    return item


def _movie_external_id(adapter: Any, item: Mapping[str, Any], key: str) -> str | None:
    if not _rewatches_enabled(adapter):
        return None
    for field in (
        "provider_event_id",
        "_trakt_history_id",
        "trakt_history_id",
        "_publicmetadb_history_id",
        "_simkl_history_id",
        "_simkl_rewatch_id",
        "_punchplay_history_id",
        "rewatch_id",
    ):
        value = str(item.get(field) or "").strip()
        if value:
            return f"cw:{field}:{value}"[:255]
    return f"cw:{key}"[:255] if key else None


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    event_mode = _rewatches_enabled(adapter)
    for row in paged(adapter, "media/movie", params={"status": COMPLETED}):
        if int_or_none(row.get("status")) != COMPLETED and not row.get("end_date"):
            continue
        item = item_from_row(row, force_type="movie")
        if not item:
            continue
        if event_mode:
            tmdb_id = tmdb_id_for_item(item)
            history_rows = _movie_history(adapter, tmdb_id) if tmdb_id else []
            expanded = [_movie_history_item(tmdb_id, hist) for hist in history_rows] if tmdb_id else []
            expanded = [hist for hist in expanded if hist]
            if expanded:
                for hist_item in expanded:
                    key = _history_key(adapter, hist_item)
                    if key:
                        out[key] = _history_minimal(adapter, hist_item)
                continue
        item["watched"] = True
        item["watched_at"] = row.get("end_date") or row.get("progressed_at") or row.get("created_at")
        item["_floppy_consumption_id"] = row.get("consumption_id")
        if event_mode:
            key = _history_key(adapter, item)
            if key:
                out[key] = _history_minimal(adapter, item)
        else:
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
        if event_mode:
            key = _history_key(adapter, item)
            if key:
                out[key] = _history_minimal(adapter, item)
        else:
            _put_latest(out, item)
    return _rekey_to_source_numbering(adapter, out)


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_rows: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    for raw in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        key = _history_key(adapter, raw)
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
                results.append({"status": "dry_run", "item": _history_minimal(adapter, raw), "canonical_key": key})
                continue
            try:
                _write_movie_history(adapter, tmdb_id, _watched_at(raw), raw, key)
            except Exception as exc:
                entry = unresolved(raw, failure_reason(exc))
                unresolved_rows.append(entry)
                results.append(entry)
                continue
            confirmed.append(key)
            results.append({"status": "applied", "item": _history_minimal(adapter, raw), "canonical_key": key})
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
                results.append({"status": "dry_run", "item": _history_minimal(adapter, raw), "canonical_key": key})
                continue
            try:
                season, episode = _write_coord(adapter, tmdb_id, raw, season, episode)
                if _rewatches_enabled(adapter) or not _episode_history(adapter, tmdb_id, season, episode):
                    api_post(adapter, f"media/tv/tmdb/{tmdb_id}/{season}/episodes/{episode}/watch", json={"end_date": _watched_at(raw)})
            except Exception as exc:
                entry = unresolved(raw, failure_reason(exc))
                unresolved_rows.append(entry)
                results.append(entry)
                continue
            confirmed.append(key)
            results.append({"status": "applied", "item": _history_minimal(adapter, raw), "canonical_key": key})
            continue
        entry = unresolved(raw, "floppy_history_type_unsupported")
        unresolved_rows.append(entry)
        results.append(entry)
    return build_op_result(ok=not unresolved_rows, count=len(confirmed), confirmed_keys=confirmed, unresolved_keys=unresolved_keys(unresolved_rows, lambda it: _history_key(adapter, it)), unresolved=unresolved_rows, results=results, attempted=len(confirmed) + len(unresolved_rows))


def _write_movie_history(adapter: Any, tmdb_id: str, watched_at: str, item: Mapping[str, Any], key: str) -> None:
    if _rewatches_enabled(adapter):
        payload = {"end_date": watched_at}
        external_id = _movie_external_id(adapter, item, key)
        if external_id:
            payload["external_id"] = external_id
        api_post(adapter, f"media/movie/tmdb/{tmdb_id}/watch", json=payload)
        return
    track_media(adapter, "movie", tmdb_id, payload={"status": COMPLETED, "end_date": watched_at})


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_rows: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    for raw in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        key = _history_key(adapter, raw)
        typ = str(id_minimal(raw).get("type") or "").lower()
        if dry_run:
            confirmed.append(key)
            results.append({"status": "dry_run", "item": _history_minimal(adapter, raw), "canonical_key": key})
            continue
        try:
            if typ == "movie":
                tmdb_id = tmdb_id_for_item(raw)
                if not tmdb_id:
                    raise ValueError("missing tmdb")
                history_id = _history_id(raw)
                if not history_id and _rewatches_enabled(adapter):
                    raise ValueError("missing history id")
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
                season, episode = _write_coord(adapter, tmdb_id, raw, season, episode)
                history_id = _history_id(raw)
                if not history_id and _rewatches_enabled(adapter):
                    raise ValueError("missing history id")
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
        results.append({"status": "applied", "item": _history_minimal(adapter, raw), "canonical_key": key})
    return build_op_result(ok=not unresolved_rows, count=len(confirmed), confirmed_keys=confirmed, unresolved_keys=unresolved_keys(unresolved_rows, lambda it: _history_key(adapter, it)), unresolved=unresolved_rows, results=results, attempted=len(confirmed) + len(unresolved_rows))
