# providers/sync/floppy/_history.py
# CrossWatch - Floppy history sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from typing import Any

from cw_platform.anime_mapping.coordinates import translate
from cw_platform.anime_mapping.overrides import find_source_override
from cw_platform.anime_mapping.service import PAIR_FEATURE_OPTIONS_KEY, mapping_enabled_for_feature, runtime_pair_feature_options
from cw_platform.anime_mapping.storage import query_edges
from cw_platform.history_events import history_epoch_from_value, history_sync_key, minimal_history_item
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


def _anime_mapping_enabled(adapter: Any) -> bool:
    cfg = getattr(adapter, "config", None)
    if not isinstance(cfg, Mapping):
        return False
    if PAIR_FEATURE_OPTIONS_KEY in cfg:
        return bool(runtime_pair_feature_options(cfg, "history").get("use_anime_mapping"))
    return bool(mapping_enabled_for_feature(cfg, "history"))


def _anime_release_tag(adapter: Any) -> str:
    cfg = getattr(adapter, "config", None)
    block = cfg.get("anime_mapping") if isinstance(cfg, Mapping) else {}
    if isinstance(block, Mapping):
        value = str(block.get("release_tag") or "").strip()
        if value:
            return value
    return "v3"


def _anime_native_ids(item: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in (item.get("show_ids"), item.get("ids")):
        if not isinstance(raw, Mapping):
            continue
        for key in ("anidb", "mal", "anilist", "kitsu", "simkl"):
            value = str(raw.get(key) or "").strip()
            if value:
                out.setdefault(key, value)
    amap = item.get("_cw_anime_map")
    if isinstance(amap, Mapping):
        namespace = str(amap.get("namespace") or "").strip().lower()
        target_id = str(amap.get("target_id") or "").strip()
        if namespace and target_id:
            out.setdefault(namespace, target_id)
    return out


def _show_ids(item: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    raw = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else item.get("ids")
    if not isinstance(raw, Mapping):
        return out
    for key, value in raw.items():
        text = str(value or "").strip()
        if text:
            out[str(key).strip().lower()] = text
    return out


def _override_coordinate_for_absolute(
    item: Mapping[str, Any],
    tmdb_id: str,
    native_ids: Mapping[str, str],
    absolute: int,
) -> tuple[int, int] | None:
    known_ids = _show_ids(item)
    if tmdb_id:
        known_ids.setdefault("tmdb", str(tmdb_id))
    for namespace in ("simkl", "anidb", "mal", "anilist", "kitsu"):
        ident = native_ids.get(namespace, "")
        if not ident:
            continue
        try:
            ruled = find_source_override(namespace, ident, absolute)
        except Exception:
            ruled = None
        if ruled is None:
            continue
        provider = str(ruled.provider or "").strip().lower()
        expected = known_ids.get(provider)
        if expected and str(ruled.ident or "").strip() != expected:
            continue
        if provider == "tmdb" and tmdb_id and str(ruled.ident or "").strip() != str(tmdb_id):
            continue
        season = int_or_none(ruled.season)
        episode = int_or_none(ruled.episode)
        if season is not None and season >= 0 and episode is not None and episode > 0:
            return season, episode
    return None


def _anime_coordinate_for_absolute(
    adapter: Any,
    tmdb_id: str,
    item: Mapping[str, Any],
    absolute: int,
) -> tuple[int, int] | None:
    if not _anime_mapping_enabled(adapter):
        return None
    if str(item.get("type") or "").strip().lower() != "episode":
        return None
    native_ids = _anime_native_ids(item)
    if not native_ids:
        return None

    release_tag = _anime_release_tag(adapter)
    override_coord = _override_coordinate_for_absolute(item, tmdb_id, native_ids, absolute)
    if override_coord is not None:
        return override_coord

    found: set[tuple[int, int]] = set()
    for namespace in ("anidb", "mal", "anilist", "kitsu"):
        ident = native_ids.get(namespace)
        if not ident:
            continue
        try:
            rows = query_edges(release_tag, namespace, ident)
        except Exception:
            rows = []
        for row in rows:
            if namespace == "anidb" and str(row.get("source_scope") or "").strip().upper() != "R":
                continue
            if str(row.get("target_provider") or "").strip().lower() != "tmdb":
                continue
            if str(row.get("target_kind") or "").strip().lower() != "show":
                continue
            target_id = str(row.get("target_id") or "").strip()
            if target_id and str(target_id) != str(tmdb_id):
                continue
            season = _season_from_scope(row.get("target_scope"))
            if season is None or season < 0:
                continue
            episode = translate(row.get("source_range"), row.get("target_range"), absolute)
            if episode is None or int(episode) <= 0:
                continue
            found.add((int(season), int(episode)))
    if len(found) == 1:
        return next(iter(found))
    return None


def _season_from_scope(value: Any) -> int | None:
    text = str(value or "").strip().lower()
    if not text.startswith("s"):
        return None
    return int_or_none(text[1:])


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
        if season is None or episode is None or season < 0 or episode < 1:
            continue
        record = shows.setdefault(tmdb_id, {"coords": set(), "abs": {}})
        record["coords"].add((season, episode))
        hint = _absolute_hint(item, season, episode)
        if hint is not None:
            record["abs"][(season, episode)] = hint
        record.setdefault("items", {})[(season, episode)] = dict(item)
        count += 1
    _SRC_SNAPSHOT["scope"] = _pair_scope()
    _SRC_SNAPSHOT["shows"] = shows
    return count


def _rekey_to_source_numbering(adapter: Any, out: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    shows: dict[str, dict[str, Any]] = _SRC_SNAPSHOT.get("shows") or {}
    if not shows or not out or _SRC_SNAPSHOT.get("scope") != _pair_scope():
        return out
    event_mode = _rewatches_enabled(adapter)
    owned_by_show: dict[str, dict[tuple[int, int], list[str]]] = {}
    for key, item in out.items():
        if str(item.get("type") or "").strip().lower() != "episode":
            continue
        raw_show_ids = item.get("show_ids")
        show_ids: Mapping[str, Any] = raw_show_ids if isinstance(raw_show_ids, Mapping) else {}
        show = str(show_ids.get("tmdb") or "").strip()
        season, episode = _episode_numbers(item)
        if not show or season is None or episode is None:
            continue
        owned_by_show.setdefault(show, {}).setdefault((season, episode), []).append(key)

    for show, record in shows.items():
        owned = owned_by_show.get(show)
        if not owned:
            continue
        wanted: set[tuple[int, int]] = record["coords"]
        missing = [coord for coord in wanted if coord not in owned]
        if not missing:
            continue
        layout: list[tuple[int, int]] | None = None
        for coord in sorted(missing):
            absolute = record["abs"].get(coord)
            if absolute is None:
                continue
            source_item = (record.get("items") or {}).get(coord)
            real = (
                _anime_coordinate_for_absolute(adapter, show, source_item, absolute)
                if isinstance(source_item, Mapping)
                else None
            )
            if real is None:
                if layout is None:
                    layout = show_layout(adapter, show)
                if not layout:
                    continue
                real = absolute_to_coord(layout, absolute)
            if real is None or real == coord:
                continue
            keys = owned.get(real) or []
            key = ""
            if event_mode and isinstance(source_item, Mapping):
                source_key = _history_key(adapter, source_item)
                source_event = source_key.rsplit("@", 1)[-1] if "@" in source_key else ""
                for candidate in keys:
                    candidate_event = candidate.rsplit("@", 1)[-1] if "@" in candidate else ""
                    if source_event and candidate_event == source_event:
                        key = candidate
                        break
            if not key and real not in wanted:
                key = keys[0] if keys else ""
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
            if key in keys:
                keys.remove(key)
            if keys:
                owned[real] = keys
            else:
                owned.pop(real, None)
            owned.setdefault(coord, []).append(new_key)
    return out


def _write_coord_result(adapter: Any, tmdb_id: str, item: Mapping[str, Any], season: int, episode: int) -> tuple[int, int, bool]:
    stored = _floppy_coord(item)
    if stored is not None:
        return stored[0], stored[1], False
    absolute = _explicit_absolute(item)
    if absolute is None:
        return season, episode, False
    mapped = _anime_coordinate_for_absolute(adapter, tmdb_id, item, absolute)
    if mapped is not None:
        return mapped[0], mapped[1], mapped != (season, episode)
    layout = show_layout(adapter, tmdb_id)
    if not layout or has_coord(layout, season, episode):
        return season, episode, False
    real = absolute_to_coord(layout, absolute)
    if real is not None:
        return real[0], real[1], real != (season, episode)
    return season, episode, False


def _write_coord(adapter: Any, tmdb_id: str, item: Mapping[str, Any], season: int, episode: int) -> tuple[int, int]:
    real_season, real_episode, _verify = _write_coord_result(adapter, tmdb_id, item, season, episode)
    return real_season, real_episode


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


def _history_rows_include_write(adapter: Any, item: Mapping[str, Any], rows: Iterable[Mapping[str, Any]]) -> bool:
    rows_list = [row for row in rows or [] if isinstance(row, Mapping)]
    if not rows_list:
        return False
    if not _rewatches_enabled(adapter):
        return True
    target = history_epoch_from_value(_watched_at(item))
    if target is None:
        return True
    for row in rows_list:
        row_ts = history_epoch_from_value(row.get("end_date") or row.get("progressed_at") or row.get("created_at") or row.get("watched_at"))
        if row_ts == target:
            return True
    return False


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
                season, episode, verify_after_write = _write_coord_result(adapter, tmdb_id, raw, season, episode)
                existing_history = _episode_history(adapter, tmdb_id, season, episode)
                if _rewatches_enabled(adapter) or not existing_history:
                    api_post(adapter, f"media/tv/tmdb/{tmdb_id}/{season}/episodes/{episode}/watch", json={"end_date": _watched_at(raw)})
                if verify_after_write and not _history_rows_include_write(adapter, raw, _episode_history(adapter, tmdb_id, season, episode)):
                    entry = unresolved(raw, "floppy_history_write_not_readable")
                    unresolved_rows.append(entry)
                    results.append(entry)
                    continue
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
