# providers/sync/floppy/_collection.py
# CrossWatch - Floppy collection sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.collection_media import media_block, to_floppy_fields
from cw_platform.id_map import minimal as id_minimal
from providers.auth._auth_FLOPPY import FloppyAuthError
from providers.sync._mod_common import build_op_result, unresolved_keys

from ._common import ANIME_BUCKET, api_patch, api_put, api_request, bucket_params, canonical_item_key, confirmed_destination, failure_reason, int_or_none, paged, tmdb_enriched_item, tmdb_id_for_item, unresolved, write_bucket


def _media_numbers(item: Mapping[str, Any]) -> tuple[int | None, int | None]:
    season = int_or_none(item.get("season") if item.get("season") is not None else item.get("season_number"))
    episode = int_or_none(item.get("episode") if item.get("episode") is not None else item.get("episode_number"))
    return season, episode


def _ids_from_floppy_item(row: Mapping[str, Any]) -> dict[str, str]:
    ids_raw = row.get("ids")
    ids = {str(k).strip().lower(): str(v).strip() for k, v in ids_raw.items() if str(v or "").strip()} if isinstance(ids_raw, Mapping) else {}
    if str(row.get("source") or "").strip().lower() == "tmdb":
        media_id = str(row.get("media_id") or "").strip()
        if media_id:
            ids.setdefault("tmdb", media_id)
    return {k: v for k, v in ids.items() if k in {"tmdb", "imdb", "tvdb", "trakt", "simkl", "mal", "anilist", "kitsu", "anidb"}}


def _to_minimal(row: Mapping[str, Any]) -> dict[str, Any] | None:
    item_raw = row.get("item")
    item: Mapping[str, Any] = item_raw if isinstance(item_raw, Mapping) else row
    ids = _ids_from_floppy_item(item)
    if not ids:
        return None
    raw_type = str(item.get("media_type") or "").strip().lower()
    season = int_or_none(item.get("season_number") if item.get("season_number") is not None else item.get("season"))
    episode = int_or_none(item.get("episode_number") if item.get("episode_number") is not None else item.get("episode"))
    if raw_type == "episode" or (season is not None and episode is not None):
        if season is None or episode is None:
            return None
        out: dict[str, Any] = {"type": "episode", "show_ids": dict(ids), "season": season, "episode": episode}
    elif raw_type == "season" or season is not None:
        if season is None:
            return None
        out = {"type": "season", "ids": dict(ids), "show_ids": dict(ids), "season": season}
    elif raw_type in {"tv", "show", "series"}:
        out = {"type": "show", "ids": dict(ids)}
    elif raw_type == "movie":
        out = {"type": "movie", "ids": dict(ids)}
    else:
        return None

    title = str(item.get("title") or "").strip()
    if title:
        out["title"] = title
    year = int_or_none(item.get("year"))
    if year:
        out["year"] = year
    poster = str(item.get("image") or item.get("poster") or "").strip()
    if poster:
        out["poster"] = poster
    entry_id = row.get("id")
    if entry_id is not None:
        out["_floppy_collection_id"] = str(entry_id)
        out["_floppy_collection_ids"] = [str(entry_id)]
    fmt = str(row.get("media_type") or "").strip()
    if fmt:
        out["format"] = fmt
    media_info = media_block({"resolution": row.get("resolution"), "hdr": row.get("hdr"), "audio": row.get("audio_codec"), "audio_channels": row.get("audio_channels"), "is_3d": True if row.get("is_3d") else None})
    if media_info:
        out["media_info"] = media_info
    collected_at = str(row.get("collected_at") or "").strip()
    if collected_at:
        out["collected_at"] = collected_at
    return id_minimal(out) | {k: v for k, v in out.items() if k.startswith("_floppy_") or k in {"format", "media_info", "collected_at", "poster"}}


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in paged(adapter, "collection", require_total=True):
        item = _to_minimal(row)
        if not item:
            continue
        key = canonical_item_key(item)
        if key in out:
            ids = out[key].setdefault("_floppy_collection_ids", [])
            if isinstance(ids, list) and item.get("_floppy_collection_id"):
                ids.append(str(item["_floppy_collection_id"]))
            continue
        out[key] = item
    return out


def _floppy_media_type(item: Mapping[str, Any]) -> str | None:
    typ = str(id_minimal(item).get("type") or "").strip().lower()
    if typ == "movie":
        return "movie"
    if typ in {"show", "series", "tv"}:
        return "tv"
    if typ == "season":
        return "season"
    if typ == "episode":
        return "episode"
    return None


def _collection_path(item: Mapping[str, Any]) -> tuple[str | None, str]:
    media_type = _floppy_media_type(item)
    if not media_type or media_type in {"tv", "season"}:
        return None, "floppy_collection_type_unsupported"
    tmdb_id = tmdb_id_for_item(item, episode_show=media_type == "episode")
    if not tmdb_id:
        return None, "floppy_tmdb_id_missing"
    if media_type == "movie":
        return f"media/movie/tmdb/{tmdb_id}/collection", ""
    season, episode = _media_numbers(item)
    if season is None:
        return None, "floppy_season_id_missing"
    if episode is None:
        return None, "floppy_episode_id_missing"
    return f"media/tv/tmdb/{tmdb_id}/{season}/episodes/{episode}/collection", ""


def _collection_target(adapter: Any, item: Mapping[str, Any]) -> tuple[str | None, dict[str, str] | None, str]:
    path, reason = _collection_path(item)
    if path is None or _floppy_media_type(item) != "episode":
        return path, None, reason
    try:
        bucket = write_bucket(adapter, tmdb_id_for_item(item, episode_show=True), item)
    except Exception as exc:
        return None, None, failure_reason(exc)
    return path, bucket_params(bucket), ""


def _collected_at(value: Any) -> str:
    text = str(value or "").strip()
    return f"{text}T00:00:00Z" if len(text) == 10 and text[4] == "-" and text[7] == "-" else text


def _collection_body(item: Mapping[str, Any]) -> dict[str, Any]:
    body: dict[str, Any] = {}
    fmt = str(item.get("format") or item.get("edition_format") or "digital").strip().lower()
    if fmt:
        body["media_type"] = fmt[:20]
    body.update(to_floppy_fields(item))
    collected_at = _collected_at(item.get("collected_at"))
    if collected_at:
        body["collected_at"] = collected_at
    return body


def _update_entry_id(item: Mapping[str, Any]) -> str:
    if not item.get("_cw_media_update"):
        return ""
    ids = item.get("_floppy_collection_ids")
    first = next((str(x).strip() for x in ids if str(x or "").strip()), "") if isinstance(ids, list) else ""
    return first or str(item.get("_floppy_collection_id") or "").strip()


def _update_body(item: Mapping[str, Any]) -> dict[str, Any]:
    body: dict[str, Any] = {}
    fmt = str(item.get("format") or "").strip().lower()
    if fmt:
        body["media_type"] = fmt[:20]
    body.update(to_floppy_fields(item))
    return body


def _scoped_delete(adapter: Any, path: str, params: dict[str, str] | None) -> str:
    try:
        api_request(adapter, "DELETE", path, ok=(200, 202, 204), params=params)
    except FloppyAuthError as exc:
        if exc.status_code != 404:
            raise
        detail = str(exc).lower()
        if "through the api" in detail:
            return "not_api_copy"
        if "collection entry not found" in detail:
            return "missing"
        raise
    return "removed"


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    confirmed_destinations: dict[str, dict[str, Any]] = {}
    unresolved_rows: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    for raw in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        key = canonical_item_key(raw)
        entry_id = _update_entry_id(raw)
        if raw.get("_cw_media_update") and not entry_id:
            entry = unresolved(raw, "floppy_collection_id_missing")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        if entry_id:
            if not dry_run:
                try:
                    api_patch(adapter, f"collection/{entry_id}", json=_update_body(raw))
                except Exception as exc:
                    entry = unresolved(raw, failure_reason(exc))
                    unresolved_rows.append(entry)
                    results.append(entry)
                    continue
            confirmed.append(key)
            confirmed_destinations[key] = confirmed_destination(raw)
            results.append({"status": "dry_run" if dry_run else "updated", "item": id_minimal(raw), "canonical_key": key})
            continue
        if _floppy_media_type(raw) in {"tv", "season"}:
            entry = unresolved(raw, "floppy_collection_episodes_only")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        try:
            item = tmdb_enriched_item(adapter, raw, episode_show=str(id_minimal(raw).get("type") or "").strip().lower() in {"season", "episode"})
        except Exception as exc:
            entry = unresolved(raw, failure_reason(exc))
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        if dry_run:
            confirmed.append(key)
            confirmed_destinations[key] = confirmed_destination(item)
            results.append({"status": "dry_run", "item": id_minimal(item), "canonical_key": key})
            continue
        try:
            path, params, reason = _collection_target(adapter, item)
        except Exception as exc:
            path, params, reason = None, None, failure_reason(exc)
        if path is None:
            entry = unresolved(item, reason or "floppy_collection_unresolved")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        try:
            api_put(adapter, path, json=_collection_body(item), params=params)
        except Exception as exc:
            entry = unresolved(item, failure_reason(exc))
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        confirmed.append(key)
        confirmed_destinations[key] = confirmed_destination(item)
        results.append({"status": "applied", "item": id_minimal(item), "canonical_key": key})
    return build_op_result(ok=not unresolved_rows, count=len(confirmed), confirmed_keys=confirmed, confirmed_destinations=confirmed_destinations, unresolved_keys=unresolved_keys(unresolved_rows, canonical_item_key), unresolved=unresolved_rows, results=results, attempted=len(confirmed) + len(unresolved_rows))


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    confirmed_destinations: dict[str, dict[str, Any]] = {}
    unresolved_rows: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    index: dict[str, dict[str, Any]] | None = None
    index_error: Exception | None = None
    for raw in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        key = canonical_item_key(raw)
        if dry_run:
            confirmed.append(key)
            confirmed_destinations[key] = confirmed_destination(raw)
            results.append({"status": "dry_run", "item": id_minimal(raw), "canonical_key": key})
            continue
        path, params, reason = _collection_target(adapter, raw)
        if path is None:
            entry = unresolved(raw, reason or "floppy_collection_unresolved")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        try:
            outcome = _scoped_delete(adapter, path, params)
            if outcome == "missing" and params is None and _floppy_media_type(raw) == "episode":
                outcome = _scoped_delete(adapter, path, bucket_params(ANIME_BUCKET))
            if outcome == "missing":
                if index is None and index_error is None:
                    try:
                        index = build_index(adapter)
                    except Exception as exc:
                        index_error = exc
                if index_error is not None:
                    raise index_error
                if index is not None and key in index:
                    outcome = "unlinked"
        except Exception as exc:
            outcome, reason = "failed", failure_reason(exc)
        if outcome != "removed" and outcome != "missing":
            entry = unresolved(raw, {"not_api_copy": "floppy_collection_not_api_copy", "unlinked": "floppy_collection_entry_unlinked"}.get(outcome) or reason or "floppy_collection_remove_failed")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        confirmed.append(key)
        confirmed_destinations[key] = confirmed_destination(raw)
        results.append({"status": "applied", "item": id_minimal(raw), "canonical_key": key})
    return build_op_result(ok=not unresolved_rows, count=len(confirmed), confirmed_keys=confirmed, confirmed_destinations=confirmed_destinations, unresolved_keys=unresolved_keys(unresolved_rows, canonical_item_key), unresolved=unresolved_rows, results=results, attempted=len(confirmed) + len(unresolved_rows))
