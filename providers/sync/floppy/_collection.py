# providers/sync/floppy/_collection.py
# CrossWatch - Floppy collection sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.id_map import minimal as id_minimal
from providers.auth._auth_FLOPPY import FloppyAuthError
from providers.sync._mod_common import build_op_result, unresolved_keys

from ._common import PLANNING, api_delete, api_get, api_post, canonical_item_key, confirmed_destination, failure_reason, int_or_none, paged, tmdb_enriched_item, tmdb_id_for_item, unresolved


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
    collected_at = str(row.get("collected_at") or "").strip()
    if collected_at:
        out["collected_at"] = collected_at
    return id_minimal(out) | {k: v for k, v in out.items() if k.startswith("_floppy_") or k in {"format", "collected_at", "poster"}}


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in paged(adapter, "collection"):
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


def _detail_path(media_type: str, tmdb_id: str, season: int | None = None, episode: int | None = None) -> str:
    if media_type == "episode" and season is not None and episode is not None:
        return f"media/episode/tmdb/{tmdb_id}/{season}/{episode}"
    if media_type == "season" and season is not None:
        return f"media/season/tmdb/{tmdb_id}/{season}"
    return f"media/{media_type}/tmdb/{tmdb_id}"


def _item_id_from_response(data: Any) -> str | None:
    if not isinstance(data, Mapping):
        return None
    for key in ("id", "item_pk", "item_db_id"):
        value = str(data.get(key) or "").strip()
        if value.isdigit():
            return value
    return None


def _ensure_item_id(adapter: Any, item: Mapping[str, Any]) -> tuple[str | None, str]:
    media_type = _floppy_media_type(item)
    if not media_type:
        return None, "floppy_collection_type_unsupported"
    episode_show = media_type in {"season", "episode"}
    tmdb_id = tmdb_id_for_item(item, episode_show=episode_show)
    if not tmdb_id:
        return None, "floppy_tmdb_id_missing"
    season, episode = _media_numbers(item)
    if media_type in {"season", "episode"} and season is None:
        return None, "floppy_season_id_missing"
    if media_type == "episode" and episode is None:
        return None, "floppy_episode_id_missing"

    body: dict[str, Any] = {"source": "tmdb", "media_id": str(tmdb_id), "status": PLANNING}
    if season is not None:
        body["season_number"] = season
    if episode is not None:
        body["episode_number"] = episode
    title = str(item.get("title") or item.get("series_title") or "").strip()
    if title:
        body["title"] = title
    poster = str(item.get("poster") or item.get("image") or "").strip()
    if poster:
        body["image"] = poster

    try:
        item_id = _item_id_from_response(api_post(adapter, f"media/{media_type}", json=body))
        if item_id:
            return item_id, ""
    except FloppyAuthError as exc:
        if getattr(exc, "status_code", None) not in {400, 409}:
            return None, failure_reason(exc)

    try:
        item_id = _item_id_from_response(api_get(adapter, _detail_path(media_type, str(tmdb_id), season, episode)))
        if item_id:
            return item_id, ""
    except Exception as exc:
        return None, failure_reason(exc)
    return None, "floppy_item_id_missing"


def _collection_body(adapter: Any, item: Mapping[str, Any]) -> tuple[dict[str, Any] | None, str]:
    item_id, reason = _ensure_item_id(adapter, item)
    if not item_id:
        return None, reason
    body: dict[str, Any] = {"item_id": item_id}
    fmt = str(item.get("format") or item.get("edition_format") or "digital").strip().lower()
    if fmt:
        body["media_type"] = fmt[:20]
    collected_at = str(item.get("collected_at") or "").strip()
    if collected_at:
        body["collected_at"] = collected_at
    return body, ""


def _collection_ids_for_remove(adapter: Any, item: Mapping[str, Any]) -> list[str]:
    direct = item.get("_floppy_collection_ids")
    ids = [str(x).strip() for x in direct if str(x or "").strip()] if isinstance(direct, list) else []
    single = str(item.get("_floppy_collection_id") or item.get("collection_id") or "").strip()
    if single:
        ids.append(single)
    if ids:
        return list(dict.fromkeys(ids))
    key = canonical_item_key(item)
    if not key:
        return []
    try:
        match = build_index(adapter).get(key)
    except Exception:
        match = None
    if not isinstance(match, Mapping):
        return []
    found = match.get("_floppy_collection_ids")
    ids = [str(x).strip() for x in found if str(x or "").strip()] if isinstance(found, list) else []
    single = str(match.get("_floppy_collection_id") or "").strip()
    if single:
        ids.append(single)
    return list(dict.fromkeys(ids))


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    confirmed_destinations: dict[str, dict[str, Any]] = {}
    unresolved_rows: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    for raw in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        key = canonical_item_key(raw)
        item = tmdb_enriched_item(adapter, raw, episode_show=str(id_minimal(raw).get("type") or "").strip().lower() in {"season", "episode"})
        if dry_run:
            confirmed.append(key)
            confirmed_destinations[key] = confirmed_destination(item)
            results.append({"status": "dry_run", "item": id_minimal(item), "canonical_key": key})
            continue
        body, reason = _collection_body(adapter, item)
        if body is None:
            entry = unresolved(item, reason or "floppy_collection_unresolved")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        try:
            api_post(adapter, "collection", json=body)
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
    for raw in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        key = canonical_item_key(raw)
        if dry_run:
            confirmed.append(key)
            confirmed_destinations[key] = confirmed_destination(raw)
            results.append({"status": "dry_run", "item": id_minimal(raw), "canonical_key": key})
            continue
        entry_ids = _collection_ids_for_remove(adapter, raw)
        if not entry_ids:
            entry = unresolved(raw, "floppy_collection_id_missing")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        failed = False
        for entry_id in entry_ids:
            try:
                api_delete(adapter, f"collection/{entry_id}")
            except Exception:
                failed = True
        if failed:
            entry = unresolved(raw, "floppy_collection_remove_failed")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        confirmed.append(key)
        confirmed_destinations[key] = confirmed_destination(raw)
        results.append({"status": "applied", "item": id_minimal(raw), "canonical_key": key})
    return build_op_result(ok=not unresolved_rows, count=len(confirmed), confirmed_keys=confirmed, confirmed_destinations=confirmed_destinations, unresolved_keys=unresolved_keys(unresolved_rows, canonical_item_key), unresolved=unresolved_rows, results=results, attempted=len(confirmed) + len(unresolved_rows))
