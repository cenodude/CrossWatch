# providers/sync/floppy/_ratings.py
# CrossWatch - Floppy ratings sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from typing import Any

from cw_platform.id_map import minimal as id_minimal
from providers.auth._auth_FLOPPY import FloppyAuthError
from providers.sync._mod_common import build_op_result, unresolved_keys

from ._common import PLANNING, api_patch, api_post, canonical_item_key, confirmed_destination, failure_reason, floppy_type_for_item, int_or_none, item_from_row, media_parts_from_item_id, paged, rating_number, tmdb_enriched_item, track_media, tmdb_id_for_item, unresolved
from ._history import _write_target

_SHADOW_TTL = 180.0
_WRITE_SHADOW: dict[tuple[str, str], dict[str, Any]] = {}


def _scope(adapter: Any) -> str:
    pair = os.getenv("CW_PAIR_KEY") or os.getenv("CW_PAIR_SCOPE") or os.getenv("CW_SYNC_PAIR") or os.getenv("CW_PAIR")
    if not pair:
        return ""
    instance = str(getattr(adapter, "instance_id", "default") or "default").strip() or "default"
    return f"{instance}:{pair}"


def _merge_shadow(adapter: Any, out: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    scope = _scope(adapter)
    if not scope:
        return out
    now = time.time()
    for shadow_key, row in list(_WRITE_SHADOW.items()):
        row_scope, key = shadow_key
        if row_scope != scope:
            continue
        if now - float(row.get("_ts") or 0) > _SHADOW_TTL:
            _WRITE_SHADOW.pop(shadow_key, None)
            continue
        if key in out:
            _WRITE_SHADOW.pop(shadow_key, None)
            continue
        item = row.get("item")
        if isinstance(item, Mapping):
            out[key] = dict(item)
    return out


def _remember(adapter: Any, key: str, item: Mapping[str, Any] | None) -> None:
    scope = _scope(adapter)
    if not scope:
        return
    shadow_key = (scope, key)
    if item is None:
        _WRITE_SHADOW.pop(shadow_key, None)
        return
    _WRITE_SHADOW[shadow_key] = {"_ts": time.time(), "item": dict(item)}


def _rated_at(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _season_from_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    parts = [p for p in str(row.get("item_id") or "").strip("/").split("/") if p]
    if len(parts) != 4 or parts[0] != "tv" or parts[1] != "tmdb":
        return None
    season = int_or_none(parts[3])
    if season is None or season < 0:
        return None
    return {"type": "season", "show_ids": {"tmdb": parts[2]}, "season": season}


def _rated_item(row: Mapping[str, Any], media_type: str) -> dict[str, Any] | None:
    if media_type == "season":
        return _season_from_row(row)
    if media_type == "episode":
        _typ, _source, _media_id, season, episode = media_parts_from_item_id(row.get("item_id"))
        return item_from_row(row, force_type="episode") if season is not None and episode is not None else None
    return item_from_row(row, force_type=media_type)


def _write_season_score(adapter: Any, tmdb_id: str, season: int, rating: float | None) -> None:
    try:
        api_patch(adapter, f"media/tv/tmdb/{tmdb_id}/{season}", json={"score": rating})
    except FloppyAuthError as exc:
        if getattr(exc, "status_code", None) != 404:
            raise
        if rating is None:
            return
        api_post(adapter, "media/season", json={"source": "tmdb", "media_id": str(tmdb_id), "season_number": int(season), "status": PLANNING, "score": rating})


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for media_type, params in (("movie", None), ("tv", None), ("season", {"rating": "rated"}), ("episode", {"rating": "rated"})):
        try:
            rows = paged(adapter, f"media/{media_type}", params=params)
        except FloppyAuthError as exc:
            if media_type in {"movie", "tv"} or getattr(exc, "status_code", None) not in {400, 404}:
                raise
            rows = []
        for row in rows:
            rating = rating_number(row.get("score"))
            if rating is None or rating <= 0:
                continue
            item = _rated_item(row, media_type)
            if not item:
                continue
            item["rating"] = rating
            rated_at = _rated_at(row.get("scored_at"))
            if rated_at:
                item["rated_at"] = rated_at
            item["_floppy_consumption_id"] = row.get("consumption_id")
            out[canonical_item_key(item)] = item
    return _merge_shadow(adapter, out)


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, items, clear=False, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, items, clear=True, dry_run=dry_run)


def _write(adapter: Any, items: Iterable[Mapping[str, Any]], *, clear: bool, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    confirmed_destinations: dict[str, dict[str, Any]] = {}
    skipped: list[str] = []
    unresolved_rows: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    for raw in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        key = canonical_item_key(raw)
        raw_type = str(raw.get("type") or "").strip().lower()
        typ = "season" if raw_type == "season" else floppy_type_for_item(raw)
        nested = typ in {"season", "episode"}
        item = tmdb_enriched_item(adapter, raw, episode_show=nested)
        tmdb_id = tmdb_id_for_item(item, episode_show=nested)
        rating = None if clear else rating_number(item.get("rating"))
        season = int_or_none(item.get("season")) if nested else None
        episode = int_or_none(item.get("episode")) if typ == "episode" else None
        if typ not in {"movie", "tv", "season", "episode"}:
            skipped.append(key)
            results.append({"status": "skipped", "reason": "floppy_rating_type_unsupported", "item": id_minimal(item), "canonical_key": key})
            continue
        if not tmdb_id:
            entry = unresolved(item, "floppy_tmdb_id_missing")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        if nested and (season is None or season < 0 or (typ == "episode" and (episode is None or episode <= 0))):
            entry = unresolved(item, "floppy_episode_id_missing")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        if rating is None and not clear:
            entry = unresolved(item, "floppy_rating_missing")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        if rating is not None and rating <= 0 and not clear:
            skipped.append(key)
            results.append({"status": "skipped", "reason": "floppy_rating_zero_is_clear", "item": id_minimal(item), "canonical_key": key})
            continue
        if dry_run:
            confirmed.append(key)
            confirmed_destinations[key] = confirmed_destination(item)
            results.append({"status": "dry_run", "item": id_minimal(item), "canonical_key": key})
            continue
        try:
            if typ == "episode" and season is not None and episode is not None:
                target_id, target_season, target_episode = _write_target(adapter, str(tmdb_id), item, season, episode)
                api_patch(adapter, f"media/tv/tmdb/{target_id}/{target_season}/episodes/{target_episode}/score", json={"score": rating})
            elif typ == "season" and season is not None:
                _write_season_score(adapter, str(tmdb_id), season, rating)
            elif clear:
                api_patch(adapter, f"media/{typ}/tmdb/{tmdb_id}", json={"score": None})
            else:
                track_media(adapter, typ, tmdb_id, payload={"status": PLANNING, "score": rating}, patch_payload={"score": rating})
        except Exception as exc:
            entry = unresolved(item, failure_reason(exc))
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        confirmed.append(key)
        confirmed_destinations[key] = confirmed_destination(item)
        if clear:
            _remember(adapter, key, None)
        else:
            cached = id_minimal(item)
            cached["rating"] = rating
            if item.get("rated_at"):
                cached["rated_at"] = item.get("rated_at")
            _remember(adapter, key, cached)
        results.append({"status": "applied", "item": id_minimal(item), "canonical_key": key})
    return build_op_result(ok=not unresolved_rows, count=len(confirmed), confirmed_keys=confirmed, confirmed_destinations=confirmed_destinations, unresolved_keys=unresolved_keys(unresolved_rows, canonical_item_key), unresolved=unresolved_rows, results=results, attempted=len(confirmed) + len(skipped) + len(unresolved_rows), skipped=len(skipped), skipped_keys=skipped)
