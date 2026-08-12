# /providers/sync/scrob/_ratings.py
# Scrob ratings sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import math
from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal

from ._common import (
    MEDIA_TYPE_EPISODE,
    MEDIA_TYPE_MOVIE,
    MEDIA_TYPE_SERIES,
    PATH_RATINGS,
    as_int,
    error_of,
    ids_for_scrob,
    iso_z,
    mapping,
    not_future,
    ok_status,
    positive_int,
    safe_json,
    scrob_request,
    show_ids_for_scrob,
    year_of,
    _dbg,
    _info,
    _warn,
)

FEATURE = "ratings"

EPISODE_DETAIL_PATH = "media/episode/{tmdb_id}"
EPISODE_LOOKUP_CAP = 500


def _key_of(obj: Mapping[str, Any]) -> str:
    try:
        return str(canonical_key(id_minimal(obj)) or "").strip()
    except Exception:
        return ""


def _rating_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        num = float(value)
    except Exception:
        return None
    if num <= 0:
        return None
    return max(1, min(10, int(math.floor(num + 0.5))))


def _scope_of(item: Mapping[str, Any]) -> str:
    typ = str(item.get("type") or "").strip().lower()
    if typ == "episode" or (item.get("season") is not None and item.get("episode") is not None):
        return "episode"
    if typ == "season" or (typ in ("show", "series", "tv") and item.get("season") is not None):
        return "season"
    if typ in ("show", "series", "tv"):
        return "show"
    return "movie"


def _episode_context(adapter: Any, tmdb_id: int, cache: dict[int, dict[str, Any]]) -> dict[str, Any]:
    if tmdb_id in cache:
        return cache[tmdb_id]
    if len(cache) >= EPISODE_LOOKUP_CAP:
        cache[tmdb_id] = {}
        return {}
    resp = scrob_request(adapter, "GET", EPISODE_DETAIL_PATH.format(tmdb_id=tmdb_id))
    cache[tmdb_id] = mapping(safe_json(resp)) if ok_status(resp) else {}
    return cache[tmdb_id]


def _row_to_minimal(adapter: Any, row: Mapping[str, Any], cache: dict[int, dict[str, Any]]) -> dict[str, Any] | None:
    rating = _rating_int(row.get("rating"))
    if rating is None:
        return None
    media = mapping(row.get("media"))
    tmdb = positive_int(media.get("tmdb_id"))
    if not tmdb:
        return None

    media_type = str(media.get("type") or "").strip().lower()
    season = as_int(row.get("season_number"))
    out: dict[str, Any]

    if media_type == MEDIA_TYPE_EPISODE:
        detail = _episode_context(adapter, tmdb, cache)
        show_tmdb = positive_int(detail.get("show_tmdb_id"))
        ep_season = as_int(detail.get("season_number"))
        ep_number = as_int(detail.get("episode_number"))
        if not show_tmdb or ep_season is None or ep_number is None:
            _dbg(FEATURE, "episode_context_unresolved", tmdb=tmdb)
            return None
        out = {
            "type": "episode",
            "ids": {"tmdb": str(tmdb)},
            "show_ids": {"tmdb": str(show_tmdb)},
            "season": ep_season,
            "episode": ep_number,
        }
        series_title = str(detail.get("show_title") or "").strip()
        if series_title:
            out["series_title"] = series_title
    elif media_type == MEDIA_TYPE_SERIES and season is not None:
        out = {"type": "season", "ids": {"tmdb": str(tmdb)}, "show_ids": {"tmdb": str(tmdb)}, "season": season}
    elif media_type == MEDIA_TYPE_SERIES:
        out = {"type": "show", "ids": {"tmdb": str(tmdb)}}
    else:
        out = {"type": "movie", "ids": {"tmdb": str(tmdb)}}
        year = year_of(media)
        if year:
            out["year"] = year

    title = str(media.get("title") or "").strip()
    if title:
        out["title"] = title
    out["rating"] = rating
    rated = iso_z(row.get("rated_at"))
    if rated:
        out["rated_at"] = rated
    return out


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    resp = scrob_request(adapter, "GET", PATH_RATINGS)
    if not ok_status(resp):
        _warn(FEATURE, "fetch_failed", status=int(resp.status_code), error=error_of(resp))
        return {}
    data = safe_json(resp)
    rows = data.get("results") if isinstance(data, Mapping) else None
    rows = [r for r in rows if isinstance(r, Mapping)] if isinstance(rows, list) else []

    cache: dict[int, dict[str, Any]] = {}
    out: dict[str, dict[str, Any]] = {}
    skipped = 0
    for row in rows:
        minimal = _row_to_minimal(adapter, row, cache)
        if not minimal:
            skipped += 1
            continue
        key = _key_of(minimal)
        if not key:
            skipped += 1
            continue
        out[key] = minimal

    _info(FEATURE, "index_done", count=len(out), rows=len(rows), skipped=skipped)
    return out


def _payload_for(item: Mapping[str, Any]) -> dict[str, Any] | None:
    scope = _scope_of(item)
    payload: dict[str, Any] = {}

    if scope == "episode":
        tmdb = positive_int(ids_for_scrob(item).get("tmdb_id"))
        if not tmdb:
            return None
        payload["tmdb_id"] = tmdb
        payload["media_type"] = MEDIA_TYPE_EPISODE
    elif scope == "season":
        show_tmdb = positive_int(show_ids_for_scrob(item).get("tmdb_id")) or positive_int(ids_for_scrob(item).get("tmdb_id"))
        season = as_int(item.get("season"))
        if not show_tmdb or season is None:
            return None
        payload["tmdb_id"] = show_tmdb
        payload["media_type"] = MEDIA_TYPE_SERIES
        payload["season_number"] = season
    elif scope == "show":
        tmdb = positive_int(ids_for_scrob(item).get("tmdb_id")) or positive_int(show_ids_for_scrob(item).get("tmdb_id"))
        if not tmdb:
            return None
        payload["tmdb_id"] = tmdb
        payload["media_type"] = MEDIA_TYPE_SERIES
    else:
        tmdb = positive_int(ids_for_scrob(item).get("tmdb_id"))
        if not tmdb:
            return None
        payload["tmdb_id"] = tmdb
        payload["media_type"] = MEDIA_TYPE_MOVIE

    return payload


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    planned = 0
    ok = True

    for item in items or []:
        key = _key_of(item)
        if not key:
            continue
        payload = _payload_for(item)
        rating = _rating_int(item.get("rating"))
        if payload is None or rating is None:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "missing_supported_id_or_rating"})
            _dbg(FEATURE, "item_unresolved_before_write", key=key)
            continue

        payload["rating"] = float(rating)
        review = str(item.get("review") or "").strip()
        if review:
            payload["review"] = review[:2000]

        planned += 1
        if dry_run:
            confirmed.append(key)
            continue

        resp = scrob_request(adapter, "POST", PATH_RATINGS, json=payload)
        if ok_status(resp):
            confirmed.append(key)
            continue
        ok = False
        unresolved_keys.append(key)
        unresolved.append({"key": key, "status": f"http:{int(resp.status_code)}", "error": error_of(resp)})
        _warn(FEATURE, "rating_add_failed", key=key, status=int(resp.status_code), error=error_of(resp))

    _dbg(FEATURE, "write_prepare", action="add", items=planned, skipped=len(unresolved_keys))
    _info(FEATURE, "write_done", action="add", confirmed=len(confirmed), unresolved=len(unresolved_keys), dry_run=bool(dry_run))
    return {
        "ok": ok,
        "count": len(confirmed),
        "confirmed_keys": confirmed,
        "unresolved_keys": unresolved_keys,
        "deferred_keys": [],
        "unresolved": unresolved,
    }


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    ok = True

    for item in items or []:
        key = _key_of(item)
        if not key:
            continue
        payload = _payload_for(item)
        if payload is None:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "missing_supported_id"})
            continue
        if dry_run:
            confirmed.append(key)
            continue

        params: dict[str, Any] = {"tmdb_id": payload["tmdb_id"], "media_type": payload["media_type"]}
        if payload.get("season_number") is not None:
            params["season_number"] = payload["season_number"]

        resp = scrob_request(adapter, "DELETE", PATH_RATINGS, params=params)
        code = int(resp.status_code)
        if ok_status(resp) or code == 404:
            confirmed.append(key)
            continue
        ok = False
        unresolved_keys.append(key)
        unresolved.append({"key": key, "status": f"http:{code}", "error": error_of(resp)})
        _warn(FEATURE, "rating_delete_failed", key=key, status=code, error=error_of(resp))

    _info(FEATURE, "write_done", action="remove", confirmed=len(confirmed), unresolved=len(unresolved_keys), dry_run=bool(dry_run))
    return {
        "ok": ok,
        "count": len(confirmed),
        "confirmed_keys": confirmed,
        "unresolved_keys": unresolved_keys,
        "deferred_keys": [],
        "unresolved": unresolved,
    }
