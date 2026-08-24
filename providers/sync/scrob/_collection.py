# /providers/sync/scrob/_collection.py
# Scrob collection sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import io
import json
import zipfile
from pathlib import PurePosixPath
from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, ids_from, minimal as id_minimal

from ._common import (
    MEDIA_TYPE_EPISODE,
    MEDIA_TYPE_MOVIE,
    MEDIA_TYPE_SERIES,
    as_int,
    error_of,
    ids_for_scrob,
    iso_z,
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

FEATURE = "collection"

PATH_EXPORT = "export/data"
PATH_COLLECT = "media/collect"
PATH_COLLECT_SHOW = "media/collect-show"
PATH_COLLECT_SEASON = "media/collect-season"

COLLECTION_ID_FIELD = "_scrob_collection_id"


def _key_of(obj: Mapping[str, Any]) -> str:
    try:
        return str(canonical_key(id_minimal(obj)) or "").strip()
    except Exception:
        return ""


def _ids_clean(*sources: Mapping[str, Any] | None) -> dict[str, str]:
    merged: dict[str, Any] = {}
    for source in sources:
        if isinstance(source, Mapping):
            merged.update(source)
    ids = ids_from({"ids": merged})
    out = {key: str(value) for key, value in ids.items() if str(value or "").strip()}
    for src_key, dst_key in (("tmdb_id", "tmdb"), ("imdb_id", "imdb"), ("tvdb_id", "tvdb")):
        if dst_key in out:
            continue
        value = merged.get(src_key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            out[dst_key] = text
    return out


def _nested(row: Mapping[str, Any], *names: str) -> Mapping[str, Any]:
    for name in names:
        value = row.get(name)
        if isinstance(value, Mapping):
            return value
    return {}


def _item_with_meta(item: dict[str, Any], *rows: Mapping[str, Any]) -> dict[str, Any]:
    for row in rows:
        collected = iso_z(row.get("collected_at") or row.get("listed_at") or row.get("added_at"))
        if collected:
            item["collected_at"] = collected
            break
    for row in rows:
        cid = positive_int(row.get("id") or row.get("collection_id"))
        if cid:
            item[COLLECTION_ID_FIELD] = str(cid)
            break
    return id_minimal(item) | {k: v for k, v in item.items() if k.startswith("_scrob_") or k == "collected_at"}


def _movie_from_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    movie = _nested(row, "movie", "media") or row
    ids = _ids_clean(movie.get("ids") if isinstance(movie.get("ids"), Mapping) else {}, movie)
    if not ids:
        return None
    item: dict[str, Any] = {"type": "movie", "ids": ids}
    title = str(movie.get("title") or row.get("title") or "").strip()
    if title:
        item["title"] = title
    year = as_int(movie.get("year") or row.get("year")) or year_of(movie)
    if year:
        item["year"] = year
    return _item_with_meta(item, row, movie)


def _show_from_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    show = _nested(row, "show", "series", "media") or row
    ids = _ids_clean(show.get("ids") if isinstance(show.get("ids"), Mapping) else {}, show)
    if not ids:
        return None
    item: dict[str, Any] = {"type": "show", "ids": ids}
    title = str(show.get("title") or show.get("name") or row.get("title") or "").strip()
    if title:
        item["title"] = title
    year = as_int(show.get("year") or row.get("year")) or year_of(show)
    if year:
        item["year"] = year
    return _item_with_meta(item, row, show)


def _items_from_row(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_type = str(row.get("type") or row.get("media_type") or _nested(row, "media").get("type") or "").strip().lower()
    if raw_type == MEDIA_TYPE_MOVIE or isinstance(row.get("movie"), Mapping):
        movie = _movie_from_row(row)
        return [movie] if movie else []

    show_item = _show_from_row(row)
    show = _nested(row, "show", "series", "media") or row
    show_ids = dict((show_item or {}).get("ids") or {})
    title = (show_item or {}).get("title")
    year = (show_item or {}).get("year")

    season_number = as_int(row.get("season") if row.get("season") is not None else row.get("season_number"))
    episode_number = as_int(row.get("episode") if row.get("episode") is not None else row.get("episode_number"))
    if raw_type == MEDIA_TYPE_EPISODE or episode_number is not None:
        if not show_ids or season_number is None or episode_number is None:
            return []
        ep_ids = _ids_clean(row.get("ids") if isinstance(row.get("ids"), Mapping) else {}, row)
        item = {
            "type": "episode",
            "ids": ep_ids,
            "show_ids": show_ids,
            "season": season_number,
            "episode": episode_number,
            "series_title": title,
            "year": year,
        }
        ep_title = str(row.get("title") or "").strip()
        if ep_title:
            item["title"] = ep_title
        return [_item_with_meta(item, row)]

    seasons = row.get("seasons")
    if not isinstance(seasons, list):
        seasons = show.get("seasons") if isinstance(show, Mapping) else None
    if not isinstance(seasons, list) or not seasons:
        if raw_type in {"series", "show", "tv"} or isinstance(row.get("show"), Mapping):
            return [show_item] if show_item else []
        return []

    out: list[dict[str, Any]] = []
    for season in seasons:
        if not isinstance(season, Mapping):
            continue
        season_no = as_int(season.get("number") if season.get("number") is not None else season.get("season_number"))
        episodes = season.get("episodes")
        if isinstance(episodes, list) and episodes:
            for ep in episodes:
                if not isinstance(ep, Mapping):
                    continue
                ep_no = as_int(ep.get("number") if ep.get("number") is not None else ep.get("episode_number"))
                if season_no is None or ep_no is None:
                    continue
                item = {
                    "type": "episode",
                    "ids": _ids_clean(ep.get("ids") if isinstance(ep.get("ids"), Mapping) else {}, ep),
                    "show_ids": show_ids,
                    "season": season_no,
                    "episode": ep_no,
                    "series_title": title,
                    "year": year,
                }
                ep_title = str(ep.get("title") or "").strip()
                if ep_title:
                    item["title"] = ep_title
                out.append(_item_with_meta(item, row, season, ep))
        elif season_no is not None and show_ids:
            item = {"type": "season", "ids": dict(show_ids), "show_ids": show_ids, "season": season_no, "series_title": title, "title": title, "year": year}
            out.append(_item_with_meta(item, row, season))
    return out or ([show_item] if show_item else [])


def _json_rows(data: Any) -> list[Mapping[str, Any]]:
    if isinstance(data, list):
        return [row for row in data if isinstance(row, Mapping)]
    if not isinstance(data, Mapping):
        return []
    for key in ("collection", "items", "results", "data"):
        value = data.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, Mapping)]
    rows: list[Mapping[str, Any]] = []
    for key in ("movies", "shows", "seasons", "episodes"):
        value = data.get(key)
        if isinstance(value, list):
            rows.extend(row for row in value if isinstance(row, Mapping))
    return rows


def _loads_json(data: bytes) -> Any:
    try:
        return json.loads(data.decode("utf-8", "replace"))
    except Exception:
        return None


def _export_payload(resp: Any) -> list[Mapping[str, Any]]:
    content = getattr(resp, "content", b"") or b""
    if isinstance(content, str):
        content = content.encode("utf-8")
    if not content:
        text = getattr(resp, "text", "") or ""
        content = text.encode("utf-8") if text else b""

    rows: list[Mapping[str, Any]] = []
    if content and zipfile.is_zipfile(io.BytesIO(content)):
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            for info in archive.infolist():
                if info.is_dir():
                    continue
                base = PurePosixPath(info.filename).name.lower()
                if not base.startswith("collection-") and "collection" not in PurePosixPath(info.filename).parts:
                    continue
                if not base.endswith((".json", ".txt")):
                    continue
                loaded = _loads_json(archive.read(info))
                rows.extend(_json_rows(loaded))
        return rows

    loaded = safe_json(resp)
    if loaded is None and content:
        loaded = _loads_json(content)
    return _json_rows(loaded)


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    resp = scrob_request(
        adapter,
        "GET",
        PATH_EXPORT,
        params={
            "watched": False,
            "ratings": False,
            "collection": True,
            "lists": False,
            "comments": False,
            "api_keys": False,
            "media_connections": False,
            "scrobble_connections": False,
            "connections": False,
        },
    )
    if not ok_status(resp):
        _warn(FEATURE, "fetch_failed", status=int(resp.status_code), error=error_of(resp))
        return {}

    rows = _export_payload(resp)
    out: dict[str, dict[str, Any]] = {}
    skipped = 0
    for row in rows:
        produced = _items_from_row(row)
        if not produced:
            skipped += 1
            continue
        for item in produced:
            key = _key_of(item)
            if key:
                out[key] = item
            else:
                skipped += 1
    _info(FEATURE, "index_done", count=len(out), rows=len(rows), skipped=skipped)
    return out


def _scope(item: Mapping[str, Any]) -> str:
    typ = str(item.get("type") or "").strip().lower()
    if typ == "episode" or (item.get("season") is not None and item.get("episode") is not None):
        return "episode"
    if typ == "season":
        return "season"
    if typ in ("show", "series", "tv"):
        return "show"
    return "movie"


def _payload_for(item: Mapping[str, Any]) -> tuple[str, dict[str, Any]] | None:
    scope = _scope(item)
    if scope == "show":
        tmdb = positive_int(ids_for_scrob(item).get("tmdb_id")) or positive_int(show_ids_for_scrob(item).get("tmdb_id"))
        return (PATH_COLLECT_SHOW, {"tmdb_id": tmdb, "media_type": MEDIA_TYPE_SERIES}) if tmdb else None
    if scope == "season":
        tmdb = positive_int(show_ids_for_scrob(item).get("tmdb_id")) or positive_int(ids_for_scrob(item).get("tmdb_id"))
        season = as_int(item.get("season"))
        return (PATH_COLLECT_SEASON, {"series_tmdb_id": tmdb, "season_number": season}) if tmdb and season is not None else None
    if scope == "episode":
        tmdb = positive_int(ids_for_scrob(item).get("tmdb_id"))
        show_tmdb = positive_int(show_ids_for_scrob(item).get("tmdb_id"))
        season = as_int(item.get("season"))
        episode = as_int(item.get("episode"))
        if not tmdb or not show_tmdb or season is None or episode is None:
            return None
        return (PATH_COLLECT, {"tmdb_id": tmdb, "media_type": MEDIA_TYPE_EPISODE, "series_tmdb_id": show_tmdb, "season_number": season, "episode_number": episode})
    tmdb = positive_int(ids_for_scrob(item).get("tmdb_id"))
    return (PATH_COLLECT, {"tmdb_id": tmdb, "media_type": MEDIA_TYPE_MOVIE}) if tmdb else None


def _remove_params(item: Mapping[str, Any]) -> tuple[str, dict[str, Any]] | None:
    scope = _scope(item)
    if scope == "show":
        tmdb = positive_int(ids_for_scrob(item).get("tmdb_id")) or positive_int(show_ids_for_scrob(item).get("tmdb_id"))
        return (PATH_COLLECT_SHOW, {"tmdb_id": tmdb}) if tmdb else None
    if scope == "season":
        tmdb = positive_int(show_ids_for_scrob(item).get("tmdb_id")) or positive_int(ids_for_scrob(item).get("tmdb_id"))
        season = as_int(item.get("season"))
        return (PATH_COLLECT_SEASON, {"series_tmdb_id": tmdb, "season_number": season}) if tmdb and season is not None else None
    media_type = MEDIA_TYPE_EPISODE if scope == "episode" else MEDIA_TYPE_MOVIE
    params: dict[str, Any] = {"media_type": media_type}
    cid = positive_int(item.get(COLLECTION_ID_FIELD) or item.get("collection_id"))
    if cid:
        params["id"] = cid
        return PATH_COLLECT, params
    tmdb = positive_int(ids_for_scrob(item).get("tmdb_id"))
    if tmdb:
        params["tmdb_id"] = tmdb
        return PATH_COLLECT, params
    return None


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
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
            unresolved.append({"key": key, "status": "missing_supported_id_or_type"})
            _dbg(FEATURE, "item_unresolved_before_write", key=key)
            continue
        path, body = payload
        if dry_run:
            confirmed.append(key)
            continue
        resp = scrob_request(adapter, "POST", path, json=body)
        if ok_status(resp) or int(resp.status_code) == 409:
            confirmed.append(key)
            continue
        ok = False
        unresolved_keys.append(key)
        unresolved.append({"key": key, "status": f"http:{int(resp.status_code)}", "error": error_of(resp)})
        _warn(FEATURE, "collection_add_failed", key=key, status=int(resp.status_code), error=error_of(resp))
    _info(FEATURE, "write_done", action="add", confirmed=len(confirmed), unresolved=len(unresolved_keys), dry_run=bool(dry_run))
    return {"ok": ok, "count": len(confirmed), "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "deferred_keys": [], "unresolved": unresolved}


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    ok = True
    for item in items or []:
        key = _key_of(item)
        if not key:
            continue
        payload = _remove_params(item)
        if payload is None:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "missing_supported_id_or_type"})
            continue
        path, params = payload
        if dry_run:
            confirmed.append(key)
            continue
        resp = scrob_request(adapter, "DELETE", path, params=params)
        if ok_status(resp) or int(resp.status_code) == 404:
            confirmed.append(key)
            continue
        ok = False
        unresolved_keys.append(key)
        unresolved.append({"key": key, "status": f"http:{int(resp.status_code)}", "error": error_of(resp)})
        _warn(FEATURE, "collection_remove_failed", key=key, status=int(resp.status_code), error=error_of(resp))
    _info(FEATURE, "write_done", action="remove", confirmed=len(confirmed), unresolved=len(unresolved_keys), dry_run=bool(dry_run))
    return {"ok": ok, "count": len(confirmed), "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "deferred_keys": [], "unresolved": unresolved}
