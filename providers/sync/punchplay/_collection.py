# /providers/sync/punchplay/_collection.py
# PunchPlay collection sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal

from ._common import (
    URL_COLLECTION,
    URL_COLLECTION_ITEM,
    URL_COLLECTION_WRITE,
    cfg_section,
    error_of,
    ids_for_punchplay,
    punchplay_request,
    request_id_of,
    safe_json,
    _as_int,
    _dbg,
    _info,
    _warn,
)

FEATURE = "collection"
VALID_FORMATS = {"4k_bluray", "bluray", "hd_dvd", "dvd", "digital"}


def _key_of(obj: Mapping[str, Any]) -> str:
    try:
        return str(canonical_key(id_minimal(obj)) or "").strip()
    except Exception:
        return ""


def _pick(row: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in row and row.get(name) is not None:
            return row.get(name)
    return None


def _nested(row: Mapping[str, Any], *names: str) -> Mapping[str, Any]:
    for name in names:
        value = row.get(name)
        if isinstance(value, Mapping):
            return value
    return {}


def _kind_of(item: Mapping[str, Any]) -> str:
    raw = str(item.get("type") or item.get("kind") or "").strip().lower()
    if raw in ("movie", "movies", "film"):
        return "movie"
    return "show"


def _format_value(adapter: Any, item: Mapping[str, Any]) -> str:
    raw = str(item.get("format") or item.get("edition_format") or "").strip().lower()
    if raw in VALID_FORMATS:
        return raw
    cfg = cfg_section(adapter)
    raw_cfg = str(cfg.get("collection_default_format") or "digital").strip().lower()
    return raw_cfg if raw_cfg in VALID_FORMATS else "digital"


def _row_ids(row: Mapping[str, Any], title: Mapping[str, Any]) -> dict[str, str]:
    ids: dict[str, str] = {}
    source_id = _pick(row, "sourceId", "source_id", "tmdbId", "tmdb_id", "titleTmdbId", "title_tmdb_id") or _pick(title, "sourceId", "source_id", "tmdbId", "tmdb_id")
    tmdb = _as_int(source_id)
    if tmdb:
        ids["tmdb"] = str(tmdb)
    imdb = _pick(row, "imdbId", "imdb_id") or _pick(title, "imdbId", "imdb_id")
    if imdb:
        text = str(imdb).strip()
        if text:
            ids["imdb"] = text if text.startswith("tt") else f"tt{text.lstrip('t')}"
    tvdb = _as_int(_pick(row, "tvdbId", "tvdb_id") or _pick(title, "tvdbId", "tvdb_id"))
    if tvdb:
        ids["tvdb"] = str(tvdb)
    mal = _as_int(_pick(row, "malId", "mal_id") or _pick(title, "malId", "mal_id"))
    if mal:
        ids["mal"] = str(mal)
    return ids


def _to_minimal(row: Mapping[str, Any]) -> dict[str, Any] | None:
    title_obj = _nested(row, "title", "item")
    ids = _row_ids(row, title_obj)
    if not ids:
        return None

    raw_kind = str(_pick(row, "kind", "type", "mediaType", "media_type") or _pick(title_obj, "kind", "type", "mediaType", "media_type") or "").strip().lower()
    season = _as_int(_pick(row, "season", "seasonNumber", "season_number"))
    typ = "movie" if raw_kind in ("movie", "movies", "film") else ("season" if season is not None else "show")

    if typ == "season":
        out: dict[str, Any] = {"type": "season", "ids": dict(ids), "show_ids": dict(ids), "season": season}
    else:
        out = {"type": typ, "ids": dict(ids)}

    title = _pick(row, "title", "name")
    if isinstance(title, Mapping):
        title = _pick(title, "title", "name")
    if not title:
        title = _pick(title_obj, "title", "name")
    if title:
        out["title"] = str(title).strip()

    year = _pick(row, "year") or _pick(title_obj, "year")
    try:
        if year is not None:
            out["year"] = int(year)
    except Exception:
        pass

    entry_id = _pick(row, "id", "collectionId", "collection_id")
    if entry_id is not None:
        out["_punchplay_collection_id"] = str(entry_id)
    fmt = str(_pick(row, "format") or "").strip().lower()
    if fmt:
        out["format"] = fmt
    notes = str(_pick(row, "notes") or "").strip()
    if notes:
        out["notes"] = notes
    added = _pick(row, "addedAt", "added_at", "createdAt", "created_at", "collectedAt", "collected_at")
    if added:
        out["collected_at"] = str(added)
    return id_minimal(out) | {k: v for k, v in out.items() if str(k).startswith("_punchplay_") or k in {"format", "notes", "collected_at"}}


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    collected: dict[str, dict[str, Any]] = {}
    cursor: str | None = None
    guard = 0
    while True:
        guard += 1
        if guard > 2000:
            _warn(FEATURE, "collection_page_guard_tripped")
            break
        params: dict[str, Any] = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        resp = punchplay_request(adapter, "GET", URL_COLLECTION, params=params)
        if resp.status_code != 200:
            _warn(FEATURE, "collection_fetch_failed", status=resp.status_code, error=error_of(resp), request_id=request_id_of(resp))
            break
        data = safe_json(resp) or {}
        if not isinstance(data, Mapping):
            break
        rows = data.get("items")
        rows = [row for row in rows if isinstance(row, Mapping)] if isinstance(rows, list) else []
        for row in rows:
            item = _to_minimal(row)
            if not item:
                continue
            key = _key_of(item)
            if key:
                collected[key] = item
        cursor = str(data.get("nextCursor") or "").strip() or None
        if not cursor:
            break
    _info(FEATURE, "index_done", count=len(collected))
    return collected


def _payload_for(adapter: Any, item: Mapping[str, Any]) -> tuple[dict[str, Any], str] | None:
    m = id_minimal(item)
    key = _key_of(m)
    if not key:
        return None
    typ = str(m.get("type") or "").strip().lower()
    if typ == "episode":
        return None
    source: Mapping[str, Any] = m
    if typ == "season" and isinstance(m.get("show_ids"), Mapping):
        source = {"ids": m.get("show_ids")}
    ids = ids_for_punchplay(source)
    source_id = ids.get("tmdb_id")
    if not source_id:
        return None
    payload: dict[str, Any] = {
        "kind": _kind_of(m),
        "sourceId": int(source_id),
        "format": _format_value(adapter, item),
    }
    if typ == "season":
        season = _as_int(m.get("season") if m.get("season") is not None else m.get("season_number"))
        if season is None:
            return None
        payload["season"] = season
    title = str((m.get("series_title") or m.get("title") or "") if typ == "season" else (m.get("title") or "")).strip()
    if title:
        payload["title"] = title[:500]
    year = _as_int(m.get("year"))
    if year:
        payload["year"] = year
    notes = str(item.get("notes") or "").strip()
    if notes:
        payload["notes"] = notes[:1000]
    return payload, key


def _collection_id_for_remove(adapter: Any, item: Mapping[str, Any]) -> str | None:
    direct = str(item.get("_punchplay_collection_id") or item.get("collection_id") or "").strip()
    if direct:
        return direct
    key = _key_of(item)
    if not key:
        return None
    try:
        idx = build_index(adapter)
    except Exception:
        idx = {}
    match = idx.get(key)
    if isinstance(match, Mapping):
        found = str(match.get("_punchplay_collection_id") or "").strip()
        if found:
            return found
    return None


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    for item in items or []:
        built = _payload_for(adapter, item)
        if built is None:
            key = _key_of(item)
            if key:
                unresolved_keys.append(key)
            unresolved.append({"item": id_minimal(item), "status": "missing_tmdb_id_or_unsupported_type"})
            continue
        payload, key = built
        resp = punchplay_request(adapter, "POST", URL_COLLECTION_WRITE, json=payload)
        if 200 <= int(resp.status_code) < 300:
            confirmed.append(key)
        else:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": f"http:{resp.status_code}", "error": error_of(resp), "request_id": request_id_of(resp)})
    _info(FEATURE, "write_done", action="add", confirmed=len(confirmed), unresolved=len(unresolved_keys))
    return {"ok": not unresolved_keys, "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "unresolved": unresolved}


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    for item in items or []:
        key = _key_of(item)
        entry_id = _collection_id_for_remove(adapter, item)
        if not key or not entry_id:
            if key:
                unresolved_keys.append(key)
            unresolved.append({"item": id_minimal(item), "status": "missing_collection_id"})
            continue
        resp = punchplay_request(adapter, "DELETE", URL_COLLECTION_ITEM.format(entry_id=entry_id))
        if 200 <= int(resp.status_code) < 300 or int(resp.status_code) == 404:
            confirmed.append(key)
        else:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": f"http:{resp.status_code}", "error": error_of(resp), "request_id": request_id_of(resp)})
    _info(FEATURE, "write_done", action="remove", confirmed=len(confirmed), unresolved=len(unresolved_keys))
    return {"ok": not unresolved_keys, "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "unresolved": unresolved}
