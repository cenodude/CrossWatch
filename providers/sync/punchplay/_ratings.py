# /providers/sync/punchplay/_ratings.py
# PunchPlay ratings sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal

from ._common import (
    URL_RATINGS,
    bulk_write,
    cfg_section,
    client_item_id,
    error_of,
    has_write_id,
    ids_for_punchplay,
    iso_z,
    needs_client_item_id,
    punchplay_request,
    request_id_of,
    safe_json,
    snapshot_pages,
    _dbg,
    _info,
    _warn,
)

FEATURE = "ratings"

SHOW_STATUSES = ("WATCHED", "WATCHING", "PLANNING", "ON_HOLD", "DROPPED")
WRITE_SCOPES = ("title", "series", "season", "episode")


def _key_of(obj: Mapping[str, Any]) -> str:
    try:
        return str(canonical_key(id_minimal(obj)) or "").strip()
    except Exception:
        return ""


def _pick(row: Mapping[str, Any], *names: str) -> Any:
    for n in names:
        if n in row and row.get(n) is not None:
            return row.get(n)
    return None


def _kind_of(item: Mapping[str, Any]) -> str:
    raw = str(item.get("type") or item.get("kind") or "").strip().lower()
    if raw in ("movie", "movies", "film"):
        return "movie"
    return "show"


def _scope_of(item: Mapping[str, Any]) -> str:
    raw = str(item.get("scope") or "").strip().lower()
    if raw in WRITE_SCOPES:
        return raw
    typ = str(item.get("type") or "").strip().lower()
    if typ == "episode" or (item.get("season") is not None and item.get("episode") is not None):
        return "episode"
    if typ == "season" or (item.get("season") is not None and item.get("episode") is None):
        return "season"
    return "title"


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _rating_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        num = float(value)
    except Exception:
        return None
    if num <= 0:
        return None
    return max(1, min(10, int(round(num))))


def _to_minimal(row: Mapping[str, Any]) -> dict[str, Any] | None:
    tmdb = _pick(row, "tmdbId", "tmdb_id")
    try:
        tmdb_i = int(tmdb) if tmdb is not None else None
    except Exception:
        tmdb_i = None
    if not tmdb_i or tmdb_i <= 0:
        return None

    kind = str(_pick(row, "kind", "type") or "").strip().lower()
    scope = str(_pick(row, "scope") or "").strip().lower() or "title"
    season = _pick(row, "season")
    episode = _pick(row, "episode")

    if scope == "episode" and season is not None and episode is not None:
        typ = "episode"
    elif scope == "season" and season is not None:
        typ = "season"
    elif kind in ("movie", "movies", "film"):
        typ = "movie"
    else:
        typ = "show"

    out: dict[str, Any] = {"type": typ, "ids": {"tmdb": str(tmdb_i)}}
    title = _pick(row, "title")
    if title:
        out["title"] = str(title).strip()
    year = _pick(row, "year")
    try:
        if year is not None:
            out["year"] = int(year)
    except Exception:
        pass
    if season is not None:
        try:
            out["season"] = int(season)
        except Exception:
            pass
    if episode is not None:
        try:
            out["episode"] = int(episode)
        except Exception:
            pass

    rating = _rating_int(_pick(row, "rating"))
    if rating is not None:
        out["rating"] = rating
    rated_at = iso_z(_pick(row, "ratedAt", "rated_at", "updatedAt", "updated_at"))
    if rated_at:
        out["rated_at"] = rated_at

    fav = _pick(row, "isFavourite", "is_favourite")
    if fav is True:
        out["is_favourite"] = True
    status = _pick(row, "showStatus", "show_status")
    if status and str(status).strip().upper() in SHOW_STATUSES:
        out["show_status"] = str(status).strip().upper()

    return out


def _index_from_snapshot(adapter: Any) -> dict[str, dict[str, Any]]:
    collected: dict[str, dict[str, Any]] = {}
    seen_rows = 0
    for page in snapshot_pages(adapter, "interaction", feature=FEATURE):
        for row in page:
            seen_rows += 1
            minimal = _to_minimal(row)
            if not minimal:
                continue
            if minimal.get("rating") is None:
                continue
            key = _key_of(minimal)
            if key:
                collected[key] = minimal
    _dbg(FEATURE, "snapshot_scanned", rows=seen_rows, rated=len(collected))
    return collected


def _index_from_library(adapter: Any) -> dict[str, dict[str, Any]]:
    resp = punchplay_request(adapter, "GET", URL_RATINGS)
    if resp.status_code != 200:
        _warn(FEATURE, "ratings_fetch_failed", status=resp.status_code, error=error_of(resp), request_id=request_id_of(resp))
        return {}
    data = safe_json(resp) or {}
    if not isinstance(data, Mapping):
        return {}
    rows = data.get("items")
    rows = [r for r in rows if isinstance(r, Mapping)] if isinstance(rows, list) else []
    collected: dict[str, dict[str, Any]] = {}
    for row in rows:
        minimal = _to_minimal(row)
        if not minimal or minimal.get("rating") is None:
            continue
        key = _key_of(minimal)
        if key:
            collected[key] = minimal
    if data.get("hasMore"):
        _warn(
            FEATURE,
            "ratings_library_truncated",
            returned=len(rows),
            total=data.get("total"),
            note="GET /me/ratings exposes no documented paging parameters",
        )
    return collected


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    collected = _index_from_snapshot(adapter)
    source = "snapshot"
    if not collected:
        collected = _index_from_library(adapter)
        source = "library"
    _info(FEATURE, "index_done", count=len(collected), source=source)
    return collected


def _payload_for(item: Mapping[str, Any], *, clear: bool) -> tuple[dict[str, Any], str] | None:
    key = _key_of(item)
    if not key:
        return None

    scope = _scope_of(item)
    source: Mapping[str, Any] = item
    if scope in ("season", "episode", "series"):
        show_ids = item.get("show_ids")
        if isinstance(show_ids, Mapping) and show_ids:
            source = {"ids": show_ids}
    payload = ids_for_punchplay(source)
    if not has_write_id(payload):
        return None

    payload["kind"] = _kind_of(item)
    if scope != "title":
        payload["scope"] = scope
    if scope in ("season", "episode"):
        season = _as_int(item.get("season"))
        if season is None:
            return None
        payload["season"] = season
    if scope == "episode":
        episode = _as_int(item.get("episode"))
        if episode is None:
            return None
        payload["episode"] = episode

    if clear:
        payload["rating"] = None
    else:
        rating = _rating_int(item.get("rating"))
        if rating is None:
            return None
        payload["rating"] = rating

    if item.get("is_favourite") is not None:
        payload["is_favourite"] = bool(item.get("is_favourite"))
    status = str(item.get("show_status") or "").strip().upper()
    if status in SHOW_STATUSES:
        payload["show_status"] = status

    if needs_client_item_id(payload):
        payload["client_item_id"] = client_item_id("cw", "ratings", key, scope)
    return payload, key


def _write(adapter: Any, items: Iterable[Mapping[str, Any]], *, clear: bool) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    unresolved_keys: list[str] = []

    for item in items or []:
        built = _payload_for(item, clear=clear)
        if built is None:
            key = _key_of(item)
            if key:
                unresolved_keys.append(key)
                unresolved.append({"key": key, "status": "missing_supported_id_or_rating"})
                _dbg(FEATURE, "item_unresolved_before_write", key=key)
            continue
        payload, key = built
        entries.append({"payload": payload, "key": key})

    if not entries:
        return {
            "ok": True,
            "confirmed_keys": [],
            "unresolved_keys": unresolved_keys,
            "deferred_keys": [],
            "unresolved": unresolved,
        }

    _dbg(FEATURE, "write_prepare", action="clear" if clear else "set", items=len(entries), skipped=len(unresolved_keys))
    res = bulk_write(adapter, "ratings", entries, _key_of, feature=FEATURE)
    res["unresolved_keys"] = list(res.get("unresolved_keys") or []) + unresolved_keys
    res["unresolved"] = list(res.get("unresolved") or []) + unresolved
    _info(
        FEATURE,
        "write_done",
        action="clear" if clear else "set",
        confirmed=len(res.get("confirmed_keys") or []),
        deferred=len(res.get("deferred_keys") or []),
        unresolved=len(res.get("unresolved_keys") or []),
    )
    return res


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, items, clear=False)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, items, clear=True)
