# /providers/sync/trakt/_common.py

from __future__ import annotations
import os
from typing import Any, Dict, Iterable, Mapping

# ── headers ───────────────────────────────────────────────────────────────────
UA = os.environ.get("CW_UA", "CrossWatch/3.0 (Trakt)")

def build_headers(arg1: Any, access_token: str | None = None) -> Dict[str, str]:
    client_id = ""
    token = ""
    if isinstance(arg1, Mapping) and access_token is None:
        t = (arg1.get("trakt") or arg1)
        client_id = str(t.get("client_id") or "").strip()
        token     = str(t.get("access_token") or "").strip()
    else:
        client_id = str(arg1 or "").strip()
        token     = str(access_token or "").strip()

    h = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": client_id,
        "User-Agent": UA,
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h

# ── ids / keys ────────────────────────────────────────────────────────────────
try:
    from cw_platform.id_map import minimal as id_minimal, canonical_key
except Exception:
    from _id_map import minimal as id_minimal, canonical_key  # type: ignore

_ALLOWED_ID_KEYS = ("imdb", "tmdb", "tvdb", "trakt")

def _fix_imdb(ids: Dict[str, Any]) -> None:
    v = str(ids.get("imdb") or "").strip()
    if not v:
        return
    if not v.startswith("tt"):
        digits = "".join(ch for ch in v if ch.isdigit())
        if digits:
            ids["imdb"] = f"tt{digits}"

def normalize_watchlist_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    t = str(row.get("type") or "movie").lower()
    payload = (row.get("movie") if t == "movie" else row.get("show")) or {}
    ids = dict(payload.get("ids") or {})
    _fix_imdb(ids)
    m = {
        "type": "movie" if t == "movie" else "show",
        "title": payload.get("title"),
        "year": payload.get("year"),
        "ids": {k: str(v) for k, v in ids.items() if v},
    }
    return id_minimal(m)

def normalize(obj: Mapping[str, Any]) -> Dict[str, Any]:
    if "ids" in obj and "type" in obj:
        return id_minimal(obj)
    if "type" in obj and ("movie" in obj or "show" in obj):
        return normalize_watchlist_row(obj)
    return id_minimal(obj)

def key_of(item: Mapping[str, Any]) -> str:
    return canonical_key(item)

def ids_for_trakt(item: Mapping[str, Any]) -> Dict[str, str]:
    ids = dict(item.get("ids") or {})
    _fix_imdb(ids)
    t = str(item.get("type") or "").lower()

    if t == "episode":
        has_scope = (item.get("season") is not None) and (item.get("episode") is not None)
        if has_scope and not item.get("show_ids"):
            return {}
        show_ids = dict(item.get("show_ids") or {})
        for key in list(ids.keys()):
            if key in show_ids and str(ids.get(key)) == str(show_ids.get(key)):
                ids.pop(key, None)
        for key in ("imdb", "tvdb", "trakt", "tmdb"):
            v = ids.get(key)
            if v:
                return {key: str(v)}
        return {}

    return {k: str(v) for k, v in ids.items() if k in _ALLOWED_ID_KEYS and v}

def pick_trakt_kind(item: Mapping[str, Any]) -> str:
    t = str(item.get("type") or "movie").lower()
    if t in ("episode",):
        return "episodes"
    if t in ("season",):
        return "seasons"
    if t in ("show", "series", "tv"):
        return "shows"
    return "movies"

def build_watchlist_body(items: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    movies: list = []
    shows: list = []
    for it in items or []:
        ids = ids_for_trakt(it)
        if not ids:
            continue
        kind = pick_trakt_kind(it)
        if kind == "shows":
            shows.append({"ids": ids})
        elif kind == "movies":
            movies.append({"ids": ids})
    body: Dict[str, Any] = {}
    if movies: body["movies"] = movies
    if shows:  body["shows"]  = shows
    return body
