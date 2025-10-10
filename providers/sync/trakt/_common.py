# /providers/sync/trakt/_common.py

from __future__ import annotations
import os
from typing import Any, Dict, Iterable, Mapping

# ── headers ───────────────────────────────────────────────────────────────────
UA = os.environ.get("CW_UA", "CrossWatch/3.0 (Trakt)")

def build_headers(arg1: Any, access_token: str | None = None) -> Dict[str, str]:
    """
    Trakt required headers.

    Usage:
      build_headers(client_id, access_token)
      build_headers({"trakt": {"client_id": "...", "access_token": "..."}})
      build_headers({"client_id": "...", "access_token": "..."})

    Returns a stable header set; Authorization is included only if a token is provided.
    """
    client_id = ""
    token = ""

    # Back-compat: allow dict inputs (flat or {"trakt": {...}})
    if isinstance(arg1, Mapping) and access_token is None:
        t = (arg1.get("trakt") or arg1)  # accept both shapes
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
    """Normalize imdb to ttNNN if Trakt (or sources) return bare numbers."""
    v = str(ids.get("imdb") or "").strip()
    if not v:
        return
    if not v.startswith("tt"):
        digits = "".join(ch for ch in v if ch.isdigit())
        if digits:
            ids["imdb"] = f"tt{digits}"

def normalize_watchlist_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Trakt /sync/watchlist row:
      { "type":"movie|show", "movie|show": { "title","year","ids":{...} } }
    → CrossWatch minimal.
    """
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
    """If already minimal, pass through; else best-effort."""
    if "ids" in obj and "type" in obj:
        return id_minimal(obj)
    if "type" in obj and ("movie" in obj or "show" in obj):
        return normalize_watchlist_row(obj)
    return id_minimal(obj)

def key_of(item: Mapping[str, Any]) -> str:
    """Canonical, id-based key (no timestamps)."""
    return canonical_key(item)

# ── tiny helpers for feature modules (watchlist/ratings/history) ──────────────

def ids_for_trakt(item: Mapping[str, Any]) -> Dict[str, str]:
    """
    Return a clean Trakt ids dict for the item's scope.
    Keeps only imdb/tmdb/tvdb/trakt, imdb normalized to tt… where needed.
    """
    ids = dict(item.get("ids") or {})
    _fix_imdb(ids)
    return {k: str(v) for k, v in ids.items() if k in _ALLOWED_ID_KEYS and v}

def pick_trakt_kind(item: Mapping[str, Any]) -> str:
    """
    Map item['type'] -> Trakt collection keys:
      movie   -> "movies"
      show    -> "shows"
      season  -> "seasons"
      episode -> "episodes"
    Default: "movies".
    """
    t = str(item.get("type") or "movie").lower()
    if t in ("episode",):
        return "episodes"
    if t in ("season",):
        return "seasons"
    if t in ("show", "series", "tv"):
        return "shows"
    return "movies"

def build_watchlist_body(items: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    """
    Group items for Trakt /sync/watchlist or /sync/watchlist/remove payloads.
    IDs only, never titles. (Trakt watchlist supports movies + shows.)
    """
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
        # ignore seasons/episodes here by design (watchlist doesn’t accept them)
    body: Dict[str, Any] = {}
    if movies: body["movies"] = movies
    if shows:  body["shows"]  = shows
    return body
