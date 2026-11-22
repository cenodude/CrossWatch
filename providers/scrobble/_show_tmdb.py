# providers/scrobble/_show_tmdb.py
# CrossWatch - Show TMDB Identity Lookup
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
import threading
import time
from collections.abc import Mapping
from typing import Any

import requests

TMDB_FIND = "https://api.themoviedb.org/3/find"
_SOURCES = (("tvdb", "tvdb_id"), ("imdb", "imdb_id"))
_LOOKUPS = (("show", "tv_results", "id"), ("episode", "tv_episode_results", "show_id"))
_MISS_TTL_SECONDS = 6 * 3600.0
_CACHE_MAX = 2048
_CACHE: dict[tuple[str, str, str], tuple[float, int | None]] = {}
_LOCK = threading.Lock()
_IMDB_RE = re.compile(r"^tt\d+$")


def _api_key(cfg: Mapping[str, Any]) -> str:
    tmdb = cfg.get("tmdb") or {}
    md = cfg.get("metadata") or {}
    return str((tmdb.get("api_key") if isinstance(tmdb, Mapping) else "") or (md.get("tmdb_api_key") if isinstance(md, Mapping) else "") or "").strip()


def _clean(namespace: str, value: Any) -> str:
    text = str(value or "").strip()
    if namespace == "imdb":
        return text if _IMDB_RE.match(text) else ""
    return text if text.isdecimal() and int(text) > 0 else ""


def _positive_int(value: Any) -> int | None:
    try:
        number = int(str(value).strip())
    except Exception:
        return None
    return number if number > 0 else None


def _find(api_key: str, source: str, external_id: str, bucket: str, field: str) -> int | None:
    key = (bucket, source, external_id)
    with _LOCK:
        hit = _CACHE.get(key)
    if hit and (hit[1] is not None or (time.time() - hit[0]) < _MISS_TTL_SECONDS):
        return hit[1]

    from providers.metadata._meta_TMDB import TmdbProvider

    provider = TmdbProvider(lambda: {"tmdb": {"api_key": api_key}}, lambda _: None)
    found: int | None = None
    try:
        data = provider.get_response(f"{TMDB_FIND}/{external_id}", {"external_source": source},
                                     quiet_404=True, timeout=10, max_retries=0, copy=False).data
    except requests.exceptions.RequestException as exc:
        if getattr(getattr(exc, "response", None), "status_code", None) != 404:
            return None
        data = {}
    except Exception:
        return None
    rows = data.get(bucket) if isinstance(data, Mapping) else None
    for row in rows if isinstance(rows, list) else []:
        found = _positive_int(row.get(field)) if isinstance(row, Mapping) else None
        if found:
            break

    with _LOCK:
        if len(_CACHE) > _CACHE_MAX:
            _CACHE.clear()
        _CACHE[key] = (time.time(), found)
    return found


def show_tmdb_id(
    cfg: Mapping[str, Any],
    show_ids: Mapping[str, Any] | None,
    episode_ids: Mapping[str, Any] | None,
) -> int | None:
    api_key = _api_key(cfg or {})
    if not api_key:
        return None
    candidates = {"show": show_ids or {}, "episode": episode_ids or {}}
    for kind, bucket, field in _LOOKUPS:
        for namespace, source in _SOURCES:
            external_id = _clean(namespace, candidates[kind].get(namespace))
            if not external_id:
                continue
            found = _find(api_key, source, external_id, bucket, field)
            if found:
                return found
    return None


def tmdb_configured(cfg: Mapping[str, Any] | None) -> bool:
    return bool(_api_key(cfg or {}))


def movie_tmdb_id(cfg: Mapping[str, Any], ids: Mapping[str, Any] | None) -> int | None:
    api_key = _api_key(cfg or {})
    imdb = _clean("imdb", (ids or {}).get("imdb"))
    if not api_key or not imdb:
        return None
    return _find(api_key, "imdb_id", imdb, "movie_results", "id")


def fill_display_tmdb(cfg: Mapping[str, Any], media_type: str, ids: dict[str, Any], show_ids: dict[str, Any]) -> bool:
    if not tmdb_configured(cfg):
        return False
    kind = str(media_type or "").strip().lower()
    if kind in {"episode", "season"}:
        if _positive_int(show_ids.get("tmdb")):
            return False
        shows = dict(show_ids)
        if not _clean("imdb", shows.get("imdb")) and _clean("imdb", ids.get("imdb")):
            shows["imdb"] = ids.get("imdb")
        found = show_tmdb_id(cfg, shows, ids if kind == "episode" else {})
        if found:
            show_ids["tmdb"] = str(found)
        return bool(found)
    if _positive_int(ids.get("tmdb")):
        return False
    found = movie_tmdb_id(cfg, ids) if kind == "movie" else show_tmdb_id(cfg, ids, {}) if kind in {"show", "tv", "anime"} else None
    if found:
        ids["tmdb"] = str(found)
    return bool(found)


def tmdb_title(cfg: Mapping[str, Any], media_type: str, tmdb: Any) -> tuple[str, int | None]:
    api_key = _api_key(cfg or {})
    ident = _positive_int(tmdb)
    if not api_key or not ident:
        return "", None
    from providers.metadata._meta_TMDB import TmdbProvider

    movie = str(media_type or "").strip().lower() == "movie"
    provider = TmdbProvider(lambda: {"tmdb": {"api_key": api_key}}, lambda _: None)
    try:
        data = provider.get_response(f"https://api.themoviedb.org/3/{'movie' if movie else 'tv'}/{ident}", {"language": "en-US"},
                                     quiet_404=True, timeout=10, max_retries=0, copy=False).data
    except Exception:
        return "", None
    if not isinstance(data, Mapping):
        return "", None
    title = str(data.get("title" if movie else "name") or "").strip()
    year = _positive_int(str(data.get("release_date" if movie else "first_air_date") or "")[:4])
    return title, year
