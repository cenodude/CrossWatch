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

from cw_platform.app_version import user_agent as http_user_agent

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

    try:
        r = requests.get(
            f"{TMDB_FIND}/{external_id}",
            params={"external_source": source, "api_key": api_key},
            headers={"User-Agent": http_user_agent("Scrobble", override_env="CW_TMDB_UA"), "Accept": "application/json"},
            timeout=10,
        )
    except Exception:
        return None
    if r.status_code not in (200, 404):
        return None

    found: int | None = None
    if r.status_code == 200:
        try:
            rows = (r.json() or {}).get(bucket) or []
        except Exception:
            return None
        for row in rows:
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
