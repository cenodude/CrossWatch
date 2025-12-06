# /providers/sync/plex/_common.py
# Plex Module for common utilities
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import re
import uuid
import xml.etree.ElementTree as ET
from typing import Any, Mapping

import requests

from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from_guid

__all__ = [
    "DISCOVER",
    "METADATA",
    "CLIENT_ID",
    "configure_plex_context",
    "plex_headers",
    "normalize",
    "key_of",
    "candidate_guids_from_ids",
    "section_find_by_guid",
    "meta_guids",
    "minimal_from_history_row",
    "server_find_rating_key_by_guid",
]

_PLEX_CTX: dict[str, str | None] = {"baseurl": None, "token": None}


def configure_plex_context(baseurl: str, token: str) -> None:
    _PLEX_CTX["baseurl"] = (baseurl or "").rstrip("/")
    _PLEX_CTX["token"] = token or None


DISCOVER = "https://discover.provider.plex.tv"

METADATA: dict[str, dict[str, Any]] = {
    "plex": {
        "ids": ("plex",),
        "guid_fields": ("guid",),
        "guid_patterns": ("plex://",),
    },
    "imdb": {
        "ids": ("imdb",),
        "guid_fields": ("guid",),
        "guid_patterns": ("imdb://", "com.plexapp.agents.imdb://", "com.plexapp.agents.none://"),
    },
    "tmdb": {
        "ids": ("tmdb",),
        "guid_fields": ("guid",),
        "guid_patterns": ("tmdb://", "com.plexapp.agents.themoviedb://"),
    },
    "tvdb": {
        "ids": ("tvdb",),
        "guid_fields": ("guid",),
        "guid_patterns": ("tvdb://", "com.plexapp.agents.thetvdb://"),
    },
    "tvmaze": {
        "ids": ("tvmaze",),
        "guid_fields": ("guid",),
        "guid_patterns": ("tvmaze://",),
    },
    "anidb": {
        "ids": ("anidb",),
        "guid_fields": ("guid",),
        "guid_patterns": ("anidb://",),
    },
}

CLIENT_ID = f"crosswatch-{uuid.uuid4().hex[:8]}"


def _env(name: str, default: str | None = None) -> str | None:
    v = os.environ.get(name)
    return v if v is not None else default


def _log(msg: str) -> None:
    if _env("CW_DEBUG") or _env("CW_PLEX_DEBUG"):
        print(f"[PLEX] {msg}")


def plex_headers(extra: Mapping[str, str] | None = None) -> dict[str, str]:
    token = _PLEX_CTX.get("token")
    ua = _env("CW_UA", "CrossWatch/1.0 (Plex)")
    headers: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": ua or "CrossWatch/1.0 (Plex)",
        "X-Plex-Product": "CrossWatch",
        "X-Plex-Version": "1.0",
        "X-Plex-Client-Identifier": CLIENT_ID,
    }
    if token:
        headers["X-Plex-Token"] = token
    if extra:
        headers.update({str(k): str(v) for k, v in extra.items()})
    return headers


def _xml_to_container(text: str) -> dict[str, Any]:
    try:
        root = ET.fromstring(text or "")
    except Exception:
        return {}
    out: dict[str, Any] = {}
    out.update(root.attrib or {})
    children: list[dict[str, Any]] = []
    for child in root:
        item: dict[str, Any] = {}
        item.update(child.attrib or {})
        item["tag"] = child.tag
        children.append(item)
    out["items"] = children
    return out


def _xml_to_dict(resp: requests.Response) -> dict[str, Any]:
    try:
        txt = resp.text or ""
        if not txt.strip():
            return {}
        return _xml_to_container(txt)
    except Exception:
        return {}


def type_of(obj: Mapping[str, Any]) -> str:
    t = str(obj.get("type") or obj.get("subtype") or "").strip().lower()
    if t:
        return t
    media_type = str(obj.get("MediaType") or "").strip().lower()
    if media_type in ("episode", "movie", "show", "artist", "track"):
        return media_type
    if "grandparentGuid" in obj and "parentGuid" in obj:
        return "episode"
    if "parentGuid" in obj and not obj.get("grandparentGuid"):
        return "season"
    if "grandparentRatingKey" in obj and "parentRatingKey" in obj:
        return "episode"
    if "parentRatingKey" in obj and not obj.get("grandparentRatingKey"):
        return "season"
    return "movie"


def parse_int_or_none(v: Any) -> int | None:
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if not s:
            return None
        return int(s)
    except Exception:
        return None


def ids_from_obj(obj: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    guid = str(obj.get("guid") or "").strip()
    if guid.startswith("plex://"):
        out["plex"] = guid.replace("plex://", "", 1)
    if "ratingKey" in obj:
        out["plex_rating_key"] = str(obj.get("ratingKey"))
    if "imdb://tt" in guid or "com.plexapp.agents.imdb://" in guid:
        m = re.search(r"(tt\d+)", guid)
        if m:
            out["imdb"] = m.group(1)
    if "tmdb://" in guid or "themoviedb://" in guid or "com.plexapp.agents.themoviedb://" in guid:
        m = re.search(r"(\d+)", guid)
        if m:
            out["tmdb"] = m.group(1)
    if "tvdb://" in guid or "thetvdb://" in guid or "com.plexapp.agents.thetvdb://" in guid:
        m = re.search(r"(\d+)", guid)
        if m:
            out["tvdb"] = m.group(1)
    if "tvmaze://" in guid:
        m = re.search(r"(\d+)", guid)
        if m:
            out["tvmaze"] = m.group(1)
    if "anidb://" in guid:
        m = re.search(r"(\d+)", guid)
        if m:
            out["anidb"] = m.group(1)
    return out


def key_of(obj: Mapping[str, Any]) -> str:
    ids = ids_from_obj(obj)
    return canonical_key(ids)


def _audio_tag(obj: Mapping[str, Any]) -> str | None:
    media = obj.get("Media") or []
    if not isinstance(media, list):
        return None
    for m in media:
        if not isinstance(m, Mapping):
            continue
        parts = m.get("Part") or []
        if isinstance(parts, Mapping):
            parts = [parts]
        for p in parts:
            if not isinstance(p, Mapping):
                continue
            streams = p.get("Stream") or []
            if isinstance(streams, Mapping):
                streams = [streams]
            for s in streams:
                if not isinstance(s, Mapping):
                    continue
                if s.get("streamType") == 2:
                    lang = str(s.get("languageCode") or "").strip().lower()
                    return lang or None
    return None


def _normalize_discover_row(row: Mapping[str, Any]) -> dict[str, Any]:
    item = dict(row.get("metadata") or {})
    ids = dict(row.get("ids") or {})
    out: dict[str, Any] = {}
    out["type"] = str(item.get("type") or "").strip().lower() or "movie"
    out["title"] = item.get("title") or item.get("name") or row.get("title")
    out["year"] = parse_int_or_none(item.get("year") or item.get("releaseYear") or row.get("year"))
    out["guid"] = item.get("guid") or ""
    out["ratingKey"] = item.get("ratingKey") or None
    for k in ("imdb", "tmdb", "tvdb", "tvmaze", "anidb"):
        if k in ids and ids.get(k):
            out[k] = str(ids[k])
    return out


def normalize(obj: Mapping[str, Any]) -> dict[str, Any]:
    if "metadataType" in obj and "MediaContainer" in obj:
        try:
            items = obj["MediaContainer"].get("Metadata") or []
            if isinstance(items, Mapping):
                items = [items]
            obj = items[0] if items else {}
        except Exception:
            pass
    t = type_of(obj)
    out: dict[str, Any] = {}
    out["type"] = t
    out["title"] = obj.get("title") or obj.get("grandparentTitle") or obj.get("parentTitle")
    out["year"] = parse_int_or_none(obj.get("year"))
    out["guid"] = obj.get("guid") or obj.get("grandparentGuid") or obj.get("parentGuid")
    out["ratingKey"] = obj.get("ratingKey") or obj.get("grandparentRatingKey") or obj.get("parentRatingKey")
    out["audio_language"] = _audio_tag(obj)
    ids = ids_from_obj(obj)
    out.update(ids)
    hydrate_external_ids(out)
    return out


def candidate_key(obj: Mapping[str, Any]) -> str:
    return canonical_key(ids_from_obj(obj))


_GUID_CACHE: dict[str, dict[str, Any]] = {}
_HYDRATE_404: set[str] = set()


def hydrate_external_ids(obj: dict[str, Any]) -> None:
    key = candidate_key(obj)
    if not key or key in _HYDRATE_404:
        return
    cached = _GUID_CACHE.get(key)
    if cached:
        obj.update(cached)
        return
    guid = obj.get("guid")
    if not guid:
        return
    plex_id = obj.get("plex") or ids_from_guid(str(guid)).get("plex")
    if not plex_id:
        return
    baseurl = _PLEX_CTX.get("baseurl")
    token = _PLEX_CTX.get("token")
    if not baseurl or not token:
        return
    url = f"{baseurl}/library/metadata/{plex_id}"
    try:
        resp = requests.get(url, headers=plex_headers(), timeout=10)
        if resp.status_code != 200:
            if resp.status_code == 404:
                _HYDRATE_404.add(key)
            return
        data = _xml_to_dict(resp)
        items = (data.get("items") or []) if isinstance(data, Mapping) else []
        if not items:
            return
        ids = ids_from_obj(items[0])
        if not ids:
            _HYDRATE_404.add(key)
            return
        _GUID_CACHE[key] = ids
        obj.update(ids)
    except Exception:
        return


def meta_guids(obj: Mapping[str, Any]) -> dict[str, Any]:
    ids = ids_from_obj(obj)
    hydrate_external_ids(ids)
    return ids


def _iso8601_any(v: Any) -> int | None:
    if not v:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        import datetime as dt

        s2 = s.replace("Z", "+0000").split(".")[0]
        return int(dt.datetime.strptime(s2, "%Y-%m-%dT%H:%M:%S%z").timestamp())
    except Exception:
        try:
            import datetime as dt

            return int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
        except Exception:
            return None


def _watched_at_from_row(row: Mapping[str, Any]) -> int | None:
    for key in ("viewedAt", "lastViewedAt", "lastViewedAtSimkl"):
        v = row.get(key)
        ts = _iso8601_any(v)
        if ts:
            return ts
    return None


def _build_minimal_from_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    ids = ids_from_obj(row)
    hydrate_external_ids(ids)
    if not ids:
        return None
    t = type_of(row)
    year = parse_int_or_none(row.get("year"))
    title = row.get("title") or row.get("grandparentTitle") or row.get("parentTitle")
    watched_at = _watched_at_from_row(row)
    item: dict[str, Any] = dict(row)
    existing_ids = item.get("ids")
    if isinstance(existing_ids, Mapping):
        merged_ids = dict(existing_ids)
        merged_ids.update(ids)
        item["ids"] = merged_ids
    else:
        item["ids"] = dict(ids)
    if t:
        item["type"] = t
    if title:
        item["title"] = title
    if year is not None:
        item["year"] = year
    if watched_at is not None:
        item["watched_at"] = watched_at
    return id_minimal(item)


def minimal_from_history_row(row: Mapping[str, Any] | None, allow_discover: bool = False) -> dict[str, Any] | None:
    if not isinstance(row, Mapping):
        return None
    try:
        return _build_minimal_from_row(row)
    except Exception as e:
        _log(f"minimal_from_history_row: error: {e}")
        if allow_discover:
            return None
        return None


def candidate_guids_from_ids(ids: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    plex = ids.get("plex")
    if plex:
        out["plex://"] = plex
    imdb = ids.get("imdb")
    if imdb:
        out["imdb://"] = imdb
    tmdb = ids.get("tmdb")
    if tmdb:
        out["tmdb://"] = tmdb
    tvdb = ids.get("tvdb")
    if tvdb:
        out["tvdb://"] = tvdb
    tvmaze = ids.get("tvmaze")
    if tvmaze:
        out["tvmaze://"] = tvmaze
    anidb = ids.get("anidb")
    if anidb:
        out["anidb://"] = anidb
    return out


def section_find_by_guid(
    session: requests.Session,
    *,
    baseurl: str,
    section_key: str,
    ids: Mapping[str, Any],
    try_discover: bool = True,
) -> dict[str, Any] | None:
    candidates = candidate_guids_from_ids(ids)
    if not candidates:
        return None
    base = baseurl.rstrip("/")
    token = _PLEX_CTX.get("token")
    params: dict[str, Any] = {}
    if token:
        params["X-Plex-Token"] = token
    for prefix, ident in candidates.items():
        guid = f"{prefix}{ident}"
        url = f"{base}/library/sections/{section_key}/all"
        try:
            resp = session.get(url, params={**params, "guid": guid}, timeout=10)
            if resp.status_code != 200:
                continue
            data = _xml_to_dict(resp)
            items = (data.get("items") or []) if isinstance(data, Mapping) else []
            if not items:
                continue
            return items[0]
        except Exception:
            continue
    if not try_discover:
        return None
    try:
        payload = {"ids": dict(ids), "type": "movie"}
        resp = requests.post(f"{DISCOVER}/library/metadata", json=payload, timeout=10)
        if resp.status_code != 200:
            return None
        data = resp.json()
        meta = (data.get("MediaContainer") or {}).get("Metadata") or []
        if isinstance(meta, Mapping):
            meta = [meta]
        if not meta:
            return None
        item = meta[0]
        guid = item.get("guid")
        if not guid:
            return None
        url = f"{base}/library/metadata/{item.get('ratingKey')}"
        resp2 = session.get(url, params=params, timeout=10)
        if resp2.status_code != 200:
            return None
        data2 = _xml_to_dict(resp2)
        items2 = (data2.get("items") or []) if isinstance(data2, Mapping) else []
        if not items2:
            return None
        return items2[0]
    except Exception:
        return None


def server_find_rating_key_by_guid(
    session: requests.Session,
    *,
    baseurl: str,
    ids: Mapping[str, Any],
) -> str | None:
    base = baseurl.rstrip("/")
    token = _PLEX_CTX.get("token")
    params: dict[str, Any] = {}
    if token:
        params["X-Plex-Token"] = token
    candidates = candidate_guids_from_ids(ids)
    if not candidates:
        return None
    for prefix, ident in candidates.items():
        guid = f"{prefix}{ident}"
        url = f"{base}/library/all"
        try:
            resp = session.get(url, params={**params, "guid": guid}, timeout=10)
            if resp.status_code != 200:
                continue
            data = _xml_to_dict(resp)
            items = (data.get("items") or []) if isinstance(data, Mapping) else []
            if not items:
                continue
            rk = items[0].get("ratingKey")
            if rk:
                return str(rk)
        except Exception:
            continue
    return None