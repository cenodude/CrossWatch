# cw_platform/id_map.py
# ID Mapping and Canonical Keys
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from itertools import chain
from typing import Any

# Policy
ID_KEYS: tuple[str, ...] = (
    "imdb",
    "tmdb",
    "tvdb",
    "trakt",
    "simkl",
    "mal",
    "anilist",
    "kitsu",
    "anidb",
    "plex",
    "jellyfin",
    "guid",
    "slug",
)
KEY_PRIORITY: tuple[str, ...] = (
    "imdb",
    "tmdb",
    "tvdb",
    "trakt",
    "mal",
    "anilist",
    "kitsu",
    "anidb",
    "simkl",
    "plex",
    "guid",
    "slug",
)

__all__ = [
    "ID_KEYS",
    "KEY_PRIORITY",
    "ids_from",
    "ids_from_guid",
    "ids_from_jellyfin_providerids",
    "merge_ids",
    "coalesce_ids",
    "canonical_key",
    "keys_for_item",
    "unified_keys_from_ids",
    "any_key_overlap",
    "minimal",
    "has_external_ids",
    "preferred_id_key",
]

# utils
_CLEAN_SENTINELS = {"none", "null", "nan", "undefined", "unknown", "0", ""}


def _norm_str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _norm_type(t: Any) -> str:
    x = (str(t or "")).strip().lower()
    if x in ("movies", "movie"):
        return "movie"
    if x in ("shows", "show", "series", "tv"):
        return "show"
    if x in ("seasons", "season"):
        return "season"
    if x in ("episodes", "episode"):
        return "episode"
    return x or "movie"


def _normalize_id(key: str, val: Any) -> str | None:
    k = (key or "").lower().strip()
    s = _norm_str(val)
    if not s:
        return None
    if s.lower() in _CLEAN_SENTINELS:
        return None

    if k in ("tmdb", "tvdb", "trakt", "simkl", "mal", "anilist", "kitsu", "anidb", "plex", "jellyfin"):
        digits = re.sub(r"\D+", "", s)
        return digits or None

    if k == "imdb":
        s = s.lower()
        m = re.search(r"(tt\d+)", s)
        if m:
            return m.group(1)
        digits = re.sub(r"\D+", "", s)
        return f"tt{digits}" if digits else None

    if k == "slug":
        return s.lower()

    if k == "guid":
        return s

    return s


# --- GUID to ID
_GUID_PATTERNS: tuple[tuple[re.Pattern, str], ...] = (
    # com.plexapp agents
    (re.compile(r"com\.plexapp\.agents\.imdb://(?P<imdb>tt\d+)", re.I), "imdb"),
    (re.compile(r"com\.plexapp\.agents\.themoviedb://(?P<tmdb>\d+)", re.I), "tmdb"),
    (re.compile(r"com\.plexapp\.agents\.thetvdb://(?P<tvdb>\d+)", re.I), "tvdb"),
    # generic schemes
    (re.compile(r"imdb://(?:title/)?(?P<imdb>tt\d+)", re.I), "imdb"),
    (re.compile(r"tmdb://(?:(?:movie|show|tv)/)?(?P<tmdb>\d+)", re.I), "tmdb"),
    (re.compile(r"tvdb://(?:(?:series|show|tv)/)?(?P<tvdb>\d+)", re.I), "tvdb"),
    (re.compile(r"^plex://", re.I), "guid"),
)


def ids_from_guid(guid: str | None) -> dict[str, str]:
    out: dict[str, str] = {}
    g = _norm_str(guid)
    if not g:
        return out
    for rx, label in _GUID_PATTERNS:
        m = rx.search(g)
        if not m:
            continue
        if label in ("imdb", "tmdb", "tvdb"):
            raw = m.groupdict().get(label)
            norm = _normalize_id(label, raw)
            if norm:
                out[label] = norm
        elif label == "guid":
            out["guid"] = g
    return out


# Jellyfin ProviderIds to ids
_JF_MAP: dict[str, str] = {
    "Imdb": "imdb",
    "Tmdb": "tmdb",
    "Tvdb": "tvdb",
    "Trakt": "trakt",
    "Simkl": "simkl",
    "Anidb": "anidb",
    "AniDB": "anidb",
    "Anilist": "anilist",
    "AniList": "anilist",
    "Kitsu": "kitsu",
    "Mal": "mal",
    "MAL": "mal",
    "MyAnimeList": "mal",
}


def ids_from_jellyfin_providerids(pids: Mapping[str, Any] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    if not isinstance(pids, Mapping):
        return out
    for k, v in pids.items():
        dst = _JF_MAP.get(str(k))
        if not dst:
            continue
        n = _normalize_id(dst, v)
        if n:
            out[dst] = n
    return out


# Collect and merge
def coalesce_ids(*many: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for ids in many:
        if not isinstance(ids, Mapping):
            continue
        for k in ID_KEYS:
            n = _normalize_id(k, ids.get(k))
            if n:
                out[k] = n
    return out


def ids_from(item: Mapping[str, Any]) -> dict[str, str]:
    base = item.get("ids") if isinstance(item.get("ids"), Mapping) else {}
    top = {k: item.get(k) for k in ID_KEYS if item.get(k) is not None}
    guid_val = item.get("guid") or (base.get("guid") if isinstance(base, Mapping) else None)
    from_guid = ids_from_guid(str(guid_val)) if guid_val else {}
    return coalesce_ids(top, base or {}, from_guid)


def merge_ids(old: Mapping[str, Any] | None, new: Mapping[str, Any] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    old = dict(old or {})
    new = dict(new or {})

    for k in KEY_PRIORITY:
        v = _normalize_id(k, old.get(k)) or _normalize_id(k, new.get(k))
        if v:
            out[k] = v

    for k, v in chain(old.items(), new.items()):
        if k not in out or not out[k]:
            n = _normalize_id(k, v)
            if n:
                out[k] = n

    return {k: v for k, v in out.items() if v}


# C-keys
def _title_year_key(item: Mapping[str, Any]) -> str | None:
    t = _norm_str(item.get("title"))
    y = _norm_str(item.get("year")) or ""
    typ = _norm_type(item.get("type"))
    if not t:
        return None
    # Prefer stable cross-provider matching when we only have title/year.
    # Most providers treat anime as "show", so we do the same for this fallback key.
    typ_key = "show" if typ == "anime" else typ
    return f"{typ_key}|title:{t.lower()}|year:{y}"


def _best_id_key(idmap: Mapping[str, str]) -> str | None:
    for k in KEY_PRIORITY:
        v = idmap.get(k)
        if v:
            return f"{k}:{v}".lower()
    return None


def _show_id_from(item: Mapping[str, Any]) -> str | None:
    show_ids = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else None
    if show_ids:
        kid = _best_id_key(coalesce_ids(show_ids))
        if kid:
            return kid
    return _best_id_key(ids_from(item))


def _se_fragment(item: Mapping[str, Any]) -> str | None:
    s = item.get("season") or item.get("season_number")
    e = item.get("episode") or item.get("episode_number")
    try:
        s = int(s) if s is not None else None
        e = int(e) if e is not None else None
    except Exception:
        return None
    if s is None:
        return None
    if item and _norm_type(item.get("type")) == "season":
        return f"#season:{s}"
    if e is None:
        return None
    return f"#s{str(s).zfill(2)}e{str(e).zfill(2)}"


def canonical_key(item: Mapping[str, Any]) -> str:
    typ = _norm_type(item.get("type"))
    if typ in ("season", "episode"):
        show_id = _show_id_from(item)
        frag = _se_fragment(item)
        if show_id and frag:
            return f"{show_id}{frag}".lower()
    idkey = _best_id_key(ids_from(item))
    if idkey:
        return idkey
    ty = _title_year_key(item)
    return ty or "unknown:"


def unified_keys_from_ids(idmap: Mapping[str, Any]) -> set[str]:
    out: set[str] = set()
    for k in ID_KEYS:
        n = _normalize_id(k, idmap.get(k))
        if n:
            out.add(f"{k}:{n}".lower())
    return out


def keys_for_item(item: Mapping[str, Any]) -> set[str]:
    out = unified_keys_from_ids(ids_from(item))
    ty = _title_year_key(item)
    if ty:
        out.add(ty)
    typ = _norm_type(item.get("type"))
    if typ in ("season", "episode"):
        sid = _show_id_from(item)
        frag = _se_fragment(item)
        if sid and frag:
            out.add(f"{sid}{frag}".lower())
    return out


def any_key_overlap(a: Iterable[str], b: Iterable[str]) -> bool:
    sa, sb = set(a or []), set(b or [])
    return bool(sa and sb and not sa.isdisjoint(sb))


def minimal(item: Mapping[str, Any]) -> dict[str, Any]:
    ids = ids_from(item)
    typ = _norm_type(item.get("type"))
    out: dict[str, Any] = {
        "type": typ,
        "title": item.get("title"),
        "year": item.get("year"),
        "ids": {k: ids[k] for k in ID_KEYS if k in ids},
    }
    for opt in ("watched", "watched_at", "rating", "rated_at", "season", "episode", "series_title"):
        if opt in item:
            out[opt] = item.get(opt)

    if typ in ("season", "episode"):
        sids_raw = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else None
        if sids_raw:
            sids = coalesce_ids(sids_raw)
            if sids:
                out["show_ids"] = {k: sids[k] for k in ID_KEYS if k in sids}
    return out


# Helpers
def has_external_ids(obj: Mapping[str, Any]) -> bool:
    ids = ids_from(obj) if "ids" in obj or "guid" in obj else obj
    return any(ids.get(k) for k in ("imdb", "tmdb", "tvdb"))


def preferred_id_key(obj: Mapping[str, Any]) -> str | None:
    ids = ids_from(obj) if "ids" in obj or "guid" in obj else obj
    return _best_id_key(ids)  # type: ignore[arg-type]