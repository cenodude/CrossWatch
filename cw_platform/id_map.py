# /cw_platform/id_map.py (or _id_map.py)
# Common ID handling for movies/shows/seasons/episodes.
# - Normalize/clean IDs from various sources (Plex, Jellyfin, etc).
# - Merge multiple ID maps (fill-only, prefer strong IDs).
# - Generate canonical keys for deduplication/joins.
# - Extract all comparable keys for an item (for aliasing).
# - Minimal projection for logs/UI/shadows (keep show_ids for SIMKL specific).

from __future__ import annotations
import re
from typing import Any, Dict, Iterable, Mapping, Optional, Set, Tuple
from itertools import chain

# Public policy: use these everywhere (planning, shadows, logging).
ID_KEYS: Tuple[str, ...]       = ("imdb", "tmdb", "tvdb", "trakt", "simkl", "plex", "jellyfin", "guid", "slug")
KEY_PRIORITY: Tuple[str, ...]  = ("imdb", "tmdb", "tvdb", "trakt", "simkl", "plex", "guid", "slug")

__all__ = [
    "ID_KEYS", "KEY_PRIORITY",
    "ids_from", "ids_from_guid", "ids_from_jellyfin_providerids",
    "merge_ids", "coalesce_ids",
    "canonical_key", "keys_for_item", "unified_keys_from_ids", "any_key_overlap",
    "minimal",
    # small helpers
    "has_external_ids", "preferred_id_key",
]

# --- tiny utils ---------------------------------------------------------------

_CLEAN_SENTINELS = {"none", "null", "nan", "undefined", "unknown", "0", ""}

def _norm_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None

def _norm_type(t: Any) -> str:
    x = (str(t or "")).strip().lower()
    if x in ("movies", "movie"): return "movie"
    if x in ("shows", "show", "series", "tv"): return "show"
    if x in ("seasons", "season"): return "season"
    if x in ("episodes", "episode"): return "episode"
    return x or "movie"

def _normalize_id(key: str, val: Any) -> Optional[str]:
    """Normalize common provider IDs so we can compare apples with apples."""
    k = (key or "").lower().strip()
    s = _norm_str(val)
    if not s:
        return None
    if s.lower() in _CLEAN_SENTINELS:
        return None

    if k in ("tmdb", "tvdb", "trakt", "simkl", "plex", "jellyfin"):
        digits = re.sub(r"\D+", "", s)
        return digits or None

    if k == "imdb":
        s = s.lower()
        # Accept plain tt\d+ or permissive strings containing it
        m = re.search(r"(tt\d+)", s)
        if m:
            return m.group(1)
        digits = re.sub(r"\D+", "", s)
        return f"tt{digits}" if digits else None

    if k == "slug":
        return s.lower()

    if k == "guid":
        # Keep raw GUID (we further parse with ids_from_guid when needed)
        return s

    return s

# --- Plex GUID to IDs ---------------------------------------------------------

# Accept many real-world variants Plex returns in <Guid id="...">
_GUID_PATTERNS: Tuple[Tuple[re.Pattern, str], ...] = (
    # com.plexapp agents
    (re.compile(r"com\.plexapp\.agents\.imdb://(?P<imdb>tt\d+)", re.I), "imdb"),
    (re.compile(r"com\.plexapp\.agents\.themoviedb://(?P<tmdb>\d+)", re.I), "tmdb"),
    (re.compile(r"com\.plexapp\.agents\.thetvdb://(?P<tvdb>\d+)", re.I), "tvdb"),

    # generic schemes (permissive)
    (re.compile(r"imdb://(?:title/)?(?P<imdb>tt\d+)", re.I), "imdb"),
    (re.compile(r"tmdb://(?:(?:movie|show|tv)/)?(?P<tmdb>\d+)", re.I), "tmdb"),
    (re.compile(r"tvdb://(?:(?:series|show|tv)/)?(?P<tvdb>\d+)", re.I), "tvdb"),

    # newer plex:// scheme → keep GUID if nothing else found
    (re.compile(r"^plex://", re.I), "guid"),
)

def ids_from_guid(guid: Optional[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
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

# --- Jellyfin ProviderIds → ids (optional helper) ----------------------------

_JF_MAP = {
    "Imdb": "imdb",
    "Tmdb": "tmdb",
    "Tvdb": "tvdb",
    "Trakt": "trakt",
    "Simkl": "simkl",
}

def ids_from_jellyfin_providerids(pids: Mapping[str, Any] | None) -> Dict[str, str]:
    out: Dict[str, str] = {}
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

# --- Collect / merge ----------------------------------------------------------

def coalesce_ids(*many: Mapping[str, Any]) -> Dict[str, str]:
    """Merge several 'ids' maps into one normalized dict."""
    out: Dict[str, str] = {}
    for ids in many:
        if not isinstance(ids, Mapping):
            continue
        for k in ID_KEYS:
            n = _normalize_id(k, ids.get(k))
            if n:
                out[k] = n
    return out

def ids_from(item: Mapping[str, Any]) -> Dict[str, str]:
    """
    Pull IDs from:
      - item["ids"] (canonical place),
      - top level fields (imdb/tmdb/...),
      - item["guid"] (Plex), converted to external ids when possible.
    """
    base = item.get("ids") if isinstance(item.get("ids"), Mapping) else {}
    top = {k: item.get(k) for k in ID_KEYS if item.get(k) is not None}
    guid_val = item.get("guid") or (base.get("guid") if isinstance(base, Mapping) else None)
    from_guid = ids_from_guid(str(guid_val)) if guid_val else {}
    return coalesce_ids(top, base or {}, from_guid)

def merge_ids(old: Mapping[str, Any] | None, new: Mapping[str, Any] | None) -> Dict[str, str]:
    """Fill-only merge; prefer existing strong IDs; never clobber."""
    out: Dict[str, str] = {}
    old = dict(old or {})
    new = dict(new or {})

    # Fill in priority order from 'old' first, then 'new'
    for k in KEY_PRIORITY:
        out[k] = _normalize_id(k, old.get(k)) or _normalize_id(k, new.get(k)) or out.get(k)

    # Keep any extra keys (e.g., slug/guid) if present
    for k, v in chain(old.items(), new.items()):
        if k not in out or not out[k]:
            n = _normalize_id(k, v)
            if n:
                out[k] = n

    return {k: v for k, v in out.items() if v}

# --- Canonical keys -----------------------------------------------------------

def _title_year_key(item: Mapping[str, Any]) -> Optional[str]:
    t = _norm_str(item.get("title"))
    y = _norm_str(item.get("year")) or ""
    typ = _norm_type(item.get("type"))
    if not t:
        return None
    return f"{typ}|title:{t.lower()}|year:{y}"

def _best_id_key(idmap: Mapping[str, str]) -> Optional[str]:
    for k in KEY_PRIORITY:
        v = idmap.get(k)
        if v:
            return f"{k}:{v}".lower()
    return None

def _show_id_from(item: Mapping[str, Any]) -> Optional[str]:
    # Prefer explicit show_ids if present (cheap way for episodes/seasons).
    show_ids = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else None
    if show_ids:
        kid = _best_id_key(coalesce_ids(show_ids))
        if kid:
            return kid
    # Otherwise use the item's own ids (most agents put show-level ids on eps).
    return _best_id_key(ids_from(item))

def _se_fragment(item: Mapping[str, Any]) -> Optional[str]:
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
    """
    One stable string per entity:
    - Prefer strong IDs (by KEY_PRIORITY).
    - Episodes/Seasons: if we have (show-id + S/E), emit that composite.
    - Fallback: type|title|year (only when no IDs are available).
    """
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

def unified_keys_from_ids(idmap: Mapping[str, Any]) -> Set[str]:
    """Return all comparable keys from an ID dict (for joins/aliasing)."""
    out: Set[str] = set()
    for k in ID_KEYS:
        n = _normalize_id(k, idmap.get(k))
        if n:
            out.add(f"{k}:{n}".lower())
    return out

def keys_for_item(item: Mapping[str, Any]) -> Set[str]:
    """
    All keys that can represent this item:
    - ID-based keys (all known),
    - title|year fallback,
    - plus composite show#sXXeYY / show#season:N if data present.
    """
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

# --- Minimal projection (for logs/UI/shadows) --------------------------------

def minimal(item: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Minimal, but TV-safe: keep show_ids for seasons/episodes so downstream
    writers (e.g., SIMKL) can build proper show/season/episode payloads.
    """
    ids = ids_from(item)
    typ = _norm_type(item.get("type"))
    out: Dict[str, Any] = {
        "type": typ,
        "title": item.get("title"),
        "year": item.get("year"),
        "ids": {k: ids[k] for k in ID_KEYS if k in ids},
    }
    # Common optional fields
    for opt in ("watched", "watched_at", "rating", "rated_at", "season", "episode", "series_title"):
        if opt in item:
            out[opt] = item.get(opt)
    # Preserve show-level ids for S/E (normalized + trimmed)
    if typ in ("season", "episode"):
        sids_raw = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else None
        if sids_raw:
            sids = coalesce_ids(sids_raw)
            if sids:
                out["show_ids"] = {k: sids[k] for k in ID_KEYS if k in sids}
    return out

# --- Small helpers ------------------------------------------------------------

def has_external_ids(obj: Mapping[str, Any]) -> bool:
    ids = ids_from(obj) if "ids" in obj or "guid" in obj else obj  # accept either item or ids map
    return any(ids.get(k) for k in ("imdb", "tmdb", "tvdb"))

def preferred_id_key(obj: Mapping[str, Any]) -> Optional[str]:
    """Return 'source:value' for the highest-priority id present (or None)."""
    ids = ids_from(obj) if "ids" in obj or "guid" in obj else obj
    return _best_id_key(ids)  # already lower-cased format
