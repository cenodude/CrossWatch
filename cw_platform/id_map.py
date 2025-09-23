# --------------- Canonical ID mapping helpers for consistent multi-provider matching ---------------
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, Mapping, Optional, Set, Tuple

# Keep aligned with orchestrator/_mod_* keys; SIMKL is included for convenience (last in priority).
_ID_KEYS: Tuple[str, ...] = ("tmdb", "imdb", "tvdb", "trakt", "plex", "guid", "slug", "simkl")
_KEY_PRIORITY: Tuple[str, ...] = ("tmdb", "imdb", "tvdb", "trakt", "plex", "guid", "slug", "simkl")


# -------------------- normalization --------------------
def _norm_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _normalize_id(key: str, val: Any) -> Optional[str]:
    """Normalize provider IDs to stable, comparable forms."""
    k = (key or "").lower().strip()
    s = _norm_str(val)
    if not s:
        return None

    if k in ("tmdb", "tvdb", "trakt", "simkl"):
        # Numeric identifiers → digits only
        digits = re.sub(r"\D+", "", s)
        return digits or None

    if k == "imdb":
        s = s.lower()
        if s.startswith("tt") and re.search(r"\d", s):
            return s
        digits = re.sub(r"\D+", "", s)
        return f"tt{digits}" if digits else None

    if k == "slug":
        return s.lower()

    if k == "plex":
        # Often a numeric ratingKey → digits only
        digits = re.sub(r"\D+", "", s)
        return digits or None

    if k == "guid":
        # Free-form. Keep trimmed; canonical keys will lower-case.
        return s

    return s


# -------------------- GUID parsing (Plex agent URIs → ids) --------------------
_GUID_PATTERNS = (
    # com.plexapp agents
    (re.compile(r"com\.plexapp\.agents\.imdb://(?P<imdb>tt\d+)", re.I), "imdb"),
    (re.compile(r"com\.plexapp\.agents\.themoviedb://(?P<tmdb>\d+)", re.I), "tmdb"),
    (re.compile(r"com\.plexapp\.agents\.thetvdb://(?P<tvdb>\d+)", re.I), "tvdb"),
    # newer plex:// scheme (we still preserve GUID as-is)
    (re.compile(r"plex://", re.I), "guid"),
    # generic agents
    (re.compile(r"imdb://(?P<imdb>tt\d+)", re.I), "imdb"),
    (re.compile(r"tmdb://(?P<tmdb>\d+)", re.I), "tmdb"),
    (re.compile(r"tvdb://(?P<tvdb>\d+)", re.I), "tvdb"),
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


# -------------------- ids helpers (public API) --------------------
def coalesce_ids(*many: Mapping[str, Any]) -> Dict[str, str]:
    """Merge several 'ids' mappings into one normalized dict."""
    out: Dict[str, str] = {}
    for ids in many:
        if not isinstance(ids, Mapping):
            continue
        for k in _ID_KEYS:
            v = _normalize_id(k, ids.get(k))
            if v:
                out[k] = v
    return out


def ids_from(item: Mapping[str, Any]) -> Dict[str, str]:
    """Collect and normalize IDs from an item (top-level + ids + GUID-derived)."""
    base = item.get("ids") if isinstance(item.get("ids"), Mapping) else {}
    top_level: Dict[str, Any] = {k: item.get(k) for k in _ID_KEYS if item.get(k) is not None}
    guid_val = item.get("guid") or (base.get("guid") if isinstance(base, Mapping) else None)
    guid_ids = ids_from_guid(str(guid_val) if guid_val else None)
    return coalesce_ids(top_level, base or {}, guid_ids)


def any_id(item: Mapping[str, Any], *keys: str) -> Optional[str]:
    """
    Return the first matching ID value from the item.
    Order: provided keys (if any), else _KEY_PRIORITY.
    """
    idmap = ids_from(item)
    order = tuple(k.lower() for k in keys) if keys else _KEY_PRIORITY
    for k in order:
        v = idmap.get(k)
        if v:
            return v
    return None


def merge_ids(primary: Mapping[str, Any], secondary: Mapping[str, Any]) -> Dict[str, str]:
    """
    Merge two id dicts into a normalized set:
    - Keep normalized values from 'primary' when present.
    - Fill gaps from 'secondary'.
    - Never emit empty/None values.
    """
    p = {k: _normalize_id(k, primary.get(k)) for k in _ID_KEYS}
    s = {k: _normalize_id(k, secondary.get(k)) for k in _ID_KEYS}
    out: Dict[str, str] = {}
    for k in _ID_KEYS:
        if p.get(k):
            out[k] = p[k]  # type: ignore[assignment]
        elif s.get(k):
            out[k] = s[k]  # type: ignore[assignment]
    return out


# -------------------- canonical keys --------------------
def title_year_key(item: Mapping[str, Any]) -> Optional[str]:
    t = _norm_str(item.get("title"))
    y = _norm_str(item.get("year")) or ""
    typ = (_norm_str(item.get("type")) or "movie").lower()
    if not t:
        return None
    return f"{typ}|title:{t.lower()}|year:{y}"


def canonical_key(item: Mapping[str, Any]) -> str:
    """
    Pick a single stable key:
    - Prefer IDs by priority.
    - Fallback to type|title|year.
    """
    idmap = ids_from(item)
    for k in _KEY_PRIORITY:
        v = idmap.get(k)
        if v:
            return f"{k}:{v}".lower()
    ty = title_year_key(item)
    return ty or "unknown:"


def unified_keys_from_ids(idmap: Mapping[str, Any]) -> Set[str]:
    """Build the full set of comparable keys from an id dict."""
    keys: Set[str] = set()
    if not isinstance(idmap, Mapping):
        return keys
    for k in _ID_KEYS:
        v = _normalize_id(k, idmap.get(k))
        if v:
            keys.add(f"{k}:{v}".lower())
    return keys


def keys_for_item(item: Mapping[str, Any]) -> Set[str]:
    """Return all comparable keys for an item (IDs + title/year)."""
    idmap = ids_from(item)
    out = unified_keys_from_ids(idmap)
    ty = title_year_key(item)
    if ty:
        out.add(ty)
    return out


# -------------------- comparisons --------------------
def any_key_overlap(a: Iterable[str], b: Iterable[str]) -> bool:
    """True if any comparable key intersects."""
    sa, sb = set(a or []), set(b or [])
    if not sa or not sb:
        return False
    return not sa.isdisjoint(sb)


# -------------------- minimal/projection (optional helper) --------------------
def minimal(item: Mapping[str, Any]) -> Dict[str, Any]:
    """Compact, normalized projection; safe for logs and UIs."""
    ids = ids_from(item)
    out = {
        "ids": {k: ids[k] for k in _ID_KEYS if k in ids},
        "title": item.get("title"),
        "year": item.get("year"),
        "type": (_norm_str(item.get("type")) or "movie").lower(),
    }
    if item.get("rating") is not None:
        out["rating"] = item.get("rating")
    if item.get("rated_at") is not None:
        out["rated_at"] = item.get("rated_at")
    if item.get("watched_at") is not None:
        out["watched_at"] = item.get("watched_at")
    return out


__all__ = [
    "_ID_KEYS",
    "canonical_key",
    "ids_from",
    "any_id",
    "merge_ids",
    # Extras kept for convenience
    "unified_keys_from_ids",
    "title_year_key",
    "keys_for_item",
    "any_key_overlap",
    "coalesce_ids",
    "ids_from_guid",
    "minimal",
]
