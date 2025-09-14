# --------------- Canonical ID mapping helpers for consistent multi-provider matching ---------------
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Iterable, Mapping, Optional, Set, Tuple

_ID_KEYS = ("tmdb", "imdb", "tvdb", "trakt", "simkl", "guid", "plex", "slug")

def _norm_str(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

def unified_keys_from_ids(ids: Mapping[str, object]) -> Set[str]:
    keys: Set[str] = set()
    if not isinstance(ids, Mapping):
        return keys
    for k in _ID_KEYS:
        v = _norm_str(ids.get(k))
        if v:
            keys.add(f"{k.lower()}:{v.lower()}")
    return keys

def title_year_key(item: Mapping[str, object]) -> Optional[str]:
    t = _norm_str(item.get("title"))
    y = _norm_str(item.get("year"))
    typ = _norm_str(item.get("type")) or "movie"
    if not t:
        return None
    return f"{typ.lower()}|title:{t.lower()}|year:{y or ''}"

def keys_for_item(item: Mapping[str, object]) -> Set[str]:
    ids = item.get("ids") or {}
    if not isinstance(ids, Mapping):
        ids = {}
    out = unified_keys_from_ids(ids)
    ty = title_year_key(item)
    if ty:
        out.add(ty)
    return out

def any_key_overlap(a: Iterable[str], b: Iterable[str]) -> bool:
    sa, sb = set(a or []), set(b or [])
    if not sa or not sb:
        return False
    return not sa.isdisjoint(sb)
