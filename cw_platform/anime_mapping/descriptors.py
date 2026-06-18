# /cw_platform/anime_mapping/descriptors.py
# CrossWatch - Anime Mapping Descriptor Utilities
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Descriptor:
    raw: str
    provider: str
    id: str
    scope: str = ""
    media_kind: str = ""


_PROVIDER_ALIASES: dict[str, tuple[str, str]] = {
    "anilist": ("anilist", ""),
    "mal": ("mal", ""),
    "anidb": ("anidb", ""),
    "tmdb_show": ("tmdb", "show"),
    "tmdb_movie": ("tmdb", "movie"),
    "tvdb_show": ("tvdb", "show"),
    "tvdb_movie": ("tvdb", "movie"),
    "imdb_show": ("imdb", "show"),
    "imdb_movie": ("imdb", "movie"),
}


def parse_descriptor(value: Any) -> Descriptor | None:
    raw = str(value or "").strip()
    if not raw or ":" not in raw:
        return None
    parts = raw.split(":")
    if len(parts) < 2:
        return None
    base = parts[0].strip().lower()
    ident = parts[1].strip()
    if not base or not ident:
        return None
    provider, media_kind = _PROVIDER_ALIASES.get(base, (base, ""))
    if provider not in {"anilist", "mal", "anidb", "tmdb", "tvdb", "imdb"}:
        return None
    scope = ":".join(parts[2:]).strip() if len(parts) > 2 else ""
    return Descriptor(raw=raw, provider=provider, id=ident, scope=scope, media_kind=media_kind)


def descriptor_candidates_for_id(provider: str, ident: Any, *, media_type: str | None = None) -> list[str]:
    p = str(provider or "").strip().lower()
    v = str(ident or "").strip()
    if not p or not v:
        return []

    typ = str(media_type or "").strip().lower()
    is_movie = typ in {"movie", "movies", "film"}

    if p in {"anilist", "mal", "anidb"}:
        return [f"{p}:{v}"]
    if p == "tmdb":
        order = ["tmdb_movie", "tmdb_show"] if is_movie else ["tmdb_show", "tmdb_movie"]
        return [f"{base}:{v}" for base in order]
    if p == "tvdb":
        order = ["tvdb_movie", "tvdb_show"] if is_movie else ["tvdb_show", "tvdb_movie"]
        return [f"{base}:{v}" for base in order]
    if p == "imdb":
        order = ["imdb_movie", "imdb_show"] if is_movie else ["imdb_show", "imdb_movie"]
        return [f"{base}:{v}" for base in order]
    return []
