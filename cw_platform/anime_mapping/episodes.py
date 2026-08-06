# /cw_platform/anime_mapping/episodes.py
# CrossWatch - Anime Mapping Episode Resolver
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable

from .coordinates import translate
from .storage import normalize_release_tag, query_edges, query_show_pair

NATIVE_ORDER = ("anidb", "mal", "anilist")
AIRED_ORDER = ("tvdb", "tmdb")
_ANIDB_REGULAR = "R"


@dataclass(frozen=True)
class Resolution:
    absolute: int
    namespace: str
    target_id: str
    basis: str
    entry: str


def _as_int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _ids_of(item: Mapping[str, Any]) -> dict[str, str]:
    src = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else item.get("ids")
    out: dict[str, str] = {}
    for k, v in (src if isinstance(src, Mapping) else {}).items():
        text = str(v or "").strip()
        if text:
            out[str(k).strip().lower()] = text
    return out


def _season_episode(item: Mapping[str, Any]) -> tuple[int, int] | None:
    season = _as_int(item.get("season") if item.get("season") is not None else item.get("season_number"))
    episode = _as_int(item.get("episode") if item.get("episode") is not None else item.get("episode_number"))
    if season is None or season < 0 or episode is None or episode <= 0:
        return None
    return season, episode


def _entry_points(
    ids: Mapping[str, str],
    tag: str,
    resolve_external: Callable[[str, str], dict[str, str]] | None,
) -> list[tuple[str, str, str]]:
    out: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()

    def add(provider: str, ident: str, basis: str) -> None:
        key = (provider, ident)
        if ident and key not in seen:
            seen.add(key)
            out.append((provider, ident, basis))

    for provider in AIRED_ORDER:
        add(provider, ids.get(provider, ""), f"{provider}_direct")

    tmdb = ids.get("tmdb", "")
    if tmdb and not ids.get("tvdb"):
        paired = query_show_pair(tag, "tmdb", tmdb)
        if paired:
            add("tvdb", paired, "show_pair_hop")

    if resolve_external and not out:
        for source in ("imdb", "tmdb"):
            ident = ids.get(source, "")
            if not ident:
                continue
            try:
                found = resolve_external(source, ident) or {}
            except Exception:
                found = {}
            for provider in AIRED_ORDER:
                add(provider, str(found.get(provider) or ""), "external_lookup")
            if out:
                break
    return out


def resolve_absolute(
    item: Mapping[str, Any],
    *,
    release_tag: str = "v3",
    resolve_external: Callable[[str, str], dict[str, str]] | None = None,
    allow_specials: bool = False,
) -> Resolution | None:
    coord = _season_episode(item)
    if coord is None:
        return None
    season, episode = coord
    if season == 0 and not allow_specials:
        return None

    tag = normalize_release_tag(release_tag)
    ids = _ids_of(item)
    scope = f"s{season}"

    for provider, ident, basis in _entry_points(ids, tag, resolve_external):
        try:
            rows = query_edges(tag, provider, ident)
        except Exception:
            continue
        found: dict[str, tuple[int, str]] = {}
        for row in rows:
            if str(row.get("source_kind") or "").strip().lower() != "show":
                continue
            if str(row.get("source_scope") or "").strip().lower() != scope:
                continue
            namespace = str(row.get("target_provider") or "").strip().lower()
            if namespace not in NATIVE_ORDER:
                continue
            if namespace == "anidb":
                target_scope = str(row.get("target_scope") or "").strip().upper()
                if (target_scope == _ANIDB_REGULAR) != (season > 0):
                    continue
            mapped = translate(row.get("source_range"), row.get("target_range"), episode)
            if mapped is None:
                continue
            target_id = str(row.get("target_id") or "").strip()
            if not target_id:
                continue
            prior = found.get(namespace)
            if prior is not None and prior[0] != mapped:
                found[namespace] = (0, "")
                continue
            if prior is None:
                found[namespace] = (mapped, target_id)
        agreed = {hit[0] for hit in found.values() if hit[0] > 0}
        if not agreed:
            continue
        if len(agreed) != 1:
            return None
        absolute = agreed.pop()
        for namespace in NATIVE_ORDER:
            hit = found.get(namespace)
            if hit and hit[0] == absolute:
                return Resolution(
                    absolute=absolute,
                    namespace=namespace,
                    target_id=hit[1],
                    basis="anibridge_absolute",
                    entry=basis,
                )
    return None


def resolution_matches_ids(resolution: Resolution | None, ids: Mapping[str, Any] | None) -> bool:
    if resolution is None:
        return False
    value = str((ids or {}).get(resolution.namespace) or "").strip()
    return bool(value) and value == resolution.target_id
