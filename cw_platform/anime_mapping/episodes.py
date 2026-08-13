# /cw_platform/anime_mapping/episodes.py
# CrossWatch - Anime Mapping Episode Resolver
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable

from .coordinates import covers, translate
from .overrides import find_episode_override, find_source_override
from .storage import normalize_release_tag, query_edges, query_show_pair

NATIVE_ORDER = ("anidb", "mal", "anilist")
AIRED_ORDER = ("tvdb", "tmdb")
REVERSE_NAMESPACES = ("simkl", "anidb", "mal", "anilist", "kitsu")
_ANIDB_REGULAR = "R"


@dataclass(frozen=True)
class Resolution:
    absolute: int
    namespace: str
    target_id: str
    basis: str
    entry: str


@dataclass(frozen=True)
class SourceCoordinate:
    provider: str
    ident: str
    season: int
    episode: int
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

    tag = normalize_release_tag(release_tag)
    ids = _ids_of(item)

    try:
        ruled = find_episode_override(ids, season, episode)
    except Exception:
        ruled = None
    if ruled is not None:
        return Resolution(
            absolute=ruled.absolute,
            namespace=ruled.namespace,
            target_id=ruled.target_id,
            basis="user_override",
            entry=f"override:{ruled.rule_id}",
        )

    if season == 0 and not allow_specials:
        return None

    scope = f"s{season}"

    for provider, ident, basis in _entry_points(ids, tag, resolve_external):
        try:
            rows = query_edges(tag, provider, ident, scope=scope)
        except Exception:
            continue
        found: dict[str, tuple[int, str]] = {}
        for row in rows:
            if str(row.get("source_kind") or "").strip().lower() != "show":
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
    return _native_passthrough(tag, ids, season, episode)


def _native_passthrough(tag: str, ids: Mapping[str, str], season: int, episode: int) -> Resolution | None:
    if season != 1:
        return None
    for namespace in NATIVE_ORDER:
        ident = ids.get(namespace, "")
        if not ident:
            continue
        try:
            rows = query_edges(tag, namespace, ident)
        except Exception:
            continue
        if namespace == "anidb":
            rows = [r for r in rows if str(r.get("source_scope") or "").strip().upper() == _ANIDB_REGULAR]
        if not any(covers(r.get("source_range"), episode) for r in rows):
            continue
        return Resolution(
            absolute=episode,
            namespace=namespace,
            target_id=ident,
            basis="anibridge_native",
            entry=f"{namespace}_native",
        )
    return None


def _scope_season(scope: Any) -> int | None:
    text = str(scope or "").strip().lower()
    if not text.startswith("s") or not text[1:].isdigit():
        return None
    return int(text[1:])


def _override_source_coordinate(ids: Mapping[str, str], absolute: int) -> SourceCoordinate | None:
    for namespace in REVERSE_NAMESPACES:
        ident = ids.get(namespace, "")
        if not ident:
            continue
        try:
            ruled = find_source_override(namespace, ident, absolute)
        except Exception:
            ruled = None
        if ruled is None:
            continue
        return SourceCoordinate(
            provider=ruled.provider,
            ident=ruled.ident,
            season=ruled.season,
            episode=ruled.episode,
            basis="user_override",
            entry=f"override:{ruled.rule_id}",
        )
    return None


def _bridge_source_coordinate(tag: str, ids: Mapping[str, str], absolute: int) -> SourceCoordinate | None:
    found: dict[tuple[str, str], tuple[int, int]] = {}
    for namespace in NATIVE_ORDER:
        ident = ids.get(namespace, "")
        if not ident:
            continue
        try:
            rows = query_edges(tag, namespace, ident)
        except Exception:
            continue
        for row in rows:
            if not int(row.get("reverse") or 0):
                continue
            if namespace == "anidb" and str(row.get("source_scope") or "").strip().upper() != _ANIDB_REGULAR:
                continue
            provider = str(row.get("target_provider") or "").strip().lower()
            if provider not in AIRED_ORDER:
                continue
            if str(row.get("target_kind") or "").strip().lower() != "show":
                continue
            season = _scope_season(row.get("target_scope"))
            if season is None or season <= 0:
                continue
            target_id = str(row.get("target_id") or "").strip()
            if not target_id:
                continue
            episode = translate(row.get("source_range"), row.get("target_range"), absolute)
            if episode is None or episode <= 0:
                continue
            key = (provider, target_id)
            prior = found.get(key)
            if prior is not None and prior != (season, episode):
                return None
            found[key] = (season, episode)

    if not found:
        return None
    coords = set(found.values())
    if len(coords) != 1:
        return None
    season, episode = coords.pop()
    for provider in AIRED_ORDER:
        for (row_provider, target_id), _coord in found.items():
            if row_provider != provider:
                continue
            return SourceCoordinate(
                provider=provider,
                ident=target_id,
                season=season,
                episode=episode,
                basis="anibridge_reverse",
                entry=f"{provider}_reverse",
            )
    return None


def _axis_coordinates(tag: str, ids: Mapping[str, str], absolute: int) -> dict[tuple[str, str], tuple[int, int]]:
    found: dict[tuple[str, str], tuple[int, int] | None] = {}
    for namespace in NATIVE_ORDER:
        ident = ids.get(namespace, "")
        if not ident:
            continue
        try:
            rows = query_edges(tag, namespace, ident)
        except Exception:
            continue
        for row in rows:
            if not int(row.get("reverse") or 0):
                continue
            if namespace == "anidb" and str(row.get("source_scope") or "").strip().upper() != _ANIDB_REGULAR:
                continue
            provider = str(row.get("target_provider") or "").strip().lower()
            if provider not in AIRED_ORDER:
                continue
            if str(row.get("target_kind") or "").strip().lower() != "show":
                continue
            season = _scope_season(row.get("target_scope"))
            if season is None or season <= 0:
                continue
            target_id = str(row.get("target_id") or "").strip()
            if not target_id:
                continue
            episode = translate(row.get("source_range"), row.get("target_range"), absolute)
            if episode is None or episode <= 0:
                continue
            key = (provider, target_id)
            coord = (season, episode)
            if key in found and found[key] != coord:
                found[key] = None
            elif key not in found:
                found[key] = coord
    return {k: v for k, v in found.items() if v is not None}


def resolve_axis_coordinates(
    ids: Mapping[str, Any] | None,
    absolute: Any,
    *,
    release_tag: str = "v3",
) -> set[tuple[int, int]]:
    abs_num = _as_int(absolute)
    if abs_num is None or abs_num <= 0:
        return set()
    clean = {
        str(k).strip().lower(): str(v).strip()
        for k, v in dict(ids or {}).items()
        if str(v or "").strip()
    }
    if not clean:
        return set()
    ruled = _override_source_coordinate(clean, abs_num)
    if ruled is not None:
        return {(ruled.season, ruled.episode)}
    return set(_axis_coordinates(normalize_release_tag(release_tag), clean, abs_num).values())


def resolve_source_coordinate(
    ids: Mapping[str, Any] | None,
    absolute: Any,
    *,
    release_tag: str = "v3",
) -> SourceCoordinate | None:
    abs_num = _as_int(absolute)
    if abs_num is None or abs_num <= 0:
        return None
    clean = {
        str(k).strip().lower(): str(v).strip()
        for k, v in dict(ids or {}).items()
        if str(v or "").strip()
    }
    if not clean:
        return None
    ruled = _override_source_coordinate(clean, abs_num)
    if ruled is not None:
        return ruled
    return _bridge_source_coordinate(normalize_release_tag(release_tag), clean, abs_num)


def resolution_matches_ids(resolution: Resolution | None, ids: Mapping[str, Any] | None) -> bool:
    if resolution is None:
        return False
    value = str((ids or {}).get(resolution.namespace) or "").strip()
    return bool(value) and value == resolution.target_id
