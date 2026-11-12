# /cw_platform/anime_mapping/resolver.py
# CrossWatch - Central Anime Episode Resolver
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass, field
from typing import Any

from .coordinates import translate
from .overrides import find_episode_overrides, find_source_overrides
from .storage import index_ready, normalize_release_tag, query_edges, query_identity_natives, query_native_identity

NUMBERINGS = ("tmdb", "tvdb", "simkl", "anime_entry")
AIRED = ("tmdb", "tvdb")
ENTRY_NAMESPACES = ("anidb", "mal", "anilist", "kitsu")
EDGE_NAMESPACES = ("anidb", "mal", "anilist")
SIMKL_NUMBERING = ("mal", "anidb")
REASONS = (
    "not_anime",
    "no_mapping",
    "ambiguous_mapping",
    "target_season_missing",
    "target_episode_missing",
    "mapping_unavailable",
    "season_split",
    "incomplete_item",
)

Layout = Mapping[int, int | Collection[int]]
LayoutSource = Layout | Callable[[str], Layout | None] | None


@dataclass(frozen=True)
class EntryEpisode:
    namespace: str
    ident: str
    episode: int
    scope: str = ""


@dataclass(frozen=True)
class Coordinate:
    numbering: str
    show: str
    episode: int
    season: int | None = None
    namespace: str = ""


@dataclass(frozen=True)
class Outcome:
    coordinate: Coordinate | None = None
    reason: str = ""
    basis: str = ""
    entries: tuple[EntryEpisode, ...] = field(default_factory=tuple)
    candidates: tuple[Coordinate, ...] = field(default_factory=tuple)

    @property
    def ok(self) -> bool:
        return self.coordinate is not None


def _int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _ids(item: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for key in ("ids", "show_ids"):
        raw = item.get(key)
        if not isinstance(raw, Mapping):
            continue
        for k, v in raw.items():
            text = str(v or "").strip()
            if text:
                out[str(k).strip().lower()] = text
    return out


def _show_ids(item: Mapping[str, Any]) -> dict[str, str]:
    raw = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else item.get("ids")
    return {str(k).strip().lower(): str(v).strip() for k, v in dict(raw or {}).items() if str(v or "").strip()}


def _is_anime_show(tag: str, provider: str, show: str) -> bool:
    for row in query_edges(tag, provider, show):
        if str(row.get("target_provider") or "") in EDGE_NAMESPACES:
            return True
    return False


def _aired_to_entries(tag: str, provider: str, show: str, season: int, episode: int) -> list[EntryEpisode]:
    out: list[EntryEpisode] = []
    for row in query_edges(tag, provider, show, scope=f"s{season}"):
        namespace = str(row.get("target_provider") or "")
        if namespace not in EDGE_NAMESPACES:
            continue
        scope = str(row.get("target_scope") or "")
        if namespace == "anidb" and (scope.upper() == "R") != (season > 0):
            continue
        mapped = translate(row.get("source_range"), row.get("target_range"), episode)
        if mapped is None:
            continue
        out.append(EntryEpisode(namespace, str(row.get("target_id") or ""), int(mapped), scope.upper() if namespace == "anidb" else ""))
    return out


def _simkl_entries(tag: str, item: Mapping[str, Any]) -> list[EntryEpisode]:
    ids = _show_ids(item)
    episode = _int(item.get("_simkl_episode_number"))
    if episode is None:
        episode = _int(item.get("episode"))
    if episode is None or episode <= 0:
        return []
    natives = dict(query_identity_natives(tag, "simkl", ids.get("simkl", ""))) if ids.get("simkl") else {}
    for ns in SIMKL_NUMBERING:
        if ids.get(ns):
            natives.setdefault(ns, ids[ns])
    return [EntryEpisode(ns, natives[ns], episode, "R" if ns == "anidb" else "") for ns in SIMKL_NUMBERING if natives.get(ns)]


def _source_entries(tag: str, item: Mapping[str, Any], source: str, namespace: str) -> tuple[list[EntryEpisode], str, str]:
    ids = _show_ids(item)
    season = _int(item.get("season") if item.get("season") is not None else item.get("season_number"))
    episode = _int(item.get("episode") if item.get("episode") is not None else item.get("episode_number"))
    if source in AIRED:
        show = ids.get(source, "")
        if not show or season is None or season < 0 or episode is None or episode <= 0:
            return [], "incomplete_item", ""
        ruled = find_episode_overrides(ids, season, episode)
        if ruled:
            entries = [EntryEpisode(r.namespace, r.target_id, r.absolute, "R" if r.namespace == "anidb" else "") for r in ruled]
            return entries, "", "user_override"
        if not _is_anime_show(tag, source, show):
            return [], "not_anime", ""
        entries = _aired_to_entries(tag, source, show, season, episode)
        return entries, ("" if entries else "no_mapping"), "mapping"
    if source == "simkl":
        entries = _simkl_entries(tag, item)
        return entries, ("" if entries else "no_mapping"), "simkl_entry"
    ns = namespace if namespace in ENTRY_NAMESPACES else ""
    ident = ids.get(ns, "") if ns else ""
    if not ident or episode is None or episode <= 0:
        return [], "no_mapping", ""
    return [EntryEpisode(ns, ident, episode, "R" if ns == "anidb" else "")], "", "anime_entry"


def _entry_to_aired(tag: str, entry: EntryEpisode, target: str) -> set[Coordinate]:
    ruled = find_source_overrides(entry.namespace, entry.ident, entry.episode)
    hits = {Coordinate(target, r.ident, r.episode, r.season) for r in ruled if r.provider == target}
    if hits:
        return hits
    out: set[Coordinate] = set()
    if entry.namespace not in EDGE_NAMESPACES:
        return out
    for row in query_edges(tag, entry.namespace, entry.ident):
        if entry.namespace == "anidb" and str(row.get("source_scope") or "").upper() != (entry.scope or "R"):
            continue
        if str(row.get("target_provider") or "") != target or str(row.get("target_kind") or "") != "show":
            continue
        scope = str(row.get("target_scope") or "")
        season = _int(scope[1:]) if scope.startswith("s") else None
        if season is None or season < 0:
            continue
        mapped = translate(row.get("source_range"), row.get("target_range"), entry.episode)
        if mapped is None:
            continue
        out.add(Coordinate(target, str(row.get("target_id") or ""), int(mapped), season))
    return out


def _entry_to_entry(tag: str, entry: EntryEpisode, namespace: str) -> set[Coordinate]:
    if entry.namespace == namespace:
        return {Coordinate("anime_entry", entry.ident, entry.episode, None, namespace)}
    if namespace == "kitsu":
        kitsu = query_native_identity(tag, entry.namespace, entry.ident).get("kitsu") if entry.namespace in EDGE_NAMESPACES else ""
        return {Coordinate("anime_entry", kitsu, entry.episode, None, "kitsu")} if kitsu and entry.namespace in SIMKL_NUMBERING else set()
    out: set[Coordinate] = set()
    if entry.namespace not in EDGE_NAMESPACES:
        return out
    for row in query_edges(tag, entry.namespace, entry.ident):
        if entry.namespace == "anidb" and str(row.get("source_scope") or "").upper() != (entry.scope or "R"):
            continue
        if str(row.get("target_provider") or "") != namespace:
            continue
        if namespace == "anidb" and str(row.get("target_scope") or "").upper() != "R":
            continue
        mapped = translate(row.get("source_range"), row.get("target_range"), entry.episode)
        if mapped is not None:
            out.add(Coordinate("anime_entry", str(row.get("target_id") or ""), int(mapped), None, namespace))
    return out


def _entry_to_simkl(tag: str, entry: EntryEpisode) -> set[Coordinate]:
    if entry.namespace == "simkl":
        return {Coordinate("simkl", entry.ident, entry.episode)}
    if entry.namespace not in SIMKL_NUMBERING:
        return set()
    simkl = query_native_identity(tag, entry.namespace, entry.ident).get("simkl")
    return {Coordinate("simkl", simkl, entry.episode)} if simkl else set()


def _targets(tag: str, entries: list[EntryEpisode], target: str, namespace: str) -> set[Coordinate]:
    out: set[Coordinate] = set()
    for entry in entries:
        if target in AIRED:
            out |= _entry_to_aired(tag, entry, target)
        elif target == "simkl":
            out |= _entry_to_simkl(tag, entry)
        else:
            out |= _entry_to_entry(tag, entry, namespace)
    return out


def _episode_listed(season_layout: int | Collection[int], episode: int) -> bool:
    if isinstance(season_layout, bool):
        return False
    if isinstance(season_layout, int):
        return 1 <= episode <= season_layout
    return episode in season_layout


def _layout_for(layout: LayoutSource, show: str) -> Layout | None:
    if layout is None:
        return None
    if callable(layout):
        return layout(show)
    return layout


def resolve_episode(
    item: Mapping[str, Any],
    *,
    source: str,
    target: str,
    source_namespace: str = "",
    target_namespace: str = "",
    layout: LayoutSource = None,
    release_tag: str = "v3",
) -> Outcome:
    if source not in NUMBERINGS or target not in NUMBERINGS:
        raise ValueError(f"unknown numbering {source!r} -> {target!r}")
    tag = normalize_release_tag(release_tag)
    if not index_ready(tag):
        return Outcome(reason="mapping_unavailable")
    entries, reason, basis = _source_entries(tag, item, source, source_namespace)
    if reason:
        return Outcome(reason=reason, basis=basis)
    found = _targets(tag, entries, target, target_namespace)
    if not found:
        return Outcome(reason="no_mapping", basis=basis, entries=tuple(entries))
    if len(found) > 1:
        return Outcome(reason="ambiguous_mapping", basis=basis, entries=tuple(entries), candidates=tuple(sorted(found, key=repr)))
    coord = next(iter(found))
    if target in AIRED and coord.season is not None:
        known = _layout_for(layout, coord.show)
        if known is not None:
            season_layout = known.get(coord.season)
            if season_layout is None:
                return Outcome(reason="target_season_missing", basis=basis, entries=tuple(entries), candidates=(coord,))
            if not _episode_listed(season_layout, coord.episode):
                return Outcome(reason="target_episode_missing", basis=basis, entries=tuple(entries), candidates=(coord,))
    return Outcome(coordinate=coord, basis=basis, entries=tuple(entries))


def _season_episodes(tag: str, provider: str, show: str, season: int) -> list[int]:
    numbers: set[int] = set()
    for row in query_edges(tag, provider, show, scope=f"s{season}"):
        if str(row.get("target_provider") or "") not in EDGE_NAMESPACES:
            continue
        for part in str(row.get("source_range") or "").split(","):
            lo, _, hi = part.partition("-")
            first, last = _int(lo), _int(hi or lo)
            if first is None or last is None or first <= 0 or last < first or last - first > 5000:
                continue
            numbers.update(range(first, last + 1))
    return sorted(numbers)


def resolve_season(
    item: Mapping[str, Any],
    *,
    source: str,
    target: str,
    release_tag: str = "v3",
) -> Outcome:
    if source not in AIRED or target not in AIRED:
        raise ValueError(f"season numbering must be tmdb or tvdb: {source!r} -> {target!r}")
    tag = normalize_release_tag(release_tag)
    if not index_ready(tag):
        return Outcome(reason="mapping_unavailable")
    ids = _show_ids(item)
    show = ids.get(source, "")
    season = _int(item.get("season") if item.get("season") is not None else item.get("season_number"))
    if not show or season is None or season < 0:
        return Outcome(reason="incomplete_item")
    if not _is_anime_show(tag, source, show):
        return Outcome(reason="not_anime")
    episodes = _season_episodes(tag, source, show, season)
    if not episodes:
        return Outcome(reason="no_mapping")
    found: set[tuple[str, int]] = set()
    for episode in episodes:
        out = resolve_episode({"type": "episode", "season": season, "episode": episode, "show_ids": ids},
                              source=source, target=target, release_tag=tag)
        if not out.ok or out.coordinate is None or out.coordinate.season is None:
            return Outcome(reason=out.reason or "no_mapping", basis="season")
        found.add((out.coordinate.show, out.coordinate.season))
    if len(found) != 1:
        return Outcome(reason="season_split", basis="season",
                       candidates=tuple(Coordinate(target, show_id, 0, s) for show_id, s in sorted(found)))
    show_id, target_season = next(iter(found))
    return Outcome(coordinate=Coordinate(target, show_id, 0, target_season), basis="season")
