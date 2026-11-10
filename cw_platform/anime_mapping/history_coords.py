# /cw_platform/anime_mapping/history_coords.py
# CrossWatch - Anime History Coordinate Aliases
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
import threading
import time
from collections.abc import Mapping
from typing import Any, Iterable

from .episodes import resolve_absolute, resolve_axis_coordinate_map, resolve_axis_coordinates
from .overrides import overrides_path
from .storage import index_ready, normalize_release_tag, paths

NATIVE_ANIME_ID_KEYS = ("mal", "anilist", "anidb", "kitsu")
ABSOLUTE_ID_KEYS = ("tmdb", "imdb", "mal", "anilist", "anidb", "kitsu", "simkl")
ABSOLUTE_FRAGMENT = "#abs:"
_MIN_ABSOLUTE_RUN = 2
_MAX_DUPLICATE_RATIO = 0.98
_MAX_ORDINARY_SEASON = 50
ALIAS_FIELD = "_cw_anime_aliases"
_ALIAS_TOKEN = re.compile(r"(?:tvdb|tmdb):[^#\s]+#s\d{2,}e\d{2,}")
_ALIAS_CACHE: dict[str, Any] = {"key": None, "rows": None}
_ALIAS_LOCK = threading.Lock()
ALIAS_MISS_SECONDS = 300.0


def coordinate_tokens(item: Mapping[str, Any]) -> set[str]:
    rows = item.get(ALIAS_FIELD)
    if not isinstance(rows, list):
        return set()
    return {row for row in rows if isinstance(row, str) and _ALIAS_TOKEN.fullmatch(row)}


def alias_release_tag(cfg: Mapping[str, Any] | None) -> str:
    block = cfg.get("anime_mapping") if isinstance(cfg, Mapping) else None
    options = cfg.get("_cw_pair_feature_options") if isinstance(cfg, Mapping) else None
    if not isinstance(block, Mapping) or not block.get("enabled"):
        return ""
    if isinstance(options, Mapping) and options.get("use_anime_mapping") is False:
        return ""
    return str(block.get("release_tag") or "v3")


def alias_context(cfg: Mapping[str, Any] | None) -> tuple[str, dict[Any, Any] | None]:
    tag = alias_release_tag(cfg)
    if not tag:
        return "", None
    try:
        files = paths(tag)
        key = tuple((str(p), p.stat().st_mtime_ns, p.stat().st_size) if p.exists() else str(p)
                    for p in (files["db"], files["state"], overrides_path()))
        with _ALIAS_LOCK:
            if _ALIAS_CACHE["key"] != key:
                _ALIAS_CACHE.update(key=key, rows={} if index_ready(tag) else None)
            return tag, _ALIAS_CACHE["rows"]
    except Exception:
        return tag, None


def _resolve_aliases(tag: str, rows: dict[Any, Any], item: Mapping[str, Any], fresh: bool) -> list[str]:
    ids = {str(k): str(v) for k, v in (item.get("show_ids") or {}).items() if v}
    axes = [(p, i) for p, i in ids.items() if p in ("tvdb", "tmdb")]
    if not axes:
        return []
    own = (int(item["season"]), int(item["episode"]))
    key = (tuple(sorted(ids.items())), own)
    cached = rows.get(key)
    if fresh or cached is None or cached[1] < time.monotonic():
        hit = resolve_absolute(item, release_tag=tag)
        coords = resolve_axis_coordinate_map({hit.namespace: hit.target_id}, hit.absolute, release_tag=tag) if hit else {}
        tokens = sorted(f"{p}:{i}#s{s:02d}e{e:02d}".lower() for (p, i), (s, e) in coords.items())
        valid = all(coords.get(axis) == own for axis in axes) and len(coords) == len({p for p, _ in coords}) > 1
        rows[key] = (tokens, float("inf")) if valid else ([], time.monotonic() + ALIAS_MISS_SECONDS)
    return rows[key][0]


def current_aliases(item: Mapping[str, Any], context: tuple[str, dict[Any, Any] | None], fresh: bool = False) -> list[str]:
    tag, rows = context
    if not tag or str(item.get("type") or "").strip().lower() != "episode":
        return []
    saved = sorted(coordinate_tokens(item))
    if rows is None:
        return saved
    try:
        return _resolve_aliases(tag, rows, item, fresh)
    except Exception:
        return saved


def tmdb_coordinate(item: Mapping[str, Any], context: tuple[str, dict[Any, Any] | None]) -> tuple[str, int, int] | None:
    tag, rows = context
    if not tag or str(item.get("type") or "").strip().lower() != "episode":
        return None
    ids = show_ids_of(item)
    order = str(item.get("_cw_episode_order") or "").strip().lower()
    if order and order not in ("tvdb", "tmdb"):
        return None
    axes = [axis for axis in ("tvdb", "tmdb") if ids.get(axis)]
    if rows is not None:
        own = _episode_coordinate(item)
        if own is None or order and not ids.get(order):
            return None
        key = ("tmdb_coordinate", order, tuple(sorted(ids.items())), own)
        cached = rows.get(key)
        if cached is None or cached[1] < time.monotonic():
            candidates = set()
            try:
                for source in [order] if order else axes or [""]:
                    selected = {k: v for k, v in ids.items() if k not in ("tvdb", "tmdb") or k == source}
                    hit = resolve_absolute({**item, "show_ids": selected}, release_tag=tag)
                    if hit is None:
                        continue
                    native = {hit.namespace: hit.target_id}
                    if source:
                        original = resolve_axis_coordinate_map(native, hit.absolute, release_tag=tag, target_provider=source)
                        if original.get((source, ids[source])) != own:
                            continue
                    coords = resolve_axis_coordinate_map(native, hit.absolute, release_tag=tag, target_provider="tmdb")
                    candidates.update((ident, season, episode) for (_, ident), (season, episode) in coords.items())
                value = candidates.pop() if len(candidates) == 1 else None
            except Exception:
                value = None
            cached = (value, float("inf") if value else time.monotonic() + ALIAS_MISS_SECONDS)
            rows[key] = cached
        return cached[0]
    candidates = set()
    for token in current_aliases(item, context):
        provider, _, rest = token.partition(":")
        show, _, numbers = rest.partition("#s")
        season, _, episode = numbers.partition("e")
        if provider == "tmdb":
            candidates.add((show, int(season), int(episode)))
    return candidates.pop() if len(candidates) == 1 else None


def _as_int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _is_episode(item: Mapping[str, Any]) -> bool:
    return str(item.get("type") or "").strip().lower() == "episode"


def show_ids_of(item: Mapping[str, Any]) -> dict[str, str]:
    raw = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else item.get("ids")
    out: dict[str, str] = {}
    for key, value in (raw if isinstance(raw, Mapping) else {}).items():
        text = str(value or "").strip()
        if text:
            out[str(key).strip().lower()] = text
    return out


def show_tokens(item: Mapping[str, Any]) -> set[str]:
    return {f"{k}:{v.lower()}" for k, v in show_ids_of(item).items()}


def absolute_show_tokens(item: Mapping[str, Any]) -> set[str]:
    return {f"{k}:{v.lower()}" for k, v in show_ids_of(item).items() if k in ABSOLUTE_ID_KEYS}


def native_anime_absolute(item: Mapping[str, Any]) -> int | None:
    if not _is_episode(item):
        return None
    absolute = _as_int(item.get("_simkl_episode_number"))
    if absolute is None or absolute <= 0:
        return None
    ids = show_ids_of(item)
    if not any(ids.get(key) for key in NATIVE_ANIME_ID_KEYS):
        return None
    return absolute


def _episode_coordinate(item: Mapping[str, Any]) -> tuple[int, int] | None:
    season = _as_int(item.get("season") if item.get("season") is not None else item.get("season_number"))
    episode = _as_int(item.get("episode") if item.get("episode") is not None else item.get("episode_number"))
    if season is None or episode is None or season < 0 or episode <= 0:
        return None
    return season, episode


def _absolute_runs_in(index: Mapping[str, Any] | None) -> dict[str, tuple[set[int], set[int]]]:
    per_show: dict[str, list[tuple[int, int]]] = {}
    for item in (index or {}).values():
        if not isinstance(item, Mapping) or not _is_episode(item):
            continue
        coord = _episode_coordinate(item)
        if coord is None or coord[0] <= 0:
            continue
        for token in absolute_show_tokens(item):
            per_show.setdefault(token, []).append(coord)

    out: dict[str, tuple[set[int], set[int]]] = {}
    for token, coords in per_show.items():
        numbers = [episode for _, episode in coords]
        if len(numbers) < _MIN_ABSOLUTE_RUN:
            continue
        unique = set(numbers)
        if len(unique) < len(numbers) * _MAX_DUPLICATE_RATIO:
            continue
        if min(unique) != 1 or max(unique) != len(unique):
            continue
        seasons = {season for season, _ in coords}
        if len(seasons) < 2 and max(unique) <= _MAX_ORDINARY_SEASON:
            continue
        counts: dict[int, int] = {}
        for number in numbers:
            counts[number] = counts.get(number, 0) + 1
        unambiguous = {n for n, c in counts.items() if c == 1}
        if unambiguous:
            out[token] = (unambiguous, seasons)
    return out


def absolute_numbered_runs(*indexes: Mapping[str, Any] | None) -> dict[str, tuple[set[int], set[int]]]:
    out: dict[str, tuple[set[int], set[int]]] = {}
    for index in indexes:
        for token, (numbers, seasons) in _absolute_runs_in(index).items():
            merged = out.setdefault(token, (set(), set()))
            merged[0].update(numbers)
            merged[1].update(seasons)
    return out


def absolute_numbered_shows(*indexes: Mapping[str, Any] | None) -> dict[str, set[int]]:
    return {token: numbers for token, (numbers, _) in absolute_numbered_runs(*indexes).items()}


class HistoryCoordinateAliases:
    def __init__(
        self,
        *indexes: Mapping[str, Any] | None,
        release_tag: str = "v3",
        enabled: bool = True,
    ) -> None:
        self.enabled = bool(enabled)
        self.release_tag = normalize_release_tag(release_tag)
        self._absolute_runs: dict[str, tuple[set[int], set[int]]] = absolute_numbered_runs(*indexes) if self.enabled else {}
        self._coord_cache: dict[tuple[str, int], frozenset[tuple[int, int]]] = {}
        self._ready: bool | None = None

    @classmethod
    def disabled(cls) -> "HistoryCoordinateAliases":
        return cls(enabled=False)

    def _mapping_ready(self) -> bool:
        if self._ready is None:
            try:
                self._ready = index_ready(self.release_tag)
            except Exception:
                self._ready = False
        return bool(self._ready)

    def _axis_coordinates(self, ids: Mapping[str, str], absolute: int) -> frozenset[tuple[int, int]]:
        key = ("|".join(f"{k}={ids[k]}" for k in sorted(ids)), absolute)
        cached = self._coord_cache.get(key)
        if cached is None:
            try:
                found = resolve_axis_coordinates(ids, absolute, release_tag=self.release_tag)
            except Exception:
                found = set()
            cached = frozenset(found)
            self._coord_cache[key] = cached
        return cached

    def stats(self) -> dict[str, int]:
        return {"absolute_shows": len(self._absolute_runs), "coord_cache": len(self._coord_cache)}

    def tokens(self, item: Mapping[str, Any]) -> set[str]:
        if not self.enabled or not isinstance(item, Mapping) or not _is_episode(item):
            return set()

        tokens: set[str] = coordinate_tokens(item)
        prefixes = show_tokens(item)
        if not prefixes:
            return tokens

        absolute = native_anime_absolute(item)
        if absolute is not None:
            tokens.update(f"{p}{ABSOLUTE_FRAGMENT}{absolute}" for p in absolute_show_tokens(item))
            if self._mapping_ready():
                for season, episode in self._axis_coordinates(show_ids_of(item), absolute):
                    frag = f"#s{season:02d}e{episode:02d}"
                    tokens.update(f"{p}{frag}" for p in prefixes)
            return tokens

        coord = _episode_coordinate(item)
        if coord is not None and coord[0] > 0:
            for prefix in prefixes:
                numbers, seasons = self._absolute_runs.get(prefix, ((), ()))
                if coord[0] in seasons and coord[1] in numbers:
                    tokens.add(f"{prefix}{ABSOLUTE_FRAGMENT}{coord[1]}")

        return tokens


def build_history_coordinate_aliases(
    cfg: Mapping[str, Any] | None,
    feature: Any,
    indexes: Iterable[Mapping[str, Any] | None] = (),
) -> HistoryCoordinateAliases:
    if str(feature or "").strip().lower() != "history":
        return HistoryCoordinateAliases.disabled()
    block = cfg.get("anime_mapping") if isinstance(cfg, Mapping) else {}
    block = block if isinstance(block, Mapping) else {}
    if not bool(block.get("enabled", False)):
        return HistoryCoordinateAliases.disabled()
    return HistoryCoordinateAliases(
        *list(indexes),
        release_tag=str(block.get("release_tag") or "v3"),
        enabled=True,
    )
