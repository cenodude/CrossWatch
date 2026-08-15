# /cw_platform/anime_mapping/history_coords.py
# CrossWatch - Anime History Coordinate Aliases
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Iterable

from .episodes import resolve_axis_coordinates
from .storage import index_ready, normalize_release_tag

NATIVE_ANIME_ID_KEYS = ("mal", "anilist", "anidb", "kitsu")
ABSOLUTE_FRAGMENT = "#abs:"
_MIN_ABSOLUTE_RUN = 2
_MAX_DUPLICATE_RATIO = 0.98
_MAX_ORDINARY_SEASON = 50


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
        for token in show_tokens(item):
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

        tokens: set[str] = set()
        prefixes = show_tokens(item)
        if not prefixes:
            return tokens

        absolute = native_anime_absolute(item)
        if absolute is not None:
            tokens.update(f"{p}{ABSOLUTE_FRAGMENT}{absolute}" for p in prefixes)
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
