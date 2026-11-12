# tests/test_anime_declared_order.py
# CrossWatch - Declared source episode numbering in anime history matching
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from cw_platform.anime_mapping import storage
from cw_platform.anime_mapping.history_coords import HistoryCoordinateAliases
from cw_platform.id_map import minimal

KAI: dict[str, Any] = {
    "anidb:10452:R": {"anilist:20635": {"1-61": "1-61"}, "mal:22777": {"1-61": "1-61"}, "tmdb_show:61709:s2": {"1-26": "1-26"}, "tvdb_show:88031:s2": {"1-61": "1-61"}},
    "anidb:6399:R": {"anilist:6033": {"1-97": "1-97"}, "mal:6033": {"1-97": "1-97"}, "tmdb_show:61709:s1": {"1-26": "1-26"}, "tvdb_show:88031:s1": {"1-97": "1-97"}},
    "anilist:20635": {"anidb:10452:R": {"1-61": "1-61"}, "tmdb_show:61709:s5": {"9-43": "1-35"}, "tmdb_show:61709:s6": {"44-69": "1-26"}, "tvdb_show:88031:s2": {"9-69": "1-61"}},
    "mal:22777": {"anidb:10452:R": {"1-61": "1-61"}, "tmdb_show:61709:s5": {"1-35": "1-35"}, "tmdb_show:61709:s6": {"36-61": "1-26"}, "tvdb_show:88031:s2": {"1-61": "1-61"}},
    "anilist:6033": {"anidb:6399:R": {"1-97": "1-97"}, "mal:6033": {"1-97": "1-97"}, "tmdb_show:61709:s1": {"1-26": "1-26"}, "tmdb_show:61709:s2": {"27-52": "1-26"}, "tmdb_show:61709:s3": {"53-77": "1-25"}, "tmdb_show:61709:s4": {"78-97": "1-20"}, "tvdb_show:88031:s1": {"1-97": "1-97"}},
    "tvdb_show:88031:s1": {"anidb:6399:R": {"1-97": "1-97"}, "anilist:6033": {"1-97": "1-97"}, "mal:6033": {"1-97": "1-97"}},
    "tvdb_show:88031:s2": {"anidb:10452:R": {"1-61": "1-61"}, "anilist:20635": {"1-61": "9-69"}, "mal:22777": {"1-61": "1-61"}},
}
KAI_IDS = {"tvdb": "88031", "tmdb": "61709", "imdb": "tt1409055"}


def _plex_ep(season: int, episode: int, order: str = "tvdb") -> dict[str, Any]:
    return {"type": "episode", "season": season, "episode": episode, "watched_at": "2025-01-01T12:00:00Z",
            "show_ids": dict(KAI_IDS), "ids": {}, "series_title": "Dragon Ball Z Kai", "_cw_episode_order": order}


@pytest.fixture()
def kai_index(config_base: Path) -> Path:
    pp = storage.paths("v3")
    pp["root"].mkdir(parents=True, exist_ok=True)
    pp["mappings"].write_bytes(json.dumps(KAI).encode("utf-8"))
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    return config_base


def test_minimal_keeps_the_declared_order() -> None:
    assert minimal(_plex_ep(1, 32))["_cw_episode_order"] == "tvdb"


def test_fragment_ids_are_limited_only_when_the_episode_translates(kai_index: Path) -> None:
    aliases = HistoryCoordinateAliases(release_tag="v3")
    assert aliases.fragment_show_ids(_plex_ep(1, 32), KAI_IDS) == {"tvdb": "88031"}
    assert aliases.fragment_show_ids(_plex_ep(3, 1), KAI_IDS) == KAI_IDS
    assert aliases.fragment_show_ids({"type": "episode", "season": 1, "episode": 32}, KAI_IDS) == KAI_IDS


def test_fragment_ids_are_untouched_when_anime_mapping_is_off(kai_index: Path) -> None:
    off = HistoryCoordinateAliases.disabled()
    assert off.fragment_show_ids(_plex_ep(1, 32), KAI_IDS) == KAI_IDS
    assert off.tokens(_plex_ep(1, 32)) == set()


def test_history_aliases_add_the_translated_coordinate(kai_index: Path) -> None:
    aliases = HistoryCoordinateAliases(release_tag="v3")
    tokens = aliases.tokens(_plex_ep(1, 32))
    assert "tmdb:61709#s02e06" in tokens
    assert "tmdb:61709#s01e32" not in tokens
