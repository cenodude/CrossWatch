# tests/test_anime_mapping_kind_collision.py
# CrossWatch - Anime ID Mapping keeps TMDB/TVDB/IMDb movie and show numbers apart
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from cw_platform.anime_mapping import storage
from cw_platform.anime_mapping.service import AnimeMappingService

MAPPINGS: dict[str, Any] = {
    "anilist:9001": {"tmdb_movie:603": {}},
    "anilist:9002": {"tmdb_show:604": {}},
    "anilist:9003": {"tmdb_show:605": {}},
    "anilist:9004": {"tmdb_movie:605": {}},
    "anilist:9005": {"tvdb_movie:77": {}},
}
CFG = {"anime_mapping": {"enabled": True, "release_tag": "v3"}}


@pytest.fixture()
def svc(config_base: Path) -> AnimeMappingService:
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    yield AnimeMappingService(CFG)
    storage.close_readers()


def _anilist(svc: AnimeMappingService, ids: dict[str, str], media_type: str | None) -> str | None:
    return svc.enrich_ids(ids, media_type=media_type)["ids"].get("anilist")


def test_show_does_not_take_the_anime_of_a_movie_with_the_same_number(svc: AnimeMappingService) -> None:
    assert _anilist(svc, {"tmdb": "603"}, "show") is None
    assert _anilist(svc, {"tvdb": "77"}, "show") is None
    assert _anilist(svc, {"tmdb": "603"}, "episode") is None


def test_movie_does_not_take_the_anime_of_a_show_with_the_same_number(svc: AnimeMappingService) -> None:
    assert _anilist(svc, {"tmdb": "604"}, "movie") is None


def test_matching_kinds_still_map(svc: AnimeMappingService) -> None:
    assert _anilist(svc, {"tmdb": "603"}, "movie") == "9001"
    assert _anilist(svc, {"tmdb": "604"}, "show") == "9002"
    assert _anilist(svc, {"tvdb": "77"}, "movie") == "9005"


def test_shared_number_picks_the_entry_of_the_right_kind(svc: AnimeMappingService) -> None:
    assert _anilist(svc, {"tmdb": "605"}, "show") == "9003"
    assert _anilist(svc, {"tmdb": "605"}, "movie") == "9004"


def test_unknown_kind_keeps_previous_behaviour(svc: AnimeMappingService) -> None:
    assert _anilist(svc, {"tmdb": "603"}, "anime") == "9001"
    assert _anilist(svc, {"tmdb": "603"}, None) == "9001"
    assert _anilist(svc, {"tmdb": "605"}, "anime") is None
