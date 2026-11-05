# tests/test_kodi_matching.py
# CrossWatch - Kodi media identity matching regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from cw_platform.id_map import canonical_key
from cw_platform import library_presence
from providers.sync.kodi import _history, _presence, _progress, _ratings
from providers.sync.kodi._common import LibraryIndex, resolution_keys
from providers.sync.scrob._common import item_from_media


def library(*, movies=True, episodes=True, show_ids=None, duplicate=False):
    movie = {"movieid": 40, "title": "Animals", "year": 2026,
             "uniqueid": {"tmdb": "1236045", "imdb": "tt31049299"}}
    episode = {"episodeid": 99, "tvshowid": 5, "title": "Episode", "season": 1, "episode": 1,
               "uniqueid": {"tmdb": "1236045"}}
    return LibraryIndex(
        [movie, {**movie, "movieid": 41}] if duplicate else [movie] if movies else [],
        [episode] if episodes else [],
        [{"tvshowid": 5, "uniqueid": show_ids or {"tmdb": "1399", "tvdb": "121361"}}],
    )


@pytest.mark.parametrize("kind, expected", [("movie", 40), ("episode", 99)])
def test_numeric_id_collision_only_matches_same_media_type(kind, expected):
    target, reason = library().resolve({"type": kind, "ids": {"tmdb": "1236045"}})
    assert reason == "ok"
    assert target["_kodi_id"] == expected
    assert target["_kodi_type"] == kind


@pytest.mark.parametrize("kind", ["movie", "episode"])
@pytest.mark.parametrize("feature", [_history, _progress, _ratings])
def test_wrong_media_type_never_reaches_a_write(kind, feature):
    index = library(movies=kind == "episode", episodes=kind == "movie")
    client = Mock()
    adapter = SimpleNamespace(client=client)
    setattr(adapter, "_kodi_library_index_" + feature.__name__.rsplit("_", 1)[-1], index)
    result = feature.add(adapter, [{"type": kind, "ids": {"tmdb": "1236045"}}])
    assert result["count"] == 0
    assert result["reason_counts"] == {"not_found": 1}
    assert not client.mock_calls


def test_same_type_duplicate_remains_ambiguous():
    target, reason = library(duplicate=True).resolve({"type": "movie", "ids": {"tmdb": "1236045"}})
    assert target is None
    assert reason == "ambiguous"


def test_wrong_media_type_is_not_library_presence():
    adapter = SimpleNamespace(_kodi_library_index_history=library(movies=False))
    verdicts = _presence.presence(adapter, [{"type": "movie", "ids": {"tmdb": "1236045"}}])
    assert verdicts == [library_presence.verdict(library_presence.ABSENT, reason=library_presence.REASON)]


@pytest.mark.parametrize("source_ids,kodi_ids", [
    ({"tmdb": "1399", "tvdb": "121361"}, {"tvdb": "121361"}),
    ({"tvdb": "121361"}, {"tmdb": "1399", "tvdb": "121361"}),
    ({"tmdb": "1399", "imdb": "tt0944947"}, {"imdb": "tt0944947"}),
    ({"tmdb": "1399", "tvdb": "121361"}, {"tmdb": "1399", "tvdb": "121361"}),
])
def test_secondary_show_ids_match_without_changing_canonical_key(source_ids, kodi_ids):
    source = {"type": "episode", "show_ids": source_ids, "season": 1, "episode": 1}
    before = canonical_key(source)
    target, reason = library(movies=False, show_ids=kodi_ids).resolve(source)
    assert reason == "ok"
    assert target["_kodi_id"] == 99
    assert canonical_key(source) == before
    assert before in resolution_keys(source)


@pytest.mark.parametrize("season,episode", [(2, 1), (1, 2), (None, 1), (1, None)])
def test_show_id_requires_matching_episode_coordinates(season, episode):
    source = {"type": "episode", "show_ids": {"tvdb": "121361"}, "season": season, "episode": episode}
    assert library(movies=False).resolve(source) == (None, "not_found")


def test_conflicting_show_candidates_remain_ambiguous():
    index = LibraryIndex([], [
        {"episodeid": 1, "tvshowid": 1, "season": 1, "episode": 1},
        {"episodeid": 2, "tvshowid": 2, "season": 1, "episode": 1},
    ], [
        {"tvshowid": 1, "uniqueid": {"tmdb": "1399"}},
        {"tvshowid": 2, "uniqueid": {"tvdb": "121361"}},
    ])
    source = {"type": "episode", "show_ids": {"tmdb": "1399", "tvdb": "121361"}, "season": 1, "episode": 1}
    assert index.resolve(source) == (None, "ambiguous")


def test_scrob_movie_matches_kodi_with_only_imdb():
    source = item_from_media({"type": "movie", "tmdb_id": 1236045, "imdb_id": "tt31049299"})
    index = LibraryIndex([{"movieid": 40, "uniqueid": {"imdb": "tt31049299"}}], [], [])
    target, reason = index.resolve(source)
    assert reason == "ok"
    assert target["_kodi_id"] == 40
