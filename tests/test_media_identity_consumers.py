# tests/test_media_identity_consumers.py
# CrossWatch - Movie/show identity in interactive sync, playlists and analyzer fixes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy

import pytest

from api.interactiveSyncAPI import prepare_mapping
from cw_platform.id_map import any_key_overlap, canonical_key
from cw_platform.playlists import PlaylistItem
from cw_platform.playlists_runner import _playlist_item_keys, _typed_baseline
from services.analyzer import _rekey


MOVIE = {"type": "movie", "title": "The Two Towers", "year": 2002, "ids": {"tmdb": "121", "imdb": "tt0167261"}}
SHOW = {"type": "show", "title": "Doctor Who", "year": 1963, "ids": {"tmdb": "121", "imdb": "tt0056751"}}


@pytest.mark.parametrize("original,corrected", [(MOVIE, SHOW), (SHOW, MOVIE)])
def test_interactive_cross_type_correction_blocks_original(original, corrected):
    row = {"item": copy.deepcopy(original), "key": canonical_key(original)}
    key, item, blocks = prepare_mapping(row, copy.deepcopy(corrected))
    assert key == canonical_key(corrected)
    assert item["type"] == corrected["type"]
    assert blocks == [canonical_key(original)]


def test_interactive_same_item_correction_does_not_block():
    row = {"item": copy.deepcopy(SHOW), "key": "imdb:tt0056751"}
    _, _, blocks = prepare_mapping(row, copy.deepcopy(SHOW))
    assert blocks == []


def _playlist_item(item):
    return PlaylistItem(key=canonical_key(item), item=copy.deepcopy(item))


@pytest.mark.parametrize("source,target", [(MOVIE, SHOW), (SHOW, MOVIE)])
def test_playlist_movie_and_show_are_not_the_same_entry(source, target):
    assert not any_key_overlap(_playlist_item_keys(_playlist_item(source)), _playlist_item_keys(_playlist_item(target)))


@pytest.mark.parametrize("item", [MOVIE, SHOW])
def test_playlist_same_item_still_matches(item):
    assert any_key_overlap(_playlist_item_keys(_playlist_item(item)), _playlist_item_keys(_playlist_item(item)))


def test_legacy_playlist_baseline_follows_the_target_show():
    assert _typed_baseline({"tmdb:121", "tmdb:5"}, {"tmdb:121#show": SHOW, "tmdb:5": MOVIE}) == {"tmdb:121#show", "tmdb:5"}


def test_legacy_playlist_baseline_keeps_movie_when_both_types_exist():
    assert _typed_baseline({"tmdb:121"}, {"tmdb:121": MOVIE, "tmdb:121#show": SHOW}) == {"tmdb:121"}


def test_analyzer_rekey_drops_show_suffix_outside_tmdb():
    item = {"type": "show", "ids": {"imdb": "tt0056751"}}
    index = {"tmdb:121#show": item}
    assert _rekey(index, "tmdb:121#show", item) == "imdb:tt0056751"
    assert set(index) == {"imdb:tt0056751"}


def test_analyzer_rekey_adds_show_suffix_for_tmdb_shows():
    item = {"type": "show", "ids": {"tmdb": "121"}}
    index = {"imdb:tt0056751": item}
    assert _rekey(index, "imdb:tt0056751", item) == "tmdb:121#show"


def test_analyzer_rekey_keeps_episode_coordinates():
    item = {"type": "episode", "ids": {"tmdb": "121"}}
    index = {"tmdb:99#s01e02": item}
    assert _rekey(index, "tmdb:99#s01e02", item) == "tmdb:121#s01e02"
