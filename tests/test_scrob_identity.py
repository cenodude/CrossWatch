# tests/test_scrob_identity.py
# CrossWatch - Scrob external identifier preservation tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import pytest

from providers.scrobble.scrob.watch import _ids_from_media
from providers.sync.scrob import _history, _progress, _ratings
from providers.sync.scrob._common import item_from_media, media_show_ids, media_to_ids


MOVIE = {"type": "movie", "tmdb_id": 1236045, "imdb_id": "tt31049299", "tvdb_id": 7}
EPISODE = {"type": "episode", "tmdb_id": 63056, "imdb_id": "tt1480055", "tvdb_id": 3254641,
           "show_tmdb_id": 1399, "show_imdb_id": "tt0944947", "show_tvdb_id": 121361,
           "season_number": 1, "episode_number": 1}


def test_history_retains_ids_and_existing_event_key():
    item = _history._row_to_minimal({"media": MOVIE, "watched_at": "2026-01-01T00:00:00Z"})
    assert item["ids"] == {"tmdb": "1236045", "imdb": "tt31049299", "tvdb": "7"}
    assert _history._event_key(item) == "tmdb:1236045@1767225600"


def test_episode_and_show_ids_are_separate():
    item = item_from_media(EPISODE)
    assert item["ids"] == {"tmdb": "63056", "imdb": "tt1480055", "tvdb": "3254641"}
    assert item["show_ids"] == {"tmdb": "1399", "imdb": "tt0944947", "tvdb": "121361"}


def test_progress_preserves_movie_ids():
    item = _progress._row_to_minimal({"media": MOVIE, "progress_percent": 0.5, "progress_seconds": 1800})
    assert item["ids"] == media_to_ids(MOVIE)
    assert item["progress_percent"] == 50


@pytest.mark.parametrize("value", [None, "", "unknown", "0", -1, True, "tt0", "ttinvalid"])
def test_invalid_ids_are_not_preserved(value):
    assert media_to_ids({"tmdb_id": value, "tvdb_id": value, "imdb_id": value}) == {}


def test_watcher_preserves_movie_ids():
    assert _ids_from_media(MOVIE, "movie") == media_to_ids(MOVIE)


def test_watcher_namespaces_episode_and_show_ids():
    ids = _ids_from_media(EPISODE, "episode")
    assert ids == {**{k + "_episode": v for k, v in media_to_ids(EPISODE).items()},
                   **{k + "_show": v for k, v in media_show_ids(EPISODE).items()}}


@pytest.mark.parametrize("kind,season", [("movie", None), ("series", None), ("series", 2)])
def test_ratings_preserve_supplied_ids(kind, season):
    item = _ratings._row_to_minimal(None, {"media": {**MOVIE, "type": kind}, "rating": 8, "season_number": season}, {})
    assert item["ids"] == media_to_ids(MOVIE)
    if season is not None:
        assert item["show_ids"] == media_to_ids(MOVIE)


def test_episode_ratings_preserve_show_ids(monkeypatch):
    monkeypatch.setattr(_ratings, "_episode_context", lambda *_: EPISODE)
    item = _ratings._row_to_minimal(None, {"media": EPISODE, "rating": 8}, {})
    assert item["ids"] == media_to_ids(EPISODE)
    assert item["show_ids"] == media_show_ids(EPISODE)
