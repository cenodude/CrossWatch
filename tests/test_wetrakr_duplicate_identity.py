# tests/test_wetrakr_duplicate_identity.py
# CrossWatch - WeTrakr indexes keep going when two entries share a media identity
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace

import pytest

from cw_platform.id_map import canonical_key
from providers.sync.wetrakr import _common


MOVIE = {"type": "movie", "id": 95, "title": "The Two Towers", "ids": {"tmdb": 121, "imdb": "tt0167261"}}
SHOW = {"type": "show", "id": 1390810, "title": "Doctor Who", "ids": {"tmdb": 121, "imdb": "tt0056751"}}
SHOW_COPY = {**SHOW, "id": 1390811}


@pytest.fixture
def warnings(monkeypatch):
    seen = []
    monkeypatch.setattr(_common, "log", lambda *args, **fields: seen.append((args, fields)))
    return seen


def _watchlist(monkeypatch, rows):
    from providers.sync.wetrakr import _watchlist

    monkeypatch.setattr(_watchlist, "tracking_rows", lambda *a, **kw: rows)
    monkeypatch.setattr(_watchlist, "log", lambda *a, **kw: None)
    return _watchlist.build_index.__wrapped__(SimpleNamespace(config={}))


def test_duplicate_watchlist_entry_is_skipped_with_warning(monkeypatch, warnings):
    index = _watchlist(monkeypatch, {"movie": [], "show": [SHOW, SHOW_COPY]})
    assert [row["ids"]["wetrakr"] for row in index.values()] == ["1390810"]
    assert len(warnings) == 1
    args, fields = warnings[0]
    assert args == ("WETRAKR", "watchlist", "warn", "duplicate_media_identity")
    assert fields == {"key": canonical_key(SHOW_COPY), "media_id": "1390811"}


def test_movie_and_show_with_same_tmdb_do_not_stop_the_watchlist(monkeypatch, warnings):
    index = _watchlist(monkeypatch, {"movie": [MOVIE], "show": [SHOW]})
    assert index["tmdb:121"]["type"] == "movie"
    assert index["tmdb:121"]["ids"]["wetrakr"] == "95"
    assert len(index) + len(warnings) == 2


def test_duplicate_rating_is_skipped_with_warning(monkeypatch, warnings):
    from providers.sync.wetrakr import _ratings

    rated = [{**row, "interactions": {"user": {"rating": {"rating": value}}}} for row, value in ((SHOW, 8), (SHOW_COPY, 6))]
    monkeypatch.setattr(_ratings, "tracking_rows", lambda *a, **kw: {"show": rated})
    monkeypatch.setattr(_ratings, "log", lambda *a, **kw: None)
    index = _ratings.build_index.__wrapped__(SimpleNamespace(config={}))
    assert [row["rating"] for row in index.values()] == [8.0]
    assert [args[1] for args, _ in warnings] == ["ratings"]


def test_duplicate_progress_entry_is_skipped_with_warning(monkeypatch, warnings):
    from providers.sync.wetrakr import _progress

    playback = {"progress_percent": 40, "runtime_seconds": 7200, "status": "paused", "tracked_at": "2026-01-01T00:00:00Z"}
    movie_copy = {**MOVIE, "id": 96}
    rows = {"movie": [{**MOVIE, "playback": playback}, {**movie_copy, "playback": playback}], "episode": []}
    monkeypatch.setattr(_progress, "pages", lambda adapter, path, **kw: rows["movie" if path.endswith("/movies") else "episode"])
    monkeypatch.setattr(_progress, "log", lambda *a, **kw: None)
    monkeypatch.setattr(_progress, "raise_if_cancelled", lambda: None)
    index = _progress.build_index.__wrapped__(SimpleNamespace(config={}))
    assert [row["ids"]["wetrakr"] for row in index.values()] == ["95"]
    assert [args[1] for args, _ in warnings] == ["progress"]
