# tests/test_trakt_season_ratings.py
# CrossWatch - Trakt season ratings from sources without season-level ids
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from providers.sync.flicklist._common import read_minimal
from providers.sync.trakt import _ratings as trakt_ratings
from providers.sync.trakt._ratings import _bucketize_for_upsert


class _Session:
    def __init__(self) -> None:
        self.posts: list[dict[str, Any]] = []

    def post(self, url: str, *, headers: Any = None, json: Any = None, timeout: Any = None) -> Any:
        self.posts.append(json)
        return SimpleNamespace(status_code=200, json=lambda: {"deleted": {"seasons": 1, "episodes": 1}}, text="")


def _remove(monkeypatch: pytest.MonkeyPatch, items: list[dict[str, Any]]) -> tuple[_Session, int]:
    session = _Session()
    monkeypatch.setattr(trakt_ratings, "headers_for_adapter", lambda _adapter: {})
    monkeypatch.setattr(trakt_ratings, "_load_cache_doc", lambda: {"items": {}, "wm": {}})
    monkeypatch.setattr(trakt_ratings, "_save_cache_doc", lambda *_a, **_k: None)
    adapter = SimpleNamespace(client=SimpleNamespace(session=session), cfg=SimpleNamespace(timeout=10, ratings_chunk_size=100))
    count, _unresolved = trakt_ratings.remove(adapter, items)
    return session, count


def test_cached_parent_scoped_season_can_be_removed(monkeypatch: pytest.MonkeyPatch) -> None:
    _, accepted = _bucketize_for_upsert([{"type": "season", "show_ids": {"tmdb": "1396", "tvdb": "81189"}, "season": 2, "rating": 8}])

    session, count = _remove(monkeypatch, accepted)

    assert count > 0
    assert session.posts == [{"shows": [{"ids": {"tmdb": 1396, "tvdb": 81189}, "seasons": [{"number": 2}]}]}]


def test_parent_scoped_episode_and_real_season_ids_are_removed(monkeypatch: pytest.MonkeyPatch) -> None:
    items = [
        {"type": "episode", "show_ids": {"tmdb": "1396"}, "season": 1, "episode": 3},
        {"type": "season", "ids": {"trakt": "3950", "tmdb": "3572"}, "show_ids": {"tmdb": "1396"}, "season": 2},
    ]

    session, _ = _remove(monkeypatch, items)

    assert {"shows": [{"ids": {"tmdb": 1396}, "seasons": [{"number": 1, "episodes": [{"number": 3}]}]}]} in session.posts
    assert {"seasons": [{"ids": {"tmdb": 3572, "trakt": 3950}}]} in session.posts


def test_parent_scoped_season_is_rated_under_the_show() -> None:
    row = {"season_number": 2, "episode_number": None, "title": "Breaking Bad", "year": 2008, "media_type": "tv", "ids": {"fldb": "flt_5b", "tmdb": 1396, "imdb": "tt0903747", "tvdb": 81189}}
    item = read_minimal(row, default_type="tv", seasons=True) or {}
    item.update(rating=8, rated_at="2026-10-02T00:00:00.000Z")

    body, accepted = _bucketize_for_upsert([item])

    assert body == {"shows": [{"ids": {"tmdb": 1396, "imdb": "tt0903747", "tvdb": 81189}, "seasons": [{"number": 2, "rating": 8, "rated_at": "2026-10-02T00:00:00.000Z"}]}]}
    assert len(accepted) == 1


def test_season_ids_that_are_the_show_ids_go_under_the_show() -> None:
    body, _ = _bucketize_for_upsert([{"type": "season", "ids": {"tmdb": "1396"}, "show_ids": {"tmdb": "1396"}, "season": 3, "rating": 7}])

    assert body == {"shows": [{"ids": {"tmdb": 1396}, "seasons": [{"number": 3, "rating": 7}]}]}


def test_real_season_ids_stay_in_the_seasons_bucket() -> None:
    body, _ = _bucketize_for_upsert([{"type": "season", "ids": {"trakt": "3950", "tmdb": "3572"}, "show_ids": {"trakt": "1388", "tmdb": "1396"}, "season": 2, "rating": 9}])

    assert body == {"seasons": [{"ids": {"tmdb": 3572, "trakt": 3950}, "rating": 9}]}


def test_season_without_any_parent_identity_is_skipped() -> None:
    body, accepted = _bucketize_for_upsert([{"type": "season", "season": 2, "rating": 9}])

    assert body == {}
    assert accepted == []
