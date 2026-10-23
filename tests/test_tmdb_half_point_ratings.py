# tests/test_tmdb_half_point_ratings.py
# CrossWatch - TMDb half-point ratings on read and write
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from cw_platform.orchestrator._pairs_utils import ratings_step_for
from cw_platform.orchestrator._planner import diff_ratings
from providers.sync import _mod_TMDB
from providers.sync.tmdb import _common
from providers.sync.tmdb import _ratings as ratings


class _Resp:
    def __init__(self, payload: Any = None, status: int = 200) -> None:
        self.status_code = status
        self._payload = payload or {}
        self.text = "x"

    def json(self) -> Any:
        return self._payload


class _Client:
    BASE = "https://api.themoviedb.org/3"

    def __init__(self, pages: dict[str, list[dict[str, Any]]]) -> None:
        self.pages = pages
        self.posts: list[tuple[str, Any]] = []

    def account_id(self) -> str:
        return "1"

    def _user_params(self, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        return dict(extra or {})

    def get(self, url: str, params: Any = None) -> _Resp:
        path = url.split("/account/1/", 1)[1]
        return _Resp({"total_pages": 1, "results": self.pages.get(path, [])})

    def post(self, url: str, params: Any = None, json: Any = None) -> _Resp:
        self.posts.append((url, json))
        return _Resp({"success": True}, 201)


def _adapter(client: _Client) -> SimpleNamespace:
    prog = SimpleNamespace(tick=lambda *a, **kw: None, done=lambda **kw: None)
    return SimpleNamespace(client=client, progress_factory=lambda _feature: prog)


@pytest.fixture(autouse=True)
def _quiet(monkeypatch):
    monkeypatch.setattr(ratings, "enrich_ids_dict", lambda *a, **kw: None)
    monkeypatch.setattr(ratings, "touch_feature_state", lambda *a, **kw: None)


@pytest.mark.parametrize("value,expected", [
    (7, 7.0), (7.5, 7.5), ("7.5", 7.5), (7.25, 7.5), (7.2, 7.0), (0.5, 0.5), (0.2, 0.5),
    (10.5, 10.0), (0, None), (None, None), (True, None), (float("nan"), None), ("x", None),
])
def test_half_point(value, expected):
    assert _common.half_point(value) == expected


def test_index_keeps_half_points_and_whole_ratings_stay_int():
    client = _Client({
        "rated/movies": [{"id": 1, "title": "A", "rating": 7.5, "created_at": "2026-10-01T00:00:00Z"}],
        "rated/tv": [{"id": 2, "name": "B", "rating": 8.0}],
        "rated/tv/episodes": [{"id": 3, "show_id": 2, "season_number": 1, "episode_number": 4, "rating": 6.5}],
    })
    idx = ratings.build_index(_adapter(client))
    by_type = {v["type"]: v["rating"] for v in idx.values()}
    assert by_type == {"movie": 7.5, "show": 8, "episode": 6.5}
    assert isinstance(by_type["show"], int)


@pytest.mark.parametrize("rating,sent", [(7.5, 7.5), (8, 8.0), (7.3, 7.5), (0.5, 0.5)])
def test_add_sends_half_point_value(rating, sent):
    client = _Client({})
    ok, unresolved = ratings.add(_adapter(client), [{"type": "movie", "ids": {"tmdb": 9}, "rating": rating}])
    assert (ok, unresolved) == (1, [])
    assert client.posts == [(f"{_Client.BASE}/movie/9/rating", {"value": sent})]


def test_add_rejects_missing_rating():
    client = _Client({})
    ok, unresolved = ratings.add(_adapter(client), [{"type": "movie", "ids": {"tmdb": 9}, "rating": 0}])
    assert ok == 0 and unresolved[0]["reason"] == "missing_rating"
    assert client.posts == []


def test_manifest_declares_half_point_step():
    assert ratings_step_for(_mod_TMDB.OPS) == 0.5


def test_planner_syncs_half_point_drift_only_when_both_sides_support_it():
    src = {"tmdb:1": {"type": "movie", "ids": {"tmdb": 1}, "rating": 7.5}}
    dst = {"tmdb:1": {"type": "movie", "ids": {"tmdb": 1}, "rating": 8}}
    adds, _ = diff_ratings(src, dst, step=0.5)
    assert [a["rating"] for a in adds] == [7.5]
    assert diff_ratings(src, dst, step=1.0) == ([], [])
