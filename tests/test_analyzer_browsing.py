# tests/test_analyzer_browsing.py
# CrossWatch - Analyzer Browsing and Feature Regression Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.analyzer as A


@pytest.fixture
def rows(monkeypatch):
    items = [
        {"provider": "PLEX", "feature": A._ANALYZER_FEATURES[n % 5],
         "title": f"Movie {n:05d}", "type": "movie", "key": f"tmdb:{n}",
         "ids": {"tmdb": str(n)}}
        for n in range(50000)
    ]
    monkeypatch.setattr(A, "_cached_scoped_rows", lambda pairs: (items, A._counts_from_rows(items)))
    return items


def test_search_and_feature_filter_precede_paging(rows):
    page = A.api_state(q="Movie 49", feature="ratings", offset=250, limit=250, sort="title")
    expected = [r for r in rows if "49" in r["title"] and r["feature"] == "ratings"]
    assert page["total"] == len(expected)
    assert page["items"] == expected[250:500]
    assert page["has_more"] is (500 < len(expected))


def test_global_sort_is_stable_and_does_not_mutate_cached_rows(rows):
    page = A.api_state(sort="title", direction="desc", offset=250, limit=250)
    assert page["items"] == list(reversed(rows))[250:500]
    assert rows[0]["title"] == "Movie 00000"
    assert A.api_state(limit=1)["items"] == rows[:1]


def test_id_search_finds_items_beyond_first_page(rows):
    page = A.api_state(q="plex tmdb:49999")
    assert page["items"] == rows[-1:]
    assert page["total"] == 1
    assert not page["has_more"]


def test_bounded_pages_and_metadata_only_requests(rows):
    assert len(A.api_state(limit=50000)["items"]) == 500
    assert A.api_state(limit=0)["items"] == []
    assert A.api_state(limit=0)["total"] == 50000
    assert A.api_state(q="no such title")["total"] == 0
    assert A.api_state(feature="unsupported")["total"] == 0


def test_episode_search_and_malformed_coordinates(monkeypatch):
    rows = [
        {"title": "Pilot", "series_title": "Series", "type": "episode", "season": 2, "episode": 3},
        {"title": "Broken metadata", "type": "episode", "season": "unknown", "episode": 1},
    ]
    monkeypatch.setattr(A, "_cached_scoped_rows", lambda pairs: (rows, {}))
    assert A.api_state(q="series s02e03")["items"] == rows[:1]
    assert A.api_state(sort="title")["items"] == list(reversed(rows))


def test_http_filters_preserve_managed_pair_scope(monkeypatch):
    captured = []
    monkeypatch.setattr(A, "request_user", lambda request: {"is_admin": False})
    monkeypatch.setattr(A, "_cfg", lambda: {})
    monkeypatch.setattr(A, "pair_ids_for_user", lambda cfg, user: {"allowed"})
    def cached(pairs):
        captured.append(pairs)
        return [], {}
    monkeypatch.setattr(A, "_cached_scoped_rows", cached)
    app = FastAPI()
    app.include_router(A.router)
    with TestClient(app) as client:
        for pairs, expected in [("allowed,private", "allowed"), ("private", "")]:
            response = client.get("/api/analyzer/state", params={"pairs": pairs, "q": "secret", "feature": "ratings", "sort": "title"})
            assert response.status_code == 200
            assert captured[-1] == A._STRICT_PAIRS_PREFIX + expected


@pytest.mark.parametrize("feature", A._ANALYZER_FEATURES)
@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_presence_checks_respect_feature_and_direction(feature, mode, tmp_path, monkeypatch):
    monkeypatch.setattr(A, "CWS_DIR", tmp_path)
    first = {"type": "movie", "title": "Source only", "ids": {"tmdb": "10"}, "watched_at": "2026-01-01T00:00:00Z", "rating": 7, "progress": 50}
    second = {**first, "title": "Target only", "ids": {"tmdb": "20"}}
    state = {"providers": {
        "TRAKT": {feature: {"baseline": {"items": {"tmdb:10": first}}}},
        "SIMKL": {feature: {"baseline": {"items": {"tmdb:20": second}}}},
    }}
    cfg = {"pairs": [{"id": "p1", "source": "TRAKT", "target": "SIMKL", "enabled": True,
                      "mode": mode, "features": {feature: {"enabled": True}}}]}
    ctx = A._analysis_context(state, cfg)
    findings = A._problems(state, None, cfg=cfg, ctx=ctx, include_system=False, include_hints=False)
    missing = {(p["provider"], p["feature"], p["key"], tuple(p["targets"])) for p in findings if p["type"] == "missing_peer"}
    expected = {("TRAKT", feature, "tmdb:10", ("SIMKL",))}
    if mode == "two-way":
        expected.add(("SIMKL", feature, "tmdb:20", ("TRAKT",)))
    assert missing == expected
    cfg["pairs"][0]["features"][feature] = {"enable": False}
    assert A._pair_map(cfg, state) == {}
    assert A._scoped_item_rows(state, cfg) == []
