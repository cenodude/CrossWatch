from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import insightAPI as insight_api


def test_insights_history_counts_stremio_watched_episode_state(monkeypatch) -> None:
    state = {
        "providers": {
            "TRAKT": {
                "history": {
                    "baseline": {
                        "items": {
                            "tmdb:328735#s01e03": {
                                "type": "episode",
                                "title": "This Is Forever",
                                "series_title": "The Idaho Murders: College Nightmare",
                                "season": 1,
                                "episode": 3,
                                "watched_at": "2026-08-02T01:07:00Z",
                                "ids": {"tmdb": "7535444", "imdb": "tt43681035"},
                                "show_ids": {"tmdb": "328735", "imdb": "tt43680755"},
                            }
                        }
                    }
                }
            },
            "STREMIO": {
                "history": {
                    "baseline": {
                        "items": {
                            "imdb:tt43680755#s01e03": {
                                "type": "episode",
                                "title": "This Is Forever",
                                "series_title": "The Idaho Murders: College Nightmare",
                                "season": 1,
                                "episode": 3,
                                "watched": True,
                                "ids": {"imdb": "tt43680755"},
                                "show_ids": {"imdb": "tt43680755"},
                            }
                        }
                    }
                }
            },
        }
    }

    class Stats:
        data = {"samples": [], "events": []}

    cw = SimpleNamespace(
        STATS=Stats(),
        REPORT_DIR=None,
        CACHE_DIR=None,
        _load_wall_snapshot=lambda: [],
        _append_log=lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(insight_api, "_env", lambda: (cw, lambda: {}, lambda _cfg: None, lambda *a, **k: None))
    monkeypatch.setattr(insight_api, "_load_state_features", lambda _features: state)

    app = FastAPI()
    insight_api.register_insights(app)
    data = TestClient(app).get("/api/insights?limit_samples=0&history=0&include_events=0").json()

    history = data["features"]["history"]
    assert history["providers"]["trakt"] == 1
    assert history["providers"]["stremio"] == 1
    assert history["providers_mse"]["stremio"]["episodes"] == 1
    assert history["providers_mse"]["stremio"]["shows"] == 1


def test_insights_history_provider_counts_collapse_native_namespace_duplicates(monkeypatch) -> None:
    state = {
        "providers": {
            "SIMKL": {
                "history": {
                    "baseline": {
                        "items": {
                            "imdb:tt0944947#s01e01": {
                                "type": "episode",
                                "series_title": "Game of Thrones",
                                "season": 1,
                                "episode": 1,
                                "watched_at": "2026-08-22T12:00:00Z",
                                "ids": {"imdb": "tt0944947"},
                                "show_ids": {"imdb": "tt0944947"},
                            },
                            "tmdb:1399#s01e01": {
                                "type": "episode",
                                "series_title": "Game of Thrones",
                                "season": 1,
                                "episode": 1,
                                "watched_at": "1970-01-01T00:00:01Z",
                                "ids": {"tmdb": "1399"},
                                "show_ids": {"tmdb": "1399"},
                            },
                            "imdb:tt0944947#s01e02": {
                                "type": "episode",
                                "series_title": "Game of Thrones",
                                "season": 1,
                                "episode": 2,
                                "watched_at": "2026-08-22T12:00:00Z",
                                "ids": {"imdb": "tt0944947"},
                                "show_ids": {"imdb": "tt0944947"},
                            },
                            "tmdb:1399#s01e02": {
                                "type": "episode",
                                "series_title": "Game of Thrones",
                                "season": 1,
                                "episode": 2,
                                "watched_at": "1970-01-01T00:00:01Z",
                                "ids": {"tmdb": "1399"},
                                "show_ids": {"tmdb": "1399"},
                            },
                            "tmdb:603": {
                                "type": "movie",
                                "title": "The Matrix",
                                "year": 1999,
                                "watched_at": "2026-08-22T12:00:00Z",
                                "ids": {"tmdb": "603"},
                            },
                        }
                    }
                }
            }
        }
    }

    class Stats:
        data = {"samples": [], "events": []}

    cw = SimpleNamespace(
        STATS=Stats(),
        REPORT_DIR=None,
        CACHE_DIR=None,
        _load_wall_snapshot=lambda: [],
        _append_log=lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(insight_api, "_env", lambda: (cw, lambda: {}, lambda _cfg: None, lambda *a, **k: None))
    monkeypatch.setattr(insight_api, "_load_state_features", lambda _features: state)

    app = FastAPI()
    insight_api.register_insights(app)
    data = TestClient(app).get("/api/insights?limit_samples=0&history=0&include_events=0").json()

    history = data["features"]["history"]
    assert history["providers"]["simkl"] == 3
    assert history["providers_mse"]["simkl"] == {
        "movies": 1,
        "shows": 1,
        "anime": 0,
        "episodes": 2,
    }
