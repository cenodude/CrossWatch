from __future__ import annotations

import time
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import insightAPI as insight_api

NOW = int(time.time())
DAY = 86400


def _movie(idx: int, extra: dict) -> dict:
    base = {"type": "movie", "title": f"Movie {idx}", "ids": {"tmdb": str(1000 + idx)}}
    base.update(extra)
    return base


def _client(monkeypatch, state, events):
    class Stats:
        data = {"samples": [], "events": list(events)}

    cw = SimpleNamespace(
        STATS=Stats(),
        REPORT_DIR=None,
        CACHE_DIR=None,
        _load_wall_snapshot=lambda: [],
        _append_log=lambda *_a, **_k: None,
    )
    monkeypatch.setattr(insight_api, "_env", lambda: (cw, lambda: {}, lambda _cfg: None, lambda *a, **k: None))
    monkeypatch.setattr(insight_api, "_load_state_features", lambda _features: state)

    app = FastAPI()
    insight_api.register_insights(app)
    return TestClient(app).get("/api/insights?limit_samples=0&history=0&include_events=0").json()


def test_week_and_month_count_what_was_added_in_each_window(monkeypatch) -> None:
    state = {
        "providers": {
            "PLEX": {
                "history": {"baseline": {"items": {
                    f"tmdb:{1000 + i}": _movie(i, {"watched_at": "2026-09-01T00:00:00Z"}) for i in range(2)
                }}},
                "ratings": {"baseline": {"items": {
                    f"tmdb:{2000 + i}": _movie(i, {"rating": 8, "rated_at": "2026-08-01T00:00:00Z"}) for i in range(2)
                }}},
            }
        }
    }
    events = [
        {"ts": NOW - DAY, "action": "add", "feature": "history", "key": f"h{i}"} for i in range(2)
    ] + [
        {"ts": NOW - 20 * DAY, "action": "rate", "feature": "ratings", "key": f"r{i}"} for i in range(2)
    ]

    feats = _client(monkeypatch, state, events)["features"]
    hist, rate = feats["history"], feats["ratings"]

    # history: 2 added a day ago -> inside both windows
    assert hist["week"] == 2
    assert hist["month"] == 2

    # ratings: 2 added 20 days ago -> outside the week, inside the month
    assert rate["week"] == 0
    assert rate["month"] == 2


def test_quiet_feature_reports_nothing_added(monkeypatch) -> None:
    state = {
        "providers": {
            "PLEX": {
                "watchlist": {"baseline": {"items": {
                    f"tmdb:{3000 + i}": _movie(i, {}) for i in range(4)
                }}}
            }
        }
    }
    wl = _client(monkeypatch, state, [])["features"]["watchlist"]
    assert wl["now"] == 4
    assert wl["week"] == 0
    assert wl["month"] == 0
    assert wl["week_removed"] == 0


def test_removals_are_reported_separately_from_adds(monkeypatch) -> None:
    state = {
        "providers": {
            "PLEX": {
                "watchlist": {"baseline": {"items": {
                    f"tmdb:{4000 + i}": _movie(i, {}) for i in range(3)
                }}}
            }
        }
    }
    events = [
        {"ts": NOW - 2 * DAY, "action": "watchlist_add", "feature": "watchlist", "key": f"a{i}"}
        for i in range(2)
    ] + [
        {"ts": NOW - 2 * DAY, "action": "watchlist_remove", "feature": "watchlist", "key": f"w{i}"}
        for i in range(5)
    ]
    wl = _client(monkeypatch, state, events)["features"]["watchlist"]
    assert wl["now"] == 3
    assert wl["week"] == 2
    assert wl["week_removed"] == 5
    assert wl["month"] == 2
    assert wl["month_removed"] == 5


def test_collection_events_feed_the_collection_lane(monkeypatch) -> None:
    state = {
        "providers": {
            "PLEX": {
                "collection": {"baseline": {"items": {
                    f"tmdb:{7000 + i}": _movie(i, {}) for i in range(10)
                }}}
            }
        }
    }
    events = (
        [{"ts": NOW - DAY, "action": "add", "feature": "collection", "key": f"c{i}"} for i in range(4)]
        + [{"ts": NOW - 20 * DAY, "action": "add", "feature": "collection", "key": f"c{40 + i}"} for i in range(6)]
        + [{"ts": NOW - 2 * DAY, "action": "remove", "feature": "collection", "key": f"c{80 + i}"} for i in range(2)]
    )

    coll = _client(monkeypatch, state, events)["features"]["collection"]
    assert coll["now"] == 10
    assert coll["week"] == 4
    assert coll["week_removed"] == 2
    assert coll["month"] == 10
    assert coll["month_removed"] == 2


def test_collection_events_do_not_leak_into_other_lanes(monkeypatch) -> None:
    state = {
        "providers": {
            "PLEX": {
                "collection": {"baseline": {"items": {
                    f"tmdb:{7000 + i}": _movie(i, {}) for i in range(3)
                }}},
                "watchlist": {"baseline": {"items": {"tmdb:8000": _movie(9, {})}}},
            }
        }
    }
    events = [{"ts": NOW - DAY, "action": "add", "feature": "collection", "key": f"c{i}"} for i in range(3)]
    feats = _client(monkeypatch, state, events)["features"]
    assert feats["collection"]["week"] == 3
    assert feats["watchlist"]["week"] == 0
    assert feats["history"]["week"] == 0
    assert feats["playlists"]["week"] == 0


def test_repeated_runs_do_not_inflate_the_window_adds(monkeypatch) -> None:
    # 1561 rows on disk, but three successive runs each reported them as added
    state = {
        "providers": {
            "CROSSWATCH": {
                "history": {"baseline": {"items": {
                    f"tmdb:{9000 + i}": _movie(i, {"watched_at": "2026-09-01T00:00:00Z"})
                    for i in range(30)
                }}}
            }
        }
    }
    events = [
        {"ts": NOW - run * DAY, "action": "add", "feature": "history", "key": f"agg:history:add:{run}:{i}"}
        for run in (1, 2, 3)
        for i in range(30)
    ]

    hist = _client(monkeypatch, state, events)["features"]["history"]
    assert hist["now"] == 30
    # 90 raw add events, but only 30 items can possibly exist
    assert hist["week"] == 30
    assert hist["month"] == 30


def test_churn_still_counts_adds_above_the_current_level(monkeypatch) -> None:
    # 10 present now, 40 removed during the window -> up to 50 distinct adds are reachable
    state = {
        "providers": {
            "PLEX": {
                "watchlist": {"baseline": {"items": {
                    f"tmdb:{9500 + i}": _movie(i, {}) for i in range(10)
                }}}
            }
        }
    }
    events = (
        [{"ts": NOW - DAY, "action": "watchlist_add", "feature": "watchlist", "key": f"a{i}"} for i in range(50)]
        + [{"ts": NOW - DAY, "action": "watchlist_remove", "feature": "watchlist", "key": f"r{i}"} for i in range(40)]
    )
    wl = _client(monkeypatch, state, events)["features"]["watchlist"]
    assert wl["now"] == 10
    assert wl["week"] == 50
    assert wl["week_removed"] == 40
