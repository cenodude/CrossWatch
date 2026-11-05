# tests/test_tracker_experience.py
# CrossWatch - Tracker timeline, title panel, shows, stats and logging watches
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import trackerAPI
from cw_platform.tracker_store import TrackerStore

NOW = datetime.now(timezone.utc)


def _at(days: float) -> str:
    return (NOW - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.000Z")


HEAT = {"type": "movie", "title": "Heat", "year": 1995, "ids": {"tmdb": "949", "imdb": "tt0113277"}}
MATRIX = {"type": "movie", "title": "The Matrix", "year": 1999, "ids": {"tmdb": "603"}}


def _ep(season, episode, days, title="Severance", tmdb="95396"):
    return {"type": "episode", "title": f"S{season:02d}E{episode:02d}", "series_title": title, "series_year": 2022,
            "season": season, "episode": episode, "ids": {}, "show_ids": {"tmdb": tmdb}, "watched_at": _at(days)}


@pytest.fixture()
def base(tmp_path):
    root = tmp_path / ".cw_provider"
    store = TrackerStore(root)
    store.write_state("history", {
        "tmdb:949": {**HEAT, "watched_at": _at(2)},
        "tmdb:95396#s01e01": _ep(1, 1, 0.1),
        "tmdb:95396#s01e02": _ep(1, 2, 0.05),
        "tmdb:95396#s02e01": _ep(2, 1, 40),
        "tmdb:1399#s01e01": _ep(1, 1, 400, title="Game of Thrones", tmdb="1399"),
    })
    store.write_state("ratings", {"tmdb:949": {**HEAT, "rating": 8, "rated_at": _at(2)}})
    store.write_state("watchlist", {"tmdb:603": MATRIX})
    return root


class FakeTmdb:
    def lookup(self, kind, tmdb, **_):
        assert kind == "show"
        return SimpleNamespace(data={
            "id": int(tmdb), "name": "Severance", "status": "Returning Series",
            "seasons": [{"season_number": 0, "episode_count": 3, "name": "Specials"},
                        {"season_number": 1, "episode_count": 9, "name": "Season 1"},
                        {"season_number": 2, "episode_count": 10, "name": "Season 2"}],
            "next_episode_to_air": {"season_number": 2, "episode_number": 4, "air_date": "2026-12-01"},
        }, fetched_at=1.0, source="cache")


@pytest.fixture()
def client(base, monkeypatch):
    cfg = {"crosswatch": {"root_dir": str(base)}, "tmdb": {"api_key": "k"}}
    monkeypatch.setattr(trackerAPI, "load_config", lambda: cfg)
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    monkeypatch.setattr(trackerAPI, "_tmdb_provider", lambda: FakeTmdb())
    app = FastAPI()
    app.include_router(trackerAPI.router)
    test_client = TestClient(app)
    test_client.cfg = cfg
    return test_client


def _items(base, feature):
    return TrackerStore(base).read_current(feature)["items"]


def test_history_rows_include_saved_half_point_ratings(client, base):
    store = TrackerStore(base)
    store.write_state("ratings", {"tmdb:949": {**HEAT, "rating": 7.5, "rated_at": _at(1)}})
    rows = client.get("/api/tracker/items", params={"feature": "history"}).json()["items"]
    heat = next(row for row in rows if row["title"] == "Heat")
    assert heat["user_rating"] == 7.5
    assert heat["rating"] is None
    assert all(row["user_rating"] is None for row in rows if row["type"] == "episode")


def test_history_episode_rating_does_not_leak_to_other_episodes(client, base):
    store = TrackerStore(base)
    store.write_state("ratings", {"tmdb:95396#s01e01": {**_ep(1, 1, 1), "rating": 8.5, "rated_at": _at(1)}})
    rows = client.get("/api/tracker/items", params={"feature": "history"}).json()["items"]
    assert next(row for row in rows if row["key"] == "tmdb:95396#s01e01")["user_rating"] == 8.5
    assert next(row for row in rows if row["key"] == "tmdb:95396#s01e02")["user_rating"] is None


def test_log_a_movie_watch_and_undo_it(client, base):
    res = client.post("/api/tracker/add", json={"feature": "history", "item": {**MATRIX, "watched_at": _at(1)}})
    assert res.status_code == 200, res.text
    assert res.json()["key"] == "tmdb:603"
    assert _items(base, "history")["tmdb:603"]["title"] == "The Matrix"
    changes = client.get("/api/tracker/changes").json()["changes"]
    assert changes[0]["action"] == "add" and changes[0]["undoable"]
    assert client.post("/api/tracker/undo", json={"batch": res.json()["batch"]}).status_code == 200
    assert "tmdb:603" not in _items(base, "history")


def test_logging_an_older_watch_keeps_the_newest_and_a_newer_one_moves_it(client, base):
    older = client.post("/api/tracker/add", json={"feature": "history", "item": {**HEAT, "watched_at": _at(10)}})
    assert older.status_code == 409 and older.json()["error"] == "already_there"
    newer = client.post("/api/tracker/add", json={"feature": "history", "item": {**HEAT, "watched_at": _at(0.5)}})
    assert newer.status_code == 200
    assert _items(base, "history")["tmdb:949"]["watched_at"] == _at(0.5)


def test_rewatch_trackers_get_a_new_play_per_watch(client, base):
    TrackerStore(base).write_state("history", {f"tmdb:949@{int((NOW - timedelta(days=2)).timestamp())}": {**HEAT, "watched_at": _at(2)}})
    res = client.post("/api/tracker/add", json={"feature": "history", "item": {**HEAT, "watched_at": _at(1)}})
    assert res.status_code == 200, res.text
    assert len(_items(base, "history")) == 2 and res.json()["key"].startswith("tmdb:949@")


def test_mark_watched_moves_a_title_off_the_watchlist_in_one_undoable_step(client, base):
    res = client.post("/api/tracker/add", json={"feature": "history", "item": {**MATRIX, "watched_at": _at(0.2)}, "remove_from_watchlist": "tmdb:603"})
    assert res.status_code == 200
    assert "tmdb:603" in _items(base, "history") and "tmdb:603" not in _items(base, "watchlist")
    client.post("/api/tracker/undo", json={"batch": res.json()["batch"]})
    assert "tmdb:603" not in _items(base, "history") and "tmdb:603" in _items(base, "watchlist")


def test_log_a_whole_season_and_rate_titles(client, base):
    eps = [{"type": "episode", "series_title": "Severance", "series_year": 2022, "season": 2, "episode": n, "show_ids": {"tmdb": "95396"}, "watched_at": _at(0.01)} for n in (2, 3, 2)]
    res = client.post("/api/tracker/add", json={"feature": "history", "items": eps})
    assert res.json()["count"] == 2
    assert {"tmdb:95396#s02e02", "tmdb:95396#s02e03"} <= set(_items(base, "history"))
    assert client.post("/api/tracker/add", json={"feature": "ratings", "item": {**MATRIX, "rating": 9}}).status_code == 200
    assert client.post("/api/tracker/add", json={"feature": "ratings", "item": {**HEAT, "rating": 6}}).status_code == 200
    ratings = _items(base, "ratings")
    assert (ratings["tmdb:603"]["rating"], ratings["tmdb:949"]["rating"]) == (9, 6)


@pytest.mark.parametrize(("feature", "item", "error"), [
    ("history", {"type": "person", "title": "x", "ids": {"tmdb": "1"}}, "invalid_type"),
    ("history", {"type": "show", "title": "x", "ids": {"tmdb": "1"}}, "invalid_type"),
    ("history", {"type": "movie", "title": "x", "ids": {}}, "invalid_ids"),
    ("history", {"type": "movie", "title": "", "ids": {"tmdb": "1"}}, "invalid_title"),
    ("history", {"type": "episode", "series_title": "x", "season": 1, "episode": 0, "show_ids": {"tmdb": "1"}}, "invalid_episode"),
    ("history", {"type": "movie", "title": "x", "ids": {"tmdb": "1"}, "watched_at": "2999-01-01T00:00:00Z"}, "invalid_date"),
    ("ratings", {"type": "movie", "title": "x", "ids": {"tmdb": "1"}, "rating": 11}, "invalid_rating"),
    ("progress", {"type": "movie", "title": "x", "ids": {"tmdb": "1"}}, "unknown_feature"),
])
def test_bad_watches_are_refused(client, feature, item, error):
    res = client.post("/api/tracker/add", json={"feature": feature, "item": item})
    assert res.status_code == 400 and res.json()["error"] == error


def test_title_panel_gathers_every_list_for_a_movie(client):
    data = client.get("/api/tracker/title", params={"feature": "history", "key": "tmdb:949"}).json()
    assert data["show"] is False and data["title"]["title"] == "Heat"
    assert [p["key"] for p in data["plays"]] == ["tmdb:949"]
    assert data["ratings"][0]["rating"] == 8 and data["watchlist"] == []
    watch = client.get("/api/tracker/title", params={"feature": "watchlist", "key": "tmdb:603"}).json()
    assert watch["plays"] == [] and watch["watchlist"][0]["key"] == "tmdb:603"


def test_title_panel_for_a_show_lists_seasons(client):
    data = client.get("/api/tracker/title", params={"feature": "history", "key": "tmdb:95396#s01e02"}).json()
    assert data["show"] is True and data["show_key"] == "tmdb:95396"
    assert data["title"]["title"] == "Severance" and data["title"]["art"] == {"kind": "tv", "tmdb": "95396"}
    assert [(s["season"], [e["episode"] for e in s["episodes"]]) for s in data["seasons"]] == [(1, [1, 2]), (2, [1])]
    same = client.get("/api/tracker/title", params={"show": "tmdb:95396"}).json()
    assert len(same["plays"]) == 3


def test_shows_view_lists_progress_per_show(client):
    data = client.get("/api/tracker/shows").json()
    assert data["total"] == 2
    first = data["shows"][0]
    assert (first["key"], first["title"], first["episodes"], first["last_season"], first["last_episode"]) == ("tmdb:95396", "Severance", 3, 1, 2)
    assert first["art"] == {"kind": "tv", "tmdb": "95396"}
    assert client.get("/api/tracker/shows", params={"q": "thrones"}).json()["shows"][0]["key"] == "tmdb:1399"


def test_show_progress_finds_the_next_episode(client):
    data = client.get("/api/tracker/show-progress", params={"tmdb": "95396"}).json()
    assert (data["total"], data["watched"]) == (19, 3)
    assert data["next"] == {"season": 2, "episode": 2}
    assert data["next_air"] == {"season": 2, "episode": 4, "date": "2026-12-01"}
    assert data["completed"] is False
    assert data["seasons"] == [{"season": 1, "episodes": 9}, {"season": 2, "episodes": 10}]
    seasons = client.get("/api/tracker/seasons", params={"tmdb": "95396"}).json()["seasons"]
    assert [s["season"] for s in seasons] == [0, 1, 2]


def test_show_progress_without_a_key_stays_quiet(client):
    client.cfg.pop("tmdb")
    assert client.get("/api/tracker/show-progress", params={"tmdb": "95396"}).json() == {"ok": True, "available": False}


def test_stats_count_recent_plays_hours_and_streak(client, base):
    store = TrackerStore(base)
    text = {k: t for f, k, _, t in store.iter_records() if f == "history"}
    store.save_checks([("history", "tmdb:949", "ok", {"runtime": 170}, text["tmdb:949"])])
    data = client.get("/api/tracker/stats").json()
    assert (data["total"], data["movies"], data["episodes"]) == (5, 1, 4)
    assert (data["week"], data["month"]) == (3, 3)
    assert data["minutes"] == 170 and data["timed"] == 1
    assert data["streak"] == 1


def test_summary_lists_where_an_add_spreads(client):
    client.cfg["pairs"] = [{"source": "CROSSWATCH", "target": "TRAKT", "features": {"history": {"enable": True, "add": True}}}]
    effects = client.get("/api/tracker/summary").json()["delete_effects"]
    assert effects["history"]["adds"] == [{"provider": "TRAKT", "instance": "default"}]


def test_months_count_plays_and_a_month_jump_starts_the_list_there(client):
    months = client.get("/api/tracker/months?feature=history").json()["months"]
    want: dict[str, int] = {}
    for days in (2, 0.1, 0.05, 40, 400):
        key = _at(days)[:7]
        want[key] = want.get(key, 0) + 1
    assert months == [{"month": k, "count": v} for k, v in sorted(want.items(), reverse=True)]
    assert client.get("/api/tracker/months?feature=history&media=movie").json()["months"] == [{"month": _at(2)[:7], "count": 1}]

    old = _at(400)[:7]
    jumped = client.get(f"/api/tracker/items?feature=history&month={old}").json()
    assert jumped["total"] == 1
    assert jumped["items"][0]["key"] == "tmdb:1399#s01e01"
    assert client.get("/api/tracker/items?feature=history&month=2026-13").status_code == 422
