# tests/test_tracker_overview.py
# CrossWatch - Overview history, resumable items, episode dates and instance access
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from threading import Event, Lock
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import trackerAPI
from cw_platform.tracker_store import TrackerStore
from services.tracker_overview import overview_payload
from services.episode_overview import show_overview
from providers.metadata import _meta_TMDB as tmdb


DAY = date(2026, 11, 10)


def episode(tmdb="1", number=1, day=1):
    return {"type": "episode", "series_title": f"Show {tmdb}", "show_ids": {"tmdb": tmdb}, "season": 1, "episode": number, "watched_at": f"2026-11-{day:02d}T12:00:00Z"}


class Metadata:
    def __init__(self):
        self.calls = []

    def lookup(self, kind, tmdb, *, season=None, **kwargs):
        self.calls.append((tmdb, season, kwargs))
        if season is None:
            return SimpleNamespace(data={"name": f"Show {tmdb}", "seasons": [
                {"season_number": 1, "episode_count": 6, "air_date": "2026-01-01"},
                {"season_number": 2, "episode_count": 1, "air_date": (DAY + timedelta(days=20)).isoformat()},
            ], "next_episode_to_air": {"season_number": 1, "episode_number": 3}})
        days = [-10, -1, 0, 29, 30, None] if season == 1 else [20]
        return SimpleNamespace(data={"episodes": [{"episode_number": index + 1, "name": f"Episode {index + 1}", "air_date": (DAY + timedelta(days=offset)).isoformat() if offset is not None else None} for index, offset in enumerate(days)]})


@pytest.fixture()
def app(tmp_path, monkeypatch):
    root = tmp_path / "tracker"
    store = TrackerStore(root)
    cfg = {"crosswatch": {"root_dir": str(root)}, "tmdb": {"api_key": "test"}}
    monkeypatch.setattr(trackerAPI, "load_config", lambda: cfg)
    from cw_platform import event_archive
    monkeypatch.setattr(event_archive, "get_conn", lambda: None)
    meta = Metadata()
    monkeypatch.setattr(trackerAPI, "_tmdb_provider", lambda: meta)
    api = FastAPI()
    api.include_router(trackerAPI.router)
    return TestClient(api), store, cfg, meta


def test_overview_groups_consecutive_episodes_and_keeps_later_rewatches(app):
    client, store, _, meta = app
    movie = {"type": "movie", "title": "Movie", "ids": {"tmdb": "9"}, "watched_at": "2026-11-03T12:00:00Z"}
    store.write_state("history", {"ep1": episode(number=1, day=1), "movie": movie, "ep2": episode(number=2, day=4), "ep3": episode(number=3, day=5)})
    store.write_state("progress", {"resume": {"type": "movie", "title": "Resume", "ids": {"tmdb": "8"}, "progress_percent": 50, "duration_ms": 6000000, "progress_ms": 3000000, "progress_at": "2026-11-10T12:00:00Z"}})
    data = client.get("/api/tracker/overview").json()
    assert [row["plays"] for row in data["recent"]] == [2, 1, 1]
    assert data["recent"][0]["ref"] == {"feature": "history", "key": "ep3"}
    assert data["continue"][0]["remaining_minutes"] == 50
    assert not meta.calls


def test_recent_ratings_follow_the_selected_tracker_instance(app):
    client, store, cfg, _ = app
    movie = {"type": "movie", "title": "Movie", "ids": {"tmdb": "9"}, "watched_at": "2026-11-03T12:00:00Z"}
    unrated = {**movie, "title": "Unrated", "ids": {"tmdb": "10"}}
    history = {"movie": movie, "unrated": unrated}
    store.write_state("history", history)
    store.write_state("ratings", {"movie-rating": {**movie, "rating": 7.5}})
    cfg["crosswatch"]["instances"] = {"CW-P01": {"label": "Family"}}
    other = TrackerStore(store.root / "profiles" / "CW-P01")
    other.write_state("history", history)
    other.write_state("ratings", {"movie-rating": {**movie, "rating": 6}})
    for instance, expected in [("default", 7.5), ("CW-P01", 6)]:
        data = client.get("/api/tracker/overview", params={"provider_instance": instance}).json()
        ratings = {row["key"]: row["user_rating"] for row in data["recent"]}
        assert ratings == {"movie": expected, "unrated": None}


@pytest.mark.parametrize("episode_rating,expected", [(None, 8), (7.5, 7.5)])
def test_recent_episode_ratings_prefer_the_episode_then_the_show(app, episode_rating, expected):
    client, store, _, _ = app
    watched = episode(number=2)
    store.write_state("history", {"watched": watched})
    ratings = {
        "show": {"type": "show", "title": "Show 1", "ids": {"tmdb": "1"}, "rating": 8},
        "other-episode": {**episode(number=3), "rating": 3},
    }
    if episode_rating is not None:
        ratings["watched"] = {**watched, "rating": episode_rating}
    store.write_state("ratings", ratings)
    data = client.get("/api/tracker/overview").json()
    assert data["recent"][0]["user_rating"] == expected


def test_continue_excludes_completed_dropped_finished_and_stale_progress(app):
    client, store, _, _ = app
    rows = {str(i): {"type": "movie", "title": f"Movie {i}", "ids": {"tmdb": str(i)}, "progress_percent": 50, "progress_at": "2026-11-09T12:00:00Z"} for i in range(1, 6)}
    rows["4"]["progress_percent"] = 100
    store.write_state("progress", rows)
    store.save_note("movie:tmdb:1", {"type": "movie", "title": "Movie 1", "status": "dropped"})
    store.save_note("movie:tmdb:2", {"type": "movie", "title": "Movie 2", "status": "completed"})
    store.write_state("history", {"3": {**rows["3"], "watched_at": "2026-11-10T12:00:00Z"}})
    assert [row["key"] for row in client.get("/api/tracker/overview").json()["continue"]] == ["5"]


@pytest.mark.parametrize("duration,position,remaining", [
    (3600000, 1800000, 30), (3600000, 1810000, 30),
    (None, 1800000, None), (3600000, None, None),
    (3600000, -1, None), (0, 0, None), (3600000, 3600000, None),
    (3600000, 100, None), (True, 0, None), (3600000, True, None),
    (float("inf"), 1800000, None), (3600000, float("nan"), None),
])
def test_remaining_time_requires_consistent_finite_playback_data(app, duration, position, remaining):
    client, store, _, meta = app
    store.write_state("progress", {"resume": {"type": "episode", "series_title": "Example", "show_ids": {"tmdb": "8"}, "season": 1, "episode": 2, "progress_percent": 50, "duration_ms": duration, "progress_ms": position}})
    row = client.get("/api/tracker/overview").json()["continue"][0]
    assert row.get("remaining_minutes") == remaining
    assert row["progress_percent"] == 50
    assert not meta.calls


def test_progress_tab_filters_completed_before_paging_and_updates_counts(app):
    client, store, _, _ = app
    rows = {str(i): {"type": "movie", "title": f"Movie {i}", "ids": {"tmdb": str(i)}, "progress_percent": 49, "progress_at": "2026-10-09T12:00:00Z"} for i in range(1, 5)}
    rows["3"]["progress_at"] = "2026-11-11T12:00:00Z"
    rows["4"]["progress_at"] = "2026-11-09T12:00:00Z"
    store.write_state("progress", rows)
    before = client.get("/api/tracker/items?feature=progress&limit=1").json()
    assert before["total"] == 4
    store.write_state("history", {f"{i}@1": {**rows[str(i)], "watched_at": "2026-11-10T12:00:00Z"} for i in range(1, 4)})
    first = client.get("/api/tracker/items?feature=progress&sort=title&limit=1").json()
    second = client.get("/api/tracker/items?feature=progress&sort=title&limit=1&offset=1").json()
    assert first["total"] == second["total"] == 2
    assert [row["key"] for row in first["items"] + second["items"]] == ["3", "4"]
    assert client.get("/api/tracker/months?feature=progress").json()["months"] == [{"month": "2026-11", "count": 2}]
    summary = client.get("/api/tracker/summary").json()
    assert summary["counts"]["progress"] == 2
    assert summary["health"]["progress"] == {"ok": 0, "attention": 0, "unchecked": 2}
    store.save_checks((feature, key, "no_tmdb", {}, raw) for feature, key, _, raw in store.iter_records() if feature == "progress")
    summary = client.get("/api/tracker/summary").json()
    assert summary["attention"]["progress"] == 2
    assert summary["health"]["progress"] == {"ok": 0, "attention": 2, "unchecked": 0}
    assert store.query("progress")[0] == 4


def test_progress_completion_matches_movie_aliases_and_exact_episode(app):
    client, store, _, _ = app
    movie = {"type": "movie", "title": "Movie", "ids": {"tmdb": "1", "imdb": "tt1"}, "progress_percent": 49, "progress_at": "2026-11-10T13:00:00+01:00"}
    progress = {
        "movie": movie,
        "seen": {**episode(number=1), "progress_at": "2026-11-09T12:00:00Z", "progress_percent": 40},
        "next": {**episode(number=2), "progress_at": "2026-11-09T12:00:00Z", "progress_percent": 40},
    }
    store.write_state("progress", progress)
    store.write_state("history", {
        "imdb:tt1@1": {"type": "movie", "ids": {"imdb": "tt1"}, "watched_at": "2026-11-10T12:00:00Z"},
        "episode": episode(number=1, day=10),
    })
    data = client.get("/api/tracker/items?feature=progress").json()
    assert [row["key"] for row in data["items"]] == ["next"]
    progress["movie"]["progress_at"] = "2026-11-10T14:00:00+01:00"
    store.write_state("progress", progress)
    assert client.get("/api/tracker/items?feature=progress").json()["total"] == 2
    other = TrackerStore(store.root / "profiles" / "other")
    other.write_state("progress", {"movie": {**movie, "progress_at": "2026-11-09T12:00:00Z"}})
    assert other.query("progress", current_progress=True)[0] == 1


def test_airing_cards_include_real_season_watch_status_without_extra_lookups():
    class Season:
        def __init__(self):
            self.calls = []

        def lookup(self, kind, tmdb, *, season=None, **kwargs):
            self.calls.append(season)
            if season is None:
                return SimpleNamespace(fetched_at=time.time(), data={"name": "Show", "status": "Returning Series", "seasons": [{"season_number": 3, "episode_count": 8, "air_date": "2026-10-01"}]})
            return SimpleNamespace(fetched_at=time.time(), data={"episodes": [{"episode_number": number, "name": f"Episode {number}", "air_date": "2026-11-09" if number <= 6 else "2026-11-12", "runtime": 58} for number in range(1, 9)]})

    provider = Season()
    data = show_overview({"key": "tmdb:1"}, {(3, ep) for ep in range(1, 5)}, provider, DAY)
    status = data["airing"][0]["season_progress"]
    assert status["complete"] is True
    assert [ep["status"] for ep in status["episodes"]] == ["watched"] * 4 + ["unwatched"] * 2 + ["upcoming"] * 2
    assert sum(ep["runtime"] for ep in status["episodes"] if ep["status"] == "unwatched") == 116
    assert provider.calls == [None, 3]


def test_airing_cards_do_not_claim_complete_counts_with_unknown_air_dates():
    data = show_overview({"key": "tmdb:1"}, {(1, 1)}, Metadata(), DAY)
    status = data["airing"][0]["season_progress"]
    assert status["complete"] is False
    assert status["episodes"][-1]["status"] == "unknown"
    assert all(ep["runtime"] is None for ep in status["episodes"])


def test_episode_badge_counts_only_known_aired_unwatched_episodes():
    meta = Metadata()
    show = {"key": "tmdb:1", "title": "Show", "last_watched": "2026-11-08"}
    result = show_overview(show, {(1, 1)}, meta, DAY)
    assert result["continue"][0]["aired_episodes_left"] == 2
    assert [ep["status"] for ep in result["continue"][0]["season_progress"]["episodes"][:3]] == ["watched", "unwatched", "unwatched"]
    assert not any("progress_percent" in row for row in result["continue"])
    assert {call[1] for call in meta.calls} == {None, 1, 2}


@pytest.mark.parametrize("missing", ["unloaded", "incomplete", "duplicate"])
def test_episode_badge_does_not_guess_for_unloaded_or_incomplete_seasons(missing):
    class PartialMetadata(Metadata):
        def lookup(self, kind, tmdb, *, season=None, **kwargs):
            response = super().lookup(kind, tmdb, season=season, **kwargs)
            if season is None and missing == "unloaded":
                response.data["seasons"].append({"season_number": 3, "episode_count": 5, "air_date": "2020-01-01"})
                response.data["status"] = "Ended"
            if season == 1 and missing == "incomplete":
                response.data["episodes"] = response.data["episodes"][:2]
            if season == 1 and missing == "duplicate":
                response.data["episodes"].append(response.data["episodes"][0])
            return response
    result = show_overview({"key": "tmdb:1", "title": "Show"}, {(1, 1)}, PartialMetadata(), DAY)
    assert result["continue"]
    assert "aired_episodes_left" not in result["continue"][0]


def test_episode_dates_cover_the_whole_window_and_new_seasons():
    meta = Metadata()
    show = {"key": "tmdb:1", "title": "Show", "last_watched": "2026-11-08"}
    data = show_overview(show, {(1, 1)}, meta, DAY)
    assert [(row["season"], row["episode"] ) for row in data["continue"]] == [(1, 2)]
    assert {(row["season"], row["episode"]) for row in data["airing"]} == {(1, 3), (1, 4), (2, 1)}
    assert all(call[2].get("max_age") in (None, 86400) and not call[2].get("fresh") and call[2]["max_retries"] == 0 for call in meta.calls)
    assert not show_overview({**show, "status": "completed"}, {(1, 1)}, meta, DAY)["continue"]
    future = show_overview(show, {(1, 1), (1, 2), (1, 3)}, meta, DAY)
    assert not future["continue"]
    assert not any(row["season"] == 1 and row["episode"] == 3 for row in future["airing"])


def test_episode_snapshot_uses_bulk_inputs_and_excludes_dropped_titles(app, monkeypatch):
    client, store, _, meta = app
    store.write_state("history", {str(i): episode(str(i)) for i in range(1, 41)})
    store.save_note("show:tmdb:6", {"type": "show", "title": "Show 6", "status": "dropped"})
    monkeypatch.setattr(TrackerStore, "related", lambda *args, **kwargs: pytest.fail("Use bulk episode coordinates"))
    response = client.get("/api/tracker/overview/shows?day=2026-11-10")
    data = response.json()
    assert response.headers["content-type"].startswith("application/json")
    assert data["total"] == data["checked"] == 39
    assert data["done"] is True and len(data["continue"]) == 39
    assert "6" not in {call[0] for call in meta.calls}
    assert client.get("/api/tracker/overview/shows?day=2026-99-01").status_code == 400

def test_imdb_only_shows_get_continue_and_airing_rows(app, monkeypatch):
    from providers.scrobble import _show_tmdb

    client, store, _, meta = app
    body = {"tv_results": [{"id": 95396}]}

    def get(url, params=None, **_):
        found = url.endswith("/find/tt35309276")
        return SimpleNamespace(status_code=200, headers={}, json=lambda: body if found else {}, raise_for_status=lambda: None)

    _show_tmdb._CACHE.clear()
    monkeypatch.setattr(_show_tmdb.requests, "get", get)
    ids = {"imdb": "tt35309276"}
    store.write_state("history", {"imdb:tt35309276#s01e01": {**episode(), "series_title": "Devil", "ids": dict(ids), "show_ids": dict(ids)},
                                  "imdb:tt1#s01e01": {**episode(), "series_title": "Unknown", "ids": {"imdb": "tt1"}, "show_ids": {"imdb": "tt1"}}})
    data = client.get("/api/tracker/overview/shows?day=2026-11-10").json()
    assert (data["total"], data["unavailable"]) == (2, 1)
    row = data["continue"][0]
    assert (row["season"], row["episode"], row["title"]) == (1, 2, "Devil")
    assert row["ref"] == {"show": "title:devil"}
    assert row["art"] == {"kind": "tv", "tmdb": "95396"}
    assert data["airing"] and all(item["ref"] == {"show": "title:devil"} for item in data["airing"])
    assert {call[0] for call in meta.calls} == {"95396"}


def test_imdb_only_show_merges_with_its_tmdb_show_and_mixed_titles_stay_apart(app, monkeypatch):
    from providers.scrobble import _show_tmdb

    client, store, _, meta = app
    finds = {"tt35309276": {"tv_results": [{"id": 95396}]}, "tt2": {"tv_results": [{"id": 7}]}, "tt3": {"tv_results": [{"id": 8}]}}

    def get(url, params=None, **_):
        body = finds.get(url.rsplit("/", 1)[-1], {})
        return SimpleNamespace(status_code=200, headers={}, json=lambda: body, raise_for_status=lambda: None)

    _show_tmdb._CACHE.clear()
    monkeypatch.setattr(_show_tmdb.requests, "get", get)
    imdb = {"imdb": "tt35309276"}
    store.write_state("history", {
        "tmdb:95396#s01e01": episode("95396", 1, 2),
        "imdb:tt35309276#s01e01": {**episode("x", 1, 3), "series_title": "Show 95396 (US)", "ids": dict(imdb), "show_ids": dict(imdb)},
        "imdb:tt35309276#s01e02": {**episode("x", 2, 4), "series_title": "Show 95396 (US)", "ids": dict(imdb), "show_ids": dict(imdb)},
        "imdb:tt2#s01e05": {**episode("x", 5, 5), "series_title": "Doctor", "ids": {"imdb": "tt2"}, "show_ids": {"imdb": "tt2"}},
        "imdb:tt3#s01e01": {**episode("x", 1, 6), "series_title": "Doctor", "ids": {"imdb": "tt3"}, "show_ids": {"imdb": "tt3"}},
    })
    data = client.get("/api/tracker/overview/shows?day=2026-11-10").json()
    assert (data["total"], data["unavailable"]) == (2, 1)
    assert [(row["ref"], row["season"], row["episode"]) for row in data["continue"]] == [({"show": "tmdb:95396"}, 1, 3)]
    assert data["continue"][0]["date"] == "2026-11-04T12:00:00Z"
    assert {call[0] for call in meta.calls} == {"95396"}
    store.write_state("history", {**store.read_current("history")["items"],
                                  "title-only": {**episode("x", 9, 7), "series_title": "Show 95396 (US)", "ids": {}, "show_ids": {}}})
    mixed = client.get("/api/tracker/overview/shows?day=2026-11-10").json()
    assert {row["ref"]["show"] for row in mixed["continue"]} == {"tmdb:95396"}
    assert mixed["unavailable"] == 2
    store.save_note("show:tmdb:95396", {"type": "show", "title": "Show 95396", "status": "dropped"})
    dropped = client.get("/api/tracker/overview/shows?day=2026-11-10").json()
    assert dropped["continue"] == [] and dropped["total"] == 2


def test_dropped_aliases_do_not_stop_overview_paging(app, monkeypatch):
    from providers.scrobble import _show_tmdb

    client, store, _, meta = app

    def get(url, params=None, **_):
        imdb = url.rsplit("/", 1)[-1]
        body = {"tv_results": [{"id": 1000 + int(imdb[2:])}]}
        return SimpleNamespace(status_code=200, headers={}, json=lambda: body, raise_for_status=lambda: None)

    _show_tmdb._CACHE.clear()
    monkeypatch.setattr(_show_tmdb.requests, "get", get)
    history = {}
    for n in range(40):
        ids = {"imdb": f"tt{n}"}
        history[f"imdb:tt{n}#s01e01"] = {**episode("x"), "series_title": f"Alias {n}", "ids": dict(ids), "show_ids": dict(ids),
                                         "watched_at": f"2026-11-09T{n // 2:02d}:{n % 2 * 30:02d}:00Z"}
        store.save_note(f"show:tmdb:{1000 + n}", {"type": "show", "title": f"Alias {n}", "status": "dropped"})
    for n in range(3):
        history[f"tmdb:{50 + n}#s01e01"] = {**episode(str(50 + n)), "watched_at": f"2026-10-0{n + 1}T12:00:00Z"}
    store.write_state("history", history)
    data = client.get("/api/tracker/overview/shows?day=2026-11-10").json()
    assert data["total"] == 3
    assert {call[0] for call in meta.calls} == {"50", "51", "52"}


def test_imdb_only_show_lookup_failure_counts_as_unavailable(app, monkeypatch):
    from providers.scrobble import _show_tmdb

    client, store, _, meta = app

    def get(url, params=None, **_):
        raise _show_tmdb.requests.exceptions.ConnectionError("down")

    _show_tmdb._CACHE.clear()
    monkeypatch.setattr(_show_tmdb.requests, "get", get)
    store.write_state("history", {"imdb:tt9#s01e01": {**episode("x"), "series_title": "Gone", "ids": {"imdb": "tt9"}, "show_ids": {"imdb": "tt9"}}})
    data = client.get("/api/tracker/overview/shows?day=2026-11-10").json()
    assert (data["ok"], data["unavailable"]) == (True, 1)
    assert not meta.calls


def test_missing_metadata_and_failed_lookups_are_explicit(app):
    client, store, cfg, meta = app
    store.write_state("history", {"one": episode()})
    cfg["tmdb"] = {}
    assert client.get("/api/tracker/overview").json()["metadata_available"] is False
    assert client.get("/api/tracker/overview/shows").json()["metadata_available"] is False
    assert not meta.calls
    cfg["tmdb"] = {"api_key": "test"}
    meta.lookup = lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError())
    data = client.get("/api/tracker/overview/shows").json()
    assert data["unavailable"] == 1 and data["airing"] == [] and data["continue"] == []


@pytest.mark.parametrize("path", ["/overview", "/overview/shows", "/overview/revision"])
def test_overview_enforces_instance_access_before_loading_data(app, monkeypatch, path):
    client, _, _, meta = app
    monkeypatch.setattr(trackerAPI, "user_can_access_instance", lambda *args: False)
    response = client.get("/api/tracker" + path)
    assert response.status_code == 403
    assert response.json()["detail"] == "profile_scope_denied"
    assert not meta.calls


def test_bulk_watched_coordinates_are_scoped_and_not_capped(app):
    _, store, _, _ = app
    history = {str(i): episode(number=i) for i in range(1, 5101)}
    history["other"] = episode("2", number=9)
    store.write_state("history", history)
    store.write_state("progress", {"pending": episode(number=9999)})
    result = store.watched_episodes(["tmdb:1"])
    assert set(result) == {"tmdb:1"}
    assert len(result["tmdb:1"]) == 5100
    assert (1, 9999) not in result["tmdb:1"]


def test_completed_shows_do_not_fetch_seasons_outside_the_calendar_window():
    meta = Metadata()
    meta.lookup = lambda *args, **kwargs: SimpleNamespace(data={
        "status": "Ended", "in_production": False,
        "seasons": [{"season_number": 1, "episode_count": 10, "air_date": "2020-01-01"}],
        "last_episode_to_air": {"season_number": 1, "air_date": "2020-02-01"},
    }) if kwargs.get("season") is None else pytest.fail("An old completed season should not be fetched")
    assert show_overview({"key": "tmdb:1", "status": "completed"}, {(1, 10)}, meta, DAY) == {"continue": [], "airing": [], "unavailable": 0}


def test_hub_and_tracker_share_the_four_lookup_limit(app):
    _, store, _, meta = app
    store.write_state("history", {str(i): episode(str(i)) for i in range(1, 9)})
    active = peak = 0
    lock, lookup = Lock(), meta.lookup

    def delayed(*args, **kwargs):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        try:
            time.sleep(.005)
            return lookup(*args, **kwargs)
        finally:
            with lock:
                active -= 1

    meta.lookup = delayed

    from services.hub_airing import airing_payload

    indices = [{"rows": [{"_type": "show", "ids": {"tmdb": str(i)}} for i in range(1, 9)]}]
    with ThreadPoolExecutor(max_workers=2) as pool:
        tracker = pool.submit(overview_payload, TrackerStore(store.root), meta, DAY)
        hub = pool.submit(airing_payload, indices, {}, meta, DAY)
        assert tracker.result()["checked"] == hub.result()["checked"] == 8
    assert 1 < peak <= 4


@pytest.mark.parametrize("status", ["Returning Series", "Ended", "Canceled"])
def test_airing_refresh_is_daily_targeted_and_survives_provider_restart(tmp_path, monkeypatch, status):
    clock = [1000.0]
    calls = []
    monkeypatch.setattr(tmdb.time, "time", lambda: clock[0])

    def get(url, **kwargs):
        calls.append(url)
        changed = clock[0] > 87400
        air_date = (DAY + timedelta(days=3 if changed else 1)).isoformat()
        if url.endswith("/tv/7"):
            body = {"id": 7, "name": "Show", "status": status, "in_production": status == "Returning Series",
                    "seasons": [{"season_number": 1, "episode_count": 2, "air_date": "2020-01-01"},
                                {"season_number": 2, "episode_count": 1, "air_date": "2026-11-01"}],
                    "next_episode_to_air": {"season_number": 2, "air_date": air_date}}
        else:
            season = int(url.rsplit("/", 1)[1])
            body = {"id": 70 + season, "season_number": season, "episodes": [
                {"episode_number": 1 if season == 2 else 2, "season_number": season,
                 "air_date": air_date if season == 2 else "2020-01-02"}]}
        return SimpleNamespace(status_code=200, headers={}, json=lambda: body, raise_for_status=lambda: None)

    monkeypatch.setattr(tmdb.requests, "get", get)

    def provider():
        value = tmdb.TmdbProvider(lambda: {"tmdb": {"api_key": "test"}}, lambda _: None)
        value._disk_dir = tmp_path
        return value

    first = provider()
    for season in (None, 1, 2):
        first.lookup("show", "7", season=season)
    calls.clear()
    show = {"key": "tmdb:7", "title": "Show"}
    clock[0] = 87399
    data = show_overview(show, {(1, 1)}, first, DAY)
    assert data["airing"][0]["air_date"] == (DAY + timedelta(days=1)).isoformat()
    assert not calls
    clock[0] = 87401
    data = show_overview(show, {(1, 1)}, first, DAY)
    if status == "Returning Series":
        assert calls == ["https://api.themoviedb.org/3/tv/7", "https://api.themoviedb.org/3/tv/7/season/2"]
        assert data["airing"][0]["air_date"] == (DAY + timedelta(days=3)).isoformat()
    else:
        assert not calls
        assert data["airing"][0]["air_date"] == (DAY + timedelta(days=1)).isoformat()
    calls.clear()
    assert show_overview(show, {(1, 1)}, provider(), DAY) == data
    assert not calls


def test_failed_daily_airing_refresh_keeps_cached_dates():
    meta = Metadata()
    lookup = meta.lookup

    def unavailable(*args, **kwargs):
        if kwargs.get("max_age") is not None:
            raise TimeoutError()
        return lookup(*args, **kwargs)

    meta.lookup = unavailable
    data = show_overview({"key": "tmdb:1"}, {(1, 1)}, meta, DAY)
    assert data["unavailable"] == 1
    assert {(row["season"], row["episode"]) for row in data["airing"]} == {(1, 3), (1, 4), (2, 1)}


def test_overview_hero_prefers_watching_and_scrobbles_then_recent(app, monkeypatch):
    from cw_platform import event_archive

    client, store, _, _ = app
    store.write_state("history", {"one": episode("1"), "two": episode("2", day=2)})
    assert client.get("/api/tracker/overview").json()["hero"]["key"] == "two"
    store.write_state("progress", {"resume": {"type": "movie", "title": "Resume", "ids": {"tmdb": "8"}, "progress_percent": 50, "progress_at": "2026-11-03T12:00:00Z"}})
    assert client.get("/api/tracker/overview").json()["hero"]["key"] == "resume"
    store.save_note("show:tmdb:3", {"type": "show", "title": "Watching", "ids": {"tmdb": "3"}, "status": "watching"})
    assert client.get("/api/tracker/overview").json()["hero"]["title"] == "Watching"
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute("CREATE TABLE events(domain TEXT, event_type TEXT, destination_provider TEXT, destination_instance TEXT, item_key TEXT, created_at INTEGER, media_type TEXT)")
    conn.execute("INSERT INTO events VALUES('scrobble','scrobble_completed','CROSSWATCH','default','tmdb:1',?,'episode')", (int(time.time()) + 60,))
    conn.execute("INSERT INTO events VALUES('scrobble','scrobble_completed','CROSSWATCH','other','tmdb:2',?,'episode')", (int(time.time()) + 120,))
    monkeypatch.setattr(event_archive, "get_conn", lambda: conn)
    try:
        assert client.get("/api/tracker/overview").json()["hero"]["key"] == "one"
    finally:
        conn.close()


@pytest.fixture()
def cached_app(app, tmp_path, monkeypatch):
    from api import view_cache
    from services.view_cache import ViewCache

    client, store, cfg, meta = app
    cache = ViewCache()
    monkeypatch.setattr(view_cache, "AIRING_CACHE", cache)
    monkeypatch.setattr(view_cache, "_context", lambda request, arguments: (tmp_path, "tracker", store.revision))
    yield client, store, cfg, meta, cache
    cache.stop()


def test_background_episode_build_finishes_after_the_request_and_survives_restart(cached_app):
    client, store, _, meta, cache = cached_app
    store.write_state("history", {"one": episode()})
    started, release = Event(), Event()
    lookup = meta.lookup

    def delayed(*args, **kwargs):
        started.set()
        assert release.wait(5)
        return lookup(*args, **kwargs)

    meta.lookup = delayed
    url = "/api/tracker/overview/shows?day=2026-11-10&background=1"
    try:
        assert client.get(url).json()["_view"]["ready"] is False
        assert started.wait(3)
        assert client.get(url).json()["_view"]["ready"] is False
    finally:
        release.set()
    cache.pool.submit(lambda: None).result(timeout=5)
    ready = client.get(url).json()
    assert ready["done"] and ready["airing"]
    calls = len(meta.calls)
    cache.entries.clear()
    assert client.get(url).json()["airing"] == ready["airing"]
    cache.pool.submit(lambda: None).result(timeout=5)
    assert len(meta.calls) == calls


def test_episode_refresh_keeps_saved_results_and_rebuilds_for_new_history(cached_app, monkeypatch):
    client, store, _, _, cache = cached_app
    store.write_state("history", {"one": episode()})
    url = "/api/tracker/overview/shows?day=2026-11-10&background=1"
    client.get(url)
    cache.pool.submit(lambda: None).result(timeout=5)
    saved = client.get(url).json()
    store.save_note("show:tmdb:1", {"type": "show", "status": "dropped"})
    started, release = Event(), Event()
    build = trackerAPI.overview_payload

    def delayed(*args):
        started.set()
        assert release.wait(5)
        return build(*args)

    monkeypatch.setattr(trackerAPI, "overview_payload", delayed)
    cache.mark_stale()
    try:
        stale = client.get(url).json()
        assert stale["airing"] == saved["airing"] and stale["_view"]["refreshing"]
        assert started.wait(3)
    finally:
        release.set()
    cache.pool.submit(lambda: None).result(timeout=5)
    assert client.get(url).json()["airing"] == []
    assert not client.get(url.replace("2026-11-10", "2026-11-11")).json()["_view"]["ready"]
    cache.pool.submit(lambda: None).result(timeout=5)


@pytest.mark.parametrize("failure", ["all", "show", "season", "rate_limit", "server_error"])
def test_failed_episode_refresh_preserves_snapshot(cached_app, monkeypatch, failure):
    client, store, _, meta, cache = cached_app
    store.write_state("history", {"one": episode(), "other": episode(tmdb="2")})
    url = "/api/tracker/overview/shows?day=2026-11-10&background=1"
    client.get(url)
    cache.pool.submit(lambda: None).result(timeout=5)
    saved = client.get(url).json()
    assert saved["continue"] and saved["airing"]
    store.write_state("history", {"two": episode(number=2), "other": episode(tmdb="2")})
    lookup = meta.lookup

    def unavailable(kind, tmdb, *, season=None, **kwargs):
        if tmdb == "1" and failure in {"rate_limit", "server_error"}:
            raise http_error(429 if failure == "rate_limit" else 503)
        if failure == "all" or (tmdb == "1" and (season is None if failure == "show" else season == 1)):
            raise TimeoutError("Metadata request timed out")
        return lookup(kind, tmdb, season=season, **kwargs)

    monkeypatch.setattr(meta, "lookup", unavailable)
    cache.mark_stale()
    client.get(url)
    cache.pool.submit(lambda: None).result(timeout=5)
    failed = client.get(url).json()
    assert failed["continue"] == saved["continue"]
    assert failed["airing"] == saved["airing"]
    assert failed["_view"]["version"] == saved["_view"]["version"]
    assert failed["_view"]["error"] == "refresh_failed"
    cache.entries.clear()
    assert client.get(url).json()["airing"] == saved["airing"]

    monkeypatch.setattr(meta, "lookup", lookup)
    client.get(url + "&retry_view=1")
    cache.pool.submit(lambda: None).result(timeout=5)
    recovered = client.get(url).json()
    assert recovered["_view"]["error"] is None
    assert recovered["_view"]["version"] != saved["_view"]["version"]
    assert next(row for row in recovered["continue"] if row["show_ids"]["tmdb"] == "1")["episode"] == 3


def http_error(status):
    response = tmdb.requests.Response()
    response.status_code = status
    return tmdb.requests.HTTPError(response=response)


@pytest.mark.parametrize("warm", [False, True])
@pytest.mark.parametrize("daily", [False, True])
@pytest.mark.parametrize("missing_season", [None, 1])
def test_missing_metadata_does_not_block_other_episodes(cached_app, monkeypatch, warm, daily, missing_season):
    client, store, _, meta, cache = cached_app
    store.write_state("history", {"one": episode(), "other": episode(tmdb="2")})
    url = "/api/tracker/overview/shows?day=2026-11-10&background=1"
    if warm:
        client.get(url)
        cache.pool.submit(lambda: None).result(timeout=5)
        assert client.get(url).json()["_view"]["ready"]
        store.write_state("history", {"one": episode(number=2), "other": episode(tmdb="2")})
        cache.mark_stale()
    lookup = meta.lookup

    def missing(kind, identifier, *, season=None, **kwargs):
        if identifier == "1" and season == missing_season and (not daily or kwargs.get("max_age") is not None):
            raise http_error(404)
        return lookup(kind, identifier, season=season, **kwargs)

    monkeypatch.setattr(meta, "lookup", missing)
    client.get(url)
    cache.pool.submit(lambda: None).result(timeout=5)
    data = client.get(url).json()
    assert data["_view"]["ready"] and data["_view"]["error"] is None
    assert data["unavailable"] == 1
    assert any(row["show_ids"]["tmdb"] == "2" for row in data["continue"])
    assert any(row["show_ids"]["tmdb"] == "2" for row in data["airing"])
    assert not any(row["show_ids"]["tmdb"] == "1" and (missing_season is None or row["season"] == missing_season)
                   for row in data["continue"] + data["airing"])


def test_all_missing_metadata_publishes_an_empty_snapshot(cached_app, monkeypatch):
    client, store, _, meta, cache = cached_app
    store.write_state("history", {"one": episode()})

    def missing(*args, **kwargs):
        raise http_error(404)

    monkeypatch.setattr(meta, "lookup", missing)
    url = "/api/tracker/overview/shows?day=2026-11-10&background=1"
    client.get(url)
    cache.pool.submit(lambda: None).result(timeout=5)
    data = client.get(url).json()
    assert data["_view"]["ready"] and data["_view"]["error"] is None
    assert data["airing"] == data["continue"] == []
    assert data["unavailable"] == 1
