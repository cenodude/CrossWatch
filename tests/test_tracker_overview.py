# tests/test_tracker_overview.py
# CrossWatch - Overview history, resumable items, episode dates and instance access
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import asyncio
import json
import sqlite3
import time
from datetime import date, timedelta
from threading import Event, Lock
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import trackerAPI
from cw_platform.tracker_store import TrackerStore
from services.tracker_overview import cached_overview, overview_events, show_overview
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


def test_continue_excludes_completed_dropped_finished_and_stale_progress(app):
    client, store, _, _ = app
    rows = {str(i): {"type": "movie", "title": f"Movie {i}", "ids": {"tmdb": str(i)}, "progress_percent": 50, "progress_at": "2026-11-09T12:00:00Z"} for i in range(1, 6)}
    rows["4"]["progress_percent"] = 100
    store.write_state("progress", rows)
    store.save_note("movie:tmdb:1", {"type": "movie", "title": "Movie 1", "status": "dropped"})
    store.save_note("movie:tmdb:2", {"type": "movie", "title": "Movie 2", "status": "completed"})
    store.write_state("history", {"3": {**rows["3"], "watched_at": "2026-11-10T12:00:00Z"}})
    assert [row["key"] for row in client.get("/api/tracker/overview").json()["continue"]] == ["5"]


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


def test_shows_stream_in_one_request_and_dropped_titles_are_not_queried(app, monkeypatch):
    client, store, _, meta = app
    store.write_state("history", {str(i): episode(str(i), day=i) for i in range(1, 7)})
    store.save_note("show:tmdb:6", {"type": "show", "title": "Show 6", "status": "dropped"})
    monkeypatch.setattr(TrackerStore, "related", lambda *args, **kwargs: pytest.fail("The stream must use bulk episode coordinates"))
    response = client.get("/api/tracker/overview/shows?day=2026-11-10")
    assert response.headers["content-type"].startswith("application/x-ndjson")
    events = [json.loads(line) for line in response.text.splitlines()]
    assert events[0]["total"] == 5 and events[0]["checked"] == 4
    assert events[-1]["checked"] == 5 and events[-1]["done"] is True
    assert sum(len(event["continue"]) for event in events) == 5
    assert "6" not in {call[0] for call in meta.calls}
    assert client.get("/api/tracker/overview/shows?day=2026-99-01").status_code == 400


def test_missing_metadata_and_failed_lookups_are_explicit(app):
    client, store, cfg, meta = app
    store.write_state("history", {"one": episode()})
    cfg["tmdb"] = {}
    assert client.get("/api/tracker/overview").json()["metadata_available"] is False
    assert client.get("/api/tracker/overview/shows").json()["metadata_available"] is False
    assert not meta.calls
    cfg["tmdb"] = {"api_key": "test"}
    meta.lookup = lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError())
    data = json.loads(client.get("/api/tracker/overview/shows").text.splitlines()[-1])
    assert data["unavailable"] == 1 and data["airing"] == [] and data["continue"] == []


@pytest.mark.parametrize("path", ["/overview", "/overview/shows", "/overview/revision"])
def test_overview_enforces_instance_access_before_loading_data(app, monkeypatch, path):
    client, _, _, meta = app
    monkeypatch.setattr(trackerAPI, "user_can_access_instance", lambda *args: False)
    response = client.get("/api/tracker" + path)
    assert response.status_code == 403
    assert response.json()["detail"] == "profile_scope_denied"
    assert not meta.calls


def test_stream_stops_scheduling_on_disconnect(app):
    _, store, _, meta = app
    store.write_state("history", {str(i): episode(str(i)) for i in range(1, 41)})

    async def run():
        stopped = False

        async def disconnected():
            return stopped

        stream = overview_events(store, meta, DAY, disconnected)
        first = json.loads(await anext(stream))
        assert first["checked"] == 4
        calls = len(meta.calls)
        stopped = True
        assert [event async for event in stream] == []
        assert len(meta.calls) == calls

    asyncio.run(run())


def test_cancelled_lookup_does_not_start_another_season():
    meta, cancelled = Metadata(), Event()
    lookup = meta.lookup

    def cancel_after_show(*args, **kwargs):
        result = lookup(*args, **kwargs)
        cancelled.set()
        return result

    meta.lookup = cancel_after_show
    show_overview({"key": "tmdb:1"}, {(1, 1)}, meta, DAY, cancelled)
    assert len(meta.calls) == 1


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


def test_concurrent_streams_share_the_four_lookup_limit(app):
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

    async def run():
        async def disconnected(): return False
        async def scan():
            return [json.loads(line) async for line in overview_events(TrackerStore(store.root), meta, DAY, disconnected)]
        streams = await asyncio.gather(scan(), scan())
        assert all(events[-1]["checked"] == 8 and events[-1]["done"] for events in streams)

    asyncio.run(run())
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


def test_finished_overview_reuses_results_until_revision_or_day_changes(app, monkeypatch):
    client, store, _, meta = app
    store.write_state("history", {"one": episode()})

    def fetch(day=DAY):
        response = client.get(f"/api/tracker/overview/shows?day={day.isoformat()}")
        assert response.status_code == 200
        return [json.loads(line) for line in response.text.splitlines()]

    first = fetch()
    calls = len(meta.calls)
    assert calls > 0
    with monkeypatch.context() as patch:
        from services import tracker_overview
        patch.setattr(tracker_overview, "_inputs", lambda *args: pytest.fail("Cached results must skip rebuilding the show list"))
        assert fetch() == first
    assert len(meta.calls) == calls
    data = client.get(f"/api/tracker/overview?day={DAY.isoformat()}").json()
    assert data["shows"] == first[0]
    revision = client.get("/api/tracker/overview/revision").json()["revision"]
    store.save_note("show:tmdb:1", {"type": "show", "title": "Show 1", "status": "dropped"})
    assert store.revision() != revision
    assert cached_overview(store, DAY) is None
    assert fetch()[0]["airing"] == []
    assert cached_overview(store, DAY + timedelta(days=1)) is None


def test_failed_metadata_is_not_saved_as_a_finished_overview(app):
    client, store, _, meta = app
    store.write_state("history", {"one": episode()})
    meta.lookup = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("offline"))
    client.get(f"/api/tracker/overview/shows?day={DAY.isoformat()}")
    assert cached_overview(store, DAY) is None


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


def test_overview_cache_is_scoped_and_rejects_results_changed_during_build(app, tmp_path):
    from services.tracker_overview import _save_overview

    _, store, _, _ = app
    store.write_state("history", {"one": episode()})
    result = {"continue": [], "airing": [], "done": True}
    revision = store.revision()
    _save_overview(store, DAY, revision, result)
    assert cached_overview(store, DAY) == result
    assert cached_overview(TrackerStore(tmp_path / "other"), DAY) is None
    store.write_state("history", {"two": episode(number=2)})
    _save_overview(store, DAY, revision, result)
    assert cached_overview(store, DAY) is None
