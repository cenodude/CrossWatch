# tests/test_tracker_wrapped.py
# CrossWatch - Yearly aggregation, background generation, scheduling and profile isolation
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from contextlib import closing
from datetime import datetime, timezone
from threading import Event
from zoneinfo import ZoneInfo
import json
import sqlite3
import time

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from api import trackerAPI
from cw_platform.tracker_store import TrackerStore
from services import tracker_month as worker


def movie(stamp, identifier="1"):
    return {"type": "movie", "title": "Example", "ids": {"tmdb": identifier}, "watched_at": stamp}


def episode(stamp, number=1):
    return {"type": "episode", "series_title": "Series", "show_ids": {"tmdb": "2"}, "season": 1, "episode": number, "watched_at": stamp}


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    worker._STOP.clear()
    monkeypatch.setattr(worker, "_busy", lambda: False)
    monkeypatch.setattr(worker, "_LOAD_CONFIG", None)
    monkeypatch.setattr(worker, "_metadata", lambda kind, *args: {"runtime_minutes": 100 if kind == "movie" else 30})
    yield
    worker.stop()
    worker._STOP.clear()
    worker._FAILURES.clear()


def ready(store, cfg, year):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        result = worker.get_report(store, cfg, year, yearly=True)
        if result["report"] is not None and not result["stale"] and not result["pending"]:
            return result
        time.sleep(.02)
    pytest.fail("Wrapped did not finish")


def test_year_boundaries_leap_day_cross_month_streak_and_replays(tmp_path):
    store = TrackerStore(tmp_path)
    store.write_state("history", {
        "first": movie("2023-12-31T23:30:00Z"),
        "leap": movie("2024-02-28T23:30:00Z"),
        "march": episode("2024-03-01T18:00:00Z"),
        "repeat": episode("2024-03-02T18:00:00Z"),
        "next": episode("2024-03-03T18:00:00Z", 2),
        "last": movie("2024-12-31T22:30:00Z", "3"),
        "outside": movie("2024-12-31T23:30:00Z"),
    })
    report = worker.build_report(store, "2024", ZoneInfo("Europe/Amsterdam"), now=datetime(2025, 1, 2, tzinfo=timezone.utc))
    assert len(report["days"]) == 366
    assert report["days"][59]["date"] == "2024-02-29"
    assert (report["plays"], report["minutes"], report["movies"], report["shows"]) == (6, 390, 2, 1)
    assert (report["streak"], report["streak_start"], report["streak_end"]) == (4, "2024-02-29", "2024-03-03")
    assert report["replays"] == 2
    assert report["months"][2]["plays"] == 3
    assert report["hours"][19] == 3
    assert report["weekdays"][5:7] == [1, 1]
    assert report["first"]["date"].startswith("2024-01-01")
    assert report["last"]["date"].startswith("2024-12-31")


def test_year_so_far_excludes_future_and_handles_unknown_metadata(tmp_path):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"seen": movie("2024-01-10T10:00:00Z"), "later": movie("2024-12-20T10:00:00Z")})
    report = worker.build_report(store, "2024", timezone.utc, now=datetime(2024, 2, 29, tzinfo=timezone.utc), metadata=lambda *_: {})
    assert report["current"] and report["elapsed_days"] == 60
    assert (report["plays"], report["timed"], report["minutes"]) == (1, 0, 0)
    assert report["titles"][0]["poster"] == ""


def test_month_and_wrapped_share_one_worker_and_open_without_waiting(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"seen": movie("2024-01-10T10:00:00Z")})
    entered, release = Event(), Event()
    original = worker.build_report
    builds = []

    def build(*args, **kwargs):
        builds.append(args[1])
        entered.set()
        assert release.wait(3)
        return original(*args, **kwargs)

    monkeypatch.setattr(worker, "build_report", build)
    try:
        assert worker.get_report(store, {}, "2024", yearly=True)["pending"]
        assert entered.wait(2)
        pool = worker._POOL
        start = time.monotonic()
        for _ in range(10):
            assert worker.get_report(store, {}, "2024", yearly=True)["pending"]
        assert worker.get_report(store, {}, "2024-01")["pending"]
        assert time.monotonic() - start < .5
        assert builds == ["2024"] and worker._POOL is pool
    finally:
        release.set()
    assert ready(store, {}, "2024")["report"]["plays"] == 1


def test_disabled_yearly_option_is_independent_and_keeps_saved_reports(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"seen": movie("2024-01-10T10:00:00Z")})
    cfg = {"crosswatch": {"root_dir": str(tmp_path), "month_review": False, "wrapped": True}}
    saved = ready(store, cfg, "2024")
    assert worker.get_report(store, cfg, "2024-01")["disabled"]
    cfg["crosswatch"]["wrapped"] = False
    monkeypatch.setattr(worker, "_saved", lambda *args: pytest.fail("Disabled report was read"))
    assert worker.get_report(store, cfg, "2024", yearly=True)["disabled"]
    with closing(worker._readonly(tmp_path / "month-reviews.db")) as conn:
        assert conn.execute("SELECT generated FROM reviews WHERE month='2024'").fetchone()[0] == saved["generated"]


def test_unrelated_updates_do_not_scan_or_write_yearly_cache(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    history = {"seen": movie("2024-01-10T10:00:00Z")}
    store.write_state("history", history)
    first = ready(store, {}, "2024")
    monkeypatch.setattr(worker, "_rows", lambda *args: pytest.fail("Unrelated change scanned history"))
    monkeypatch.setattr(worker, "_publish", lambda *args: pytest.fail("Unrelated change wrote cache"))
    for feature in ("progress", "ratings", "watchlist"):
        store.write_state(feature, {"different": movie("2024-03-01T10:00:00Z")})
        assert ready(store, {}, "2024")["generated"] == first["generated"]
    store.write_state("history", {**history, "other_year": movie("2025-05-01T10:00:00Z")})
    assert ready(store, {}, "2024")["generated"] == first["generated"]


def test_completed_year_daily_validation_does_not_rewrite_unchanged_report(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"seen": movie("2024-01-10T10:00:00Z")})
    first = ready(store, {}, "2024")
    with closing(worker._cache(store)) as conn, conn:
        conn.execute("UPDATE review_state SET revision=json_set(revision,'$.checked',0)")
    publications = []
    original = worker._publish

    def publish(*args):
        publications.append(args[-1])
        return original(*args)

    monkeypatch.setattr(worker, "_publish", publish)
    monkeypatch.setattr(worker, "build_report", lambda *args, **kwargs: pytest.fail("Unchanged year rebuilt"))
    assert ready(store, {}, "2024")["generated"] == first["generated"]
    assert publications == [None]


def test_lock_and_sync_keep_last_year_available_and_recover(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"seen": movie("2024-01-10T10:00:00Z")})
    first = ready(store, {}, "2024")
    store.write_state("history", {"seen": movie("2024-01-10T10:00:00Z"), "new": movie("2024-03-01T10:00:00Z")})
    monkeypatch.setattr(worker, "_busy", lambda: True)
    with closing(sqlite3.connect(tmp_path / "month-reviews.db")) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE reviews SET generated=0")
        start = time.monotonic()
        result = worker.get_report(store, {}, "2024", yearly=True)
        assert time.monotonic() - start < .5
        assert result["report"] == first["report"]
        assert result["stale"] and result["pending"]
    monkeypatch.setattr(worker, "_busy", lambda: False)
    assert ready(store, {}, "2024")["report"]["plays"] == 2


def test_wrapped_checkpoint_obeys_its_own_toggle(tmp_path, monkeypatch):
    cfg = {"crosswatch": {"root_dir": str(tmp_path), "month_review": False, "wrapped": True}}
    monkeypatch.setattr(worker, "_LOAD_CONFIG", lambda: cfg)
    monkeypatch.setattr(worker._WORK, "root", tmp_path, raising=False)
    monkeypatch.setattr(worker._WORK, "option", "wrapped", raising=False)
    monkeypatch.setattr(worker._WORK, "checked", 0, raising=False)
    worker._checkpoint()
    cfg["crosswatch"]["wrapped"] = False
    worker._WORK.checked = 0
    with pytest.raises(InterruptedError):
        worker._checkpoint()


def test_banner_is_january_only_completed_nonempty_and_scoped(tmp_path, monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2025, 1, 10, tzinfo=timezone.utc).astimezone(tz)

    monkeypatch.setattr(worker, "datetime", Clock)
    store = TrackerStore(tmp_path / "ann")
    cfg = {"scheduling": {"timezone": "UTC"}}
    assert worker.available_wrapped(store, cfg) == {}
    with closing(worker._cache(store)) as conn, conn:
        for period, current, plays in (("2024", True, 3), ("2024-12", False, 2)):
            worker._publish(conn, period, "UTC", "{}", 1, json.dumps({"current": current, "plays": plays, "titles": [{"poster": "poster.jpg"}]}))
    assert worker.available_wrapped(store, cfg) == {}
    with closing(worker._cache(store)) as conn, conn:
        worker._publish(conn, "2024", "UTC", "{}", 2, json.dumps({"current": False, "plays": 3, "titles": [{"poster": "poster.jpg"}]}))
    assert worker.available_wrapped(store, cfg) == {"year": "2024", "posters": ["poster.jpg"]}
    assert worker.available_month(store, cfg) == "2024-12"
    assert worker.available_wrapped(TrackerStore(tmp_path / "bob"), cfg) == {}
    with closing(worker._cache(store)) as conn, conn:
        worker._publish(conn, "2024", "UTC", "{}", 3, json.dumps({"current": False, "plays": 0, "titles": []}))
    assert worker.available_wrapped(store, cfg) == {}
    with closing(worker._cache(store)) as conn, conn:
        worker._publish(conn, "2024", "UTC", "{}", 4, json.dumps({"current": False, "plays": 5, "titles": []}))
    monkeypatch.setattr(Clock, "now", classmethod(lambda cls, tz=None: cls(2025, 2, 1, tzinfo=timezone.utc).astimezone(tz)))
    assert worker.available_wrapped(store, cfg) == {}


def test_banner_preview_migrates_once_and_polling_never_reads_reports(tmp_path, monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2025, 1, 10, tzinfo=timezone.utc).astimezone(tz)

    monkeypatch.setattr(worker, "datetime", Clock)
    store = TrackerStore(tmp_path)
    cfg = {"scheduling": {"timezone": "UTC"}}
    report = {"current": False, "plays": 10, "titles": [{"poster": "one.jpg"}, {"poster": ""}, {"poster": "three.jpg"}], "padding": "x" * 2_000_000}
    with closing(worker._cache(store)) as conn, conn:
        conn.execute("DROP TABLE wrapped_previews")
        conn.execute("INSERT INTO reviews VALUES(?,?,?,?,?,?)", ("2024", "UTC", worker._VERSION, "{}", 1, json.dumps(report)))
    with closing(worker._cache(store)) as conn:
        assert conn.execute("SELECT length(posters) FROM wrapped_previews").fetchone()[0] < 100
        saved = conn.execute("SELECT data,generated FROM reviews").fetchone()
    with closing(worker._cache(store)) as conn:
        assert conn.total_changes == 0
        assert conn.execute("SELECT data,generated FROM reviews").fetchone() == saved
    original = worker._readonly
    denied = []

    def readonly(path):
        conn = original(path)

        def authorize(action, table, column, *_):
            if action == sqlite3.SQLITE_READ and table == "reviews":
                denied.append(column)
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        conn.set_authorizer(authorize)
        return conn

    monkeypatch.setattr(worker, "_readonly", readonly)
    for _ in range(10):
        assert worker.available_wrapped(store, cfg) == {"year": "2024", "posters": ["one.jpg", "three.jpg"]}
    assert denied == []


def test_banner_preview_and_report_publish_atomically(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    old = json.dumps({"current": False, "plays": 1, "titles": [{"poster": "old.jpg"}]})
    new = json.dumps({"current": False, "plays": 2, "titles": [{"poster": "new.jpg"}]})
    with closing(worker._cache(store)) as conn:
        worker._publish(conn, "2024", "UTC", "{}", 1, old)
        checkpoints = 0

        def interrupt():
            nonlocal checkpoints
            checkpoints += 1
            if checkpoints == 2:
                raise InterruptedError()

        monkeypatch.setattr(worker, "_checkpoint", interrupt)
        with pytest.raises(InterruptedError):
            worker._publish(conn, "2024", "UTC", "{}", 2, new)
        assert conn.execute("SELECT data FROM reviews").fetchone()[0] == old
        assert json.loads(conn.execute("SELECT posters FROM wrapped_previews").fetchone()[0]) == ["old.jpg"]


def test_daily_scheduler_respects_independent_options_and_configuration_changes(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path / "tracker")
    store.write_state("history", {})
    cfg = {"crosswatch": {"root_dir": str(store.root), "month_review": False, "wrapped": True}}
    calls = []

    def report(store, cfg, period, **kwargs):
        calls.append((period, kwargs))
        return {"pending": False, "stale": False, "error": None}

    monkeypatch.setattr(worker, "get_report", report)
    worker._maintenance_tick(cfg, tmp_path)
    worker._maintenance_tick(cfg, tmp_path)
    assert calls == [(str(datetime.now(worker.review_timezone(cfg)).year - 1), {"yearly": True})]
    cfg["crosswatch"].update(month_review=True, wrapped=False)
    worker._maintenance_tick(cfg, tmp_path)
    assert len(calls) == 3 and all(len(period) == 7 for period, _ in calls[1:])


def test_wrapped_api_validates_year_and_authorizes_before_generation(tmp_path, monkeypatch):
    cfg = {"crosswatch": {"root_dir": str(tmp_path), "instances": {"ann": {"root_dir": str(tmp_path / "ann")}, "bob": {"root_dir": str(tmp_path / "bob")}}}}
    monkeypatch.setattr(trackerAPI, "load_config", lambda: cfg)
    monkeypatch.setattr(trackerAPI, "user_can_access_instance", lambda _cfg, _user, _provider, instance: instance == "ann")
    app = FastAPI()

    @app.middleware("http")
    async def user(request: Request, call_next):
        request.state.cw_user = {"username": "ann", "is_admin": False, "profile_id": "ann"}
        return await call_next(request)

    app.include_router(trackerAPI.router)
    client = TestClient(app)
    for year in ("bad", "1899", "9999", "20-1", "-123"):
        assert client.get(f"/api/tracker/overview/wrapped?provider_instance=ann&year={year}").status_code == 400
    calls = []

    def report(store, config, year, **kwargs):
        calls.append((store.root, year, kwargs))
        return {"ok": True, "year": year}

    monkeypatch.setattr(trackerAPI, "month_report", report)
    assert client.get("/api/tracker/overview/wrapped?provider_instance=bob&year=2024").status_code == 403
    assert client.get("/api/tracker/overview/wrapped?provider_instance=default&year=2024").status_code == 403
    assert calls == []
    assert client.get("/api/tracker/overview/wrapped?provider_instance=ann&year=2024").status_code == 200
    assert calls == [(tmp_path / "ann", "2024", {"yearly": True})]


def test_unknown_episode_numbers_do_not_invent_replays(tmp_path):
    store = TrackerStore(tmp_path)
    events = {"one": episode("2024-03-01T10:00:00Z"), "two": episode("2024-03-02T10:00:00Z")}
    for row in events.values():
        row.pop("episode")
    store.write_state("history", events)
    report = worker.build_report(store, "2024", timezone.utc)
    assert report["plays"] == 2
    assert report["replays"] == 0
    assert report["titles"][0]["episodes"] == 0
