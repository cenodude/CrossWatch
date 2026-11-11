# tests/test_tracker_month.py
# CrossWatch - Monthly report aggregation, persistent caching and access isolation
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from datetime import datetime, timezone
from threading import Event, Thread
import json
import sqlite3
from zoneinfo import ZoneInfo
import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import trackerAPI
from cw_platform.tracker_store import TrackerStore
from services import tracker_month as month


def movie(day, identifier="1", **extra):
    return {"type": "movie", "title": f"Movie {identifier}", "ids": {"tmdb": identifier}, "watched_at": day, **extra}


def episode(day, number=1):
    return {"type": "episode", "series_title": "Example show", "show_ids": {"tmdb": "2"}, "season": 1, "episode": number, "watched_at": day}


def meta(kind, identifier, locale=None):
    return {"runtime_minutes": 100 if kind == "movie" else 30, "genres": ["Drama", "Mystery"], "images": {"poster": [{"url": f"https://image.tmdb.org/t/p/w780/{identifier}.jpg"}], "backdrop": [{"url": f"https://image.tmdb.org/t/p/w1280/{identifier}.jpg"}]}}


@pytest.fixture(autouse=True)
def isolated_worker(monkeypatch):
    month._STOP.clear()
    monkeypatch.setattr(month, "_busy", lambda: False)
    yield
    deadline = time.monotonic() + 5
    while month._PENDING and time.monotonic() < deadline:
        time.sleep(.01)
    assert not month._PENDING
    month.stop()
    month._FAILURES.clear()


def test_local_month_boundaries_rewatches_and_runtime_coverage(tmp_path):
    store = TrackerStore(tmp_path)
    store.write_state("history", {
        "boundary": movie("2026-09-30T22:30:00Z"),
        "rewatch": movie("2026-10-02T21:00:00Z"),
        "episode": episode("2026-10-03T21:00:00Z"),
        "repeat": episode("2026-10-04T21:00:00Z"),
        "unknown": movie("2026-10-05T21:00:00Z", "3"),
        "outside": movie("2026-10-31T23:30:00Z"),
    })
    calls = []

    def metadata(kind, identifier):
        calls.append((kind, identifier))
        return {} if identifier == "3" else meta(kind, identifier)

    report = month.build_report(store, "2026-10", ZoneInfo("Europe/Amsterdam"), metadata=metadata, now=datetime(2026, 11, 2, tzinfo=timezone.utc))
    assert (report["plays"], report["movies"], report["shows"], report["minutes"], report["timed"]) == (5, 2, 1, 260, 4)
    assert report["first"]["date"].startswith("2026-10-01T00:30")
    assert report["last"]["date"].startswith("2026-10-05")
    assert report["streak"] == 5
    assert len(calls) == 3
    show = next(t for t in report["titles"] if t["type"] == "show")
    assert (show["plays"], show["episodes"], show["days"]) == (2, 1, [3, 4])
    assert report["genres"][0]["count"] == 2
    assert report["days"][0]["movies"] == 1


def test_checked_episode_runtime_wins_and_pair_scopes_do_not_double_count(tmp_path):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"one": episode("2026-10-02T10:00:00Z")})
    TrackerStore(tmp_path, scope="pair-a").write_state("history", {"one": episode("2026-10-02T10:00:00Z")})
    store.save_checks((feature, key, "ok", {"runtime": 51}, raw) for feature, key, _, raw in store.iter_records() if feature == "history")
    report = month.build_report(store, "2026-10", timezone.utc, metadata=meta)
    assert (report["plays"], report["minutes"]) == (1, 51)


def wait_report(store, cfg, key):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        result = month.get_report(store, cfg, key)
        if result["report"] is not None and not result["stale"] and not result["pending"]:
            return result
        time.sleep(.02)
    pytest.fail("Monthly report did not finish")


def test_saved_report_survives_new_store_and_history_edits_refresh_it(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    cfg = {"scheduling": {"timezone": "UTC"}}
    store.write_state("history", {"one": movie("2026-08-01T12:00:00Z")})
    original = month.build_report
    calls = []

    def build(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs, metadata=meta)

    monkeypatch.setattr(month, "build_report", build)
    first = wait_report(store, cfg, "2026-08")
    assert first["report"]["plays"] == 1
    assert month.get_report(TrackerStore(tmp_path), cfg, "2026-08")["generated"] == first["generated"]
    assert len(calls) == 1
    store.write_state("history", {"two": movie("2026-08-02T12:00:00Z"), "three": movie("2026-08-03T12:00:00Z")})
    second = wait_report(store, cfg, "2026-08")
    assert second["report"]["plays"] == 2 and second["report"]["first"]["date"].startswith("2026-08-02")
    assert len(calls) == 2


def test_simultaneous_requests_share_one_background_job(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    store.write_state("history", {})
    entered, release = Event(), Event()
    original = month.build_report
    calls = []

    def build(*args, **kwargs):
        calls.append(1)
        entered.set()
        release.wait(3)
        return original(*args, **kwargs, metadata=meta)

    monkeypatch.setattr(month, "build_report", build)
    try:
        assert month.get_report(store, {}, "2026-08")["pending"]
        assert entered.wait(2)
        for _ in range(10):
            assert month.get_report(store, {}, "2026-08")["pending"]
        assert len(calls) == 1
    finally:
        release.set()
    assert wait_report(store, {}, "2026-08")["report"]["plays"] == 0


def test_daily_generation_only_refreshes_due_months(tmp_path, monkeypatch):
    root = tmp_path / "tracker"
    store = TrackerStore(root)
    store.write_state("history", {})
    cfg = {"crosswatch": {"root_dir": str(root)}, "scheduling": {"timezone": "UTC"}}
    monkeypatch.setattr(month, "_metadata", meta)
    month.run_daily(cfg, tmp_path)
    current = datetime.now(timezone.utc).strftime("%Y-%m")
    first = wait_report(store, cfg, current)
    month.run_daily(cfg, tmp_path)
    assert month.get_report(store, cfg, current)["generated"] == first["generated"]
    with month._cache(store) as conn:
        conn.execute("UPDATE reviews SET generated=generated-86400")
    assert wait_report(store, cfg, current)["generated"] > first["generated"]


def test_review_api_validates_month_and_checks_access_before_scheduling(tmp_path, monkeypatch):
    root = tmp_path / "tracker"
    TrackerStore(root).write_state("history", {})
    monkeypatch.setattr(trackerAPI, "load_config", lambda: {"crosswatch": {"root_dir": str(root)}})
    app = FastAPI()
    app.include_router(trackerAPI.router)
    client = TestClient(app)
    for key in ("2026-13", "2026-1", "bad", "9999-12", "1899-01"):
        assert client.get(f"/api/tracker/overview/month?month={key}").status_code == 400
    assert client.get("/api/tracker/overview/month?provider_instance=missing&month=2026-10").status_code == 404
    monkeypatch.setattr(trackerAPI, "user_can_access_instance", lambda *args: False)
    monkeypatch.setattr(trackerAPI, "month_report", lambda *args: pytest.fail("Unauthorized report was scheduled"))
    assert client.get("/api/tracker/overview/month?month=2026-10").status_code == 403


def test_missing_metadata_and_future_watches_do_not_invent_statistics(tmp_path):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"past": movie("2026-10-01T12:00:00Z"), "future": movie("2026-10-25T12:00:00Z")})
    report = month.build_report(store, "2026-10", timezone.utc, metadata=lambda *_: {}, now=datetime(2026, 10, 10, tzinfo=timezone.utc))
    assert report["plays"] == 1 and report["minutes"] == 0 and report["timed"] == 0
    assert report["elapsed_days"] == 10 and report["current"]
    assert report["genres"] == [] and report["titles"][0]["poster"] == ""


def test_banner_announces_only_completed_nonempty_reports_for_its_tracker(tmp_path):
    import json

    store = TrackerStore(tmp_path / "ann")
    cfg = {"scheduling": {"timezone": "UTC"}}
    assert month.available_month(store, cfg) == ""
    assert not (store.root / "month-reviews.db").exists()
    with month._cache(store) as conn:
        for key, current, plays in (("2025-08", False, 2), ("2025-09", True, 3), ("2025-10", False, 0)):
            conn.execute("INSERT INTO reviews VALUES(?,?,?,?,?,?)", (key, "UTC", month._VERSION, "1", time.time(), json.dumps({"current": current, "plays": plays})))
    assert month.available_month(store, cfg) == "2025-08"
    with month._cache(store) as conn:
        conn.execute("UPDATE reviews SET data=? WHERE month='2025-09'", (json.dumps({"current": False, "plays": 3}),))
    assert month.available_month(store, cfg) == "2025-09"
    assert month.available_month(TrackerStore(tmp_path / "bob"), cfg) == ""


def test_locked_cache_reads_fail_fast_and_wal_keeps_saved_report_readable(tmp_path):
    store = TrackerStore(tmp_path)
    store.write_state("history", {})
    first = wait_report(store, {}, "2026-08")
    conn = sqlite3.connect(tmp_path / "month-reviews.db")
    try:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        conn.execute("BEGIN EXCLUSIVE")
        conn.execute("UPDATE reviews SET generated=0")
        before = time.monotonic()
        assert month.get_report(store, {}, "2026-08")["generated"] == first["generated"]
        assert time.monotonic() - before < .5
        conn.rollback()
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("BEGIN EXCLUSIVE")
        before = time.monotonic()
        result = month.get_report(store, {}, "2026-08")
        assert time.monotonic() - before < .5
        assert result["stale"] and (result["pending"] or result["error"])
    finally:
        conn.rollback()
        conn.close()


def test_unrelated_activity_and_other_month_edits_do_not_rebuild_report(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    history = {"august": movie("2026-08-01T12:00:00Z")}
    store.write_state("history", history)
    original = month.build_report
    calls = []

    def build(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs, metadata=meta)

    monkeypatch.setattr(month, "build_report", build)
    first = wait_report(store, {}, "2026-08")
    scans, publications = [], []
    original_rows, original_publish = month._rows, month._publish

    def rows(*args):
        scans.append(1)
        yield from original_rows(*args)

    def publish(*args):
        publications.append(1)
        return original_publish(*args)

    monkeypatch.setattr(month, "_rows", rows)
    monkeypatch.setattr(month, "_publish", publish)
    for feature in ("progress", "ratings", "watchlist"):
        store.write_state(feature, {"one": movie("2026-10-01T12:00:00Z")})
        assert wait_report(store, {}, "2026-08")["generated"] == first["generated"]
    TrackerStore(tmp_path, scope="pair-a").write_state("history", history)
    assert wait_report(store, {}, "2026-08")["generated"] == first["generated"]
    store.write_state("history", {**history, "october": movie("2026-10-01T12:00:00Z")})
    assert wait_report(store, {}, "2026-08")["generated"] == first["generated"]
    assert len(calls) == 1
    assert scans == publications == []
    store.save_checks((feature, key, "ok", {"runtime": 51}, raw) for feature, key, _, raw in store.iter_records() if feature == "history")
    assert wait_report(store, {}, "2026-08")["report"]["minutes"] == 51
    assert len(calls) == 2


def test_sync_start_interrupts_aggregation_before_remaining_metadata(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    store.write_state("history", {str(i): movie("2026-08-01T12:00:00Z", str(i)) for i in range(5)})
    calls = []

    def metadata(kind, identifier):
        calls.append(identifier)
        monkeypatch.setattr(month, "_busy", lambda: True)
        return meta(kind, identifier)

    with pytest.raises(InterruptedError):
        month.build_report(store, "2026-08", timezone.utc, metadata=metadata)
    assert len(calls) == 1


def test_shutdown_joins_active_work_cancels_queue_and_prevents_publication(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    store.write_state("history", {})
    entered, release, stopped = Event(), Event(), Event()
    original = month.build_report
    calls = []

    def build(*args, **kwargs):
        calls.append(1)
        result = original(*args, **kwargs)
        entered.set()
        release.wait(3)
        return result

    monkeypatch.setattr(month, "build_report", build)
    month.get_report(store, {}, "2026-08")
    assert entered.wait(2)
    month.get_report(store, {}, "2026-09")
    worker = Thread(target=lambda: (month.stop(), stopped.set()))
    worker.start()
    try:
        assert month._STOP.wait(2)
        assert not stopped.is_set()
    finally:
        release.set()
        worker.join(3)
    assert stopped.is_set() and month._POOL is None and not month._PENDING
    assert calls == [1]
    assert month._saved(store, "2026-08", month.review_timezone({})) is None
    month.start(lambda: {}, tmp_path)
    assert wait_report(store, {}, "2026-08")["report"] is not None


def test_cached_metadata_and_locale_are_revalidated_without_expiring_metadata(tmp_path, monkeypatch):
    from api import metaAPI

    store = TrackerStore(tmp_path / "tracker")
    store.write_state("history", {"one": movie("2026-08-01T12:00:00Z")})
    path = tmp_path / "metadata.json"
    monkeypatch.setattr(metaAPI, "_meta_cache_path", lambda *args: path)
    monkeypatch.setattr(month, "_metadata", lambda kind, identifier, locale: {"runtime_minutes": json.loads(path.read_text())["runtime"] if path.exists() else 0, "genres": [locale]})
    cfg = {"metadata": {"locale": "en-US"}}
    first = wait_report(store, cfg, "2026-08")
    assert first["report"]["minutes"] == 0
    path.write_text('{"runtime":120}')
    with month._cache(store) as conn:
        conn.execute("UPDATE review_state SET revision=json_set(revision,'$.checked',0)")
    second = wait_report(store, cfg, "2026-08")
    assert second["report"]["minutes"] == 120
    path.write_text('{"runtime":90}')
    with month._cache(store) as conn:
        conn.execute("UPDATE review_state SET revision=json_set(revision,'$.checked',0)")
    assert wait_report(store, cfg, "2026-08")["report"]["minutes"] == 90
    third = wait_report(store, {"metadata": {"locale": "nl-NL"}}, "2026-08")
    assert third["report"]["genres"][0]["name"] == "nl-NL"
    assert third["generated"] > second["generated"]


def test_corrupt_cache_recovers_without_starving_other_trackers(tmp_path, monkeypatch):
    roots = [tmp_path / "ann", tmp_path / "bob"]
    for root in roots:
        TrackerStore(root).write_state("history", {})
    (roots[0] / "month-reviews.db").write_bytes(b"broken sqlite")
    monkeypatch.setattr(month, "tracker_roots", lambda *args: [(str(i), root) for i, root in enumerate(roots)])
    month.run_daily({}, tmp_path)
    current = datetime.now(month.review_timezone({})).strftime("%Y-%m")
    for root in roots:
        assert wait_report(TrackerStore(root), {}, current)["report"] is not None
    assert list(roots[0].glob("month-reviews.db.corrupt-*"))


def test_unrelated_write_during_generation_does_not_discard_report(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"one": movie("2026-08-01T12:00:00Z")})
    original = month.build_report
    calls = []

    def build(*args, **kwargs):
        calls.append(1)
        result = original(*args, **kwargs, metadata=meta)
        store.write_state("progress", {"one": movie("2026-10-01T12:00:00Z")})
        return result

    monkeypatch.setattr(month, "build_report", build)
    assert wait_report(store, {}, "2026-08")["report"]["plays"] == 1
    assert len(calls) == 1


@pytest.mark.parametrize("stamp,expected", [
    ("2026-03-29T04:59:00+02:00", "2026-03-28T05:00:00+01:00"),
    ("2026-03-29T05:00:00+02:00", "2026-03-29T05:00:00+02:00"),
    ("2026-10-25T04:59:00+01:00", "2026-10-24T05:00:00+02:00"),
    ("2026-10-25T05:00:00+01:00", "2026-10-25T05:00:00+01:00"),
])
def test_daily_slot_follows_local_five_am_across_dst(stamp, expected):
    now = datetime.fromisoformat(stamp).astimezone(ZoneInfo("Europe/Amsterdam"))
    assert month._scheduled_at(now).isoformat() == expected


def test_maintenance_catches_up_once_and_remembers_completed_runs(tmp_path, monkeypatch):
    cfg = {"scheduling": {"timezone": "Europe/Amsterdam"}}
    zone = month.review_timezone(cfg)
    calls = []
    monkeypatch.setattr(month, "run_daily", lambda *args: calls.append(1) or True)
    month._maintenance_tick(cfg, tmp_path, now=datetime(2026, 11, 11, 8, tzinfo=zone))
    assert len(calls) == 1
    month._maintenance_tick(cfg, tmp_path, now=datetime(2026, 11, 11, 20, tzinfo=zone))
    month._maintenance_tick(cfg, tmp_path, now=datetime(2026, 11, 12, 4, 59, tzinfo=zone))
    assert len(calls) == 1
    month._maintenance_tick(cfg, tmp_path, now=datetime(2026, 11, 12, 5, tzinfo=zone))
    assert len(calls) == 2
    month._maintenance_tick(cfg, tmp_path, now=datetime(2026, 11, 15, 8, tzinfo=zone))
    assert len(calls) == 3
    marker = json.loads((tmp_path / "month-review-maintenance.json").read_text())
    assert marker["scheduled"] == datetime(2026, 11, 15, 5, tzinfo=zone).timestamp()


def test_maintenance_retries_idle_pending_and_failed_runs(tmp_path, monkeypatch):
    cfg = {"scheduling": {"timezone": "UTC"}}
    now = datetime(2026, 11, 11, 5, tzinfo=timezone.utc)
    calls = []
    monkeypatch.setattr(month, "run_daily", lambda *args: calls.append(1) or len(calls) > 1)
    monkeypatch.setattr(month, "_busy", lambda: True)
    month._maintenance_tick(cfg, tmp_path, now=now)
    assert not calls
    monkeypatch.setattr(month, "_busy", lambda: False)
    month._maintenance_tick(cfg, tmp_path, now=now)
    assert not (tmp_path / "month-review-maintenance.json").exists()
    month._maintenance_tick(cfg, tmp_path, now=now)
    assert (tmp_path / "month-review-maintenance.json").exists()
    assert len(calls) == 2


def test_daily_cache_refresh_waits_until_five_am(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    store.write_state("history", {})
    cfg = {"scheduling": {"timezone": "UTC"}}
    wait_report(store, cfg, "2026-08")
    checked = datetime(2026, 11, 11, 5, 1, tzinfo=timezone.utc).timestamp()
    with month._cache(store) as conn:
        conn.execute("UPDATE reviews SET generated=?", (checked,))
        conn.execute("UPDATE review_state SET revision=json_set(revision,'$.checked',?)", (checked,))
    clock = [datetime(2026, 11, 12, 4, 59, tzinfo=timezone.utc)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0].astimezone(tz)

    monkeypatch.setattr(month, "datetime", Clock)
    month._STOP.set()
    assert not month.get_report(store, cfg, "2026-08")["stale"]
    clock[0] = datetime(2026, 11, 12, 5, tzinfo=timezone.utc)
    assert month.get_report(store, cfg, "2026-08")["stale"]


def test_slow_publication_does_not_hold_request_lock_and_shutdown_cancels_it(tmp_path, monkeypatch):
    ann, bob = TrackerStore(tmp_path / "ann"), TrackerStore(tmp_path / "bob")
    for store in (ann, bob):
        store.write_state("history", {"one": movie("2026-08-01T12:00:00Z")})
    first = wait_report(ann, {}, "2026-08")
    second = wait_report(bob, {}, "2026-08")
    entered, release, requested, stopped = Event(), Event(), Event(), Event()
    original = month._publish
    results = []

    def publish(*args):
        with month._PUBLICATION:
            entered.set()
            release.wait(3)
        original(*args)

    monkeypatch.setattr(month, "_publish", publish)
    ann.write_state("history", {"two": movie("2026-08-02T12:00:00Z")})
    month.get_report(ann, {}, "2026-08")
    assert entered.wait(2)
    reader = Thread(target=lambda: (results.append(month.get_report(bob, {}, "2026-08")), requested.set()))
    shutdown = Thread(target=lambda: (month.stop(), stopped.set()))
    reader.start()
    try:
        assert requested.wait(.4), "Unrelated request waited for publication"
        assert results[0]["generated"] == second["generated"]
        shutdown.start()
        assert month._STOP.wait(2)
        assert not stopped.is_set()
    finally:
        release.set()
        reader.join(3)
        if shutdown.ident:
            shutdown.join(3)
    assert stopped.is_set()
    assert month._saved(ann, "2026-08", month.review_timezone({}))[2] == first["generated"]


def test_unchanged_daily_validation_updates_only_bookkeeping(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"one": movie("2026-08-01T12:00:00Z")})
    first = wait_report(store, {}, "2026-08")
    with month._cache(store) as conn:
        conn.execute("UPDATE review_state SET revision=json_set(revision,'$.checked',0)")
    original_cache, original_rows = month._cache, month._rows
    statements, scans = [], []

    def cache(*args):
        conn = original_cache(*args)
        conn.set_trace_callback(statements.append)
        return conn

    def rows(*args):
        scans.append(1)
        yield from original_rows(*args)

    monkeypatch.setattr(month, "_cache", cache)
    monkeypatch.setattr(month, "_rows", rows)
    assert wait_report(store, {}, "2026-08")["generated"] == first["generated"]
    writes = [sql for sql in statements if sql.startswith(("INSERT", "UPDATE", "DELETE"))]
    assert len(writes) == 1 and writes[0].startswith("INSERT OR REPLACE INTO review_state ")
    assert scans == [1]


def test_wal_recovery_does_not_schedule_a_duplicate_build(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    store.write_state("history", {})
    first = wait_report(store, {}, "2026-08")
    original = month._saved

    def locked(*args):
        error = sqlite3.OperationalError("database is locked")
        error.sqlite_errorcode = sqlite3.SQLITE_BUSY_RECOVERY
        raise error

    monkeypatch.setattr(month, "_saved", locked)
    result = month.get_report(store, {}, "2026-08")
    assert result["pending"] and result["stale"] and not month._PENDING
    assert result["generated"] == first["generated"]
    assert result["report"] == first["report"]
    assert month.get_report(TrackerStore(tmp_path / "other"), {}, "2026-08")["report"] is None
    monkeypatch.setattr(month, "_saved", original)
    assert wait_report(store, {}, "2026-08")["generated"] == first["generated"]


def test_history_revision_tracks_moved_dates_runtime_removal_and_noop_checks(tmp_path):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"one": movie("2026-08-01T12:00:00Z")})
    august = month._revision(store, "2026-08", timezone.utc)
    october = month._revision(store, "2026-10", timezone.utc)
    store.write_state("history", {"one": movie("2026-10-15T12:00:00Z")})
    assert month._revision(store, "2026-08", timezone.utc) != august
    assert month._revision(store, "2026-10", timezone.utc) != october
    records = list(store.iter_records())
    store.save_checks((f, k, "ok", {"runtime": 51}, raw) for f, k, _, raw in records)
    checked = month._revision(store, "2026-10", timezone.utc)
    store.save_checks((f, k, "ok", {"runtime": 51, "note": "new check"}, raw) for f, k, _, raw in records)
    assert month._revision(store, "2026-10", timezone.utc) == checked
    store.save_checks((f, k, "ok", {}, raw) for f, k, _, raw in records)
    assert month._revision(store, "2026-10", timezone.utc) != checked


def test_existing_tracker_upgrade_seeds_history_revisions_without_losing_history(tmp_path):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"one": movie("2026-08-01T12:00:00Z")})
    with sqlite3.connect(store.db_path) as conn:
        for table in ("items", "checks"):
            for event in ("insert", "delete", "update"):
                conn.execute(f"DROP TRIGGER history_revision_{table}_{event}")
        conn.execute("DROP TABLE history_revisions")
        conn.execute("PRAGMA user_version=6")
    assert wait_report(store, {}, "2026-08")["report"]["plays"] == 1
    assert month._revision(store, "2026-08", timezone.utc)
    store.write_state("history", {})
    assert wait_report(store, {}, "2026-08")["report"]["plays"] == 0
