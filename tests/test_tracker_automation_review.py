# tests/test_tracker_automation_review.py
# CrossWatch - Bounded tracker review, bulk queues and automatic change retention
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from cw_platform import tracker_store
from cw_platform.tracker_store import TrackerStore
from services import tracker_automation as worker
from test_tracker_notes import api, base, MOVIE, EPISODE


def movies(count):
    return {str(n): {**MOVIE, "ids": {"tmdb": str(n)}, "title": f"Movie {n}"} for n in range(1, count + 1)}


def historical(root, count):
    store = TrackerStore(root)
    store.configure_automation(False)
    store.write_state("history", movies(count))
    store.configure_automation(True)
    return store


def attach(monkeypatch, store):
    monkeypatch.setattr(worker, "tracker_roots", lambda *args: [("default", store.root)])
    monkeypatch.setattr(worker, "busy", lambda: False)
    monkeypatch.setattr(worker, "cached_metadata", lambda *args: pytest.fail("Movies and protected titles need no metadata"))


def test_bulk_deduplicates_and_tracks_old_and_new_titles(tmp_path):
    store = TrackerStore(tmp_path)
    data = {str(n): {**EPISODE, "episode": n} for n in range(1, 201)}
    assert store.write_state("history", data)
    initial = dict(store.pending_statuses())
    assert set(initial) == {"show:tmdb:95396"}
    assert store.write_state("history", data)
    assert dict(store.pending_statuses()) == initial
    changed = {key: {**item, "show_ids": {"tmdb": "123"}} for key, item in data.items()}
    assert store.write_state("history", changed)
    pending = dict(store.pending_statuses())
    assert set(pending) == {"show:tmdb:95396", "show:tmdb:123"}
    assert pending["show:tmdb:95396"] != initial["show:tmdb:95396"]
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute("SELECT value FROM meta WHERE key='status_bulk'").fetchone() is None
    assert store.write_state("history", {})
    assert dict(store.pending_statuses())["show:tmdb:123"] != pending["show:tmdb:123"]


def test_bulk_failure_rolls_back_queue_and_flag(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    assert store.write_state("history", movies(120))
    before, pending = store.read_current("history"), store.pending_statuses(200)
    def fail(conn):
        raise RuntimeError("Synthetic failure after writes")
    with monkeypatch.context() as patch:
        patch.setattr(TrackerStore, "_bump", staticmethod(fail))
        assert not store.write_state("history", movies(300))
    assert store.read_current("history") == before
    assert store.pending_statuses(200) == pending
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute("SELECT value FROM meta WHERE key='status_bulk'").fetchone() is None
    assert store.write_state("history", movies(300))
    assert len(store.pending_statuses(500)) == 300


def test_disabled_bulk_does_not_collect_titles_or_queue(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    store.configure_automation(False)
    monkeypatch.setattr(TrackerStore, "_automatic_key", staticmethod(lambda *args: pytest.fail("Disabled import must not collect titles")))
    assert store.write_state("history", movies(200))
    assert store.pending_statuses() == []


def test_bulk_flag_is_not_visible_to_other_connections_and_later_write_queues(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    store.write_state("history", {})
    entered, release = Event(), Event()
    original = TrackerStore._connect
    def wait_during_bulk():
        if not entered.is_set():
            entered.set()
            assert release.wait(5)
        return 1
    def connect(self):
        conn = original(self)
        conn.create_function("test_pause_bulk", 0, wait_during_bulk)
        return conn
    monkeypatch.setattr(TrackerStore, "_connect", connect)
    with store._db() as conn:
        conn.execute("CREATE TRIGGER test_bulk_pause AFTER INSERT ON items WHEN NEW.item_key='1' BEGIN SELECT test_pause_bulk(); END")
    with ThreadPoolExecutor(max_workers=2) as pool:
        bulk = pool.submit(store.write_state, "history", movies(200))
        assert entered.wait(5)
        try:
            with sqlite3.connect(store.db_path) as conn:
                assert conn.execute("SELECT value FROM meta WHERE key='status_bulk'").fetchone() is None
            later = pool.submit(TrackerStore(tmp_path).change_items, "history", [(None, "live", {**MOVIE, "ids": {"tmdb": "9999"}})], action="add")
        finally:
            release.set()
        assert bulk.result() and later.result()
    assert len(store.pending_statuses(300)) == 201
    assert "movie:tmdb:9999" in dict(store.pending_statuses(300))


def test_review_scan_is_bounded_deduplicated_and_survives_restart(tmp_path):
    store = historical(tmp_path, 1)
    store.configure_automation(False)
    store.write_state("history", {str(n).zfill(5): {**EPISODE, "episode": n} for n in range(1, 1101)})
    store.configure_automation(True)
    assert store.review_status()["state"] == "idle" and not store.pending_statuses()
    assert store.start_review()["state"] == "scanning"
    with pytest.raises(ValueError, match="review_running"):
        store.start_review()
    store.scan_review(5000)
    assert store.review_status()["scanned"] == 1000
    assert store.review_status()["titles"] == 1
    store = TrackerStore(tmp_path)
    store.scan_review()
    assert store.review_status()["state"] == "processing"
    assert store.review_status()["scanned"] == 1100
    assert store.review_pending() == ["show:tmdb:95396"]
    store.evaluate_status("show:tmdb:95396", review=True)
    assert store.review_status()["state"] == "complete"
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute("SELECT due FROM status_pending").fetchone()[0] > time.time() + 86000


def test_review_worker_prioritizes_live_history_and_keeps_budget(tmp_path, monkeypatch):
    store = historical(tmp_path, 300)
    attach(monkeypatch, store)
    store.start_review()
    worker.run_once({}, tmp_path)
    assert store.review_status()["processed"] == 250
    data = movies(300)
    data["fresh"] = {**MOVIE, "ids": {"tmdb": "9999"}}
    store.write_state("history", data)
    calls = []
    evaluate = TrackerStore.evaluate_status
    def counted(self, key, *args, **kwargs):
        calls.append((key, kwargs.get("review", False)))
        return evaluate(self, key, *args, **kwargs)
    monkeypatch.setattr(TrackerStore, "evaluate_status", counted)
    worker.run_once({}, tmp_path)
    assert calls[0] == ("movie:tmdb:9999", False)
    assert len(calls) == 51
    assert store.review_status()["state"] == "complete"
    assert store.review_status()["processed"] == 300


def test_live_work_on_other_instance_precedes_review(tmp_path, monkeypatch):
    first, second = historical(tmp_path / "first", 40), historical(tmp_path / "second", 1)
    first.start_review()
    second.write_state("history", movies(40))
    monkeypatch.setattr(worker, "tracker_roots", lambda *args: [("default", first.root), ("other", second.root)])
    monkeypatch.setattr(worker, "busy", lambda: False)
    worker.run_once({}, tmp_path)
    assert first.review_status()["scanned"] == 0
    assert len(second.pending_statuses(100)) == 14


def test_review_preserves_manual_legacy_and_new_synced_drop(tmp_path, monkeypatch):
    store = historical(tmp_path, 3)
    attach(monkeypatch, store)
    store.save_note("movie:tmdb:1", {"type": "movie", "status": ""}, origin="manual")
    store.save_note("movie:tmdb:2", {"type": "movie", "status": "on_hold"})
    store.start_review()
    store.scan_review()
    store.save_note("movie:tmdb:3", {"type": "movie", "status": "dropped"}, origin="sync")
    worker.run_once({}, tmp_path)
    assert store.review_status()["state"] == "complete"
    assert not store.note("movie:tmdb:1")["status"]
    assert store.status_control("movie:tmdb:1")["manual"] is True
    assert store.note("movie:tmdb:2")["status"] == "on_hold"
    assert store.note("movie:tmdb:3")["status"] == "dropped"
    assert store.status_control("movie:tmdb:3")["manual"] is False


@pytest.mark.parametrize("stop", ["cancel", "disable", "clear"])
def test_stopped_review_rejects_already_loaded_work(tmp_path, stop):
    store = historical(tmp_path, 2)
    store.start_review()
    store.scan_review()
    key = store.review_pending()[0]
    if stop == "cancel":
        store.cancel_review()
    elif stop == "disable":
        store.configure_automation(False)
    else:
        store.clear(state=True, snapshots=False)
    assert not store.review_pending()
    assert store.evaluate_status(key, review=True)["skipped"] == "changed"
    assert store.note(key) is None
    if stop != "clear":
        store.configure_automation(True)
        assert store.review_status()["state"] == "cancelled"


def test_review_does_not_consume_or_refresh_live_retry(tmp_path):
    store = historical(tmp_path, 1)
    store.start_review()
    store.scan_review()
    store.queue_statuses(["movie:tmdb:1"])
    pending = store.pending_statuses()
    assert store.evaluate_status("movie:tmdb:1", review=True)["skipped"] == "live"
    assert store.pending_statuses() == pending
    assert store.review_status()["processed"] == 1


def test_automatic_groups_keep_individual_undo(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    store.write_state("history", movies(3))
    attach(monkeypatch, store)
    worker.run_once({}, tmp_path)
    total, log = store.change_log()
    assert total == 3 and len({row.automatic_group for row in log}) == 1
    assert log[0].automatic_group
    store.undo(log[0].batch)
    assert sum(store.note(f"movie:tmdb:{n}") is not None for n in range(1, 4)) == 2
    assert store.change_log()[1][0].automatic_group == ""


def test_retention_expires_automatic_batches_before_older_user_edits(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    store.write_state("history", movies(5))
    manual = store.save_note("movie:tmdb:100", {"type": "movie", "status": "dropped"}, origin="manual")
    monkeypatch.setattr(tracker_store, "CHANGE_LIMIT", 6)
    batches = [store.evaluate_status(f"movie:tmdb:{n}")["batch"] for n in range(1, 6)]
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute("SELECT count(*) FROM changes").fetchone()[0] == 4
        retained = {row[0] for row in conn.execute("SELECT DISTINCT batch FROM changes")}
        assert retained == {manual, batches[-1]}
        assert conn.execute("SELECT count(*) FROM automatic_batches").fetchone()[0] == 1
    store.undo(manual)
    assert store.note("movie:tmdb:100") is None


def test_full_change_log_leaves_headroom_without_repeated_full_prunes(tmp_path):
    store = TrackerStore(tmp_path)
    manual = store.save_note("movie:tmdb:100", {"type": "movie", "status": "dropped"}, origin="manual")
    now = time.time_ns()
    with store.session():
        conn = store._shared
        with store._write(conn):
            conn.executemany("INSERT INTO changes(batch,scope,feature,action,old_key,created_ns) VALUES(?,'','history','add','',?)", [(f"user-{n}", now) for n in range(39998)])
        statements = []
        conn.set_trace_callback(statements.append)
        def add_automatic(start, count):
            with store._write(conn):
                conn.executemany("INSERT INTO changes(batch,scope,feature,action,old_key,created_ns) VALUES(?,'','notes','note','',?)", [(f"auto-{n}", now) for n in range(start, start + count)])
                conn.executemany("INSERT INTO automatic_batches VALUES(?,?)", [(f"auto-{n}", "group") for n in range(start, start + count)])
        def full_prunes():
            return sum("LEFT JOIN automatic_batches" in statement for statement in statements)
        add_automatic(0, 10000)
        store.prune_status_changes()
        assert full_prunes() == 0
        add_automatic(10000, 1)
        store.prune_status_changes()
        assert full_prunes() == 1
        assert conn.execute("SELECT count(*) FROM changes").fetchone()[0] == 45000
        assert conn.execute("SELECT count(*) FROM automatic_batches").fetchone()[0] == 5000
        add_automatic(10001, 4998)
        store.prune_status_changes()
        store.save_note("movie:tmdb:101", {"type": "movie", "status": "watching"}, origin="manual")
        assert conn.execute("SELECT count(*) FROM changes").fetchone()[0] == 50000
        assert full_prunes() == 1
        add_automatic(14999, 1)
        store.prune_status_changes()
        assert full_prunes() == 2
        assert conn.execute("SELECT count(*) FROM changes").fetchone()[0] == 45000
        assert conn.execute("SELECT count(*) FROM automatic_batches").fetchone()[0] == 4998
        store.undo(manual)
        assert store.note("movie:tmdb:100") is None


def test_upgrade_marks_old_automatic_changes_without_starting_review(tmp_path):
    store = TrackerStore(tmp_path)
    store.write_state("history", movies(1))
    batch = store.evaluate_status("movie:tmdb:1")["batch"]
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("DROP TABLE automatic_batches")
        conn.execute("DROP TABLE status_review")
        conn.execute("PRAGMA user_version=8")
    store = TrackerStore(tmp_path)
    assert store.change_log()[1][0].automatic_group == batch
    assert store.review_status()["state"] == "idle"
    assert not store.pending_statuses()


def test_review_api_start_read_cancel_and_disabled(api, base, monkeypatch):
    wakes = []
    monkeypatch.setattr(worker, "wake", lambda: wakes.append(1))
    monkeypatch.setattr("api.trackerAPI._sync_running", lambda: False)
    assert api.get("/api/tracker/automatic-status/review").json()["review"]["state"] == "idle"
    assert api.post("/api/tracker/automatic-status", json={"action": "review-start"}).json()["review"]["state"] == "scanning"
    assert api.post("/api/tracker/automatic-status", json={"action": "review-start"}).status_code == 409
    assert wakes == [1]
    monkeypatch.setattr("api.trackerAPI._sync_running", lambda: True)
    assert api.post("/api/tracker/automatic-status", json={"action": "review-cancel"}).json()["review"]["state"] == "cancelled"
    monkeypatch.setattr("api.trackerAPI._sync_running", lambda: False)
    monkeypatch.setattr("api.trackerAPI.load_config", lambda: {"crosswatch": {"root_dir": str(base), "automatic_status": False}})
    assert api.post("/api/tracker/automatic-status", json={"action": "review-start"}).status_code == 409


def test_bulk_catchup_runs_larger_batches_then_returns_to_idle(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    store.write_state("history", movies(600))
    attach(monkeypatch, store)
    assert TrackerStore(tmp_path).status_catchup()
    assert worker.run_once({}, tmp_path) is True
    assert len(store.pending_statuses(1000)) == 350
    assert worker.run_once({}, tmp_path) is True
    assert len(store.pending_statuses(1000)) == 100
    assert worker.run_once({}, tmp_path) is True
    assert not store.pending_statuses()
    assert worker.run_once({}, tmp_path) is False
    assert not store.status_catchup()
    store.queue_statuses([f"movie:tmdb:{n}" for n in range(1, 41)])
    assert worker.run_once({}, tmp_path) is False
    assert len(store.pending_statuses()) == 15


def test_fast_work_pauses_for_sync_and_cancellation(tmp_path, monkeypatch):
    store = historical(tmp_path, 300)
    attach(monkeypatch, store)
    store.start_review()
    monkeypatch.setattr(worker, "busy", lambda: True)
    assert not worker.run_once({}, tmp_path)
    assert store.review_status()["scanned"] == 0
    monkeypatch.setattr(worker, "busy", lambda: False)
    evaluate = TrackerStore.evaluate_status
    calls = []
    def cancel_after_first(self, *args, **kwargs):
        result = evaluate(self, *args, **kwargs)
        calls.append(result)
        if len(calls) == 1:
            self.cancel_review()
        return result
    monkeypatch.setattr(TrackerStore, "evaluate_status", cancel_after_first)
    worker.run_once({}, tmp_path)
    assert sum(bool(row.get("batch")) for row in calls) == 1
    assert store.review_status()["state"] == "cancelled"
    assert worker.run_once({}, tmp_path) is False


def test_missing_metadata_leaves_fast_mode_until_daily_retry(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    store.write_state("history", {str(n): {**EPISODE, "episode": n} for n in range(1, 201)})
    attach(monkeypatch, store)
    monkeypatch.setattr(worker, "cached_metadata", lambda key: None)
    assert worker.run_once({}, tmp_path) is True
    assert worker.run_once({}, tmp_path) is False
    assert not store.status_catchup()
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute("SELECT due FROM status_pending").fetchone()[0] > time.time() + 86000


def test_worker_yields_briefly_only_for_requested_fast_work(monkeypatch):
    waits, calls = [], []
    class Wake:
        def wait(self, delay):
            waits.append(delay)
        def clear(self):
            pass
        def set(self):
            pass
    def run(*args):
        calls.append(1)
        if len(calls) == 3:
            worker._STOP.set()
        return len(calls) == 1
    monkeypatch.setattr(worker, "_WAKE", Wake())
    monkeypatch.setattr(worker, "_STOP", Event())
    monkeypatch.setattr(worker, "_THREAD", None)
    monkeypatch.setattr(worker, "run_once", run)
    worker.start(lambda: {}, None)
    worker._THREAD.join(timeout=2)
    assert not worker._THREAD.is_alive()
    assert waits == [15, .05, 15]


def test_explicit_small_imports_request_fast_catchup(tmp_path, monkeypatch):
    import json
    from services.tracker_import import remember_source
    store = TrackerStore(tmp_path)
    store.import_json(json.dumps({"items": movies(40)}).encode(), "history.json")
    assert store.status_catchup()
    store.configure_automation(False)
    assert not store.status_catchup()
    store.configure_automation(True)
    store.queue_statuses(["movie:tmdb:1"])
    assert not store.status_catchup()
    wakes = []
    monkeypatch.setattr(worker, "wake", lambda: wakes.append(1))
    remember_source(store, "TRAKT", "default", {"features": {"history": {"added": 1}}})
    assert wakes == [1] and store.status_catchup()


@pytest.mark.parametrize("origin,status", [("sync", "watching"), ("automatic", "watching"), ("manual", "watching"), (None, "watching"), (None, "dropped"), (None, "on_hold"), (None, "completed"), (None, "planned"), ("sync", "dropped"), ("sync", "on_hold"), ("sync", "completed")])
@pytest.mark.parametrize("trigger", ["metadata", "review"])
def test_ended_eight_episode_show_completes_without_overriding_protected_statuses(tmp_path, monkeypatch, origin, status, trigger):
    key = "show:tmdb:95396"
    store = TrackerStore(tmp_path)
    store.configure_automation(False)
    store.write_state("history", {str(n): {**EPISODE, "episode": n} for n in range(1, 9)})
    store.save_note(key, {"type": "show", "status": status}, origin=origin)
    store.configure_automation(True)
    attach(monkeypatch, store)
    monkeypatch.setattr(worker, "_METADATA_PENDING", {})
    reads = []
    def metadata(title):
        reads.append(title)
        return {"status": "Ended", "number_of_episodes": 8, "number_of_seasons": 1, "seasons": [{"season_number": 1, "episode_count": 8}]}
    monkeypatch.setattr(worker, "cached_metadata", metadata)
    if trigger == "metadata":
        for _ in range(10):
            worker.metadata_changed("95396")
        assert len(worker._METADATA_PENDING) == 1
    else:
        store.start_review()
    worker.run_once({}, tmp_path)
    if trigger == "review":
        worker.run_once({}, tmp_path)
        assert store.review_status()["state"] == "complete"
    eligible = origin in {None, "sync", "automatic"} and status == "watching"
    assert store.note(key)["status"] == ("completed" if eligible else status)
    assert reads == ([key] if eligible else [])
    assert not store.pending_statuses()
    assert not store.status_catchup()


def test_metadata_work_is_bounded_pauses_for_sync_and_respects_disabled_instances(tmp_path, monkeypatch):
    store = historical(tmp_path, 0)
    attach(monkeypatch, store)
    monkeypatch.setattr(worker, "_METADATA_PENDING", {})
    for n in range(30):
        worker.metadata_changed(n + 1)
    monkeypatch.setattr(worker, "busy", lambda: True)
    worker.run_once({}, tmp_path)
    assert len(worker._METADATA_PENDING) == 30
    monkeypatch.setattr(worker, "busy", lambda: False)
    worker.run_once({}, tmp_path)
    assert len(worker._METADATA_PENDING) == 5
    assert not store.pending_statuses()
    worker.run_once({"crosswatch": {"automatic_status": False}}, tmp_path)
    assert not worker._METADATA_PENDING


def test_metadata_queue_keeps_live_history_priority_and_retries_failed_delivery(tmp_path, monkeypatch):
    store = historical(tmp_path, 0)
    attach(monkeypatch, store)
    monkeypatch.setattr(worker, "_METADATA_PENDING", {})
    key = "show:tmdb:95396"
    store.save_note(key, {"type": "show", "status": "watching"}, origin="sync")
    store.queue_statuses([key])
    version = dict(store.pending_statuses())[key]
    store.queue_metadata_statuses([key])
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute("SELECT due FROM status_pending WHERE title_key=?", (key,)).fetchone()[0] == 0
    assert dict(store.pending_statuses())[key] != version
    store.configure_automation(False)
    store.queue_metadata_statuses([key])
    assert not store.pending_statuses()
    worker.metadata_changed("95396")
    with monkeypatch.context() as patch:
        patch.setattr(TrackerStore, "queue_metadata_statuses", lambda *args: (_ for _ in ()).throw(RuntimeError("locked")))
        with pytest.raises(RuntimeError, match="locked"):
            worker._queue_metadata([(store, False, False)])
    assert key in worker._METADATA_PENDING


@pytest.mark.parametrize("origin,status", [("manual", "watching"), ("sync", "on_hold"), ("sync", "dropped")])
def test_status_changed_after_metadata_queue_stays_protected(tmp_path, origin, status):
    store = historical(tmp_path, 0)
    key = "show:tmdb:95396"
    store.save_note(key, {"type": "show", "status": "watching"}, origin="sync")
    store.queue_metadata_statuses([key])
    version = dict(store.pending_statuses())[key]
    store.save_note(key, {"type": "show", "status": status}, origin=origin)
    store.evaluate_status(key, lambda: pytest.fail("Protected statuses need no metadata"), version=version)
    assert store.note(key)["status"] == status


def test_metadata_notifications_wait_for_migrating_instances(tmp_path, monkeypatch):
    from services import tracker_migration
    first, second = historical(tmp_path / "first", 0), historical(tmp_path / "second", 0)
    attach(monkeypatch, first)
    monkeypatch.setattr(worker, "tracker_roots", lambda *args: [("default", first.root), ("second", second.root)])
    monkeypatch.setattr(worker, "_METADATA_PENDING", {})
    monkeypatch.setattr(tracker_migration, "migrating", lambda root: root == second.root)
    worker.metadata_changed("95396")
    worker.run_once({}, tmp_path)
    assert "show:tmdb:95396" in worker._METADATA_PENDING
    monkeypatch.setattr(tracker_migration, "migrating", lambda root: False)
    worker.run_once({}, tmp_path)
    assert not worker._METADATA_PENDING


@pytest.mark.parametrize("trigger", ["metadata", "review", "history"])
@pytest.mark.parametrize("unusable", [None, {"status": "Ended", "number_of_episodes": 9, "seasons": [{"season_number": 1, "episode_count": 8}]}])
def test_synced_watching_waits_for_metadata_without_daily_retries(tmp_path, monkeypatch, trigger, unusable):
    store = TrackerStore(tmp_path)
    key = "show:tmdb:95396"
    store.configure_automation(False)
    store.write_state("history", {str(n): {**EPISODE, "episode": n} for n in range(1, 9)})
    store.save_note(key, {"type": "show", "status": "watching"}, origin="sync")
    store.configure_automation(True)
    attach(monkeypatch, store)
    monkeypatch.setattr(worker, "_METADATA_PENDING", {})
    monkeypatch.setattr(worker, "cached_metadata", lambda key: unusable)
    if trigger == "review":
        store.start_review()
    elif trigger == "history":
        store.queue_statuses([key])
    else:
        worker.metadata_changed("95396")
    worker.run_once({}, tmp_path)
    assert store.note(key)["status"] == "watching"
    assert store.status_control(key)["origin"] == "sync"
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute("SELECT count(*) FROM status_pending").fetchone()[0] == 0
    monkeypatch.setattr(worker, "cached_metadata", lambda key: pytest.fail("Settled titles must not be checked again"))
    worker.run_once({}, tmp_path)
    monkeypatch.setattr(worker, "cached_metadata", lambda key: {"status": "Ended", "number_of_episodes": 8, "seasons": [{"season_number": 1, "episode_count": 8}]})
    worker.metadata_changed("95396")
    worker.run_once({}, tmp_path)
    assert store.note(key)["status"] == "completed"


def test_one_review_includes_all_legacy_watching_movies_without_enabling_titles(tmp_path, monkeypatch):
    store = historical(tmp_path, 40)
    for n in range(1, 41):
        store.save_note(f"movie:tmdb:{n}", {"type": "movie", "status": "watching"})
    attach(monkeypatch, store)
    store.start_review()
    worker.run_once({}, tmp_path)
    assert store.review_status()["state"] == "complete"
    assert store.review_status()["processed"] == 40
    assert all(store.note(f"movie:tmdb:{n}")["status"] == "completed" for n in range(1, 41))
    assert not store.pending_statuses()
