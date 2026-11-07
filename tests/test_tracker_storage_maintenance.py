# tests/test_tracker_storage_maintenance.py
# CrossWatch - Background snapshot maintenance and notification status tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import json
import sqlite3
import threading
from contextlib import contextmanager, nullcontext

import pytest

from api import syncAPI, trackerAPI
from cw_platform import tracker_store
from cw_platform.tracker_store import TrackerStore
from services import tracker_storage_maintenance as maintenance


@pytest.fixture
def storage(tmp_path, monkeypatch):
    monkeypatch.setattr(maintenance, "_JOBS", {})
    monkeypatch.setattr(maintenance.tracker_migration, "migrating", lambda root: False)
    monkeypatch.setattr(syncAPI, "_is_sync_running", lambda: False)
    monkeypatch.setattr(maintenance, "exclusive_operation", lambda root: nullcontext())
    store = TrackerStore(tmp_path / "tracker")
    conn = store._connect()
    for n in range(3):
        conn.execute(
            "INSERT INTO restore_points(scope, feature, name, ts, created_ns, data) VALUES('', 'history', ?, 1, 1, ?)",
            (f"{n}-history.json", json.dumps({"ts": 1, "items": {"x": {"title": "x" * 1000}}})),
        )
    conn.close()
    return store, {"crosswatch": {"root_dir": str(store.root)}}, tmp_path


def test_worker_compresses_reports_progress_and_stays_quiet_after_completion(storage, monkeypatch):
    store, cfg, base = storage
    updates = []
    real = maintenance._update

    def capture(key, **values):
        real(key, **values)
        updates.append(values)

    monkeypatch.setattr(maintenance, "_update", capture)
    maintenance.run_once(cfg, base, threading.Event())
    job = maintenance.status()[0]
    assert job["state"] == "completed"
    assert job["done"] == job["total"] == 3
    assert job["bytes_saved"] > 0
    assert any(row.get("phase") == "compacting" for row in updates)
    assert store.read_restore_point("history", "0-history.json")["items"]["x"]["title"] == "x" * 1000
    maintenance.run_once(cfg, base, threading.Event())
    assert maintenance.status() == [job]


def test_sync_defers_compaction_but_not_compression(storage, monkeypatch):
    store, cfg, base = storage
    monkeypatch.setattr(syncAPI, "_is_sync_running", lambda: True)
    calls = []
    monkeypatch.setattr(TrackerStore, "_compact_snapshots", lambda *args: calls.append(True) or "completed")
    maintenance.run_once(cfg, base, threading.Event())
    job = maintenance.status()[0]
    assert (job["phase"], job["reason"], job["done"]) == ("waiting", "idle", 3)
    assert calls == []
    monkeypatch.setattr(syncAPI, "_is_sync_running", lambda: False)
    maintenance.run_once(cfg, base, threading.Event())
    assert calls == [True]
    assert maintenance.status()[0]["state"] == "completed"


def test_operation_lock_closes_race_with_sync_start(storage, monkeypatch):
    _, cfg, base = storage

    @contextmanager
    def occupied(root):
        raise RuntimeError("database_operation_busy")
        yield

    monkeypatch.setattr(maintenance, "exclusive_operation", occupied)
    monkeypatch.setattr(TrackerStore, "_compact_snapshots", lambda *args: pytest.fail("compaction while sync owns lock"))
    maintenance.run_once(cfg, base, threading.Event())
    assert maintenance.status()[0]["phase"] == "waiting"


def test_disk_space_wait_is_visible_and_retried(storage, monkeypatch):
    _, cfg, base = storage
    monkeypatch.setattr(TrackerStore, "_compact_snapshots", lambda *args: "disk_space")
    maintenance.run_once(cfg, base, threading.Event())
    assert maintenance.status()[0]["reason"] == "disk_space"
    monkeypatch.setattr(TrackerStore, "_compact_snapshots", lambda *args: "completed")
    maintenance.run_once(cfg, base, threading.Event())
    assert maintenance.status()[0]["state"] == "completed"


def test_migration_never_overwrites_a_concurrent_snapshot_update(storage, monkeypatch):
    store, _, _ = storage
    original = tracker_store._pack_snapshot
    changed = False

    def pack(text):
        nonlocal changed
        if not changed:
            changed = True
            conn = store._connect()
            conn.execute("UPDATE restore_points SET data=? WHERE name='0-history.json'",
                         (original(json.dumps({"ts": 2, "items": {"new": {}}})),))
            conn.close()
        return original(text)

    monkeypatch.setattr(tracker_store, "_pack_snapshot", pack)
    conn = store._connect()
    store._migrate_snapshots(conn)
    conn.close()
    assert store.read_restore_point("history", "0-history.json")["items"] == {"new": {}}


def test_shutdown_leaves_migration_resumable(storage):
    store, _, _ = storage
    stop = threading.Event()
    conn = store._connect()

    def progress(done, total, saved):
        if done == 1:
            stop.set()

    result = store._migrate_snapshots(conn, progress, stop.is_set)
    assert result["done"] == 1
    assert conn.execute("SELECT count(*) FROM restore_points WHERE typeof(data)='text'").fetchone()[0] == 2
    assert store._migrate_snapshots(conn)["done"] == 2
    conn.close()


def test_unchanged_refresh_does_not_reopen_payload_reuse_window(storage, monkeypatch):
    store, _, _ = storage
    now = 1_800_000_000_000_000_000
    monkeypatch.setattr(tracker_store.time, "time_ns", lambda: now)
    store.add_restore_point("ratings", {"A": {}}, reuse_window=60)
    now += 3600_000_000_000
    store.add_restore_point("ratings", {"A": {}}, reuse_window=60)
    now += 10_000_000_000
    store.add_restore_point("ratings", {"B": {}}, reuse_window=60)
    points = store.restore_points("ratings")
    assert len(points) == 2
    assert [store.read_restore_point("ratings", p.name)["items"] for p in points] == [{"A": {}}, {"B": {}}]


def test_conversion_pauses_before_writing_on_low_space(storage, monkeypatch):
    from types import SimpleNamespace

    store, cfg, base = storage
    usage = tracker_store.shutil.disk_usage
    monkeypatch.setattr(tracker_store.shutil, "disk_usage", lambda root: SimpleNamespace(free=0))
    maintenance.run_once(cfg, base, threading.Event())
    assert maintenance.status()[0]["reason"] == "disk_space"
    conn = store._connect()
    assert conn.execute("SELECT count(*) FROM restore_points WHERE typeof(data)='text'").fetchone()[0] == 3
    conn.close()
    monkeypatch.setattr(tracker_store.shutil, "disk_usage", usage)
    maintenance.run_once(cfg, base, threading.Event())
    assert maintenance.status()[0]["state"] == "completed"


def test_reader_pinned_wal_pauses_and_resumes_with_cumulative_counts(storage, monkeypatch):
    store, cfg, base = storage
    reader = store._connect()
    reader.execute("BEGIN")
    reader.execute("SELECT * FROM restore_points").fetchall()
    monkeypatch.setattr(tracker_store, "_SNAPSHOT_WAL_LIMIT", 1)
    try:
        maintenance.run_once(cfg, base, threading.Event())
        job = maintenance.status()[0]
        assert (job["reason"], job["done"], job["total"]) == ("wal_pressure", 1, 3)
    finally:
        reader.close()
    maintenance.run_once(cfg, base, threading.Event())
    job = maintenance.status()[0]
    assert (job["state"], job["done"], job["total"]) == ("completed", 3, 3)


@pytest.mark.parametrize(("code", "reason"), [(sqlite3.SQLITE_BUSY, "busy"), (sqlite3.SQLITE_FULL, "disk_space"), (sqlite3.SQLITE_IOERR, None)])
def test_compaction_classifies_sqlite_errors(storage, code, reason):
    store, _, _ = storage

    class BrokenConnection:
        in_transaction = False

        def execute(self, *args):
            error = sqlite3.OperationalError("injected")
            error.sqlite_errorcode = code
            raise error

    if reason:
        assert store._compact_snapshots(BrokenConnection()) == reason
    else:
        with pytest.raises(sqlite3.OperationalError):
            store._compact_snapshots(BrokenConnection())


def test_conversion_reports_signed_net_payload_savings(storage):
    store, _, _ = storage
    conn = store._connect()
    conn.execute("UPDATE restore_points SET data='{}' WHERE id=1")
    before = conn.execute("SELECT sum(length(cast(data AS blob))) FROM restore_points").fetchone()[0]
    result = store._migrate_snapshots(conn)
    after = conn.execute("SELECT sum(length(data)) FROM restore_points").fetchone()[0]
    assert result["bytes_saved"] == before - after
    conn.close()


def test_retry_counts_and_timestamps_survive_partial_failure(storage, monkeypatch):
    _, cfg, base = storage
    pack = tracker_store._pack_snapshot
    calls = 0

    def fail_second(text):
        nonlocal calls
        calls += 1
        if calls == 2:
            error = sqlite3.OperationalError("disk I/O error")
            error.sqlite_errorcode = sqlite3.SQLITE_IOERR
            raise error
        return pack(text)

    monkeypatch.setattr(tracker_store, "_pack_snapshot", fail_second)
    maintenance.run_once(cfg, base, threading.Event())
    first = maintenance.status()[0]
    assert (first["state"], first["done"], first["total"]) == ("failed", 1, 3)
    assert first["finished_at"]
    monkeypatch.setattr(tracker_store, "_pack_snapshot", pack)
    updates = []
    update = maintenance._update

    def capture(key, **values):
        update(key, **values)
        updates.append(maintenance.status()[0])

    monkeypatch.setattr(maintenance, "_update", capture)
    maintenance.run_once(cfg, base, threading.Event())
    last = maintenance.status()[0]
    assert (last["id"], last["done"], last["total"], last["state"]) == (first["id"], 3, 3, "completed")
    assert all(row.get("finished_at") is None for row in updates if row["state"] == "running")
    assert last["bytes_saved"] == first["bytes_saved"] * 3


def test_sqlite_full_auto_rollback_preserves_error_and_allows_retry(storage):
    store, _, _ = storage
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.execute("PRAGMA max_page_count=2")
    conn.execute("CREATE TABLE data(value BLOB)")
    with pytest.raises(sqlite3.OperationalError) as raised:
        with store._write(conn):
            conn.execute("INSERT INTO data VALUES(zeroblob(100000))")
    assert raised.value.sqlite_errorcode == sqlite3.SQLITE_FULL
    assert not conn.in_transaction
    assert conn.execute("SELECT count(*) FROM data").fetchone()[0] == 0
    conn.execute("PRAGMA max_page_count=100")
    with store._write(conn):
        conn.execute("INSERT INTO data VALUES(zeroblob(100000))")
    assert conn.execute("SELECT count(*) FROM data").fetchone()[0] == 1
    conn.close()


def test_failure_is_reported_and_retried(storage, monkeypatch):
    _, cfg, base = storage
    real = TrackerStore._migrate_snapshots
    monkeypatch.setattr(TrackerStore, "_migrate_snapshots", lambda *args: (_ for _ in ()).throw(ValueError("failed")))
    maintenance.run_once(cfg, base, threading.Event())
    job_id = maintenance.status()[0]["id"]
    assert maintenance.status()[0]["state"] == "failed"
    monkeypatch.setattr(TrackerStore, "_migrate_snapshots", real)
    maintenance.run_once(cfg, base, threading.Event())
    assert maintenance.status()[0]["state"] == "completed"
    assert maintenance.status()[0]["id"] == job_id


def test_worker_starts_once_and_stops_cooperatively(tmp_path, monkeypatch):
    entered = threading.Event()
    stop = threading.Event()
    calls = []
    monkeypatch.setattr(maintenance, "_THREAD", None)
    monkeypatch.setattr(maintenance, "_STOP", stop)

    def run(cfg, base, cancelled):
        calls.append(base)
        entered.set()
        cancelled.wait(2)

    monkeypatch.setattr(maintenance, "run_once", run)
    maintenance.start(lambda: {}, tmp_path)
    try:
        assert entered.wait(2)
        maintenance.start(lambda: {}, tmp_path)
        assert calls == [tmp_path]
    finally:
        maintenance.stop()
    assert stop.is_set()
    assert not maintenance._THREAD.is_alive()


@pytest.mark.parametrize("managed", [False, True])
def test_notification_endpoint_exposes_storage_jobs_only_to_admin(storage, monkeypatch, managed):
    _, cfg, _ = storage
    monkeypatch.setattr(trackerAPI, "_cfg", lambda: cfg)
    monkeypatch.setattr(trackerAPI, "_user", lambda request: {} if managed else None)
    monkeypatch.setattr(trackerAPI, "_instances", lambda cfg, user: [{"id": "default", "label": "Default"}])
    monkeypatch.setattr(trackerAPI.tracker_migration, "busy", lambda: False)
    monkeypatch.setattr(trackerAPI.tracker_check, "status", lambda store: {})
    monkeypatch.setattr(trackerAPI, "_latest_import", lambda *args: None)
    monkeypatch.setattr(maintenance, "_JOBS", {"x": {"id": "s1", "instance": "default", "state": "running"}})
    result = trackerAPI.tracker_check_current(None)
    assert len(result["storage"]) == (0 if managed else 1)
