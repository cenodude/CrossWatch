# tests/test_local_db_storage.py
# CrossWatch - Local database filesystem, lock holder and rebuild tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from cw_platform.local_db import db
from cw_platform.local_db.storage import storage_info
from services import health_details

MOUNTS = "\n".join(
    [
        "overlay / overlay rw,relatime 0 0",
        "//nas/docker /config cifs rw,vers=3.0 0 0",
        "/dev/sda1 /config/local\\040disk ext4 rw 0 0",
    ]
)


@pytest.fixture()
def mounts(tmp_path):
    path = tmp_path / "mounts"
    path.write_text(MOUNTS, encoding="utf-8")
    return path


def test_database_on_a_cifs_share_is_network_storage(mounts):
    info = storage_info("/config/.cw_databases/crosswatch.sqlite3", mounts)
    assert info == {"filesystem": "cifs", "mount_point": "/config", "network": True}


def test_longest_matching_mount_wins_and_escaped_spaces_are_read(mounts):
    info = storage_info("/config/local disk/crosswatch.sqlite3", mounts)
    assert info == {"filesystem": "ext4", "mount_point": "/config/local disk", "network": False}
    assert storage_info("/data/crosswatch.sqlite3", mounts)["filesystem"] == "overlay"


def test_missing_mounts_file_reports_unknown(tmp_path):
    assert storage_info("/config/x.sqlite3", tmp_path / "absent") == {"filesystem": None, "mount_point": None, "network": False}


@pytest.fixture()
def database(tmp_path, monkeypatch):
    monkeypatch.setenv("CROSSWATCH_DB", str(tmp_path / "crosswatch.sqlite3"))
    monkeypatch.setenv("CONFIG_BASE", str(tmp_path))
    monkeypatch.setattr(db, "_LAST_WARNING", {})
    monkeypatch.setattr(db, "_SUPPRESSED", {})
    db.close_conn()
    conn = db.get_conn()
    assert conn is not None
    with conn:
        conn.execute("CREATE TABLE lock_test (value TEXT)")
    yield conn
    db.close_conn()


def hold_write_lock(started: threading.Event, release: threading.Event) -> None:
    conn = db.get_conn()
    assert conn is not None
    with conn:
        conn.execute("INSERT INTO lock_test VALUES ('held')")
        started.set()
        assert release.wait(10)


def test_lock_failure_names_the_operation_holding_the_lock(database, caplog):
    started, release = threading.Event(), threading.Event()
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="holder") as pool:
        future = pool.submit(hold_write_lock, started, release)
        try:
            assert started.wait(10)
            writers = db.active_writers()
            assert [w["operation"] for w in writers] == ["test_local_db_storage.hold_write_lock"]
            assert writers[0]["thread"].startswith("holder")
            database.execute("PRAGMA busy_timeout=50")
            with caplog.at_level("WARNING", logger="crosswatch.local_db"), pytest.raises(sqlite3.OperationalError):
                with database:
                    database.execute("INSERT INTO lock_test VALUES ('blocked')")
        finally:
            release.set()
        future.result(timeout=10)
    message = next(r.getMessage() for r in caplog.records if "database is locked in" in r.getMessage())
    assert "in test_local_db_storage.test_lock_failure_names_the_operation_holding_the_lock" in message
    assert "held by test_local_db_storage.hold_write_lock on holder" in message
    assert db.active_writers() == []


def test_lock_failure_without_a_local_holder_points_outside_the_process(database, tmp_path, caplog):
    other = sqlite3.connect(str(tmp_path / "crosswatch.sqlite3"), isolation_level=None)
    other.execute("BEGIN IMMEDIATE")
    try:
        database.execute("PRAGMA busy_timeout=50")
        with caplog.at_level("WARNING", logger="crosswatch.local_db"), pytest.raises(sqlite3.OperationalError):
            with database:
                database.execute("INSERT INTO lock_test VALUES ('blocked')")
    finally:
        other.rollback()
        other.close()
    assert any("another process or the storage holds the lock" in r.getMessage() for r in caplog.records)


def test_health_details_flags_network_storage_and_slow_writers(tmp_path, monkeypatch):
    monkeypatch.setenv("CROSSWATCH_DB", str(tmp_path / "crosswatch.sqlite3"))
    issues: list[dict[str, str]] = []
    out = health_details.database_health(
        issues,
        storage=lambda _path: {"filesystem": "nfs4", "mount_point": "/config", "network": True},
        writers=lambda: [{"operation": "state.save_pair_blocks", "thread": "sync", "seconds": 42.0}],
    )
    assert out["network_filesystem"] is True and out["filesystem"] == "nfs4"
    assert [i["message"] for i in issues] == [
        "Database is on a network filesystem (nfs4); SQLite locking is unreliable there",
        "Database write held by state.save_pair_blocks for 42s",
    ]
    assert all(i["level"] == "degraded" and i["component"] == "database" for i in issues)
    quiet: list[dict[str, str]] = []
    health_details.database_health(
        quiet,
        storage=lambda _path: {"filesystem": "ext4", "mount_point": "/", "network": False},
        writers=lambda: [{"operation": "state.save_state", "thread": "sync", "seconds": 3.0}],
    )
    assert quiet == []


def test_replace_database_is_refused_while_a_write_is_open(database, tmp_path):
    started, release = threading.Event(), threading.Event()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(hold_write_lock, started, release)
        try:
            assert started.wait(10)
            assert db.replace_database() == {"ok": False, "error": "database_busy", "operation": "test_local_db_storage.hold_write_lock"}
        finally:
            release.set()
        future.result(timeout=10)
    assert not (tmp_path / "backups").exists()


def test_replace_database_moves_all_files_and_the_next_connection_is_fresh(database, tmp_path):
    with database:
        database.execute("INSERT INTO lock_test VALUES ('old')")
    result = db.replace_database(stamp="test")
    backup = tmp_path / "backups" / "crosswatch-test.sqlite3"
    assert result["backup"] == str(backup)
    assert str(backup) in result["moved"]
    assert not (tmp_path / "crosswatch.sqlite3").exists()
    fresh = db.get_conn()
    assert fresh is not None and fresh is not database
    assert fresh.execute("SELECT name FROM sqlite_master WHERE name='lock_test'").fetchone() is None
    old = sqlite3.connect(str(backup))
    try:
        assert old.execute("SELECT value FROM lock_test").fetchall() == [("old",)]
    finally:
        old.close()


def test_cached_connection_is_replaced_when_the_file_changes(database, monkeypatch):
    real = db._file_id
    monkeypatch.setattr(db, "_file_id", lambda path: (-1, -1))
    replacement = db.get_conn()
    assert replacement is not None and replacement is not database
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        database.execute("SELECT 1")
    monkeypatch.setattr(db, "_file_id", real)


def test_database_status_reports_busy_writes_after_ten_seconds():
    idle = health_details.database_status(
        storage=lambda _path: {"filesystem": "ext4", "mount_point": "/", "network": False},
        writers=lambda: [{"operation": "state.save_state", "thread": "sync", "seconds": 4.0, "since": 100}],
    )
    assert idle == {"busy": False, "operation": None, "seconds": 0, "since": None, "filesystem": "ext4", "network_filesystem": False}
    busy = health_details.database_status(
        storage=lambda _path: {"filesystem": "cifs", "mount_point": "/config", "network": True},
        writers=lambda: [{"operation": "state.save_pair_blocks", "thread": "sync", "seconds": 12.6, "since": 100}],
    )
    assert busy == {"busy": True, "operation": "state.save_pair_blocks", "seconds": 12, "since": 100, "filesystem": "cifs", "network_filesystem": True}
