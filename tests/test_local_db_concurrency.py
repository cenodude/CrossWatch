from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from cw_platform.local_db import db


@pytest.fixture()
def database(tmp_path, monkeypatch):
    monkeypatch.setenv("CROSSWATCH_DB", str(tmp_path / "crosswatch.sqlite3"))
    monkeypatch.setenv("CONFIG_BASE", str(tmp_path))
    db.close_conn()
    conn = db.get_conn()
    assert conn is not None
    with conn:
        conn.execute("CREATE TABLE concurrency_test (value TEXT)")
    yield conn
    db.close_conn()


def test_workers_cannot_read_or_commit_each_others_transaction(database):
    written = threading.Event()
    release = threading.Event()

    def writer():
        conn = db.get_conn()
        assert conn is not None
        try:
            with conn:
                conn.execute("INSERT INTO concurrency_test VALUES ('uncommitted')")
                written.set()
                assert release.wait(10)
                raise ValueError("roll back this worker")
        except ValueError:
            pass

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(writer)
        try:
            assert written.wait(10)
            assert database.execute("SELECT COUNT(*) FROM concurrency_test").fetchone()[0] == 0
            # A reader's commit must not commit the writer's pending insert.
            database.commit()
        finally:
            release.set()
        future.result(timeout=10)
    assert database.execute("SELECT COUNT(*) FROM concurrency_test").fetchone()[0] == 0


def test_connection_cache_and_close_cover_all_workers(database):
    barrier = threading.Barrier(3)

    def worker():
        conn = db.get_conn()
        assert conn is db.get_conn()
        barrier.wait(timeout=10)
        return conn

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(worker) for _ in range(2)]
        barrier.wait(timeout=10)
        connections = [database, *(future.result(timeout=10) for future in futures)]
        assert len({id(conn) for conn in connections}) == 3
        db.close_conn()
        for conn in connections:
            with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                conn.execute("SELECT 1")
        replacement = db.get_conn()
        assert replacement is not database
        assert replacement.execute("SELECT COUNT(*) FROM concurrency_test").fetchone()[0] == 0


def test_finished_worker_connection_is_closed_on_next_lookup(database):
    with ThreadPoolExecutor(max_workers=1) as pool:
        conn = pool.submit(db.get_conn).result(timeout=10)
    assert db.get_conn() is database
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        conn.execute("SELECT 1")


def test_switching_database_replaces_only_current_workers_connection(database, tmp_path, monkeypatch):
    monkeypatch.setenv("CROSSWATCH_DB", str(tmp_path / "replacement.sqlite3"))
    replacement = db.get_conn()
    assert replacement is not None and replacement is not database
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        database.execute("SELECT 1")
    assert replacement.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
