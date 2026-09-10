from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from cw_platform.event_archive import db, groups
from cw_platform.event_archive.recorder import record_events


def _events(start, count):
    return [
        {
            "event_hash": f"event-{i}",
            "domain": "sync",
            "created_at": 1770000000 + i,
            "run_id": f"run-{i}",
            "event_type": "sync_run_started",
            "severity": "info",
        }
        for i in range(start, start + count)
    ]


@pytest.fixture()
def archive(tmp_path, monkeypatch):
    monkeypatch.setenv("CROSSWATCH_DB", str(tmp_path / "crosswatch.sqlite3"))
    monkeypatch.setenv("CONFIG_BASE", str(tmp_path))
    db.close_conn()
    conn = db.get_conn()
    assert conn is not None
    assert record_events(_events(0, 8), conn=conn) == 8
    yield conn
    db.close_conn()


def test_failed_rebuild_preserves_groups_and_retries_version(archive, monkeypatch, caplog):
    original = groups.list_groups(visibility="all", conn=archive)
    assert original["total"] == 8
    old_version = groups.CORRELATION_VERSION - 1
    archive.execute(f"PRAGMA application_id={old_version}")
    original_ids = [tuple(row) for row in archive.execute("SELECT id, group_id FROM events ORDER BY id")]

    with monkeypatch.context() as patch:
        def fail(*args):
            raise RuntimeError("injected rebuild failure")

        patch.setattr(groups, "_recompute", fail)
        with caplog.at_level(logging.WARNING, logger="crosswatch.event_archive"):
            result = groups.list_groups(visibility="all", conn=archive)
        assert result == original
        assert archive.execute("PRAGMA application_id").fetchone()[0] == old_version
        assert [tuple(row) for row in archive.execute("SELECT id, group_id FROM events ORDER BY id")] == original_ids
        assert any(record.exc_info for record in caplog.records if "event correlation failed" in record.message)

    result = groups.list_groups(visibility="all", conn=archive)
    assert result["total"] == 8
    assert archive.execute("PRAGMA application_id").fetchone()[0] == groups.CORRELATION_VERSION


def test_concurrent_feed_rebuild_and_recording_preserve_all_events(archive, caplog):
    barrier = threading.Barrier(4)

    def reader(tree):
        barrier.wait(timeout=10)
        for _ in range(12):
            result = (groups.list_tree if tree else groups.list_groups)(visibility="all", limit=100)
            assert 8 <= result["total"] <= 20
            assert len(result["items"]) == result["total"]
            assert all(item["event_count"] == 1 for item in result["items"])

    def rebuilder():
        barrier.wait(timeout=10)
        for _ in range(12):
            assert groups.correlate(reset=True)["ok"]

    def writer():
        barrier.wait(timeout=10)
        for i in range(8, 20):
            assert record_events(_events(i, 1)) == 1

    with caplog.at_level(logging.WARNING, logger="crosswatch.event_archive"):
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(reader, False), pool.submit(reader, True), pool.submit(rebuilder), pool.submit(writer)]
            for future in futures:
                future.result(timeout=30)
    result = groups.list_groups(visibility="all", conn=archive)
    assert result["total"] == 20
    assert archive.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 20
    assert archive.execute(
        "SELECT COUNT(*) FROM events e LEFT JOIN event_groups g ON g.id=e.group_id WHERE g.id IS NULL"
    ).fetchone()[0] == 0
    assert archive.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert not [record for record in caplog.records if record.levelno >= logging.WARNING]
