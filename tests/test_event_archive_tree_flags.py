# tests/test_event_archive_tree_flags.py
# CrossWatch - Event tree ownership, health flags and thread caps
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import threading

import pytest

from cw_platform.event_archive import groups, retention
from cw_platform.event_archive.db import connect
from cw_platform.event_archive.recorder import make_event, record_events

NOW = 2_000_000_000
DAY = 86400


@pytest.fixture
def conn(tmp_path):
    c = connect(tmp_path / "events.db")
    yield c
    c.close()


def _add(c, event_type, created_at, *, run_id=None, item_key=None, **kw):
    record_events([make_event(
        event_type=event_type, created_at=created_at, run_id=run_id, item_key=item_key,
        title=item_key, feature=kw.pop("feature", "history"), destination_provider=kw.pop("destination_provider", "TRAKT"),
        operation=kw.pop("operation", "run" if event_type.startswith("sync_run_") else "add"),
        hash_extra=f"{event_type}-{created_at}-{item_key}", **kw,
    )], conn=c)


def _run(c, run_id, start, end, failures=()):
    _add(c, "sync_run_started", start, run_id=run_id)
    for key, at in failures:
        _add(c, "write_failed", at, run_id=run_id, item_key=key)
        _add(c, "unresolved_recorded", at, run_id=run_id, item_key=key)
    _add(c, "sync_run_finished", end, run_id=run_id)


def _settle(c):
    groups.correlate(conn=c)
    c.execute(f"PRAGMA application_id={groups.CORRELATION_VERSION}")


def _top(c, visibility="all"):
    out = groups.list_tree(conn=c, visibility=visibility)
    return out, {g.get("item_key") or g.get("operation") for g in out["items"]}


def _flags(c, key):
    row = c.execute("SELECT run_owned, health_only FROM event_groups WHERE item_key=?", (key,)).fetchone()
    return tuple(row)


def test_tree_lifts_run_problems_and_keeps_standalone_items(conn):
    _run(conn, "r1", NOW - 100, NOW - 10, failures=[("imdb:owned", NOW - 50)])
    _add(conn, "write_failed", NOW - 5, item_key="imdb:standalone")
    groups.correlate(conn=conn)

    out, top = _top(conn)
    assert top == {"run", "imdb:standalone"}
    run = next(g for g in out["items"] if g.get("operation") == "run")
    assert [k["item_key"] for k in run["children"]] == ["imdb:owned"]
    assert _flags(conn, "imdb:owned") == (1, 0)
    assert _flags(conn, "imdb:standalone") == (0, 0)


def test_window_growth_marks_items_seen_mid_run(conn):
    _add(conn, "sync_run_started", NOW - 100, run_id="r1")
    _add(conn, "write_failed", NOW - 50, run_id="r1", item_key="imdb:late")
    groups.correlate(conn=conn)
    assert _flags(conn, "imdb:late") == (0, 0)

    _add(conn, "sync_run_finished", NOW - 10, run_id="r1")
    groups.correlate(conn=conn)
    assert _flags(conn, "imdb:late") == (1, 0)
    assert _top(conn)[1] == {"run"}


def test_health_only_groups_are_hidden_by_default(conn):
    _add(conn, "provider_health", NOW - 20, operation=None, feature=None, source_provider="PLEX")
    _add(conn, "write_failed", NOW - 10, item_key="imdb:x")
    groups.correlate(conn=conn)
    health = conn.execute("SELECT health_only FROM event_groups WHERE COALESCE(item_key,'')=''").fetchone()[0]
    assert health == 1

    default = groups.list_groups(conn=conn)
    assert [g["item_key"] for g in default["items"]] == ["imdb:x"]
    assert groups.list_groups(conn=conn, visibility="all")["total"] == 2


def test_existing_archives_backfill_flags_once(conn):
    _run(conn, "r1", NOW - 100, NOW - 10, failures=[("imdb:owned", NOW - 50)])
    _add(conn, "provider_health", NOW - 5, operation=None, feature=None, source_provider="PLEX")
    _settle(conn)
    with conn:
        conn.execute("UPDATE event_groups SET run_owned=0, health_only=0")
        conn.execute("DELETE FROM local_meta WHERE key=?", (groups._GROUP_FLAGS_META,))

    assert _top(conn, visibility="open")[1] == {"run"}
    assert _flags(conn, "imdb:owned") == (1, 0)
    assert conn.execute("SELECT COUNT(*) FROM event_groups WHERE health_only=1").fetchone()[0] == 1
    assert groups.refresh_group_flags(conn=conn) is False


def test_reads_do_not_wait_for_the_group_lock(tmp_path, monkeypatch):
    monkeypatch.setenv("CROSSWATCH_DB", str(tmp_path / "crosswatch.sqlite3"))
    monkeypatch.setenv("CONFIG_BASE", str(tmp_path))
    from cw_platform.event_archive import db

    db.close_conn()
    c = db.get_conn()
    _run(c, "r1", NOW - 100, NOW - 10, failures=[("imdb:owned", NOW - 50)])
    _settle(c)
    groups.refresh_group_flags(conn=c)

    held, release = threading.Event(), threading.Event()

    def holder():
        with groups._GROUP_LOCK:
            held.set()
            release.wait(10)

    t = threading.Thread(target=holder)
    t.start()
    held.wait(5)
    result: dict = {}
    reader = threading.Thread(target=lambda: result.update(tree=groups.list_tree(visibility="all")))
    reader.start()
    reader.join(5)
    finished = not reader.is_alive()
    release.set()
    t.join(5)
    reader.join(5)
    db.close_conn()
    assert finished
    assert result["tree"]["total"] == 1


def test_retention_caps_long_item_threads_and_keeps_newest(conn, monkeypatch):
    monkeypatch.setattr(retention, "THREAD_EVENT_CAP", 10)
    for i in range(12):
        start = NOW - 1000 + i * 50
        _run(conn, f"r{i}", start, start + 20, failures=[("imdb:loop", start + 10)])
    _add(conn, "write_failed", NOW - 5, item_key="imdb:short")
    groups.correlate(conn=conn)
    before = conn.execute("SELECT status FROM event_groups WHERE item_key='imdb:loop'").fetchone()[0]

    out = retention.apply_retention(conn=conn, now=NOW)

    assert out["trimmed_threads"] == 1
    row = conn.execute("SELECT id, event_count, status, first_event_at FROM event_groups WHERE item_key='imdb:loop'").fetchone()
    assert row["event_count"] == 10
    assert row["status"] == before
    assert row["first_event_at"] == NOW - 1000 + 7 * 50 + 10
    assert conn.execute("SELECT COUNT(*) FROM events WHERE item_key='imdb:short'").fetchone()[0] == 1
    assert _flags(conn, "imdb:loop") == (1, 0)


def _flapper(c):
    _add(c, "sync_run_started", NOW - 5000, run_id="r1")
    _add(c, "unresolved_recorded", NOW - 4990, run_id="r1", item_key="imdb:flap")
    _add(c, "sync_run_finished", NOW - 4980, run_id="r1", detail={"errors": 0, "unresolved": 0})
    for i in range(30):
        _add(c, "write_succeeded", NOW - 3000 + i, item_key="imdb:flap")
    _settle(c)


def _run_row(c):
    return tuple(c.execute("SELECT status, summary FROM event_groups WHERE operation='run'").fetchone())


def test_trimming_the_last_owned_problem_releases_the_item(conn, monkeypatch):
    monkeypatch.setattr(retention, "THREAD_EVENT_CAP", 10)
    _flapper(conn)
    assert _flags(conn, "imdb:flap") == (1, 0)

    retention.apply_retention(conn=conn, now=NOW)

    assert _flags(conn, "imdb:flap") == (0, 0)
    assert "imdb:flap" in _top(conn)[1]


def test_retention_heals_stale_ownership_flags(conn):
    _add(conn, "write_failed", NOW - 5, item_key="imdb:loose")
    _settle(conn)
    with conn:
        conn.execute("UPDATE event_groups SET run_owned=1 WHERE item_key='imdb:loose'")

    retention.apply_retention(conn=conn, now=NOW)

    assert _flags(conn, "imdb:loose") == (0, 0)


@pytest.mark.parametrize("existing_archive", [False, True])
def test_trimming_keeps_historical_run_outcomes(conn, monkeypatch, existing_archive):
    monkeypatch.setattr(retention, "THREAD_EVENT_CAP", 10)
    _flapper(conn)
    if existing_archive:
        with conn:
            conn.execute("DELETE FROM run_issue_counts")
            conn.execute("DELETE FROM local_meta WHERE key=?", (groups._GROUP_FLAGS_META,))
    before = _run_row(conn)
    assert before[0] == "warning"

    retention.apply_retention(conn=conn, now=NOW)
    assert groups.correlate(conn=conn, reset=True)["ok"]

    assert _run_row(conn) == before


def test_retention_releases_items_whose_runs_expired(conn):
    _run(conn, "old", NOW - 400 * DAY, NOW - 400 * DAY + 20, failures=[("imdb:kept", NOW - 400 * DAY + 10)])
    groups.correlate(conn=conn)
    with conn:
        conn.execute("UPDATE event_groups SET acknowledged_at=1 WHERE operation='run'")
    assert _flags(conn, "imdb:kept") == (1, 0)

    retention.apply_retention(conn=conn, now=NOW)

    assert conn.execute("SELECT COUNT(*) FROM event_groups WHERE operation='run'").fetchone()[0] == 0
    assert _flags(conn, "imdb:kept") == (0, 0)
    assert _top(conn)[1] == {"imdb:kept"}
