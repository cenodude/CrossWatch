# tests/test_event_retention.py
# CrossWatch - Event archive and sync report retention
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import pytest

from cw_platform.event_archive import retention
from cw_platform.event_archive.db import connect
from cw_platform.event_archive.groups import acknowledge_group, correlate
from cw_platform.event_archive.recorder import make_event, record_events

NOW = 2_000_000_000
DAY = 86400


@pytest.fixture
def conn(tmp_path):
    c = connect(tmp_path / "events.db")
    yield c
    c.close()


def _ev(c, domain, event_type, age_days, key, **kw):
    record_events([make_event(
        domain=domain, event_type=event_type, created_at=NOW - age_days * DAY,
        item_key=key, title=key, feature=kw.pop("feature", "watchlist"), hash_extra=key, **kw,
    )], conn=c)


def _groups(c):
    return sorted(
        (r["domain"], r["item_key"], r["status"])
        for r in c.execute("SELECT domain, item_key, status FROM event_groups").fetchall()
    )


def test_threads_expire_per_domain_and_keep_open_failures(conn):
    _ev(conn, "sync", "write_succeeded", 400, "imdb:old_ok", destination_provider="TRAKT")
    _ev(conn, "sync", "write_failed", 400, "imdb:old_fail", destination_provider="TRAKT")
    _ev(conn, "sync", "write_succeeded", 10, "imdb:new_ok", destination_provider="TRAKT")
    _ev(conn, "scrobble", "scrobble_completed", 400, "imdb:old_watch", destination_provider="SIMKL", session_key="1")
    _ev(conn, "audit", "audit_login_failed", 100, "audit_old")
    _ev(conn, "audit", "audit_login", 30, "audit_new")
    correlate(conn=conn)
    out = retention.apply_retention(conn=conn, now=NOW, vacuum=False)
    assert out["threads"] == {"sync": 1, "scrobble": 1, "audit": 1}
    assert _groups(conn) == [
        ("audit", "audit_new", "completed"),
        ("sync", "imdb:new_ok", "resolved"),
        ("sync", "imdb:old_fail", "failed"),
    ]
    assert conn.execute("SELECT COUNT(*) FROM events WHERE group_id IS NULL OR group_id NOT IN (SELECT id FROM event_groups)").fetchone()[0] == 0


def test_acknowledged_failures_expire(conn, monkeypatch):
    _ev(conn, "sync", "write_failed", 400, "imdb:old_fail", destination_provider="TRAKT")
    correlate(conn=conn)
    gid = conn.execute("SELECT id FROM event_groups").fetchone()[0]
    acknowledge_group(gid, conn=conn)
    retention.apply_retention(conn=conn, now=NOW, vacuum=False)
    assert _groups(conn) == []


def test_sync_reports_keep_newest(conn, monkeypatch):
    monkeypatch.setattr(retention, "REPORT_KEEP", 2)
    for i in range(4):
        conn.execute("INSERT INTO sync_run_reports(run_id, created_at, updated_at) VALUES(?,?,?)", (f"r{i}", i, i))
        conn.execute("INSERT INTO sync_run_provider_counts(run_id, phase, provider, value, updated_at) VALUES(?,?,?,?,?)",
                     (f"r{i}", "pre", "PLEX", 1, i))
    conn.commit()
    out = retention.apply_retention(conn=conn, now=NOW, vacuum=False)
    assert out["reports"] == 2
    assert [r[0] for r in conn.execute("SELECT run_id FROM sync_run_reports ORDER BY run_id")] == ["r2", "r3"]
    assert [r[0] for r in conn.execute("SELECT run_id FROM sync_run_provider_counts ORDER BY run_id")] == ["r2", "r3"]


def test_old_sync_runs_without_events_expire(conn):
    conn.execute("INSERT INTO sync_runs(run_id, started_at, finished_at) VALUES('old', ?, ?)", (NOW - 400 * DAY, NOW - 400 * DAY))
    conn.execute("INSERT INTO sync_runs(run_id, started_at, finished_at) VALUES('new', ?, ?)", (NOW - DAY, NOW - DAY))
    conn.execute("INSERT INTO run_pairs(run_id, pair_id) VALUES('old', 'p')")
    conn.commit()
    out = retention.apply_retention(conn=conn, now=NOW, vacuum=False)
    assert out["runs"] == 1
    assert [r[0] for r in conn.execute("SELECT run_id FROM sync_runs")] == ["new"]
    assert conn.execute("SELECT COUNT(*) FROM run_pairs").fetchone()[0] == 0
