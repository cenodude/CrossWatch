# tests/test_scrobble_archive_prune.py
# CrossWatch - Scrobble archive pruning of superseded in-progress threads
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import pytest

from cw_platform.event_archive import recorder, scrobble_recorder
from cw_platform.event_archive.db import connect
from cw_platform.event_archive.groups import correlate


@pytest.fixture
def conn(monkeypatch, tmp_path):
    c = connect(tmp_path / "events.db")
    monkeypatch.setattr(scrobble_recorder, "get_conn", lambda: c)
    monkeypatch.setattr(recorder, "get_conn", lambda: c)
    monkeypatch.setattr(scrobble_recorder, "_SEEN", scrobble_recorder.OrderedDict())
    yield c
    c.close()


def _rec(event_type, *, dst="wetrakr", session="1", ts=1000, progress=50, ids=None):
    scrobble_recorder.record(
        event_type=event_type, source_provider="plex", destination_provider=dst,
        ids=ids or {"imdb": "tt1"}, media_type="movie", title="Unabomber", year=2026,
        progress=progress, session_key=session, source_kind="watcher", created_at=ts,
    )


def _threads(c):
    return sorted(
        (r["destination_provider"], r["status"], r["event_count"])
        for r in c.execute("SELECT destination_provider, status, event_count FROM event_groups").fetchall()
    )


def test_completion_removes_older_in_progress_threads(conn):
    _rec("scrobble_started", session="1", ts=1000, progress=27)
    _rec("scrobble_started", dst="trakt", session="1", ts=1000, progress=47)
    correlate(conn=conn)
    _rec("scrobble_started", session="2", ts=2000, progress=47)
    _rec("scrobble_completed", session="2", ts=3000, progress=95)
    correlate(conn=conn)
    assert _threads(conn) == [("TRAKT", "running", 1), ("WETRAKR", "completed", 2)]


def test_completion_keeps_other_items_and_completed_threads(conn):
    _rec("scrobble_started", session="1", ts=1000)
    _rec("scrobble_completed", session="1", ts=1500, progress=95)
    _rec("scrobble_started", session="9", ts=1600, ids={"imdb": "tt2"})
    correlate(conn=conn)
    _rec("scrobble_started", session="2", ts=2000)
    _rec("scrobble_completed", session="2", ts=3000, progress=95)
    correlate(conn=conn)
    assert _threads(conn) == [("WETRAKR", "completed", 2), ("WETRAKR", "completed", 2), ("WETRAKR", "running", 1)]


def test_backfill_prunes_existing_records_once(conn, monkeypatch):
    real = scrobble_recorder._prune_superseded
    monkeypatch.setattr(scrobble_recorder, "_prune_superseded", lambda row, c=None: 0)
    _rec("scrobble_started", session="1", ts=1000, progress=27)
    _rec("scrobble_started", session="2", ts=2000)
    _rec("scrobble_completed", session="2", ts=3000, progress=95)
    correlate(conn=conn)
    monkeypatch.setattr(scrobble_recorder, "_prune_superseded", real)
    assert _threads(conn) == [("WETRAKR", "completed", 2), ("WETRAKR", "running", 1)]
    assert scrobble_recorder.prune_superseded_backfill() == 1
    assert _threads(conn) == [("WETRAKR", "completed", 2)]
    _rec("scrobble_started", session="0", ts=500)
    correlate(conn=conn)
    monkeypatch.setattr(scrobble_recorder, "_prune_superseded", lambda row, c=None: 1)
    assert scrobble_recorder.prune_superseded_backfill() == 0
    assert _threads(conn) == [("WETRAKR", "completed", 2), ("WETRAKR", "running", 1)]
