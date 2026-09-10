# tests/test_watcher_log_stream.py
# CrossWatch - Watcher Log Ordering and Timestamp Regression Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest


@pytest.fixture
def logs(monkeypatch):
    import crosswatch

    monkeypatch.setattr(crosswatch, "WATCH_LOG_BUFFER", deque(maxlen=5))
    monkeypatch.setattr(crosswatch, "WATCH_LOG_NEXT_SEQ", 1)
    monkeypatch.setattr(crosswatch, "LOG_BUFFERS", {})
    monkeypatch.setattr(crosswatch, "LOG_BASE_SEQ", {})
    monkeypatch.setattr(crosswatch, "LOG_NEXT_SEQ", {})
    monkeypatch.setattr("services.log_archive.capture", lambda *a, **kw: None)
    stamp = datetime(2026, 9, 9, 21, 49, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(crosswatch, "datetime", SimpleNamespace(now=lambda *a: stamp))
    return crosswatch


def test_watcher_backlog_keeps_capture_time_and_cross_provider_order(logs):
    logs._append_log_to_buffer("PLEX-WATCH", "INFO first")
    logs._append_log_to_buffer("SCROBBLE", "DEBUG second")
    logs._append_log_to_buffer("PLEX-WATCH", "INFO third")
    cursor, rows = logs._watch_log_snapshot(["SCROBBLE", "PLEX-WATCH"], 0, 200)
    assert cursor == 3
    assert [row[1] for row in rows] == ["PLEX-WATCH", "SCROBBLE", "PLEX-WATCH"]
    assert all(row[2].startswith("[2026-09-09T21:49:02.000+00:00]") for row in rows)
    assert logs._watch_log_snapshot(["SCROBBLE", "PLEX-WATCH"], 0, 200) == (cursor, rows)


def test_watcher_tail_filters_and_buffer_eviction_keep_order(logs):
    for index in range(8):
        logs._append_log_to_buffer("PLEX-WATCH" if index % 2 else "SCROBBLE", f"INFO row {index}")
    cursor, rows = logs._watch_log_snapshot(["PLEX-WATCH", "SCROBBLE"], 0, 1)
    assert cursor == 8 and [row[0] for row in rows] == [7, 8]
    assert len(logs.WATCH_LOG_BUFFER) == 5
    assert [row[0] for row in logs._watch_log_snapshot(["SCROBBLE"], 0, None)[1]] == [5, 7]
    assert logs._watch_log_snapshot(["PLEX-WATCH"], cursor, None) == (8, [])


@pytest.mark.parametrize("skip_backlog", [False, True])
def test_watcher_stream_keeps_events_arriving_during_replay(logs, skip_backlog):
    async def disconnected():
        return False

    async def collect():
        logs._append_log_to_buffer("PLEX-WATCH", "INFO old source")
        logs._append_log_to_buffer("SCROBBLE", "DEBUG old sink")
        request = SimpleNamespace(state=SimpleNamespace(), is_disconnected=disconnected)
        response = await logs.api_logs_watcher(request, tail=200, tags="SCROBBLE,PLEX-WATCH",
                                               max_backlog=80, skip_backlog=skip_backlog, plain=True)
        iterator = response.body_iterator
        if skip_backlog:
            async def arrival():
                logs._append_log_to_buffer("PLEX-WATCH", "INFO live source")
                return False
            request.is_disconnected = arrival
        else:
            first = await anext(iterator)
            assert "old source" in first and "2026-09-09T21:49:02.000+00:00" in first
            logs._append_log_to_buffer("PLEX-WATCH", "INFO live source")
            assert "old sink" in await anext(iterator)
        assert "live source" in await anext(iterator)
        await iterator.aclose()

    asyncio.run(collect())


def test_watcher_feed_keeps_multiline_text_in_one_sse_record(logs):
    logs._append_log_to_buffer("PLEX-WATCH", "INFO <test>\nevent: forged")
    _, rows = logs._watch_log_snapshot(["PLEX-WATCH"], 0, None)
    line = logs._log_stream_text(rows[0][2], True)
    assert "\n" not in line and "<test>" in line
