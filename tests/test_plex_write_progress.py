from __future__ import annotations

from typing import Any

import pytest


@pytest.fixture
def captured(monkeypatch):
    from providers.sync.plex import _history as h

    events: list[dict[str, Any]] = []
    monkeypatch.setattr(h, "_emit", lambda evt: events.append(evt))
    return h, events


def _progress(events, phase):
    return [
        e for e in events
        if e.get("event") == "plex.write" and e.get("action") == "progress" and e.get("phase") == phase
    ]


def test_first_and_last_tick_always_emit(captured) -> None:
    h, events = captured
    p = h._WriteProgress("resolve", 1102)

    p.tick(0, force=True)
    p.tick(1102, force=True)

    got = _progress(events, "resolve")
    assert [e["done"] for e in got] == [0, 1102]
    assert all(e["total"] == 1102 for e in got)


def test_ticks_emit_on_the_step_boundary(captured) -> None:
    h, events = captured
    p = h._WriteProgress("resolve", 1000, step=100, interval_s=9999)

    for i in range(1, 351):
        p.tick(i)

    assert [e["done"] for e in _progress(events, "resolve")] == [100, 200, 300]


def test_repeated_same_value_does_not_spam(captured) -> None:
    h, events = captured
    p = h._WriteProgress("scrobble", 10, step=1, interval_s=0)

    p.tick(5)
    p.tick(5)
    p.tick(5)

    assert len(_progress(events, "scrobble")) == 1


def test_slow_progress_still_reports_on_the_interval(captured, monkeypatch) -> None:
    h, events = captured
    clock = {"t": 1000.0}
    monkeypatch.setattr(h.time, "time", lambda: clock["t"])

    p = h._WriteProgress("resolve", 500, step=100, interval_s=3.0)

    p.tick(7)
    p.tick(8)
    assert len(_progress(events, "resolve")) == 0, "should be throttled inside the interval"

    clock["t"] += 4.0
    p.tick(9)
    assert len(_progress(events, "resolve")) == 1, "should report once the interval passes"

    p.tick(10)
    assert len(_progress(events, "resolve")) == 1, "interval restarts after an emit"


def test_phase_is_carried_through(captured) -> None:
    h, events = captured

    h._WriteProgress("resolve", 5).tick(0, force=True)
    h._WriteProgress("scrobble", 5).tick(0, force=True)

    assert {e["phase"] for e in events} == {"resolve", "scrobble"}
