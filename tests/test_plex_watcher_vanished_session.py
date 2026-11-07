# tests/test_plex_watcher_vanished_session.py
# CrossWatch - Plex watcher closes sessions that end without a stop
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

import pytest

DURATION = 6_000_000


@pytest.fixture(autouse=True)
def no_cloud_user_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    monkeypatch.setattr(watch, "fetch_cloud_home_users", lambda *args, **kwargs: [])
    monkeypatch.setattr(watch, "fetch_cloud_account_users", lambda *args, **kwargs: [])


class CaptureSink:
    def __init__(self) -> None:
        self.events: list[Any] = []
        self.on_stop: Any = None
        self.on_seek: Any = None

    def send(self, event: Any, *args: Any, **kwargs: Any) -> None:
        self.events.append(event)
        hook = None
        if event.action == "stop":
            hook, self.on_stop = self.on_stop, None
        elif (event.raw or {}).get("_cw_seek"):
            hook, self.on_seek = self.on_seek, None
        if hook is not None:
            hook()


class FakePlex:
    machineIdentifier = "server-1"

    def __init__(self) -> None:
        self.listing = '<MediaContainer size="0" />'
        self.error: Exception | None = None
        self.on_query: Any = None

    def query(self, path: str) -> ET.Element:
        hook, self.on_query = self.on_query, None
        if hook is not None:
            hook()
        if self.error is not None:
            raise self.error
        return ET.fromstring(self.listing)

    def sessions(self) -> list[Any]:
        return []

    def fetchItem(self, rating_key: int) -> None:
        return None


def _cfg() -> dict[str, Any]:
    return {
        "plex": {"server_url": "http://plex.test", "account_token": "token", "username": "owner"},
        "scrobble": {"enabled": True, "sources": {"watcher": True}, "watch": {"filters": {}}},
    }


def _psn(session_key: str, state: str, offset: int, *, rating_key: str = "49156", itemless: bool = False) -> dict[str, Any]:
    entry = {
        "sessionKey": session_key,
        "clientIdentifier": "web",
        "ratingKey": rating_key,
        "key": f"/library/metadata/{rating_key}",
        "guid": f"imdb://tt{rating_key}",
        "type": "movie",
        "title": "Resident Evil",
        "year": 2026,
        "duration": DURATION,
        "viewOffset": offset,
        "state": state,
        "machineIdentifier": "server-1",
    }
    if itemless:
        for key in ("ratingKey", "key", "guid"):
            entry.pop(key)
    return {"type": "playing", "size": 1, "PlaySessionStateNotification": [entry]}


def _service(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, CaptureSink, list[float], FakePlex]:
    from providers.scrobble.plex import watch
    from providers.scrobble.scrobble import Dispatcher

    clock = [1000.0]
    monkeypatch.setattr(watch, "_cw_update", lambda *args, **kwargs: None)
    monkeypatch.setattr(watch, "_cw_update_payload", lambda *args, **kwargs: None)
    monkeypatch.setattr(watch.time, "sleep", lambda *args, **kwargs: None)
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    cfg = _cfg()
    sink = CaptureSink()
    service = watch.WatchService(dispatcher=Dispatcher([sink], cfg_provider=lambda: cfg), cfg_provider=lambda: cfg, quiet_startup=True)
    plex = FakePlex()
    service._plex = plex
    return service, sink, clock, plex


def _skip_credits(service: Any, clock: list[float]) -> None:
    for delay, alert in (
        (0, _psn("28", "playing", 5_520_000)),
        (46, _psn("28", "playing", DURATION)),
        (0.2, _psn("28", "paused", DURATION)),
    ):
        clock[0] += delay
        service._handle_alert(alert)


def _stops(sink: CaptureSink) -> list[Any]:
    return [event for event in sink.events if event.action == "stop"]


def test_session_gone_after_skip_credits_sends_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock, _ = _service(monkeypatch)
    _skip_credits(service, clock)
    assert [event.action for event in sink.events] == ["start", "pause"]

    clock[0] += 61
    service._close_vanished_sessions()
    clock[0] += 31
    service._close_vanished_sessions()

    stops = _stops(sink)
    assert len(stops) == 1
    assert stops[0].progress == 100
    assert stops[0].session_key == "28"
    assert stops[0].raw["_cw_vanished_stop"] is True


def test_listed_paused_session_is_kept(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock, plex = _service(monkeypatch)
    _skip_credits(service, clock)
    plex.listing = '<MediaContainer size="1"><Video sessionKey="28" ratingKey="49156" /></MediaContainer>'

    clock[0] += 61
    service._close_vanished_sessions()

    assert _stops(sink) == []


def test_failed_listing_is_not_treated_as_ended(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock, plex = _service(monkeypatch)
    _skip_credits(service, clock)
    plex.error = RuntimeError("(403) forbidden")

    clock[0] += 61
    service._close_vanished_sessions()

    assert _stops(sink) == []


def test_session_within_grace_is_kept(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock, _ = _service(monkeypatch)
    _skip_credits(service, clock)

    clock[0] += 30
    service._close_vanished_sessions()

    assert _stops(sink) == []


@pytest.mark.parametrize("listing", [
    '<MediaContainer size="2"><Video sessionKey="40" ratingKey="1" /></MediaContainer>',
    '<MediaContainer><Video sessionKey="40" ratingKey="1" /></MediaContainer>',
    '<MediaContainer size="1"><Video ratingKey="1" /></MediaContainer>',
    '<Response code="500" status="Internal Server Error" />',
])
def test_incomplete_listing_is_not_treated_as_ended(monkeypatch: pytest.MonkeyPatch, listing: str) -> None:
    service, sink, clock, plex = _service(monkeypatch)
    _skip_credits(service, clock)
    plex.listing = listing

    clock[0] += 61
    service._close_vanished_sessions()

    assert _stops(sink) == []


def test_seek_to_end_without_pause_stops_at_last_sent_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock, _ = _service(monkeypatch)
    for delay, alert in (
        (0, _psn("28", "playing", 600_000)),
        (20, _psn("28", "playing", DURATION)),
    ):
        clock[0] += delay
        service._handle_alert(alert)

    clock[0] += 61
    service._close_vanished_sessions()

    stops = _stops(sink)
    assert len(stops) == 1
    assert stops[0].progress == 10


def test_new_item_during_listing_is_not_stopped(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock, plex = _service(monkeypatch)
    _skip_credits(service, clock)
    clock[0] += 61
    plex.on_query = lambda: service._handle_alert(_psn("28", "playing", 60_000, rating_key="50000"))
    plex.listing = '<MediaContainer size="1"><Video sessionKey="28" ratingKey="50000" /></MediaContainer>'

    service._close_vanished_sessions()

    stops = _stops(sink)
    assert len(stops) == 1
    assert stops[0].raw.get("_cw_replaced_stop") is True
    assert "_cw_vanished_stop" not in stops[0].raw
    assert sink.events[-1].action == "start"
    assert sink.events[-1].ids.get("imdb") == "tt50000"


def test_itemless_stop_closes_session_for_reaper(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock, _ = _service(monkeypatch)
    _skip_credits(service, clock)
    clock[0] += 2
    service._handle_alert(_psn("28", "stopped", DURATION, itemless=True))

    clock[0] += 61
    service._close_vanished_sessions()

    assert _stops(sink) == []


def test_late_real_stop_after_ended_session_is_suppressed(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock, _ = _service(monkeypatch)
    _skip_credits(service, clock)
    clock[0] += 61
    service._close_vanished_sessions()
    assert len(_stops(sink)) == 1

    clock[0] += 5
    service._handle_alert(_psn("28", "stopped", DURATION))

    assert len(_stops(sink)) == 1


def test_late_real_stop_with_older_offset_is_suppressed(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock, _ = _service(monkeypatch)
    _skip_credits(service, clock)
    clock[0] += 61
    service._close_vanished_sessions()
    assert len(_stops(sink)) == 1

    clock[0] += 5
    service._handle_alert(_psn("28", "stopped", 5_520_000))

    assert len(_stops(sink)) == 1


def test_new_item_during_stop_dispatch_keeps_its_state(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    service, sink, clock, _ = _service(monkeypatch)
    cards: list[tuple[str, Any]] = []
    monkeypatch.setattr(watch, "_cw_update", lambda _src, ev, **kwargs: cards.append((ev.action, ev.ids.get("imdb"))))
    _skip_credits(service, clock)
    clock[0] += 61
    sink.on_stop = lambda: service._handle_alert(_psn("28", "playing", 60_000, rating_key="50000"))

    service._close_vanished_sessions()

    assert len(_stops(sink)) == 1
    assert cards[-1] == ("start", "tt50000")
    assert service._last_emit["28"][0] == "start"


def test_resume_on_new_session_closes_old_one_without_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    service, sink, clock, _ = _service(monkeypatch)
    cleared: list[Any] = []
    monkeypatch.setattr(watch, "_cw_update_payload", lambda *args, **kwargs: cleared.append(kwargs.get("session_key")))
    for delay, alert in (
        (0, _psn("33", "playing", 4_164_000)),
        (20, _psn("33", "paused", 4_182_000)),
        (140, _psn("35", "buffering", 5_742_000)),
        (20, _psn("35", "playing", 5_850_000)),
    ):
        clock[0] += delay
        service._handle_alert(alert)

    clock[0] += 61
    service._close_vanished_sessions()

    stops = _stops(sink)
    assert [stop.session_key for stop in stops] == ["35"]
    assert stops[0].progress >= 97
    assert "33" in cleared


def test_real_stop_during_stop_dispatch_clears_card(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    service, sink, clock, _ = _service(monkeypatch)
    cleared: list[Any] = []
    monkeypatch.setattr(watch, "_cw_update_payload", lambda *args, **kwargs: cleared.append(kwargs.get("session_key")))
    _skip_credits(service, clock)
    clock[0] += 61
    sink.on_stop = lambda: service._handle_alert(_psn("28", "stopped", DURATION - 60_000))

    service._close_vanished_sessions()

    assert len(_stops(sink)) == 1
    assert "28" in cleared


def test_reused_session_for_other_item_keeps_its_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock, _ = _service(monkeypatch)
    _skip_credits(service, clock)
    clock[0] += 61
    service._close_vanished_sessions()
    assert len(_stops(sink)) == 1

    for delay, alert in (
        (300, _psn("28", "playing", 5_970_000, rating_key="50000")),
        (5, _psn("28", "stopped", 5_940_000, rating_key="50000")),
    ):
        clock[0] += delay
        service._handle_alert(alert)

    stops = _stops(sink)
    assert len(stops) == 2
    assert stops[1].ids.get("imdb") == "tt50000"


def test_same_item_on_other_device_at_lower_progress_keeps_completion(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock, _ = _service(monkeypatch)
    _skip_credits(service, clock)
    for delay, alert in (
        (10, _psn("40", "playing", 30_000)),
        (30, _psn("40", "stopped", 600_000)),
    ):
        clock[0] += delay
        service._handle_alert(alert)

    clock[0] += 61
    service._close_vanished_sessions()

    reaped = [stop for stop in _stops(sink) if stop.session_key == "28"]
    assert len(reaped) == 1
    assert reaped[0].progress == 100


def test_seek_after_reap_is_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    service, sink, clock, _ = _service(monkeypatch)
    cards: list[Any] = []
    monkeypatch.setattr(watch, "_cw_update", lambda _src, ev, **kwargs: cards.append(ev.action))
    _skip_credits(service, clock)
    clock[0] += 61
    service._close_vanished_sessions()
    count = len(sink.events)

    clock[0] += 2
    service._emit_seek_update("28", 50, _cfg(), "timeline")

    assert len(sink.events) == count
    assert cards[-1] == "stop"
    assert service._last_emit["28"][0] == "stop"


def test_reap_during_slow_seek_dispatch_keeps_card_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    service, sink, clock, _ = _service(monkeypatch)
    cards: list[Any] = []
    monkeypatch.setattr(watch, "_cw_update", lambda _src, ev, **kwargs: cards.append(ev.action))
    for delay, alert in (
        (0, _psn("28", "playing", 600_000)),
        (10, _psn("28", "playing", 660_000)),
    ):
        clock[0] += delay
        service._handle_alert(alert)

    def reap() -> None:
        clock[0] += 61
        service._close_vanished_sessions()

    sink.on_seek = reap
    clock[0] += 5
    service._emit_seek_update("28", 50, _cfg(), "timeline")

    assert service._last_emit["28"][0] == "stop"
    assert cards[-1] == "stop"


def test_seek_before_reap_keeps_session_open(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock, _ = _service(monkeypatch)
    for delay, alert in (
        (0, _psn("28", "playing", 600_000)),
        (10, _psn("28", "playing", 660_000)),
    ):
        clock[0] += delay
        service._handle_alert(alert)
    clock[0] += 61
    service._emit_seek_update("28", 50, _cfg(), "timeline")

    service._close_vanished_sessions()

    assert _stops(sink) == []


def test_real_stop_is_not_repeated(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock, _ = _service(monkeypatch)
    _skip_credits(service, clock)
    clock[0] += 5
    service._handle_alert(_psn("28", "stopped", DURATION))
    assert len(_stops(sink)) == 1

    clock[0] += 61
    service._close_vanished_sessions()

    assert len(_stops(sink)) == 1
