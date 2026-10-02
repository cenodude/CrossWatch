from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

import pytest

DURATION = 1_000_000


@pytest.fixture(autouse=True)
def no_cloud_user_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    monkeypatch.setattr(watch, "fetch_cloud_home_users", lambda *args, **kwargs: [])
    monkeypatch.setattr(watch, "fetch_cloud_account_users", lambda *args, **kwargs: [])


class CaptureSink:
    def __init__(self) -> None:
        self.events: list[Any] = []

    def send(self, event: Any, *args: Any, **kwargs: Any) -> None:
        self.events.append(event)


class FakePlex:
    machineIdentifier = "server-1"

    def query(self, path: str) -> ET.Element:
        return ET.fromstring("<MediaContainer />")

    def sessions(self) -> list[Any]:
        return []

    def fetchItem(self, rating_key: int) -> None:
        return None


def _cfg() -> dict[str, Any]:
    return {
        "plex": {"server_url": "http://plex.test", "account_token": "token", "username": "owner"},
        "scrobble": {"enabled": True, "sources": {"watcher": True}, "watch": {"filters": {}}},
    }


def _psn(session_key: str, episode: int, state: str, offset: int, *, client: str = "android") -> dict[str, Any]:
    entry = {
        "sessionKey": session_key,
        "clientIdentifier": client,
        "ratingKey": str(24712 + episode),
        "key": f"/library/metadata/{24712 + episode}",
        "guid": f"imdb://tt000000{episode}",
        "type": "episode",
        "title": f"Episode {episode}",
        "grandparentTitle": "Show",
        "grandparentGuid": "tvdb://123",
        "grandparentIndex": 1,
        "index": episode,
        "duration": DURATION,
        "viewOffset": offset,
        "state": state,
        "machineIdentifier": "server-1",
    }
    return {"type": "playing", "size": 1, "PlaySessionStateNotification": [entry]}


def _service(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, CaptureSink, list[float]]:
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
    service._plex = FakePlex()
    return service, sink, clock


def _replay(service: Any, clock: list[float], steps: list[tuple[float, dict[str, Any]]]) -> None:
    for delay, alert in steps:
        clock[0] += delay
        service._handle_alert(alert)


def _summary(sink: CaptureSink) -> list[tuple[str, int | None]]:
    return [(event.action, event.number) for event in sink.events if event.action != "pause"]


def test_item_change_on_same_session_without_stop_closes_previous_item(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock = _service(monkeypatch)

    _replay(service, clock, [
        (0, _psn("296", 3, "playing", 890_000)),
        (10, _psn("296", 3, "playing", 980_000)),
        (5, _psn("296", 4, "playing", 10_000)),
    ])

    assert _summary(sink) == [("start", 3), ("start", 3), ("stop", 3), ("start", 4)]
    stop = sink.events[2]
    assert stop.progress == 98
    assert stop.session_key == "296"
    assert stop.ids.get("imdb") == "tt0000003"
    assert stop.raw["_cw_replaced_stop"] is True


def test_new_session_per_item_keeps_single_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock = _service(monkeypatch)

    _replay(service, clock, [
        (0, _psn("509", 1, "playing", 950_000, client="web")),
        (5, _psn("509", 1, "stopped", 960_000, client="web")),
        (10, _psn("510", 2, "playing", 0, client="web")),
    ])

    assert _summary(sink) == [("start", 1), ("stop", 1), ("start", 2)]


def test_reused_session_after_real_stop_does_not_stop_twice(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock = _service(monkeypatch)

    _replay(service, clock, [
        (0, _psn("512", 1, "playing", 950_000)),
        (2, _psn("512", 1, "stopped", 970_000)),
        (0.05, _psn("512", 1, "paused", 970_000)),
        (11, _psn("512", 2, "buffering", 970_000)),
        (2, _psn("512", 2, "playing", 0)),
        (10, _psn("512", 2, "playing", 10_000)),
    ])

    assert [entry for entry in _summary(sink) if entry[0] == "stop"] == [("stop", 1)]
    assert _summary(sink)[-1] == ("start", 2)


def test_trailing_event_on_next_session_does_not_stop_twice(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock = _service(monkeypatch)

    _replay(service, clock, [
        (0, _psn("512", 2, "playing", 950_000)),
        (9, _psn("512", 2, "stopped", 970_000)),
        (0.05, _psn("513", 2, "paused", 970_000)),
        (13, _psn("513", 3, "playing", 0)),
    ])

    assert [entry for entry in _summary(sink) if entry[0] == "stop"] == [("stop", 2)]
    assert _summary(sink)[-1] == ("start", 3)


def test_stale_playback_is_not_closed_on_item_change(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink, clock = _service(monkeypatch)

    _replay(service, clock, [
        (0, _psn("296", 3, "playing", 500_000)),
        (300, _psn("296", 4, "playing", 10_000)),
    ])

    assert _summary(sink) == [("start", 3), ("start", 4)]
