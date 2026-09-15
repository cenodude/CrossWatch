from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

import pytest


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

    def __init__(self, sessions: list[str]) -> None:
        self._sessions = list(sessions)
        self.queries: list[str] = []

    def query(self, path: str) -> ET.Element:
        self.queries.append(path)
        return ET.fromstring(self._sessions.pop(0) if self._sessions else "<MediaContainer />")

    def sessions(self) -> list[Any]:
        return []

    def fetchItem(self, rating_key: int) -> None:
        return None


OWNER_SESSION = '<MediaContainer><Video sessionKey="85"><User id="1" title="owner" /></Video></MediaContainer>'


def _cfg(whitelist: list[str] | None, *, pkc: bool) -> dict[str, Any]:
    filters: dict[str, Any] = {}
    if whitelist is not None:
        filters["username_whitelist"] = whitelist
    return {
        "plex": {"server_url": "http://plex.test", "account_token": "token", "username": "owner"},
        "scrobble": {
            "enabled": True,
            "sources": {"watcher": True},
            "watch": {"filters": filters, "route_options": {"watch": {"plexkodiconnect_support": pkc}}},
        },
    }


def _psn(
    session_key: str,
    *,
    state: str,
    rating_key: str = "",
    imdb: str = "tt0000001",
    offset: int = 50_000,
    client: str = "pkc-client",
    duration: int | None = None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "sessionKey": session_key,
        "clientIdentifier": client,
        "guid": f"imdb://{imdb}" if rating_key else "",
        "ratingKey": rating_key,
        "url": "",
        "key": f"/library/metadata/{rating_key}" if rating_key else "",
        "viewOffset": offset,
        "state": state,
        "machineIdentifier": "server-1",
    }
    if rating_key:
        entry.update({"type": "movie", "title": f"Movie {rating_key}"})
    if duration is not None:
        entry["duration"] = duration
    return {"type": "playing", "size": 1, "PlaySessionStateNotification": [entry]}


def _prepare(monkeypatch: pytest.MonkeyPatch) -> Any:
    from providers.scrobble.plex import watch

    monkeypatch.setattr(watch, "_cw_update", lambda *args, **kwargs: None)
    monkeypatch.setattr(watch, "_cw_update_payload", lambda *args, **kwargs: None)
    monkeypatch.setattr(watch.time, "sleep", lambda *args, **kwargs: None)
    return watch


def _service(monkeypatch: pytest.MonkeyPatch, cfg: dict[str, Any], plex: FakePlex) -> tuple[Any, CaptureSink]:
    from providers.scrobble.scrobble import Dispatcher

    watch = _prepare(monkeypatch)
    sink = CaptureSink()
    service = watch.WatchService(dispatcher=Dispatcher([sink], cfg_provider=lambda: cfg), cfg_provider=lambda: cfg, quiet_startup=True)
    service._plex = plex
    return service, sink


def _pkc_stop(service: Any, *, rating_key: str = "1001", imdb: str = "tt0000001", offset: int = 50_000, client: str = "pkc-client") -> None:
    service._handle_alert(_psn("85", state="stopped", offset=50_000))
    service._handle_alert(_psn("86", state="stopped", rating_key=rating_key, imdb=imdb, offset=offset, client=client))
    service._handle_alert(_psn("87", state="stopped", offset=50_000))


def _start(service: Any, *, rating_key: str = "1001", imdb: str = "tt0000001", offset: int = 50_000) -> None:
    service._handle_alert(_psn("85", state="playing", rating_key=rating_key, imdb=imdb, offset=offset, duration=1_000_000))


def test_pkc_stop_keeps_user_and_item_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink = _service(monkeypatch, _cfg(["owner"], pkc=True), FakePlex([OWNER_SESSION]))

    _start(service)
    _pkc_stop(service)

    assert [e.action for e in sink.events] == ["start", "stop"]
    stop = sink.events[1]
    assert stop.account == "owner"
    assert stop.session_key == "85"
    assert stop.ids.get("imdb") == "tt0000001"
    assert stop.raw["_cw_pkc_merge"] == {"session_key": "85", "stop_session_key": "86"}


def test_pkc_itemless_stops_are_dropped_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink = _service(monkeypatch, _cfg(None, pkc=False), FakePlex([]))

    _start(service)
    _pkc_stop(service)

    assert [e.action for e in sink.events] == ["start", "stop"]
    stop = sink.events[1]
    assert stop.session_key == "86"
    assert not stop.account
    assert "_cw_pkc_merge" not in (stop.raw or {})
    assert service._pkc_pending == {}


def test_pkc_enabled_with_whitelist_still_rejects_unmerged_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink = _service(monkeypatch, _cfg(["owner"], pkc=False), FakePlex([OWNER_SESSION]))

    _start(service)
    _pkc_stop(service)

    assert [e.action for e in sink.events] == ["start"]


@pytest.mark.parametrize("mismatch", [{"offset": 60_000}, {"client": "other-client"}])
def test_pkc_merge_requires_same_player_and_offset(monkeypatch: pytest.MonkeyPatch, mismatch: dict[str, Any]) -> None:
    service, sink = _service(monkeypatch, _cfg(["owner"], pkc=True), FakePlex([OWNER_SESSION]))

    _start(service)
    _pkc_stop(service, **mismatch)

    assert [e.action for e in sink.events] == ["start"]


def test_pkc_merge_expires(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink = _service(monkeypatch, _cfg(["owner"], pkc=True), FakePlex([OWNER_SESSION]))
    watch = _prepare(monkeypatch)
    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])

    _start(service)
    service._handle_alert(_psn("85", state="stopped"))
    clock[0] += 3.0
    service._handle_alert(_psn("86", state="stopped", rating_key="1001"))

    assert [e.action for e in sink.events] == ["start"]


def test_pkc_autoplay_stop_carries_the_new_item(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink = _service(monkeypatch, _cfg(["owner"], pkc=True), FakePlex([OWNER_SESSION]))

    _start(service)
    _start(service, rating_key="1002", imdb="tt0000002", offset=12_619)
    _pkc_stop(service, rating_key="1002", imdb="tt0000002", offset=50_000)

    stops = [e for e in sink.events if e.action == "stop"]
    assert len(stops) == 1
    assert stops[0].ids.get("imdb") == "tt0000002"
    assert stops[0].account == "owner"


def test_pkc_merge_is_applied_per_route(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.scrobble import Dispatcher
    from providers.scrobble.watch_manager import MultiDispatcher

    watch = _prepare(monkeypatch)
    cfg_on = _cfg(["owner"], pkc=True)
    cfg_off = _cfg(None, pkc=False)
    sink_on, sink_off = CaptureSink(), CaptureSink()
    multi = MultiDispatcher([Dispatcher([sink_on], cfg_provider=lambda: cfg_on), Dispatcher([sink_off], cfg_provider=lambda: cfg_off)])
    service = watch.WatchService(dispatcher=multi, cfg_provider=lambda: cfg_on, quiet_startup=True)
    service._plex = FakePlex([OWNER_SESSION])

    _start(service)
    _pkc_stop(service)

    on_stop = [e for e in sink_on.events if e.action == "stop"]
    off_stop = [e for e in sink_off.events if e.action == "stop"]
    assert on_stop[0].account == "owner"
    assert on_stop[0].session_key == "85"
    assert not off_stop[0].account
    assert off_stop[0].session_key == "86"
    assert "_cw_pkc_merge" not in (off_stop[0].raw or {})


def test_normal_client_stop_is_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink = _service(monkeypatch, _cfg(["owner"], pkc=True), FakePlex([OWNER_SESSION]))

    _start(service)
    service._handle_alert(_psn("85", state="stopped", rating_key="1001"))

    assert [e.action for e in sink.events] == ["start", "stop"]
    assert sink.events[1].session_key == "85"
    assert "_cw_pkc_merge" not in (sink.events[1].raw or {})
    assert service._pkc_pending == {}


@pytest.mark.parametrize(("value", "expected"), [(True, True), (False, False), ("true", False)])
def test_route_option_is_strict_boolean(value: Any, expected: bool) -> None:
    from providers.scrobble.routes import normalize_route_options

    assert normalize_route_options({"watch": {"plexkodiconnect_support": value}})["watch"]["plexkodiconnect_support"] is expected
    assert "plexkodiconnect_support" not in normalize_route_options({})["watch"]
