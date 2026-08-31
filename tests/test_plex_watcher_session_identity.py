from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

import pytest


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
        xml = self._sessions.pop(0) if self._sessions else "<MediaContainer />"
        return ET.fromstring(xml)

    def sessions(self) -> list[Any]:
        return []


def _session_xml(
    session_key: str,
    *,
    user_name: str = "",
    user_id: str = "",
    account_name: str = "",
    account_id: str = "",
    account_uuid: str = "",
) -> str:
    user_attrs = []
    if user_name:
        user_attrs.append(f'title="{user_name}"')
    if user_id:
        user_attrs.append(f'id="{user_id}"')
    account_attrs = []
    if account_name:
        account_attrs.append(f'title="{account_name}"')
    if account_id:
        account_attrs.append(f'id="{account_id}"')
    if account_uuid:
        account_attrs.append(f'uuid="{account_uuid}"')
    user = f"<User {' '.join(user_attrs)} />" if user_attrs else ""
    account = f"<Account {' '.join(account_attrs)} />" if account_attrs else ""
    return f'<MediaContainer><Video sessionKey="{session_key}">{user}{account}</Video></MediaContainer>'


def _cfg(username_whitelist: list[str] | None, *, unresolved_user_fallback: bool = False) -> dict[str, Any]:
    filters: dict[str, Any] = {}
    if username_whitelist is not None:
        filters["username_whitelist"] = username_whitelist
    watch: dict[str, Any] = {"filters": filters}
    if unresolved_user_fallback:
        watch["route_options"] = {"watch": {"unresolved_user_fallback": True}}
    return {
        "plex": {
            "server_url": "http://plex.test",
            "account_token": "token",
            "username": "owner",
        },
        "scrobble": {
            "enabled": True,
            "sources": {"watcher": True},
            "watch": watch,
        },
    }


def _alert(session_key: str, *, media_type: str = "movie", state: str = "playing") -> dict[str, Any]:
    entry: dict[str, Any] = {
        "state": state,
        "sessionKey": session_key,
        "title": "Movie" if media_type == "movie" else "Episode",
        "type": media_type,
        "guid": "imdb://tt0000001",
        "duration": 1_000_000,
        "viewOffset": 50_000,
        "machineIdentifier": "server-1",
    }
    if media_type == "episode":
        entry.update(
            {
                "grandparentTitle": "Show",
                "grandparentGuid": "tvdb://123",
                "grandparentIndex": 1,
                "index": 2,
            }
        )
    return {"type": "playing", "PlaySessionStateNotification": [entry]}


def _service(monkeypatch: pytest.MonkeyPatch, cfg: dict[str, Any], plex: FakePlex) -> tuple[Any, CaptureSink]:
    from providers.scrobble.plex import watch
    from providers.scrobble.scrobble import Dispatcher

    monkeypatch.setattr(watch, "_cw_update", lambda *args, **kwargs: None)
    monkeypatch.setattr(watch, "_cw_update_payload", lambda *args, **kwargs: None)

    sink = CaptureSink()
    dispatcher = Dispatcher([sink], cfg_provider=lambda: cfg)
    service = watch.WatchService(dispatcher=dispatcher, cfg_provider=lambda: cfg, quiet_startup=True)
    service._plex = plex
    return service, sink


def test_owner_playback_with_whitelist_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["owner"])
    plex = FakePlex([_session_xml("s-owner", user_name="owner", user_id="u1", account_id="a1", account_uuid="uuid-owner")])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert("s-owner"))

    assert len(sink.events) == 1
    assert sink.events[0].account == "owner"


def test_shared_user_playback_with_whitelist_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["owner"])
    plex = FakePlex([_session_xml("s-shared", user_name="shared", user_id="u2", account_id="a2", account_uuid="uuid-shared")])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert("s-shared"))

    assert sink.events == []


def test_unknown_user_with_whitelist_is_rejected_despite_configured_username(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.scrobble import from_plex_pssn

    parser_alert = _alert("s-unknown")
    parser_alert["PlaySessionStateNotification"][0]["accountID"] = "a1"
    parsed = from_plex_pssn(parser_alert, defaults={"username": "owner"})
    assert parsed is not None
    assert parsed.account is None

    cfg = _cfg(["owner"])
    plex = FakePlex(["<MediaContainer />"])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert("s-unknown"))

    assert sink.events == []


def test_unknown_user_without_whitelist_is_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(None)
    plex = FakePlex(["<MediaContainer />"])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert("s-open"))

    assert len(sink.events) == 1
    assert sink.events[0].account is None


def test_unresolved_user_fallback_restores_legacy_configured_username(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["owner"], unresolved_user_fallback=True)
    plex = FakePlex(["<MediaContainer />"])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert("s-fallback"))

    assert len(sink.events) == 1
    assert sink.events[0].account == "owner"
    assert sink.events[0].raw["_cw_unresolved_user_fallback_used"] is True


def test_cached_session_identity_is_used_on_stopped_event(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["owner"])
    plex = FakePlex([_session_xml("s-stop", user_name="owner", user_id="u1", account_id="a1", account_uuid="uuid-owner")])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert("s-stop"))
    service._last_event.clear()
    service._handle_alert(_alert("s-stop", state="stopped"))

    assert [event.action for event in sink.events] == ["start", "stop"]
    assert [event.account for event in sink.events] == ["owner", "owner"]
    assert plex.queries == ["/status/sessions"]


@pytest.mark.parametrize("media_type", ["movie", "episode"])
def test_movie_and_episode_use_resolved_session_identity(monkeypatch: pytest.MonkeyPatch, media_type: str) -> None:
    cfg = _cfg(["owner"])
    plex = FakePlex([_session_xml(f"s-{media_type}", user_name="owner", user_id="u1", account_id="a1", account_uuid="uuid-owner")])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert(f"s-{media_type}", media_type=media_type))

    assert len(sink.events) == 1
    assert sink.events[0].account == "owner"
    assert sink.events[0].media_type == media_type
