from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import replace
from typing import Any

import pytest


@pytest.fixture(autouse=True)
def no_cloud_user_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    monkeypatch.setattr(watch, "fetch_cloud_home_users", lambda *args, **kwargs: [])
    monkeypatch.setattr(watch, "fetch_cloud_account_users", lambda *args, **kwargs: [])


@pytest.mark.parametrize("whitelist", ["AccountName", "accountname", "Display Name", "id:12345"])
@pytest.mark.parametrize("directory", ["fetch_cloud_home_users", "fetch_cloud_account_users"])
def test_session_display_name_matches_username_by_user_id(monkeypatch: pytest.MonkeyPatch, whitelist: str, directory: str) -> None:
    from providers.scrobble.plex import watch

    monkeypatch.setattr(watch, directory, lambda *args, **kwargs: [{"id": 12345, "username": "AccountName", "title": "Display Name"}])
    cfg = _cfg([whitelist])
    service, sink = _service(monkeypatch, cfg, FakePlex([_session_xml("s-alias", user_name="Display Name", user_id="12345")]))

    service._handle_alert(_alert("s-alias"))

    assert len(sink.events) == 1
    assert sink.events[0].account == "Display Name"


@pytest.mark.parametrize("user_id", ["99999", ""])
def test_same_display_name_does_not_link_different_users(monkeypatch: pytest.MonkeyPatch, user_id: str) -> None:
    from providers.scrobble.plex import watch

    monkeypatch.setattr(watch, "fetch_cloud_home_users", lambda *args, **kwargs: [{"id": 12345, "username": "AccountName", "title": "Display Name"}])
    service, sink = _service(monkeypatch, _cfg(["AccountName"]), FakePlex([_session_xml("s-other", user_name="Display Name", user_id=user_id)]))

    service._handle_alert(_alert("s-other"))

    assert sink.events == []


def test_session_username_is_used_without_cloud_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    calls: list[str] = []
    monkeypatch.setattr(watch, "fetch_cloud_home_users", lambda *args, **kwargs: calls.append("home") or [])
    xml = '<MediaContainer><Video sessionKey="s-direct"><User id="12345" title="Display Name" username="AccountName" /></Video></MediaContainer>'
    service, sink = _service(monkeypatch, _cfg(["AccountName"]), FakePlex([xml]))

    service._handle_alert(_alert("s-direct"))

    assert len(sink.events) == 1
    assert calls == []


@pytest.mark.parametrize("restriction", ["user", "server", "empty_profile"])
def test_username_alias_preserves_route_restrictions(monkeypatch: pytest.MonkeyPatch, restriction: str) -> None:
    from providers.scrobble.plex import watch

    monkeypatch.setattr(watch, "fetch_cloud_home_users", lambda *args, **kwargs: [{"id": 12345, "username": "AccountName"}])
    cfg = _cfg(["AccountName"])
    filters = cfg["scrobble"]["watch"]["filters"]
    if restriction == "user":
        filters["user_id"] = "99999"
    elif restriction == "server":
        filters["server_uuid_whitelist"] = ["other-server"]
    else:
        filters["username_whitelist"] = []
        cfg["scrobble"]["watch"]["route_profile_id"] = "profile-test"
    service, sink = _service(monkeypatch, cfg, FakePlex([_session_xml("s-filter", user_name="Display Name", user_id="12345")]))

    service._handle_alert(_alert("s-filter"))

    assert sink.events == []


def test_user_alias_cache_refreshes_and_isolates_profiles(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    clock = [100.0]
    calls: list[str] = []
    rows = [{"id": 12345, "username": "AccountName"}]

    def fetch(token: str, **kwargs: Any) -> list[dict[str, Any]]:
        calls.append(token)
        return list(rows)

    monkeypatch.setattr(watch.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(watch, "fetch_cloud_home_users", fetch)
    cfg = _cfg(["AccountName"])
    service, _ = _service(monkeypatch, cfg, FakePlex([]))
    assert service._user_aliases("12345") == ["AccountName"]
    assert service._user_aliases("12345") == ["AccountName"]
    assert len(calls) == 1
    rows[0] = {"id": 12345, "username": "RenamedAccount"}
    clock[0] += 301
    assert service._user_aliases("12345") == ["RenamedAccount"]
    assert len(calls) == 2
    rows.clear()
    cfg["plex"]["account_token"] = "other-test-token"
    assert service._user_aliases("12345") == []
    assert len(calls) == 3
    rows.append({"id": 12345, "username": "NewProfileAccount"})
    other, _ = _service(monkeypatch, cfg, FakePlex([]))
    assert other._user_aliases("12345") == ["NewProfileAccount"]
    assert service._user_aliases("12345") == []
    clock[0] += 61
    assert service._user_aliases("12345") == ["NewProfileAccount"]


def test_cloud_failure_does_not_guess_username(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    def failed(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        raise RuntimeError("unavailable")

    monkeypatch.setattr(watch, "fetch_cloud_home_users", failed)
    monkeypatch.setattr(watch, "fetch_cloud_account_users", failed)
    service, sink = _service(monkeypatch, _cfg(["AccountName"]), FakePlex([_session_xml("s-failed", user_name="Display Name", user_id="12345")]))

    service._handle_alert(_alert("s-failed"))

    assert sink.events == []


def test_plex_aliases_do_not_apply_to_other_source_providers(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.scrobble import Dispatcher, from_plex_pssn

    cfg = _cfg(["AccountName"])
    cfg["scrobble"]["watch"]["route_provider"] = "jellyfin"
    service, _ = _service(monkeypatch, cfg, FakePlex([]))
    event = from_plex_pssn(_alert("s-source"))
    assert event is not None
    event = service._event_with_session_identity(event, {"name": "Display Name", "user_username": "AccountName", "user_id": "12345"})

    assert Dispatcher([], cfg_provider=lambda: cfg)._identity_allowed(event, cfg) is False


class CaptureSink:
    def __init__(self) -> None:
        self.events: list[Any] = []

    def send(self, event: Any, *args: Any, **kwargs: Any) -> None:
        self.events.append(event)


class FakePlex:
    machineIdentifier = "server-1"

    def __init__(self, sessions: list[str | Exception]) -> None:
        self._sessions = list(sessions)
        self.queries: list[str] = []

    def query(self, path: str) -> ET.Element:
        self.queries.append(path)
        xml = self._sessions.pop(0) if self._sessions else "<MediaContainer />"
        if isinstance(xml, Exception):
            raise xml
        return ET.fromstring(xml)

    def sessions(self) -> list[Any]:
        return []


def _video_xml(
    session_key: str,
    *,
    user_name: str = "",
    user_id: str = "",
    account_name: str = "",
    account_id: str = "",
    account_uuid: str = "",
    rating_key: str = "",
    client: str = "",
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
    player = f'<Player machineIdentifier="{client}" />' if client else ""
    return f'<Video sessionKey="{session_key}" ratingKey="{rating_key}" guid="imdb://tt0000001">{user}{account}{player}</Video>'


def _sessions_xml(*videos: str) -> str:
    return f"<MediaContainer>{''.join(videos)}</MediaContainer>"


def _session_xml(session_key: str, **kwargs: Any) -> str:
    return _sessions_xml(_video_xml(session_key, **kwargs))


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
    monkeypatch.setattr(watch.time, "sleep", lambda *args, **kwargs: None)

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
    assert plex.queries == []


def test_event_user_is_used_without_session_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["Carmen"])
    plex = FakePlex(["<MediaContainer />"])
    service, sink = _service(monkeypatch, cfg, plex)
    alert = _alert("s-event-user")
    alert["PlaySessionStateNotification"][0]["account"] = "Carmen"

    service._handle_alert(alert)

    assert len(sink.events) == 1
    assert sink.events[0].account == "Carmen"
    assert plex.queries == []


def test_unresolved_user_fallback_restores_legacy_configured_username(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["owner"], unresolved_user_fallback=True)
    plex = FakePlex([RuntimeError("403 forbidden")])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert("s-fallback"))

    assert len(sink.events) == 1
    assert sink.events[0].account == "owner"
    assert sink.events[0].raw["_cw_unresolved_user_fallback_used"] is True
    assert service._last_event["s-fallback"].raw["_cw_sessions_access_unavailable"] is True


def test_unresolved_user_fallback_disabled_rejects_when_sessions_inaccessible(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["owner"], unresolved_user_fallback=False)
    plex = FakePlex([RuntimeError("403 forbidden")])
    service, sink = _service(monkeypatch, cfg, plex)
    messages: list[str] = []
    monkeypatch.setattr(service, "_dbg", lambda msg: messages.append(msg))

    service._handle_alert(_alert("s-disabled"))

    assert sink.events == []
    assert [m for m in messages if m.startswith("event filtered by route dispatcher")] == []


def test_unresolved_user_fallback_waits_when_sessions_are_accessible(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["owner"], unresolved_user_fallback=True)
    plex = FakePlex(["<MediaContainer />"])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert("s-temporary-miss"))

    assert sink.events == []
    assert service._no_sessions_access is False


def test_session_identity_uses_one_fetch_and_resolves_on_the_next_event(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["Carmen"])
    plex = FakePlex(["<MediaContainer />", _session_xml("s-race", user_name="Carmen", user_id="176467484")])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert("s-race"))
    assert sink.events == []
    assert plex.queries == ["/status/sessions"]

    service._handle_alert(_alert("s-race"))
    assert len(sink.events) == 1
    assert sink.events[0].account == "Carmen"
    assert plex.queries == ["/status/sessions", "/status/sessions"]


def test_route_filtered_log_does_not_show_unresolved_fallback_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch
    from providers.scrobble.scrobble import ScrobbleEvent

    cfg = _cfg(["owner"], unresolved_user_fallback=False)
    service = watch.WatchService(dispatcher=None, cfg_provider=lambda: cfg, quiet_startup=True)
    messages: list[str] = []
    monkeypatch.setattr(service, "_dbg", lambda msg: messages.append(msg))
    ev = ScrobbleEvent(
        action="start",
        media_type="movie",
        ids={"imdb": "tt1"},
        title="Movie",
        year=2020,
        season=None,
        number=None,
        progress=5.0,
        account=None,
        server_uuid="server-1",
        session_key="s-log",
        raw={"_cw_sessions_access_unavailable": True, "_cw_unresolved_user_fallback": {"account": "owner"}},
    )

    service._throttled_route_filtered_log(ev)

    assert messages == []


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


def _route_dispatcher_cfg(route_id: str, sink_name: str, whitelist: list[str] | None, fallback: bool) -> dict[str, Any]:
    cfg = _cfg(whitelist, unresolved_user_fallback=fallback)
    watch_cfg = cfg["scrobble"]["watch"]
    watch_cfg["route_id"] = route_id
    watch_cfg["route_provider"] = "plex"
    watch_cfg["route_sink"] = sink_name
    return cfg


def _dispatch_and_capture(monkeypatch: pytest.MonkeyPatch, cfg: dict[str, Any], raw: dict[str, Any], account: str | None = None) -> list[str]:
    import providers.scrobble.scrobble as scrobble_mod
    from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent

    messages: list[str] = []
    monkeypatch.setattr(scrobble_mod, "_log", lambda msg, lvl="INFO": messages.append(str(msg)))
    dispatcher = Dispatcher([CaptureSink()], cfg_provider=lambda: cfg)
    ev = ScrobbleEvent(
        action="start",
        media_type="movie",
        ids={"imdb": "tt1"},
        title="Movie",
        year=2020,
        season=None,
        number=None,
        progress=5.0,
        account=account,
        server_uuid="server-1",
        session_key="30",
        raw=raw,
    )
    dispatcher.dispatch(ev)
    return messages


def test_route_filter_log_names_the_route_and_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _route_dispatcher_cfg("R1", "simkl", ["Carmen"], False)

    messages = _dispatch_and_capture(monkeypatch, cfg, {}, account="Pascal")

    assert messages == ["route R1 plex->simkl: filtered user=Pa*** sess=30 reason=username_whitelist"]


def test_route_filter_stays_quiet_while_identity_is_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _route_dispatcher_cfg("R1", "simkl", ["Carmen"], False)

    messages = _dispatch_and_capture(monkeypatch, cfg, {})

    assert messages == []


@pytest.mark.parametrize("restriction", ["username_whitelist", "user_id", "profile_scoped_without_whitelist"])
def test_route_filter_logs_stay_quiet_during_long_playback(monkeypatch: pytest.MonkeyPatch, restriction: str) -> None:
    from providers.scrobble import scrobble

    cfg = _route_dispatcher_cfg("R1", "simkl", ["owner"], False)
    if restriction == "user_id":
        cfg["scrobble"]["watch"]["filters"]["user_id"] = "owner-id"
    elif restriction == "profile_scoped_without_whitelist":
        cfg["scrobble"]["watch"]["filters"] = {}
        cfg["scrobble"]["watch"]["route_profile_id"] = "profile-1"
    clock = [1000.0]
    messages: list[tuple[str, str]] = []
    monkeypatch.setattr(scrobble.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(scrobble.time, "time", lambda: clock[0])
    monkeypatch.setattr(scrobble, "_log", lambda msg, lvl="INFO": messages.append((msg, lvl)))
    sink = CaptureSink()
    dispatcher = scrobble.Dispatcher([sink], cfg_provider=lambda: cfg)
    parsed = scrobble.from_plex_pssn(_alert("30"))
    assert parsed is not None
    ev = replace(parsed, account="shared")

    for _ in range(240):
        assert not dispatcher.accepts_user(ev)
        assert not dispatcher.dispatch(ev)
        clock[0] += 30.0

    level = "WARNING" if restriction == "profile_scoped_without_whitelist" else "DEBUG"
    assert messages == [(f"route R1 plex->simkl: filtered user=sh*** sess=30 reason={restriction}", level)]
    assert sink.events == []


def test_route_filter_logs_report_changes_and_rejections_after_acceptance(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble import scrobble

    cfg = _route_dispatcher_cfg("R1", "simkl", ["owner"], False)
    messages: list[str] = []
    monkeypatch.setattr(scrobble, "_log", lambda msg, lvl="INFO": messages.append(msg))
    dispatcher = scrobble.Dispatcher([], cfg_provider=lambda: cfg)
    parsed = scrobble.from_plex_pssn(_alert("30"))
    assert parsed is not None
    ev = replace(parsed, account="shared")

    assert not dispatcher.accepts_user(ev)
    assert not dispatcher.accepts_user(replace(ev, raw={"_cw_session_identity": {"user_id": "new-user"}}))
    cfg["scrobble"]["watch"]["filters"]["user_id"] = "owner-id"
    assert not dispatcher.accepts_user(ev)
    cfg["scrobble"]["watch"]["filters"] = {"username_whitelist": ["shared"]}
    assert dispatcher.accepts_user(ev)
    cfg["scrobble"]["watch"]["filters"] = {"username_whitelist": ["owner"]}
    assert not dispatcher.accepts_user(ev)

    assert len(messages) == 4
    assert "reason=user_id" in messages[2]
    assert "reason=username_whitelist" in messages[3]


@pytest.mark.parametrize("method", ["accepts", "dispatch"])
def test_cached_acceptance_resets_route_filter_logs(monkeypatch: pytest.MonkeyPatch, method: str) -> None:
    from providers.scrobble import scrobble

    cfg = _route_dispatcher_cfg("R1", "simkl", ["alice"], False)
    messages: list[str] = []
    monkeypatch.setattr(scrobble, "_log", lambda msg, lvl="INFO": messages.append(msg))
    dispatcher = scrobble.Dispatcher([], cfg_provider=lambda: cfg)
    parsed = scrobble.from_plex_pssn(_alert("dev-1"))
    assert parsed is not None
    check = getattr(dispatcher, method)

    for _ in range(2):
        assert check(replace(parsed, account="alice"))
        assert not check(replace(parsed, account="bob"))

    assert messages == [
        "route R1 plex->simkl: filtered user=bo*** sess=dev-1 reason=username_whitelist"
    ] * 2


@pytest.mark.parametrize("method", ["accepts", "accepts_user", "dispatch", "preflight_and_dispatch"])
def test_filter_edits_reset_rejection_logs(monkeypatch: pytest.MonkeyPatch, method: str) -> None:
    from providers.scrobble import scrobble

    cfg = _route_dispatcher_cfg("R1", "simkl", ["alice"], False)
    messages: list[str] = []
    monkeypatch.setattr(scrobble, "_log", lambda msg, lvl="INFO": messages.append(msg))
    dispatcher = scrobble.Dispatcher([], cfg_provider=lambda: cfg)
    parsed = scrobble.from_plex_pssn(_alert("dev-1"))
    assert parsed is not None
    ev = replace(parsed, account="bob")

    for whitelist in (["alice"], ["b0b"]):
        cfg["scrobble"]["watch"]["filters"]["username_whitelist"] = whitelist
        for _ in range(2):
            if method == "preflight_and_dispatch":
                assert not dispatcher.accepts_user(ev)
                assert not dispatcher.dispatch(ev)
            else:
                assert not getattr(dispatcher, method)(ev)

    assert messages == [
        "route R1 plex->simkl: filtered user=bo*** sess=dev-1 reason=username_whitelist"
    ] * 2


def test_route_filter_log_state_is_scoped_bounded_and_expires(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble import scrobble

    cfg = _route_dispatcher_cfg("R1", "simkl", ["owner"], False)
    clock = [1000.0]
    messages: list[str] = []
    monkeypatch.setattr(scrobble.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(scrobble, "_log", lambda msg, lvl="INFO": messages.append(msg))
    dispatcher = scrobble.Dispatcher([], cfg_provider=lambda: cfg)
    other_route = scrobble.Dispatcher([], cfg_provider=lambda: _route_dispatcher_cfg("R2", "trakt", ["owner"], False))
    parsed = scrobble.from_plex_pssn(_alert("30"))
    assert parsed is not None
    ev = replace(parsed, account="shared")

    assert not dispatcher.accepts_user(ev)
    assert not other_route.accepts_user(ev)
    assert not dispatcher.accepts_user(replace(ev, server_uuid="server-2"))
    assert not dispatcher.accepts_user(replace(ev, session_key="31"))
    assert len(messages) == 4
    clock[0] += 1800.0
    assert not dispatcher.accepts_user(ev)
    assert len(messages) == 5

    for i in range(1100):
        assert not dispatcher.accepts_user(replace(ev, session_key=f"session-{i}"))
    assert not dispatcher.accepts_user(ev)
    assert len(messages) == 1106


def test_route_fallback_and_send_logs_name_the_route(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _route_dispatcher_cfg("R2", "mdblist", None, True)

    messages = _dispatch_and_capture(monkeypatch, cfg, {"_cw_sessions_access_unavailable": True})

    assert messages == [
        "route R2 plex->mdblist: unresolved user fallback used user=ow*** sess=30",
        "route R2 plex->mdblist: accepted start user=ow*** p=5.0 sess=30",
    ]


def _identity_logs(monkeypatch: pytest.MonkeyPatch, service: Any) -> list[str]:
    lines: list[str] = []
    monkeypatch.setattr(service, "_dbg", lambda msg: lines.append(str(msg)))
    return lines


def test_stopped_watcher_ignores_late_alert(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink = _service(monkeypatch, _cfg(["owner"]), FakePlex([_session_xml("late", user_name="owner", user_id="1")]))
    service.stop()
    service._handle_alert(_alert("late"))
    assert sink.events == []


def test_listener_created_during_stop_is_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    service, _ = _service(monkeypatch, _cfg(["owner"]), FakePlex([]))
    closed = []

    class Listener:
        def stop(self):
            closed.append(True)

    class Server:
        def startAlertListener(self, **kwargs):
            service.stop()
            return Listener()

    monkeypatch.setattr(watch, "PlexServer", lambda *args: Server())
    monkeypatch.setattr(service, "_refresh_account_context", lambda: None)
    service.start()
    assert closed == [True]
    assert service._listener is None


def test_async_stop_before_thread_starts_is_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    service, _ = _service(monkeypatch, _cfg(["owner"]), FakePlex([]))
    targets = []
    connections = []

    class DeferredThread:
        def __init__(self, *, target, **kwargs):
            targets.append(target)

        def start(self):
            pass

        def is_alive(self):
            return False

    monkeypatch.setattr(watch.threading, "Thread", DeferredThread)
    monkeypatch.setattr(watch, "PlexServer", lambda *args: connections.append(args))
    service.start_async()
    service.stop()
    targets[0]()
    assert service.is_stopping()
    assert connections == []


def test_identity_log_reports_resolution_details(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["Carmen"])
    plex = FakePlex([_session_xml("31", user_name="Carmen", user_id="176467484")])
    service, sink = _service(monkeypatch, cfg, plex)
    lines = _identity_logs(monkeypatch, service)

    service._handle_alert(_alert("31"))

    resolved = [ln for ln in lines if ln.startswith("identity resolved")]
    assert len(resolved) == 1
    assert "sess=31" in resolved[0]
    assert "user=Ca***" in resolved[0]
    assert "user_id=176467484" in resolved[0]
    assert sink.events[0].account == "Carmen"


def test_first_session_not_listed_miss_stays_silent(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["Carmen"])
    plex = FakePlex(["<MediaContainer />"])
    service, _sink = _service(monkeypatch, cfg, plex)
    lines = _identity_logs(monkeypatch, service)

    service._handle_alert(_alert("31"))

    assert [ln for ln in lines if ln.startswith("identity unresolved")] == []
    assert plex.queries == ["/status/sessions"]


def test_identity_log_reports_forbidden_without_retrying(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["Carmen"])
    plex = FakePlex([RuntimeError("403 forbidden")])
    service, _sink = _service(monkeypatch, cfg, plex)
    lines = _identity_logs(monkeypatch, service)

    service._handle_alert(_alert("31"))

    unresolved = [ln for ln in lines if ln.startswith("identity unresolved")]
    assert len(unresolved) == 1
    assert "reason=sessions_forbidden" in unresolved[0]
    assert plex.queries == ["/status/sessions"]


def test_identity_miss_log_is_emitted_once_per_unresolved_session(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    cfg = _cfg(["Carmen"])
    plex = FakePlex([])
    service, sink = _service(monkeypatch, cfg, plex)
    lines = _identity_logs(monkeypatch, service)
    now = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: now[0])

    for _ in range(450):
        service._handle_alert(_alert("31"))
        now[0] += 20.0

    assert len([ln for ln in lines if ln.startswith("identity unresolved")]) == 1
    assert sink.events == []
    assert plex.queries == ["/status/sessions"] * 450

    service._handle_alert(_alert("32"))
    service._handle_alert(_alert("32"))

    unresolved = [ln for ln in lines if ln.startswith("identity unresolved")]
    assert len(unresolved) == 2
    assert "sess=32" in unresolved[1]

    plex._sessions.append(_session_xml("31", user_name="Carmen", user_id="176467484"))
    service._handle_alert(_alert("31"))

    assert len(sink.events) == 1
    assert sink.events[0].account == "Carmen"


def test_username_whitelist_matches_plex_user_id(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["id:176467484"])
    plex = FakePlex([_session_xml("31", user_name="Carmen", user_id="176467484")])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert("31"))

    assert len(sink.events) == 1
    assert sink.events[0].account == "Carmen"


def test_username_whitelist_user_id_does_not_match_other_users(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["id:176467484"])
    plex = FakePlex([_session_xml("31", user_name="Pascal", user_id="1")])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert("31"))

    assert sink.events == []


class CountingPlex(FakePlex):
    def __init__(self, sessions: list[str | Exception]) -> None:
        super().__init__(sessions)
        self.fetches: list[int] = []

    def fetchItem(self, rating_key: int) -> None:
        self.fetches.append(int(rating_key))
        return None


def _alert_with_rating_key(session_key: str, rating_key: int = 1234) -> dict[str, Any]:
    alert = _alert(session_key)
    alert["PlaySessionStateNotification"][0]["ratingKey"] = rating_key
    return alert


def test_user_gate_skips_enrichment_for_filtered_user(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["Carmen"])
    plex = CountingPlex([_session_xml("31", user_name="Pascal", user_id="1")])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert_with_rating_key("31"))

    assert plex.fetches == []
    assert sink.events == []


def test_user_gate_still_enriches_allowed_user(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["Carmen"])
    plex = CountingPlex([_session_xml("31", user_name="Carmen", user_id="176467484")])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert_with_rating_key("31"))

    assert plex.fetches == [1234]
    assert len(sink.events) == 1


def test_user_gate_is_a_noop_without_a_whitelist(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(None)
    plex = CountingPlex(["<MediaContainer />"])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert_with_rating_key("31"))

    assert plex.queries == []
    assert plex.fetches == [1234]
    assert len(sink.events) == 1


def test_fallback_route_without_whitelist_resolves_the_real_user(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(None, unresolved_user_fallback=True)
    plex = FakePlex([_session_xml("31", user_name="Carmen", user_id="176467484")])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert("31"))

    assert plex.queries == ["/status/sessions"]
    assert len(sink.events) == 1
    assert sink.events[0].account == "Carmen"


def test_fallback_route_without_whitelist_falls_back_when_forbidden(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(None, unresolved_user_fallback=True)
    plex = FakePlex([RuntimeError("403 forbidden")])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert("31"))

    assert len(sink.events) == 1
    assert sink.events[0].account == "owner"
    assert sink.events[0].raw["_cw_unresolved_user_fallback_used"] is True


def test_route_without_whitelist_or_fallback_never_looks_up_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(None, unresolved_user_fallback=False)
    plex = FakePlex(["<MediaContainer />"])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert("31"))

    assert plex.queries == []
    assert len(sink.events) == 1


def test_forbidden_verdict_does_not_leak_into_a_later_network_error(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(None, unresolved_user_fallback=True)
    plex = FakePlex([RuntimeError("403 forbidden"), RuntimeError("connection reset by peer")])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert("31"))
    assert sink.events[0].account == "owner"
    assert service._no_sessions_access is True

    service._handle_alert(_alert("32"))
    assert service._no_sessions_access is False
    assert sink.events[1].account is None
    assert "_cw_unresolved_user_fallback_used" not in (sink.events[1].raw or {})


class NullResponsePlex(FakePlex):
    def query(self, path: str) -> Any:
        self.queries.append(path)
        nxt = self._sessions.pop(0) if self._sessions else None
        if isinstance(nxt, Exception):
            raise nxt
        return None


def test_forbidden_verdict_does_not_leak_into_a_later_bad_response(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(None, unresolved_user_fallback=True)
    plex = NullResponsePlex([RuntimeError("403 forbidden"), None])
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert("31"))
    assert sink.events[0].account == "owner"
    assert service._no_sessions_access is True

    service._handle_alert(_alert("32"))
    assert service._no_sessions_access is False
    assert sink.events[1].account is None


def test_repeated_session_not_listed_miss_is_logged(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["Carmen"])
    plex = FakePlex(["<MediaContainer />"] * 3)
    service, _sink = _service(monkeypatch, cfg, plex)
    lines = _identity_logs(monkeypatch, service)

    service._handle_alert(_alert("31"))
    assert [ln for ln in lines if ln.startswith("identity unresolved")] == []

    service._handle_alert(_alert("31"))
    unresolved = [ln for ln in lines if ln.startswith("identity unresolved")]
    assert len(unresolved) == 1
    assert "reason=session_not_listed" in unresolved[0]
    assert "misses=2" in unresolved[0]


def test_miss_counter_resets_once_the_session_resolves(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["Carmen"])
    plex = FakePlex([
        "<MediaContainer />",
        "<MediaContainer />",
        _session_xml("31", user_name="Carmen", user_id="176467484"),
        "<MediaContainer />",
        "<MediaContainer />",
    ])
    service, _sink = _service(monkeypatch, cfg, plex)
    lines = _identity_logs(monkeypatch, service)

    service._handle_alert(_alert("31"))
    service._handle_alert(_alert("31"))
    assert len([ln for ln in lines if ln.startswith("identity unresolved")]) == 1
    service._handle_alert(_alert("31"))
    assert service._identity_miss.get("31") is None

    service._sess_identity_cache.clear()
    service._last_event.clear()
    lines.clear()
    service._handle_alert(_alert("31"))
    assert [ln for ln in lines if ln.startswith("identity unresolved")] == []

    service._handle_alert(_alert("31"))
    unresolved = [ln for ln in lines if ln.startswith("identity unresolved")]
    assert len(unresolved) == 1
    assert "misses=2" in unresolved[0]


def test_forbidden_is_logged_on_the_very_first_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["Carmen"])
    plex = FakePlex([RuntimeError("403 forbidden")])
    service, _sink = _service(monkeypatch, cfg, plex)
    lines = _identity_logs(monkeypatch, service)

    service._handle_alert(_alert("31"))

    unresolved = [ln for ln in lines if ln.startswith("identity unresolved")]
    assert len(unresolved) == 1
    assert "reason=sessions_forbidden" in unresolved[0]


def _service_with_account_ctx(
    monkeypatch: pytest.MonkeyPatch, cfg: dict[str, Any], plex: FakePlex, ctx: dict[str, Any] | None
) -> tuple[Any, CaptureSink]:
    service, sink = _service(monkeypatch, cfg, plex)
    service._account_ctx = ctx
    return service, sink


def test_shared_instance_uses_plex_tv_identity_instead_of_the_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["ceno88"], unresolved_user_fallback=True)
    plex = FakePlex([RuntimeError("403 forbidden")])
    ctx = {"owned": False, "name": "ceno88", "user_id": "716210920"}
    service, sink = _service_with_account_ctx(monkeypatch, cfg, plex, ctx)

    service._handle_alert(_alert("40"))

    assert len(sink.events) == 1
    assert sink.events[0].account == "ceno88"
    assert "_cw_unresolved_user_fallback_used" not in (sink.events[0].raw or {})
    assert sink.events[0].raw["_cw_session_identity"]["user_id"] == "716210920"
    assert sink.events[0].raw["_cw_session_identity"]["account_uuid"] == ""
    assert plex.queries == []


@pytest.mark.parametrize("whitelist", [None, []])
@pytest.mark.parametrize("state", ["playing", "paused", "stopped"])
def test_shared_identity_without_user_filter_requires_no_requests(monkeypatch: pytest.MonkeyPatch, whitelist: list[str] | None, state: str) -> None:
    from providers.scrobble.plex import watch

    def unexpected(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("Known shared identity must not perform a network lookup")

    monkeypatch.setattr(watch, "fetch_cloud_home_users", unexpected)
    monkeypatch.setattr(watch, "fetch_cloud_account_users", unexpected)
    cfg = _cfg(whitelist)
    plex = FakePlex([])
    service, sink = _service_with_account_ctx(monkeypatch, cfg, plex, {"owned": False, "name": "shared-user", "user_id": "12345"})
    monkeypatch.setattr(service, "_resolve_session_identity", unexpected)
    assert not service._needs_user_resolution()

    service._handle_alert(_alert("s-known", state=state))

    assert len(sink.events) == 1
    assert sink.events[0].account == "shared-user"
    assert sink.events[0].raw["_cw_session_identity"]["user_id"] == "12345"
    assert plex.queries == []


@pytest.mark.parametrize("ctx", [None, {"owned": True, "name": "owner"}])
def test_no_user_filter_does_not_guess_an_owner_identity(monkeypatch: pytest.MonkeyPatch, ctx: dict[str, Any] | None) -> None:
    service, sink = _service_with_account_ctx(monkeypatch, _cfg([]), FakePlex([]), ctx)

    service._handle_alert(_alert("s-unknown"))

    assert len(sink.events) == 1
    assert not sink.events[0].account
    assert service._plex.queries == []


def test_owned_instance_never_probes_plex_tv(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["Carmen"])
    plex = FakePlex([_session_xml("41", user_name="Carmen", user_id="176467484")])
    service, sink = _service_with_account_ctx(monkeypatch, cfg, plex, {"owned": True, "name": "owner"})

    service._handle_alert(_alert("41"))

    assert sink.events[0].account == "Carmen"
    assert plex.queries == ["/status/sessions"]


def test_shared_identity_falls_back_when_plex_tv_is_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["owner"], unresolved_user_fallback=True)
    plex = FakePlex([RuntimeError("403 forbidden")])
    service, sink = _service_with_account_ctx(monkeypatch, cfg, plex, None)

    service._handle_alert(_alert("42"))

    assert len(sink.events) == 1
    assert sink.events[0].account == "owner"
    assert sink.events[0].raw["_cw_unresolved_user_fallback_used"] is True


class _Resp:
    def __init__(self, text: str = "", payload: Any = None, ok: bool = True) -> None:
        self.text = text
        self._payload = payload
        self._ok = ok

    def raise_for_status(self) -> None:
        if not self._ok:
            raise RuntimeError("http error")

    def json(self) -> Any:
        return self._payload


def _fake_plex_tv(monkeypatch: pytest.MonkeyPatch, *, owned: str, identity: Any) -> list[str]:
    from providers.scrobble.plex import watch

    calls: list[str] = []

    def _get(url: str, **kwargs: Any) -> _Resp:
        calls.append(url)
        if "resources" in url:
            return _Resp(text=f'<MediaContainer><Device clientIdentifier="server-1" owned="{owned}" /></MediaContainer>')
        if identity is None:
            return _Resp(ok=False)
        return _Resp(payload=identity)

    monkeypatch.setattr(watch.requests, "get", _get)
    return calls


def _service_for_refresh(monkeypatch: pytest.MonkeyPatch) -> Any:
    cfg = _cfg(["ceno88"])
    service, _sink = _service(monkeypatch, cfg, FakePlex([]))
    return service


def test_refresh_account_context_detects_shared_token(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service_for_refresh(monkeypatch)
    calls = _fake_plex_tv(monkeypatch, owned="0", identity={"username": "ceno88", "id": 716210920})

    service._refresh_account_context()

    assert service._account_ctx == {"owned": False, "name": "ceno88", "user_id": "716210920"}
    assert len(calls) == 2


def test_refresh_account_context_skips_identity_for_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service_for_refresh(monkeypatch)
    calls = _fake_plex_tv(monkeypatch, owned="1", identity={"username": "owner", "id": 1})

    service._refresh_account_context()

    assert service._account_ctx is None
    assert len(calls) == 1


def test_refresh_account_context_falls_back_when_identity_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service_for_refresh(monkeypatch)
    _fake_plex_tv(monkeypatch, owned="0", identity=None)

    service._refresh_account_context()

    assert service._account_ctx is None


def test_refresh_account_context_clears_stale_state(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _service_for_refresh(monkeypatch)
    service._account_ctx = {"owned": False, "name": "old-user", "user_id": "1"}
    _fake_plex_tv(monkeypatch, owned="1", identity={"username": "owner", "id": 1})

    service._refresh_account_context()

    assert service._account_ctx is None


def test_route_label_names_non_default_instances(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _route_dispatcher_cfg("R4", "crosswatch", ["zzz-no-one"], False)
    watch_cfg = cfg["scrobble"]["watch"]
    watch_cfg["route_provider_instance"] = "PLEX-P01"
    watch_cfg["route_sink_instance"] = "default"
    cfg["plex"]["label"] = "Ceno"

    messages = _dispatch_and_capture(monkeypatch, cfg, {}, account="ceno88")

    assert messages == [
        "route R4 plex[Ceno]->crosswatch: filtered user=ce*** sess=30 reason=username_whitelist"
    ]


def test_route_label_falls_back_to_instance_id_without_a_label(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _route_dispatcher_cfg("R4", "crosswatch", ["zzz-no-one"], False)
    cfg["scrobble"]["watch"]["route_provider_instance"] = "PLEX-P02"

    messages = _dispatch_and_capture(monkeypatch, cfg, {}, account="ceno88")

    assert messages == [
        "route R4 plex[PLEX-P02]->crosswatch: filtered user=ce*** sess=30 reason=username_whitelist"
    ]


def test_route_label_omits_default_instances(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _route_dispatcher_cfg("R1", "simkl", ["Carmen"], False)
    cfg["scrobble"]["watch"]["route_provider_instance"] = "default"

    messages = _dispatch_and_capture(monkeypatch, cfg, {}, account="Pascal")

    assert messages == ["route R1 plex->simkl: filtered user=Pa*** sess=30 reason=username_whitelist"]


def test_watcher_log_prefixes_use_friendly_instance_names(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    lines: list[str] = []
    monkeypatch.setattr(watch, "BASE_LOG", lambda msg, level="INFO", module="": lines.append(str(msg)))

    def _svc(instance_id: str, plex_cfg: dict[str, Any]) -> Any:
        cfg = _cfg(None)
        cfg["plex"].update(plex_cfg)
        return watch.WatchService(
            dispatcher=None, cfg_provider=lambda: cfg, quiet_startup=True, instance_id=instance_id
        )

    _svc("default", {})._log("hello")
    _svc("default", {"label": "Main"})._log("hello")
    _svc("PLEX-P01", {"instances": {"PLEX-P01": {"label": "Ceno"}}})._log("hello")
    _svc("PLEX-P02", {"instances": {"PLEX-P02": {}}})._log("hello")
    _svc("PLEX-P01", {"instances": {"PLEX-P01": {"label": "Ceno"}}})._log("Watcher connected; inst=Ceno")

    assert lines == [
        "hello",
        "[Main] hello",
        "[Ceno] hello",
        "[PLEX-P02] hello",
        "Watcher connected; inst=Ceno",
    ]


def test_one_sessions_fetch_resolves_every_active_session(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["owner"])
    plex = FakePlex(
        [
            _sessions_xml(
                _video_xml("s-shared", user_name="shared", user_id="u2", account_id="a2"),
                _video_xml("s-owner", user_name="owner", user_id="u1", account_id="a1"),
            )
        ]
    )
    service, sink = _service(monkeypatch, cfg, plex)

    service._handle_alert(_alert("s-shared"))
    service._handle_alert(_alert("s-owner"))

    assert plex.queries == ["/status/sessions"]
    assert [e.account for e in sink.events] == ["owner"]


def test_identity_resolved_is_logged_once_per_session(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["Carmen"])
    row = _session_xml("31", user_name="Carmen", user_id="176467484")
    plex = FakePlex([row, row, row])
    service, _sink = _service(monkeypatch, cfg, plex)
    lines = _identity_logs(monkeypatch, service)

    for _ in range(3):
        service._sess_identity_cache.clear()
        service._last_event.clear()
        service._handle_alert(_alert("31"))

    assert plex.queries == ["/status/sessions"] * 3
    assert len([ln for ln in lines if ln.startswith("identity resolved")]) == 1


def test_filtered_identity_revalidation_logs_only_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    row = _session_xml("31", user_name="shared", user_id="2")
    changed = _session_xml("31", user_name="other", user_id="3")
    plex = FakePlex([row] * 12 + [changed])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    lines = _identity_logs(monkeypatch, service)

    for _ in range(12):
        service._handle_alert(_alert("31"))
        clock[0] += 30.0

    assert len(plex.queries) == 12
    assert len([line for line in lines if line.startswith("identity resolved")]) == 1
    service._handle_alert(_alert("31"))
    resolved = [line for line in lines if line.startswith("identity resolved")]
    assert len(resolved) == 2
    assert "user=ot***" in resolved[-1]
    assert sink.events == []


def test_identity_is_logged_again_when_the_session_key_changes_hands(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["Carmen", "Pascal"])
    plex = FakePlex([
        _session_xml("31", user_name="Carmen", user_id="176467484"),
        _session_xml("31", user_name="Pascal", user_id="114349713"),
    ])
    service, _sink = _service(monkeypatch, cfg, plex)
    lines = _identity_logs(monkeypatch, service)

    service._handle_alert(_alert("31"))
    service._sess_identity_cache.clear()
    service._last_event.clear()
    service._handle_alert(_alert("31"))

    resolved = [ln for ln in lines if ln.startswith("identity resolved")]
    assert len(resolved) == 2
    assert "user=Ca***" in resolved[0]
    assert "user=Pa***" in resolved[1]


def test_route_filtered_log_is_emitted_once_per_session(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["owner"])
    row = _session_xml("s-shared", user_name="shared", user_id="u2", account_id="a2")
    plex = FakePlex([row, row, row])
    service, sink = _service(monkeypatch, cfg, plex)
    messages: list[str] = []
    monkeypatch.setattr(service, "_dbg", lambda msg: messages.append(str(msg)))

    for _ in range(3):
        service._sess_identity_cache.clear()
        service._handle_alert(_alert("s-shared"))
        for key in list(service._route_filtered_ts):
            service._route_filtered_ts[key] -= 60.0

    assert sink.events == []
    assert len([m for m in messages if m.startswith("event filtered by route dispatcher")]) == 1


@pytest.mark.parametrize("whitelist", [["owner"], []])
@pytest.mark.parametrize("media_type", ["movie", "episode"])
def test_failed_delivery_logs_identity_before_dispatch(monkeypatch: pytest.MonkeyPatch, whitelist: list[str], media_type: str) -> None:
    cfg = _cfg(whitelist)
    service, sink = _service(monkeypatch, cfg, FakePlex([_session_xml("s-failed", user_name="owner", user_id="u1")]))
    messages: list[str] = []
    monkeypatch.setattr(service, "_log", lambda msg, *args: messages.append(str(msg)))
    monkeypatch.setattr(service, "_dbg", lambda msg: messages.append(str(msg)))

    def fail_delivery(event: Any, **kwargs: Any) -> dict[str, Any]:
        messages.append("delivery attempted")
        return {"ok": False, "retryable": False, "error": "unmatched_in_jellyfin"}

    monkeypatch.setattr(sink, "send", fail_delivery)
    service._handle_alert(_alert("s-failed", media_type=media_type))

    attempt = messages.index("delivery attempted")
    incoming = next(i for i, msg in enumerate(messages) if msg.startswith("incoming 'start'"))
    ids = next(i for i, msg in enumerate(messages) if msg.startswith("ids resolved:"))
    assert incoming < ids < attempt
    assert "sess=s-failed" in messages[incoming]
    assert "imdb:tt0000001" in messages[ids]
    assert "sess=s-failed" in messages[ids]
    assert any(msg.startswith("event not delivered by route dispatcher") for msg in messages)
    assert not any("filtered by route dispatcher" in msg for msg in messages)
    if not whitelist:
        assert any("user=unknown" in msg for msg in messages if "not delivered" in msg)


def test_id_diagnostics_preserve_episode_and_show_namespaces() -> None:
    from providers.scrobble.plex.watch import _ids_desc

    assert _ids_desc({"plex": "42", "tmdb": "123", "imdb": "tt0000001", "tmdb_show": "456"}) == "plex:42, tmdb:123, imdb:tt0000001, tmdb_show:456"
    assert _ids_desc({}) == "none"


@pytest.mark.parametrize("cache_only", [False, True])
@pytest.mark.parametrize("change", ["item", "client", "age"])
def test_recycled_session_does_not_inherit_owner(monkeypatch: pytest.MonkeyPatch, cache_only: bool, change: str) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    plex = CountingPlex([
        _session_xml("8", user_name="owner", user_id="1"),
        _session_xml("8", user_name="shared", user_id="2"),
    ])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    first = _alert_with_rating_key("8", 101)
    first["PlaySessionStateNotification"][0]["clientIdentifier"] = "owner-client"
    service._handle_alert(first)
    if cache_only:
        service._last_event.clear()
    clock[0] += 40 * 3600 if change == "age" else 1
    second = _alert_with_rating_key("8", 202 if change == "item" else 101)
    second["PlaySessionStateNotification"][0]["clientIdentifier"] = "shared-client" if change == "client" else "owner-client"
    service._handle_alert(second)
    second["PlaySessionStateNotification"][0]["viewOffset"] = 60_000
    service._handle_alert(second)

    assert len(sink.events) == 1
    assert plex.queries == ["/status/sessions", "/status/sessions"]


@pytest.mark.parametrize("kind", ["playing", "timeline", "progress"])
def test_recycled_session_cannot_emit_owner_seek(monkeypatch: pytest.MonkeyPatch, kind: str) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    service, sink = _service(monkeypatch, _cfg(["owner"]), CountingPlex([_session_xml("8", user_name="owner")]))
    service._handle_alert(_alert_with_rating_key("8", 101))
    clock[0] += 10
    entry = dict(_alert_with_rating_key("8", 202)["PlaySessionStateNotification"][0])
    entry.update(account="shared", viewOffset=300_000)
    field = {"playing": "PlaySessionStateNotification", "timeline": "TimelineEntry", "progress": "ProgressNotification"}[kind]
    service._handle_alert({"type": kind, field: [entry]})
    clock[0] += 10
    entry = {**entry, "viewOffset": 600_000}
    service._handle_alert({"type": kind, field: [entry]})

    assert len(sink.events) == 1
    assert not any(event.raw.get("_cw_seek") for event in sink.events)


@pytest.mark.parametrize("whitelist", [["owner"], ["id:12345"], ["AccountName"]])
@pytest.mark.parametrize("missing_client", [False, True])
def test_owner_identity_survives_pause_stop_and_late_tick(monkeypatch: pytest.MonkeyPatch, whitelist: list[str], missing_client: bool) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    row = '<MediaContainer><Video sessionKey="8"><User id="12345" title="owner" username="AccountName" /></Video></MediaContainer>'
    plex = CountingPlex([row])
    service, sink = _service(monkeypatch, _cfg(whitelist), plex)
    first = _alert_with_rating_key("8", 101)
    first["PlaySessionStateNotification"][0]["clientIdentifier"] = "client"
    service._handle_alert(first)
    for state, delay, offset in [("paused", 10, 500_000), ("stopped", 3600, 950_000), ("playing", 1, 960_000)]:
        clock[0] += delay
        alert = _alert_with_rating_key("8", 101)
        alert["PlaySessionStateNotification"][0].update(state=state, viewOffset=offset)
        if not missing_client:
            alert["PlaySessionStateNotification"][0]["clientIdentifier"] = "client"
        service._handle_alert(alert)

    assert any(event.action == "stop" and event.account == "owner" for event in sink.events)
    assert sink.events[-1].account == "owner"
    assert sink.events[-1].raw["_cw_session_identity"]["user_id"] == "12345"
    assert plex.queries == ["/status/sessions", "/status/sessions"]
    assert service._last_event["8"].account == "owner"


def test_recycled_session_resets_previous_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    plex = CountingPlex([_session_xml("8", user_name="owner"), _session_xml("8", user_name="owner")])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    old = _alert_with_rating_key("8", 101)
    old["PlaySessionStateNotification"][0]["viewOffset"] = 900_000
    service._handle_alert(old)
    clock[0] += 10
    service._handle_alert(_alert_with_rating_key("8", 202))

    assert [event.progress for event in sink.events] == [90, 5]
    assert service._max_seen["8"] == 5
    assert service._first_seen["8"] == clock[0]


@pytest.mark.parametrize("response", ["<MediaContainer />", RuntimeError("connection reset")])
def test_changed_item_cannot_use_stale_identity_on_lookup_failure(monkeypatch: pytest.MonkeyPatch, response: Any) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    plex = CountingPlex([_session_xml("8", user_name="owner"), response])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    service._handle_alert(_alert_with_rating_key("8", 101))
    clock[0] += 30
    service._handle_alert(_alert_with_rating_key("8", 202))

    assert len(sink.events) == 1
    assert plex.queries == ["/status/sessions", "/status/sessions"]


@pytest.mark.parametrize("age", [1, 30])
@pytest.mark.parametrize("change", ["item", "client"])
def test_prefetched_identity_is_bound_to_its_playback(monkeypatch: pytest.MonkeyPatch, age: int, change: str) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    plex = CountingPlex([
        _sessions_xml(
            _video_xml("other", user_name="shared"),
            _video_xml("8", user_name="owner", rating_key="101", client="owner-client"),
        ),
        "<MediaContainer />",
    ])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    service._handle_alert(_alert("other"))
    clock[0] += age
    alert = _alert_with_rating_key("8", 202 if change == "item" else 101)
    alert["PlaySessionStateNotification"][0]["clientIdentifier"] = "shared-client" if change == "client" else "owner-client"
    service._handle_alert(alert)

    assert sink.events == []
    assert plex.queries == ["/status/sessions", "/status/sessions"]


@pytest.mark.parametrize("change", ["item", "client"])
def test_live_session_with_conflicting_playback_is_rejected(monkeypatch: pytest.MonkeyPatch, change: str) -> None:
    row = _session_xml("8", user_name="owner", rating_key="101", client="owner-client")
    plex = CountingPlex([row])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    alert = _alert_with_rating_key("8", 202 if change == "item" else 101)
    alert["PlaySessionStateNotification"][0]["clientIdentifier"] = "shared-client" if change == "client" else "owner-client"
    service._handle_alert(alert)

    assert sink.events == []


@pytest.mark.parametrize("kind", ["playing", "timeline", "progress"])
def test_current_playback_seek_retains_owner(monkeypatch: pytest.MonkeyPatch, kind: str) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    service, sink = _service(monkeypatch, _cfg(["owner"]), CountingPlex([_session_xml("8", user_name="owner")]))
    service._handle_alert(_alert_with_rating_key("8", 101))
    field = {"playing": "PlaySessionStateNotification", "timeline": "TimelineEntry", "progress": "ProgressNotification"}[kind]
    for delay, offset in [(1, 50_000), (10, 300_000)]:
        clock[0] += delay
        entry = dict(_alert_with_rating_key("8", 101)["PlaySessionStateNotification"][0])
        entry["viewOffset"] = offset
        service._handle_alert({"type": kind, field: [entry]})

    assert sink.events[-1].progress == 30
    assert sink.events[-1].account == "owner"
    assert sink.events[-1].raw.get("_cw_seek") is True


@pytest.mark.parametrize("offset, duration", [(12_619, 1_000_000), (5_000, 20_000)])
def test_native_plex_offsets_remain_milliseconds(monkeypatch: pytest.MonkeyPatch, offset: int, duration: int) -> None:
    service, sink = _service(monkeypatch, _cfg(["owner"]), CountingPlex([_session_xml("8", user_name="owner")]))
    alert = _alert_with_rating_key("8", 101)
    alert["PlaySessionStateNotification"][0].update(viewOffset=offset, duration=duration)

    service._handle_alert(alert)

    assert len(sink.events) == 1
    assert sink.events[0].position_ms == offset
    assert sink.events[0].duration_ms == duration
    assert sink.events[0].progress == round(offset * 100 / duration)


@pytest.mark.parametrize("delay", [30, 3600])
def test_matching_cached_identity_closes_missing_live_session(monkeypatch: pytest.MonkeyPatch, delay: int) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    plex = CountingPlex([_session_xml("8", user_name="owner", user_id="12345"), "<MediaContainer />"])
    service, sink = _service(monkeypatch, _cfg(["id:12345"]), plex)
    service._handle_alert(_alert_with_rating_key("8", 101))
    service._last_event.clear()
    clock[0] += delay
    stop = _alert_with_rating_key("8", 101)
    stop["PlaySessionStateNotification"][0].update(state="stopped", viewOffset=950_000)
    service._handle_alert(stop)

    assert [event.action for event in sink.events] == ["start", "stop"]
    assert sink.events[-1].account == "owner"
    assert plex.queries == ["/status/sessions", "/status/sessions"]


@pytest.mark.parametrize("hours", [5, 7, 24])
@pytest.mark.parametrize("stop_user", ["missing", "owner", "shared"])
def test_overnight_stop_requires_live_identity_after_expiry(monkeypatch: pytest.MonkeyPatch, hours: int, stop_user: str) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    response = "<MediaContainer />" if stop_user == "missing" else _session_xml("8", user_name=stop_user)
    plex = CountingPlex([_session_xml("8", user_name="owner"), response])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    for state, delay in [("playing", 0), ("paused", 10), ("stopped", hours * 3600)]:
        clock[0] += delay
        alert = _alert_with_rating_key("8", 101)
        alert["PlaySessionStateNotification"][0].update(state=state, clientIdentifier="clientA", viewOffset=900_000)
        service._handle_alert(alert)

    closes = hours < 6 or stop_user == "owner"
    assert [event.action for event in sink.events] == ["start", "pause"] + (["stop"] if closes else [])
    assert sink.events[-1].account == "owner"
    assert len(plex.queries) == (1 if hours < 6 else 2)


def test_itemless_stop_without_client_preserves_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    service, sink = _service(monkeypatch, _cfg(["owner"]), CountingPlex([_session_xml("8", user_name="owner")]))
    service._handle_alert(_alert_with_rating_key("8", 101))
    cleared: list[Any] = []
    monkeypatch.setattr(service, "_clear_currently_watching", cleared.append)

    service._handle_alert({"type": "playing", "PlaySessionStateNotification": [{"sessionKey": "8", "state": "stopped"}]})

    assert cleared == [sink.events[0]]
    assert service._last_event["8"] == sink.events[0]


@pytest.mark.parametrize("hours", [0.01, 3])
@pytest.mark.parametrize("previous_state", ["playing", "paused", "stopped"])
@pytest.mark.parametrize("next_state", ["playing", "paused"])
def test_same_item_and_client_do_not_pin_previous_user(monkeypatch: pytest.MonkeyPatch, hours: float, previous_state: str, next_state: str) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    plex = CountingPlex([_session_xml("8", user_name="owner"), _session_xml("8", user_name="shared")])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    first = _alert_with_rating_key("8", 101)
    first["PlaySessionStateNotification"][0]["clientIdentifier"] = "clientA"
    service._handle_alert(first)
    if previous_state != "playing":
        old = _alert_with_rating_key("8", 101)
        old["PlaySessionStateNotification"][0].update(state=previous_state, clientIdentifier="clientA")
        service._handle_alert(old)
    count = len(sink.events)
    clock[0] += hours * 3600
    new = _alert_with_rating_key("8", 101)
    new["PlaySessionStateNotification"][0].update(clientIdentifier="clientA", viewOffset=300_000, state=next_state)
    service._handle_alert(new)

    assert len(sink.events) == count
    assert plex.queries == ["/status/sessions", "/status/sessions"]


def test_flat_fingerprint_does_not_mix_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    service, _ = _service(monkeypatch, _cfg(["owner"]), CountingPlex([]))
    raw = {"TimelineEntry": [
        {"sessionKey": "8", "ratingKey": "101"},
        {"sessionKey": "9", "clientIdentifier": "clientB", "guid": "imdb://ttOTHER"},
    ]}

    assert service._playback_fingerprint(raw) == {}
    assert service._playback_fingerprint(raw, "8") == {"ratingKey": "101"}
    assert service._playback_fingerprint(raw, "9") == {"clientIdentifier": "clientB", "guid": "imdb://ttOTHER"}


def test_nonmatching_psn_does_not_discard_matching_timeline(monkeypatch: pytest.MonkeyPatch) -> None:
    service, _ = _service(monkeypatch, _cfg(["owner"]), CountingPlex([_session_xml("8", user_name="owner")]))
    service._handle_alert(_alert_with_rating_key("8", 101))
    service._handle_alert({
        "type": "timeline",
        "PlaySessionStateNotification": [{"sessionKey": "other", "ratingKey": "999"}],
        "TimelineEntry": [{"sessionKey": "8", "ratingKey": "101", "viewOffset": 300_000, "duration": 1_000_000}],
    })

    assert service._best_offset["8"][:2] == (300_000, 1_000_000)


@pytest.mark.parametrize("field", ["TimelineEntry", "ProgressNotification"])
def test_progress_item_id_can_match_verified_playback(monkeypatch: pytest.MonkeyPatch, field: str) -> None:
    service, _ = _service(monkeypatch, _cfg(["owner"]), CountingPlex([_session_xml("8", user_name="owner")]))
    service._handle_alert(_alert_with_rating_key("8", 101))
    service._handle_alert({"type": "progress", field: [{"sessionKey": "8", "itemID": "101", "viewOffset": 300_000, "duration": 1_000_000}]})

    assert service._best_offset["8"][:2] == (300_000, 1_000_000)


@pytest.mark.parametrize("kind, field", [("timeline", "TimelineEntry"), ("progress", "ProgressNotification")])
def test_same_item_progress_revalidates_user(monkeypatch: pytest.MonkeyPatch, kind: str, field: str) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    plex = CountingPlex([_session_xml("8", user_name="owner"), _session_xml("8", user_name="shared")])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    service._handle_alert(_alert_with_rating_key("8", 101))
    for delay, offset in [(1, 50_000), (35, 300_000)]:
        clock[0] += delay
        service._handle_alert({"type": kind, field: [{"sessionKey": "8", "itemID": "101", "viewOffset": offset, "duration": 1_000_000}]})

    assert len(sink.events) == 1
    assert plex.queries == ["/status/sessions", "/status/sessions"]


@pytest.mark.parametrize("response", ["<MediaContainer />", RuntimeError("connection reset")])
@pytest.mark.parametrize("gap", [30, 3 * 3600])
def test_unresolved_start_cannot_use_matching_stale_identity(monkeypatch: pytest.MonkeyPatch, response: Any, gap: int) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    plex = CountingPlex([_session_xml("8", user_name="owner"), response])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    service._handle_alert(_alert_with_rating_key("8", 101))
    clock[0] += gap
    new = _alert_with_rating_key("8", 101)
    new["PlaySessionStateNotification"][0]["viewOffset"] = 300_000
    service._handle_alert(new)

    assert len(sink.events) == 1
    assert plex.queries == ["/status/sessions", "/status/sessions"]


def test_overnight_resume_resolves_owner_before_completion(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    row = _session_xml("8", user_name="owner", user_id="12345")
    plex = CountingPlex([row, row])
    service, sink = _service(monkeypatch, _cfg(["id:12345"]), plex)
    for state, delay, offset in [("playing", 0, 500_000), ("paused", 10, 500_000), ("playing", 7 * 3600, 900_000), ("stopped", 10, 900_000)]:
        clock[0] += delay
        alert = _alert_with_rating_key("8", 101)
        alert["PlaySessionStateNotification"][0].update(state=state, viewOffset=offset, clientIdentifier="clientA")
        service._handle_alert(alert)

    assert [event.action for event in sink.events] == ["start", "pause", "start", "stop"]
    assert sink.events[-1].progress == 90
    assert sink.events[-1].account == "owner"
    assert plex.queries == ["/status/sessions", "/status/sessions"]


@pytest.mark.parametrize("cadence", [5, 10, 14, 16, 25, 60])
def test_continuous_ticks_use_one_identity_lookup(monkeypatch: pytest.MonkeyPatch, cadence: int) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    row = _session_xml("8", user_name="owner")
    plex = CountingPlex([row] * 30)
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    for index in range(21):
        alert = _alert_with_rating_key("8", 101)
        alert["PlaySessionStateNotification"][0].update(
            clientIdentifier="clientA", viewOffset=50_000 + index * cadence * 1000, duration=6_000_000,
        )
        service._handle_alert(alert)
        clock[0] += cadence

    assert sink.events
    assert {event.account for event in sink.events} == {"owner"}
    assert plex.queries == ["/status/sessions"]


@pytest.mark.parametrize("resume_response", ["<MediaContainer />", RuntimeError("connection reset")])
@pytest.mark.parametrize("pause_event", [False, True])
def test_missed_resume_lookup_keeps_stop_only_identity(monkeypatch: pytest.MonkeyPatch, resume_response: Any, pause_event: bool) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    plex = CountingPlex([_session_xml("8", user_name="owner", user_id="12345"), resume_response])
    service, sink = _service(monkeypatch, _cfg(["id:12345"]), plex)
    first = _alert_with_rating_key("8", 101)
    first["PlaySessionStateNotification"][0].update(clientIdentifier="clientA", viewOffset=900_000)
    service._handle_alert(first)
    if pause_event:
        clock[0] += 1
        paused = _alert_with_rating_key("8", 101)
        paused["PlaySessionStateNotification"][0].update(state="paused", clientIdentifier="clientA", viewOffset=900_000)
        service._handle_alert(paused)
    clock[0] += 300
    service._handle_alert(first)
    assert [event.action for event in sink.events].count("start") == 1
    clock[0] += 1
    stop = _alert_with_rating_key("8", 101)
    stop["PlaySessionStateNotification"][0].update(state="stopped", clientIdentifier="clientA", viewOffset=900_000)
    service._handle_alert(stop)

    assert sink.events[-1].action == "stop"
    assert sink.events[-1].account == "owner"
    assert sink.events[-1].progress == 90
    assert sink.events[-1].raw["_cw_session_identity"]["user_id"] == "12345"


@pytest.mark.parametrize("replacement", ["shared", "different_item"])
def test_stop_fallback_is_discarded_for_replacement_playback(monkeypatch: pytest.MonkeyPatch, replacement: str) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    plex = CountingPlex([_session_xml("8", user_name="owner"), "<MediaContainer />", _session_xml("8", user_name="shared")])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    service._handle_alert(_alert_with_rating_key("8", 101))
    clock[0] += 300
    service._handle_alert(_alert_with_rating_key("8", 101))
    clock[0] += 1
    new = _alert_with_rating_key("8", 202 if replacement == "different_item" else 101)
    service._handle_alert(new)
    new["PlaySessionStateNotification"][0]["state"] = "stopped"
    service._handle_alert(new)

    assert len(sink.events) == 1



def test_continuity_does_not_override_newly_resolved_user(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    plex = CountingPlex([
        _session_xml("8", user_name="owner", rating_key="101", client="clientA"),
        _sessions_xml(
            _video_xml("other", user_name="shared"),
            _video_xml("8", user_name="shared", rating_key="101", client="clientA"),
        ),
    ])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    first = _alert_with_rating_key("8", 101)
    first["PlaySessionStateNotification"][0]["clientIdentifier"] = "clientA"
    service._handle_alert(first)
    clock[0] += 25
    service._handle_alert(_alert("other"))
    tick = _alert_with_rating_key("8", 101)
    tick["PlaySessionStateNotification"][0].update(clientIdentifier="clientA", viewOffset=75_000)
    service._handle_alert(tick)
    tick["PlaySessionStateNotification"][0]["state"] = "stopped"
    service._handle_alert(tick)

    assert len(sink.events) == 1
    assert plex.queries == ["/status/sessions", "/status/sessions"]


def test_itemless_stop_after_resume_miss_still_cleans_up(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    service, sink = _service(monkeypatch, _cfg(["owner"]), CountingPlex([_session_xml("8", user_name="owner")]))
    service._handle_alert(_alert_with_rating_key("8", 101))
    clock[0] += 300
    service._handle_alert(_alert_with_rating_key("8", 101))
    cleared: list[Any] = []
    monkeypatch.setattr(service, "_clear_currently_watching", cleared.append)
    service._handle_alert({"type": "playing", "PlaySessionStateNotification": [{"sessionKey": "8", "state": "stopped"}]})

    assert cleared == [sink.events[0]]


@pytest.mark.parametrize("resume_user", ["owner", "shared"])
def test_repeated_paused_notifications_reuse_identity_until_resume(monkeypatch: pytest.MonkeyPatch, resume_user: str) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    row = _session_xml("8", user_name="owner")
    plex = CountingPlex([row] * 20)
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    service._handle_alert(_alert_with_rating_key("8", 101))
    for index in range(15):
        clock[0] += 20 if index == 0 else 10
        paused = _alert_with_rating_key("8", 101)
        paused["PlaySessionStateNotification"][0].update(state="paused", viewOffset=50_000)
        service._handle_alert(paused)

    assert plex.queries == ["/status/sessions", "/status/sessions"]
    assert [event.action for event in sink.events] == ["start", "pause"]
    plex._sessions = [_session_xml("8", user_name=resume_user)]
    clock[0] += 1
    resumed = _alert_with_rating_key("8", 101)
    resumed["PlaySessionStateNotification"][0]["viewOffset"] = 60_000
    service._handle_alert(resumed)

    assert len(plex.queries) == 3
    assert len(sink.events) == (3 if resume_user == "owner" else 2)


@pytest.mark.parametrize("age", [3600, 6 * 3600 - 1, 6 * 3600, 7 * 3600, 40 * 3600])
@pytest.mark.parametrize("missed_resume", [False, True])
def test_stop_identity_expires_from_last_verified_playback(monkeypatch: pytest.MonkeyPatch, age: int, missed_resume: bool) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    plex = CountingPlex([_session_xml("8", user_name="owner")])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    alert = _alert_with_rating_key("8", 101)
    alert["PlaySessionStateNotification"][0].update(clientIdentifier="clientA", viewOffset=900_000)
    service._handle_alert(alert)
    if missed_resume:
        clock[0] = 1300.0
        service._handle_alert(alert)
    clock[0] = 1000.0 + age
    alert["PlaySessionStateNotification"][0]["state"] = "stopped"
    service._handle_alert(alert)

    expected = ["start", "stop"] if age < watch.SESSION_CONTINUITY_SECONDS else ["start"]
    assert [event.action for event in sink.events] == expected
    if age >= watch.SESSION_CONTINUITY_SECONDS:
        assert "8" not in service._stop_fallback


@pytest.mark.parametrize("gap", [1, 10, 60])
@pytest.mark.parametrize("next_user", ["owner", "shared", "missing"])
@pytest.mark.parametrize("kind", ["playing", "timeline", "progress"])
def test_backwards_offset_requires_fresh_identity(monkeypatch: pytest.MonkeyPatch, gap: int, next_user: str, kind: str) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    response = "<MediaContainer />" if next_user == "missing" else _session_xml("8", user_name=next_user)
    plex = CountingPlex([_session_xml("8", user_name="owner"), response])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    alert = _alert_with_rating_key("8", 101)
    alert["PlaySessionStateNotification"][0]["clientIdentifier"] = "clientA"
    service._handle_alert(alert)
    clock[0] += gap
    entry = dict(alert["PlaySessionStateNotification"][0], viewOffset=5_000)
    field = {"playing": "PlaySessionStateNotification", "timeline": "TimelineEntry", "progress": "ProgressNotification"}[kind]
    service._handle_alert({"type": kind, field: [entry]})

    assert plex.queries == ["/status/sessions", "/status/sessions"]
    if next_user != "owner":
        assert len(sink.events) == 1
        clock[0] += 1
        entry["state"] = "stopped"
        service._handle_alert({"type": "playing", "PlaySessionStateNotification": [entry]})
        assert len(sink.events) == 1
    elif kind == "playing":
        assert len(sink.events) == 2
        assert sink.events[-1].account == "owner"


@pytest.mark.parametrize("idle_before_resume", [300, 7 * 3600])
def test_itemless_stop_cannot_use_expired_fallback(monkeypatch: pytest.MonkeyPatch, idle_before_resume: int) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    service, _ = _service(monkeypatch, _cfg(["owner"]), CountingPlex([_session_xml("8", user_name="owner")]))
    alert = _alert_with_rating_key("8", 101)
    service._handle_alert(alert)
    clock[0] += idle_before_resume
    service._handle_alert(alert)
    clock[0] = 1000.0 + 7 * 3600 + 1
    cleared: list[Any] = []
    monkeypatch.setattr(service, "_clear_currently_watching", cleared.append)
    service._handle_alert({"type": "playing", "PlaySessionStateNotification": [{"sessionKey": "8", "state": "stopped"}]})

    assert cleared == []
    assert "8" not in service._stop_fallback


@pytest.mark.parametrize("kind", ["playing", "timeline", "progress"])
@pytest.mark.parametrize("recovery_at", ["next_tick", "stop"])
@pytest.mark.parametrize("resolved_user", ["owner", "shared", "missing"])
def test_rewind_lookup_miss_retries_identity_on_subsequent_playback(
    monkeypatch: pytest.MonkeyPatch, kind: str, recovery_at: str, resolved_user: str,
) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    response = "<MediaContainer />" if resolved_user == "missing" else _session_xml("8", user_name=resolved_user)
    plex = CountingPlex([_session_xml("8", user_name="owner"), "<MediaContainer />", response])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    alert = _alert_with_rating_key("8", 101)
    entry = alert["PlaySessionStateNotification"][0]
    entry.update(clientIdentifier="clientA", viewOffset=500_000)
    service._handle_alert(alert)
    clock[0] += 25
    entry["viewOffset"] = 120_000
    field = {"playing": "PlaySessionStateNotification", "timeline": "TimelineEntry", "progress": "ProgressNotification"}[kind]
    service._handle_alert({"type": kind, field: [dict(entry)]})
    assert [event.action for event in sink.events] == ["start"]
    assert len(plex.queries) == 2
    if recovery_at == "next_tick":
        clock[0] += 25
        entry["viewOffset"] = 145_000
        service._handle_alert(alert)
    clock[0] += 1
    entry.update(state="stopped", viewOffset=146_000)
    service._handle_alert(alert)

    expected = ["start"]
    if resolved_user == "owner":
        expected += ["start", "stop"] if recovery_at == "next_tick" else ["stop"]
    assert [event.action for event in sink.events] == expected
    assert {event.account for event in sink.events} == {"owner"}
    assert len(plex.queries) == (4 if recovery_at == "next_tick" and resolved_user == "missing" else 3)


@pytest.mark.parametrize("rewind_kind", ["playing", "timeline", "progress"])
@pytest.mark.parametrize("recovery_kind", ["timeline", "progress"])
@pytest.mark.parametrize("resolved_user", ["owner", "shared", "missing"])
def test_rewind_miss_recovers_from_progress_before_session_disappears(
    monkeypatch: pytest.MonkeyPatch, rewind_kind: str, recovery_kind: str, resolved_user: str,
) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    plex = CountingPlex([_session_xml("8", user_name="owner"), "<MediaContainer />"])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    alert = _alert_with_rating_key("8", 101)
    entry = alert["PlaySessionStateNotification"][0]
    entry.update(clientIdentifier="clientA", viewOffset=500_000)
    service._handle_alert(alert)
    fields = {"playing": "PlaySessionStateNotification", "timeline": "TimelineEntry", "progress": "ProgressNotification"}
    clock[0] += 25
    entry["viewOffset"] = 120_000
    service._handle_alert({"type": rewind_kind, fields[rewind_kind]: [dict(entry)]})
    assert len(sink.events) == 1
    assert len(plex.queries) == 2
    plex._sessions = [] if resolved_user == "missing" else [_session_xml("8", user_name=resolved_user)]
    clock[0] += 25
    entry["viewOffset"] = 145_000
    service._handle_alert({"type": recovery_kind, fields[recovery_kind]: [dict(entry)]})
    assert len(plex.queries) == 3
    plex._sessions = []
    clock[0] += 1
    entry.update(state="stopped", viewOffset=950_000)
    service._handle_alert(alert)

    assert [event.action for event in sink.events] == (["start", "start", "stop"] if resolved_user == "owner" else ["start"])
    if resolved_user == "owner":
        assert sink.events[-2].raw.get("_cw_seek") is True
        assert sink.events[-1].account == "owner"
        assert sink.events[-1].progress == 95


@pytest.mark.parametrize("change", ["item", "client", "expiry"])
def test_pending_rewind_recovery_rejects_changed_or_expired_context(monkeypatch: pytest.MonkeyPatch, change: str) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    plex = CountingPlex([_session_xml("8", user_name="owner"), "<MediaContainer />"])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    alert = _alert_with_rating_key("8", 101)
    entry = alert["PlaySessionStateNotification"][0]
    entry.update(clientIdentifier="clientA", viewOffset=500_000)
    service._handle_alert(alert)
    clock[0] += 25
    entry["viewOffset"] = 120_000
    service._handle_alert(alert)
    assert "8" in service._identity_recovery
    clock[0] = 1000 + watch.SESSION_CONTINUITY_SECONDS if change == "expiry" else clock[0] + 25
    entry["viewOffset"] = 145_000
    if change == "item":
        entry["ratingKey"] = 202
    elif change == "client":
        entry["clientIdentifier"] = "clientB"
    service._handle_alert({"type": "progress", "ProgressNotification": [dict(entry)]})

    assert len(plex.queries) == 2
    assert "8" not in service._identity_recovery
    assert len(sink.events) == 1
    entry["state"] = "stopped"
    service._handle_alert(alert)
    assert len(sink.events) == 1


def test_progress_recovery_retries_misses_then_reuses_verified_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex import watch

    clock = [1000.0]
    monkeypatch.setattr(watch.time, "time", lambda: clock[0])
    row = _session_xml("8", user_name="owner")
    plex = CountingPlex([row, "<MediaContainer />", "<MediaContainer />", row])
    service, sink = _service(monkeypatch, _cfg(["owner"]), plex)
    alert = _alert_with_rating_key("8", 101)
    entry = alert["PlaySessionStateNotification"][0]
    entry.update(clientIdentifier="clientA", viewOffset=500_000)
    service._handle_alert(alert)
    clock[0] += 25
    entry["viewOffset"] = 120_000
    service._handle_alert(alert)
    for offset in (145_000, 170_000, 195_000, 220_000):
        clock[0] += 25
        entry["viewOffset"] = offset
        service._handle_alert({"type": "timeline", "TimelineEntry": [dict(entry)]})

    assert len(plex.queries) == 4
    assert "8" not in service._identity_recovery
    clock[0] += 1
    entry.update(state="stopped", viewOffset=221_000)
    service._handle_alert(alert)
    assert [event.action for event in sink.events] == ["start", "stop"]
    assert sink.events[-1].account == "owner"
