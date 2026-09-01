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


def test_route_fallback_and_send_logs_name_the_route(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _route_dispatcher_cfg("R2", "mdblist", None, True)

    messages = _dispatch_and_capture(monkeypatch, cfg, {"_cw_sessions_access_unavailable": True})

    assert messages == [
        "route R2 plex->mdblist: unresolved user fallback used user=ow*** sess=30",
        "route R2 plex->mdblist: sent start user=ow*** sess=30",
    ]


def _identity_logs(monkeypatch: pytest.MonkeyPatch, service: Any) -> list[str]:
    lines: list[str] = []
    monkeypatch.setattr(service, "_dbg", lambda msg: lines.append(str(msg)))
    return lines


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


def test_identity_miss_log_is_throttled_per_session(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(["Carmen"])
    plex = FakePlex(["<MediaContainer />"] * 12)
    service, _sink = _service(monkeypatch, cfg, plex)
    lines = _identity_logs(monkeypatch, service)

    service._handle_alert(_alert("31"))
    service._handle_alert(_alert("31"))

    assert len([ln for ln in lines if ln.startswith("identity unresolved")]) == 1


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
        _session_xml("31", user_name="Carmen", user_id="176467484"),
        "<MediaContainer />",
    ])
    service, _sink = _service(monkeypatch, cfg, plex)
    lines = _identity_logs(monkeypatch, service)

    service._handle_alert(_alert("31"))
    service._handle_alert(_alert("31"))
    assert service._identity_miss.get("31") is None

    service._sess_identity_cache.clear()
    service._last_event.clear()
    service._handle_alert(_alert("31"))
    assert [ln for ln in lines if ln.startswith("identity unresolved")] == []


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
