# tests/test_watcher_filter_logging.py
# CrossWatch - Watcher filter logging regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import importlib
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from providers.scrobble import _log_dedupe
from providers.scrobble._log_dedupe import LogDeduplicator
from providers.scrobble.scrobble import ScrobbleEvent


class FakeDispatcher:
    def __init__(self) -> None:
        self.accepted = False
        self.events: list[ScrobbleEvent] = []

    def dispatch(self, event: ScrobbleEvent) -> bool:
        self.events.append(event)
        return self.accepted


@pytest.fixture(params=["emby", "jellyfin", "kodi"])
def watcher(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> Any:
    provider = request.param
    module = importlib.import_module(f"providers.scrobble.{provider}.watch")
    monkeypatch.setattr(module, "_cw_update", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "_cw_update_payload", lambda *args, **kwargs: None)
    if provider != "kodi":
        monkeypatch.setattr(module, "_server_id", lambda *args, **kwargs: "server-1")
    cls = getattr(module, f"{provider.capitalize()}WatchService")
    dispatcher = FakeDispatcher()
    service = cls(dispatcher=dispatcher, cfg_provider=lambda: {}, quiet_startup=True)
    messages: list[str] = []
    if provider == "kodi":
        monkeypatch.setattr(module, "_log", lambda msg, level="INFO": messages.append(msg))
    else:
        monkeypatch.setattr(service, "_passes_filters", lambda *args: True)
        monkeypatch.setattr(service, "_dbg", messages.append)
        monkeypatch.setattr(service, "_log", lambda msg, level="INFO": messages.append(msg))

    def emit(event: ScrobbleEvent) -> None:
        if provider == "kodi":
            service._dispatch_event(event)
        else:
            service._emit(event, {})

    clock = [1000.0]
    monkeypatch.setattr(_log_dedupe.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(module.time, "time", lambda: clock[0])
    event = ScrobbleEvent(
        action="start", media_type="movie", ids={"imdb": "tt123"}, title="Movie",
        year=2020, season=None, number=None, progress=5.0, account="Robert",
        server_uuid="server-1", session_key="session-1", raw={},
    )
    return SimpleNamespace(
        provider=provider, service=service, dispatcher=dispatcher, messages=messages,
        emit=emit, clock=clock, event=event,
    )


def test_unchanged_rejected_playback_stays_quiet(watcher: Any) -> None:
    for _ in range(240):
        watcher.emit(watcher.event)
        watcher.clock[0] += 30.0

    assert len(watcher.dispatcher.events) == 240
    if watcher.provider == "kodi":
        assert len([message for message in watcher.messages if message.startswith("incoming")]) == 1
        assert len([message for message in watcher.messages if message.startswith("ids resolved")]) == 1
    assert [message for message in watcher.messages if "not dispatched" in message] == [
        "event not dispatched: user=Ro*** server=server-1 sess=session-1"
    ]


def test_rejection_logs_distinguish_users_servers_and_sessions(watcher: Any) -> None:
    for event in (
        watcher.event,
        replace(watcher.event, account="Roger"),
        watcher.event,
        replace(watcher.event, server_uuid="server-2"),
        replace(watcher.event, session_key="session-2"),
    ):
        watcher.emit(event)
        watcher.emit(event)

    assert len([message for message in watcher.messages if "not dispatched" in message]) == 5


def test_acceptance_allows_a_later_rejection_to_be_logged(watcher: Any) -> None:
    watcher.emit(watcher.event)
    watcher.dispatcher.accepted = True
    watcher.emit(watcher.event)
    watcher.dispatcher.accepted = False
    watcher.emit(replace(watcher.event, progress=6.0))

    rejected = [message for message in watcher.messages if "not dispatched" in message]
    assert len(rejected) == 2
    assert any(message.startswith("incoming") for message in watcher.messages)
    assert any(message.startswith("ids resolved") for message in watcher.messages)


def test_rejection_is_logged_again_after_session_inactivity(watcher: Any) -> None:
    watcher.emit(watcher.event)
    watcher.clock[0] += 1800.0
    watcher.emit(watcher.event)
    assert len([message for message in watcher.messages if "not dispatched" in message]) == 2


def test_seek_and_regular_events_share_rejection_log_state(watcher: Any) -> None:
    if watcher.provider == "kodi":
        watcher.emit(watcher.event)
        watcher.emit(replace(watcher.event, raw={"_cw_seek": True}))
    else:
        watcher.service._throttled_route_filtered_log(watcher.event, "seek update")
        watcher.clock[0] += 60.0
        watcher.emit(watcher.event)
    assert len([message for message in watcher.messages if "not dispatched" in message]) == 1


@pytest.mark.parametrize("watcher", ["emby", "jellyfin"], indirect=True)
def test_accepted_seek_resets_rejection_log_state(watcher: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    service = watcher.service
    monkeypatch.setattr(service, "_build_event", lambda *args: watcher.event)
    session = {"Id": watcher.event.session_key}
    watcher.emit(watcher.event)

    watcher.dispatcher.accepted = True
    assert service._emit_seek_update(session, 20, {}, "poll")
    accepted_seek = watcher.dispatcher.events[-1]
    assert accepted_seek.raw["_cw_seek"] is True
    assert accepted_seek.progress == 20

    watcher.dispatcher.accepted = False
    watcher.clock[0] += 2.0
    assert not service._emit_seek_update(session, 30, {}, "poll")
    watcher.clock[0] += 2.0
    assert not service._emit_seek_update(session, 30, {}, "poll")
    watcher.emit(replace(watcher.event, progress=31.0))

    assert len(watcher.dispatcher.events) == 5
    assert [message for message in watcher.messages if "not dispatched" in message] == [
        "event not dispatched: user=Ro*** server=server-1 sess=session-1",
        "seek update not dispatched: user=Ro*** server=server-1 sess=session-1",
    ]


def test_log_cache_evicts_oldest_sessions_and_retains_active_sessions() -> None:
    cache = LogDeduplicator(max_entries=3)
    assert cache.should_log("active", "user")
    assert cache.should_log("oldest", "user")
    assert cache.should_log("other", "user")
    assert not cache.should_log("active", "user")
    assert cache.should_log("new", "user")
    assert not cache.should_log("active", "user")
    assert cache.should_log("oldest", "user")


def test_log_cache_is_isolated_between_watchers() -> None:
    first = LogDeduplicator()
    second = LogDeduplicator()
    assert first.should_log("session", "user")
    assert second.should_log("session", "user")
    assert not first.should_log("session", "user")


def test_kodi_retains_id_diagnostics_when_delivery_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble import scrobble
    from providers.scrobble.kodi import watch

    cfg = {"scrobble": {"watch": {"route_provider": "kodi", "route_sink": "trakt", "route_id": "R1"}}}
    messages: list[str] = []
    calls: list[ScrobbleEvent] = []
    logs_at_send: list[list[str]] = []

    class FailingSink:
        def send(self, ev: ScrobbleEvent) -> dict[str, Any]:
            calls.append(ev)
            logs_at_send.append(list(messages))
            return {"ok": False, "error": "not found", "retryable": True}

    monkeypatch.setattr(watch, "_log", lambda msg, level="INFO": messages.append(msg))
    monkeypatch.setattr(scrobble, "_log", lambda msg, lvl="INFO": messages.append(msg))
    service = watch.KodiWatchService(
        dispatcher=scrobble.Dispatcher([FailingSink()], cfg_provider=lambda: cfg),
        cfg_provider=lambda: cfg, quiet_startup=True,
    )
    ev = ScrobbleEvent("start", "movie", {"imdb": "tt1"}, "Movie", 2020, None, None, 5, "user", "server", "session", {})
    assert not service._dispatch_event(ev)
    assert not service._dispatch_event(ev)
    assert any("reason=not found" in message for message in messages)
    assert len(calls) == 1
    assert any("ids resolved:" in message and "imdb:tt1" in message for message in logs_at_send[0])
    assert len([message for message in messages if message.startswith("incoming")]) == 1
    assert len([message for message in messages if message.startswith("ids resolved")]) == 1
    assert not any("filtered by route dispatcher" in message for message in messages)

    assert not service._dispatch_event(replace(ev, ids={"imdb": "tt2"}))
    assert len([message for message in messages if message.startswith("ids resolved")]) == 2
    assert any("imdb:tt2" in message for message in messages)
