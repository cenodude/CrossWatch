from __future__ import annotations

from typing import Any

from providers.scrobble.emby import watch as emby_watch
from providers.scrobble.jellyfin import watch as jellyfin_watch
from providers.scrobble.plex import watch as plex_watch


class FakeDispatcher:
    def __init__(self) -> None:
        self.events: list[Any] = []

    def dispatch(self, event: Any) -> bool:
        self.events.append(event)
        return True


def _cfg(provider: str) -> dict[str, Any]:
    return {
        provider: {
            "server": "http://media.local",
            "access_token": "token",
            "timeout": 12,
        },
        "runtime": {"debug": True},
    }


def test_emby_offline_backoff_is_quiet_and_does_not_stop(monkeypatch):
    monkeypatch.setattr(emby_watch, "_server_id", lambda *a, **k: "server")
    service = emby_watch.EmbyWatchService(dispatcher=FakeDispatcher(), cfg_provider=lambda: _cfg("emby"), quiet_startup=True)
    logs: list[tuple[str, str]] = []
    service._log = lambda msg, level="INFO": logs.append((level, msg))  # type: ignore[method-assign]
    service._last["session-1"] = {"p": 33, "ts": 1.0, "meta": {"title": "Movie", "media_type": "movie"}}

    timeouts: list[float | None] = []

    def fail(*args: Any, **kwargs: Any) -> Any:
        timeouts.append(kwargs.get("timeout"))
        raise TimeoutError("timeout")

    monkeypatch.setattr(emby_watch, "_get_json", fail)

    assert service._tick() is False
    assert service._tick() is False

    assert service._offline is True
    assert service._offline_retry == 60.0
    assert timeouts == [12.0, 2.0]
    assert "session-1" in service._last
    assert logs == [("WARNING", "Emby watcher offline: timeout; retrying with backoff")]


def test_jellyfin_offline_backoff_is_quiet_and_recovers(monkeypatch):
    monkeypatch.setattr(jellyfin_watch, "_server_id", lambda *a, **k: "server")
    service = jellyfin_watch.JellyfinWatchService(dispatcher=FakeDispatcher(), cfg_provider=lambda: _cfg("jellyfin"), quiet_startup=True)
    logs: list[tuple[str, str]] = []
    service._log = lambda msg, level="INFO": logs.append((level, msg))  # type: ignore[method-assign]

    calls = 0

    def flaky(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("timeout")
        return []

    monkeypatch.setattr(jellyfin_watch, "_get_json", flaky)

    assert service._tick() is False
    assert service._offline is True
    assert service._tick() is False
    assert service._offline is False
    assert service._offline_retry == 30.0
    assert logs == [
        ("WARNING", "Jellyfin watcher offline: timeout; retrying with backoff"),
        ("INFO", "Jellyfin watcher reconnected"),
    ]


def test_plex_offline_backoff_is_quiet_and_recovers():
    service = plex_watch.WatchService(dispatcher=FakeDispatcher(), cfg_provider=lambda: {}, quiet_startup=True)
    logs: list[tuple[str, str]] = []
    service._log = lambda msg, level="INFO": logs.append((level, msg))  # type: ignore[method-assign]

    service._mark_offline(RuntimeError("down"))
    service._mark_offline(RuntimeError("down"))
    service._mark_online()

    assert service._offline is False
    assert service._offline_retry == 30.0
    assert logs == [
        ("WARNING", "Plex watcher offline: down; retrying with backoff"),
        ("INFO", "Plex watcher reconnected"),
    ]


def test_plex_listener_error_ignored_while_stopping():
    service = plex_watch.WatchService(dispatcher=FakeDispatcher(), cfg_provider=lambda: {}, quiet_startup=True)
    logs: list[tuple[str, str]] = []
    service._log = lambda msg, level="INFO": logs.append((level, msg))  # type: ignore[method-assign]

    service._stop.set()
    service._listener_error(AttributeError("'NoneType' object has no attribute 'sock'"))
    assert service._offline is False
    assert logs == []

    service._stop.clear()
    service._listener_error("closed")
    assert service._offline is True
    assert logs == [("WARNING", "Plex watcher offline: closed; retrying with backoff")]


def _poll_cfg(provider: str, poll: Any) -> dict[str, Any]:
    cfg = _cfg(provider)
    cfg["scrobble"] = {"watch": {"poll_seconds": poll}}
    return cfg


def test_emby_poll_interval_follows_config(monkeypatch):
    monkeypatch.setattr(emby_watch, "_server_id", lambda *a, **k: "server")
    state: dict[str, Any] = {"poll": 60}
    service = emby_watch.EmbyWatchService(dispatcher=FakeDispatcher(), cfg_provider=lambda: _poll_cfg("emby", state["poll"]), quiet_startup=True)
    paths: list[str] = []

    def sessions(base: str, tok: str, path: str, **kwargs: Any) -> Any:
        paths.append(path)
        return []

    monkeypatch.setattr(emby_watch, "_get_json", sessions)

    service._tick()
    assert service._poll == 60.0
    state["poll"] = None
    service._tick()
    assert service._poll == 10.0
    state["poll"] = 2
    service._tick()
    assert service._poll == 10.0
    assert paths == ["/Sessions?ActiveWithinSeconds=120", "/Sessions?ActiveWithinSeconds=20", "/Sessions?ActiveWithinSeconds=20"]


def test_jellyfin_poll_interval_follows_config(monkeypatch):
    monkeypatch.setattr(jellyfin_watch, "_server_id", lambda *a, **k: "server")
    service = jellyfin_watch.JellyfinWatchService(dispatcher=FakeDispatcher(), cfg_provider=lambda: _poll_cfg("jellyfin", 5), quiet_startup=True)
    paths: list[str] = []

    def sessions(base: str, tok: str, path: str, *args: Any, **kwargs: Any) -> Any:
        paths.append(path)
        return []

    monkeypatch.setattr(jellyfin_watch, "_get_json", sessions)

    service._tick()
    assert service._poll == 5.0
    assert paths == ["/Sessions?ActiveWithinSeconds=15"]


def test_group_poll_seconds_uses_lowest_enabled_route():
    from providers.scrobble.routes import group_poll_seconds, normalize_route

    def route(rid: str, provider: str, poll: Any, enabled: bool = True) -> dict[str, Any]:
        return {"id": rid, "enabled": enabled, "provider": provider, "sink": "trakt", "options": {"watch": {"poll_seconds": poll}}}

    cfg = {"scrobble": {"watch": {"routes": [
        route("R1", "jellyfin", 60),
        route("R2", "jellyfin", 20),
        route("R3", "jellyfin", 5, enabled=False),
        route("R4", "jellyfin", 2),
        route("R5", "emby", 400),
        route("R6", "plex", 30),
    ]}}}

    assert group_poll_seconds(cfg, "jellyfin", "default") == 20
    assert group_poll_seconds(cfg, "emby", "default") is None
    assert group_poll_seconds(cfg, "plex", "default") is None
    assert "poll_seconds" not in normalize_route(route("R6", "plex", 30), "R6")["options"]["watch"]
    assert normalize_route(route("R1", "kodi", 60), "R1")["options"]["watch"]["poll_seconds"] == 60


def test_idle_poll_interval_follows_config(monkeypatch):
    from providers.scrobble.routes import group_poll_seconds, normalize_route

    monkeypatch.setattr(emby_watch, "_server_id", lambda *a, **k: "server")
    cfg = _poll_cfg("emby", 5)
    cfg["scrobble"]["watch"]["idle_poll_seconds"] = 120
    service = emby_watch.EmbyWatchService(dispatcher=FakeDispatcher(), cfg_provider=lambda: cfg, quiet_startup=True)
    monkeypatch.setattr(emby_watch, "_get_json", lambda *a, **k: [])

    assert service._max_idle_sleep == 30.0
    service._tick()
    assert (service._poll, service._max_idle_sleep) == (5.0, 120.0)

    routes = {"scrobble": {"watch": {"routes": [
        {"id": "R1", "provider": "emby", "sink": "trakt", "options": {"watch": {"idle_poll_seconds": 90}}},
        {"id": "R2", "provider": "emby", "sink": "simkl", "options": {"watch": {"idle_poll_seconds": 45, "poll_seconds": 10}}},
    ]}}}
    assert group_poll_seconds(routes, "emby", "default", "idle_poll_seconds") == 45
    assert group_poll_seconds(routes, "emby", "default") == 10
    assert "idle_poll_seconds" not in normalize_route({"provider": "plex", "sink": "trakt", "options": {"watch": {"idle_poll_seconds": 45}}}, "R1")["options"]["watch"]


def _session(item_id: str, name: str, episode: int, position_ticks: int, paused: bool = False) -> dict[str, Any]:
    return {
        "Id": "session-1",
        "UserName": "user",
        "NowPlayingItem": {
            "Id": item_id,
            "Type": "Episode",
            "Name": name,
            "SeriesName": "Show",
            "ParentIndexNumber": 1,
            "IndexNumber": episode,
            "RunTimeTicks": 12_000_000_000,
            "ProviderIds": {"Tvdb": str(1000 + episode)},
        },
        "PlayState": {"PositionTicks": position_ticks, "IsPaused": paused},
    }


def _item_change_events(monkeypatch, module: Any, service_cls: Any, provider: str) -> list[tuple[str, Any, int]]:
    monkeypatch.setattr(module, "_server_id", lambda *a, **k: "server")
    monkeypatch.setattr(module, "_cw_update", lambda *a, **k: None)
    monkeypatch.setattr(module, "show_tmdb_id", lambda *a, **k: None)
    dispatcher = FakeDispatcher()
    dispatcher.accepts = lambda event: True  # type: ignore[attr-defined]
    service = service_cls(dispatcher=dispatcher, cfg_provider=lambda: _cfg(provider), quiet_startup=True)
    service._passes_filters = lambda ev, cfg: True  # type: ignore[method-assign]
    polls = [
        [_session("ep1", "One", 1, 6_000_000_000)],
        [_session("ep1", "One", 1, 11_760_000_000)],
        [_session("ep2", "Two", 2, 120_000_000)],
    ]

    def sessions(base: str, tok: str, path: str, *args: Any, **kwargs: Any) -> Any:
        return polls.pop(0) if path.startswith("/Sessions") else {}

    monkeypatch.setattr(module, "_get_json", sessions)
    for _ in range(3):
        service._tick()
    return [(e.action, e.number, int(e.progress)) for e in dispatcher.events]


def test_jellyfin_item_change_on_same_session_closes_previous_item(monkeypatch):
    events = _item_change_events(monkeypatch, jellyfin_watch, jellyfin_watch.JellyfinWatchService, "jellyfin")
    assert events == [("start", 1, 50), ("start", 1, 98), ("stop", 1, 98), ("start", 2, 1)]


def test_emby_item_change_on_same_session_closes_previous_item(monkeypatch):
    events = _item_change_events(monkeypatch, emby_watch, emby_watch.EmbyWatchService, "emby")
    assert events == [("start", 1, 50), ("start", 1, 98), ("stop", 1, 98), ("start", 2, 1)]
