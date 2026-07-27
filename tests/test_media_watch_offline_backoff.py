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
