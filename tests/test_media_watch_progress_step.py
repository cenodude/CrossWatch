# tests/test_media_watch_progress_step.py
# CrossWatch - Emby and Jellyfin watchers send playing updates only per progress step.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import pytest

from providers.scrobble.emby import watch as emby_watch
from providers.scrobble.jellyfin import watch as jellyfin_watch
from providers.scrobble.scrobble import ScrobbleEvent

RUNTIME = 60_000_000_000


@pytest.fixture(autouse=True)
def _isolated_registry(config_base):
    return config_base


class FakeDispatcher:
    def __init__(self) -> None:
        self.events: list[ScrobbleEvent] = []

    def accepts(self, ev: ScrobbleEvent) -> bool:
        return True

    def accepts_user(self, ev: ScrobbleEvent) -> bool:
        return True

    def wants_manual_marks(self) -> bool:
        return False

    def dispatch(self, ev: ScrobbleEvent) -> bool:
        self.events.append(ev)
        return True


def _session(pct: int, paused: bool = False) -> dict[str, Any]:
    return {
        "Id": "sess-1",
        "UserId": "u1",
        "UserName": "anna",
        "NowPlayingItem": {"Id": "item-1", "Type": "Movie", "Name": "Heat", "ProductionYear": 1995,
                           "ProviderIds": {"Imdb": "tt0113277", "Tmdb": "949"}, "RunTimeTicks": RUNTIME},
        "PlayState": {"PositionTicks": RUNTIME * pct // 100, "IsPaused": paused},
    }


@pytest.mark.parametrize("module,cls,name", [(emby_watch, "EmbyWatchService", "emby"), (jellyfin_watch, "JellyfinWatchService", "jellyfin")])
@pytest.mark.parametrize("configured,expected", [(15, [10, 26, 42]), (1, [10, 26, 42]), (50, [10])])
def test_playing_updates_follow_the_progress_step(monkeypatch, module, cls, name, configured, expected) -> None:
    monkeypatch.setattr(module, "_server_id", lambda *a, **k: "server")
    monkeypatch.setattr(module, "_cw_update", lambda *a, **k: None)
    monkeypatch.setattr(module, "_cw_update_payload", lambda *a, **k: None)
    cfg = {name: {"server": "http://media.local", "access_token": "token"},
           "scrobble": {"watch": {}, "trakt": {"progress_step": configured}}}
    dispatcher = FakeDispatcher()
    service = getattr(module, cls)(dispatcher=dispatcher, cfg_provider=lambda: cfg, quiet_startup=True)
    service._log = lambda msg, level="INFO": None
    monkeypatch.setattr(service, "_is_seek_jump", lambda *a, **k: (False, 0, 0.0))
    monkeypatch.setattr(service, "_note_mark_session", lambda *a, **k: None, raising=False)
    sessions: list[dict[str, Any]] = []
    monkeypatch.setattr(service, "_current_sessions", lambda _cfg: list(sessions))
    for pct in (10, 20, 26, 35, 42, 50):
        sessions[:] = [_session(pct)]
        service._tick()
    assert [(e.action, round(e.progress)) for e in dispatcher.events] == [("start", p) for p in expected]
