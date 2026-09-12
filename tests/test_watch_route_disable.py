from dataclasses import replace
from typing import Any

import pytest

from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent
from providers.scrobble import watch_manager


@pytest.mark.parametrize("change", ["disable", "delete", "source", "global"])
@pytest.mark.parametrize("delivered", [False, True])
def test_existing_dispatcher_stops_sending_after_route_disabled(monkeypatch, change, delivered):
    route = {"id": "R1", "enabled": True, "provider": "plex", "sink": "mdblist"}
    cfg = {"scrobble": {"enabled": True, "sources": {"watcher": True}, "watch": {"routes": [route]}}}
    monkeypatch.setattr(watch_manager, "load_config", lambda: cfg)
    calls = []

    class Sink:
        def send(self, event, cfg=None) -> Any:
            calls.append(event)
            return {"ok": delivered}

    dispatcher = Dispatcher([Sink()], cfg_provider=watch_manager._route_cfg_provider("R1"))
    event = ScrobbleEvent("start", "movie", {"tmdb": "10"}, "Movie", 2020, None, None,
                          10, "TestUser", "server", "session", {})
    dispatcher.dispatch(event)
    assert len(calls) == 1
    assert bool(dispatcher._pending) is not delivered
    if change == "disable":
        route["enabled"] = False
    elif change == "delete":
        cfg["scrobble"]["watch"]["routes"] = []
    elif change == "source":
        cfg["scrobble"]["sources"]["watcher"] = False
    else:
        cfg["scrobble"]["enabled"] = False
    dispatcher._retry_after = {key: (0, 1) for key in dispatcher._pending}
    dispatcher.retry_pending()
    assert not dispatcher.accepts_user(event)
    assert not dispatcher.accepts(event)
    assert not dispatcher.dispatch(replace(event, progress=20))
    assert len(calls) == 1 and not dispatcher._pending
    route["enabled"] = True
    cfg["scrobble"]["enabled"] = True
    cfg["scrobble"]["sources"]["watcher"] = True
    cfg["scrobble"]["watch"]["routes"] = [route]
    assert dispatcher.accepts_user(event)
    dispatcher.dispatch(replace(event, progress=30))
    assert len(calls) == 2
