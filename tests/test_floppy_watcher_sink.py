# tests/test_floppy_watcher_sink.py
# CrossWatch test scripts
from __future__ import annotations

from pathlib import Path
from typing import Any

import providers.scrobble.floppy.sink as floppy_sink
import providers.scrobble.watch_manager as watch_manager
from providers.scrobble.floppy.sink import FloppySink
from providers.scrobble.routes import build_route_cfg, normalize_route
from providers.scrobble.scrobble import ScrobbleEvent
from providers.webhooks.config import sink_configured, webhook_sinks


ROOT = Path(__file__).resolve().parents[1]


def _cfg() -> dict[str, Any]:
    return {
        "floppy": {
            "server_url": "http://floppy.local",
            "api_token": "token",
            "verify_ssl": False,
        },
        "scrobble": {
            "watch": {
                "route_provider": "plex",
                "route_provider_instance": "default",
                "route_sink": "floppy",
                "route_sink_instance": "default",
            },
            "trakt": {"watched_at": 90},
        },
    }


def _movie(action: str = "pause", progress: float = 40.0) -> ScrobbleEvent:
    return ScrobbleEvent(
        action=action,
        media_type="movie",
        ids={"tmdb": "1435092", "imdb": "tt1234567"},
        title="Goodbye June",
        year=2026,
        season=None,
        number=None,
        progress=progress,
        account="Living Room",
        server_uuid="server-1",
        session_key="session-1",
        raw={"duration": 6000000, "viewOffset": 2400000},
    )


def _episode(action: str = "stop", progress: float = 92.0) -> ScrobbleEvent:
    return ScrobbleEvent(
        action=action,
        media_type="episode",
        ids={"tmdb_show": "124800", "tvdb_episode": "9621656"},
        title="Love & Death",
        year=2023,
        season=1,
        number=7,
        progress=progress,
        account="Living Room",
        server_uuid="server-1",
        session_key="session-2",
        raw={"duration": 3000000},
    )


def test_watcher_routes_accept_floppy_sink_profile() -> None:
    cfg = {"plex": {"account_token": "source-token"}, "floppy": {"instances": {"FLOPPY-P01": {"server_url": "http://x", "api_token": "t"}}}}
    route = normalize_route({"id": "R1", "provider": "plex", "sink": "floppy", "sink_instance": "FLOPPY-P01"}, "R1")
    view = build_route_cfg(cfg, route)

    assert route["sink"] == "floppy"
    assert view["floppy"]["server_url"] == "http://x"


def test_watch_manager_can_create_floppy_sink() -> None:
    sink = watch_manager._make_sink("floppy", _cfg, "default")

    assert isinstance(sink, FloppySink)


def test_floppy_is_available_as_configured_watcher_destination() -> None:
    cfg = _cfg()
    cfg["scrobble"]["webhook"] = {"sinks": ["floppy", "trakt"]}

    assert sink_configured(cfg, "floppy", "default") is True
    assert "floppy" in webhook_sinks(cfg, "plex", "default")


def test_floppy_sink_sends_movie_pause_payload(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    monkeypatch.setattr(floppy_sink, "api_post", lambda _adapter, path, **kwargs: calls.append({"path": path, "body": kwargs.get("json")}) or {"detail": "ok"})
    monkeypatch.setattr(floppy_sink, "record_watch", lambda *args, **kwargs: events.append(kwargs))

    FloppySink(cfg_provider=_cfg).send(_movie())

    assert calls == [
        {
            "path": "scrobble",
            "body": {
                "action": "pause",
                "media_type": "movie",
                "ids": {"tmdb": "1435092", "imdb": "tt1234567"},
                "title": "Goodbye June",
                "duration_seconds": 6000,
                "position_seconds": 2400,
            },
        }
    ]
    assert events[0]["destination_provider"] == "floppy"


def test_floppy_sink_sends_completed_episode_stop(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []
    activities: list[dict[str, Any]] = []
    monkeypatch.setattr(floppy_sink, "api_post", lambda _adapter, path, **kwargs: calls.append({"path": path, "body": kwargs.get("json")}) or {"detail": "ok"})
    monkeypatch.setattr(floppy_sink, "record_watch", lambda *args, **kwargs: None)
    monkeypatch.setattr(floppy_sink, "record_scrobble_event", lambda *args, **kwargs: activities.append(kwargs))
    monkeypatch.setattr(floppy_sink, "utc_now_iso", lambda: "2026-08-01T12:00:00Z")

    FloppySink(cfg_provider=_cfg).send(_episode())

    body = calls[0]["body"]
    assert body["action"] == "stop"
    assert body["media_type"] == "episode"
    assert body["ids"] == {"tmdb": "124800"}
    assert body["series_title"] == "Love & Death"
    assert body["season_number"] == 1
    assert body["episode_number"] == 7
    assert body["position_seconds"] == 2760
    assert body["duration_seconds"] == 3000
    assert body["completed"] is True
    assert body["played_at"] == "2026-08-01T12:00:00Z"
    assert activities[0]["target"] == "floppy"


def test_scrobbler_route_modal_lists_floppy_sink() -> None:
    text = (ROOT / "assets" / "js" / "modals" / "scrobbler-route" / "index.js").read_text("utf-8")
    meta = (ROOT / "assets" / "helpers" / "provider-meta.js").read_text("utf-8")

    assert 'const sinks = ["crosswatch", "trakt", "simkl", "mdblist", "floppy"];' in text
    assert 'floppy: "Floppy"' in text
    assert 'const ratingSinks = ["trakt", "simkl", "mdblist"];' in text
    assert "FLOPPY:" in meta
    assert "scrobblerSink: true" in meta
