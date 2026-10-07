# tests/test_stremio_addon.py
# CrossWatch - Stremio add-on event source tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from providers.scrobble.stremio import addon
from providers.scrobble.stremio import watch as stremio_watch
from providers.scrobble.stremio.watch import StremioWatchService

TOKEN = "tok-stremio-default-0123456789"
SECOND_TOKEN = "tok-stremio-second-0123456789"
MOVIE = "tt0133093"
EPISODE = "tt0903747:2:5"


class FakeDispatcher:
    def __init__(self, accept: bool = True) -> None:
        self.accept = accept
        self.events: list[Any] = []

    def dispatch(self, event: Any) -> bool:
        if self.accept:
            self.events.append(event)
        return self.accept


def make_cfg(routes: list[dict[str, Any]] | None = None, *, watcher: bool = True) -> dict[str, Any]:
    return {
        "security": {"webhook_ids": {"stremioaddon:default": TOKEN, "stremioaddon:second": SECOND_TOKEN}},
        "stremio": {},
        "scrobble": {
            "enabled": True,
            "sources": {"watcher": watcher},
            "watch": {"routes": list(routes or [])},
            "trakt": {"force_stop_at": 95},
        },
    }


def route(route_id: str = "R1", instance: str = "default") -> dict[str, Any]:
    return {"id": route_id, "enabled": True, "provider": "stremio", "provider_instance": instance, "sink": "trakt", "sink_instance": "default"}


def make_service(cfg: dict[str, Any], dispatcher: FakeDispatcher, instance: str = "default") -> StremioWatchService:
    return StremioWatchService(dispatcher=dispatcher, cfg_provider=lambda: cfg, instance_id=instance)


def make_app(service: StremioWatchService, instance: str = "default") -> SimpleNamespace:
    group = SimpleNamespace(provider="stremio", provider_instance=instance, watcher=service, routes=[])
    return SimpleNamespace(state=SimpleNamespace(watch_groups={f"stremio:{instance}": group}))


def player(action: str, position: int | None = None, duration: int | None = None) -> dict[str, str]:
    args = {"action": action}
    if position is not None:
        args["currentTime"] = str(position)
    if duration is not None:
        args["duration"] = str(duration)
    return args


@pytest.fixture(autouse=True)
def isolated_state(tmp_path, monkeypatch):
    cards: list[tuple[str, Any]] = []
    monkeypatch.setattr(addon, "_state_file", lambda: tmp_path / "stremio_addon.json")
    monkeypatch.setattr(stremio_watch, "_cw_update", lambda *a, **k: cards.append(("event", a[1].action)))
    monkeypatch.setattr(stremio_watch, "_cw_update_payload", lambda *a, **k: cards.append(("payload", k.get("state"))))
    monkeypatch.setattr(stremio_watch, "_fetch_meta", lambda kind, imdb: {"title": f"Title {imdb}", "year": 1999})
    stremio_watch._META_CACHE.clear()
    addon.reset_state()
    yield cards
    addon.reset_state()


def test_tokens_enable_and_resolve_instances() -> None:
    cfg = make_cfg()
    assert addon.source_ready(cfg, "default") is True
    assert addon.instance_for_token(cfg, TOKEN) == "default"
    assert addon.instance_for_token(cfg, SECOND_TOKEN) == "second"
    assert addon.instance_for_token(cfg, "nope") is None
    assert addon.source_ready(cfg, "third") is False

    cfg["security"]["webhook_ids"]["stremioaddon:default"] = "•" * 8
    assert addon.instance_enabled(cfg, "default") is False


def test_set_instance_enabled_creates_regenerates_and_removes() -> None:
    cfg: dict[str, Any] = {}
    first = addon.set_instance_enabled(cfg, "default", True)
    assert len(first) >= addon.MIN_TOKEN_LENGTH
    assert addon.set_instance_enabled(cfg, "default", True) == first
    assert addon.set_instance_enabled(cfg, "default", True, regenerate=True) != first
    assert addon.set_instance_enabled(cfg, "default", False) == ""
    assert addon.instance_enabled(cfg, "default") is False


def test_urls_and_installable_flag() -> None:
    assert addon.manifest_url("https://cw.example/", TOKEN) == f"https://cw.example/webhook/stremio/{TOKEN}/manifest.json"
    assert addon.install_url("https://cw.example", TOKEN) == f"stremio://cw.example/webhook/stremio/{TOKEN}/manifest.json"
    encoded = f"https%3A%2F%2Fcw.example%3A9898%2Fwebhook%2Fstremio%2F{TOKEN}%2Fmanifest.json"
    assert addon.install_url("https://cw.example:9898", TOKEN) == f"stremio:///addons?addon={encoded}"
    assert addon.web_install_url("https://cw.example:9898", TOKEN) == f"https://web.stremio.com/#/addons?addon={encoded}"
    assert addon.url_installable("https://cw.example") is True
    assert addon.url_installable("http://127.0.0.1:8787") is True
    assert addon.url_installable("http://192.168.2.100:8787") is False
    assert addon.redact_path(f"/webhook/stremio/{TOKEN}/player/movie/{MOVIE}/x.json") == f"/webhook/stremio/***/player/movie/{MOVIE}/x.json"


def test_manifest_declares_player_resource() -> None:
    data = addon.manifest(make_cfg(), "default")
    assert data["id"] == addon.ADDON_ID
    assert data["catalogs"] == []
    assert data["resources"] == [{"name": "player", "types": ["movie", "series"], "idPrefixes": ["tt", "tmdb:", "tvdb:"]}]
    assert len(data["version"].split(".")) == 3
    assert addon.manifest(make_cfg(), "second")["id"] == f"{addon.ADDON_ID}.second"
    assert data["logo"] == addon.LOGO_URL and data["logo"].startswith("https://")


def test_split_resource_path() -> None:
    assert addon.split_resource_path(f"movie/{MOVIE}/action=start&currentTime=1000&duration=5000.json") == (
        "movie",
        MOVIE,
        {"action": "start", "currentTime": "1000", "duration": "5000"},
    )
    assert addon.split_resource_path("movie.json") == ("", "", {})


def test_build_event_for_movie_and_episode() -> None:
    movie, _ = addon.build_event("movie", MOVIE, player("start", 1000, 5000), "default")
    assert movie is not None
    assert (movie.media_type, movie.ids, movie.position_ms, movie.duration_ms) == ("movie", {"imdb": MOVIE}, 1000, 5000)
    assert movie.session_key == f"stremio:default:addon:{MOVIE}"

    episode, _ = addon.build_event("series", EPISODE, player("pause", 0, 0), "second")
    assert episode is not None
    assert (episode.media_type, episode.ids, episode.season, episode.number) == ("episode", {"imdb_show": "tt0903747"}, 2, 5)
    assert episode.duration_ms is None

    special, _ = addon.build_event("series", "tmdb:1396:0:3", player("stop"), "default")
    assert special is not None and special.ids == {"tmdb_show": "1396"} and special.season == 0


@pytest.mark.parametrize(
    ("media_type", "video_id", "args", "reason"),
    [
        ("movie", MOVIE, {"action": "seek"}, "unsupported_event"),
        ("channel", MOVIE, {"action": "start"}, "unsupported_media"),
        ("series", "tt0903747", {"action": "start"}, "missing_episode_number"),
        ("series", "kitsu:123:5", {"action": "start"}, "no_ids"),
        ("movie", "12345", {"action": "start"}, "no_ids"),
    ],
)
def test_build_event_rejects(media_type: str, video_id: str, args: dict[str, str], reason: str) -> None:
    assert addon.build_event(media_type, video_id, args, "default") == (None, reason)


def test_playback_flow_dispatches_start_pause_stop(isolated_state) -> None:
    cfg = make_cfg([route()])
    disp = FakeDispatcher()
    service = make_service(cfg, disp)
    app = make_app(service)

    assert addon.handle(app, cfg, "default", "movie", MOVIE, player("start", 0, 100000)) == {"success": True}
    assert addon.handle(app, cfg, "default", "movie", MOVIE, player("pause", 50000, 100000)) == {"success": True}
    assert addon.handle(app, cfg, "default", "movie", MOVIE, player("stop", 97000, 100000)) == {"success": True}

    assert [(e.action, round(e.progress)) for e in disp.events] == [("start", 1), ("pause", 50), ("stop", 97)]
    first, last = disp.events[0], disp.events[-1]
    assert first.title == f"Title {MOVIE}" and first.year == 1999 and first.account is None
    assert first.session_key == f"stremio:default:addon:{MOVIE}" and first.server_uuid == "stremio:default"
    assert last.raw["_cw_preserve_stop"] is True
    assert service._sessions == {}
    assert addon.snapshot("default")["last_event"] == "stop"


def test_stop_without_times_uses_last_known_progress() -> None:
    cfg = make_cfg([route()])
    disp = FakeDispatcher()
    app = make_app(make_service(cfg, disp))

    addon.handle(app, cfg, "default", "series", EPISODE, player("start", 30000, 100000))
    assert addon.handle(app, cfg, "default", "series", EPISODE, player("stop")) == {"success": True}
    assert [(e.action, round(e.progress)) for e in disp.events] == [("start", 30), ("stop", 30)]
    assert disp.events[0].media_type == "episode" and disp.events[0].ids == {"imdb_show": "tt0903747"}


def test_start_without_duration_near_the_beginning_is_dispatched() -> None:
    cfg = make_cfg([route()])
    disp = FakeDispatcher()
    service = make_service(cfg, disp)
    app = make_app(service)

    assert addon.handle(app, cfg, "default", "movie", MOVIE, player("start", 0, 0)) == {"success": True}
    assert addon.handle(app, cfg, "default", "movie", MOVIE, player("pause", 50000, 100000)) == {"success": True}
    assert [(e.action, round(e.progress)) for e in disp.events] == [("start", 1), ("pause", 50)]


def test_loading_pause_and_repeated_start_are_not_dispatched() -> None:
    cfg = make_cfg([route()])
    disp = FakeDispatcher()
    app = make_app(make_service(cfg, disp))

    assert addon.handle(app, cfg, "default", "movie", MOVIE, player("pause", 0, 0)) == {"success": False, "ignored": "not_started"}
    assert addon.handle(app, cfg, "default", "movie", MOVIE, player("start", 0, 0)) == {"success": True}
    assert addon.handle(app, cfg, "default", "movie", MOVIE, player("start", 0, 0)) == {"success": False, "ignored": "duplicate"}
    assert addon.handle(app, cfg, "default", "movie", MOVIE, player("pause", 30030, 6345013)) == {"success": True}
    assert [e.action for e in disp.events] == ["start", "pause"]


def test_unknown_duration_mid_playback_is_tracked_but_not_dispatched() -> None:
    cfg = make_cfg([route()])
    disp = FakeDispatcher()
    service = make_service(cfg, disp)
    app = make_app(service)

    assert addon.handle(app, cfg, "default", "movie", MOVIE, player("start", 600000, 0)) == {"success": False, "ignored": "progress_unknown"}
    assert disp.events == [] and len(service._sessions) == 1
    assert addon.handle(app, cfg, "default", "movie", MOVIE, player("stop")) == {"success": False, "ignored": "progress_unknown"}
    assert disp.events == [] and service._sessions == {}


def test_runtime_estimate_is_used_for_start_and_capped_on_stop(monkeypatch) -> None:
    monkeypatch.setattr(stremio_watch, "_fetch_meta", lambda kind, imdb: {"title": "T", "year": 1999, "runtime_ms": 100 * 60_000})
    cfg = make_cfg([route()])
    disp = FakeDispatcher()
    app = make_app(make_service(cfg, disp))

    assert addon.handle(app, cfg, "default", "movie", MOVIE, player("start", 90 * 60_000)) == {"success": True}
    assert addon.handle(app, cfg, "default", "movie", MOVIE, player("stop")) == {"success": True}
    assert [(e.action, round(e.progress)) for e in disp.events] == [("start", 90), ("stop", 50)]


def test_handle_ignores_without_watcher_or_route() -> None:
    cfg = make_cfg([route()])
    app = make_app(make_service(cfg, FakeDispatcher()))
    args = player("start", 0, 100000)

    assert addon.handle(app, make_cfg(watcher=False), "default", "movie", MOVIE, args)["ignored"] == "watcher_disabled"
    assert addon.handle(SimpleNamespace(state=SimpleNamespace(watch_groups={})), cfg, "default", "movie", MOVIE, args)["ignored"] == "no_routes"
    assert addon.handle(app, cfg, "second", "movie", MOVIE, args)["ignored"] == "no_routes"
    assert addon.handle(make_app(make_service(cfg, FakeDispatcher(accept=False))), cfg, "default", "movie", MOVIE, args)["ignored"] == "no_matching_route"


def test_sweep_refreshes_card_then_times_out(isolated_state) -> None:
    cfg = make_cfg([route()])
    disp = FakeDispatcher()
    service = make_service(cfg, disp)
    item, _ = addon.build_event("movie", MOVIE, player("start", 10000, 100000), "default")
    assert item is not None
    service.ingest(item, now=1000.0)
    isolated_state.clear()

    service.sweep(now=1000.0 + stremio_watch.CARD_REFRESH_SECONDS - 1)
    assert isolated_state == []
    service.sweep(now=1000.0 + stremio_watch.CARD_REFRESH_SECONDS)
    assert isolated_state == [("payload", "playing")]
    assert len(service._sessions) == 1

    service.sweep(now=1000.0 + 90.0 + stremio_watch.STALE_GRACE_SECONDS + 1)
    assert service._sessions == {}
    assert [(e.action, round(e.progress)) for e in disp.events] == [("start", 10), ("stop", 10)]


def test_paused_session_times_out() -> None:
    cfg = make_cfg([route()])
    disp = FakeDispatcher()
    service = make_service(cfg, disp)
    item, _ = addon.build_event("movie", MOVIE, player("pause", 40000, 100000), "default")
    assert item is not None
    service.ingest(item, now=1000.0)

    service.sweep(now=1000.0 + stremio_watch.PAUSED_TIMEOUT_SECONDS - 1)
    assert len(service._sessions) == 1
    service.sweep(now=1000.0 + stremio_watch.PAUSED_TIMEOUT_SECONDS + 1)
    assert service._sessions == {} and disp.events[-1].action == "stop"


def test_metadata_lookup_is_cached_and_failure_safe(monkeypatch) -> None:
    calls: list[str] = []

    def fetch(kind: str, imdb: str) -> dict[str, Any]:
        calls.append(imdb)
        raise RuntimeError("offline")

    monkeypatch.setattr(stremio_watch, "_fetch_meta", fetch)
    assert stremio_watch.lookup_meta("movie", {"imdb": MOVIE}) == {}
    assert stremio_watch.lookup_meta("movie", {"imdb": MOVIE}) == {}
    assert calls == [MOVIE]
    assert stremio_watch.lookup_meta("movie", {"tmdb": "603"}) == {}


def test_status_reports_urls_and_routes() -> None:
    cfg = make_cfg([route()])
    info = addon.status(cfg, "default", "https://cw.example", now=2000.0)
    assert info["enabled"] is True and info["installable"] is True and info["installed"] is False
    assert info["manifest_url"].endswith(f"/webhook/stremio/{TOKEN}/manifest.json")
    assert info["routes"] == 1 and info["watcher_enabled"] is True

    addon.mark_seen("default", "start", now=1900.0)
    seen = addon.status(cfg, "default", "http://192.168.2.100:8787", now=2000.0)
    assert seen["installed"] is True and seen["age_seconds"] == 100 and seen["last_event"] == "start"
    assert seen["installable"] is False

    off = addon.status(cfg, "third", "https://cw.example")
    assert off["enabled"] is False and off["manifest_url"] == "" and off["install_url"] == ""


def _client(monkeypatch, cfg: dict[str, Any], app_state: SimpleNamespace | None = None) -> TestClient:
    from api import scrobbleAPI

    monkeypatch.setattr(scrobbleAPI, "load_config", lambda: cfg)
    app = FastAPI()
    app.include_router(scrobbleAPI.router)
    if app_state is not None:
        app.state.watch_groups = app_state.state.watch_groups
    return TestClient(app)


def test_endpoints_serve_manifest_and_events_with_cors(monkeypatch) -> None:
    cfg = make_cfg([route()])
    disp = FakeDispatcher()
    client = _client(monkeypatch, cfg, make_app(make_service(cfg, disp)))

    manifest = client.get(f"/webhook/stremio/{TOKEN}/manifest.json")
    assert manifest.status_code == 200 and manifest.json()["id"] == addon.ADDON_ID
    assert manifest.headers["access-control-allow-origin"] == "*"

    event = client.get(f"/webhook/stremio/{TOKEN}/player/series/{EPISODE}/action=start&currentTime=5000&duration=100000.json")
    assert event.status_code == 200 and event.json() == {"success": True}
    assert event.headers["access-control-allow-origin"] == "*" and event.headers["cache-control"] == "no-store"
    assert [(e.action, e.season, e.number, round(e.progress)) for e in disp.events] == [("start", 2, 5, 5)]

    encoded = client.get(f"/webhook/stremio/{TOKEN}/player/series/tt0903747%3A2%3A5/action%3Dstop%26currentTime%3D99000%26duration%3D100000.json")
    assert encoded.json() == {"success": True} and disp.events[-1].action == "stop"

    assert client.options(f"/webhook/stremio/{TOKEN}/player/movie/{MOVIE}/action=start.json").headers["access-control-allow-origin"] == "*"


def test_endpoints_reject_unknown_tokens(monkeypatch) -> None:
    client = _client(monkeypatch, make_cfg())
    assert client.get("/webhook/stremio/wrong-token-0123456789/manifest.json").status_code == 401
    assert client.get(f"/webhook/stremio/wrong-token-0123456789/player/movie/{MOVIE}/action=start.json").status_code == 401


def test_stremio_is_a_route_source_without_account_filter() -> None:
    from providers.scrobble.routes import ROUTE_PROVIDERS, normalize_route, route_needs_account_filter

    assert "stremio" in ROUTE_PROVIDERS
    assert normalize_route(route(), "R1")["provider"] == "stremio"
    cfg = make_cfg([{**route(), "profile_id": "a" * 32}])
    cfg["user_profiles"] = {"a" * 32: {"label": "Alice", "instance_uids": []}}
    assert route_needs_account_filter(cfg, cfg["scrobble"]["watch"]["routes"][0]) is False


def test_profile_scoped_stremio_route_accepts_events_without_whitelist() -> None:
    from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent

    watch = {"route_enabled": True, "route_profile_id": "a" * 32, "route_provider": "stremio", "filters": {}}
    event = ScrobbleEvent("start", "movie", {"imdb": MOVIE}, "T", 1999, None, None, 5.0, None, "stremio:default", "s1", {})

    assert Dispatcher([], cfg_provider=lambda: {})._identity_allowed(event, {"scrobble": {"watch": watch}}) is True
    assert Dispatcher([], cfg_provider=lambda: {})._identity_allowed(event, {"scrobble": {"watch": {**watch, "route_provider": "plex"}}}) is False
