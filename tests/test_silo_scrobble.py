# CrossWatch test scripts
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

import pytest

SERVER = "http://silo.local:8090"
PROFILE = "p-1"


@dataclass
class ResponseStub:
    status_code: int = 200
    payload: Any = None
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def text(self) -> str:
        return "" if self.payload is None else "x"

    def json(self) -> Any:
        return self.payload


class FakeServer:
    def __init__(self) -> None:
        self.routes: dict[tuple[str, str], Callable[[dict[str, Any]], ResponseStub]] = {}
        self.calls: list[dict[str, Any]] = []

    def on(self, method: str, path: str, handler: Callable[[dict[str, Any]], ResponseStub]) -> None:
        self.routes[(method, path)] = handler

    def __call__(self, session: Any, method: str, url: str, **kwargs: Any) -> ResponseStub:
        path = url.split("/api/v2", 1)[1]
        self.calls.append({"method": method, "path": path, **kwargs})
        handler = self.routes.get((method, path))
        if handler is None:
            raise AssertionError(f"unexpected {method} {path}")
        return handler(kwargs)


def _cfg() -> dict[str, Any]:
    return {
        "silo": {"server_url": SERVER, "access_token": "a", "refresh_token": "r", "profile_id": PROFILE, "profile_name": "Alice", "user_id": "1"},
        "scrobble": {"enabled": True, "sources": {"watcher": True}},
    }


@pytest.fixture()
def server(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> FakeServer:
    from providers.sync.silo import _common

    from providers.scrobble import _media_server

    fake = FakeServer()
    monkeypatch.setattr(_common, "request_with_auth", fake)
    monkeypatch.setattr(_common, "STATE_DIR", tmp_path)
    monkeypatch.setattr(_media_server, "record_watch", lambda *a, **k: None)
    monkeypatch.setattr(_media_server, "record_scrobble_event", lambda *a, **k: None)
    monkeypatch.setattr(_media_server, "auto_remove", lambda *a, **k: None)
    _common._LIBRARIES.clear()
    yield fake
    _common._LIBRARIES.clear()


class Collector:
    def __init__(self) -> None:
        self.events: list[Any] = []

    def dispatch(self, ev: Any) -> bool:
        self.events.append(ev)
        return True


def _session(**extra: Any) -> dict[str, Any]:
    row = {
        "id": "s-1", "content_id": "movie-tmdb-100", "media_type": "movie", "media_title": "Alpha",
        "profile_id": PROFILE, "profile_name": "Alice", "position_seconds": 600.0, "file_duration": 6000,
        "is_paused": False, "client_name": "Silo Web", "started_at": "2026-10-27T17:51:01.012Z",
        "season_number": None, "episode_number": None, "series_name": "", "episode_name": "",
    }
    row.update(extra)
    return row


def _watcher(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, Collector]:
    from providers.scrobble.silo import watch

    monkeypatch.setattr(watch, "_cw_update", lambda *a, **k: None)
    monkeypatch.setattr(watch, "_cw_update_payload", lambda *a, **k: None)
    collector = Collector()
    cfg = _cfg()
    return watch.SiloWatchService(dispatcher=collector, cfg_provider=lambda: cfg), collector


def test_watcher_emits_start_pause_resume_stop_for_connected_profile_only(monkeypatch: pytest.MonkeyPatch, server: FakeServer) -> None:
    rows: list[dict[str, Any]] = []
    server.on("GET", "/profiles/household/sessions", lambda kw: ResponseStub(200, {"items": rows}))
    server.on("GET", "/catalog/items/movie-tmdb-100", lambda kw: ResponseStub(200, {"content_id": "movie-tmdb-100", "tmdb_id": "100", "imdb_id": "tt100"}))
    svc, got = _watcher(monkeypatch)

    rows[:] = [_session(), _session(id="s-2", profile_id="p-kid", profile_name="Kid")]
    assert svc._tick() is True
    rows[:] = [_session(position_seconds=612.0, is_paused=True)]
    svc._tick()
    rows[:] = [_session(position_seconds=612.0, is_paused=True)]
    svc._tick()
    rows[:] = [_session(position_seconds=615.0)]
    svc._tick()
    rows[:] = []
    svc._tick()

    assert [e.action for e in got.events] == ["start", "pause", "start", "stop"]
    first = got.events[0]
    assert first.media_type == "movie" and first.ids == {"tmdb": "100", "imdb": "tt100"}
    assert first.account == "Alice" and first.progress == pytest.approx(10.0)
    assert first.duration_ms == 6_000_000 and first.position_ms == 600_000
    assert first.raw["provider"] == "silo" and first.raw["content_id"] == "movie-tmdb-100"
    assert first.server_uuid.startswith("silo:")
    assert got.events[-1].progress == pytest.approx(10.25)
    assert sum(1 for c in server.calls if c["path"] == "/catalog/items/movie-tmdb-100") == 1


def test_watcher_episode_with_legacy_id_uses_detail_series(monkeypatch: pytest.MonkeyPatch, server: FakeServer) -> None:
    rows = [_session(content_id="149382054478429162", media_type="episode", media_title="Episode 1", series_name="Kaos", episode_name="Episode 1", season_number=1, episode_number=1)]
    server.on("GET", "/profiles/household/sessions", lambda kw: ResponseStub(200, {"items": rows}))
    server.on("GET", "/catalog/items/149382054478429162", lambda kw: ResponseStub(200, {"content_id": "149382054478429162", "series_id": "series-tvdb-365700", "tvdb_id": "7243495", "tmdb_id": "2241240", "season_number": 1, "episode_number": 1}))
    server.on("GET", "/catalog/items/series-tvdb-365700", lambda kw: ResponseStub(200, {"content_id": "series-tvdb-365700", "tvdb_id": "365700", "tmdb_id": "117031", "imdb_id": "tt12"}))
    svc, got = _watcher(monkeypatch)

    svc._tick()

    ev = got.events[0]
    assert ev.media_type == "episode" and ev.season == 1 and ev.number == 1 and ev.title == "Kaos"
    assert ev.ids == {"tvdb_show": "365700", "tmdb_show": "117031", "imdb_show": "tt12", "tmdb_episode": "2241240", "tvdb_episode": "7243495"}


@pytest.mark.parametrize("content_id", ["episode-tvdb-477711-1-5", "149382054478429163"])
def test_watcher_episode_reported_as_series(monkeypatch: pytest.MonkeyPatch, server: FakeServer, content_id: str) -> None:
    rows = [_session(content_id=content_id, media_type="series", media_title="Line of Fire (2026)", series_name="Line of Fire (2026)", episode_name="Cul-De-Sac", season_number=1, episode_number=5)]
    server.on("GET", "/profiles/household/sessions", lambda kw: ResponseStub(200, {"items": rows}))
    server.on("GET", f"/catalog/items/{content_id}", lambda kw: ResponseStub(200, {"content_id": content_id, "series_id": "series-tvdb-477711", "tvdb_id": "9001"}))
    server.on("GET", "/catalog/items/series-tvdb-477711", lambda kw: ResponseStub(200, {"content_id": "series-tvdb-477711", "tvdb_id": "477711", "tmdb_id": "300"}))
    svc, got = _watcher(monkeypatch)

    svc._tick()

    ev = got.events[0]
    assert ev.action == "start" and ev.media_type == "episode" and ev.season == 1 and ev.number == 5
    assert ev.title == "Line of Fire (2026)" and ev.ids["tvdb_show"] == "477711" and ev.ids["tvdb_episode"] == "9001"


def test_watcher_marks_offline_on_server_error(monkeypatch: pytest.MonkeyPatch, server: FakeServer) -> None:
    server.on("GET", "/profiles/household/sessions", lambda kw: ResponseStub(403, {"type": "x/permission_denied"}))
    svc, got = _watcher(monkeypatch)

    assert svc._tick() is False
    assert svc._offline is True and got.events == []


def _sink_cfg() -> dict[str, Any]:
    cfg = _cfg()
    cfg["scrobble"]["watch"] = {"route_provider": "plex", "route_provider_instance": "default"}
    return cfg


def _catalog(server: FakeServer) -> None:
    pages = {
        "movie": [{"content_id": "movie-tmdb-100", "title": "Alpha", "year": 2024, "runtime": 100}],
        "series": [{"content_id": "series-tvdb-300", "title": "Gamma"}],
        "episode": [{"content_id": "episode-tvdb-300-1-1", "series_id": "series-tvdb-300", "season_number": 1, "episode_number": 1, "runtime": 45}],
    }
    server.on("GET", "/catalog", lambda kw: ResponseStub(200, {"items": pages[kw["params"]["type"]], "page": {"has_more": False}}))
    server.on("GET", "/profiles/household/sessions", lambda kw: ResponseStub(200, {"items": []}))


def _event(action: str, progress: float, media: str = "movie") -> Any:
    from providers.scrobble.scrobble import ScrobbleEvent

    ids = {"tmdb": "100"} if media == "movie" else {"tvdb_show": "300"}
    return ScrobbleEvent(action=action, media_type=media, ids=ids, title="Alpha", year=2024, season=1 if media == "episode" else None,
                         number=1 if media == "episode" else None, progress=progress, account="plexuser", server_uuid="plex-1",
                         session_key="k1", raw={"_cw_watched_at": 1_700_000_000})


def test_sink_complete_writes_dated_watch_and_verifies(server: FakeServer) -> None:
    from providers.scrobble.silo.sink import SiloSink

    _catalog(server)
    played = {"value": False}
    sent: list[Any] = []
    server.on("GET", "/catalog/items/movie-tmdb-100", lambda kw: ResponseStub(200, {"content_id": "movie-tmdb-100", "runtime": 100, "user_data": {"duration_seconds": 6012}, "user_state": {"played": played["value"]}}))

    def write(kw: dict[str, Any]) -> ResponseStub:
        sent.append(kw["json"])
        played["value"] = True
        return ResponseStub(200, {"items": [{"index": 0, "media_item_id": "movie-tmdb-100", "status": "success"}], "summary": {}})

    server.on("POST", "/sync/progress", write)
    sink = SiloSink(cfg_provider=_sink_cfg, instance_id="default")

    res = sink.send(_event("stop", 96.0), _sink_cfg())
    again = sink.send(_event("stop", 96.0), _sink_cfg())

    assert res == {"ok": True}
    assert sent == [{"items": [{"media_item_id": "movie-tmdb-100", "duration_ms": 6_012_000, "position_ms": 6_012_000, "updated_at": "2023-11-14T22:13:20Z"}]}]
    assert again.get("skipped") is True


def test_sink_progress_and_already_watched_paths(server: FakeServer) -> None:
    from providers.scrobble.silo.sink import SiloSink

    _catalog(server)
    state = {"played": False}
    sent: list[Any] = []
    server.on("GET", "/catalog/items/episode-tvdb-300-1-1", lambda kw: ResponseStub(200, {"content_id": "episode-tvdb-300-1-1", "runtime": 45, "user_data": {}, "user_state": state}))
    server.on("POST", "/sync/progress", lambda kw: sent.append(kw["json"]) or ResponseStub(200, {"items": [{"index": 0, "media_item_id": "episode-tvdb-300-1-1", "status": "success"}], "summary": {}}))
    sink = SiloSink(cfg_provider=_sink_cfg, instance_id="default")

    res = sink.send(_event("pause", 40.0, "episode"), _sink_cfg())
    assert res == {"ok": True}
    assert sent[0]["items"][0] == {"media_item_id": "episode-tvdb-300-1-1", "duration_ms": 2_700_000, "position_ms": 1_080_000}

    state["played"] = True
    sink._sent.clear()
    assert sink.send(_event("pause", 50.0, "episode"), _sink_cfg())["reason"] == "destination_already_watched"


def test_sink_unmatched_is_not_retryable(server: FakeServer) -> None:
    from providers.scrobble.scrobble import ScrobbleEvent
    from providers.scrobble.silo.sink import SiloSink

    _catalog(server)
    ev = ScrobbleEvent(action="stop", media_type="movie", ids={"tmdb": "999"}, title="Nope", year=None, season=None, number=None,
                       progress=99.0, account=None, server_uuid=None, session_key="k2", raw={})

    res = SiloSink(cfg_provider=_sink_cfg, instance_id="default").send(ev, _sink_cfg())

    assert res["ok"] is False and res["error"] == "unmatched_in_silo" and res["retryable"] is False


def test_factories_routes_and_sink_configuration() -> None:
    from providers.scrobble import routes, watch_manager
    from providers.scrobble.silo.sink import SiloSink
    from providers.scrobble.silo.watch import SiloWatchService
    from providers.webhooks import dispatch
    from providers.webhooks.config import sink_configured

    assert "silo" in routes.ROUTE_PROVIDERS and "silo" in routes.ROUTE_SINKS and "silo" in routes.ROUTE_POLL_PROVIDERS
    assert "silo" not in routes.ROUTE_MEDIA_SINKS
    assert isinstance(watch_manager._make_sink("silo", _cfg, "default"), SiloSink)
    assert isinstance(dispatch._make_sink("silo", "default", _cfg), SiloSink)
    assert isinstance(watch_manager._make_watcher("silo", watch_manager.MultiDispatcher([]), _cfg, "default"), SiloWatchService)
    assert sink_configured(_cfg(), "silo", "default") is True
    assert sink_configured({"silo": {"server_url": SERVER, "access_token": "a"}}, "silo", "default") is False
    assert sink_configured({"silo": {"instances": {"SILO-P01": _cfg()["silo"]}}}, "silo", "SILO-P02") is False


def test_plex_rating_forwarded_to_silo_movies_and_shows_only(server: FakeServer) -> None:
    from providers.scrobble import routes
    from providers.scrobble.plex.ratings_sync import OPS_RATING_SINKS, RATING_SINKS, send_rating

    _catalog(server)
    sent: list[Any] = []
    server.on("PUT", "/ratings/movie-tmdb-100", lambda kw: sent.append(kw["json"]) or ResponseStub(204, None))
    server.on("DELETE", "/ratings/movie-tmdb-100", lambda kw: sent.append("deleted") or ResponseStub(204, None))

    assert "silo" in OPS_RATING_SINKS and "silo" in RATING_SINKS and "silo" in routes.ROUTE_RATING_SINKS
    res = send_rating("silo", _cfg(), "default", {"type": "movie", "ids": {"tmdb": "100"}, "rating": 8}, 8.0)
    assert res["ok"] is True and sent == [{"rating": 4}]
    cleared = send_rating("silo", _cfg(), "default", {"type": "movie", "ids": {"tmdb": "100"}}, None)
    assert cleared["ok"] is True and sent[-1] == "deleted"
    ep = send_rating("silo", _cfg(), "default", {"type": "episode", "show_ids": {"tvdb": "300"}, "season": 1, "episode": 1}, 7.0)
    assert ep == {"ok": True, "skipped": True, "reason": "unsupported_media_type"}
