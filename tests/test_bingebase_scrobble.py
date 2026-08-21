# CrossWatch BingeBase scrobble tests
from __future__ import annotations

import json
from typing import Any

import pytest


class _Resp:
    def __init__(self, status_code: int = 200, payload: Any = None) -> None:
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = json.dumps(self._payload)
        self.headers: dict[str, str] = {}

    def json(self) -> Any:
        return self._payload


class Event:
    def __init__(self, **kw: Any) -> None:
        self.action = kw.get("action", "start")
        self.media_type = kw.get("media_type", "movie")
        self.ids = kw.get("ids", {"tmdb": "550", "imdb": "tt0137523"})
        self.title = kw.get("title", "Fight Club")
        self.year = kw.get("year", 1999)
        self.season = kw.get("season")
        self.number = kw.get("number")
        self.progress = kw.get("progress", 10.0)
        self.account = kw.get("account", "user@example.com")
        self.server_uuid = kw.get("server_uuid", "srv-1")
        self.session_key = kw.get("session_key", "sess-1")
        self.raw = kw.get("raw", {})
        self.position_ms = kw.get("position_ms")
        self.duration_ms = kw.get("duration_ms")


WEBHOOK_URL = "https://bingebase.com/api/webhooks/jellyfin?token=secret-token"
KODI_WEBHOOK_URL = "https://bingebase.com/webhooks/kodi/device-access"
CFG = {
    "bingebase": {"webhook_url": WEBHOOK_URL, "access_token": "device-access", "api_key": "bearer-secret"},
    "scrobble": {"watch": {"watched_at": 90}},
}


@pytest.fixture()
def sink(monkeypatch: pytest.MonkeyPatch):
    from providers.scrobble.bingebase.sink import BingeBaseSink

    calls: list[dict[str, Any]] = []
    s = BingeBaseSink(cfg_provider=lambda: dict(CFG))

    def fake_post(url: str, **kwargs: Any) -> _Resp:
        calls.append({"url": url, **kwargs})
        return _Resp(200, {})

    monkeypatch.setattr(s.session, "post", fake_post)
    return s, calls


def test_sink_is_registered_for_webhook_and_watcher_destinations() -> None:
    from providers.scrobble.bingebase.sink import BingeBaseSink
    from providers.scrobble.routes import ROUTE_RATING_SINKS, ROUTE_SINKS, build_route_cfg, normalize_route
    from providers.scrobble.watch_manager import _make_sink
    from providers.webhooks.config import sink_configured, webhook_sinks
    from providers.webhooks.dispatch import _make_sink as make_webhook_sink

    route = normalize_route({"id": "R1", "provider": "plex", "sink": "bingebase"}, "R1")
    view = build_route_cfg(CFG, route)

    assert "bingebase" in ROUTE_SINKS
    assert "bingebase" not in ROUTE_RATING_SINKS
    assert route["sink"] == "bingebase"
    assert view["bingebase"]["webhook_url"] == WEBHOOK_URL
    assert sink_configured(CFG, "bingebase", "default") is True
    assert isinstance(_make_sink("bingebase", lambda: dict(CFG), "default"), BingeBaseSink)
    assert isinstance(make_webhook_sink("bingebase", "default", lambda: dict(CFG)), BingeBaseSink)

    cfg = {**CFG, "scrobble": {"webhook": {"sinks": ["bingebase"]}}}
    assert "bingebase" in webhook_sinks(cfg, "plex", "default")


def test_movie_payload_shape(sink) -> None:
    s, calls = sink

    s.send(Event(action="start", progress=12.3456))
    payload = calls[0]["json"]

    assert calls[0]["url"] == WEBHOOK_URL
    assert calls[0]["headers"]["Authorization"] == "Bearer bearer-secret"
    assert payload["Event"] == "playback.start"
    assert payload["NotificationType"] == "PlaybackStart"
    assert payload["Percentage"] == 12.3
    assert payload["Item"]["Name"] == "Fight Club"
    assert payload["Item"]["Type"] == "Movie"
    assert payload["Item"]["ProductionYear"] == 1999
    assert payload["Item"]["ProviderIds"] == {"Tmdb": "550", "Imdb": "tt0137523"}


def test_episode_payload_shape(sink) -> None:
    s, calls = sink

    s.send(Event(
        action="pause",
        media_type="episode",
        title="Severance",
        season=1,
        number=2,
        ids={"tmdb": "5978363", "imdb": "tt11280740", "tmdb_show": "95396"},
        progress=45,
    ))
    item = calls[0]["json"]["Item"]

    assert calls[0]["json"]["Event"] == "playback.pause"
    assert item["Type"] == "Episode"
    assert item["SeriesName"] == "Severance"
    assert item["ParentIndexNumber"] == 1
    assert item["IndexNumber"] == 2
    assert item["ProviderIds"] == {"Tmdb": "5978363", "Imdb": "tt11280740", "ShowTmdb": "95396"}


def test_kodi_webhook_uses_kodi_payload_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.bingebase.sink import BingeBaseSink

    calls: list[dict[str, Any]] = []
    cfg = {"bingebase": {"webhook_url": KODI_WEBHOOK_URL, "access_token": "device-access"}}
    s = BingeBaseSink(cfg_provider=lambda: cfg)
    monkeypatch.setattr(s.session, "post", lambda url, **kwargs: calls.append({"url": url, **kwargs}) or _Resp(200, {}))

    s.send(Event(action="start", progress=25, duration_ms=400000, position_ms=100000))

    payload = calls[0]["json"]
    assert calls[0]["url"] == KODI_WEBHOOK_URL
    assert "Authorization" not in calls[0]["headers"]
    assert payload["event"] == "start"
    assert payload["mediaType"] == "movie"
    assert payload["title"] == "Fight Club"
    assert payload["uniqueIds"] == {"tmdb": "550", "imdb": "tt0137523"}
    assert payload["duration"] == 400
    assert payload["progress"] == {"percent": 25.0, "time": 100}


def test_kodi_webhook_episode_payload_includes_show_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.bingebase.sink import BingeBaseSink

    calls: list[dict[str, Any]] = []
    cfg = {"bingebase": {"webhook_url": KODI_WEBHOOK_URL, "access_token": "device-access"}}
    s = BingeBaseSink(cfg_provider=lambda: cfg)
    monkeypatch.setattr(s.session, "post", lambda url, **kwargs: calls.append({"url": url, **kwargs}) or _Resp(200, {}))

    s.send(Event(
        action="pause",
        media_type="episode",
        title="Severance",
        season=1,
        number=2,
        ids={"tmdb_episode": "5978363", "imdb_episode": "tt11280740", "tmdb_show": "95396"},
        progress=45,
    ))

    payload = calls[0]["json"]
    assert payload["event"] == "pause"
    assert payload["mediaType"] == "episode"
    assert payload["tvShowTitle"] == "Severance"
    assert payload["season"] == 1
    assert payload["episode"] == 2
    assert payload["uniqueIds"] == {"tmdb": "5978363", "imdb": "tt11280740"}
    assert payload["showUniqueIds"] == {"tmdb": "95396"}


def test_start_pause_stop_mapping(sink) -> None:
    s, calls = sink

    for action in ("start", "pause", "stop"):
        s.send(Event(action=action))

    payloads = [call["json"] for call in calls]
    assert [p["Event"] for p in payloads] == ["playback.start", "playback.pause", "playback.stop"]
    assert [p["NotificationType"] for p in payloads] == ["PlaybackStart", "PlaybackStart", "PlaybackStop"]


def test_webhook_dispatch_reaches_bingebase(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.bingebase import sink as sink_mod
    from providers.webhooks.dispatch import dispatch_scrobble

    calls: list[dict[str, Any]] = []

    def fake_post(self: Any, url: str, **kwargs: Any) -> _Resp:
        calls.append({"url": url, **kwargs})
        return _Resp(200, {})

    monkeypatch.setattr(sink_mod.requests.Session, "post", fake_post)
    cfg = {
        "plex": {"account_token": "source-token"},
        "bingebase": {"webhook_url": WEBHOOK_URL, "api_key": "bearer-secret"},
        "scrobble": {"webhook": {"sinks": ["bingebase"]}},
    }

    res = dispatch_scrobble("plex", "/start", media_type="movie", ids={"tmdb": "550"}, title="Fight Club", progress=10, cfg=cfg)

    assert res.status_code == 200
    assert res.json()["targets"][0]["target"] == "bingebase"
    assert calls and calls[0]["json"]["Item"]["ProviderIds"]["Tmdb"] == "550"
    assert calls[0]["headers"]["Authorization"] == "Bearer bearer-secret"


def test_watcher_dispatch_reaches_bingebase(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.bingebase import sink as sink_mod
    from providers.scrobble.scrobble import Dispatcher
    from providers.scrobble.watch_manager import _make_sink

    calls: list[dict[str, Any]] = []

    def fake_post(self: Any, url: str, **kwargs: Any) -> _Resp:
        calls.append({"url": url, **kwargs})
        return _Resp(200, {})

    monkeypatch.setattr(sink_mod.requests.Session, "post", fake_post)
    sink = _make_sink("bingebase", lambda: dict(CFG), "default")
    dispatcher = Dispatcher([sink], cfg_provider=lambda: dict(CFG))

    assert dispatcher.dispatch(Event(action="start")) is True
    assert calls and calls[0]["url"] == WEBHOOK_URL


def test_successful_stop_records_recent_scrobble_activity(sink, monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.bingebase import sink as sink_mod

    s, _calls = sink
    activity: list[dict[str, Any]] = []
    monkeypatch.setattr(sink_mod, "record_scrobble_event", lambda ev, **kwargs: activity.append(kwargs))

    s.send(Event(action="start", progress=86))
    s.send(Event(action="stop", progress=97))

    assert len(activity) == 1
    assert activity[0]["source"] == "watcher"
    assert activity[0]["source_instance"] == "default"
    assert activity[0]["target"] == "bingebase"
    assert activity[0]["target_instance"] == "default"
    assert activity[0]["progress"] == 97.0


def test_failed_stop_does_not_record_recent_scrobble_activity(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.bingebase import sink as sink_mod
    from providers.scrobble.bingebase.sink import BingeBaseSink

    activity: list[dict[str, Any]] = []
    s = BingeBaseSink(cfg_provider=lambda: dict(CFG))
    monkeypatch.setattr(s.session, "post", lambda url, **kwargs: _Resp(500, {}))
    monkeypatch.setattr(sink_mod, "record_scrobble_event", lambda ev, **kwargs: activity.append(kwargs))

    s.send(Event(action="stop", progress=97))

    assert activity == []


def test_secret_url_redaction(monkeypatch: pytest.MonkeyPatch) -> None:
    from cw_platform.config_base import redact_config
    from providers.scrobble.bingebase import sink as sink_mod
    from providers.scrobble.bingebase.sink import BingeBaseSink, _is_kodi_webhook, _redact_url

    logs: list[str] = []
    sink = BingeBaseSink(cfg_provider=lambda: dict(CFG))
    monkeypatch.setattr(sink_mod, "BASE_LOG", lambda msg, **kw: logs.append(str(msg)))
    monkeypatch.setattr(sink.session, "post", lambda url, **kw: _Resp(500, {"error": "nope"}))

    sink.send(Event(action="stop"))

    assert _redact_url(WEBHOOK_URL) == "https://bingebase.com/api/webhooks/jellyfin"
    assert _redact_url(KODI_WEBHOOK_URL) == "https://bingebase.com/webhooks/kodi"
    assert _redact_url("https://evilbingebase.com/webhooks/kodi/device-access") == "https://evilbingebase.com/webhooks/kodi"
    assert _is_kodi_webhook(KODI_WEBHOOK_URL) is True
    assert _is_kodi_webhook("https://evilbingebase.com/webhooks/kodi/device-access") is False
    assert "secret-token" not in "\n".join(logs)
    assert "device-access" not in _redact_url(KODI_WEBHOOK_URL)
    assert "token=" not in "\n".join(logs)
    redacted = json.dumps(redact_config({"bingebase": {"webhook_url": WEBHOOK_URL, "api_key": "bearer-secret"}}))
    assert "secret-token" not in redacted
    assert "bearer-secret" not in redacted
