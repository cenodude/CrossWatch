# CrossWatch FlickList provider tests
from __future__ import annotations

import json
from typing import Any

import pytest


class _Resp:
    def __init__(self, status_code: int = 200, payload: Any = None, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = json.dumps(self._payload)
        self.headers: dict[str, str] = dict(headers or {})

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


CFG = {
    "flicklist": {"api_key": "fs_live_test"},
    "scrobble": {"watch": {"watched_at": 90}},
}


def test_sync_manifest_declares_full_provider_features() -> None:
    from providers.sync._mod_FLICKLIST import OPS, get_manifest

    manifest = dict(get_manifest())
    features = dict(manifest["features"])
    caps = dict(manifest["capabilities"])

    assert manifest["name"] == "FLICKLIST"
    assert manifest["version"] == "0.1"
    assert features == {"watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": True}
    assert OPS.features() == features
    assert caps["watchlist"]["write"] is True
    assert caps["ratings"]["remove"] is True
    assert caps["history"]["write"] is True
    assert caps["progress"]["read"] is True
    assert caps["playlists"]["create"] is True
    assert caps["playlists"]["reorder"] is False
    assert caps["scrobble"]["write"] is True


def test_flicklist_is_registered_as_sync_provider() -> None:
    from cw_platform.modules_registry import MODULES

    assert MODULES["SYNC"]["_mod_FLICKLIST"] == "providers.sync._mod_FLICKLIST"


def test_device_code_uses_built_in_public_client_id() -> None:
    from providers.auth import _auth_FLICKLIST as auth

    assert auth.app_client_id({}) == "crosswatch_923f4294"


def test_common_helpers_preserve_flicklist_ids() -> None:
    from providers.sync.flicklist._common import half_point, read_minimal, write_ident

    item = read_minimal({"media_type": "movie", "title": "Fight Club", "ids": {"fldb": "fl_123", "tmdb": 550}})

    assert item["_flicklist_fldb"] == "fl_123"
    assert item["ids"]["tmdb"] == "550"
    assert write_ident(item) == {"ids": {"fldb": "fl_123"}, "media_type": "movie"}
    assert half_point(7.26) == 7.5


def test_episode_write_ident_uses_show_ids() -> None:
    from providers.sync.flicklist._common import write_ident

    item = {
        "type": "episode",
        "title": "Episode",
        "series_title": "Ted Lasso",
        "season": 2,
        "episode": 4,
        "ids": {"tmdb": "3089818", "imdb": "tt14968638", "tvdb": "8554003"},
        "show_ids": {"tmdb": "97546", "imdb": "tt10986410", "tvdb": "383203"},
    }

    assert write_ident(item) == {
        "ids": {"tmdb": 97546, "imdb": "tt10986410", "tvdb": 383203},
        "media_type": "show",
        "season": 2,
        "episode": 4,
    }


def test_episode_write_ident_without_show_ids_is_unresolved() -> None:
    from providers.sync.flicklist._common import write_ident

    item = {
        "type": "episode",
        "season": 1,
        "episode": 1,
        "ids": {"tmdb": "2181581", "imdb": "tt11957006", "tvdb": "8444132"},
    }

    assert write_ident(item) is None


def test_history_add_sends_episode_payload_with_show_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.flicklist import _history as history

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(history, "flicklist_request", lambda adapter, method, url, **kwargs: calls.append({"method": method, "url": url, **kwargs}) or _Resp(200, {"added": 1}))

    item = {
        "type": "episode",
        "season": 1,
        "episode": 1,
        "watched_at": "2026-08-21T20:00:00.000Z",
        "ids": {"tmdb": "2181581", "imdb": "tt11957006", "tvdb": "8444132"},
        "show_ids": {"tmdb": "100088", "imdb": "tt3581920", "tvdb": "392256"},
    }

    res = history.add(object(), [item])

    assert res["count"] == 1
    assert calls[0]["json"]["items"] == [{
        "ids": {"tmdb": 100088, "imdb": "tt3581920", "tvdb": 392256},
        "media_type": "show",
        "season": 1,
        "episode": 1,
        "watched_at": "2026-08-21T20:00:00.000Z",
    }]


def test_history_add_rejects_success_status_without_write_counters(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.flicklist import _history as history

    monkeypatch.setattr(history, "flicklist_request", lambda adapter, method, url, **kwargs: _Resp(200, {"ok": True}))

    res = history.add(object(), [{
        "type": "movie",
        "title": "Fight Club",
        "watched_at": "2026-08-21T20:00:00.000Z",
        "ids": {"tmdb": "550"},
    }])

    assert res["ok"] is False
    assert res["count"] == 0
    assert res["unresolved"][0]["status"] == "invalid_response"


def test_history_add_uses_configured_flicklist_write_batch_size(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.flicklist import _history as history

    class Adapter:
        config = {"flicklist": {"write_batch_size": 100}}

    sizes: list[int] = []

    def fake_request(adapter: Any, method: str, url: str, **kwargs: Any) -> _Resp:
        items = list((kwargs.get("json") or {}).get("items") or [])
        sizes.append(len(items))
        return _Resp(200, {"added": len(items), "not_found": []})

    monkeypatch.setattr(history, "flicklist_request", fake_request)

    items = [
        {
            "type": "movie",
            "title": f"Movie {idx}",
            "watched_at": "2026-08-21T20:00:00.000Z",
            "ids": {"tmdb": str(100000 + idx)},
        }
        for idx in range(205)
    ]

    res = history.add(Adapter(), items)

    assert sizes == [100, 100, 5]
    assert res["count"] == 205


def test_ratings_and_progress_episode_payloads_use_show_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.flicklist import _progress as progress
    from providers.sync.flicklist import _ratings as ratings

    calls: list[dict[str, Any]] = []

    def fake_request(adapter: Any, method: str, url: str, **kwargs: Any) -> _Resp:
        calls.append({"method": method, "url": url, **kwargs})
        return _Resp(200, {"added": 1})

    monkeypatch.setattr(ratings, "flicklist_request", fake_request)
    monkeypatch.setattr(progress, "flicklist_request", fake_request)

    item = {
        "type": "episode",
        "season": 2,
        "episode": 4,
        "rating": 8,
        "progress_percent": 42.5,
        "ids": {"tmdb": "3089818", "imdb": "tt14968638", "tvdb": "8554003"},
        "show_ids": {"tmdb": "97546", "imdb": "tt10986410", "tvdb": "383203"},
    }

    assert ratings.add(object(), [item])["count"] == 1
    assert progress.add(object(), [item])["count"] == 1
    assert calls[0]["json"]["items"] == [{
        "ids": {"tmdb": 97546, "imdb": "tt10986410", "tvdb": 383203},
        "media_type": "show",
        "season": 2,
        "episode": 4,
        "rating": 8.0,
    }]
    assert calls[1]["json"] == {
        "ids": {"tmdb": 97546, "imdb": "tt10986410", "tvdb": 383203},
        "media_type": "show",
        "season": 2,
        "episode": 4,
        "progress": 42.5,
    }


def test_watchlist_rejects_episode_items_before_write(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.flicklist import _watchlist as watchlist

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(watchlist, "flicklist_request", lambda adapter, method, url, **kwargs: calls.append(kwargs) or _Resp(200, {}))

    item = {
        "type": "episode",
        "season": 1,
        "episode": 1,
        "ids": {"tmdb": "2181581"},
        "show_ids": {"tmdb": "100088"},
    }

    res = watchlist.add(object(), [item])

    assert calls == []
    assert res["count"] == 0
    assert res["unresolved"][0]["status"] == "unsupported_media_type"


def test_playlist_episode_payload_uses_show_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.flicklist import _playlists as playlists

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(playlists, "flicklist_request", lambda adapter, method, url, **kwargs: calls.append({"method": method, "url": url, **kwargs}) or _Resp(200, {"added": 1}))

    item = {
        "type": "episode",
        "season": 1,
        "episode": 3,
        "ids": {"tmdb": "4071040"},
        "show_ids": {"tmdb": "100088", "imdb": "tt3581920"},
    }

    res = playlists.add(object(), "42", [item])

    assert res["count"] == 1
    assert calls[0]["url"].endswith("/sync/lists/42/items")
    assert calls[0]["json"]["items"] == [{
        "ids": {"tmdb": 100088, "imdb": "tt3581920"},
        "media_type": "show",
        "season": 1,
        "episode": 3,
    }]


def test_sink_is_registered_for_webhook_and_watcher_destinations() -> None:
    from providers.scrobble.flicklist.sink import FlickListSink
    from providers.scrobble.routes import ROUTE_RATING_SINKS, ROUTE_SINKS, build_route_cfg, normalize_route
    from providers.scrobble.watch_manager import _make_sink
    from providers.webhooks.config import sink_configured, webhook_sinks
    from providers.webhooks.dispatch import _make_sink as make_webhook_sink

    route = normalize_route({"id": "R1", "provider": "plex", "sink": "flicklist"}, "R1")
    view = build_route_cfg(CFG, route)

    assert "flicklist" in ROUTE_SINKS
    assert "flicklist" in ROUTE_RATING_SINKS
    assert route["sink"] == "flicklist"
    assert view["flicklist"]["api_key"] == "fs_live_test"
    assert sink_configured(CFG, "flicklist", "default") is True
    assert sink_configured({"flicklist": {"token": "session-token"}}, "flicklist", "default") is True
    assert isinstance(_make_sink("flicklist", lambda: dict(CFG), "default"), FlickListSink)
    assert isinstance(make_webhook_sink("flicklist", "default", lambda: dict(CFG)), FlickListSink)

    cfg = {**CFG, "scrobble": {"webhook": {"sinks": ["flicklist"]}}}
    assert "flicklist" in webhook_sinks(cfg, "plex", "default")


def test_movie_scrobble_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.flicklist import sink as sink_mod
    from providers.scrobble.flicklist.sink import FlickListSink

    calls: list[dict[str, Any]] = []

    def fake_request(adapter: Any, method: str, url: str, **kwargs: Any) -> _Resp:
        calls.append({"method": method, "url": url, **kwargs})
        return _Resp(204, {})

    monkeypatch.setattr(sink_mod, "flicklist_request", fake_request)
    monkeypatch.setattr(sink_mod, "record_watch", lambda *args, **kwargs: None)

    FlickListSink(cfg_provider=lambda: dict(CFG)).send(Event(action="start", progress=12.4))

    assert calls[0]["method"] == "POST"
    assert calls[0]["url"].endswith("/scrobble/start")
    assert calls[0]["json"] == {"ids": {"tmdb": 550, "imdb": "tt0137523"}, "media_type": "movie", "progress": 12.4}


def test_episode_scrobble_uses_show_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.flicklist import sink as sink_mod
    from providers.scrobble.flicklist.sink import FlickListSink

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(sink_mod, "flicklist_request", lambda adapter, method, url, **kwargs: calls.append({"method": method, "url": url, **kwargs}) or _Resp(204, {}))
    monkeypatch.setattr(sink_mod, "record_watch", lambda *args, **kwargs: None)

    FlickListSink(cfg_provider=lambda: dict(CFG)).send(Event(
        action="pause",
        media_type="episode",
        title="Severance",
        season=1,
        number=2,
        ids={"tmdb": "5978363", "imdb": "tt11280740", "tmdb_show": "95396", "imdb_show": "tt11280740"},
        progress=45,
    ))

    assert calls[0]["url"].endswith("/scrobble/pause")
    assert calls[0]["json"] == {
        "ids": {"tmdb": 95396, "imdb": "tt11280740"},
        "media_type": "show",
        "season": 1,
        "episode": 2,
        "progress": 45.0,
    }


def test_episode_scrobble_without_show_identity_is_not_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.flicklist import sink as sink_mod
    from providers.scrobble.flicklist.sink import FlickListSink

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(sink_mod, "flicklist_request", lambda adapter, method, url, **kwargs: calls.append({"method": method, "url": url, **kwargs}) or _Resp(200, {}))

    FlickListSink(cfg_provider=lambda: dict(CFG)).send(Event(
        action="start",
        media_type="episode",
        title="Severance",
        season=1,
        number=2,
        ids={"tmdb": "5978363", "imdb": "tt11280740"},
    ))

    assert calls == []


def test_scrobble_sink_accepts_route_cfg_override(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.flicklist import sink as sink_mod
    from providers.scrobble.flicklist.sink import FlickListSink

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(sink_mod, "flicklist_request", lambda adapter, method, url, **kwargs: calls.append({"method": method, "url": url, "cfg": adapter.config, **kwargs}) or _Resp(200, {}))
    monkeypatch.setattr(sink_mod, "record_watch", lambda *args, **kwargs: None)

    route_cfg = {
        "flicklist": {"api_key": "fs_live_route"},
        "scrobble": {"watch": {"route_provider": "plex", "route_provider_instance": "P1"}},
    }

    FlickListSink(cfg_provider=lambda: {}).send(Event(action="start"), cfg=route_cfg)

    assert calls
    assert calls[0]["cfg"]["flicklist"]["api_key"] == "fs_live_route"


def test_global_and_route_rating_surfaces_include_flicklist() -> None:
    from pathlib import Path

    import providers.webhooks.plex as wh
    from providers.scrobble.plex.ratings_sync import OPS_RATING_SINKS, RATING_SINKS

    root = Path(__file__).resolve().parents[1]
    watch = (root / "providers" / "scrobble" / "plex" / "watch.py").read_text(encoding="utf-8")
    api = (root / "api" / "scrobblerManagementAPI.py").read_text(encoding="utf-8")
    webhook_modal = (root / "assets" / "js" / "modals" / "scrobbler-webhook" / "index.js").read_text(encoding="utf-8")

    assert 'enable_flicklist = "flicklist" in custom_targets' in watch
    assert 'enable_flicklist = bool(watch_cfg.get("plex_flicklist_ratings"))' in watch
    assert '"flicklist": bool(watch.get("plex_flicklist_ratings"))' in api
    assert '"flicklist"' in webhook_modal.split("ratingSinks")[1].split("]")[0]
    assert "plex_flicklist_ratings" in wh._DEF_WEBHOOK
    assert "flicklist" in OPS_RATING_SINKS
    assert "flicklist" in RATING_SINKS
    assert "flicklist" in wh.RATING_SINKS


def test_history_index_uses_flicklist_header_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.flicklist import _history as history

    class Adapter:
        config = {"flicklist": {"history_per_page": 100, "history_max_pages": 10}}

    calls: list[dict[str, Any]] = []

    def fake_request(adapter: Any, method: str, url: str, **kwargs: Any) -> _Resp:
        calls.append(kwargs)
        page = int((kwargs.get("params") or {}).get("page") or 1)
        payload = [
            {
                "media_type": "movie",
                "title": f"Movie {page}",
                "year": 2026,
                "watched_at": f"2026-08-2{page}T20:00:00.000Z",
                "ids": {"tmdb": 1000 + page},
            }
        ]
        return _Resp(200, payload, headers={"X-FlickList-Page-Count": "2"})

    monkeypatch.setattr(history, "flicklist_request", fake_request)

    out = history.build_index(Adapter())

    assert len(calls) == 3
    assert [call["params"]["page"] for call in calls[:2]] == [1, 2]
    assert sorted(out) == ["tmdb:1001@1787342400", "tmdb:1002@1787428800"]


def test_history_index_supplements_missing_movies_from_watched_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.flicklist import _history as history
    from providers.sync.flicklist._common import URL_HISTORY, URL_WATCHED_MOVIES

    class Adapter:
        config = {"flicklist": {"history_per_page": 100, "history_max_pages": 10}}

    calls: list[str] = []

    def fake_request(adapter: Any, method: str, url: str, **kwargs: Any) -> _Resp:
        calls.append(url)
        if url == URL_HISTORY:
            return _Resp(200, [
                {
                    "id": 42,
                    "type": "episode",
                    "title": "Severance",
                    "episode_name": "Goodbye, Mrs. Selvig",
                    "season_number": 1,
                    "episode_number": 2,
                    "watched_at": "2026-08-21T20:00:00.000Z",
                    "ids": {"tmdb": 95396, "imdb": "tt11280740"},
                }
            ], headers={"X-FlickList-Page-Count": "1"})
        if url == URL_WATCHED_MOVIES:
            return _Resp(200, [
                {
                    "title": "Fight Club",
                    "year": 1999,
                    "plays": 1,
                    "last_watched_at": "2026-08-20T20:00:00.000Z",
                    "ids": {"tmdb": 550, "imdb": "tt0137523"},
                }
            ])
        return _Resp(404, {})

    monkeypatch.setattr(history, "flicklist_request", fake_request)

    out = history.build_index(Adapter())

    assert calls == [URL_HISTORY, URL_WATCHED_MOVIES]
    assert "tmdb:95396#s01e02@1787342400" in out
    assert out["tmdb:550"]["type"] == "movie"
    assert out["tmdb:550"]["watched_at"] == "2026-08-20T20:00:00.000Z"


def test_history_index_supplements_movie_only_accounts(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.flicklist import _history as history
    from providers.sync.flicklist._common import URL_HISTORY, URL_WATCHED_MOVIES, URL_WATCHED_SHOWS

    calls: list[str] = []

    def fake_request(adapter: Any, method: str, url: str, **kwargs: Any) -> _Resp:
        calls.append(url)
        if url == URL_HISTORY:
            return _Resp(200, [], headers={"X-FlickList-Page-Count": "1"})
        if url == URL_WATCHED_MOVIES:
            return _Resp(200, [{"title": "Heat", "year": 1995, "ids": {"tmdb": 949}}])
        if url == URL_WATCHED_SHOWS:
            return _Resp(200, [])
        return _Resp(404, {})

    monkeypatch.setattr(history, "flicklist_request", fake_request)

    out = history.build_index(object())

    assert calls == [URL_HISTORY, URL_WATCHED_MOVIES, URL_WATCHED_SHOWS]
    assert sorted(out) == ["tmdb:949"]


def test_history_index_supplements_empty_history_from_watched_show_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.flicklist import _history as history
    from providers.sync.flicklist._common import URL_HISTORY, URL_WATCHED_MOVIES, URL_WATCHED_SHOWS

    calls: list[str] = []

    def fake_request(adapter: Any, method: str, url: str, **kwargs: Any) -> _Resp:
        calls.append(url)
        if url == URL_HISTORY:
            return _Resp(200, [], headers={"X-FlickList-Page-Count": "1"})
        if url == URL_WATCHED_MOVIES:
            return _Resp(200, [])
        if url == URL_WATCHED_SHOWS:
            return _Resp(200, [
                {
                    "show": {
                        "title": "Severance",
                        "year": 2022,
                        "ids": {"tmdb": 95396, "imdb": "tt11280740", "tvdb": 371980},
                    },
                    "last_watched_at": "2026-08-21T20:00:00.000Z",
                    "seasons": [
                        {
                            "number": 1,
                            "episodes": [
                                {"number": 1, "plays": 1, "last_watched_at": "2026-08-20T20:00:00.000Z"},
                                {"number": 2, "plays": 1, "last_watched_at": "2026-08-21T20:00:00.000Z"},
                            ],
                        }
                    ],
                }
            ])
        return _Resp(404, {})

    monkeypatch.setattr(history, "flicklist_request", fake_request)

    out = history.build_index(object())

    assert calls == [URL_HISTORY, URL_WATCHED_MOVIES, URL_WATCHED_SHOWS]
    assert sorted(out) == ["tmdb:95396#s01e01", "tmdb:95396#s01e02"]
    assert out["tmdb:95396#s01e02"]["watched_at"] == "2026-08-21T20:00:00.000Z"
    assert out["tmdb:95396#s01e02"]["show_ids"]["tmdb"] == "95396"


def test_playback_progress_service_registers_flicklist() -> None:
    from services.playback_progress.service import PHASE1_PROVIDERS, PlaybackProgressService

    cfg = {
        "flicklist": {
            "instances": {
                "kid": {"access_token": "session-token", "username": "kid"},
            }
        }
    }

    service = PlaybackProgressService()
    specs = [spec for spec in service.provider_instances(cfg) if spec["provider"] == "flicklist"]

    assert "flicklist" in PHASE1_PROVIDERS
    assert service._adapter("flicklist") is not None
    assert specs == [{"provider": "flicklist", "instance_id": "kid", "instance_label": "flicklist kid"}]


class _FakeFlickListOps:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def is_configured(self, cfg: dict[str, Any]) -> bool:
        return bool((cfg.get("flicklist") or {}).get("access_token") or (cfg.get("flicklist") or {}).get("api_key"))

    def capabilities(self) -> dict[str, Any]:
        return {
            "progress": {
                "read": True,
                "remove": True,
                "upsert": True,
                "types": {"movies": True, "episodes": True},
            },
            "history": {"write": True},
        }

    def build_index(self, cfg: dict[str, Any], *, feature: str) -> dict[str, dict[str, Any]]:
        assert feature == "progress"
        return {
            "tmdb:95396:s1e2": {
                "type": "episode",
                "title": "Goodbye, Mrs. Selvig",
                "series_title": "Severance",
                "season": 1,
                "episode": 2,
                "ids": {"tmdb": "95396", "imdb": "tt11280740"},
                "show_ids": {"tmdb": "95396", "imdb": "tt11280740"},
                "progress_percent": 45,
                "progress_at": "2026-08-22T20:00:00Z",
                "updated_at": "2026-08-22T20:00:00Z",
                "_flicklist_playback_id": "4471",
            }
        }

    def add(self, cfg: dict[str, Any], items: list[dict[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        self.calls.append({"op": "add", "feature": feature, "items": items, "dry_run": dry_run})
        return {"ok": True, "confirmed_keys": ["tmdb:95396:s1e2"]}

    def remove(self, cfg: dict[str, Any], items: list[dict[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        self.calls.append({"op": "remove", "feature": feature, "items": items, "dry_run": dry_run})
        return {"ok": True, "confirmed_keys": ["tmdb:95396:s1e2"]}


def test_flicklist_playback_adapter_lists_and_routes_actions() -> None:
    from services.playback_progress.adapters.flicklist import FlickListPlaybackAdapter

    fake = _FakeFlickListOps()
    adapter = FlickListPlaybackAdapter()
    adapter.ops = fake
    cfg = {"flicklist": {"access_token": "session-token"}}

    listed = adapter.list_progress(cfg, instance_id="default", instance_label="Default")
    assert listed.ok is True
    record = listed.items[0].to_dict()

    assert record["provider"] == "flicklist"
    assert record["remote_id"] == "4471"
    assert record["media_type"] == "episode"
    assert record["can_remove_progress"] is True
    assert record["can_mark_watched"] is True
    assert record["can_update_progress"] is True
    assert record["provider_metadata"]["show_ids"] == {"tmdb": "95396", "imdb": "tt11280740"}

    assert adapter.update_progress(cfg, record, 25, instance_id="default", instance_label="Default").ok is True
    assert adapter.mark_watched(cfg, record, instance_id="default", instance_label="Default").ok is True
    assert adapter.remove_progress(cfg, record, instance_id="default", instance_label="Default").ok is True

    assert fake.calls[0]["op"] == "add"
    assert fake.calls[0]["feature"] == "progress"
    assert fake.calls[0]["items"][0]["progress_percent"] == 25
    assert fake.calls[1]["op"] == "add"
    assert fake.calls[1]["feature"] == "history"
    assert fake.calls[2]["op"] == "remove"
    assert fake.calls[2]["feature"] == "progress"
    assert fake.calls[2]["items"][0]["_flicklist_playback_id"] == "4471"


def test_history_add_does_not_confirm_items_echoed_in_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.flicklist import _history as history

    body = {
        "added": 0,
        "existing": 0,
        "not_found": [
            {"ids": {"fldb": None, "tmdb": 550, "imdb": "tt0137523", "tvdb": None}, "reason": "no_catalog_match"},
        ],
    }
    monkeypatch.setattr(history, "flicklist_request", lambda adapter, method, url, **kwargs: _Resp(200, body))

    res = history.add(object(), [{
        "type": "movie",
        "title": "Fight Club",
        "watched_at": "2026-08-21T20:00:00.000Z",
        "ids": {"tmdb": "550", "imdb": "tt0137523"},
    }])

    assert res["ok"] is False
    assert res["count"] == 0
    assert res["confirmed_keys"] == []
    assert res["unresolved"][0]["status"] == "not_found"


def test_history_add_does_not_confirm_when_nothing_was_applied(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.flicklist import _history as history

    monkeypatch.setattr(history, "flicklist_request", lambda adapter, method, url, **kwargs: _Resp(200, {"added": 0, "existing": 0, "not_found": []}))

    res = history.add(object(), [{
        "type": "movie",
        "title": "Fight Club",
        "watched_at": "2026-08-21T20:00:00.000Z",
        "ids": {"tmdb": "550"},
    }])

    assert res["ok"] is False
    assert res["count"] == 0
    assert res["unresolved"][0]["status"] == "write_not_applied"


def test_history_add_counts_existing_plays_as_confirmed(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.flicklist import _history as history

    monkeypatch.setattr(history, "flicklist_request", lambda adapter, method, url, **kwargs: _Resp(200, {"added": 0, "existing": 1, "not_found": []}))

    res = history.add(object(), [{
        "type": "movie",
        "title": "Fight Club",
        "watched_at": "2026-08-21T20:00:00.000Z",
        "ids": {"tmdb": "550"},
    }])

    assert res["ok"] is True
    assert res["count"] == 1
