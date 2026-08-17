from fastapi import FastAPI
from fastapi.routing import APIRoute
from typing import cast

from api import wallAPI
from cw_platform.orchestrator._state_store import StateStore
import cw_platform.provider_instances as provider_instances
import services.watchlist as watchlist_service

BOB_PROFILE_ID = "22222222222242228222222222222222"


def test_wall_total_counts_filtered_items_before_limit(monkeypatch):
    app = FastAPI()
    wallAPI.register_wall(app)
    wallAPI._WALL_CACHE.clear()
    wallAPI._WALL_CACHE.update({"key": None, "data": None})

    monkeypatch.setattr(wallAPI, "load_config", lambda: {"tmdb": {"api_key": "tmdb-key"}})
    monkeypatch.setattr(wallAPI, "_load_state", lambda: {"last_sync_epoch": 123})
    monkeypatch.setattr(wallAPI.sqlite_state, "fingerprint", lambda *_args, **_kwargs: ("state", 1))
    monkeypatch.setattr(wallAPI.sqlite_manual_policy, "fingerprint", lambda *_args, **_kwargs: ("manual", 1))
    monkeypatch.setattr(wallAPI.sqlite_watchlist_hide, "fingerprint", lambda *_args, **_kwargs: ("hidden", 1))
    monkeypatch.setattr(wallAPI, "config_path", lambda: "config.json")
    monkeypatch.setattr(
        wallAPI,
        "build_watchlist",
        lambda _state, tmdb_ok: [
            {"key": "one", "status": "both"},
            {"key": "two", "status": "both"},
            {"key": "three", "status": "both"},
        ],
    )

    endpoint = next(cast(APIRoute, route).endpoint for route in app.routes if getattr(route, "path", "") == "/api/state/wall")
    data = endpoint(both_only=False, active_only=False, limit=2)

    assert data["ok"] is True
    assert data["total"] == 3
    assert [item["key"] for item in data["items"]] == ["one", "two"]


def test_wall_cache_key_tracks_db_watchlist(monkeypatch, tmp_path):
    monkeypatch.setattr(wallAPI, "CONFIG", tmp_path)
    monkeypatch.setattr(watchlist_service, "CONFIG", tmp_path)
    monkeypatch.setattr(wallAPI, "config_path", lambda: tmp_path / "config.json")

    before = wallAPI._cache_key(both_only=False, active_only=False, limit=20)
    StateStore(tmp_path).save_feature_baseline(
        provider="TRAKT",
        feature="watchlist",
        items={"movie:tmdb:550": {"type": "movie", "title": "Fight Club", "ids": {"tmdb": 550}}},
        last_sync_epoch=123,
    )
    after = wallAPI._cache_key(both_only=False, active_only=False, limit=20)

    assert after != before


def test_wall_endpoint_filters_by_user_profile(monkeypatch):
    app = FastAPI()
    wallAPI.register_wall(app)
    wallAPI._WALL_CACHE.clear()
    wallAPI._WALL_CACHE.update({"key": None, "data": None})
    cfg = {
        "tmdb": {"api_key": "tmdb-key"},
        "plex": {"instances": {"PLEX-P02": {}}},
        "mdblist": {"instances": {"MDBLIST-P01": {}}},
        "user_profiles": {BOB_PROFILE_ID: {"label": "Bob", "instances": {"PLEX": ["PLEX-P02"], "MDBLIST": ["MDBLIST-P01"]}}},
    }
    provider_instances.ensure_provider_instance_uids(cfg)

    monkeypatch.setattr(wallAPI, "load_config", lambda: cfg)
    monkeypatch.setattr(wallAPI, "_load_state", lambda: {"last_sync_epoch": 123})
    monkeypatch.setattr(wallAPI.sqlite_state, "fingerprint", lambda *_args, **_kwargs: ("state", 1))
    monkeypatch.setattr(wallAPI.sqlite_manual_policy, "fingerprint", lambda *_args, **_kwargs: ("manual", 1))
    monkeypatch.setattr(wallAPI.sqlite_watchlist_hide, "fingerprint", lambda *_args, **_kwargs: ("hidden", 1))
    monkeypatch.setattr(wallAPI, "config_path", lambda: "config.json")
    monkeypatch.setattr(
        wallAPI,
        "build_watchlist",
        lambda _state, tmdb_ok: [
            {"key": "alice", "status": "both", "sources_by_provider": {"plex": ["default"]}},
            {
                "key": "bob",
                "status": "both",
                "sources": ["plex", "mdblist", "simkl"],
                "sources_by_provider": {
                    "mdblist": ["MDBLIST-P01"],
                    "plex": ["PLEX-P02"],
                    "simkl": ["default"],
                },
            },
        ],
    )

    endpoint = next(cast(APIRoute, route).endpoint for route in app.routes if getattr(route, "path", "") == "/api/state/wall")
    data = endpoint(both_only=False, active_only=False, limit=20, user_profile=BOB_PROFILE_ID)

    assert data["total"] == 1
    assert data["items"][0]["key"] == "bob"
    assert data["items"][0]["sources"] == ["mdblist", "plex"]
    assert data["items"][0]["sources_by_provider"] == {"mdblist": ["MDBLIST-P01"], "plex": ["PLEX-P02"]}


def test_wall_endpoint_reads_db_watchlist(monkeypatch, tmp_path):
    app = FastAPI()
    wallAPI.register_wall(app)
    wallAPI._WALL_CACHE.clear()
    wallAPI._WALL_CACHE.update({"key": None, "data": None})

    monkeypatch.setattr(wallAPI, "CONFIG", tmp_path)
    monkeypatch.setattr(watchlist_service, "CONFIG", tmp_path)
    monkeypatch.setattr(wallAPI, "load_config", lambda: {"tmdb": {"api_key": "tmdb-key"}})
    monkeypatch.setattr(wallAPI, "config_path", lambda: tmp_path / "config.json")
    StateStore(tmp_path).save_feature_baseline(
        provider="TRAKT",
        feature="watchlist",
        items={"movie:tmdb:550": {"type": "movie", "title": "Fight Club", "ids": {"tmdb": 550}}},
        last_sync_epoch=123,
    )

    endpoint = next(cast(APIRoute, route).endpoint for route in app.routes if getattr(route, "path", "") == "/api/state/wall")
    data = endpoint(both_only=False, active_only=False, limit=20)

    assert data["total"] == 1
    assert data["items"][0]["tmdb"] == 550
    assert data["last_sync_epoch"] == 123
