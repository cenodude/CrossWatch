from fastapi import FastAPI

from api import wallAPI
from cw_platform.orchestrator._state_store import StateStore
import services.watchlist as watchlist_service


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

    endpoint = next(route.endpoint for route in app.routes if getattr(route, "path", "") == "/api/state/wall")
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

    endpoint = next(route.endpoint for route in app.routes if getattr(route, "path", "") == "/api/state/wall")
    data = endpoint(both_only=False, active_only=False, limit=20)

    assert data["total"] == 1
    assert data["items"][0]["tmdb"] == 550
    assert data["last_sync_epoch"] == 123
