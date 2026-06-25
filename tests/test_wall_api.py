from fastapi import FastAPI

from api import wallAPI


def test_wall_total_counts_filtered_items_before_limit(monkeypatch):
    app = FastAPI()
    wallAPI.register_wall(app)
    wallAPI._WALL_CACHE.clear()
    wallAPI._WALL_CACHE.update({"key": None, "data": None})

    monkeypatch.setattr(wallAPI, "load_config", lambda: {"tmdb": {"api_key": "tmdb-key"}})
    monkeypatch.setattr(wallAPI, "_load_state", lambda: {"last_sync_epoch": 123})
    monkeypatch.setattr(wallAPI, "_peek_state_key", lambda: ("state", 1))
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
