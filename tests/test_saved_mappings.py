# tests/test_saved_mappings.py
# CrossWatch - saved correction history and visibility
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from copy import deepcopy

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from test_interactive_sync import item


def test_saved_mappings_is_registered_in_full_api_with_legacy_overrides(config_base, monkeypatch):
    from api import register, editorAPI

    monkeypatch.setattr(editorAPI, "_STATE_BASE", config_base)
    monkeypatch.setattr(editorAPI, "load_config", lambda: {})
    editorAPI._save_policy_manual("history", "SIMKL", {"imdb:tt0000001": item(1)}, [])
    app = FastAPI()
    register(app, lambda: {})
    with TestClient(app) as client:
        response = client.get("/api/editor/mappings?q=&feature=history&offset=0&limit=50&user_profile=")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["items"][0]["original"] is None
    assert data["items"][0]["corrected"]["ids"] == item(1)["ids"]


def test_saved_mapping_view_survives_reload_and_filters_scope(config_base, monkeypatch):
    from api import editorAPI as api
    from cw_platform.local_db import manual_policy

    monkeypatch.setattr(api, "_STATE_BASE", config_base)
    monkeypatch.setattr(api, "load_config", lambda: {})
    original, corrected = item(1), item(2)
    key = next(iter(api._canonicalize_manual_items({"key": corrected}, "history")))
    record = dict(original_key="imdb:tt0000001", original=original, origin="interactive_sync", saved_at=100)
    api._save_policy_manual_batch([
        ("history", "SIMKL", {key: corrected}, ["imdb:tt0000001"], "default"),
        ("history", "SIMKL", {"old": item(3)}, [], "private"),
        ("ratings", "TRAKT", {"legacy": item(4)}, [], "default"),
    ], mappings={("history", "SIMKL", "default"): {key: record}})
    policy = manual_policy.load_policy(config_base)
    assert policy["providers"]["SIMKL"]["history"]["mappings"][key] == record
    # Backup merges and subsequent policy writes retain the original comparison.
    merged = api._merge_policy({"version": 1, "providers": {}}, deepcopy(policy), "merge")
    manual_policy.save_policy(config_base, merged)
    app = FastAPI()

    @app.middleware("http")
    async def user(request: Request, call_next):
        request.state.user = dict(id="alice", permissions=dict(write=True))
        return await call_next(request)

    app.include_router(api.router)
    monkeypatch.setattr(api, "user_can_access_instance", lambda cfg, user, provider, instance: instance != "private")
    with TestClient(app) as client:
        data = client.get("/api/editor/mappings").json()
        assert data["total"] == 2
        assert all(source["instance"] != "private" for source in data["sources"])
        assert data["items"][0]["original"] == original
        assert data["items"][0]["corrected"]["ids"] == corrected["ids"]
        assert data["items"][1]["original"] is None
        assert client.get("/api/editor/mappings?q=tt0000001").json()["total"] == 1
        assert client.get("/api/editor/mappings?feature=ratings").json()["total"] == 1
        assert client.get("/api/editor/mappings?instance=private").json()["total"] == 0
        assert len(client.get("/api/editor/mappings?limit=1&offset=1").json()["items"]) == 1
        from cw_platform import access_policy
        monkeypatch.setattr(access_policy, "profile_instances_map", lambda cfg, profile: {"TRAKT": ["default"]})
        scoped = client.get("/api/editor/mappings?user_profile=alice").json()
        assert scoped["total"] == 1
        assert scoped["sources"] == [dict(provider="TRAKT", instance="default")]
        assert scoped["items"][0]["provider"] == "TRAKT"


def test_editor_saves_original_from_its_own_source(config_base, monkeypatch):
    from api import editorAPI as api
    from services.saved_mappings import saved_corrections

    monkeypatch.setattr(api, "_STATE_BASE", config_base)
    monkeypatch.setattr(api, "load_config", lambda: {})
    monkeypatch.setattr(api, "_load_state_items", lambda *a: {"imdb:tt0000001": item(1)})
    api.api_editor_save_state(dict(source="state", kind="watchlist", provider="SIMKL", provider_instance="default",
                                  items={"imdb:tt0000002": item(2)}, blocks=["imdb:tt0000001"],
                                  mapping_originals={"imdb:tt0000002": "imdb:tt0000001"}), request=None)
    row = next(saved_corrections(api._load_policy()))
    assert row["origin"] == "editor"
    assert row["original"]["ids"] == item(1)["ids"]
    assert row["corrected"]["ids"] == item(2)["ids"]
