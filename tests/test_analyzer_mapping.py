import pytest
from fastapi import HTTPException

from services import analyzer as an
from services import analyzer_mapping as mapping


@pytest.fixture
def mapping_case(monkeypatch):
    key = "tmdb:123#s01e02"
    item = dict(type="episode", title="Original", series_title="Original", season=1, episode=2,
                ids={"tmdb": "123"}, watched_at="2026-09-07T12:00:00Z", rating=8)
    pair = dict(id="p1", source="SIMKL", source_instance="alice", target="MDBLIST",
                target_instance="destination", mode="one-way", features={"history": True})
    state = {"providers": {"SIMKL": {"instances": {"alice": {"history": {"baseline": {"items": {key: item}}}}}}}}
    entry = dict(provider="MDBLIST", feature="history", key=key, keys=[key], unresolved=True,
                 target="MDBLIST@destination")
    cfg = {"pairs": [pair]}
    monkeypatch.setattr(an, "_cfg", lambda: cfg)
    monkeypatch.setattr(an, "_cached_analysis", lambda pid: {"attention": {"rows": [entry]}})
    monkeypatch.setattr(an, "_load_state_handles", lambda pid, features: [{"state": state}])
    monkeypatch.setattr(mapping, "request_user", lambda request: None)
    payload = mapping.MappingRequest(pair_id="p1", provider="MDBLIST", feature="history", key=key)
    return payload, pair, state, entry, item


def test_saves_source_profile_preserving_watched_date_and_rating(mapping_case, monkeypatch):
    from api import editorAPI
    payload, pair, state, entry, original = mapping_case
    saved = []
    monkeypatch.setattr(editorAPI, "_require_instance_scope", lambda *args: None)
    monkeypatch.setattr(editorAPI, "_save_policy_manual_batch", lambda edits, **kwargs: saved.append((edits, kwargs)))
    context = mapping.handle_mapping(payload, None)
    assert context["row"]["source"] == "SIMKL"
    assert context["row"]["source_instance"] == "alice"
    assert context["row"]["instance"] == "destination"
    corrected = {**original, "ids": {"tmdb": "456"}, "show_ids": {"tmdb": "456"}, "episode": 3,
                 "watched_at": "do not overwrite", "rating": 1}
    result = mapping.handle_mapping(payload.model_copy(update=dict(action="save", version=context["version"], item=corrected)), None)
    assert result["key"] == "tmdb:456#s01e03"
    edits, metadata = saved[0]
    feature, provider, items, blocks, instance = edits[0]
    assert (feature, provider, instance) == ("history", "SIMKL", "alice")
    assert items[result["key"]]["watched_at"] == original["watched_at"]
    assert items[result["key"]]["rating"] == 8
    assert blocks == [payload.key]
    record = metadata["mappings"][(feature, provider, instance)][result["key"]]
    assert record["origin"] == "analyzer" and record["original_key"] == payload.key
    assert original["episode"] == 2


def test_reverse_direction_uses_actual_source(mapping_case):
    payload, pair, state, entry, item = mapping_case
    pair["mode"] = "two-way"
    state["providers"] = {"MDBLIST": {"instances": {"destination": {"history": {"baseline": {"items": {payload.key: item}}}}}}}
    entry.update(provider="SIMKL", target="SIMKL@alice")
    context = mapping.handle_mapping(payload.model_copy(update={"provider": "SIMKL"}), None)
    assert context["row"]["source"] == "MDBLIST"
    assert context["row"]["source_instance"] == "destination"
    assert context["row"]["provider"] == "SIMKL"


def test_same_provider_instances_keep_direction(mapping_case):
    payload, pair, state, entry, item = mapping_case
    pair["target"] = "SIMKL"
    entry.update(provider="SIMKL", target="SIMKL@destination")
    context = mapping.handle_mapping(payload.model_copy(update={"provider": "SIMKL"}), None)
    assert context["row"]["source_instance"] == "alice"
    assert context["row"]["instance"] == "destination"


@pytest.mark.parametrize("change", ["pair", "item", "pending", "profile"])
def test_changed_or_unavailable_entry_is_not_saved(mapping_case, change):
    payload, pair, state, entry, item = mapping_case
    context = mapping.handle_mapping(payload, None)
    if change == "pair": pair["id"] = "different"
    if change == "item": item["watched_at"] = "later"
    if change == "pending": entry["unresolved"] = False
    if change == "profile": pair["source_instance"] = "bob"
    with pytest.raises(HTTPException) as error:
        mapping.handle_mapping(payload.model_copy(update=dict(action="save", version=context["version"], item=item)), None)
    assert error.value.status_code in {404, 409}


@pytest.mark.parametrize("write,access,status", [(False, True, 403), (True, False, 404)])
def test_permissions_checked_before_reading_item(mapping_case, monkeypatch, write, access, status):
    monkeypatch.setattr(mapping, "request_user", lambda request: {"is_admin": False, "permissions": {"write": write}})
    monkeypatch.setattr(mapping, "user_can_access_pair", lambda *args: access)
    with pytest.raises(HTTPException) as error:
        mapping.handle_mapping(mapping_case[0], None)
    assert error.value.status_code == status


def test_search_and_episodes_use_shared_mapping_services(mapping_case, monkeypatch):
    from services import interactive_sync_mapping, interactive_sync_episodes
    payload = mapping_case[0]
    context = mapping.handle_mapping(payload, None)
    calls = []
    monkeypatch.setattr(interactive_sync_mapping, "search_candidates", lambda cfg, row, q, **kw: calls.append((row, q)) or {"items": []})
    monkeypatch.setattr(interactive_sync_episodes, "suggest_episodes", lambda cfg, edits: calls.append(edits) or {"results": []})
    mapping.handle_mapping(payload.model_copy(update=dict(action="search", version=context["version"], q="Title")), None)
    mapping.handle_mapping(payload.model_copy(update=dict(action="episodes", version=context["version"], item=context["row"]["item"])), None)
    assert calls[0][0]["source_instance"] == "alice"
    assert calls[1][0][0]["id"] == "analyzer-item"


def test_api_save_persists_in_editor_saved_mappings(mapping_case, config_base, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import editorAPI

    monkeypatch.setattr(editorAPI, "_STATE_BASE", config_base)
    monkeypatch.setattr(editorAPI, "load_config", lambda: {})
    app = FastAPI()
    app.include_router(an.router)
    app.include_router(editorAPI.router)
    payload = mapping_case[0].model_dump()
    with TestClient(app) as client:
        response = client.post("/api/analyzer/mapping", json=payload)
        assert response.status_code == 200
        context = response.json()
        corrected = {**context["row"]["item"], "ids": {"tmdb": "456"}, "show_ids": {"tmdb": "456"}}
        response = client.post("/api/analyzer/mapping", json={**payload, "action": "save", "version": context["version"], "item": corrected})
        assert response.status_code == 200
        records = client.get("/api/editor/mappings").json()["items"]
    assert len(records) == 1
    assert records[0]["origin"] == "analyzer"
    assert (records[0]["provider"], records[0]["instance"]) == ("SIMKL", "alice")
    assert records[0]["original"]["ids"] == {"tmdb": "123"}
    assert records[0]["corrected"]["ids"] == {"tmdb": "456"}
