# tests/test_episode_groups_api.py
# CrossWatch - History group API validation and access checks
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from copy import deepcopy

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from cw_platform.id_map import canonical_key
from cw_platform.local_db import manual_policy
from test_episode_groups import group, watched
from test_orchestrator_dry_run_no_side_effects import _cfg


@pytest.fixture
def client(config_base, monkeypatch):
    from api import editorAPI

    cfg = _cfg(False)
    cfg["pairs"][0]["features"] = {"history": {"enable": True}}
    monkeypatch.setattr(editorAPI, "load_config", lambda: cfg)
    monkeypatch.setattr(editorAPI, "_STATE_BASE", config_base)
    app = FastAPI()
    app.include_router(editorAPI.router)
    with TestClient(app) as client:
        yield client, editorAPI, cfg


def test_group_save_list_edit_delete_and_backup_roundtrip(client, config_base):
    client, api, _ = client
    payload = dict(pair_id="p1", group=group())
    assert client.post("/api/editor/episode-groups", json=payload).status_code == 200
    rows = client.get("/api/editor/episode-groups").json()["pairs"]
    assert rows[0]["groups"] == [group()]
    payload["group"]["name"] = "Updated"
    payload["group"]["scrobble"] = True
    assert client.post("/api/editor/episode-groups", json=payload).status_code == 200
    policy = api._load_policy()
    assert policy["pairs"]["p1"]["episode_groups"][0]["scrobble"] is True
    for mode in ("merge", "replace"):
        manual_policy.save_policy(config_base, api._merge_policy({}, deepcopy(policy), mode))
        assert api._load_policy() == policy
    assert client.post("/api/editor/episode-groups", json=dict(pair_id="p1", action="delete", group_id="finale")).status_code == 200
    assert client.get("/api/editor/episode-groups").json()["pairs"][0]["groups"] == []
    assert not api._load_policy()["providers"]


@pytest.mark.parametrize("bad", ["pair", "instance", "provider", "overlap", "episode", "id", "bool", "shape"])
def test_reject_invalid_groups_without_changing_policy(client, bad):
    client, api, cfg = client
    assert client.post("/api/editor/episode-groups", json=dict(pair_id="p1", group=group())).status_code == 200
    before = deepcopy(api._load_policy())
    value = {**group(), "id": "new"}
    pair = "p1"
    if bad == "pair": pair = "missing"
    if bad == "instance": value["target"]["instance"] = "other"
    if bad == "provider": value["target"]["provider"] = "OTHER"
    if bad == "episode": value["source"]["episodes"][0]["episode"] = 0
    if bad == "bool": value["source"]["episodes"][0]["season"] = True
    if bad == "id": value["source"]["episodes"][0]["show_ids"] = {"bogus": "12"}
    if bad == "shape": value["source"]["episodes"].pop()
    response = client.post("/api/editor/episode-groups", json=dict(pair_id=pair, group=value))
    assert response.status_code in (400, 404, 422), response.text
    assert api._load_policy() == before


@pytest.mark.parametrize("denied", ["pair", "instance", "write"])
def test_group_access_checks_cover_both_endpoints(client, monkeypatch, denied):
    from services import episode_groups

    client, api, _ = client
    if denied == "pair":
        monkeypatch.setattr(episode_groups, "user_can_access_pair", lambda *_a: False)
    elif denied == "instance":
        def require(_cfg, _request, provider, instance):
            if provider == "DST":
                raise HTTPException(403, "Denied")
        monkeypatch.setattr(api, "_require_instance_scope", require)
    else:
        monkeypatch.setattr(episode_groups, "request_user", lambda *_a: {"permissions": {"write": False}})
    response = client.post("/api/editor/episode-groups", json=dict(pair_id="p1", group=group()))
    assert response.status_code in (403, 404)
    response = client.get("/api/editor/episode-groups")
    assert response.status_code == 403 or response.json()["pairs"] == []
    assert not api._load_policy().get("pairs")


def test_group_and_ordinary_correction_cannot_claim_same_episode(client):
    client, api, cfg = client
    original, corrected = watched(2), watched(9)
    api._save_policy_manual_batch([("history", "SRC", {canonical_key(corrected): corrected}, [], "default")], pair_id="p1",
        mappings={("history", "SRC", "default"): {canonical_key(corrected): dict(original_key=canonical_key(original), original=original)}})
    assert client.post("/api/editor/episode-groups", json=dict(pair_id="p1", group=group())).status_code == 400


def test_new_ordinary_correction_cannot_override_existing_group(client):
    client, api, _ = client
    assert client.post("/api/editor/episode-groups", json=dict(pair_id="p1", group=group())).status_code == 200
    before = deepcopy(api._load_policy())
    with pytest.raises(HTTPException) as error:
        api._save_policy_manual_batch([("history", "SRC", {canonical_key(watched(2)): watched(2)}, [], "default")], pair_id="p1")
    assert error.value.status_code == 400
    assert api._load_policy() == before


def test_editor_badges_match_saved_history_members_without_changing_items(client):
    client, api, _ = client
    items = {"part": watched(2), "rewatch": watched(2), "unrelated": watched(8),
             "other_show": {**watched(2), "show_ids": {"tmdb": "999"}},
             "movie": {"type": "movie", "ids": {"tmdb": "100"}}}
    api._save_state_items("history", "SRC", items)
    before = client.get("/api/editor", params=dict(kind="history", provider="SRC")).json()
    assert client.post("/api/editor/episode-groups", json=dict(pair_id="p1", group=group())).status_code == 200
    data = client.get("/api/editor", params=dict(kind="history", provider="SRC")).json()
    assert data["items"] == before["items"]
    assert set(data["episode_groups"]) == {"part", "rewatch"}
    badge = data["episode_groups"]["part"][0]
    assert badge["id"] == "finale" and badge["pair_id"] == "p1"
    assert badge["source"]["episodes"] == group()["source"]["episodes"]
    assert badge["can_edit"] is True
    api._save_state_items("ratings", "SRC", items)
    data = client.get("/api/editor", params=dict(kind="ratings", provider="SRC")).json()
    assert data["episode_groups"] == {}


@pytest.mark.parametrize("case", ["pair", "instance", "read_only", "selected_pair", "other_instance"])
def test_editor_badges_respect_pair_and_instance_access(client, monkeypatch, case):
    from services import episode_groups

    client, api, cfg = client
    assert client.post("/api/editor/episode-groups", json=dict(pair_id="p1", group=group())).status_code == 200
    pair_id, instance = "", "default"
    if case == "pair":
        monkeypatch.setattr(episode_groups, "user_can_access_pair", lambda *_a: False)
    elif case == "instance":
        def require(_cfg, _request, provider, instance):
            if provider == "DST":
                raise HTTPException(403, "Denied")
        monkeypatch.setattr(api, "_require_instance_scope", require)
    elif case == "read_only":
        monkeypatch.setattr(episode_groups, "request_user", lambda *_a: {"permissions": {"write": False}})
        monkeypatch.setattr(episode_groups, "user_can_access_pair", lambda *_a: True)
    elif case == "selected_pair":
        pair_id = "another"
    else:
        instance = "other"
    index = episode_groups.editor_group_index(cfg, None, api._load_policy(), "history", pair_id)
    badges = episode_groups.editor_badges({"part": watched(2)}, index, [("SRC", instance)])
    if case == "read_only":
        assert badges["part"][0]["can_edit"] is False
    else:
        assert badges == {}


def test_merged_editor_badges_require_presence_on_a_group_endpoint(client, monkeypatch):
    client, api, _ = client
    assert client.post("/api/editor/episode-groups", json=dict(pair_id="p1", group=group())).status_code == 200
    api._save_state_items("history", "SRC", {"part": watched(2)})
    api._save_state_items("history", "DST", {"unmapped_part": watched(3)})
    monkeypatch.setattr(api, "_editor_send_targets", lambda *a: [])
    monkeypatch.setattr(api, "_always_listed_providers", lambda: [])
    data = client.get("/api/editor/merged", params=dict(kind="history")).json()
    assert len(data["items"]) == 2
    assert len(data["episode_groups"]) == 1
    key, badges = next(iter(data["episode_groups"].items()))
    assert data["items"][key]["episode"] == 2
    assert badges[0]["pair_id"] == "p1"
