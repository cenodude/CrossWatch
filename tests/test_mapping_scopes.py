# tests/test_mapping_scopes.py
# CrossWatch - Mapping scope isolation and Editor staging
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from copy import deepcopy

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from cw_platform.id_map import canonical_key
from cw_platform.local_db import manual_policy
from cw_platform.mapping_policy import effective_policy
from cw_platform.orchestrator import Orchestrator
from cw_platform.orchestrator._interactive import InteractivePlan
from test_interactive_sync import item, setup_ops
from test_orchestrator_dry_run_no_side_effects import _cfg


def save(api, old, new, pair_id="", feature="watchlist", instance="default"):
    source, target = canonical_key(old), canonical_key(new)
    api._save_policy_manual_batch([(feature, "SRC", {target: new}, [source], instance)], pair_id=pair_id,
        mappings={(feature, "SRC", instance): {target: dict(original_key=source, original=old, origin="editor")}})


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_pair_override_replaces_shared_mapping_in_real_sync(config_base, monkeypatch, mode):
    from api import editorAPI as api
    setup_ops(config_base, monkeypatch, [item(1)])
    monkeypatch.setattr(api, "_STATE_BASE", config_base)
    cfg = _cfg(False)
    cfg["pairs"][0]["mode"] = mode
    cfg["pairs"].append({**deepcopy(cfg["pairs"][0]), "id": "p2"})
    save(api, item(1), item(2))
    save(api, item(2), item(3), "p1")

    def planned(pair_id):
        plan = InteractivePlan()
        result = Orchestrator(cfg, interactive=plan).run(dry_run=True, pair_scope_ids=[pair_id], write_state_json=False)
        assert result["ok"]
        return [row["item"]["ids"] for row in plan.rows.values()]

    assert planned("p1") == [item(3)["ids"]]
    assert planned("p2") == [item(2)["ids"]]
    save(api, item(3), item(4), "p1")
    assert planned("p1") == [item(4)["ids"]]
    assert planned("p2") == [item(2)["ids"]]
    policy = api._load_policy()
    record = next(iter(policy["pairs"]["p1"]["providers"]["SRC"]["watchlist"]["mappings"].values()))
    assert record["original_key"] == canonical_key(item(1))
    save(api, item(4), item(1), "p1")
    assert planned("p1") == [item(1)["ids"]]
    assert planned("p2") == [item(2)["ids"]]
    policy["pairs"].pop("p1")
    manual_policy.save_policy(config_base, policy)
    assert planned("p1") == [item(2)["ids"]]


@pytest.mark.parametrize("feature", ["watchlist", "history", "ratings", "progress", "collection"])
def test_scope_roundtrip_backups_and_instance_isolation(config_base, monkeypatch, feature):
    from api import editorAPI as api
    monkeypatch.setattr(api, "_STATE_BASE", config_base)
    save(api, item(1), item(2), feature=feature)
    before = manual_policy.fingerprint(config_base)
    save(api, item(1), item(3), "p1", feature, "second")
    assert manual_policy.fingerprint(config_base) != before
    policy = api._load_policy()
    for mode in ("replace", "merge"):
        restored = api._merge_policy({}, deepcopy(policy), mode)
        manual_policy.save_policy(config_base, restored)
        assert api._load_policy() == policy
    scoped = effective_policy(policy, "p1")["providers"]["SRC"]
    assert next(iter(scoped[feature]["adds"]["items"].values()))["ids"] == item(2)["ids"]
    assert next(iter(scoped["instances"]["second"][feature]["adds"]["items"].values()))["ids"] == item(3)["ids"]
    assert "instances" not in effective_policy(policy, "p2")["providers"]["SRC"]
    manual_policy.clear_policy(config_base)
    assert not manual_policy.has_policy(config_base)


@pytest.fixture
def editor_client(config_base, monkeypatch):
    from api import editorAPI as api
    cfg = _cfg(False)
    monkeypatch.setattr(api, "_STATE_BASE", config_base)
    monkeypatch.setattr(api, "load_config", lambda: cfg)
    monkeypatch.setattr(api, "_load_state_items", lambda *args, **kwargs: {canonical_key(item(1)): item(1)})
    app = FastAPI()
    app.include_router(api.router)
    with TestClient(app) as client:
        yield client, api, cfg


def test_editor_prepare_is_staged_and_save_has_explicit_scope(editor_client):
    client, api, cfg = editor_client
    original = item(1, watched_at="2026-09-08T10:00:00Z", rating=8)
    payload = dict(provider="SRC", feature="watchlist", key=canonical_key(original), original=original,
                   item=item(2, rating=1), action="prepare", pair_id="p1")
    result = client.post("/api/editor/mapping", json=payload)
    assert result.status_code == 200, result.text
    prepared = result.json()
    assert prepared["item"]["rating"] == 8
    assert prepared["item"]["watched_at"] == original["watched_at"]
    assert not api._load_policy().get("pairs")
    result = client.post("/api/editor", json=dict(provider="SRC", source="state", kind="watchlist",
        pair_id="p1", items={prepared["key"]: prepared["item"]}, blocks=prepared["blocks"],
        mapping_originals={prepared["key"]: payload["key"]}))
    assert result.status_code == 200, result.text
    assert not api._load_policy()["providers"]
    listed = client.get("/api/editor/mappings").json()["items"]
    assert len(listed) == 1 and listed[0]["pair_id"] == "p1"
    viewed = client.get("/api/editor?source=manual&provider=SRC&pair_id=p1").json()
    assert prepared["key"] in viewed["manual_adds"]
    assert viewed["mapping_scopes"][0]["id"] == "p1"
    assert not client.get("/api/editor?source=manual&provider=SRC").json()["manual_adds"]


def test_editor_rejects_foreign_pair_scopes(editor_client, monkeypatch):
    client, api, cfg = editor_client
    from services import editor_mapping
    monkeypatch.setattr(editor_mapping, "user_can_access_pair", lambda *args: False)
    assert client.get("/api/editor?provider=SRC&pair_id=p1").status_code == 404
    assert client.post("/api/editor", json=dict(provider="SRC", pair_id="p1", items={})).status_code == 404
    assert client.post("/api/editor/mapping", json=dict(provider="SRC", feature="watchlist", pair_id="p1",
        action="prepare", key=canonical_key(item(1)), original=item(1), item=item(2))).status_code == 404
    assert not api._load_policy().get("pairs")


def test_editor_removal_restores_shared_mapping_and_preserves_other_scopes(editor_client):
    client, api, cfg = editor_client
    cfg["pairs"].append({**deepcopy(cfg["pairs"][0]), "id": "p2"})
    save(api, item(1), item(2))
    save(api, item(1), item(3), "p1")
    save(api, item(1), item(4), "p2")
    save(api, item(1), item(5), "p1", feature="history")
    save(api, item(1), item(6), "p1", instance="second")
    before = deepcopy(api._load_policy())
    response = client.post("/api/editor", json=dict(source="state", provider="SRC",
        kind="watchlist", pair_id="p1", items={}, blocks=[], mapping_originals={}))
    assert response.status_code == 200, response.text
    policy = api._load_policy()
    assert policy["providers"] == before["providers"]
    assert policy["pairs"]["p2"] == before["pairs"]["p2"]
    scoped = policy["pairs"]["p1"]["providers"]["SRC"]
    assert scoped["history"] == before["pairs"]["p1"]["providers"]["SRC"]["history"]
    assert scoped["instances"] == before["pairs"]["p1"]["providers"]["SRC"]["instances"]
    assert not scoped["watchlist"]["adds"]["items"]
    assert not scoped["watchlist"]["blocks"]
    assert not scoped["watchlist"]["mappings"]
    assert next(iter(effective_policy(policy, "p1")["providers"]["SRC"]["watchlist"]["adds"]["items"].values()))["ids"] == item(2)["ids"]


@pytest.mark.parametrize("pair_id", ["", "p1"])
def test_editor_resave_keeps_original_block_and_removal_clears_mapping(editor_client, pair_id):
    client, api, cfg = editor_client
    save(api, item(1), item(2), pair_id)
    key = canonical_key(item(2))
    payload = dict(source="state", provider="SRC", kind="watchlist", pair_id=pair_id,
                   items={key: item(2)}, blocks=[], mapping_originals={key: key})
    assert client.post("/api/editor", json=payload).status_code == 200
    effective = effective_policy(api._load_policy(), pair_id)["providers"]["SRC"]["watchlist"]
    assert effective["blocks"] == [canonical_key(item(1))]
    assert set(effective["adds"]["items"]) == {key}
    viewed = client.get("/api/editor", params=dict(source="state", provider="SRC", pair_id=pair_id)).json()
    assert viewed["mapping_origins"][key] == ("pair" if pair_id else "shared")
    assert client.post("/api/editor", json={**payload, "items": {}, "mapping_originals": {}}).status_code == 200
    effective = effective_policy(api._load_policy(), pair_id)["providers"]["SRC"]["watchlist"]
    assert not effective["adds"]["items"]
    assert not effective.get("mappings")
    assert not effective["blocks"]


def test_pair_editor_keeps_ordinary_block_rules_editable(editor_client):
    client, api, cfg = editor_client
    key = canonical_key(item(1))
    payload = dict(source="state", provider="SRC", kind="watchlist", pair_id="p1", items={}, blocks=[key])
    assert client.post("/api/editor", json=payload).status_code == 200
    viewed = client.get("/api/editor", params=dict(source="state", provider="SRC", pair_id="p1")).json()
    # The Editor must be able to render this as a blocked baseline row and
    # include the rule in its next full save, or explicitly restore the row.
    assert key in viewed["items"]
    assert viewed["manual_blocks"] == [key]
    assert key not in viewed["mapping_origins"]
    assert client.post("/api/editor", json=payload).status_code == 200
    assert effective_policy(api._load_policy(), "p1")["providers"]["SRC"]["watchlist"]["blocks"] == [key]
    assert client.post("/api/editor", json={**payload, "blocks": []}).status_code == 200
    assert not effective_policy(api._load_policy(), "p1")["providers"]["SRC"]["watchlist"]["blocks"]


def test_saved_pair_mapping_visibility_requires_pair_access(editor_client, monkeypatch):
    from cw_platform import access_policy
    client, api, cfg = editor_client
    save(api, item(1), item(2))
    save(api, item(1), item(3), "p1")
    monkeypatch.setattr(access_policy, "user_can_access_pair", lambda *args: False)
    rows = client.get("/api/editor/mappings").json()["items"]
    assert len(rows) == 1 and rows[0]["scope"] == "shared"


def test_saved_mappings_filter_the_editor_scope_before_pagination(editor_client):
    client, api, cfg = editor_client
    cfg["pairs"].append({**deepcopy(cfg["pairs"][0]), "id":"p2"})
    save(api, item(1), item(2))
    save(api, item(1), item(3), "p1")
    save(api, item(1), item(4), "p2")
    save(api, item(1), item(5), "p1", instance="second")
    save(api, item(1), item(6), "p1", feature="history")
    api._save_policy_manual_batch([("watchlist", "OTHER", {canonical_key(item(7)): item(7)}, [], "default")], pair_id="p1")
    filters = dict(provider="SRC", instance="default", feature="watchlist", limit=1)
    for scope, expected in [("p1", 3), ("p2", 4), ("shared", 2)]:
        result = client.get("/api/editor/mappings", params={**filters, "pair_id":scope}).json()
        assert result["total"] == 1
        assert result["items"][0]["corrected"]["ids"] == item(expected)["ids"]
        assert not client.get("/api/editor/mappings", params={**filters, "pair_id":scope, "offset":1}).json()["items"]
    assert client.get("/api/editor/mappings", params={**filters, "pair_id":"missing"}).json()["total"] == 0


def test_editor_shared_search_uses_metadata_and_pair_search_uses_destination(editor_client):
    client, api, cfg = editor_client
    cfg["tmdb"] = {"api_key":"configured"}
    payload = dict(provider="SRC", feature="watchlist", key=canonical_key(item(1)), original=item(1), action="catalogs")
    shared = client.post("/api/editor/mapping", json=payload).json()
    assert shared["catalogs"] == [{"id":"tmdb", "label":"TMDb metadata"}]
    pair = client.post("/api/editor/mapping", json={**payload, "pair_id":"p1"}).json()
    assert pair["row"]["provider"] == "DST"
    assert pair["row"]["source"] == "SRC"
