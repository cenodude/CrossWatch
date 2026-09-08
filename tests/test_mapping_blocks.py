# CrossWatch - unified mapping and block rules
from copy import deepcopy

import pytest

from cw_platform.id_map import canonical_key
from cw_platform.local_db import manual_policy
from cw_platform.mapping_policy import effective_policy
from cw_platform.orchestrator import Orchestrator
from cw_platform.orchestrator._interactive import InteractivePlan
from test_interactive_sync import item, setup_ops
from test_mapping_scopes import editor_client, save


def rule(key="imdb:tt0000001", pair_id="p1", **extra):
    return dict(provider="SRC", instance="default", feature="watchlist", pair_id=pair_id,
                key=key, blocked=True, **extra)


def test_block_manager_is_scoped_and_unblock_preserves_other_rules(editor_client, config_base):
    client, api, cfg = editor_client
    cfg["pairs"].append({**deepcopy(cfg["pairs"][0]), "id":"p2"})
    for pair in ("", "p1", "p2"):
        assert client.post("/api/editor/mapping-block", json=rule(pair_id=pair)).status_code == 200
    before = manual_policy.fingerprint(config_base)
    for pair in ("shared", "p1", "p2"):
        viewed = client.get("/api/editor/mappings", params=dict(entry_type="block", pair_id=pair, provider="SRC", instance="default", feature="watchlist")).json()
        assert viewed["total"] == 1
        assert viewed["items"][0]["corrected"]["title"] == item(1)["title"]
    assert client.post("/api/editor/mapping-block", json={**rule(), "blocked":False}).status_code == 200
    assert manual_policy.fingerprint(config_base) != before
    policy = api._load_policy()
    assert not policy["pairs"]["p1"]["providers"]["SRC"]["watchlist"]["blocks"]
    assert policy["pairs"]["p2"]["providers"]["SRC"]["watchlist"]["blocks"]
    assert policy["providers"]["SRC"]["watchlist"]["blocks"]


def test_correction_blocks_stay_with_mapping_but_corrected_item_can_be_blocked(editor_client):
    client, api, cfg = editor_client
    save(api, item(1), item(2), "p1")
    assert client.get("/api/editor/mappings?entry_type=block&pair_id=p1").json()["total"] == 0
    assert client.post("/api/editor/mapping-block", json={**rule(), "blocked":False}).status_code == 409
    target = canonical_key(item(2))
    assert client.post("/api/editor/mapping-block", json=rule(target)).status_code == 200
    node = effective_policy(api._load_policy(), "p1")["providers"]["SRC"]["watchlist"]
    assert target in node["blocks"]
    viewed = client.get("/api/editor?source=state&provider=SRC&pair_id=p1").json()
    assert target in viewed["preserved_blocks"]
    assert client.post("/api/editor/mapping-block", json={**rule(target), "blocked":False}).status_code == 200
    node = effective_policy(api._load_policy(), "p1")["providers"]["SRC"]["watchlist"]
    assert target not in node["blocks"]
    assert canonical_key(item(1)) in node["blocks"]
    assert target in node["mappings"]


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_blocked_correction_is_excluded_in_real_sync(config_base, monkeypatch, editor_client, mode):
    client, api, cfg = editor_client
    setup_ops(config_base, monkeypatch, [item(1)])
    cfg["pairs"][0]["mode"] = mode
    cfg["pairs"].append({**deepcopy(cfg["pairs"][0]), "id":"p2"})
    save(api, item(1), item(2), "p1")

    def planned(pair):
        plan = InteractivePlan()
        result = Orchestrator(cfg, interactive=plan).run(dry_run=True, pair_scope_ids=[pair], write_state_json=False)
        assert result["ok"]
        return [row["item"]["ids"] for row in plan.rows.values()]

    assert planned("p1") == [item(2)["ids"]]
    assert client.post("/api/editor/mapping-block", json=rule(canonical_key(item(2)))).status_code == 200
    assert planned("p1") == []
    assert planned("p2") == [item(1)["ids"]]
    assert client.post("/api/editor/mapping-block", json={**rule(canonical_key(item(2))), "blocked":False}).status_code == 200
    assert planned("p1") == [item(2)["ids"]]


def test_orphan_block_is_listed_and_preserved_when_editor_saves(editor_client):
    client, api, cfg = editor_client
    key = canonical_key(item(99))
    assert client.post("/api/editor/mapping-block", json=rule(key)).status_code == 200
    data = client.get("/api/editor/mappings?entry_type=block&pair_id=p1").json()
    assert data["items"][0]["key"] == key
    assert data["items"][0]["corrected"] is None
    viewed = client.get("/api/editor?source=state&provider=SRC&pair_id=p1").json()
    assert viewed["preserved_blocks"] == [key]
    payload = dict(source="state", provider="SRC", pair_id="p1", kind="watchlist", items={}, blocks=viewed["preserved_blocks"])
    assert client.post("/api/editor", json=payload).status_code == 200
    assert client.get("/api/editor/mappings?entry_type=block&pair_id=p1").json()["total"] == 1


def test_block_updates_reject_foreign_scopes_and_read_only_users(editor_client, monkeypatch):
    from services import editor_mapping
    client, api, cfg = editor_client
    assert client.post("/api/editor/mapping-block", json=rule(pair_id="missing")).status_code == 404
    monkeypatch.setattr(editor_mapping, "request_user", lambda request: {"permissions":{"write":False}})
    assert client.post("/api/editor/mapping-block", json=rule()).status_code == 403
    monkeypatch.setattr(editor_mapping, "request_user", lambda request: None)
    monkeypatch.setattr(editor_mapping, "user_can_access_pair", lambda *args: False)
    assert client.post("/api/editor/mapping-block", json=rule()).status_code == 404
    assert not list(manual_policy.load_policy(api._STATE_BASE).get("pairs", {}))


def test_blocks_list_respects_pair_and_profile_access(editor_client, monkeypatch):
    from cw_platform import access_policy
    client, api, cfg = editor_client
    for pair in ("", "p1"):
        assert client.post("/api/editor/mapping-block", json=rule(pair_id=pair)).status_code == 200
    monkeypatch.setattr(access_policy, "user_can_access_pair", lambda *args: False)
    data = client.get("/api/editor/mappings?entry_type=block").json()
    assert data["total"] == 1 and data["items"][0]["scope"] == "shared"
    monkeypatch.setattr(api, "user_can_access_instance", lambda *args: False)
    data = client.get("/api/editor/mappings?entry_type=block").json()
    assert data["total"] == 0 and data["sources"] == []
