# tests/test_status_mapping_api.py
# CrossWatch - Pair status mapping persistence and validation boundaries.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
import json
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from api import syncAPI, interactiveSyncAPI


@pytest.fixture
def config(monkeypatch):
    cfg = {"pairs": [], "anime_mapping": {"enabled": False}}
    saved = []
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: deepcopy(cfg), lambda value: (cfg.update(value), saved.append(deepcopy(value)))))
    monkeypatch.setattr(syncAPI, "_apply_pair_profile_scope", lambda *args: None)
    monkeypatch.setattr(syncAPI, "_managed_pair_blocked", lambda *args: False)
    return cfg, saved


def mapping():
    return {"version": 1, "a": {"provider": "crosswatch", "instance": "default"},
            "b": {"provider": "simkl", "instance": "default"}, "conflict": "leave_unchanged",
            "directions": {"a_to_b": {"shows": {"dropped": {"include": True, "target": "on_hold"}}}}}


def create():
    return syncAPI.api_pairs_add(syncAPI.PairIn(source="CROSSWATCH", target="SIMKL", enabled=True,
        features={"watchlist": {"enable": True, "remove": False}, "history": {"enable": False, "use_source_status": False}},
        status_mapping=mapping()))


def test_pair_save_reload_preserves_mapping_without_enabling_status_or_watchlist_changes(config):
    cfg, saved = config
    assert create()["ok"]
    pair = cfg["pairs"][0]
    assert pair["status_mapping"] == mapping()
    assert pair["features"]["history"]["enable"] is False
    assert pair["features"]["history"]["use_source_status"] is False
    assert pair["features"]["watchlist"]["remove"] is False
    assert len(saved) == 1
    assert syncAPI.api_pairs_update(pair["id"], syncAPI.PairPatch(status_mapping=None))["ok"]
    assert cfg["pairs"][0]["status_mapping"] is None


@pytest.mark.parametrize("change", ["planned", "completion", "reverse", "endpoint", "tagging"])
def test_invalid_mapping_update_never_changes_saved_pair(config, change):
    cfg, saved = config
    assert create()["ok"]
    before = deepcopy(cfg)
    proposed = mapping()
    updates = {}
    if change == "planned":
        proposed["directions"]["a_to_b"]["shows"]["dropped"]["target"] = "planned"
    elif change == "completion":
        proposed["directions"]["a_to_b"]["shows"]["dropped"]["target"] = "completed"
    elif change == "reverse":
        proposed["directions"]["b_to_a"] = {}
    elif change == "endpoint":
        updates["target_instance"] = "other"
    else:
        updates["providers"] = {"SIMKL": {"history_dropped_mode": "tag"}}
    result = syncAPI.api_pairs_update(cfg["pairs"][0]["id"], syncAPI.PairPatch(status_mapping=proposed, **updates))
    assert result["ok"] is False
    assert cfg == before and len(saved) == 1


def test_status_updates_cannot_be_remapped_as_catalog_entries():
    row = {"operation": "update", "feature": "history", "item": {"_cw_status_mapping": {"from": "watching", "to": "dropped"}}}
    session = SimpleNamespace(plan=SimpleNamespace(rows={"row": row}))
    with pytest.raises(HTTPException) as exc:
        interactiveSyncAPI.mapping_row(session, "row")
    assert exc.value.status_code == 400


@pytest.mark.parametrize("kind", ["ordinary", "one_marker", "missing", "invalid_provider"])
def test_clearing_status_choice_does_not_relax_other_conflict_validation(monkeypatch, kind):
    conflict = {"source": "A", "target": "B", "left": {}, "right": {}}
    if kind in {"one_marker", "invalid_provider"}:
        conflict["left"]["_cw_status_mapping"] = True
    if kind == "invalid_provider":
        conflict["right"]["_cw_status_mapping"] = True
    plan = SimpleNamespace(choices={"test": "A"}, conflicts={} if kind == "missing" else {"test": conflict})
    session = SimpleNamespace(plan=plan, revision=1, status="ready")
    calls = []
    monkeypatch.setattr(interactiveSyncAPI, "load_config", lambda: {})
    monkeypatch.setattr(interactiveSyncAPI, "get_session", lambda *args: session)
    monkeypatch.setattr(interactiveSyncAPI, "launch", lambda *args: calls.append(args))
    with pytest.raises(HTTPException) as error:
        interactiveSyncAPI.refresh("test", interactiveSyncAPI.Refresh(revision=1, choices={"test": "C" if kind == "invalid_provider" else ""}), None)
    assert error.value.status_code == 400
    assert plan.choices == {"test": "A"}
    assert calls == []


def test_provider_endpoint_publishes_the_same_capabilities_used_by_validation(monkeypatch, config_base):
    from providers.sync._status_mapping import ADAPTERS, provider_caps
    monkeypatch.setattr(syncAPI, "sync_provider_names", lambda **kwargs: [name.upper() for name in ADAPTERS])
    response = syncAPI.api_sync_providers()
    providers = json.loads(bytes(response.body))
    assert {row["name"].lower() for row in providers} == set(ADAPTERS)
    for provider in providers:
        assert provider["capabilities"]["status_mapping"] == provider_caps(provider["name"])


def test_flicklist_does_not_advertise_custom_status_mapping(monkeypatch, config_base):
    monkeypatch.setattr(syncAPI, "sync_provider_names", lambda **kwargs: ["FLICKLIST"])
    response = syncAPI.api_sync_providers()
    providers = json.loads(bytes(response.body))
    assert len(providers) == 1
    assert providers[0]["name"].upper() == "FLICKLIST"
    assert "status_mapping" not in providers[0]["capabilities"]


@pytest.mark.parametrize("source,target", [("crosswatch", "flicklist"), ("flicklist", "crosswatch")])
def test_flicklist_custom_mapping_is_rejected_without_saving(config, source, target):
    cfg, saved = config
    before = deepcopy(cfg)
    proposed = mapping()
    proposed["a"]["provider"] = source
    proposed["b"]["provider"] = target
    proposed["directions"]["a_to_b"]["shows"]["dropped"]["target"] = "dropped"
    result = syncAPI.api_pairs_add(syncAPI.PairIn(source=source.upper(), target=target.upper(), enabled=True,
        features={"history": {"enable": True, "use_source_status": True}}, status_mapping=proposed))
    assert result["ok"] is False
    assert cfg == before
    assert saved == []
