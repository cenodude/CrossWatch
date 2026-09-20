# /tests/test_same_provider_pairs.py
# CrossWatch - Same-provider sync pair regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy

import pytest

from api import syncAPI as api
from cw_platform.id_map import canonical_key
from cw_platform.orchestrator import Orchestrator
from cw_platform.orchestrator._interactive import InteractivePlan
from cw_platform.orchestrator._state_store import StateStore
from cw_platform.pair_scope import pair_feature_scope
from test_interactive_sync_features import FEATURES, feature_item
from test_orchestrator_dry_run_no_side_effects import FakeOps, _cfg, _install


@pytest.fixture(autouse=True)
def fake_connection_status(monkeypatch):
    monkeypatch.setattr("cw_platform.orchestrator._pairs.record_health", lambda *args: None)


@pytest.fixture
def pair_api(monkeypatch):
    cfg = {"pairs": []}
    saves = []
    monkeypatch.setattr(api, "_env", lambda: (lambda: cfg, lambda value: saves.append(deepcopy(value))))
    return cfg, saves


@pytest.mark.parametrize("source_instance,target_instance", [(None, None), ("default", " DEFAULT "), ("PLEX-P01", "PLEX-P01")])
def test_api_rejects_same_instance(pair_api, source_instance, target_instance):
    cfg, saves = pair_api
    result = api.api_pairs_add(api.PairIn(source="plex", target=" PLEX ", source_instance=source_instance, target_instance=target_instance))
    assert result == {"ok": False, "error": "same_provider_instance"}
    assert not cfg["pairs"] and not saves


@pytest.mark.parametrize("source_instance,target_instance", [("default", "PLEX-P01"), ("PLEX-P01", "default"), ("PLEX-P01", "PLEX-P02")])
def test_api_allows_different_instances_and_rejects_self_pair_edits(pair_api, source_instance, target_instance):
    cfg, saves = pair_api
    result = api.api_pairs_add(api.PairIn(source="PLEX", target="PLEX", source_instance=source_instance, target_instance=target_instance))
    assert result["ok"]
    before = deepcopy(cfg)
    rejected = api.api_pairs_update(result["id"], api.PairPatch(target_instance=source_instance, enabled=False))
    assert rejected == {"ok": False, "error": "same_provider_instance"}
    assert cfg == before and len(saves) == 1
    assert api.api_pairs_update(result["id"], api.PairPatch(mode="two-way"))["ok"]


class InstanceOps(FakeOps):
    def __init__(self, feature):
        super().__init__("SRC", {})
        self.feature = feature
        self.accounts = {
            "account-a": FakeOps("SRC", {canonical_key(feature_item(feature, 1)): feature_item(feature, 1)}),
            "account-b": FakeOps("SRC", {canonical_key(feature_item(feature, 2)): feature_item(feature, 2)}),
        }
        self.reads = []
        self.removes = []

    def features(self):
        return {self.feature: True}

    def capabilities(self):
        return {"features": self.features(), "index_semantics": "present"}

    def health(self, cfg, **kwargs):
        return {"ok": True, "status": "ok", "features": self.features()}

    def build_index(self, cfg, *, feature):
        token = cfg["src"]["token"]
        self.reads.append(token)
        return self.accounts[token].build_index(cfg, feature=feature)

    def add(self, cfg, items, *, feature, dry_run=False):
        return self.accounts[cfg["src"]["token"]].add(cfg, items, feature=feature, dry_run=dry_run)

    def remove(self, cfg, items, *, feature, dry_run=False):
        token = cfg["src"]["token"]
        batch = list(items)
        self.removes.append((token, batch))
        if not dry_run:
            for item in batch:
                self.accounts[token].index.pop(canonical_key(item), None)
        return {"ok": True, "count": len(batch)}


@pytest.mark.parametrize("feature", FEATURES)
@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("reverse", [False, True])
def test_sync_keeps_accounts_snapshots_and_baselines_separate(config_base, monkeypatch, feature, mode, reverse):
    ops = InstanceOps(feature)
    _install(monkeypatch, ops, ops, config_base / ".cw_state")
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SRC": ops})
    cfg = _cfg(False)
    cfg["runtime"]["snapshot_ttl_sec"] = 300
    cfg["src"] = {"token": "account-a", "instances": {"P01": {"token": "account-b"}}}
    pair = cfg["pairs"][0]
    source_instance, target_instance = ("P01", "default") if reverse else ("default", "P01")
    pair.update(target="SRC", source_instance=source_instance, target_instance=target_instance, mode=mode, feature=feature,
                features={feature: {"enable": True, "add": True, "remove": False}})
    before = deepcopy(cfg)
    plan = InteractivePlan()
    result = Orchestrator(cfg, interactive=plan).run(dry_run=True)
    assert not result["errors"]
    assert {row["instance"] for row in plan.rows.values()} == ({source_instance, target_instance} if mode == "two-way" else {target_instance})
    assert all(not account.add_calls for account in ops.accounts.values())
    result = Orchestrator(cfg).run()
    assert not result["errors"]
    assert {"account-a", "account-b"} <= set(ops.reads)
    source_token, target_token = ("account-b", "account-a") if reverse else ("account-a", "account-b")
    assert len(ops.accounts[target_token].index) == 2
    assert len(ops.accounts[source_token].index) == (2 if mode == "two-way" else 1)
    assert cfg["src"] == before["src"]
    state = StateStore(config_base).for_pair(pair_feature_scope(cfg, pair, feature)).load_state()["providers"]["SRC"]
    assert feature in state and feature in state["instances"]["P01"]


def test_runner_skips_same_instance(config_base, monkeypatch):
    ops = InstanceOps("watchlist")
    _install(monkeypatch, ops, ops, config_base / ".cw_state")
    cfg = _cfg(False)
    cfg["pairs"][0].update(target="SRC")
    result = Orchestrator(cfg).run()
    assert not result["errors"]
    assert not ops.reads


@pytest.mark.parametrize("feature", ["ratings", "progress"])
def test_two_way_conflict_uses_target_instance_value(config_base, monkeypatch, feature):
    ops = InstanceOps(feature)
    left = feature_item(feature, 1)
    if feature == "progress":
        left.update(progress_ms=0, progress_percent=0)
    right = {**left, "rating": 10, "rated_at": "2025-01-01T12:00:00Z", "progress_ms": 300000, "progress_percent": 50, "progress_at": "2025-01-01T12:00:00Z"}
    key = canonical_key(left)
    ops.accounts["account-a"].index = {key: left}
    ops.accounts["account-b"].index = {key: right}
    _install(monkeypatch, ops, ops, config_base / ".cw_state")
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SRC": ops})
    cfg = _cfg(False)
    cfg["src"] = {"token": "account-a", "instances": {"P01": {"token": "account-b"}}}
    cfg["sync"]["enable_remove"] = True
    cfg["pairs"][0].update(target="SRC", target_instance="P01", mode="two-way", feature=feature,
                           features={feature: {"enable": True, "add": True, "remove": True}})
    plan = InteractivePlan()
    result = Orchestrator(cfg, interactive=plan).run(dry_run=True)
    assert not result["errors"]
    conflict = next(iter(plan.conflicts.values()))
    assert conflict["source"] == "SRC#default" and conflict["target"] == "SRC#P01"
    assert conflict["winner"] == "SRC#P01"
    result = Orchestrator(cfg).run()
    assert not result["errors"]
    field = "rating" if feature == "ratings" else "progress_ms"
    assert ops.accounts["account-a"].index[key][field] == right[field]
    assert not ops.accounts["account-b"].add_calls


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("reverse", [False, True])
def test_observed_deletion_reaches_only_the_other_instance(config_base, monkeypatch, mode, reverse):
    ops = InstanceOps("watchlist")
    _install(monkeypatch, ops, ops, config_base / ".cw_state")
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SRC": ops})
    cfg = _cfg(False)
    cfg["src"] = {"token": "account-a", "instances": {"P01": {"token": "account-b"}}}
    cfg["sync"].update(enable_remove=True, include_observed_deletes=True, allow_mass_delete=True)
    source_instance, target_instance = ("P01", "default") if reverse else ("default", "P01")
    cfg["pairs"][0].update(target="SRC", source_instance=source_instance, target_instance=target_instance, mode=mode,
                           features={"watchlist": {"enable": True, "add": True, "remove": True}})
    assert not Orchestrator(cfg).run()["errors"]
    source_token, target_token = ("account-b", "account-a") if reverse else ("account-a", "account-b")
    key = next(iter(ops.accounts[source_token].index))
    ops.accounts[source_token].index.pop(key)
    assert not Orchestrator(cfg).run()["errors"]
    assert key not in ops.accounts[target_token].index
    assert ops.removes and all(token == target_token for token, _ in ops.removes)


def test_trakt_refresh_uses_explicit_endpoint_instance(monkeypatch):
    from providers.sync import _mod_TRAKT as trakt

    monkeypatch.setenv("CW_PAIR_SRC", "TRAKT")
    monkeypatch.setenv("CW_PAIR_DST", "TRAKT")
    monkeypatch.setenv("CW_PAIR_SRC_INSTANCE", "default")
    monkeypatch.setenv("CW_PAIR_DST_INSTANCE", "TRAKT-P01")
    seen = []
    monkeypatch.setattr(trakt.AUTH_TRAKT, "refresh", lambda cfg, instance_id: seen.append(instance_id) or {"ok": False})
    client = trakt.TRAKTClient(trakt.TRAKTConfig(client_id="client", access_token="token"), {"_cw_provider_instance": "TRAKT-P01"})
    assert not client._try_refresh()
    assert seen == ["TRAKT-P01"]


def test_stremio_uses_explicit_endpoint_instance(monkeypatch):
    from providers.sync import _mod_STREMIO as stremio

    monkeypatch.setenv("CW_PAIR_SRC", "STREMIO")
    monkeypatch.setenv("CW_PAIR_SRC_INSTANCE", "default")
    module = stremio.STREMIOModule({"_cw_provider_instance": "STREMIO-P01", "stremio": {"auth_key": "test-key"}})
    assert module.instance_id == "STREMIO-P01"
