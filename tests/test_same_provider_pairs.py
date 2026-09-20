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


@pytest.mark.parametrize("provider", ["EMBY", "JELLYFIN", "KODI", "MDBLIST", "FLICKLIST", "FLOPPY", "NUVIO", "PUNCHPLAY"])
@pytest.mark.parametrize("source,target", [("default", "P01"), ("P01", "default"), ("P01", "P02")])
def test_real_adapters_use_each_endpoint_account(monkeypatch, provider, source, target):
    import importlib
    from cw_platform.provider_instances import build_config_view
    from cw_platform.orchestrator._pairs_utils import pair_endpoint_config
    from providers.auth import _auth_PUNCHPLAY

    module = importlib.import_module(f"providers.sync._mod_{provider}")
    monkeypatch.setattr("requests.Session.send", lambda *args, **kwargs: pytest.fail("Unexpected network request"))
    for name, value in {"SRC": provider, "DST": provider, "SRC_INSTANCE": source, "DST_INSTANCE": target}.items():
        monkeypatch.setenv(f"CW_PAIR_{name}", value)
    monkeypatch.setenv("CW_INSTANCE_ID", source)
    def block(instance):
        return dict(server=f"http://{instance}", server_url=f"http://{instance}", base_url=f"http://{instance}",
                    access_token=f"token-{instance}", api_token=f"token-{instance}", api_key=f"token-{instance}", user_id=instance)
    key = provider.lower()
    cfg = {key: {**block("default"), "instances": {"P01": block("P01"), "P02": block("P02")}}}
    blocks = {inst: build_config_view(cfg, {provider: inst})[key] for inst in (source, target)}
    pair_cfg = {**cfg, "_cw_pair_instance_blocks": {provider: blocks}}
    for instance in (source, target):
        endpoint = pair_endpoint_config(pair_cfg, provider, instance)
        adapter = getattr(module, f"{provider}Module")(endpoint)
        assert adapter.instance_id == instance
        if provider in {"EMBY", "JELLYFIN", "KODI"}:
            assert adapter.cfg.server.rstrip("/") == f"http://{instance}"
        elif provider == "MDBLIST":
            assert module.mdblist_auth.provider_block(adapter.client.raw_cfg, adapter.client.instance_id)["api_key"] == f"token-{instance}"
        elif provider == "FLICKLIST":
            assert adapter._section()["access_token"] == f"token-{instance}"
        elif provider == "FLOPPY":
            assert adapter.client.api_token == f"token-{instance}"
        elif provider == "NUVIO":
            assert adapter.client.block["access_token"] == f"token-{instance}"
        else:
            assert _auth_PUNCHPLAY.provider_block(adapter.config, adapter.instance_id)["access_token"] == f"token-{instance}"


@pytest.mark.parametrize("feature", ["history", "ratings", "progress", "collection"])
@pytest.mark.parametrize("provider", ["PLEX", "EMBY", "JELLYFIN", "KODI"])
def test_endpoint_library_overrides_and_connection_fallback(provider, feature):
    from cw_platform.orchestrator._pairs import _config_with_pair_feature_options
    from cw_platform.orchestrator._pairs_oneway import _effective_library_whitelist as one_way
    from cw_platform.orchestrator._pairs_twoway import _effective_library_whitelist as two_way
    key = provider.lower()
    fcfg = {"libraries": {provider: ["legacy"], f"{provider}#default": ["A"], f"{provider}#P01": ["B"]}}
    for instance, expected in [("default", ["A"]), ("P01", ["B"]), ("P02", ["legacy"])]:
        cfg = {"_cw_provider_instance": instance, key: {feature: {"libraries": ["connection"]}}}
        result = _config_with_pair_feature_options(cfg, fcfg, (provider, provider), feature)
        assert result[key][feature]["libraries"] == expected
        assert one_way(result, provider, feature, fcfg) == expected
        assert two_way(result, provider, feature, fcfg) == expected
    fcfg["libraries"][f"{provider}#P02"] = []
    result = _config_with_pair_feature_options(cfg, fcfg, (provider, provider), feature)
    assert result[key][feature]["libraries"] == ["connection"]
    assert one_way(result, provider, feature, fcfg) == ["connection"]
    assert two_way(result, provider, feature, fcfg) == ["connection"]


def test_endpoint_pending_blackbox_and_success_are_isolated(tmp_path, monkeypatch):
    from cw_platform.orchestrator import _unresolved as unresolved, _blackbox as blackbox
    from cw_platform.orchestrator._pairs_blocklist import apply_blocklist
    from cw_platform.orchestrator._scope import scope_safe, provider_call
    from types import SimpleNamespace
    monkeypatch.setattr(unresolved, "STATE_DIR", tmp_path)
    monkeypatch.setattr(blackbox, "STATE_DIR", tmp_path)
    monkeypatch.setenv("CW_PAIR_SCOPE", "cw2_pair")
    item = feature_item("watchlist", 1)
    key = canonical_key(item)
    cfg = {"sync": {"blackbox": {"promote_after": 1}}}
    unresolved.record_unresolved("PLEX", "watchlist", [item], instance="P01")
    assert not unresolved.load_unresolved_pending("PLEX", "watchlist", instance="default")
    assert unresolved.load_unresolved_pending("PLEX", "watchlist", instance="P01")
    assert not unresolved.load_unresolved_keys("PLEX", instance="default")
    assert key in unresolved.load_unresolved_keys("PLEX", instance="P01")
    blackbox.record_attempts("PLEX", "watchlist", [key], cfg=cfg, instance="P01")
    assert blackbox.load_blackbox_keys("PLEX", "watchlist", instance="P01") == {key}
    assert not blackbox.load_blackbox_keys("PLEX", "watchlist", instance="default")
    store = SimpleNamespace(load_tombstones=lambda **kwargs: {})
    assert apply_blocklist(store, [item], dst="PLEX", feature="watchlist", instance="default") == [item]
    assert apply_blocklist(store, [item], dst="PLEX", feature="watchlist", instance="P01") == []
    blackbox.record_success("PLEX", "watchlist", [key], instance="default")
    unresolved.clear_unresolved("PLEX", "watchlist", [key], instance="default")
    assert blackbox.load_blackbox_keys("PLEX", "watchlist", instance="P01") == {key}
    assert unresolved.load_unresolved_pending("PLEX", "watchlist", instance="P01")
    expected_scope = scope_safe("default")
    def provider_write(config):
        assert scope_safe() == expected_scope
        unresolved.record_unresolved("PLEX", "watchlist", [item])
        raise RuntimeError("provider error")
    with pytest.raises(RuntimeError, match="provider error"):
        provider_call(provider_write, {"_cw_provider_instance": "default", "_cw_pair_scope": "cw2_pair"})
    assert scope_safe() == "cw2_pair"
    assert unresolved.load_unresolved_pending("PLEX", "watchlist", instance="default")
    blackbox.record_success("PLEX", "watchlist", [key], instance="P01")
    assert not blackbox.load_blackbox_keys("PLEX", "watchlist", instance="P01")


@pytest.mark.parametrize("reverse", [False, True])
def test_two_way_failed_writes_stay_with_their_destination(config_base, monkeypatch, reverse):
    from cw_platform.orchestrator import _unresolved as unresolved
    ops = InstanceOps("watchlist")
    def fail_add(cfg, items, **kwargs):
        return {"ok": True, "unresolved": list(items), "count": 0}
    monkeypatch.setattr(ops, "add", fail_add)
    _install(monkeypatch, ops, ops, config_base / ".cw_state")
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SRC": ops})
    cfg = _cfg(False)
    cfg["src"] = {"token": "account-a", "instances": {"P01": {"token": "account-b"}}}
    pair = cfg["pairs"][0]
    source, target = ("P01", "default") if reverse else ("default", "P01")
    pair.update(target="SRC", source_instance=source, target_instance=target, mode="two-way")
    result = Orchestrator(cfg).run()
    assert not result["errors"]
    monkeypatch.setenv("CW_PAIR_SCOPE", pair_feature_scope(cfg, pair, "watchlist"))
    for instance, number in [("default", 2), ("P01", 1)]:
        rows = unresolved.load_unresolved_pending("SRC", "watchlist", instance=instance)
        assert {row["key"] for row in rows} == {canonical_key(feature_item("watchlist", number))}


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("feature", ["history", "ratings", "progress", "collection"])
def test_sync_filters_each_endpoint_by_its_own_libraries(config_base, monkeypatch, mode, feature):
    ops = InstanceOps(feature)
    ops.provider = "EMBY"
    reads = []
    def build_index(cfg, *, feature):
        selected = cfg["emby"]
        reads.append((selected["token"], selected[feature]["libraries"]))
        return ops.accounts[selected["token"]].build_index(cfg, feature=feature)
    monkeypatch.setattr(ops, "build_index", build_index)
    for token, number, library in [("account-a", 1, "A"), ("account-b", 2, "B")]:
        included = {**feature_item(feature, number), "library_id": library}
        excluded = {**feature_item(feature, number + 10), "library_id": "excluded"}
        ops.accounts[token].index = {canonical_key(item): item for item in (included, excluded)}
    _install(monkeypatch, ops, ops, config_base / ".cw_state")
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"EMBY": ops})
    cfg = _cfg(False)
    cfg["emby"] = {"token": "account-a", "instances": {"P01": {"token": "account-b"}}}
    pair = cfg["pairs"][0]
    pair.update(source="EMBY", target="EMBY", target_instance="P01", mode=mode, feature=feature,
                features={feature: {"enable": True, "add": True, "remove": False,
                                    "libraries": {"EMBY#default": ["A"], "EMBY#P01": ["B"]}}})
    review = InteractivePlan()
    result = Orchestrator(cfg, interactive=review).run(dry_run=True)
    assert not result["errors"]
    assert ("account-a", ["A"]) in reads
    assert ("account-b", ["B"]) in reads
    rows = list(review.rows.values())
    assert rows
    assert all(row["item"]["ids"]["imdb"] not in {"tt0000011", "tt0000012"} for row in rows)
    assert {row["instance"] for row in rows} == ({"default", "P01"} if mode == "two-way" else {"P01"})
