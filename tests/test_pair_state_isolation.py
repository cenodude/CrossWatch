# tests/test_pair_state_isolation.py
# CrossWatch - Pair State Isolation Regression Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
import importlib
import json
from types import SimpleNamespace
from pathlib import Path

import pytest

from cw_platform.orchestrator import Orchestrator
from cw_platform.orchestrator._interactive import InteractivePlan
from cw_platform.orchestrator._state_store import StateStore
from cw_platform.pair_scope import pair_feature_scope
from test_interactive_sync_features import FEATURES, feature_item, feature_setup


def block(number, feature="watchlist", checkpoint=None):
    return {"baseline": {"items": {f"imdb:tt{number:07}": feature_item(feature, number)}}, "checkpoint": checkpoint}


@pytest.mark.parametrize("feature", [*FEATURES, "playlists"])
def test_pair_database_roundtrip_does_not_inherit_or_replace_other_pairs(config_base, feature):
    store = StateStore(config_base)
    key = ("PLEX", "default", feature)
    a, b = store.for_pair("pair-a"), store.for_pair("pair-b")
    first, second = block(1), block(2)
    first["checkpoint"], second["checkpoint"] = "a-cursor", "b-cursor"
    store.save_feature_blocks({key: second})
    assert not a.load_state()["providers"]
    a.save_feature_blocks({key: first})
    assert not b.load_state()["providers"]
    b.save_feature_blocks({key: second})
    for scoped, expected in ((a, first), (b, second)):
        found = scoped.load_state_features({feature})["providers"]["PLEX"][feature]
        assert found["checkpoint"] == expected["checkpoint"]
        assert set(found["baseline"]["items"]) == set(expected["baseline"]["items"])
    a.save_feature_blocks({key: {"baseline": {"items": {}}}})
    assert not a.load_state()["providers"]["PLEX"][feature]["baseline"]["items"]
    assert b.load_state()["providers"]["PLEX"][feature]["baseline"]["items"]


@pytest.mark.parametrize("field", ["id", "profile_id", "source_instance", "target_instance", "mode"])
def test_pair_scope_distinguishes_identity(config_base, field):
    pair = dict(id="pair-a", source="PLEX", target="SIMKL", mode="one-way", profile_id="alice")
    changed = {**pair, field: "different"}
    assert pair_feature_scope({}, pair, "history") != pair_feature_scope({}, changed, "history")


def test_whitelist_change_invalidates_only_affected_feature():
    pair = dict(id="pair-a", source="PLEX", target="SIMKL", features={"history": {"libraries": {"PLEX": ["1"]}}})
    other = deepcopy(pair)
    other["features"]["history"]["libraries"]["PLEX"] = ["2"]
    assert pair_feature_scope({}, pair, "history") != pair_feature_scope({}, other, "history")
    assert pair_feature_scope({}, pair, "ratings") == pair_feature_scope({}, other, "ratings")
    assert pair_feature_scope({"plex": {"token": "old"}}, pair, "history") == pair_feature_scope({"plex": {"token": "new"}}, pair, "history")


@pytest.mark.parametrize("feature", FEATURES)
@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_normal_and_interactive_execution_keep_pair_baselines_separate(config_base, monkeypatch, feature, mode):
    common, extra = feature_item(feature, 1), feature_item(feature, 2)
    cfg, src, dst = feature_setup(config_base, monkeypatch, feature, [common], [common], mode)
    cfg["pairs"].append({**deepcopy(cfg["pairs"][0]), "id": "p2"})
    store = StateStore(config_base)
    first_scope = pair_feature_scope(cfg, cfg["pairs"][0], feature, 1)
    second_scope = pair_feature_scope(cfg, cfg["pairs"][1], feature, 2)
    first = store.for_pair(first_scope)
    second = store.for_pair(second_scope)
    first.save_feature_blocks({("SRC", "default", feature): block(1, feature), ("DST", "default", feature): block(1, feature)})
    second.save_feature_blocks({("SRC", "default", feature): block(2, feature), ("DST", "default", feature): block(2, feature)})
    before = second.load_state_features({feature})
    src.index["imdb:tt0000002"] = extra
    review = InteractivePlan()
    Orchestrator(cfg, interactive=review).run(dry_run=True, pair_scope_ids=["p1"], write_state_json=False)
    assert not src.add_calls and not dst.add_calls
    assert any(row["operation"] == "add" and row["provider"] == "DST" for row in review.rows.values())
    assert not any(row["operation"] == "remove" for row in review.rows.values())
    result = Orchestrator(cfg).run(pair_scope_ids=["p1"])
    assert not result["errors"]
    assert dst.add_calls
    assert second.load_state_features({feature})["providers"] == before["providers"]
    assert "imdb:tt0000002" in first.load_state_features({feature})["providers"]["DST"][feature]["baseline"]["items"]


@pytest.mark.parametrize("provider,feature", [("KODI", "ratings"), ("KODI", "progress"), ("STREMIO", "history")])
def test_provider_baseline_reads_use_pair_and_exact_instance(config_base, provider, feature):
    from providers.sync.kodi._common import _load_kodi_feature_baseline
    from providers.sync.stremio._history import _load_stremio_history_baseline

    store = StateStore(config_base)
    store.save_feature_blocks({(provider, "default", feature): block(9, feature)})
    store.for_pair("a").save_feature_blocks({(provider, "default", feature): block(1, feature)})
    store.for_pair("b").save_feature_blocks({(provider, "default", feature): block(2, feature)})
    def load(scope, instance):
        adapter = SimpleNamespace(config={"_cw_pair_scope": scope}, instance_id=instance)
        return _load_kodi_feature_baseline(adapter, feature) if provider == "KODI" else _load_stremio_history_baseline(adapter)
    assert set(load("a", "default")) == {"imdb:tt0000001"}
    assert set(load("b", "default")) == {"imdb:tt0000002"}
    assert not load("new", "default")
    assert not load("a", "unknown")


def test_pair_storage_handles_50000_items_and_large_removal(config_base):
    store = StateStore(config_base).for_pair("large")
    items = {f"imdb:tt{number:07}": feature_item("history", number) for number in range(50000)}
    key = ("PLEX", "default", "history")
    store.save_feature_blocks({key: {"baseline": {"items": items}}})
    assert len(store.load_state_features({"history"})["providers"]["PLEX"]["history"]["baseline"]["items"]) == 50000
    store.save_feature_blocks({key: block(1, "history")})
    assert set(store.load_state_features({"history"})["providers"]["PLEX"]["history"]["baseline"]["items"]) == {"imdb:tt0000001"}


def test_two_way_pairs_in_one_run_do_not_share_previous_state(config_base, monkeypatch):
    common, extra = feature_item("watchlist", 1), feature_item("watchlist", 2)
    cfg, src, dst = feature_setup(config_base, monkeypatch, "watchlist", [common, extra], [common])
    cfg["sync"]["allow_mass_delete"] = True
    cfg["pairs"].append({**deepcopy(cfg["pairs"][0]), "id": "p2"})
    one = block(1)
    both = {"baseline": {"items": {**one["baseline"]["items"], **block(2)["baseline"]["items"]}}}
    for pair, baseline in zip(cfg["pairs"], (one, both)):
        store = StateStore(config_base).for_pair(pair_feature_scope(cfg, pair, "watchlist"))
        store.save_feature_blocks({("SRC", "default", "watchlist"): baseline, ("DST", "default", "watchlist"): baseline})
    events = []
    result = Orchestrator(cfg, on_progress=lambda line: events.append(json.loads(line)) if line.startswith("{") else None).run(dry_run=True)
    assert not result["errors"]
    plans = [event for event in events if event.get("event") == "two:plan"]
    assert len(plans) == 2
    assert plans[0]["add_to_B"] == 1 and plans[0]["rem_from_A"] == 0
    assert plans[1]["add_to_B"] == 0 and plans[1]["rem_from_A"] == 1


def test_tombstones_and_retries_are_pair_scoped(config_base, monkeypatch):
    from cw_platform.orchestrator._pairs import _pair_env
    from cw_platform.orchestrator._tombstones import add_keys_for_feature, keys_for_feature
    from cw_platform.orchestrator import _unresolved as unresolved

    store = StateStore(config_base)
    monkeypatch.setattr(unresolved, "STATE_DIR", config_base / ".cw_state")
    a, b = "cw2_pair_a", "cw2_pair_b"
    add_keys_for_feature(store, lambda *a, **k: None, "watchlist", ["imdb:tt0000001"], pair=a)
    assert keys_for_feature(store, "watchlist", pair=a)
    assert not keys_for_feature(store, "watchlist", pair=b)
    with _pair_env({}, i=1, src="PLEX", dst="SIMKL", mode="two-way", feature="watchlist", scope=a):
        unresolved.record_unresolved("SIMKL", "watchlist", [feature_item("watchlist", 1)], hint="test")
        assert unresolved.load_unresolved_pending("SIMKL", "watchlist")
    with _pair_env({}, i=2, src="PLEX", dst="SIMKL", mode="two-way", feature="watchlist", scope=b):
        assert not unresolved.load_unresolved_pending("SIMKL", "watchlist")


@pytest.mark.parametrize("provider", ["plex", "simkl", "trakt", "mdblist", "emby", "jellyfin", "tmdb", "tautulli", "anilist", "publicmetadb", "punchplay"])
def test_new_pair_provider_cache_does_not_inherit_legacy_state(config_base, monkeypatch, provider):
    module = importlib.import_module(f"providers.sync.{provider}._common")
    root = config_base / "cache"
    root.mkdir()
    monkeypatch.setattr(module, "STATE_DIR" if hasattr(module, "STATE_DIR") else "_STATE_DIR", root)
    (root / "audit.json").write_text('{"from_other_scope": true}', encoding="utf-8")
    for key in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        monkeypatch.setenv(key, "cw2_new_pair")
    path = module.state_file("audit.json")
    reader = getattr(module, "read_json", None) or getattr(module, "_read_json", None)
    if reader:
        assert not reader(path)
    assert not Path(path).exists()


def test_plex_local_ids_do_not_collide_between_servers(monkeypatch):
    from providers.sync.plex import _common as common

    monkeypatch.setattr(common, "_SHOW_PMS_GUID_CACHE", {})
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        number = "101" if "server-a" in url else "202"
        return SimpleNamespace(ok=True, headers={"content-type": "application/json"}, json=lambda: {"MediaContainer": {"Metadata": [{"Guid": [{"id": f"tmdb://{number}"}]}]}})
    monkeypatch.setattr(common.requests, "get", get)
    for server, expected in (("a", "101"), ("b", "202"), ("a", "101")):
        obj = SimpleNamespace(grandparentRatingKey="42", _server=SimpleNamespace(_baseurl=f"http://server-{server}", _token=f"fake-{server}"))
        assert common._hydrate_show_ids_from_pms(obj)["tmdb"] == expected
    assert len(calls) == 2


def test_plex_fallback_cache_is_saved_to_its_own_pair(config_base, monkeypatch):
    from cw_platform.orchestrator._pairs import _pair_env
    from providers.sync.plex import _common as common

    monkeypatch.setattr(common, "STATE_DIR", config_base)
    monkeypatch.setattr(common, "_FBGUID_MEMO", {})
    monkeypatch.setattr(common, "_FBGUID_MEMO_PATH", None)
    monkeypatch.setattr(common, "_FBGUID_MEMO_DIRTY", False)
    with _pair_env({}, i=1, src="PLEX", dst="SIMKL", mode="one-way", feature="history", scope="cw2_a"):
        common._fb_cache_load()["only-a"] = {"title": "A"}
        common._FBGUID_MEMO_DIRTY = True
        path_a = common._fbguid_cache_path()
    with _pair_env({}, i=2, src="PLEX", dst="SIMKL", mode="one-way", feature="history", scope="cw2_b"):
        assert not common._fb_cache_load()
        common._fb_cache_load()["only-b"] = {"title": "B"}
        common._FBGUID_MEMO_DIRTY = True
        common._fb_cache_flush()
        assert set(json.loads(common._fbguid_cache_path().read_text("utf-8"))) == {"only-b"}
    assert set(json.loads(path_a.read_text("utf-8"))) == {"only-a"}


def test_clearing_one_pair_keeps_other_pairs_and_inventory(config_base):
    store = StateStore(config_base)
    for scope in ("a", "b"):
        store.for_pair(scope).save_feature_blocks({("PLEX", "default", "watchlist"): block(1)})
    store.for_pair("a").clear_state()
    assert not store.for_pair("a").load_state()["providers"]
    assert store.for_pair("b").load_state()["providers"]
    assert store.load_state()["providers"]


def test_schema_upgrade_preserves_inventory_without_assigning_it_to_a_pair(config_base):
    from cw_platform.local_db import get_conn
    from cw_platform.local_db.schema import apply_schema

    store = StateStore(config_base)
    store.save_feature_blocks({("PLEX", "default", "watchlist"): block(1)})
    conn = get_conn(config_base)
    with conn:
        conn.execute("DROP TABLE pair_baseline_items")
        conn.execute("DROP TABLE pair_feature_state")
        conn.execute("DELETE FROM schema_migrations WHERE version=2")
    apply_schema(conn)
    apply_schema(conn)
    assert store.load_state()["providers"]["PLEX"]["watchlist"]["baseline"]["items"]
    assert not store.for_pair("new-pair").load_state()["providers"]
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_same_plex_instance_with_different_libraries(config_base, monkeypatch, mode):
    cfg, src, dst = feature_setup(config_base, monkeypatch, "history", [], [], mode)
    src.provider, dst.provider = "PLEX", "SIMKL"
    cfg["plex"] = {"history": {"libraries": ["1", "2"]}}
    cfg["pairs"][0].update(source="PLEX", target="SIMKL")
    cfg["pairs"][0]["features"]["history"]["libraries"] = {"PLEX": ["1"]}
    cfg["pairs"].append({**deepcopy(cfg["pairs"][0]), "id": "p2"})
    cfg["pairs"][1]["features"]["history"]["libraries"] = {"PLEX": ["2"]}
    cfg["pairs"][1]["target"] = "TRAKT"
    other_dst = deepcopy(dst)
    other_dst.provider = "TRAKT"
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"PLEX": src, "SIMKL": dst, "TRAKT": other_dst})
    def read(config, *, feature):
        number = int(config["plex"][feature]["libraries"][0])
        return {f"imdb:tt{number:07}": {**feature_item(feature, number), "library_id": str(number)}}
    src.build_index = read
    for pid in ("p1", "p2", "p1"):
        assert not Orchestrator(cfg).run(pair_scope_ids=[pid])["errors"]
    for pair, expected in zip(cfg["pairs"], (1, 2)):
        state = StateStore(config_base).for_pair(pair_feature_scope(cfg, pair, "history")).load_state()
        assert set(state["providers"]["PLEX"]["history"]["baseline"]["items"]) == {f"imdb:tt{expected:07}"}


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_pair_persistence_failure_is_reported(config_base, monkeypatch, mode):
    cfg, _, _ = feature_setup(config_base, monkeypatch, "watchlist", [feature_item("watchlist", 1)], [], mode)
    def fail(*args, **kwargs):
        raise RuntimeError("Cannot persist pair baseline")
    monkeypatch.setattr("cw_platform.local_db.state.save_pair_blocks", fail)
    assert Orchestrator(cfg).run()["errors"] == 1
