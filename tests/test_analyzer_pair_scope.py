# tests/test_analyzer_pair_scope.py
# CrossWatch - Analyzer Pair Scope Regression Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json

import pytest

import services.analyzer as A
from cw_platform.orchestrator._state_store import StateStore
from cw_platform.pair_scope import pair_feature_scope


@pytest.fixture
def pair_state(config_base, monkeypatch):
    monkeypatch.setattr(A, "CONFIG_DIR", config_base)
    monkeypatch.setattr(A, "CWS_DIR", config_base / ".cw_state")
    for cache in ("_STATE_CACHE", "_SCOPED_ROWS_CACHE", "_ANALYSIS_CACHE"):
        monkeypatch.setattr(A, cache, {})
    cfg = {"pairs": [dict(id=pid, source="PLEX", target=target, enabled=True, mode="two-way",
                          features={feature: True for feature in A._ANALYZER_FEATURES})
                     for pid, target in (("plex-simkl", "SIMKL"), ("plex-mdblist", "MDBLIST"))]}
    monkeypatch.setattr(A, "_cfg", lambda: cfg)
    return StateStore(config_base), cfg


def save_pair(store, cfg, pid, blocks):
    pair = next(pair for pair in cfg["pairs"] if pair["id"] == pid)
    for feature in {key[2] for key in blocks}:
        scope = pair_feature_scope(cfg, pair, feature)
        store.for_pair(scope).save_feature_blocks({key: value for key, value in blocks.items() if key[2] == feature})


@pytest.mark.parametrize("feature", A._ANALYZER_FEATURES)
@pytest.mark.parametrize("destination", ["unread", "empty", "present"])
def test_shared_source_does_not_make_unread_destination_missing(pair_state, feature, destination):
    store, _cfg = pair_state
    item = dict(type="movie", title="Example", ids={"tmdb": "1"})
    block = {"baseline": {"items": {"tmdb:1": item}}}
    blocks = {("PLEX", "default", feature): block, ("SIMKL", "default", feature): block}
    if destination != "unread":
        blocks[("MDBLIST", "default", feature)] = block if destination == "present" else {"baseline": {"items": {}}}
    save_pair(store, _cfg, "plex-simkl", {key: value for key, value in blocks.items() if key[0] != "MDBLIST"})
    save_pair(store, _cfg, "plex-mdblist", {key: value for key, value in blocks.items() if key[0] != "SIMKL"})
    assert not A._cached_analysis("plex-simkl")["attention"]["counts"]["current_mismatch"]
    result = A._cached_analysis("plex-mdblist")
    missing = [row for row in result["problems"] if row["type"] == "missing_peer"]
    assert len(missing) == (1 if destination == "empty" else 0)
    assert all(row["targets"] == ["MDBLIST"] for row in missing)
    gaps = [row for row in result["snapshot_gaps"] if row["feature"] == feature]
    assert bool(gaps) == (destination == "unread")
    if gaps:
        assert gaps[0]["missing"] == ["MDBLIST"]
        assert not [row for row in result["pair_stats"] if row["feature"] == feature]
    detail = A._detail_for_item("plex-mdblist", "PLEX", feature, "tmdb:1")
    assert detail["targets"] == (["MDBLIST"] if destination == "empty" else [])


@pytest.mark.parametrize("feature", A._ANALYZER_FEATURES)
@pytest.mark.parametrize("scope_format", ["legacy", "runtime", "isolated"])
def test_retry_records_stay_with_their_pair_on_database_fallback(pair_state, feature, scope_format):
    from cw_platform.orchestrator._pairs import _pair_scope_key

    store, _cfg = pair_state
    store.save_feature_blocks({("PLEX", "default", feature): {"baseline": {"items": {}}}})
    scope = "plex-simkl" if scope_format == "legacy" else A._safe_scope(_pair_scope_key(_cfg["pairs"][0], i=0, src="PLEX", dst="SIMKL", mode="two-way"))
    if scope_format == "isolated":
        scope = pair_feature_scope(_cfg, _cfg["pairs"][0], feature)
    store.cw_state_dir.joinpath(f"PLEX_{feature}.unresolved.{scope}.json").write_text(json.dumps({
        "tmdb:1": {"item": {"type": "movie", "title": "Failed item", "ids": {"tmdb": "1"}}, "reason": "not_found"}
    }), encoding="utf-8")
    assert A._cached_analysis("plex-simkl")["attention"]["counts"]["pending_retry"] == int(scope_format == "isolated")
    assert A._cached_analysis("plex-mdblist")["attention"]["counts"]["pending_retry"] == 0
    assert A._cached_analysis("plex-simkl,plex-mdblist")["attention"]["counts"]["pending_retry"] == int(scope_format == "isolated")


def test_unknown_instance_does_not_use_default_instance_snapshot(pair_state):
    store, cfg = pair_state
    cfg["pairs"][1]["target_instance"] = "other"
    block = {"baseline": {"items": {"tmdb:1": {"type": "movie", "ids": {"tmdb": "1"}}}}}
    save_pair(store, cfg, "plex-mdblist", {("PLEX", "default", "watchlist"): block, ("MDBLIST", "default", "watchlist"): {"baseline": {"items": {}}}})
    result = A._cached_analysis("plex-mdblist")
    assert result["attention"]["counts"]["current_mismatch"] == 0
    assert any(row["missing"] == ["MDBLIST@other"] for row in result["snapshot_gaps"])


def test_empty_managed_pair_scope_does_not_read_other_pair_artifacts(pair_state):
    store, _cfg = pair_state
    store.save_feature_blocks({("PLEX", "default", "watchlist"): {"baseline": {"items": {}}}})
    store.cw_state_dir.joinpath("PLEX_watchlist.unresolved.plex-simkl.json").write_text(json.dumps({
        "tmdb:1": {"item": {"ids": {"tmdb": "1"}}, "reason": "not_found"}
    }), encoding="utf-8")
    result = A._cached_analysis(A._STRICT_PAIRS_PREFIX)
    assert result["attention"]["counts"]["pending_retry"] == 0
    assert A._cached_scoped_rows(A._STRICT_PAIRS_PREFIX)[0] == []


def test_history_normalization_does_not_compare_unread_destination(pair_state):
    store, _cfg = pair_state
    episodes = {f"tmdb:{n}#s01e01": dict(type="episode", title=f"Show {n}", series_title=f"Show {n}",
                                        show_ids={"tmdb": str(n)}, season=1, episode=1)
                for n in range(20)}
    store.save_feature_blocks({("PLEX", "default", "history"): {"baseline": {"items": episodes}}})
    result = A._cached_analysis("plex-mdblist")
    assert not [row for row in result["problems"] if row["type"] in ("missing_peer", "history_show_normalization")]


def test_legacy_snapshot_does_not_seed_a_new_pair_baseline(pair_state, config_base):
    store, _cfg = pair_state
    item = dict(type="movie", title="Shared provider item", ids={"tmdb": "1"})
    block = {"baseline": {"items": {"tmdb:1": item}}}
    store.save_feature_blocks({("PLEX", "default", "watchlist"): block})
    config_base.joinpath("state.plex-mdblist.json").write_text(json.dumps({
        "providers": {"PLEX": {"watchlist": {"baseline": {"items": {}}}},
                      "MDBLIST": {"watchlist": {"baseline": {"items": {}}}}}
    }), encoding="utf-8")
    result = A._cached_analysis("plex-mdblist")
    assert not result["attention"]["counts"]["current_mismatch"]
    assert not A._cached_scoped_rows("plex-mdblist")[0]


def test_new_destination_snapshot_invalidates_cached_pair_analysis(pair_state):
    store, _cfg = pair_state
    item = dict(type="movie", title="Example", ids={"tmdb": "1"})
    block = {"baseline": {"items": {"tmdb:1": item}}}
    save_pair(store, _cfg, "plex-mdblist", {("PLEX", "default", "watchlist"): block})
    before = A._cached_analysis("plex-mdblist")
    assert not before["attention"]["counts"]["current_mismatch"]
    save_pair(store, _cfg, "plex-mdblist", {("MDBLIST", "default", "watchlist"): {"baseline": {"items": {}}}})
    after = A._cached_analysis("plex-mdblist")
    assert after["attention"]["counts"]["current_mismatch"] == 1
    assert not [row for row in after["snapshot_gaps"] if row["feature"] == "watchlist"]


@pytest.mark.parametrize("feature", A._ANALYZER_FEATURES)
def test_all_pairs_are_compared_independently(pair_state, feature):
    store, cfg = pair_state
    first = {"baseline": {"items": {"tmdb:1": {"type": "movie", "title": "First library", "ids": {"tmdb": "1"}}}}}
    second = {"baseline": {"items": {"tmdb:2": {"type": "movie", "title": "Second library", "ids": {"tmdb": "2"}}}}}
    save_pair(store, cfg, "plex-simkl", {("PLEX", "default", feature): first, ("SIMKL", "default", feature): {"baseline": {"items": {}}}})
    save_pair(store, cfg, "plex-mdblist", {("PLEX", "default", feature): second, ("MDBLIST", "default", feature): second})
    assert not A._cached_analysis("plex-mdblist")["attention"]["counts"]["current_mismatch"]
    for selection in ("plex-simkl", "plex-simkl,plex-mdblist", None):
        result = A._cached_analysis(selection)
        missing = [row for row in result["problems"] if row["type"] == "missing_peer"]
        assert len(missing) == 1
        assert missing[0]["pair_id"] == "plex-simkl"
        assert missing[0]["key"] == "tmdb:1"
        assert missing[0]["targets"] == ["SIMKL"]
    rows, _ = A._cached_scoped_rows("plex-simkl,plex-mdblist")
    assert {row["key"] for row in rows if row["pair_id"] == "plex-mdblist"} == {"tmdb:2"}


def test_unsynced_pair_does_not_read_shared_inventory(pair_state):
    store, cfg = pair_state
    block = {"baseline": {"items": {"tmdb:1": {"type": "movie", "ids": {"tmdb": "1"}}}}}
    save_pair(store, cfg, "plex-simkl", {("PLEX", "default", "watchlist"): block, ("SIMKL", "default", "watchlist"): block})
    store.save_feature_blocks({("MDBLIST", "default", "watchlist"): {"baseline": {"items": {}}}})
    assert not A._cached_scoped_rows("plex-mdblist")[0]
    result = A._cached_analysis("plex-mdblist")
    assert result["attention"]["counts"]["current_mismatch"] == 0
    assert any(set(row["missing"]) == {"PLEX", "MDBLIST"} for row in result["snapshot_gaps"])


def test_empty_profile_cannot_reuse_all_profiles_cache(pair_state):
    store, cfg = pair_state
    block = {"baseline": {"items": {"tmdb:1": {"type": "movie", "ids": {"tmdb": "1"}}}}}
    save_pair(store, cfg, "plex-simkl", {("PLEX", "default", "watchlist"): block, ("SIMKL", "default", "watchlist"): {"baseline": {"items": {}}}})
    assert A._cached_analysis(None)["attention"]["counts"]["current_mismatch"] == 1
    assert A._cached_scoped_rows(None)[0]
    assert not A._cached_analysis(A._STRICT_PAIRS_PREFIX)["attention"]["counts"]["current_mismatch"]
    assert not A._cached_scoped_rows(A._STRICT_PAIRS_PREFIX)[0]
