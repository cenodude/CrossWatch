# tests/test_analyzer_episode_groups.py
# CrossWatch - Analyzer split and combined episode comparisons
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from copy import deepcopy

import pytest
import requests

import services.analyzer as A
from cw_platform.id_map import canonical_key
from cw_platform.local_db import episode_groups, manual_policy
from cw_platform.orchestrator._interactive import fingerprint
from cw_platform.episode_groups import endpoint, tokens
from cw_platform.pair_scope import pair_feature_scope
from test_analyzer_pair_scope import pair_state, save_pair
from test_episode_groups import group, watched


@pytest.fixture
def setup_groups(pair_state, config_base, monkeypatch):
    store, cfg = pair_state
    cfg["pairs"] = [dict(id="p1", source="SRC", target="DST", enabled=True, mode="two-way", features={"history": {"enable": True}})]
    policy = dict(version=1, providers={}, pairs={"p1": dict(providers={}, episode_groups=[group()])})
    manual_policy.save_policy(config_base, policy)
    def no_network(*args, **kwargs):
        pytest.fail("Analyzer episode groups must not call provider APIs")
    monkeypatch.setattr(requests.sessions.Session, "request", no_network)
    return store, cfg, policy


def snapshots(setup, parts, combined, *, pid="p1"):
    store, cfg, _ = setup
    pair = next(pair for pair in cfg["pairs"] if pair["id"] == pid)
    rows = [dict((canonical_key(watched(n)), watched(n)) for n in parts),
            {canonical_key(watched(2)): watched(2)} if combined else {}]
    save_pair(store, cfg, pid, {(pair[side], pair.get(f"{side}_instance", "default"), "history"):
        {"baseline": {"items": items}} for side, items in zip(("source", "target"), rows)})


@pytest.mark.parametrize("parts,combined,pending,waiting", [([], False, 0, 0), ([2], False, 0, 1),
    ([3], False, 0, 1), ([2, 3], False, 2, 0), ([], True, 1, 0), ([2], True, 1, 0), ([2, 3], True, 0, 0)])
def test_group_coverage_replaces_number_matching(setup_groups, parts, combined, pending, waiting):
    snapshots(setup_groups, parts, combined)
    result = A._cached_analysis("p1")
    missing = [row for row in result["problems"] if row["type"] == "missing_peer"]
    assert len(missing) == pending
    assert all(row["episode_groups"][0]["status"] == "pending" for row in missing)
    assert bool([row for row in result["problems"] if row["type"] == "history_episode_group_waiting"]) == bool(waiting)
    stats = result["pair_stats"]
    assert sum(row["unsynced"] for row in stats) == pending
    assert sum(row.get("episode_group_waiting", 0) for row in stats) == waiting
    assert sum(row["synced"] for row in stats) == len(parts) + int(combined) - pending - waiting
    if parts:
        detail = A._detail_for_item("p1", "SRC", "history", canonical_key(watched(parts[0])))
        assert detail["episode_groups"][0]["coverage"] == {"SRC": [2 in parts, 3 in parts], "DST": [combined]}
        assert not detail["target_show_info"]


def test_one_way_only_reports_writes_in_configured_direction(setup_groups):
    setup_groups[1]["pairs"][0]["mode"] = "one-way"
    snapshots(setup_groups, [2], True)
    result = A._cached_analysis("p1")
    assert not [row for row in result["problems"] if row["type"] == "missing_peer"]
    assert len(result["pair_stats"]) == 1
    assert result["pair_stats"][0]["synced"] == 1


def test_group_mapping_is_pair_scoped_even_with_same_endpoints(setup_groups):
    _, cfg, _ = setup_groups
    cfg["pairs"].append({**deepcopy(cfg["pairs"][0]), "id": "p2"})
    snapshots(setup_groups, [2, 3], True)
    snapshots(setup_groups, [2, 3], True, pid="p2")
    result = A._cached_analysis("p1,p2")
    missing = [row for row in result["problems"] if row["type"] == "missing_peer"]
    assert len(missing) == 1 and missing[0]["pair_id"] == "p2"
    assert missing[0]["episode"] == 3


def test_saved_hold_is_read_without_mutating_runtime_state(setup_groups, config_base):
    _, cfg, policy = setup_groups
    value = group()
    signature = fingerprint({side: dict(endpoint=endpoint(value[side]), episodes=sorted(
        sorted(tokens(ep)) for ep in value[side]["episodes"])) for side in ("source", "target")})
    scope = pair_feature_scope(cfg, cfg["pairs"][0], "history")
    previous = {"finale": dict(held=True, mapping=signature, coverage={"source": [True, False], "target": [True]})}
    episode_groups.save(config_base, scope, previous)
    snapshots(setup_groups, [2], True)
    result = A._cached_analysis("p1")
    assert not [row for row in result["problems"] if row["type"] == "missing_peer"]
    assert any("marked unwatched" in row["message"] for row in result["problems"] if row["type"] == "history_episode_group_held")
    assert sum(row.get("episode_group_held", 0) for row in result["pair_stats"]) == 2
    assert episode_groups.load(config_base, scope) == previous
    assert manual_policy.load_policy(config_base) == policy
    snapshots(setup_groups, [2, 3], True)
    assert not [row for row in A._cached_analysis("p1")["problems"] if row["type"] == "history_episode_group_held"]
    assert episode_groups.load(config_base, scope) == previous


@pytest.mark.parametrize("blocked", ["shared", "pair", "specials", "rewatches"])
def test_group_holds_match_sync_policy(setup_groups, config_base, monkeypatch, blocked):
    _, cfg, policy = setup_groups
    if blocked == "specials":
        cfg["pairs"][0]["features"]["history"]["include_specials"] = False
        policy["pairs"]["p1"]["episode_groups"][0]["source"]["episodes"][0]["season"] = 0
    elif blocked == "rewatches":
        from services import analyzer_episode_groups as groups
        cfg["pairs"][0]["features"]["history"]["rewatches"] = True
        class Ops:
            def capabilities(self):
                return {"history": {"rewatches": True}}
        monkeypatch.setattr(groups, "provider_ops", lambda _: Ops())
    else:
        target = policy if blocked == "shared" else policy["pairs"]["p1"]
        target["providers"] = {"SRC": {"history": {"blocks": [canonical_key(watched(2))]}}}
    manual_policy.save_policy(config_base, policy)
    snapshots(setup_groups, [3], True)
    result = A._cached_analysis("p1")
    assert any(row["type"] == "history_episode_group_held" for row in result["problems"])
    assert not [row for row in result["problems"] if row["type"] == "missing_peer"]


def test_unsupported_rewatches_do_not_hold_watched_state_groups(setup_groups):
    setup_groups[1]["pairs"][0]["features"]["history"]["rewatches"] = True
    snapshots(setup_groups, [2, 3], True)
    result = A._cached_analysis("p1")
    assert not [row for row in result["problems"] if row["type"] in {"history_episode_group_held", "missing_peer"}]
    assert sum(row["synced"] for row in result["pair_stats"]) == 3


def test_group_policy_changes_invalidate_cached_analysis(setup_groups, config_base):
    _, _, policy = setup_groups
    snapshots(setup_groups, [2, 3], True)
    assert not A._cached_analysis("p1")["attention"]["counts"]["current_mismatch"]
    policy["pairs"]["p1"]["episode_groups"] = []
    manual_policy.save_policy(config_base, policy)
    assert A._cached_analysis("p1")["attention"]["counts"]["current_mismatch"] == 1


def test_missing_snapshot_is_reported_without_assuming_unwatched(setup_groups):
    store, cfg, _ = setup_groups
    save_pair(store, cfg, "p1", {("SRC", "default", "history"): {"baseline": {"items": {canonical_key(watched(2)): watched(2)}}}})
    result = A._cached_analysis("p1")
    assert not [row for row in result["problems"] if row["type"] == "missing_peer"]
    assert result["snapshot_gaps"]
    assert any("snapshot is missing" in row["message"] for row in result["problems"] if row["type"] == "history_episode_group_held")


def test_instances_are_matched_individually(setup_groups, config_base):
    _, cfg, policy = setup_groups
    cfg["pairs"][0].update(target="SRC", target_instance="other")
    policy["pairs"]["p1"]["episode_groups"][0]["target"].update(provider="SRC", instance="other")
    manual_policy.save_policy(config_base, policy)
    snapshots(setup_groups, [2, 3], True)
    result = A._cached_analysis("p1")
    assert not [row for row in result["problems"] if row["type"] == "missing_peer"]
    assert {row["target"] for row in result["pair_stats"]} == {"SRC", "SRC@other"}


@pytest.mark.parametrize("parts,combined,status", [([2], False, "waiting"), ([2, 3], False, "pending"), ([2, 3], True, "synced")])
def test_group_retries_are_only_resolved_when_destination_is_complete(setup_groups, monkeypatch, parts, combined, status):
    snapshots(setup_groups, parts, combined)
    item = watched(2)
    record = dict(provider="DST", instance="default", feature="history", item=item, key=canonical_key(item),
                  alias_keys=[canonical_key(item)], reason="not_found")
    before = deepcopy(record)
    monkeypatch.setattr(A, "_unresolved_records", lambda _: [record])
    result = A._cached_analysis("p1")["attention"]
    assert result["counts"]["pending_retry"] == int(status != "synced")
    if status == "synced":
        assert result["counts"]["episode_group_resolved"] == 1
    else:
        assert next(row for row in result["rows"] if row["unresolved"])["episode_groups"][0]["status"] == status
    assert record == before


def test_all_items_exposes_group_counts_and_status(setup_groups):
    snapshots(setup_groups, [2], False)
    rows, _ = A._cached_scoped_rows("p1")
    assert rows[0]["episode_groups"][0]["status"] == "waiting"
    assert rows[0]["episode_groups"][0]["coverage"] == {"SRC": [True, False], "DST": [False]}


def test_ordinary_episode_cannot_claim_a_reserved_group_destination(setup_groups, config_base):
    _, _, policy = setup_groups
    policy["pairs"]["p1"]["episode_groups"][0]["source"]["episodes"] = [watched(4), watched(5)]
    for item in policy["pairs"]["p1"]["episode_groups"][0]["source"]["episodes"]:
        item.pop("watched_at")
    manual_policy.save_policy(config_base, policy)
    snapshots(setup_groups, [2, 4, 5], True)
    missing = [row for row in A._cached_analysis("p1")["problems"] if row["type"] == "missing_peer"]
    assert len(missing) == 1
    assert missing[0]["provider"] == "SRC" and missing[0]["episode"] == 2


def test_invalid_saved_group_is_visible_as_an_error(setup_groups, config_base):
    _, _, policy = setup_groups
    policy["pairs"]["p1"]["episode_groups"][0]["target"]["instance"] = "other"
    manual_policy.save_policy(config_base, policy)
    snapshots(setup_groups, [2, 3], True)
    result = A._cached_analysis("p1")
    assert any(row["type"] == "history_episode_group_invalid" and row["severity"] == "error" for row in result["problems"])


def test_group_presence_does_not_resolve_removal_retries(setup_groups, monkeypatch):
    snapshots(setup_groups, [2, 3], True)
    item = watched(2)
    record = dict(provider="DST", instance="default", feature="history", item=item, key=canonical_key(item),
                  alias_keys=[canonical_key(item)], reason="apply:remove_failed")
    monkeypatch.setattr(A, "_unresolved_records", lambda _: [record])
    assert A._cached_analysis("p1")["attention"]["counts"]["pending_retry"] == 1


def test_group_retry_cannot_open_an_ordinary_mapping(setup_groups, monkeypatch):
    from fastapi import HTTPException
    from services.analyzer_mapping import MappingRequest, mapping_row

    snapshots(setup_groups, [2], False)
    item = watched(2)
    record = dict(provider="DST", instance="default", feature="history", item=item, key=canonical_key(item),
                  alias_keys=[canonical_key(item)], reason="not_found")
    monkeypatch.setattr(A, "_unresolved_records", lambda _: [record])
    with pytest.raises(HTTPException) as error:
        mapping_row(MappingRequest(pair_id="p1", provider="DST", feature="history", key=canonical_key(item)), None)
    assert error.value.status_code == 409
    assert "Episode groups" in error.value.detail
