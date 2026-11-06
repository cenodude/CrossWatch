# tests/test_interactive_sync_features.py
# CrossWatch - Interactive Sync Feature Parity Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
import json
import time

import pytest

from cw_platform.id_map import canonical_key
from cw_platform.orchestrator import Orchestrator
from cw_platform.pair_scope import pair_feature_scope
from cw_platform.orchestrator._interactive import InteractivePlan
from cw_platform.orchestrator._state_store import StateStore
from test_interactive_sync import item, run, setup_ops
from test_orchestrator_dry_run_no_side_effects import _cfg


FEATURES = ["watchlist", "history", "ratings", "progress", "collection"]


def feature_item(feature, number):
    return item(number, **{
        "watchlist": {},
        "history": {"watched_at": "2024-01-01T12:00:00Z"},
        "ratings": {"rating": 8, "rated_at": "2024-01-01T12:00:00Z"},
        "progress": {"progress_percent": 30, "progress_ms": 180000, "duration_ms": 600000, "progress_at": "2024-01-01T12:00:00Z"},
        "collection": {"collected_at": "2024-01-01T12:00:00Z"},
    }[feature])


def feature_setup(config_base, monkeypatch, feature, source, target, mode="two-way"):
    src, dst = setup_ops(config_base, monkeypatch, source, target)
    for ops in (src, dst):
        ops.features = lambda: {feature: True}
        ops.capabilities = lambda: {"features": {feature: True}, "index_semantics": "present"}
        ops.health = lambda *_a, **_k: {"ok": True, "status": "ok", "features": {feature: True}}
    cfg = _cfg(False)
    cfg["sync"].update(enable_remove=True, include_observed_deletes=True, tombstone_ttl_days=30)
    cfg["pairs"][0].update(mode=mode, feature=feature, features={feature: {"enable": True, "add": True, "remove": True, "mode": "all"}})
    return cfg, src, dst


@pytest.mark.parametrize("feature", FEATURES)
@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_expired_deletion_does_not_change_preview_from_normal_plan(config_base, monkeypatch, feature, mode):
    common, extra = feature_item(feature, 1), feature_item(feature, 2)
    cfg, src, dst = feature_setup(config_base, monkeypatch, feature, [common, extra], [common], mode)
    run(cfg, InteractivePlan(preview=False))
    store = StateStore(config_base)
    store.save_tomb({"keys": {f"{feature}:{pair_feature_scope(cfg, cfg['pairs'][0], feature).upper()}|{canonical_key(extra)}": int(time.time()) - 31 * 86400}})
    before = deepcopy(store.load_tomb())
    plan = InteractivePlan()
    run(cfg, plan)
    assert store.load_tomb() == before
    events = []
    Orchestrator(cfg, on_progress=lambda line: events.append(json.loads(line)) if line.startswith("{") else None).run(dry_run=True, pair_scope_ids=["p1"], write_state_json=False)
    assert dst.add_calls and len(dst.add_calls[-1]) == 1
    assert canonical_key(dst.add_calls[-1][0]) == canonical_key(extra)
    assert [(row["operation"], row["provider"], row["key"]) for row in plan.rows.values()] == [("add", "DST", canonical_key(extra))]
    assert not any(event.get("event") == "apply:remove:start" and event.get("count") for event in events)


@pytest.mark.parametrize("feature", FEATURES)
@pytest.mark.parametrize("source", ["SRC", "DST"])
def test_two_way_feature_only_applies_selected_addition(config_base, monkeypatch, feature, source):
    common = feature_item(feature, 1)
    cfg, src, dst = feature_setup(config_base, monkeypatch, feature, [common], [common])
    run(cfg, InteractivePlan(preview=False))
    origin, destination = (src, dst) if source == "SRC" else (dst, src)
    for n in (2, 3):
        row = feature_item(feature, n)
        origin.index[canonical_key(row)] = row
    plan = InteractivePlan()
    run(cfg, plan)
    assert len(plan.rows) == 2
    assert {row["operation"] for row in plan.rows.values()} == {"add"}
    selected = {rid for rid, row in plan.rows.items() if row["key"] == canonical_key(feature_item(feature, 3))}
    assert len(selected) == 1
    run(cfg, InteractivePlan(preview=False, selected=selected))
    assert len(destination.add_calls) == 1
    assert [canonical_key(row) for row in destination.add_calls[0]] == [canonical_key(feature_item(feature, 3))]
    assert not origin.add_calls


@pytest.mark.parametrize("feature", FEATURES)
@pytest.mark.parametrize("scenario", ["active_tomb", "observed_delete", "removals_disabled"])
def test_two_way_deletion_safety_matches_normal_plan(config_base, monkeypatch, feature, scenario):
    common, extra = feature_item(feature, 1), feature_item(feature, 2)
    target = [common] if scenario == "active_tomb" else [common, extra]
    cfg, src, dst = feature_setup(config_base, monkeypatch, feature, [common, extra], target)
    run(cfg, InteractivePlan(preview=False))
    store = StateStore(config_base)
    if scenario == "active_tomb":
        store.save_tomb({"keys": {f"{feature}:{pair_feature_scope(cfg, cfg['pairs'][0], feature).upper()}|{canonical_key(extra)}": int(time.time()) - 86400}})
    else:
        dst.index.pop(canonical_key(extra))
    if scenario == "removals_disabled":
        cfg["pairs"][0]["features"][feature]["remove"] = False
    before_tomb = deepcopy(store.load_tomb())
    before_state = deepcopy(store.load_state())
    plan = InteractivePlan()
    assert run(cfg, plan)["ok"]
    assert store.load_tomb() == before_tomb
    assert store.load_state() == before_state
    assert not src.add_calls and not dst.add_calls
    events = []
    Orchestrator(cfg, on_progress=lambda line: events.append(json.loads(line)) if line.startswith("{") else None).run(dry_run=True, pair_scope_ids=["p1"], write_state_json=False)
    normal = next(event for event in events if event.get("event") == "two:plan")
    for operation, field in (("add", "add_to"), ("update", "upd_to"), ("remove", "rem_from")):
        for provider, side in (("SRC", "A"), ("DST", "B")):
            assert sum(row["operation"] == operation and row["provider"] == provider for row in plan.rows.values()) == normal[f"{field}_{side}"]
    if scenario == "removals_disabled":
        assert not any(row["operation"] == "remove" for row in plan.rows.values())


def test_progress_conflict_choice_is_replanned_and_applied(config_base, monkeypatch):
    left = {**feature_item("progress", 1), "progress_percent": 0, "progress_ms": 0}
    right = {**left, "progress_percent": 60, "progress_ms": 360000}
    cfg, src, dst = feature_setup(config_base, monkeypatch, "progress", [left], [right])
    plan = InteractivePlan()
    run(cfg, plan)
    assert len(plan.conflicts) == 1
    choices = {next(iter(plan.conflicts)): "DST"}
    refreshed = InteractivePlan(choices=choices)
    run(cfg, refreshed)
    assert len(refreshed.rows) == 1
    assert {row["provider"] for row in refreshed.rows.values()} == {"SRC"}
    run(cfg, InteractivePlan(preview=False, selected=set(refreshed.rows), choices=choices))
    assert src.add_calls[0][0]["progress_ms"] == 360000
    assert not dst.add_calls


@pytest.mark.parametrize("feature", FEATURES)
def test_mapping_recalculation_preserves_feature_values(config_base, monkeypatch, feature):
    from api import editorAPI

    original = feature_item(feature, 1)
    cfg, src, dst = feature_setup(config_base, monkeypatch, feature, [original], [])
    first = InteractivePlan()
    run(cfg, first)
    assert len(first.rows) == 1
    corrected = feature_item(feature, 2)
    monkeypatch.setattr(editorAPI, "_STATE_BASE", config_base)
    editorAPI._save_policy_manual(feature, "SRC", {canonical_key(corrected): corrected}, [canonical_key(original)], merge=True)
    refreshed = InteractivePlan()
    run(cfg, refreshed)
    assert len(refreshed.rows) == 1
    row = next(iter(refreshed.rows.values()))
    assert row["key"] == canonical_key(corrected)
    assert not set(first.rows) & set(refreshed.rows)
    first_payload = next(iter(first.rows.values()))["item"]
    for key in ("watched_at", "rating", "progress_ms", "collected_at"):
        if key in first_payload:
            assert row["item"][key] == first_payload[key]
    assert not src.add_calls and not dst.add_calls


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_history_rewatches_apply_only_selected_play(config_base, monkeypatch, mode):
    from cw_platform.history_events import history_sync_key

    cfg, src, dst = feature_setup(config_base, monkeypatch, "history", [], [], mode)
    cfg["pairs"][0]["features"]["history"]["rewatches"] = True
    for ops in (src, dst):
        ops.capabilities = lambda: {"features": {"history": True}, "index_semantics": "present", "history": {"rewatches": {"read": True, "write": True}}}
    first = feature_item("history", 1)
    second = {**first, "watched_at": "2024-02-01T12:00:00Z"}
    src.index = {history_sync_key(row, event_mode=True): row for row in (first, second)}
    plan = InteractivePlan()
    run(cfg, plan)
    assert len(plan.rows) == 2
    selected = {rid for rid, row in plan.rows.items() if row["item"].get("watched_at") == second["watched_at"]}
    assert len(selected) == 1
    run(cfg, InteractivePlan(preview=False, selected=selected))
    assert len(dst.add_calls) == 1 and len(dst.add_calls[0]) == 1
    assert dst.add_calls[0][0]["watched_at"] == second["watched_at"]


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("failure", ["read", "late", "result", "all", "cancelled", "recheck_changed", "recheck_failed"])
def test_failed_feature_does_not_block_completed_feature(config_base, monkeypatch, mode, failure):
    from api import syncAPI
    from cw_platform.orchestrator import _pairs
    from services import interactive_sync as svc

    cfg, src, dst = feature_setup(config_base, monkeypatch, "watchlist", [item(1)], [], mode)
    cfg["pairs"][0].update(feature="multi", features={
        name: {"enable": True, "add": True, "remove": True}
        for name in ("history", "watchlist")
    })
    for ops in (src, dst):
        ops.features = lambda: {"watchlist": True, "history": True}
        ops.capabilities = lambda: {"features": {"watchlist": True, "history": True}, "index_semantics": "present"}
        ops.health = lambda *_a, **_k: {"ok": True, "features": {"watchlist": True, "history": True}}
    history_calls = []
    original_read = src.build_index

    def read(config, *, feature):
        if feature == "history":
            history_calls.append(feature)
            raise RuntimeError("invalid history record")
        return original_read(config, feature=feature)

    monkeypatch.setattr(src, "build_index", read)
    runner_name = "run_two_way_feature" if mode == "two-way" else "run_one_way_feature"
    original_runner = getattr(_pairs, runner_name)

    def runner(ctx, *args, feature, **kwargs):
        if ctx.interactive.allowed_features is not None and feature == "watchlist":
            if failure == "recheck_changed":
                return {}
            if failure == "recheck_failed":
                raise RuntimeError("watchlist recheck failed")
        if failure == "all":
            raise RuntimeError("provider unavailable")
        if feature == "history" and failure != "read":
            history_calls.append(feature)
            ctx.interactive.filter("history", "DST", "default", "remove", [item(2)], source="SRC")
            ctx.interactive.conflict("history", "imdb:tt0000002", "SRC", "DST", item(2), item(2), "SRC")
            if failure == "result":
                return {"errors": 1}
            if failure == "cancelled":
                return {"cancelled": True}
            raise RuntimeError("history failed after proposing changes")
        return original_runner(ctx, *args, feature=feature, **kwargs)

    monkeypatch.setattr(_pairs, runner_name, runner)
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: deepcopy(cfg), lambda *_: None))
    session = svc.Session(pair_id="p1", owner="local")
    try:
        svc.refresh(session, cfg, {})
        if failure in ("all", "cancelled"):
            assert session.status == "error"
            assert not dst.add_calls
            return
        assert session.status == "review", session.message
        assert session.public()["failed_features"] == ["history"]
        assert session.public()["completed_features"] == ["watchlist"]
        assert session.store.features == ["watchlist"]
        assert not session.plan.conflicts
        assert history_calls == ["history"]
        previous_notices = deepcopy(session.plan.notices)
        svc.apply(session, cfg, session.store.selected_ids())
        if failure.startswith("recheck_"):
            assert session.status == ("review" if failure == "recheck_changed" else "error")
            assert "history" in session.public()["failed_features"]
            assert all(notice in session.plan.notices for notice in previous_notices)
            assert history_calls == ["history"]
            assert not dst.add_calls
            if failure == "recheck_failed":
                assert session.plan.failed_features == {"history", "watchlist"}
                assert any(notice.get("feature") == "watchlist" for notice in session.plan.notices)
            else:
                assert "Some selected proposals changed" in session.message
            monkeypatch.setattr(_pairs, runner_name, lambda ctx, *args, feature, **kwargs:
                                {} if feature == "history" else original_runner(ctx, *args, feature=feature, **kwargs))
            svc.refresh(session, cfg, {})
            assert session.status == "review"
            assert not session.plan.failed_features
            assert not any(notice.get("event") == "feature:error" for notice in session.plan.notices)
            return
        assert session.status == "complete", session.message
        assert len(dst.add_calls) == 1
        assert [canonical_key(row) for row in dst.add_calls[0]] == [canonical_key(item(1))]
        assert history_calls == ["history"]
        assert {row["feature"] for row in session.report["features"]} == {"watchlist"}
    finally:
        session.close()
