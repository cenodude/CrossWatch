# tests/test_sync_feature_order.py
# CrossWatch - History ordering regression tests for normal and interactive sync.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy

import pytest

from cw_platform.orchestrator import Orchestrator
from cw_platform.orchestrator._interactive import InteractivePlan
from cw_platform.orchestrator._pairs import _feature_list_for_pair
from test_orchestrator_dry_run_no_side_effects import FakeOps, _cfg, _install


@pytest.mark.parametrize("features,expected", [
    ({"ratings": True, "history": True}, ["history", "ratings"]),
    ({"watchlist": False, "ratings": True, "history": True}, ["history", "ratings"]),
    ({"watchlist": True, "ratings": True, "history": True}, ["history", "watchlist", "ratings"]),
    ({"ratings": True, "watchlist": True, "history": True}, ["history", "ratings", "watchlist"]),
    ({"progress": True, "ratings": True, "history": True}, ["progress", "history", "ratings"]),
    ({"ratings": True, "history": {"enable": False}}, ["ratings"]),
    ({"ratings": False, "watchlist": True, "history": True}, ["history", "watchlist"]),
    ({"ratings": True, "progress": True}, ["ratings", "progress"]),
])
def test_history_precedes_ratings_without_changing_other_feature_order(features, expected):
    pair = {"features": features}
    original = deepcopy(pair)
    assert _feature_list_for_pair(pair) == expected
    assert pair == original


def test_explicit_single_feature_selection_is_preserved():
    pair = {"feature": "ratings", "features": {"ratings": True, "history": True}}
    assert _feature_list_for_pair(pair) == ["ratings"]


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("interactive", [False, True])
def test_pair_runner_executes_history_first(config_base, monkeypatch, mode, interactive):
    src, dst = FakeOps("SRC", {}), FakeOps("DST", {})
    _install(monkeypatch, src, dst, config_base / ".cw_state")
    for ops in (src, dst):
        ops.capabilities = lambda: {"features": {"history": True, "ratings": True}}
        ops.health = lambda *_a, **_k: {"ok": True, "status": "ok"}
    cfg = _cfg(False)
    cfg["pairs"][0].update(mode=mode, feature="multi", features={
        "watchlist": {"enable": False}, "ratings": {"enable": True}, "history": {"enable": True},
    })
    calls = []

    def run_feature(ctx, *args, feature, **kwargs):
        calls.append(feature)
        return {"ok": True}

    monkeypatch.setattr("cw_platform.orchestrator._pairs.run_one_way_feature", run_feature)
    monkeypatch.setattr("cw_platform.orchestrator._pairs.run_two_way_feature", run_feature)
    review = InteractivePlan(preview=False) if interactive else None
    result = Orchestrator(cfg, interactive=review).run()
    assert result["ok"]
    assert calls == ["history", "ratings"]
