# tests/test_interactive_sync_report.py
# CrossWatch - Interactive Sync Completion Report Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
import json

import pytest

from cw_platform.orchestrator._interactive import InteractivePlan
from services.interactive_sync import Session
from services.interactive_sync_report import COUNTERS, SyncReport
from test_interactive_sync import run
from test_interactive_sync_features import FEATURES, feature_item, feature_setup


@pytest.mark.parametrize("feature", FEATURES)
@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_report_uses_actual_selected_execution_results(config_base, monkeypatch, feature, mode):
    cfg, src, dst = feature_setup(config_base, monkeypatch, feature,
                                  [feature_item(feature, 1), feature_item(feature, 2)], [], mode)
    preview = InteractivePlan()
    collector = SyncReport()
    preview.on_result = collector.record
    run(cfg, preview)
    assert collector.features == []
    selected = {next(iter(preview.rows))}
    execution = InteractivePlan(preview=False, selected=selected, on_result=collector.record)
    result = run(cfg, execution)
    report = collector.finish(Session(pair_id="p1", owner="local"), execution, result)
    assert report["outcome"] == "success"
    assert report["requested"] == report["reached_execution"] == 1
    assert report["not_reached"] == 0
    assert report["totals"]["added"] == 1
    assert len(collector.features) == 1
    assert {k: collector.features[0][k] for k in COUNTERS} == report["totals"]
    assert sum(row["added"] for row in collector.features[0]["destinations"]) == 1
    assert len(dst.add_calls[0]) == 1


def test_two_way_results_preserve_direction_and_instances():
    collector = SyncReport()
    collector.record("ratings", "PLEX", "TRAKT", "home", "other", "two-way", dict(
        adds_to_A=3, adds_to_B=7, upd_to_A=2, upd_to_B=4, rem_from_A=1, rem_from_B=5,
        skipped=6, errors=2, unresolved=3, unresolved_to_A=1, unresolved_to_B=2,
        resA_add={"skipped": 2, "errors": 1}, resB_update={"skipped": 4, "errors": 1}))
    row = collector.features[0]
    assert (row["added"], row["updated"], row["removed"], row["skipped"], row["errors"]) == (10, 6, 6, 6, 2)
    assert row["destinations"][0] == dict(provider="PLEX", instance="home", added=3, updated=2, removed=1, skipped=2, errors=1, unresolved=1)
    assert row["destinations"][1] == dict(provider="TRAKT", instance="other", added=7, updated=4, removed=5, skipped=4, errors=1, unresolved=2)


def test_playlist_results_do_not_invent_destination_or_item_receipts():
    collector = SyncReport()
    collector.record("playlists", "PLEX", "TRAKT", "default", "other", "two-way",
                     dict(added=240, updated=2, removed=8, errors=1, unresolved=4))
    row = collector.features[0]
    assert row["added"] == 240
    assert row["updated"] == 2
    assert row["destinations"] == []


@pytest.mark.parametrize("result,outcome", [(None, "incomplete"), ({"ok": True, "cancelled": True}, "cancelled"),
                                            ({"ok": True, "errors": 2}, "attention"), ({"ok": False}, "incomplete")])
def test_interrupted_and_failed_runs_never_claim_success(result, outcome):
    collector = SyncReport()
    collector.record("history", "PLEX", "SIMKL", "default", "default", "one-way", {"added": 8})
    execution = InteractivePlan(preview=False, selected={"a", "b"}, seen={"a"})
    report = collector.finish(Session(pair_id="p1", owner="local"), execution, result)
    assert report["outcome"] == outcome
    assert report["not_reached"] == 1
    assert report["requested"] == 2
    if result is None:
        assert report["totals"]["added"] == 8
        assert report["incomplete_note"]


def test_notices_are_bounded_and_do_not_export_raw_provider_errors():
    collector = SyncReport()
    for n in range(500):
        collector.event(json.dumps(dict(event="feature:error", feature="history", dst=f"provider{n}",
                                        error="secret-token", traceback="secret-token")))
    collector.event(dict(event="mass_delete:blocked", feature="watchlist"))
    assert len(collector.notices) == 100
    assert collector.notice_overflow == 401
    assert "secret-token" not in json.dumps(list(collector.notices.values()))


def test_report_is_session_scoped_and_survives_other_runs(config_base, monkeypatch):
    from api import syncAPI
    from services import interactive_sync as svc

    cfg, src, dst = feature_setup(config_base, monkeypatch, "watchlist", [feature_item("watchlist", 1)], [], "one-way")
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: deepcopy(cfg), lambda *_: None))
    session = Session(pair_id="p1", owner="local")
    session.pair = dict(source="SRC", target="DST", mode="one-way")
    try:
        svc.refresh(session, cfg, {})
        assert session.public()["report"] is None
        svc.apply(session, cfg, session.store.selected_ids())
        report = deepcopy(session.public()["report"])
        assert report["outcome"] == "success"
        assert report["proposed"] == report["requested"] == report["totals"]["added"] == 1
        assert report["not_selected"] == 0
        run(cfg, InteractivePlan(preview=False))
        assert session.public()["report"] == report
    finally:
        session.close()


def test_fatal_execution_still_keeps_a_report(config_base, monkeypatch):
    from api import syncAPI
    from services import interactive_sync as svc

    cfg, src, dst = feature_setup(config_base, monkeypatch, "watchlist", [feature_item("watchlist", 1)], [], "one-way")
    session = Session(pair_id="p1", owner="local")
    def crash(*args, **kwargs):
        raise RuntimeError("interrupted")
    monkeypatch.setattr(syncAPI, "_run_pairs_thread", crash)
    try:
        svc.refresh(session, cfg, {})
        with pytest.raises(RuntimeError, match="interrupted"):
            svc.apply(session, cfg, session.store.selected_ids())
        assert session.report["outcome"] == "incomplete"
        assert session.report["totals"]["added"] == 0
        assert session.report["not_reached"] == 1
    finally:
        session.close()
