# tests/test_sync_bar_progress.py
# CrossWatch - Server-owned sync bar progress tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

import pytest

from api import syncAPI
from cw_platform.orchestrator import Orchestrator
from cw_platform.orchestrator._interactive import InteractivePlan
from services import interactive_sync as interactive
from test_interactive_sync import item, setup_ops
from test_interactive_sync_features import feature_setup
from test_orchestrator_dry_run_no_side_effects import _cfg


@pytest.fixture
def summary(monkeypatch):
    monkeypatch.setattr(syncAPI, "_rt", lambda: (None,) * 7 + (lambda text: text,))
    syncAPI._summary_reset()
    yield syncAPI.SUMMARY
    syncAPI._summary_reset()


def send(event: str, **data: Any) -> dict[str, Any]:
    syncAPI._parse_sync_line(json.dumps({"event": event, **data}))
    return syncAPI.SUMMARY["_phase"]["apply"]


def pair(i: int = 1, n: int = 1, features: tuple[str, ...] = ("history",)) -> None:
    send("run:pair", i=i, n=n, features=list(features))
    send("run:feature", i=1, n=len(features), feature=features[0])


def write(op: str, dst: str, feature: str, count: int, seen: list[float] | None = None, attempted: int | None = None) -> None:
    send(f"apply:{op}:start", dst=dst, feature=feature, count=count)
    step = max(1, count // 4)
    for done in range(step, count, step):
        phase = send(f"apply:{op}:progress", dst=dst, feature=feature, done=done, total=count)
        if seen is not None:
            seen.append(phase["progress"])
    send(f"apply:{op}:done", dst=dst, feature=feature, count=count, attempted=count if attempted is None else attempted)


def test_a_tiny_write_does_not_use_up_the_bar(summary):
    pair()
    send("one:plan", src="STREMIO", dst="SCROB", feature="history", adds=1554, removes=0, updates=1)
    write("update", "SCROB", "history", 1)
    assert send("feature:start", feature="history")["progress"] < 0.01
    assert send("apply:add:start", dst="SCROB", feature="history", count=1554)["progress"] < 0.01
    assert send("apply:add:progress", dst="SCROB", feature="history", done=777, total=1554)["progress"] == pytest.approx(0.5, abs=0.001)


def test_two_way_between_instances_of_one_provider_counts_both_directions(summary):
    pair()
    send("two:plan", a="TRAKT", b="TRAKT", feature="history", add_to_A=100, add_to_B=100, upd_to_A=0, upd_to_B=0, rem_from_A=0, rem_from_B=0)
    write("add", "TRAKT", "history", 100)
    assert summary["_phase"]["apply"]["progress"] == 0.5
    assert send("apply:add:start", dst="TRAKT", feature="history", count=100)["progress"] == 0.5
    assert send("apply:add:progress", dst="TRAKT", feature="history", done=50, total=100)["progress"] == 0.75
    assert send("apply:add:done", dst="TRAKT", feature="history", count=100, attempted=100)["progress"] == 1.0


def test_two_pairs_writing_to_one_destination_do_not_overwrite_each_other(summary):
    pair(1, 2)
    send("one:plan", src="PLEX", dst="SCROB", feature="history", adds=100, removes=0, updates=0)
    write("add", "SCROB", "history", 100)
    send("feature:done", feature="history")
    assert summary["_phase"]["apply"]["progress"] == 0.5
    pair(2, 2)
    send("one:plan", src="JELLYFIN", dst="SCROB", feature="history", adds=100, removes=0, updates=0)
    assert send("apply:add:start", dst="SCROB", feature="history", count=100)["progress"] == 0.5
    assert send("apply:add:progress", dst="SCROB", feature="history", done=50, total=100)["progress"] == 0.75


def test_the_first_large_feature_does_not_use_up_a_multi_feature_run(summary):
    features = ("watchlist", "history", "ratings")
    pair(features=features)
    send("one:plan", src="A", dst="B", feature="watchlist", adds=500, removes=0, updates=0)
    write("add", "B", "watchlist", 500)
    assert summary["_phase"]["apply"]["progress"] == pytest.approx(1 / 3, abs=0.001)
    send("feature:done", feature="watchlist")
    send("run:feature", i=2, n=3, feature="history")
    send("one:plan", src="A", dst="B", feature="history", adds=10, removes=0, updates=0)
    assert send("apply:add:progress", dst="B", feature="history", done=5, total=10)["progress"] == 0.5


def test_an_unsupported_feature_does_not_hold_back_the_next_one(summary):
    pair(features=("ratings", "history"))
    send("feature:unsupported", feature="ratings", src_supported=False, dst_supported=True)
    send("run:feature", i=2, n=2, feature="history")
    send("one:plan", src="A", dst="B", feature="history", adds=100, removes=0, updates=0)
    assert send("apply:add:progress", dst="B", feature="history", done=50, total=100)["progress"] == 0.75
    assert send("apply:add:done", dst="B", feature="history", count=100, attempted=100)["progress"] == 1.0


def test_a_playlist_write_does_not_leak_into_the_next_feature(summary):
    pair(features=("playlists", "history"))
    send("apply:add:done", dst="B", feature="playlists", count=100, attempted=100)
    assert send("run:feature", i=2, n=2, feature="history")["progress"] == 0.5
    send("one:plan", src="A", dst="B", feature="history", adds=100, removes=0, updates=0)
    assert send("apply:add:progress", dst="B", feature="history", done=50, total=100)["progress"] == 0.75
    assert send("apply:add:done", dst="B", feature="history", count=100, attempted=100)["progress"] == 1.0


def test_a_repeated_finish_event_counts_once(summary):
    pair(features=("watchlist", "history"))
    send("feature:done", feature="watchlist")
    assert send("feature:done", feature="watchlist")["progress"] == 0.5


def test_features_without_changes_still_move_the_run_forward(summary):
    pair(features=("watchlist", "history"))
    send("one:plan", src="A", dst="B", feature="watchlist", adds=0, removes=0, updates=0)
    assert send("feature:done", feature="watchlist")["progress"] == 0.5


def test_progress_never_moves_backwards_over_a_mixed_run(summary):
    seen: list[float] = []
    features = ("watchlist", "history")
    for index in (1, 2):
        pair(index, 2, features)
        for position, feature in enumerate(features, 1):
            send("run:feature", i=position, n=len(features), feature=feature)
            send("one:plan", src="A", dst="B", feature=feature, adds=40, removes=8, updates=4)
            write("add", "B", feature, 40, seen)
            write("remove", "B", feature, 8, seen)
            write("update", "B", feature, 4, seen)
            seen.append(send("feature:done", feature=feature)["progress"])
    assert seen == sorted(seen)
    assert seen[-1] == 1.0


def test_unresolved_items_count_as_processed(summary):
    pair()
    send("one:plan", src="STREMIO", dst="SCROB", feature="history", adds=1554, removes=0, updates=0)
    write("add", "SCROB", "history", 1530, attempted=1554)
    phase = summary["_phase"]["apply"]
    assert phase["done"] == 1554
    assert phase["progress"] == 1.0
    assert summary["features"]["history"]["added"] == 1530


def test_plans_that_shrink_or_overrun_stay_in_range(summary):
    pair()
    send("one:plan", src="A", dst="B", feature="history", adds=10, removes=0, updates=0)
    write("add", "B", "history", 25)
    assert summary["_phase"]["apply"]["progress"] == 1.0


def test_a_later_plan_cannot_pull_progress_back(summary):
    pair()
    send("one:plan", src="A", dst="B", feature="history", adds=10, removes=0, updates=0)
    write("add", "B", "history", 10)
    assert send("one:plan", src="A", dst="B", feature="history", adds=10, removes=0, updates=0)["progress"] == 1.0


def test_a_partial_selection_replaces_the_planned_count(summary):
    pair()
    send("one:plan", src="A", dst="B", feature="history", adds=1554, removes=0, updates=0)
    phase = send("plan:selected", feature="history", count=10)
    assert (phase["total"], phase["step_planned"]) == (10, 10)
    assert send("apply:add:progress", dst="B", feature="history", done=5, total=10)["progress"] == 0.5


def test_a_partial_selection_keeps_earlier_features_in_the_run_total(summary):
    pair(features=("watchlist", "history"))
    send("one:plan", src="A", dst="B", feature="watchlist", adds=30, removes=0, updates=0)
    send("plan:selected", feature="watchlist", count=30)
    send("run:feature", i=2, n=2, feature="history")
    send("one:plan", src="A", dst="B", feature="history", adds=100, removes=0, updates=0)
    assert send("plan:selected", feature="history", count=4)["total"] == 34


def test_selecting_nothing_for_a_feature_still_finishes_it(summary):
    pair()
    send("one:plan", src="A", dst="B", feature="history", adds=100, removes=0, updates=0)
    assert send("plan:selected", feature="history", count=0)["total"] == 0
    assert send("feature:done", feature="history")["progress"] == 1.0


def test_a_write_without_a_planned_total_does_not_divide_by_zero(summary):
    pair()
    assert send("apply:add:progress", dst="B", feature="history", done=5, total=10)["progress"] == 0


def test_malformed_values_do_not_break_the_parser(summary):
    pair()
    assert send("one:plan", adds="many", removes=None)["total"] == 0
    assert send("apply:add:progress", done="x")["current"] == 0


def test_the_timeline_reports_read_and_write_phases(summary):
    syncAPI._parse_sync_line(json.dumps({"event": "snapshot:start", "provider": "STREMIO", "feature": "history"}))
    assert summary["timeline"]["pre"] and not summary["timeline"]["post"]
    send("apply:add:start", dst="SCROB", feature="history", count=10)
    assert summary["timeline"]["post"]


def test_a_new_run_starts_from_zero(summary):
    pair()
    send("one:plan", src="A", dst="B", feature="history", adds=10, removes=0, updates=0)
    write("add", "B", "history", 10)
    assert summary["_phase"]["apply"]["progress"] == 1.0
    syncAPI._summary_reset()
    assert send("run:pair", i=1, n=1, features=["history"])["progress"] == 0


def test_the_dispatcher_announces_every_feature_once_whatever_happens_to_it(config_base, monkeypatch, summary):
    rows = [{"type": "movie", "title": "Movie", "ids": {"tmdb": "1"}}]
    cfg, _src, dst = feature_setup(config_base, monkeypatch, "watchlist", rows, [])
    dst.capabilities = lambda: {"features": {"watchlist": True, "ratings": False}, "index_semantics": "present"}
    pair_cfg = cfg["pairs"][0]
    pair_cfg["feature"] = "multi"
    pair_cfg["features"] = {"ratings": {"enable": True}, "watchlist": pair_cfg["features"]["watchlist"]}
    events: list[dict[str, Any]] = []

    def on_progress(line: str) -> None:
        syncAPI._parse_sync_line(line)
        if line.startswith("{"):
            events.append(json.loads(line))

    assert Orchestrator(cfg, on_progress=on_progress).run()["ok"]
    assert "feature:unsupported" in [event["event"] for event in events]
    assert [(event["i"], event["n"], event["feature"]) for event in events if event["event"] == "run:feature"] == [(1, 2, "ratings"), (2, 2, "watchlist")]
    assert summary["_phase"]["apply"]["progress"] == 1.0


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_a_real_orchestrator_run_drives_the_bar_to_the_end(config_base, monkeypatch, summary, mode):
    rows = [{"type": "movie", "title": f"Movie {index}", "ids": {"tmdb": str(index + 1)}} for index in range(40)]
    cfg, _src, _dst = feature_setup(config_base, monkeypatch, "watchlist", rows, [], mode)
    cfg["runtime"]["apply_chunk_size"] = 10
    seen: list[float] = []

    def on_progress(line: str) -> None:
        syncAPI._parse_sync_line(line)
        seen.append(float(summary["_phase"]["apply"].get("progress") or 0))

    assert Orchestrator(cfg, on_progress=on_progress).run()["ok"]
    assert seen == sorted(seen)
    assert seen[-1] == 1.0
    assert len({value for value in seen if 0 < value < 1}) >= 3
    assert summary["_phase"]["apply"]["total"] == 40


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_a_preview_reports_no_selection(config_base, monkeypatch, mode):
    setup_ops(config_base, monkeypatch, [item(1), item(2)])
    cfg = _cfg(False)
    cfg["pairs"][0]["mode"] = mode
    lines: list[str] = []
    plan = InteractivePlan()
    assert Orchestrator(cfg, on_progress=lines.append, interactive=plan).run(dry_run=True, pair_scope_ids=["p1"], write_state_json=False)["ok"]
    assert plan.rows
    assert any("one:plan" in line or "two:plan" in line for line in lines)
    assert not any("plan:selected" in line for line in lines)


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_a_partial_interactive_apply_climbs_without_a_jump(config_base, monkeypatch, mode):
    src, dst = setup_ops(config_base, monkeypatch, [item(index) for index in range(1, 21)])
    cfg = _cfg(False)
    cfg["pairs"][0]["mode"] = mode
    cfg["runtime"]["apply_chunk_size"] = 1
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: deepcopy(cfg), lambda *_: None))
    parse = syncAPI._parse_sync_line
    seen: list[tuple[str, float]] = []
    names: list[str] = []

    def traced(line: str) -> None:
        parse(line)
        if line.startswith("{"):
            name = json.loads(line).get("event")
            names.append(name)
            seen.append((name, float(syncAPI.SUMMARY["_phase"]["apply"].get("progress") or 0)))

    monkeypatch.setattr(syncAPI, "_parse_sync_line", traced)
    session = interactive.Session(pair_id="p1", owner="local")
    try:
        interactive.refresh(session, cfg, {})
        interactive.apply(session, cfg, set(sorted(session.plan.rows)[:4]))
        assert session.status == "complete"
    finally:
        session.close()
    assert sum(len(call) for call in dst.add_calls) == 4
    assert names.count("plan:selected") == 1
    assert syncAPI.SUMMARY["_phase"]["apply"]["total"] == 4
    writes = [progress for name, progress in seen if name == "apply:add:progress"]
    assert writes == [0.25, 0.5, 0.75, 1.0]
