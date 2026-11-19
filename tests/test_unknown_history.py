# tests/test_unknown_history.py
# CrossWatch - Unknown watch date contracts, routing and outcomes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from importlib import import_module
from types import SimpleNamespace

import pytest

from cw_platform.history_dates import is_unknown_watch, unknown_date_capabilities, unknown_date_reason, unknown_watch
from cw_platform.history_events import history_epoch_from_item
from cw_platform.orchestrator import Orchestrator
from cw_platform.orchestrator._interactive import InteractivePlan
from test_interactive_sync import item, setup_ops
from test_orchestrator_dry_run_no_side_effects import _cfg


def capabilities(write=False):
    return {"history": {"rewatches": {"read": True, "write": True},
                        "unknown_dates": {"read": True, "write": write,
                                          "rewatches": {"read": True, "write": False}}}}


@pytest.mark.parametrize("provider,supported", [("TRAKT", True), ("SIMKL", True), ("WETRAKR", True), ("CROSSWATCH", True),
                                              ("MYANIMELIST", True), ("ANILIST", True), ("KITSU", True), ("PUNCHPLAY", False)])
def test_manifest_and_runtime_agree_on_unknown_dates(provider, supported):
    module = import_module("providers.sync._mod_" + provider)
    runtime = unknown_date_capabilities(module.OPS)
    manifest = unknown_date_capabilities(SimpleNamespace(capabilities=lambda: module.get_manifest()["capabilities"]))
    assert runtime == manifest
    assert runtime["read"] is supported and runtime["write"] is supported
    assert runtime["rewatches"]["write"] is False
    assert bool(unknown_date_reason(module.OPS, unknown_watch(item(1)))) is not supported


@pytest.mark.parametrize("raw", [None, {}, True, {"history": True}, {"history": {"unknown_dates": True}},
                                  {"history": {"unknown_dates": {"write": "true", "rewatches": {"write": True}}}}])
def test_missing_or_malformed_contract_does_not_authorize_unknown_writes(raw):
    ops = SimpleNamespace(capabilities=lambda: raw)
    assert unknown_date_reason(ops, unknown_watch(item(1))) == "unknown_watch_date_unsupported"
    assert unknown_date_reason(ops, item(1)) == ""
    assert unknown_date_reason(ops, item(1, watched_at="1995-06-15T12:00:00Z")) == ""


def test_unknown_date_does_not_fall_back_to_collection_date():
    watch = unknown_watch(item(1, collected_at="2026-01-01T00:00:00Z"))
    assert history_epoch_from_item(watch) is None


def test_wetrakr_unknown_flag_wins_over_placeholder_timestamp():
    from providers.sync.wetrakr._history import _history_item
    from test_wetrakr_sync import MOVIE

    row = {"id": "play-1", "watched_at_unknown": True, "watched_at": "2026-11-19T12:00:00Z"}
    result = _history_item(row, MOVIE, "movie", True)
    assert is_unknown_watch(result)
    assert result["_wetrakr_history_id"] == "play-1"
    assert result["_cw_event_key"].endswith("@id:play-1")


def test_simkl_unknown_roundtrip_preserves_real_old_dates(monkeypatch):
    from providers.sync.simkl import _history as history

    adapter = SimpleNamespace(raw_cfg={})
    rows = {"unknown": item(1, watched_at="1970-01-01T00:00:01Z"),
            "old": item(2, watched_at="1995-06-15T12:00:00Z")}
    monkeypatch.setattr(history, "_build_index", lambda *a, **k: rows)
    monkeypatch.setattr(history, "_with_watch_status", lambda adapter, rows: rows)
    result = history.build_index(adapter)
    undated = next(row for row in result.values() if row["ids"] == item(1)["ids"])
    assert is_unknown_watch(undated)
    assert result["old"]["watched_at"] == "1995-06-15T12:00:00Z"
    writes, skipped = history._split_undated(adapter, [undated])
    assert not skipped
    assert writes[0]["watched_at"] == "1970-01-01T00:00:01Z"
    assert "watched_at" not in undated
    monkeypatch.setattr(history, "_undated_as_today", lambda adapter: True)
    assert history._split_undated(adapter, [undated]) == ([undated], [])


def history_pair(config_base, monkeypatch, mode, *, supported=False, rewatches=False):
    source = [unknown_watch(item(1)), item(2, watched_at="2025-06-15T12:00:00Z")]
    src, dst = setup_ops(config_base, monkeypatch, source)
    for ops, write in ((src, True), (dst, supported)):
        ops.features = lambda: {"history": True}
        ops.health = lambda *_a, **_k: {"ok": True, "status": "ok", "features": {"history": True}}
        ops.capabilities = lambda write=write: capabilities(write)
    cfg = _cfg(False)
    cfg["pairs"][0].update(mode=mode, feature="history", features={"history": {"enable": True, "add": True, "remove": False, "rewatches": rewatches}})
    return src, dst, cfg


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("rewatches", [False, True])
def test_sync_keeps_unknown_in_source_and_reports_skips(config_base, monkeypatch, mode, rewatches):
    src, dst, cfg = history_pair(config_base, monkeypatch, mode, supported=rewatches, rewatches=rewatches)
    events = []
    runner = Orchestrator(cfg)
    monkeypatch.setattr(runner.emitter, "emit", lambda event, **fields: events.append((event, fields)))
    result = runner.run(pair_scope_ids=["p1"])
    assert result["errors"] == 0 and result["skipped"] == 1
    assert len(dst.add_calls) == 1
    assert [row["title"] for row in dst.add_calls[0]] == ["Movie 2"]
    assert any(is_unknown_watch(row) for row in src.index.values())
    skipped = [fields for event, fields in events if event == "history:skipped"]
    assert len(skipped) == 1
    assert skipped[0]["reason"] == ("unknown_rewatch_unsupported" if rewatches else "unknown_watch_date_unsupported")
    dst.add_calls.clear()
    Orchestrator(cfg).run(pair_scope_ids=["p1"])
    assert not dst.add_calls
    assert any(is_unknown_watch(row) for row in src.index.values())


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_interactive_unknown_skip_is_visible_and_not_selectable(config_base, monkeypatch, mode):
    src, dst, cfg = history_pair(config_base, monkeypatch, mode)
    review = InteractivePlan()
    Orchestrator(cfg, interactive=review).run(dry_run=True, pair_scope_ids=["p1"], write_state_json=False)
    unknown = [row for row in review.rows.values() if is_unknown_watch(row["item"])]
    assert len(unknown) == 1
    assert unknown[0]["result"] == "skipped" and unknown[0]["selectable"] is False
    assert unknown[0]["reason_code"] == "unknown_watch_date_unsupported"
    assert not src.add_calls and not dst.add_calls


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_supported_destination_receives_unknown_without_inventing_date(config_base, monkeypatch, mode):
    _, dst, cfg = history_pair(config_base, monkeypatch, mode, supported=True)
    result = Orchestrator(cfg).run(pair_scope_ids=["p1"])
    assert result["errors"] == 0
    assert len(dst.add_calls) == 1 and len(dst.add_calls[0]) == 2
    assert is_unknown_watch(next(row for row in dst.add_calls[0] if row["title"] == "Movie 1"))


def test_two_way_skips_do_not_become_reverse_deletions(config_base, monkeypatch):
    src, dst, cfg = history_pair(config_base, monkeypatch, "two-way")
    cfg["sync"].update(enable_remove=True, allow_mass_delete=True)
    cfg["pairs"][0]["features"]["history"].update(remove=True)
    removals = []
    src.remove = lambda cfg, items, **kw: removals.extend(items) or {"ok": True, "count": 0}
    dst.remove = src.remove
    for _ in range(2):
        result = Orchestrator(cfg).run(pair_scope_ids=["p1"])
        assert result["errors"] == 0
    assert not removals
    assert any(is_unknown_watch(row) for row in src.index.values())


def test_analyzer_uses_destination_contract(monkeypatch):
    from services import analyzer

    monkeypatch.setattr(analyzer, "load_sync_ops", lambda provider: SimpleNamespace(capabilities=lambda: capabilities(provider == "SUPPORTED")))
    context = analyzer._AnalysisContext(state={}, cfg={}, pairs={}, aliases={}, history_exact={}, pair_libs={}, pair_types={})
    watch = unknown_watch(item(1))
    assert context.skip_reason("SRC", "history", "SUPPORTED", watch) == ""
    assert context.skip_reason("SRC", "history", "UNSUPPORTED", watch) == "unknown_watch_date_unsupported"
    context.history_rewatch_pairs.add(("SRC", "SUPPORTED"))
    assert context.skip_reason("SRC", "history", "SUPPORTED", watch) == "unknown_rewatch_unsupported"


def test_unknown_skip_is_a_final_event_outcome(tmp_path):
    from cw_platform.event_archive import groups
    from cw_platform.event_archive.db import connect
    from cw_platform.event_archive.recorder import RunRecorder

    conn = connect(tmp_path / "events.db")
    try:
        recorder = RunRecorder(lambda *a, **k: None, run_id="unknown-test", conn=conn)
        recorder.emit("feature:start", src="TRAKT", dst="PUNCHPLAY", feature="history")
        recorder.emit("history:skipped", provider="PUNCHPLAY", destination_instance="default", feature="history",
                      op="add", reason="unknown_watch_date_unsupported", count=1, items=[unknown_watch(item(1))])
        recorder.close()
        groups.correlate(conn=conn)
        row = conn.execute("SELECT status, reason_code FROM event_groups WHERE item_key IS NOT NULL").fetchone()
        assert tuple(row) == ("skipped", "unknown_watch_date_unsupported")
    finally:
        conn.close()
