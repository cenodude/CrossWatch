# tests/test_plex_cloud_review_regressions.py
# Plex cloud history orchestration review regressions.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from test_plex_history_options import isolated_state
import json

import pytest

from cw_platform.orchestrator import Orchestrator
from cw_platform.orchestrator._interactive import InteractivePlan
from cw_platform.orchestrator._pairs_utils import pair_endpoint_config
from cw_platform.orchestrator._snapshots import emit_cloud_history_read
from cw_platform.orchestrator._state_store import StateStore
from cw_platform.pair_scope import pair_feature_scope


def _plex_pair(config_base, monkeypatch, mode, plex_side, plex_items, other_items):
    from test_interactive_sync_features import feature_setup

    src_items, dst_items = (plex_items, other_items) if plex_side == "source" else (other_items, plex_items)
    cfg, src, dst = feature_setup(config_base, monkeypatch, "history", src_items, dst_items, mode)
    for ops in (src, dst):
        ops.capabilities = lambda: {"features": {"history": True}, "index_semantics": "present", "history": {"rewatches": True}}
    pair = cfg["pairs"][0]
    pair[plex_side] = "PLEX"
    pair["features"]["history"]["remove"] = False
    ops = {pair["source"]: src, pair["target"]: dst}
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: ops)
    cfg["plex"] = {"history": {"read_cloud_history": True}}
    plex = src if plex_side == "source" else dst
    server_only = {k: v for k, v in plex.index.items() if not v.get("_plex_cloud_history")}
    full = dict(plex.index)

    def build(config, **kwargs):
        flag = ((config.get("plex") or {}).get("history") or {}).get("read_cloud_history") is True
        return dict(full if flag else server_only)

    monkeypatch.setattr(plex, "build_index", build)
    return cfg, src, dst, plex


def _cloud(number, ts="2024-01-01T12:00:00Z"):
    return dict(type="movie", title=f"Movie {number}", ids={"imdb": f"tt{number:07}"}, watched=True,
                watched_at=ts, _plex_cloud_history=True)


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_cloud_provenance_reaches_saved_baseline_through_orchestration(config_base, monkeypatch, mode):
    cfg, src, dst, plex = _plex_pair(config_base, monkeypatch, mode, "source", [_cloud(1)], [])
    result = Orchestrator(cfg).run(dry_run=False, pair_scope_ids=["p1"], write_state_json=True)
    assert not result.get("errors")
    scope = pair_feature_scope(cfg, cfg["pairs"][0], "history")
    baseline = StateStore(config_base).for_pair(scope).load_state()["providers"]["PLEX"]["history"]["baseline"]["items"]
    assert baseline
    assert any(v.get("_plex_cloud_history") is True for v in baseline.values()), baseline


@pytest.mark.parametrize("rewatches", [False, True])
def test_two_way_cloud_only_play_still_blocks_server_write_unlike_one_way(config_base, monkeypatch, rewatches):
    other = [dict(type="movie", title="Movie 1", ids={"imdb": "tt0000001"}, watched=True, watched_at="2024-01-01T12:00:00Z")]
    outcomes = {}
    for mode in ("one-way", "two-way"):
        cfg, src, dst, plex = _plex_pair(config_base, monkeypatch, mode, "target", [_cloud(1, "2023-06-01T00:00:00Z")], other)
        cfg["pairs"][0]["features"]["history"]["rewatches"] = rewatches
        if mode == "two-way":
            cfg["pairs"][0]["source"], cfg["pairs"][0]["target"] = "SRC", "PLEX"
        Orchestrator(cfg).run(dry_run=False, pair_scope_ids=["p1"], write_state_json=True)
        outcomes[mode] = sum(len(batch) for batch in plex.add_calls)
    assert outcomes == {"one-way": 1, "two-way": 1}


def test_two_way_start_event_reports_removals_disabled(config_base, monkeypatch):
    cfg, src, dst, plex = _plex_pair(config_base, monkeypatch, "two-way", "source", [_cloud(1)], [])
    cfg["pairs"][0]["features"]["history"]["remove"] = True
    events = []
    Orchestrator(cfg, on_progress=lambda line: events.append(json.loads(line)) if line.startswith("{") else None).run(
        dry_run=True, pair_scope_ids=["p1"], write_state_json=True)
    start = next(e for e in events if e.get("event") == "two:start")
    assert any(e.get("event") == "history:cloud_read" for e in events)
    assert start["removals"] is False


def test_cloud_read_event_records_profile_instance():
    events = []
    view = pair_endpoint_config({"plex": {"history": {"read_cloud_history": True}}}, "PLEX", "PLEX-P01")
    emit_cloud_history_read(lambda name, **f: events.append(f), "PLEX", "history", view, {"a": {"_plex_cloud_history": True}})
    assert events[0]["instance"] == "PLEX-P01"


def test_account_cloud_rows_are_independent_of_server_library_whitelist(isolated_state, monkeypatch):
    from types import SimpleNamespace
    from providers.sync.plex import _cloud_history, _history as history
    server = SimpleNamespace(baseurl="http://plex", _token="token", machineIdentifier="server", history=lambda **kwargs: [])
    adapter = SimpleNamespace(client=SimpleNamespace(server=server),
                              config={"plex": {"history_workers": 1, "history": {"include_marked_watched": False,
                                                                                 "read_cloud_history": True, "libraries": ["1"]}}},
                              libraries=lambda **kwargs: [])
    row = dict(type="movie", title="Excluded library", ids={"tmdb": "9"}, watched_at="2020-01-01T00:00:00Z", _plex_cloud_history=True)
    monkeypatch.setattr(_cloud_history, "read", lambda adapter: ([row], 0))
    assert list(history.build_index(adapter).values()) == [row]


def test_cloud_cloud_duplicates_within_window_are_collapsed():
    from providers.sync.plex import _cloud_history as cloud

    a = _cloud(1, "2024-01-01T12:00:00Z")
    b = dict(a, watched_at="2024-01-01T12:00:30Z")
    assert len(cloud.merge({}, [a, b])) == 1


@pytest.mark.parametrize("rewatches", [False, True])
def test_two_way_does_not_rewrite_a_server_play_when_newer_cloud_play_exists(config_base, monkeypatch, rewatches):
    server = {**_cloud(1), "_plex_cloud_history": False}
    cfg, src, dst, plex = _plex_pair(config_base, monkeypatch, "two-way", "target", [server, _cloud(1, "2025-01-01T12:00:00Z")], [server])
    cfg["pairs"][0]["features"]["history"]["rewatches"] = rewatches
    from cw_platform.history_events import history_event_key
    entries = {history_event_key(row): row for row in [server, _cloud(1, "2025-01-01T12:00:00Z")]}
    monkeypatch.setattr(plex, "build_index", lambda *a, **kw: entries)
    logs = []
    result = Orchestrator(cfg, on_progress=logs.append).run(dry_run=False, pair_scope_ids=["p1"], write_state_json=True)
    assert not result.get("errors"), "\n".join(logs)
    assert not plex.add_calls


@pytest.mark.parametrize("rewatches", [False, True])
@pytest.mark.parametrize("plex_side", ["source", "target"])
@pytest.mark.parametrize("write_fallback", [False, True])
def test_cloud_only_title_copied_out_is_not_written_back_to_server(config_base, monkeypatch, rewatches, plex_side, write_fallback):
    cfg, src, dst, plex = _plex_pair(config_base, monkeypatch, "two-way", plex_side, [_cloud(1)], [])
    peer = dst if plex_side == "source" else src
    cfg["plex"]["history"]["discover_fallback"] = write_fallback
    cfg["pairs"][0]["features"]["history"]["rewatches"] = rewatches
    for run in range(3):
        result = Orchestrator(cfg).run(dry_run=False, pair_scope_ids=["p1"], write_state_json=True)
        assert not result.get("errors")
        assert sum(len(batch) for batch in peer.add_calls) == 1
        assert not plex.add_calls, plex.add_calls


@pytest.mark.parametrize("rewatches", [False, True])
@pytest.mark.parametrize("seconds,expected", [(0, 0), (30, 0), (31, 1)])
def test_cloud_backfill_respects_configured_watch_tolerance(config_base, monkeypatch, rewatches, seconds, expected):
    peer_watch = {**_cloud(1, f"2024-01-01T12:00:{seconds:02d}Z"), "_plex_cloud_history": False}
    cfg, src, dst, plex = _plex_pair(config_base, monkeypatch, "two-way", "target", [_cloud(1)], [peer_watch])
    cfg["runtime"]["history_timestamp_tolerance_seconds"] = 30
    cfg["pairs"][0]["features"]["history"]["rewatches"] = rewatches
    result = Orchestrator(cfg).run(dry_run=False, pair_scope_ids=["p1"], write_state_json=True)
    assert not result.get("errors")
    assert sum(len(batch) for batch in plex.add_calls) == expected
