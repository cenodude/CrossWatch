# tests/test_analyzer_pair_skips.py
# CrossWatch - Analyzer leaves out items a pair skips on purpose: dropped shows and watches before a rewatch reset
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import pytest

import services.analyzer as A
from cw_platform.local_db import pair_skips as store
from cw_platform.orchestrator._pair_skips import PairSkips, skip_state
from cw_platform.pair_scope import pair_feature_scope
from tests.test_trakt_rewatch_resets import AFTER, BEFORE, ServerOps, TraktOps, _ep, _events, _reset, _run


@pytest.fixture(autouse=True)
def _isolated(config_base, monkeypatch):
    monkeypatch.setattr(A, "CONFIG_DIR", config_base)
    return config_base


def _state(*trakt_items: tuple[str, dict[str, Any]]) -> dict[str, Any]:
    return {"providers": {
        "TRAKT": {"history": {"baseline": {"items": dict(trakt_items)}}},
        "PLEX": {"history": {"baseline": {"items": {}}}},
    }}


def test_pair_skips_match_by_direction_reason_and_date():
    state = skip_state((("TRAKT", "default"), ("PLEX", "home")), dropped={"tmdb:7"}, resets={"tmdb:1": 1_750_000_000})
    skips = PairSkips([("history", [("TRAKT", "PLEX@home")], state)])
    pre = _ep("1", 1, 1, "2025-01-01T00:00:00Z")
    post = _ep("1", 1, 1, "2025-07-01T00:00:00Z")

    assert skips.reason("history", "TRAKT", "PLEX@home", pre) == "rewatch_reset"
    assert skips.reason("history", "PLEX@home", "TRAKT", pre) == ""
    assert skips.reason("history", "TRAKT", "PLEX@home", post) == ""
    assert skips.reason("history", "TRAKT", "PLEX@home", _ep("7", 2, 3, AFTER)) == "dropped"
    assert skips.reason("history", "TRAKT", "PLEX", pre) == ""
    assert skips.reason("ratings", "TRAKT", "PLEX@home", pre) == ""
    assert skips.reason("history", "TRAKT", "PLEX@home", {"type": "movie", "ids": {"tmdb": "7"}}) == ""
    assert skips.reason("history", "TRAKT", "PLEX@home", {"type": "episode", "season": 1, "episode": 1,
                                                           "ids": {"tmdb": "1"}, "watched_at": BEFORE}) == ""


def test_pair_skips_need_every_pair_on_a_route_to_agree():
    state = skip_state((("TRAKT", "default"), ("PLEX", "default")), resets={"tmdb:1": 1_750_000_000})
    pre = _ep("1", 1, 1, "2025-01-01T00:00:00Z")

    both = PairSkips([("history", [("TRAKT", "PLEX")], state), ("history", [("TRAKT", "PLEX")], state)])
    mixed = PairSkips([("history", [("TRAKT", "PLEX")], state), ("history", [("TRAKT", "PLEX")], None)])
    reverse = PairSkips([("history", [("TRAKT", "PLEX")], state), ("history", [("PLEX", "TRAKT")], None)])

    assert both.reason("history", "TRAKT", "PLEX", pre) == "rewatch_reset"
    assert mixed.reason("history", "TRAKT", "PLEX", pre) == ""
    assert reverse.reason("history", "TRAKT", "PLEX", pre) == "rewatch_reset"
    assert reverse.reason("history", "PLEX", "TRAKT", pre) == ""


def test_another_pair_without_the_option_keeps_its_mismatches(config_base, monkeypatch):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE)), resets=[_reset("1")])
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, BEFORE)))
    cfg = _run(monkeypatch, trakt, server)
    plain = {"id": "p2", "enabled": True, "source": "TRAKT", "target": "PLEX", "mode": "one-way", "feature": "history",
             "features": {"history": {"enable": True, "add": True, "remove": False}}}
    reverse = {**plain, "id": "p3", "source": "PLEX", "target": "TRAKT"}
    pre = _ep("1", 1, 1, BEFORE)

    shared = A._analysis_context(_state(("pre", pre)), {**cfg, "pairs": [cfg["pairs"][0], plain]})
    other_way = A._analysis_context(_state(("pre", pre)), {**cfg, "pairs": [cfg["pairs"][0], reverse]})

    assert A._missing_targets(shared, "TRAKT", "history", "pre", pre) == ["PLEX"]
    assert A._missing_targets(other_way, "TRAKT", "history", "pre", pre) == []
    assert other_way.skip_reason("PLEX", "history", "TRAKT", pre) == ""


def test_analyzer_does_not_flag_watches_before_a_reset(config_base, monkeypatch):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE)), resets=[_reset("1")])
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, BEFORE)))
    cfg = _run(monkeypatch, trakt, server)
    pre, post = _ep("1", 1, 1, BEFORE), _ep("1", 1, 3, AFTER)
    state = _state(("pre", pre), ("post", post))

    ctx = A._analysis_context(state, cfg)

    assert A._missing_targets(ctx, "TRAKT", "history", "pre", pre) == []
    assert A._missing_targets(ctx, "TRAKT", "history", "post", post) == ["PLEX"]
    [row] = A._pair_exclusions(state, ctx=ctx)
    assert row["excluded_rewatch_reset"] == 1 and row["excluded_total"] == 1
    problems = A._problems(state, None, cfg=cfg, ctx=ctx, include_system=False, include_hints=False)
    assert [p["key"] for p in problems if p["type"] == "missing_peer"] == ["post"]


def test_analyzer_does_not_flag_dropped_shows_in_ignore_mode(config_base, monkeypatch):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("7", 1, 1, BEFORE), _ep("8", 1, 1, BEFORE)), dropped={"tmdb:7"})
    server = ServerOps(provider="PLEX", index={})
    cfg = _run(monkeypatch, trakt, server, option=False, providers_cfg={"trakt": {"history_dropped_mode": "ignore"}})
    assert {it["show_ids"]["tmdb"] for batch in server.add_calls for it in batch} == {"8"}
    dropped, kept = _ep("7", 1, 1, BEFORE), _ep("8", 1, 1, BEFORE)
    state = _state(("dropped", dropped), ("kept", kept))

    ctx = A._analysis_context(state, cfg)

    assert A._missing_targets(ctx, "TRAKT", "history", "dropped", dropped) == []
    assert A._missing_targets(ctx, "TRAKT", "history", "kept", kept) == ["PLEX"]
    [row] = A._pair_exclusions(state, ctx=ctx)
    assert row["excluded_dropped"] == 1


def test_tag_mode_is_not_treated_as_a_skip(config_base, monkeypatch):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("7", 1, 1, BEFORE)), dropped={"tmdb:7"})
    server = ServerOps(provider="PLEX", index={})
    cfg = _run(monkeypatch, trakt, server, option=False, providers_cfg={"trakt": {"history_dropped_mode": "tag"}})
    item = _ep("7", 1, 1, BEFORE)

    ctx = A._analysis_context(_state(("x", item)), cfg)

    assert A._missing_targets(ctx, "TRAKT", "history", "x", item) == ["PLEX"]


def test_analyzer_flags_the_watches_again_when_the_option_is_switched_off(config_base, monkeypatch):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE)), resets=[_reset("1")])
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, BEFORE)))
    pre = _ep("1", 1, 1, BEFORE)
    cfg = _run(monkeypatch, trakt, server)
    assert A._missing_targets(A._analysis_context(_state(("pre", pre)), cfg), "TRAKT", "history", "pre", pre) == []

    cfg = _run(monkeypatch, trakt, server, option=False)

    assert A._missing_targets(A._analysis_context(_state(("pre", pre)), cfg), "TRAKT", "history", "pre", pre) == ["PLEX"]


def test_full_state_reset_removes_all_skips(config_base, monkeypatch):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE)), resets=[_reset("1")])
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, BEFORE)))
    cfg = _run(monkeypatch, trakt, server)
    scope = pair_feature_scope(cfg, cfg["pairs"][0], "history", 1)
    from cw_platform.local_db import state as sqlite_state

    sqlite_state.clear_state(config_base)

    assert store.load(config_base, [scope]) == {}


def test_clearing_pair_state_removes_its_skips(config_base, monkeypatch):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE)), resets=[_reset("1")])
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, BEFORE)))
    cfg = _run(monkeypatch, trakt, server)
    scope = pair_feature_scope(cfg, cfg["pairs"][0], "history", 1)
    from cw_platform.local_db import state as sqlite_state

    sqlite_state.clear_pair_state(config_base, scope)

    assert store.load(config_base, [scope]) == {}


def test_preview_and_dry_run_do_not_write_skips(config_base, monkeypatch):
    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE)), resets=[_reset("1")])
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, BEFORE)))
    cfg = _run(monkeypatch, trakt, server, dry_run=True)
    scope = pair_feature_scope(cfg, cfg["pairs"][0], "history", 1)

    assert store.load(config_base, [scope]) == {}


def test_maintenance_cleanup_removes_skips_of_old_pair_scopes(config_base, monkeypatch):
    from services.pair_state import prune_pair_scopes

    trakt = TraktOps(provider="TRAKT", index=_events(_ep("1", 1, 1, BEFORE)), resets=[_reset("1")])
    server = ServerOps(provider="PLEX", index=_events(_ep("1", 1, 1, BEFORE)))
    old_cfg = _run(monkeypatch, trakt, server)
    old_scope = pair_feature_scope(old_cfg, old_cfg["pairs"][0], "history", 1)
    new_cfg = _run(monkeypatch, trakt, server, remove=True)
    new_scope = pair_feature_scope(new_cfg, new_cfg["pairs"][0], "history", 1)
    assert old_scope != new_scope
    assert set(store.load(config_base, [old_scope, new_scope])) == {(old_scope, "history"), (new_scope, "history")}

    prune_pair_scopes(config_base, new_cfg)

    assert set(store.load(config_base, [old_scope, new_scope])) == {(new_scope, "history")}
