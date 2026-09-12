from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from cw_platform.history_events import history_sync_key
from cw_platform.orchestrator._history_rewatches import (
    history_event_diff, history_event_matches, history_timestamp_tolerance_seconds,
)


def movie(offset=0, **extra):
    # The eight-second example deliberately crosses a minute boundary.
    stamp = datetime(2026, 9, 12, 14, 47, 58, tzinfo=timezone.utc) + timedelta(seconds=offset)
    return dict(type="movie", title="Pressure", ids={"tmdb": "1318413"},
                watched_at=stamp.isoformat(), **extra)


def index(*items):
    return {history_sync_key(item, event_mode=True): item for item in items}


def tokens(item):
    return {f"{key}:{value}" for key, value in item.get("ids", {}).items()}


@pytest.mark.parametrize("gap", [-60, -10, -8, -3, 0, 3, 8, 10, 60])
def test_nearby_viewings_match_without_changing_records(gap):
    source, target = index(movie()), index(movie(gap))
    before = deepcopy((source, target))
    assert history_event_diff(source, target, tokens) == ([], [])
    assert (source, target) == before


@pytest.mark.parametrize("gap", [-61, 61, 294, 86400])
def test_separate_viewings_remain_different(gap):
    adds, removes = history_event_diff(index(movie()), index(movie(gap)), tokens)
    assert adds[0]["watched_at"] == movie()["watched_at"]
    assert removes[0]["watched_at"] == movie(gap)["watched_at"]


def test_exact_match_reserves_destination_for_one_viewing():
    source, target = index(movie(), movie(8)), index(movie(8))
    adds, removes = history_event_diff(source, target, tokens)
    assert [item["watched_at"] for item in adds] == [movie()["watched_at"]]
    assert removes == []


def test_unique_nearest_pairs_are_symmetric_and_order_independent():
    source, target = index(movie(), movie(20)), index(movie(8), movie(28))
    matches = history_event_matches(source, target, tokens)
    assert matches == {next(iter(index(movie()))): next(iter(index(movie(8)))),
                       next(iter(index(movie(20)))): next(iter(index(movie(28))))}
    assert history_event_matches(target, source, tokens) == {v: k for k, v in matches.items()}
    assert history_event_matches(dict(reversed(list(source.items()))), target, tokens) == matches


def test_equidistant_viewings_are_ambiguous():
    source, target = index(movie(), movie(16)), index(movie(8))
    assert history_event_matches(source, target, tokens) == {}
    assert history_event_matches(target, source, tokens) == {}


def test_tolerance_does_not_chain_viewings_or_match_other_media():
    source, target = index(movie(), movie(100)), index(movie(50))
    assert history_event_matches(source, target, tokens) == {}
    other_movie = {**movie(8), "ids": {"tmdb": "42"}}
    other_type = {**movie(8), "type": "show"}
    assert history_event_matches(index(movie()), index(other_movie, other_type), tokens) == {}


def test_exact_comparison_remains_available_for_retry_source_verification():
    assert history_event_matches(index(movie()), index(movie(8)), tokens, tolerance_seconds=0) == {}


def test_sparse_records_still_match_exact_event_keys():
    key = next(iter(index(movie())))
    sparse = {"type": "movie", "watched_at": movie()["watched_at"]}
    assert history_event_matches({key: sparse}, index(movie()), tokens) == {key: key}
    assert not history_event_matches({key: sparse}, index(movie(8)), tokens)


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("gap,tolerance", [
    (8, None), (60, None), (61, None), (300, 300), (301, 300), (0, 0), (8, 0), (8, 5),
])
def test_sync_and_preview_use_the_same_history_tolerance(config_base, monkeypatch, mode, gap, tolerance):
    from cw_platform.orchestrator import Orchestrator
    from cw_platform.orchestrator._interactive import InteractivePlan
    from cw_platform.orchestrator._state_store import StateStore
    from cw_platform.pair_scope import pair_feature_scope
    from test_interactive_sync_features import feature_setup

    cfg, source, target = feature_setup(config_base, monkeypatch, "history", [], [], mode)
    if tolerance is not None:
        cfg.setdefault("runtime", {})["history_timestamp_tolerance_seconds"] = tolerance
    matched = gap <= (60 if tolerance is None else tolerance)
    cfg["pairs"][0]["features"]["history"]["rewatches"] = True
    for ops in (source, target):
        ops.capabilities = lambda: {"features": {"history": True}, "index_semantics": "present",
                                   "history": {"rewatches": {"read": True, "write": True}}}
    source.index, target.index = index(movie()), index(movie(gap))
    before = deepcopy((source.index, target.index))
    preview = InteractivePlan()
    Orchestrator(cfg, interactive=preview).run(dry_run=True, write_state_json=False)
    assert len(preview.rows) == (0 if matched else (1 if mode == "one-way" else 2))
    if not matched:
        execution = InteractivePlan(preview=False, selected=set(preview.rows))
        Orchestrator(cfg, interactive=execution).run()
        assert sum(map(len, target.add_calls)) == 1
        assert sum(map(len, source.add_calls)) == (0 if mode == "one-way" else 1)
        return
    # Interactive execution must not reintroduce proposals excluded by the preview.
    execution = InteractivePlan(preview=False)
    Orchestrator(cfg, interactive=execution).run()
    assert not execution.rows
    assert not source.add_calls and not target.add_calls
    result = Orchestrator(cfg).run()
    assert not result["errors"]
    assert not source.add_calls and not target.add_calls
    assert (source.index, target.index) == before
    scope = pair_feature_scope(cfg, cfg["pairs"][0], "history", 1)
    state = StateStore(config_base).for_pair(scope).load_state_features({"history"})
    for name, expected in zip(("SRC", "DST"), before):
        stored = state["providers"][name]["history"]["baseline"]["items"]
        assert {row["watched_at"] for row in stored.values()} == {row["watched_at"] for row in expected.values()}


@pytest.mark.parametrize("value,expected", [
    (0, 0), (300, 300), (" 120 ", 120), (-1, 0), (None, 60),
    (True, 60), (False, 60), ("bad", 60), (1.5, 60), ({}, 60),
])
def test_runtime_history_tolerance_values(value, expected):
    assert history_timestamp_tolerance_seconds({"runtime": {"history_timestamp_tolerance_seconds": value}}) == expected


def test_history_tolerance_config_roundtrip(config_base):
    from cw_platform.config_base import load_config, save_config

    cfg = load_config()
    assert cfg["runtime"]["history_timestamp_tolerance_seconds"] == 60
    for value in (300, 0):
        cfg["runtime"]["history_timestamp_tolerance_seconds"] = value
        save_config(cfg)
        assert history_timestamp_tolerance_seconds(load_config()) == value


def test_retry_cleanup_preserves_other_events_removals_and_pair_scopes(tmp_path, monkeypatch):
    from cw_platform.orchestrator import _unresolved as unresolved

    monkeypatch.setattr(unresolved, "STATE_DIR", tmp_path)
    first, second, deletion = [next(iter(index(movie(offset)))) for offset in (0, 8, 20)]
    for scope in ("cw2_test_a", "cw2_test_b"):
        monkeypatch.setenv("CW_PAIR_SCOPE", scope)
        unresolved.record_unresolved("SIMKL", "history", [first, second], hint="apply:add:no_confirmations_fallback")
        unresolved.record_unresolved("SIMKL", "history", [deletion], hint="apply:remove:unconfirmed")
    monkeypatch.setenv("CW_PAIR_SCOPE", "cw2_test_a")
    assert unresolved.clear_matched_history_retries("SIMKL", [first, deletion]) == {first: first}
    assert set(unresolved.load_unresolved_map("SIMKL", "history", cross_features=False)) == {second, deletion}
    monkeypatch.setenv("CW_PAIR_SCOPE", "cw2_test_b")
    assert set(unresolved.load_unresolved_map("SIMKL", "history", cross_features=False)) == {first, second, deletion}


def test_legacy_retry_cleanup_uses_the_recorded_viewing_time(tmp_path, monkeypatch):
    from cw_platform.orchestrator import _unresolved as unresolved

    monkeypatch.setattr(unresolved, "STATE_DIR", tmp_path)
    monkeypatch.setenv("CW_PAIR_SCOPE", "cw2_legacy_test")
    unresolved.record_unresolved("SIMKL", "history", [movie()], hint="apply:add:no_confirmations_fallback")
    # A different viewing of the same title is not evidence that this retry succeeded.
    assert not unresolved.clear_matched_history_retries("SIMKL", index(movie(8)))
    assert unresolved.clear_matched_history_retries("SIMKL", index(movie())) == {"tmdb:1318413": next(iter(index(movie())))}
    assert not unresolved.load_unresolved_pending("SIMKL", "history")


@pytest.mark.parametrize("pending", [False, True])
@pytest.mark.parametrize("metadata", [
    {"action": "remove", "reasons": ["write_failed"]},
    {"reasons": ["write_failed", "two:apply:remove:unconfirmed"]},
    {"hint": "simkl_remove_not_confirmed"},
    {"reason": "trakt_history_remove_unconfirmed"},
])
def test_provider_removal_retry_survives_presence_confirmation(tmp_path, monkeypatch, pending, metadata):
    import json
    from cw_platform.orchestrator import _unresolved as unresolved

    monkeypatch.setattr(unresolved, "STATE_DIR", tmp_path)
    monkeypatch.setenv("CW_PAIR_SCOPE", "cw2_provider_removal")
    key = next(iter(index(movie())))
    row = {"feature": "history", "item": movie(), **metadata}
    path = unresolved._blocking_path("SIMKL", "history")
    path.write_text(json.dumps({key: row}), encoding="utf-8")
    if pending:
        unresolved.record_unresolved("SIMKL", "history", [key], hint="apply:add:failed")
    assert unresolved.clear_matched_history_retries("SIMKL", [key]) == {}
    assert json.loads(path.read_text(encoding="utf-8")) == {key: row}
    if pending:
        assert unresolved.load_unresolved_pending("SIMKL", "history")


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_retry_confirmation_uses_original_matched_event_keys(config_base, monkeypatch, mode):
    from cw_platform.orchestrator import Orchestrator, _pairs_oneway, _pairs_twoway
    from cw_platform.orchestrator._interactive import InteractivePlan
    from test_interactive_sync_features import feature_setup

    cfg, source, target = feature_setup(config_base, monkeypatch, "history", [], [], mode)
    cfg["pairs"][0]["features"]["history"]["rewatches"] = True
    for ops in (source, target):
        ops.capabilities = lambda: {"features": {"history": True}, "index_semantics": "present",
                                   "history": {"rewatches": {"read": True, "write": True}}}
    # Preserve provider event-ID keys even when a watch timestamp is available.
    source.index = {"tmdb:1318413@id:998877": movie(), "tmdb:1318413@id:998878": movie(1000)}
    target.index = {"tmdb:1318413@id:123456": movie(8), "tmdb:1318413@id:123457": movie(2000)}
    cleared = []

    def clear(provider, keys):
        cleared.append((provider, set(keys)))
        return {}

    for module in (_pairs_oneway, _pairs_twoway):
        monkeypatch.setattr(module, "clear_matched_history_retries", clear)
    # Execute without selecting the unmatched additions; only existing peer
    # matches can confirm retries, regardless of provider write outcomes.
    result = Orchestrator(cfg, interactive=InteractivePlan(preview=False)).run()
    assert not result["errors"]
    assert ("DST", {"tmdb:1318413@id:998877"}) in cleared
    if mode == "two-way":
        assert ("SRC", {"tmdb:1318413@id:123456"}) in cleared
    assert all(len(keys) == 1 for _, keys in cleared)


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_sync_clears_confirmed_retry_only_during_execution(config_base, monkeypatch, mode):
    from cw_platform.orchestrator import Orchestrator, _pairs_oneway, _pairs_twoway
    from cw_platform.orchestrator._interactive import InteractivePlan
    from test_interactive_sync_features import feature_setup

    cfg, source, target = feature_setup(config_base, monkeypatch, "history", [], [], mode)
    cfg["pairs"][0]["features"]["history"]["rewatches"] = True
    for ops in (source, target):
        ops.capabilities = lambda: {"features": {"history": True}, "index_semantics": "present",
                                   "history": {"rewatches": {"read": True, "write": True}}}
    source.index, target.index = index(movie()), index(movie(8))
    cleared = []

    def clear(provider, keys):
        cleared.append((provider, set(keys)))
        return {key: key for key in keys}

    for module in (_pairs_oneway, _pairs_twoway):
        monkeypatch.setattr(module, "clear_matched_history_retries", clear)
    Orchestrator(cfg, interactive=InteractivePlan()).run(dry_run=True, write_state_json=False)
    assert not cleared
    result = Orchestrator(cfg).run()
    assert not result["errors"]
    assert ("DST", set(source.index)) in cleared
    if mode == "two-way":
        assert ("SRC", set(target.index)) in cleared
    assert not source.add_calls and not target.add_calls


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("retry_key", ["base", "event"])
def test_confirmed_retry_archive_preserves_item_metadata(config_base, monkeypatch, mode, retry_key):
    from cw_platform.event_archive.db import connect
    from cw_platform.orchestrator import Orchestrator, _unresolved as unresolved
    from cw_platform.pair_scope import pair_feature_scope
    from test_interactive_sync_features import feature_setup

    cfg, source, target = feature_setup(config_base, monkeypatch, "history", [], [], mode)
    cfg["pairs"][0]["features"]["history"]["rewatches"] = True
    for ops in (source, target):
        ops.capabilities = lambda: {"features": {"history": True}, "index_semantics": "present",
                                   "history": {"rewatches": {"read": True, "write": True}}}
    source.index, target.index = index(movie()), index(movie(8))
    scope = pair_feature_scope(cfg, cfg["pairs"][0], "history", 1)
    monkeypatch.setenv("CW_PAIR_SCOPE", scope)
    destinations = {"DST": source.index}
    if mode == "two-way":
        destinations["SRC"] = target.index
    for provider, items in destinations.items():
        payload = items.values() if retry_key == "base" else items.keys()
        unresolved.record_unresolved(provider, "history", payload, hint="apply:add:failed")

    conn = connect(":memory:")
    monkeypatch.setattr("cw_platform.event_archive.recorder.get_conn", lambda: conn)
    try:
        result = Orchestrator(cfg).run()
        assert not result["errors"]
        rows = conn.execute("SELECT destination_provider, item_key, title, media_type, match_basis "
                            "FROM events WHERE event_type='unresolved_cleared'").fetchall()
        assert len(rows) == len(destinations)
        for provider, key, title, media_type, match_basis in rows:
            expected_key = "tmdb:1318413" if retry_key == "base" else next(iter(destinations[provider]))
            assert key == expected_key
            assert (title, media_type, match_basis) == ("Pressure", "movie", "tmdb:1318413")
            assert not unresolved.load_unresolved_pending(provider, "history")
        assert not source.add_calls and not target.add_calls
    finally:
        conn.close()
