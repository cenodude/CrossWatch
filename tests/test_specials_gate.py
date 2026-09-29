# tests/test_specials_gate.py
# CrossWatch - Season 0 (specials) pair and scrobble gate tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace

import pytest

from api.syncAPI import _normalize_features
from cw_platform.history_events import history_sync_key
from cw_platform.orchestrator._specials import filter_specials_index, is_special, specials_excluded
from cw_platform.orchestrator.facade import Orchestrator
from providers.scrobble.media_filters import event_ignore_reason
from tests.test_history_baseline_native import HistoryOps


def _episode(season, episode=1):
    return {"type": "episode", "season": season, "episode": episode, "show_ids": {"tmdb": "1"}}


@pytest.mark.parametrize("feature", ["history", "progress", "ratings"])
def test_specials_are_included_unless_the_pair_feature_opts_out(feature):
    assert specials_excluded(feature, {}) is False
    assert specials_excluded(feature, {"include_specials": True}) is False
    assert specials_excluded(feature, {"include_specials": False}) is True


def test_specials_gate_does_not_apply_to_watchlist_or_collection():
    assert specials_excluded("watchlist", {"include_specials": False}) is False
    assert specials_excluded("collection", {"include_specials": False}) is False


def test_only_season_zero_episodes_and_seasons_count_as_specials():
    assert is_special(_episode(0)) is True
    assert is_special({"type": "season", "season": 0}) is True
    assert is_special({"type": "episode", "season_number": "0", "episode": 3}) is True
    assert is_special(_episode(1)) is False
    assert is_special(_episode(None)) is False
    assert is_special({"type": "movie", "season": 0}) is False


def test_filter_specials_index_drops_season_zero_and_counts_them():
    idx = {"a": _episode(0), "b": _episode(1), "c": {"type": "movie", "ids": {"tmdb": "2"}}, "d": _episode(None)}

    kept, dropped = filter_specials_index(idx)

    assert set(kept) == {"b", "c", "d"}
    assert set(dropped) == {"a"}


def test_pair_features_default_include_specials_and_keep_an_opt_out():
    features = _normalize_features({
        "history": {"enable": True},
        "progress": {"enable": True, "include_specials": False},
        "ratings": {"enable": True, "include_specials": "false"},
        "watchlist": {"enable": True},
    })

    assert features["history"]["include_specials"] is True
    assert features["progress"]["include_specials"] is False
    assert features["ratings"]["include_specials"] is False
    assert "include_specials" not in features["watchlist"]


def _scrobble_event(season, media_type="episode"):
    return SimpleNamespace(media_type=media_type, season=season, title="Show", raw={}, session_key="1")


def _cfg(filters):
    return {"scrobble": {"watch": {"filters": filters}}}


def test_scrobble_filter_ignores_specials_only_when_enabled():
    assert event_ignore_reason(_scrobble_event(0), _cfg({})) is None
    assert event_ignore_reason(_scrobble_event(0), _cfg({"ignore_specials": False})) is None
    assert event_ignore_reason(_scrobble_event(0), _cfg({"ignore_specials": True})) == "specials"


def test_scrobble_filter_keeps_regular_episodes_movies_and_unknown_seasons():
    cfg = _cfg({"ignore_specials": True})

    assert event_ignore_reason(_scrobble_event(1), cfg) is None
    assert event_ignore_reason(_scrobble_event(None), cfg) is None
    assert event_ignore_reason(_scrobble_event(0, media_type="movie"), cfg) is None


def _watched(season, episode):
    return {**_episode(season, episode), "watched_at": "2024-01-01T00:00:00Z"}


def _history_index(*items):
    return {history_sync_key(it): dict(it) for it in items}


def _run_history_pair(monkeypatch, src, dst, *, include_specials):
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SRC": src, "DST": dst})
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda _cfg, _name: True)
    monkeypatch.setattr("cw_platform.orchestrator._pairs_blocklist.load_blackbox_keys", lambda *_a, **_k: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs_blocklist.load_unresolved_keys", lambda *_a, **_k: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs.record_health", lambda *_a, **_k: None)
    orch = Orchestrator({
        "runtime": {"debug": False, "snapshot_ttl_sec": 0, "apply_chunk_size": 0, "apply_chunk_pause_ms": 0},
        "sync": {"dry_run": False, "enable_add": True, "enable_remove": True, "include_observed_deletes": False, "allow_mass_delete": True},
        "pairs": [{
            "id": "p1", "enabled": True, "source": "SRC", "target": "DST", "mode": "one-way", "feature": "history",
            "features": {"history": {"enable": True, "add": True, "remove": True, "remove_mode": "mirror", "include_specials": include_specials}},
        }],
    })
    orch.run()
    return orch


def _sent(calls):
    return {(it.get("season"), it.get("episode")) for batch in calls for it in batch}


def test_excluded_specials_are_neither_added_nor_removed(monkeypatch):
    src = HistoryOps(provider="SRC", index=_history_index(_watched(1, 1), _watched(0, 1)))
    dst = HistoryOps(provider="DST", index=_history_index(_watched(0, 2)))

    _run_history_pair(monkeypatch, src, dst, include_specials=False)
    _run_history_pair(monkeypatch, src, dst, include_specials=False)

    assert _sent(dst.add_calls) == {(1, 1)}
    assert _sent(dst.remove_calls) == set()


def test_included_specials_sync_like_any_other_episode(monkeypatch):
    src = HistoryOps(provider="SRC", index=_history_index(_watched(1, 1), _watched(0, 1)))
    dst = HistoryOps(provider="DST", index=_history_index(_watched(0, 2)))

    _run_history_pair(monkeypatch, src, dst, include_specials=True)
    _run_history_pair(monkeypatch, src, dst, include_specials=True)

    assert _sent(dst.add_calls) == {(1, 1), (0, 1)}
    assert _sent(dst.remove_calls) == {(0, 2)}


def _baseline_seasons(orch, provider):
    st = orch.state_store.load_state() or {}
    items = ((((st.get("providers") or {}).get(provider) or {}).get("history") or {}).get("baseline") or {}).get("items") or {}
    return {(it.get("season"), it.get("episode")) for it in items.values()}


def test_excluded_specials_stay_in_the_shared_provider_baselines(monkeypatch):
    src = HistoryOps(provider="SRC", index=_history_index(_watched(1, 1), _watched(0, 1)))
    dst = HistoryOps(provider="DST", index=_history_index(_watched(0, 2)))

    orch = _run_history_pair(monkeypatch, src, dst, include_specials=False)

    assert (0, 1) in _baseline_seasons(orch, "SRC")
    assert (0, 2) in _baseline_seasons(orch, "DST")
