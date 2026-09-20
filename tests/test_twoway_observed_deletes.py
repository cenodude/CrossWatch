# tests/test_twoway_observed_deletes.py
# CrossWatch two-way deletion identity and provider capability regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from cw_platform.id_map import canonical_key
from cw_platform.orchestrator import _pairs_twoway
from providers.sync import _mod_ANILIST, _mod_PLEX, _mod_TAUTULLI, _mod_TRACEARR


class Planned(BaseException):
    def __init__(self, data):
        self.data = data


class Store:
    def __init__(self, state):
        self.state = state
        self.tomb = {}

    def load_state(self):
        return self.state

    def load_tomb(self):
        return self.tomb

    def save_tomb(self, data):
        self.tomb = data


class Ops:
    def __init__(self, feature, caps):
        self.caps = {**caps, "features": {feature: True}}

    def capabilities(self):
        return self.caps

    def activities(self, config):
        return {"updated_at": "2026-01-01T00:00:00Z"}


def deletion_plan(monkeypatch, feature, side, *, caps=None, scenario="delete", kind="movie", rows=None):
    rich = {"type": kind, "title": "Example", "ids": {"tmdb": "6435", "imdb": "tt0120791"}}
    lean = {"type": kind, "title": "Example", "ids": {"imdb": "tt0120791"}}
    if rows is not None:
        rich, lean = deepcopy(rows)
    for row in (rich, lean):
        if kind in ("season", "episode"):
            row.update(show_ids=dict(row["ids"]), season=1, episode=2)
        if feature == "history":
            row["watched_at"] = "2025-01-01T12:00:00Z"
        if feature == "collection":
            row["collected_at"] = "2025-01-01T12:00:00Z"
    if scenario == "same_id":
        rich = deepcopy(lean)
    if scenario == "unrelated":
        lean["ids"] = {"imdb": "tt9999999"}
    previous = {"A": {canonical_key(rich): rich}, "B": {canonical_key(lean): lean}}
    snapshots = deepcopy(previous)
    if scenario != "no_change":
        snapshots[side].clear()
    if scenario == "initial_missing":
        previous = {"A": {}, "B": {}}
    state = {"providers": {
        name: {feature: {"baseline": {"items": items}, "checkpoint": "2026-01-01T00:00:00Z"}}
        for name, items in previous.items()
    }}
    sync = {
        "enable_add": True,
        "enable_remove": scenario != "removal_disabled",
        "include_observed_deletes": scenario != "observed_disabled",
        "allow_mass_delete": scenario != "mass_guard",
        "drop_guard": scenario == "drop_guard",
    }
    default_caps = {"index_semantics": "present", "observed_deletes": True}
    providers = {name: Ops(feature, default_caps) for name in ("A", "B")}
    if caps is not None:
        providers[side] = Ops(feature, caps)

    def emit(event, **data):
        if event == "two:plan":
            raise Planned(data)

    ctx = SimpleNamespace(
        config={"sync": sync, "runtime": {"suspect_min_prev": 1, "suspect_shrink_ratio": 0.1}},
        providers=providers, emit=emit, emit_info=lambda *_: None, dbg=lambda *args, **kwargs: None,
        dry_run=True, snap_cache={}, snap_ttl_sec=0, state_store=Store(state),
        stats_manual_blocked=0, apply_chunk_pause_ms=0,
    )
    monkeypatch.setattr(_pairs_twoway, "build_snapshots_for_feature", lambda **kwargs: deepcopy(snapshots))
    with pytest.raises(Planned) as result:
        _pairs_twoway._two_way_sync(
            ctx, "A", "B", feature=feature, fcfg={"mode": "all", "types": ["all"]},
            health_map={side: {"status": "down"}} if scenario == "offline" else {},
        )
    return result.value.data


@pytest.mark.parametrize("feature", ["watchlist", "history", "collection"])
@pytest.mark.parametrize("side", ["A", "B"])
@pytest.mark.parametrize("kind", ["movie", "show", "season", "episode"])
def test_cross_id_deletion_removes_peer_without_restoring_item(config_base, monkeypatch, feature, side, kind):
    plan = deletion_plan(monkeypatch, feature, side, kind=kind)
    assert plan["rem_from_B" if side == "A" else "rem_from_A"] == 1
    assert plan["rem_from_A" if side == "A" else "rem_from_B"] == 0
    assert plan["add_to_A"] == plan["add_to_B"] == 0


@pytest.mark.parametrize("feature", ["watchlist", "history", "collection"])
@pytest.mark.parametrize("side", ["A", "B"])
@pytest.mark.parametrize("semantics,observed", [("present", False), ("delta", True), ("event", True)])
@pytest.mark.parametrize("per_feature", [False, True])
def test_cross_id_missing_row_respects_provider_capabilities(
    config_base, monkeypatch, feature, side, semantics, observed, per_feature,
):
    caps = {"index_semantics": semantics, "observed_deletes": observed}
    if per_feature:
        caps = {"index_semantics": "present", "observed_deletes": True, feature: caps}
    plan = deletion_plan(monkeypatch, feature, side, caps=caps)
    assert plan["rem_from_A"] == plan["rem_from_B"] == 0


@pytest.mark.parametrize("feature", ["watchlist", "history", "collection"])
@pytest.mark.parametrize("side", ["A", "B"])
@pytest.mark.parametrize("scenario", [
    "no_change", "initial_missing", "unrelated", "removal_disabled", "observed_disabled",
    "mass_guard", "drop_guard", "offline",
])
def test_cross_id_removal_preserves_existing_safety_controls(config_base, monkeypatch, feature, side, scenario):
    plan = deletion_plan(monkeypatch, feature, side, scenario=scenario)
    assert plan["rem_from_A"] == plan["rem_from_B"] == 0
    if scenario == "no_change":
        assert plan["add_to_A"] == plan["add_to_B"] == 0
    if scenario == "initial_missing":
        assert plan["add_to_A" if side == "A" else "add_to_B"] == 1


@pytest.mark.parametrize("feature", ["watchlist", "history", "collection"])
@pytest.mark.parametrize("side", ["A", "B"])
def test_same_id_deletion_still_removes_peer(config_base, monkeypatch, feature, side):
    plan = deletion_plan(monkeypatch, feature, side, scenario="same_id")
    assert plan["rem_from_B" if side == "A" else "rem_from_A"] == 1
    assert plan["add_to_A"] == plan["add_to_B"] == 0


@pytest.mark.parametrize("provider", [_mod_ANILIST, _mod_PLEX, _mod_TAUTULLI, _mod_TRACEARR])
@pytest.mark.parametrize("side", ["A", "B"])
def test_native_provider_missing_row_does_not_remove_peer(config_base, monkeypatch, provider, side):
    caps = deepcopy(provider.OPS.capabilities())
    feature = "history"
    rows = None
    if provider is _mod_ANILIST:
        feature = "watchlist"
        rows = (
            {"type": "show", "ids": {"tmdb": "31911", "mal": "5114", "anilist": "5114"}},
            {"type": "show", "ids": {"mal": "5114", "anilist": "5114"}},
        )
    if provider is _mod_PLEX:
        semantics = provider.OPS.index_semantics({"plex": {"history": {"include_marked_watched": False}}}, feature=feature)
        assert semantics == "delta"
        caps[feature] = {**caps.get(feature, {}), "index_semantics": semantics}
    plan = deletion_plan(monkeypatch, feature, side, caps=caps, rows=rows)
    assert plan["rem_from_A"] == plan["rem_from_B"] == 0
