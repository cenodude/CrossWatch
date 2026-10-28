# /tests/test_pair_library_only_defaults.py
# CrossWatch - Only items in destination library defaults for newly created sync pairs
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import pytest

from api import syncAPI as api

FEATURES = ("watchlist", "ratings", "history", "progress")
EXPECTED = {
    "PLEX": {"history", "ratings", "progress"},
    "EMBY": {"history", "progress", "watchlist"},
    "JELLYFIN": {"history", "progress", "watchlist"},
    "SILO": {"history", "ratings", "progress"},
    "TRAKT": set(),
}


def _create(monkeypatch: pytest.MonkeyPatch, source: str, target: str, features: dict, mode: str = "one-way") -> dict:
    cfg: dict = {"pairs": [], "anime_mapping": {"enabled": False}}
    monkeypatch.setattr(api, "_env", lambda: (lambda: cfg, lambda value: None))
    assert api.api_pairs_add(api.PairIn(source=source, target=target, mode=mode, features=features))["ok"]
    return cfg["pairs"][0]["features"]


@pytest.mark.parametrize("target", sorted(EXPECTED))
def test_new_pair_turns_library_only_on_where_the_destination_supports_it(monkeypatch: pytest.MonkeyPatch, target: str) -> None:
    features = {feature: {"enable": True, "add": True} for feature in FEATURES}
    actual = _create(monkeypatch, "TRAKT" if target != "TRAKT" else "SIMKL", target, features)
    on = {feature for feature in FEATURES if feature in actual and actual[feature].get("library_only") is True}
    assert on == EXPECTED[target]


def test_two_way_pair_checks_both_sides_and_explicit_choice_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    actual = _create(monkeypatch, "SILO", "TRAKT", {"history": {"enable": True}}, mode="two-way")
    assert actual["history"]["library_only"] is True
    actual = _create(monkeypatch, "TRAKT", "SILO", {"history": {"enable": True, "library_only": False}})
    assert actual["history"]["library_only"] is False
    actual = _create(monkeypatch, "SILO", "TRAKT", {"history": {"enable": True}})
    assert "library_only" not in actual["history"]


def test_plex_cloud_write_fallback_turns_library_only_off(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg: dict = {"pairs": [], "anime_mapping": {"enabled": False}}
    monkeypatch.setattr(api, "_env", lambda: (lambda: cfg, lambda value: None))
    created = api.api_pairs_add(api.PairIn(source="TRAKT", target="PLEX", features={"history": {"enable": True, "library_only": True}},
                                           providers={"plex": {"history": {"discover_fallback": True}}}))
    assert created["ok"]
    pair = cfg["pairs"][0]
    assert pair["features"]["history"]["library_only"] is False

    pair["providers"] = {}
    assert api.api_pairs_update(pair["id"], api.PairPatch(features={"history": {"enable": True, "library_only": True}}))["ok"]
    assert cfg["pairs"][0]["features"]["history"]["library_only"] is True
    assert api.api_pairs_update(pair["id"], api.PairPatch(providers={"plex": {"history": {"discover_fallback": True}}}))["ok"]
    assert cfg["pairs"][0]["features"]["history"]["library_only"] is False


def test_silo_collection_source_survives_saving(monkeypatch: pytest.MonkeyPatch) -> None:
    actual = _create(monkeypatch, "SILO", "TRAKT", {"collection": {"enable": True, "add": True}})
    assert actual["collection"]["enable"] is True
