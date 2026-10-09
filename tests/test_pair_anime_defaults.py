# /tests/test_pair_anime_defaults.py
# CrossWatch - Anime mapping defaults for newly created sync pairs
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy

import pytest

from api import syncAPI as api


@pytest.mark.parametrize("target", ["SIMKL", "MYANIMELIST", "KITSU", "ANILIST", "CROSSWATCH", "TRAKT"])
@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("tmdb", [False, True])
def test_new_pair_defaults(monkeypatch, target, enabled, tmdb):
    cfg = {"pairs": [], "anime_mapping": {"enabled": enabled}, "tmdb": {"api_key": "key" if tmdb else ""}}
    monkeypatch.setattr(api, "_env", lambda: (lambda: cfg, lambda value: None))
    features = {feature: {"enable": True, "add": True} for feature in ("watchlist", "ratings", "history", "progress")}
    result = api.api_pairs_add(api.PairIn(source="PLEX", target=target, features=features))
    assert result["ok"]
    actual = cfg["pairs"][0]["features"]
    eligible = enabled and target != "TRAKT"
    for feature in features:
        if feature == "progress" and target in {"MYANIMELIST", "KITSU", "ANILIST"}:
            assert feature not in actual
            continue
        expected = eligible and (feature != "history" or tmdb) and (feature != "progress" or target == "SIMKL")
        assert actual[feature]["use_anime_mapping"] is expected


@pytest.mark.parametrize("choice", [False, True])
def test_creation_preserves_explicit_choices(monkeypatch, choice):
    cfg = {"pairs": [], "anime_mapping": {"enabled": True}, "tmdb": {"api_key": "key"}}
    monkeypatch.setattr(api, "_env", lambda: (lambda: cfg, lambda value: None))
    features = {feature: {"enable": True, "use_anime_mapping": choice} for feature in ("watchlist", "ratings", "history", "progress")}
    assert api.api_pairs_add(api.PairIn(source="PLEX", target="SIMKL", features=features))["ok"]
    assert all(block["use_anime_mapping"] is choice for key, block in cfg["pairs"][0]["features"].items() if key in features)


def test_edit_does_not_apply_creation_defaults(monkeypatch):
    pair = {"id": "existing", "source": "PLEX", "target": "SIMKL", "features": {"history": {"enable": True}}}
    cfg = {"pairs": [deepcopy(pair)], "anime_mapping": {"enabled": True}, "tmdb": {"api_key": "key"}}
    monkeypatch.setattr(api, "_env", lambda: (lambda: cfg, lambda value: None))
    assert api.api_pairs_update("existing", api.PairPatch(enabled=True))["ok"]
    assert "use_anime_mapping" not in cfg["pairs"][0]["features"]["history"]


@pytest.mark.parametrize("mode,expected", [("one-way", False), ("two-way", True)])
def test_simkl_source_progress_default(mode, expected):
    item = {"source": "SIMKL", "target": "PLEX", "mode": mode, "features": {"progress": {}}}
    api._default_pair_anime_mapping({"anime_mapping": {"enabled": True}}, item)
    assert item["features"]["progress"]["use_anime_mapping"] is expected
