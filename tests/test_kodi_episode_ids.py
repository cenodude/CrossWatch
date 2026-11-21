# tests/test_kodi_episode_ids.py
# CrossWatch - Kodi prefers episode id matches over coordinate matches
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from providers.sync.kodi._common import LibraryIndex


SHOW_UNIQUEIDS = {"tmdb": "1429", "tvdb": "267440", "imdb": "tt2560140"}
SHOW_IDS = dict(SHOW_UNIQUEIDS)


def episode(kodi_id, season, number, uniqueid):
    return {"episodeid": kodi_id, "tvshowid": 7, "title": f"E{number}", "showtitle": "Attack on Titan",
            "season": season, "episode": number, "uniqueid": dict(uniqueid)}


def index(*episodes):
    return LibraryIndex([], list(episodes), [{"tvshowid": 7, "title": "Attack on Titan", "uniqueid": dict(SHOW_UNIQUEIDS)}])


def item(ids, season=1, number=2):
    return {"type": "episode", "ids": dict(ids), "show_ids": dict(SHOW_IDS), "season": season, "episode": number}


S1E2 = episode(102, 1, 2, {"tmdb": "65490", "tvdb": "4520243"})
S2E1 = episode(201, 2, 1, {"tmdb": "1265623", "tvdb": "5978808"})


def test_episode_id_beats_coordinates():
    target, reason = index(S1E2, S2E1).resolve(item({"tvdb": "5978808"}))

    assert reason == "ok"
    assert target["_kodi_id"] == 201


def test_conflicting_coordinate_hit_is_not_used():
    target, reason = index(S1E2).resolve(item({"tvdb": "5978808"}))

    assert target is None
    assert reason == "not_found"


def test_coordinates_still_work_without_episode_ids():
    target, reason = index(S1E2, S2E1).resolve(item({}))

    assert reason == "ok"
    assert target["_kodi_id"] == 102


def test_coordinate_hit_without_own_ids_still_matches():
    target, reason = index(episode(102, 1, 2, {})).resolve(item({"tvdb": "5978808"}))

    assert reason == "ok"
    assert target["_kodi_id"] == 102


def test_matching_ids_and_coordinates_resolve_once():
    target, reason = index(S1E2, S2E1).resolve(item({"tvdb": "4520243", "tmdb": "65490"}))

    assert reason == "ok"
    assert target["_kodi_id"] == 102
