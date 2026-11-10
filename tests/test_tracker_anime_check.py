# tests/test_tracker_anime_check.py
# CrossWatch - Anime numbering in tracker TMDB health checks
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json

import pytest

from api import trackerAPI
from cw_platform.anime_mapping import history_coords, storage
from cw_platform.anime_mapping.overrides import upsert_override
from cw_platform.tracker_store import TrackerStore
from services.tracker_check import Lookups, classify
from tests.test_tracker_check import FakeTmdb, _episode, base, client


@pytest.fixture()
def mapping(config_base):
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps({
        "tvdb_show:10:s3": {"mal:1": {"13-24": "1-12"}},
        "tmdb_show:30:s1": {"mal:1": {"1-12": "1-12"}},
    }), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    return {"anime_mapping": {"enabled": True}}


def episode(**extra):
    return {"type": "episode", "series_title": "Anime", "season": 3, "episode": 13,
            "show_ids": {"tvdb": "10"}, "_cw_episode_order": "tvdb", **extra}


def provider():
    return FakeTmdb({
        ("show", "30", None): {"name": "Anime"},
        ("show", "30", 1): {"episodes": [{"episode_number": n} for n in range(1, 12)]},
        ("tvdb", "10", None): {"tv_results": [{"id": 30}]},
    })


@pytest.mark.parametrize("item", [
    episode(),
    episode(show_ids={"tvdb": "10", "tmdb": "30"}),
    episode(show_ids={"tvdb": "10", "tmdb": "30"}, _cw_episode_order=""),
    episode(show_ids={"tvdb": "10", "tmdb": "30", "mal": "1"}),
    episode(show_ids={"tvdb": "10", "tmdb": "30"}, season=1, episode=1, _cw_episode_order="tmdb"),
    episode(show_ids={"mal": "1"}, season=1, episode=1, _cw_episode_order=""),
])
def test_health_checks_translated_tmdb_episode(mapping, item):
    tmdb = provider()
    before = dict(item)
    result = classify(item, Lookups(tmdb, pause=0), history_coords.alias_context(mapping))
    assert result[0] == "ok"
    assert tmdb.calls[-1] == ("show", "30", 1)
    assert item == before


@pytest.mark.parametrize("axis,season,first,last", [("tvdb", 3, 13, 24), ("tmdb", 1, 1, 12)])
def test_health_honors_manual_overrides_without_losing_other_axis(mapping, axis, season, first, last):
    context = history_coords.alias_context(mapping)
    assert history_coords.tmdb_coordinate(episode(), context) == ("30", 1, 1)
    upsert_override({"media_type": "show", "match_provider": axis, "match_id": "10" if axis == "tvdb" else "30",
                     "match_season": season, "episode_from": first, "episode_to": last,
                     "episode_start_at": 1, "target_namespace": "mal", "target_id": "1"})
    tmdb = provider()
    assert classify(episode(), Lookups(tmdb, pause=0), history_coords.alias_context(mapping))[0] == "ok"
    assert tmdb.calls[-1] == ("show", "30", 1)


def test_mapped_episode_still_must_exist_on_tmdb(mapping):
    result = classify(episode(episode=24), Lookups(provider(), pause=0), history_coords.alias_context(mapping))
    assert result[0] == "episode_missing"
    assert (result[1]["season"], result[1]["episode"]) == (1, 12)


def test_coordinate_cache_respects_declared_numbering(mapping):
    context = history_coords.alias_context(mapping)
    item = episode(show_ids={"tvdb": "10", "tmdb": "30"})
    assert history_coords.tmdb_coordinate(item, context) == ("30", 1, 1)
    assert history_coords.tmdb_coordinate({**item, "_cw_episode_order": "tmdb"}, context) is None


def test_unmapped_anime_is_not_reported_as_verified(mapping):
    tmdb = provider()
    item = episode(show_ids={"tmdb": "30"}, season=1, episode=999, _cw_episode_order="", _anime_absolute=999)
    assert classify(item, Lookups(tmdb, pause=0), history_coords.alias_context(mapping))[0] == "episode_unverified"
    assert tmdb.calls == [("show", "30", None)]


def test_declared_tmdb_numbering_is_checked_even_with_anime_metadata(mapping):
    item = episode(show_ids={"tmdb": "30"}, season=1, episode=999, _cw_episode_order="tmdb", _anime_absolute=999)
    assert classify(item, Lookups(provider(), pause=0), history_coords.alias_context(mapping))[0] == "episode_missing"


@pytest.mark.parametrize("context", [("", None), ("v3", None)])
def test_missing_or_disabled_mapping_does_not_check_tvdb_numbers_on_tmdb(context):
    tmdb = provider()
    item = episode(show_ids={"tvdb": "10", "mal": "1"})
    assert classify(item, Lookups(tmdb, pause=0), context)[0] == "episode_unverified"
    assert tmdb.calls[-1] == ("show", "30", None)


def test_saved_aliases_work_without_index_but_not_when_disabled():
    item = episode(_cw_anime_aliases=["tvdb:10#s03e13", "tmdb:30#s01e01"])
    assert classify(item, Lookups(provider(), pause=0), ("v3", None))[0] == "ok"
    assert classify(item, Lookups(provider(), pause=0), ("", None))[0] == "episode_unverified"


def test_ambiguous_tmdb_mapping_is_not_guessed(mapping):
    paths = storage.paths("v3")
    data = json.loads(paths["mappings"].read_text("utf-8"))
    data["tmdb_show:31:s1"] = {"mal:1": {"1-12": "1-12"}}
    paths["mappings"].write_text(json.dumps(data), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    tmdb = provider()
    assert classify(episode(), Lookups(tmdb, pause=0), history_coords.alias_context(mapping))[0] == "episode_unverified"
    assert tmdb.calls[-1] == ("show", "30", None)


@pytest.mark.parametrize("order", ["tvdb", "simkl", "anime_entry"])
@pytest.mark.parametrize("enabled", [False, True])
def test_ordinary_tv_numbering_marker_does_not_imply_anime(mapping, order, enabled):
    mapping["anime_mapping"]["enabled"] = enabled
    tmdb = FakeTmdb()
    item = _episode(_cw_episode_order=order)
    assert classify(item, Lookups(tmdb, pause=0), history_coords.alias_context(mapping))[0] == "ok"
    assert tmdb.calls[-1] == ("show", "95396", 1)


@pytest.mark.parametrize("season,expected", [(1, "ok"), (7, "episode_missing")])
@pytest.mark.parametrize("namespace", ["mal", "anilist", "anidb", "kitsu"])
def test_anime_seasons_use_normal_season_existence_check(mapping, season, expected, namespace):
    tmdb = FakeTmdb()
    item = {"type": "season", "series_title": "Severance", "season": season,
            "show_ids": {"tmdb": "95396", namespace: "1"}, "_cw_episode_order": "tvdb"}
    assert classify(item, Lookups(tmdb, pause=0), history_coords.alias_context(mapping))[0] == expected
    assert tmdb.calls[-1] == ("show", "95396", season)


def test_mapping_evidence_identifies_anime_without_native_ids(mapping):
    tmdb = provider()
    item = episode(episode=999)
    assert classify(item, Lookups(tmdb, pause=0), history_coords.alias_context(mapping))[0] == "episode_unverified"
    assert tmdb.calls[-1] == ("show", "30", None)


def test_anime_movie_with_same_tmdb_id_does_not_flag_regular_show(mapping):
    paths = storage.paths("v3")
    paths["mappings"].write_text(json.dumps({"anilist:9001": {"tmdb_movie:95396": {}}}), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    item = _episode(_cw_episode_order="tvdb")
    assert classify(item, Lookups(FakeTmdb(), pause=0), history_coords.alias_context(mapping))[0] == "ok"


def test_health_api_passes_mapping_context_without_rewriting_history(mapping, client, base, monkeypatch):
    client.cfg.update(mapping)
    tmdb = provider()
    monkeypatch.setattr(trackerAPI, "_tmdb_provider", lambda: tmdb)
    store = TrackerStore(base)
    store.write_state("history", {"tvdb:10#s03e13": episode(show_ids={"tvdb": "10", "tmdb": "30"})})
    before = store.read_current("history")
    assert client.post("/api/tracker/check", json={}).status_code == 200
    rows = client.get("/api/tracker/items", params={"feature": "history"}).json()["items"]
    assert rows[0]["check"]["code"] == "ok"
    assert ("show", "30", 1) in tmdb.calls
    assert store.read_current("history") == before


def test_unverified_results_are_visible_in_attention_counts_and_filters(client, base):
    store = TrackerStore(base)
    store.write_state("history", {"tmdb:95396#s01e999": {
        "type": "episode", "series_title": "Severance", "show_ids": {"tmdb": "95396"},
        "season": 1, "episode": 999, "_anime_absolute": 999,
    }})
    assert client.post("/api/tracker/check", json={}).status_code == 200
    summary = client.get("/api/tracker/summary").json()
    assert summary["health"]["history"] == {"ok": 0, "attention": 1, "unchecked": 0}
    rows = client.get("/api/tracker/items", params={"feature": "history", "health": "attention"}).json()["items"]
    assert rows[0]["check"]["code"] == "episode_unverified"
    assert client.get("/api/tracker/items", params={"feature": "history", "health": "ok"}).json()["total"] == 0
