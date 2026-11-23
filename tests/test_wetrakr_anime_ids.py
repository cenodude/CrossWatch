# tests/test_wetrakr_anime_ids.py
# CrossWatch - WeTrakr anime ids on reads, writes, matching and the anime mapping pair gate
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy

import pytest

from providers.sync.wetrakr import _common as common
from providers.sync.wetrakr._progress import scrobble_media
from test_wetrakr_sync import Response, env

FRIEREN = {"id": 1172487, "type": "show", "title": "Frieren: Beyond Journey's End",
           "ids": {"tmdb": 209867, "imdb": "tt22248376", "tvdb": 424536, "anilist": 154587, "myanimelist": 52991, "anidb": 18886, "kitsu": 49240}}
EPISODE = {"id": 4519112, "type": "episode", "title": "The Journey's End", "ids": {"tmdb": 3946240, "imdb": "tt23861604", "tvdb": 9350138},
           "season_number": 1, "number": 1, "show": FRIEREN}
ANIME_IDS = {"mal": "52991", "anilist": "154587", "kitsu": "49240", "anidb": "18886"}


def anime_server(env):
    def hook(method, path, kwargs):
        if method != "POST" or not path.startswith("/sync/tracking"):
            return None
        env.server.bump()
        missing = []
        for row in kwargs["json"].get("shows", []):
            known = row.get("id") == FRIEREN["id"] or any(str(FRIEREN["ids"].get(k)) == str(v) for k, v in row.get("ids", {}).items())
            if not known:
                missing.append(row)
            elif path.endswith("/remove"):
                env.server.planning = [entry for entry in env.server.planning if entry["id"] != FRIEREN["id"]]
            else:
                env.server.planning.append(copy.deepcopy(FRIEREN))
        return Response({"added": {"total": 1}, "notFound": {"shows": missing} if missing else {}})

    env.server.hook = hook


def posts(env):
    return [(path, kwargs["json"]) for method, path, kwargs in env.server.calls if method == "POST"]


def test_reads_keep_anime_ids_on_shows_only():
    show = common.media_item(FRIEREN, "show")
    assert {key: show["ids"][key] for key in ANIME_IDS} == ANIME_IDS
    episode = common.media_item(EPISODE, "episode")
    assert not set(ANIME_IDS) & (set(episode["ids"]) | set(episode["show_ids"]))
    assert episode["show_ids"]["tmdb"] == "209867"


@pytest.mark.parametrize("ids,expected", [
    ({"tmdb": "209867", "mal": "52991"}, {"ids": {"tmdb": 209867}}),
    ({"mal": "52991", "anilist": "154587"}, {"ids": {"myanimelist": 52991}}),
    ({"anilist": "154587"}, {"ids": {"anilist": 154587}}),
    ({"kitsu": "49240"}, {"ids": {"kitsu": 49240}}),
    ({"anidb": "18886"}, {"ids": {"anidb": 18886}}),
])
def test_anime_ids_are_the_last_write_identifier(ids, expected):
    assert common.identifier(ids, anime=True) == expected


def test_identifier_ignores_anime_ids_unless_asked():
    with pytest.raises(common.WeTrakrSyncError, match="missing_supported_id"):
        common.identifier({"mal": "52991"})


def test_episodes_with_only_anime_show_ids_are_not_written(env):
    item = {"type": "episode", "title": "Frieren", "ids": {}, "show_ids": {"mal": "52991"}, "season": 2, "episode": 3,
            "watched_at": "2026-11-10T12:00:00Z"}
    result = env.adapter.add("history", [item])
    assert not result["ok"]
    assert result["unresolved"][0]["reason"] == "missing_supported_id"
    assert not env.server.calls


def test_anime_only_watchlist_item_is_written_and_matched_on_reread(env):
    anime_server(env)
    source = {"type": "show", "title": "Frieren", "ids": {"anilist": "154587", "mal": "52991"}}
    result = env.adapter.add("watchlist", [source])
    assert result["ok"] and len(result["confirmed_keys"]) == 1
    assert posts(env) == [("/sync/tracking", {"shows": [{"ids": {"myanimelist": 52991}, "status": "planning"}]})]
    env.server.calls.clear()
    assert env.adapter.add("watchlist", [source])["ok"]
    assert not posts(env)
    assert env.adapter.remove("watchlist", [source])["ok"]
    assert posts(env) == [("/sync/tracking/remove", {"shows": [{"id": FRIEREN["id"], "status": "planning"}]})]
    assert not env.server.planning


def test_shared_anime_id_is_ambiguous_per_item_not_per_batch(env):
    anime_server(env)
    twin = {**copy.deepcopy(FRIEREN), "id": 1172488, "ids": {"tmdb": 999001, "myanimelist": 52991}}
    env.server.planning = [copy.deepcopy(FRIEREN), twin]
    shared = {"type": "show", "title": "Frieren", "ids": {"mal": "52991"}}
    other = {"type": "show", "title": "Frieren", "ids": {"tmdb": "209867"}}
    result = env.adapter.add("watchlist", [shared, other])
    key = common.item_key(env.adapter, "watchlist", shared)
    assert {row["key"]: row["reason"] for row in result["unresolved"]} == {key: "ambiguous_media_identity"}
    assert len(result["confirmed_keys"]) == 1
    assert not posts(env)


def test_items_with_regular_ids_keep_their_payload(env):
    anime_server(env)
    source = {"type": "show", "title": "Frieren", "ids": {"tmdb": "209867", "mal": "52991"}}
    assert env.adapter.add("watchlist", [source])["ok"]
    assert posts(env) == [("/sync/tracking", {"shows": [{"ids": {"tmdb": 209867}, "status": "planning"}]})]


def test_shared_identity_tokens_and_scrobbles_ignore_anime_ids():
    assert common.identity_tokens({"type": "show", "ids": {"mal": "52991"}}) == set()
    with pytest.raises(common.WeTrakrSyncError, match="missing_supported_id"):
        scrobble_media({"type": "episode", "show_ids": {"mal": "52991"}, "season": 1, "episode": 2})


def test_anime_mapping_pair_gate_includes_wetrakr():
    from cw_platform.anime_mapping.service import mapping_enabled_for_pair
    from cw_platform.config_base import ANIME_MAPPING_PAIRS_DEFAULT, _normalize_anime_mapping

    cfg = {"anime_mapping": {"enabled": True, "use_for_pairs": ["anilist"]}}
    _normalize_anime_mapping(cfg)
    assert "wetrakr" in cfg["anime_mapping"]["use_for_pairs"]
    assert "wetrakr" in ANIME_MAPPING_PAIRS_DEFAULT
    assert mapping_enabled_for_pair(cfg, "TRAKT", "WETRAKR")
    assert mapping_enabled_for_pair({"anime_mapping": {"enabled": True}}, "PLEX", "WETRAKR")
    assert not mapping_enabled_for_pair({"anime_mapping": {"enabled": False}}, "TRAKT", "WETRAKR")
    assert not mapping_enabled_for_pair(cfg, "TRAKT", "PLEX")
