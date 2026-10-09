# /tests/test_simkl_anime_movie_parts.py
# CrossWatch - Standalone movies mapped to parts of SIMKL multi-part anime movies
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import HTTPException

from api.interactiveSyncAPI import prepare_mapping
from cw_platform.id_map import canonical_key, keys_for_item, minimal
from cw_platform.orchestrator._planner import _strong_keys
from cw_platform.orchestrator import _applier
from providers.sync.simkl import _history as history
from services.interactive_sync_mapping import anime_parts, search_candidates

BERSERK = {"simkl": "37137", "tmdb": "113082", "imdb": "tt2210479"}


def _part(number, watched_at="2026-09-01T12:00:00Z"):
    return {"type": "movie", "title": "Berserk: Ougon Jidai Hen", "year": 2012, "ids": {"simkl": "37137"},
            "part": number, "simkl_bucket": "anime", "anime_type": "movie", "watched_at": watched_at}


def _anime_row(*episodes):
    return {"last_watched_at": max(wa for _, wa in episodes), "anime_type": "movie",
            "show": {"title": "Berserk: Ougon Jidai Hen", "year": 2012, "ids": dict(BERSERK)},
            "seasons": [{"number": 1, "episodes": [{"number": n, "watched_at": wa} for n, wa in episodes]}]}


def _response(payload, status=200):
    return SimpleNamespace(status_code=status, ok=200 <= status < 300, text=json.dumps(payload), json=lambda: payload)


@pytest.fixture
def env(config_base, monkeypatch):
    monkeypatch.setattr(history, "state_file", lambda name: config_base / name)
    monkeypatch.setattr(history, "_headers", lambda *a, **k: {})
    monkeypatch.setattr(history, "_inject_adds_into_cache", Mock())
    monkeypatch.setattr(history, "_remember_source_aliases", Mock())
    monkeypatch.setattr(_applier, "record_unresolved", Mock())
    session = SimpleNamespace(post=Mock(return_value=_response({
        "added": {"movies": 0, "shows": 1, "episodes": 1},
        "not_found": {"movies": [], "shows": [], "episodes": []},
    })), get=Mock(return_value=_response({})))
    adapter = SimpleNamespace(client=SimpleNamespace(session=session), cfg=SimpleNamespace(timeout=5),
                              config={}, raw_cfg={})
    return adapter, session


def test_part_is_part_of_the_movie_identity():
    part2, part3 = _part(2), _part(3)
    plain = {"type": "movie", "ids": {"simkl": "37137"}}
    assert canonical_key(part2) == "simkl:37137#part:2"
    assert len({canonical_key(part2), canonical_key(part3), canonical_key(plain)}) == 3
    assert keys_for_item(part2).isdisjoint(keys_for_item(plain))
    assert minimal(part2)["part"] == 2
    assert _strong_keys({**part2, "ids": BERSERK}).isdisjoint(_strong_keys({"type": "movie", "ids": BERSERK}))


@pytest.mark.parametrize("item", [
    {"type": "movie", "ids": {"simkl": "1"}, "part": 0},
    {"type": "movie", "ids": {"simkl": "1"}, "part": True},
    {"type": "movie", "ids": {"simkl": "1"}, "part": "2"},
    {"type": "show", "ids": {"simkl": "1"}, "part": 2},
])
def test_invalid_or_non_movie_part_is_ignored(item):
    assert "#part" not in canonical_key(item)
    assert "part" not in minimal(item)


def test_multi_part_row_indexes_each_watched_part_and_keeps_part_one_plain():
    row = _anime_row((1, "2026-01-01T10:00:00Z"), (2, "2026-02-01T10:00:00Z"), (3, "2026-03-01T10:00:00Z"))
    out, *_ = history._parse_rows([], [], [row], limit=None)
    items = sorted(out.values(), key=lambda item: item["watched_at"])
    assert [item.get("part") for item in items] == [None, 2, 3]
    assert [item["watched_at"] for item in items] == ["2026-01-01T10:00:00Z", "2026-02-01T10:00:00Z", "2026-03-01T10:00:00Z"]
    assert all(item["simkl_bucket"] == "anime" and item["type"] == "movie" for item in items)
    assert items[0]["ids"] == BERSERK
    assert items[1]["ids"] == items[2]["ids"] == {"simkl": "37137"}
    history._dedupe_history_movies(out)
    assert len(out) == 3


def test_single_part_row_keeps_one_plain_movie():
    row = _anime_row((1, "2026-01-01T10:00:00Z"))
    out, *_ = history._parse_rows([], [], [row], limit=None)
    assert [item.get("part") for item in out.values()] == [None]


def test_mapped_part_is_written_as_that_single_anime_episode(env):
    adapter, session = env
    count, unresolved = history.add(adapter, [_part(2)])
    body = session.post.call_args.kwargs["json"]
    assert body == {"anime": [{"ids": {"simkl": "37137"}, "added_at": "2026-09-01T12:00:00Z", "seasons": [{"number": 1, "episodes": [
        {"number": 2, "watched_at": "2026-09-01T12:00:00Z"}]}]}]}
    assert count == 1 and unresolved == []


def test_two_parts_of_one_title_share_one_anime_entry(env):
    adapter, session = env
    history.add(adapter, [_part(2), _part(3, "2026-09-02T12:00:00Z")])
    anime = session.post.call_args.kwargs["json"]["anime"]
    assert len(anime) == 1
    assert [ep["number"] for ep in anime[0]["seasons"][0]["episodes"]] == [2, 3]


def test_rejected_part_is_unresolved_and_not_confirmed(env):
    adapter, session = env
    session.post.return_value = _response({"added": {"movies": 0, "shows": 0, "episodes": 0},
                                         "not_found": {"movies": [], "shows": [{"ids": {"simkl": "37137"}}], "episodes": []}})
    count, unresolved = history.add(adapter, [_part(2)])
    assert count == 0
    assert unresolved[0]["hint"] == "simkl_not_found:shows"
    assert adapter._simkl_history_add_confirmed_keys == []


def test_part_missing_from_the_title_is_unresolved(env):
    adapter, session = env
    session.post.return_value = _response({"added": {"movies": 0, "shows": 1, "episodes": 0},
                                         "not_found": {"movies": [], "shows": [], "episodes": [
                                             {"ids": {"simkl": 37137}, "seasons": [{"number": 1, "episodes": [{"number": 9}]}]}]}})
    count, unresolved = history.add(adapter, [_part(9)])
    assert count == 0
    assert unresolved[0]["hint"] == "simkl_not_found:episodes"
    assert unresolved[0]["item"]["part"] == 9


def test_removing_a_part_removes_only_that_episode():
    body, thaw, mapped, _detected, unmapped = history._native_anime_remove_body(
        [_part(2)], session=None, headers={}, timeout=5, state=history._AnimeResolveState({}, {}))
    assert body == {"anime": [{"ids": {"simkl": "37137"}, "seasons": [{"number": 1, "episodes": [{"number": 2}]}]}]}
    assert len(mapped) == 1 and unmapped == set()


def _mapping_row():
    return {"key": "tmdb:118412", "item": {"type": "movie", "title": "Berserk: The Golden Age Arc II - The Battle for Doldrey",
                                           "year": 2012, "ids": {"tmdb": "118412"}, "watched_at": "2026-09-01T12:00:00Z"}}


def test_prepare_mapping_keeps_part_and_blocks_the_original():
    key, item, blocks = prepare_mapping(_mapping_row(), {"type": "movie", "title": "Berserk: Ougon Jidai Hen", "year": 2012,
                                                         "ids": {"simkl": "37137"}, "part": 2,
                                                         "simkl_bucket": "anime", "anime_type": "movie"})
    assert key == "simkl:37137#part:2"
    assert (item["part"], item["simkl_bucket"], item["anime_type"]) == (2, "anime", "movie")
    assert item["watched_at"] == "2026-09-01T12:00:00Z"
    assert blocks == ["tmdb:118412"]


def test_prepare_mapping_without_part_keeps_the_original_simkl_fields():
    row = {"key": "simkl:5", "item": {"type": "movie", "ids": {"simkl": "5"}, "simkl_bucket": "anime", "anime_type": "movie",
                                      "watched_at": "2026-09-01T12:00:00Z"}}
    key, item, _blocks = prepare_mapping(row, {"type": "movie", "ids": {"tmdb": "10"}})
    assert key == "tmdb:10"
    assert (item["simkl_bucket"], item["anime_type"]) == ("anime", "movie")
    assert "part" not in item


@pytest.mark.parametrize("corrected", [
    {"type": "movie", "ids": {"simkl": "37137"}, "part": 0},
    {"type": "movie", "ids": {"simkl": "37137"}, "part": "2"},
    {"type": "show", "ids": {"simkl": "37137"}, "part": 2},
    {"type": "movie", "ids": {"simkl": "37137"}, "simkl_bucket": "other"},
])
def test_prepare_mapping_rejects_invalid_part_data(corrected):
    with pytest.raises(HTTPException):
        prepare_mapping(_mapping_row(), corrected)


def _simkl_get(monkeypatch, bodies):
    import requests

    calls = []

    def get(self, url, **kwargs):
        calls.append((url, kwargs.get("headers") or {}))
        return _response(bodies[url.split("api.simkl.com", 1)[1]])
    monkeypatch.setattr(requests.Session, "get", get)
    monkeypatch.setattr("cw_platform.simkl_http.pace_session", lambda client: None)
    monkeypatch.setattr("providers.auth._auth_SIMKL.ensure_fresh", lambda instance: "")
    return calls


SIMKL_CFG = {"simkl": {"client_id": "key", "api_key": "key", "access_token": "token"}}
SIMKL_ROW = {"provider": "SIMKL", "instance": "default", "item": {"type": "movie", "title": "Berserk II"}}


def test_movie_search_finds_anime_movies_without_loading_their_parts(monkeypatch):
    calls = _simkl_get(monkeypatch, {
        "/search/movie": [],
        "/search/anime": [{"title": "Berserk: Ougon Jidai Hen", "year": 2012, "endpoint_type": "anime", "type": "movie",
                           "ids": {"simkl_id": 37137, "tmdb": "113082"}},
                          {"title": "Berserk", "year": 2016, "endpoint_type": "anime", "type": "tv", "ids": {"simkl_id": 1}}],
    })
    result = search_candidates(SIMKL_CFG, SIMKL_ROW, "berserk golden age")
    assert [(r["ids"]["simkl"], r["simkl_bucket"], r["anime_type"]) for r in result["results"]] == [("37137", "anime", "movie")]
    assert "parts" not in result["results"][0]
    assert len(calls) == 2
    assert all(headers.get("Authorization") == "Bearer token" for _url, headers in calls)
    assert "token" not in str(result)


def test_part_lookup_lists_regular_episodes_only_for_multi_part_titles(monkeypatch):
    calls = _simkl_get(monkeypatch, {
        "/anime/episodes/37137": [
            {"episode": 1, "type": "episode", "title": "The Egg of the King", "date": "2012-02-04T00:00:00+09:00"},
            {"episode": 2, "type": "episode", "title": "The Battle for Doldrey", "date": "2012-06-23T00:00:00+09:00"},
            {"episode": 1, "type": "special", "title": "Music Video"}],
        "/anime/episodes/647": [{"episode": 1, "type": "episode", "title": "Advent Children", "date": "2005-09-14"}],
    })
    assert anime_parts(SIMKL_CFG, SIMKL_ROW, "37137")["parts"] == [
        {"number": 1, "title": "The Egg of the King", "year": 2012},
        {"number": 2, "title": "The Battle for Doldrey", "year": 2012}]
    assert anime_parts(SIMKL_CFG, SIMKL_ROW, "647")["parts"] == []
    assert all(headers.get("Authorization") == "Bearer token" for _url, headers in calls)


def test_search_uses_the_refreshed_token(monkeypatch):
    calls = _simkl_get(monkeypatch, {"/search/movie": [], "/search/anime": []})
    monkeypatch.setattr("providers.auth._auth_SIMKL.ensure_fresh", lambda instance: "fresh")
    search_candidates(SIMKL_CFG, SIMKL_ROW, "berserk")
    assert {headers.get("Authorization") for _url, headers in calls} == {"Bearer fresh"}


@pytest.mark.parametrize("row,simkl_id", [({**SIMKL_ROW, "provider": "TRAKT"}, "37137"), (SIMKL_ROW, "../users")])
def test_part_lookup_is_simkl_only_and_numeric(row, simkl_id):
    with pytest.raises(HTTPException):
        anime_parts(SIMKL_CFG, row, simkl_id)
