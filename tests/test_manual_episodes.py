from __future__ import annotations

import json
from typing import Any

import pytest

from api import manualAPI

SIMKL = {"provider": "SIMKL", "instance": "default"}
TRAKT = {"provider": "TRAKT", "instance": "default"}


def _targets(_cfg: Any, _user: Any = None) -> list[dict[str, Any]]:
    return [
        {**SIMKL, "history_enabled": True, "ratings_enabled": True, "watchlist_enabled": True, "episode_list": True},
        {**TRAKT, "history_enabled": True, "ratings_enabled": True, "watchlist_enabled": True, "collection_enabled": True, "episode_list": False},
    ]


class _Ops:
    def __init__(self, calls: list[tuple[str, str, list[dict[str, Any]]]], provider: str) -> None:
        self.calls = calls
        self.provider = provider

    def add(self, _cfg: Any, items: list[dict[str, Any]], feature: str = "") -> dict[str, Any]:
        self.calls.append((self.provider, feature, items))
        return {"ok": True, "count": len(items)}


remembered: list[tuple[tuple[Any, ...], dict[str, Any]]] = []


@pytest.fixture()
def calls(monkeypatch) -> list[tuple[str, str, list[dict[str, Any]]]]:
    seen: list[tuple[str, str, list[dict[str, Any]]]] = []
    monkeypatch.setattr("cw_platform.config_base.load_config", lambda: {})
    monkeypatch.setattr(manualAPI, "_manual_history_targets", _targets)
    monkeypatch.setattr(manualAPI, "request_user", lambda _request: None)
    monkeypatch.setattr(manualAPI, "load_sync_ops", lambda provider: _Ops(seen, provider))
    monkeypatch.setattr(manualAPI, "build_provider_config_view", lambda _cfg, provider, instance: {})
    monkeypatch.setattr(manualAPI, "_manual_external_ids", lambda _type, tmdb: {"tmdb": int(tmdb), "tvdb": 71470})
    monkeypatch.setattr(manualAPI, "_title_targets", lambda _cfg, rows, _request, **_kwargs: rows)
    monkeypatch.setattr(manualAPI, "_remember_sent", lambda *args, **kwargs: remembered.append((args, kwargs)))
    remembered.clear()
    return seen


def _body(response: Any) -> dict[str, Any]:
    return json.loads(response.body)


SHOW = {"type": "show", "tmdb": 655, "title": "Star Trek: The Next Generation", "year": 1987}


def test_manual_episodes_are_cleaned_sorted_and_unique() -> None:
    rows = manualAPI._manual_episodes([
        {"season": 1, "episode": 2, "air_date": "1987-09-28"},
        {"season": "1", "episode": "1"},
        {"season": 1, "episode": 2},
        {"season": -1, "episode": 1},
        {"season": 1, "episode": 0},
        "bad",
    ])

    assert rows == [
        {"season": 1, "episode": 1, "air_date": None},
        {"season": 1, "episode": 2, "air_date": "1987-09-28"},
    ]


def test_simkl_episode_rows_group_seasons_and_keep_specials_last() -> None:
    rows = manualAPI._simkl_episode_rows([
        {"type": "episode", "season": 1, "episode": 2, "title": "Encounter at Farpoint (2)", "date": "1987-09-28T20:00:00-05:00"},
        {"type": "episode", "season": 1, "episode": 1, "title": "Encounter at Farpoint (1)", "date": "1987-09-28T20:00:00-05:00"},
        {"type": "special", "episode": 1, "title": "Making of"},
        {"type": "episode", "season": 1, "episode": 1, "title": "Duplicate"},
        {"type": "episode", "title": "No numbers"},
    ])

    assert [row["season"] for row in rows] == [1, 0]
    assert rows[0]["name"] == "Season 1"
    assert rows[0]["episode_count"] == 2
    assert [ep["episode"] for ep in rows[0]["episodes"]] == [1, 2]
    assert rows[0]["episodes"][1] == {"episode": 2, "name": "Encounter at Farpoint (2)", "air_date": "1987-09-28"}
    assert rows[1]["name"] == "Specials"


MOVIE = {"type": "movie", "tmdb": 238, "title": "The Godfather", "year": 1972}


def _send_movie(**extra: Any) -> dict[str, Any]:
    return _body(manualAPI.api_manual_watched(payload={"item": MOVIE, "providers": [TRAKT], "actions": {"history": True}, **extra}))


def test_a_watched_title_goes_out_as_a_rewatch(calls, monkeypatch) -> None:
    monkeypatch.setattr(manualAPI, "history_rewatch_supported", lambda provider, ops, mode: True)
    monkeypatch.setattr(manualAPI, "config_with_history_rewatches", lambda cfg, on: {"rewatches": on})
    assert _send_movie(rewatch=["TRAKT:default"])["ok"] is True
    _provider, _feature, items = calls[0]
    assert items[0]["_cw_rewatch_sync"] is True
    assert "@" in items[0]["_cw_event_key"]
    assert remembered[0][1] == {"rewatch": True}


def test_a_plain_add_stays_plain(calls) -> None:
    assert _send_movie()["ok"] is True
    assert "_cw_rewatch_sync" not in calls[0][2][0]
    assert remembered[0][1] == {"rewatch": False}


def test_a_provider_that_keeps_one_play_is_skipped_for_a_rewatch(calls, monkeypatch) -> None:
    monkeypatch.setattr(manualAPI, "history_rewatch_supported", lambda provider, ops, mode: False)
    body = _send_movie(rewatch=["TRAKT:default"])
    assert calls == []
    assert body["results"][0]["history_skipped"] == "rewatch_not_supported"
    assert body["ok"] is True


EPISODES = [{"season": 1, "episode": 1, "air_date": "1987-09-28"}, {"season": 1, "episode": 2, "air_date": "1987-10-05"}]


def _send_episodes(**extra: Any) -> dict[str, Any]:
    payload = {"item": SHOW, "providers": [TRAKT], "actions": {"history": True}, "date_mode": "release", "episodes": EPISODES, **extra}
    return _body(manualAPI.api_manual_watched(payload=payload))


WATCHED_FIRST = [{"target": "TRAKT:default", "episodes": [{"season": 1, "episode": 1}]}]


def test_one_play_providers_still_get_the_unwatched_episodes(calls, monkeypatch) -> None:
    monkeypatch.setattr(manualAPI, "history_rewatch_supported", lambda provider, ops, mode: False)
    body = _send_episodes(rewatch=WATCHED_FIRST)
    assert [row["episode"] for row in calls[0][2]] == [2]
    assert "_cw_rewatch_sync" not in calls[0][2][0]
    assert "history_skipped" not in body["results"][0]


def test_one_play_providers_skip_when_every_episode_is_watched(calls, monkeypatch) -> None:
    monkeypatch.setattr(manualAPI, "history_rewatch_supported", lambda provider, ops, mode: False)
    every = [{"target": "TRAKT:default", "episodes": [{"season": 1, "episode": 1}, {"season": 1, "episode": 2}]}]
    assert _send_episodes(rewatch=every)["results"][0]["history_skipped"] == "rewatch_not_supported"
    assert calls == []


def test_only_watched_episodes_go_out_as_rewatches(calls, monkeypatch) -> None:
    monkeypatch.setattr(manualAPI, "history_rewatch_supported", lambda provider, ops, mode: True)
    monkeypatch.setattr(manualAPI, "config_with_history_rewatches", lambda cfg, on: {"rewatches": on})
    _send_episodes(rewatch=WATCHED_FIRST)
    first, second = calls[0][2]
    assert first["_cw_rewatch_sync"] is True
    assert "_cw_rewatch_sync" not in second


def test_history_follows_the_whole_show_or_episode_rule_when_sending(calls, monkeypatch) -> None:
    rows = [{**TRAKT, "history_enabled": True, "history_whole_show": False, "history_episodes": True}]
    monkeypatch.setattr(manualAPI, "_title_targets", lambda _cfg, _rows, _request, **_kwargs: rows)
    whole = _body(manualAPI.api_manual_watched(payload={"item": SHOW, "providers": [TRAKT], "actions": {"history": True}}))
    assert whole["results"][0]["history_skipped"] == "history_not_supported"
    assert calls == []
    _send_episodes()
    assert len(calls[0][2]) == 2


def test_quick_add_keeps_what_was_sent_for_every_feature(calls) -> None:
    manualAPI.api_manual_watched(payload={"item": MOVIE, "providers": [TRAKT], "actions": {"history": True, "watchlist": True, "rating": True, "collection": True}, "rating": 8})
    assert [args[2] for args, _kwargs in remembered] == ["history", "watchlist", "ratings", "collection"]


def _rules(monkeypatch, *, linked: bool = False, anime: bool | None = None, caps: Any = None) -> None:
    ops = type("Ops", (), {"capabilities": lambda self: caps or {}})()
    monkeypatch.undo()
    monkeypatch.setattr(manualAPI, "load_sync_ops", lambda provider: ops)
    monkeypatch.setattr(manualAPI, "user_profiles_for_instance", lambda cfg, provider, instance: [{"id": "P02"}] if linked else [])
    monkeypatch.setattr(manualAPI, "_manual_title_is_anime", lambda cfg, media_type, tmdb: anime)


ROW = {"provider": "TRAKT", "instance": "default", "history_enabled": True, "watchlist_enabled": True}
ANILIST = {"provider": "ANILIST", "instance": "default", "history_enabled": True}


def test_quick_add_leaves_out_other_peoples_accounts(monkeypatch) -> None:
    _rules(monkeypatch, linked=True)
    assert manualAPI._title_targets({}, [ROW], None) == []


def test_quick_add_shows_a_profile_only_its_own_accounts(monkeypatch) -> None:
    import api.editorAPI as editor

    _rules(monkeypatch, linked=True)
    monkeypatch.setattr(editor, "_profile_instances", lambda cfg, profile: {("TRAKT", "default")})
    assert [row["provider"] for row in manualAPI._title_targets({}, [ROW, ANILIST], None, user_profile="P01")] == ["TRAKT"]


def test_episode_only_history_providers_stay_available_for_a_show(monkeypatch) -> None:
    caps = {"history": {"write": True, "types": {"movies": True, "shows": False, "episodes": True}}}
    _rules(monkeypatch, caps=caps)
    row = manualAPI._title_targets({}, [ROW], None, media_type="show")[0]
    assert row["history_enabled"] is True
    assert row["history_whole_show"] is False
    assert row["history_episodes"] is True
    assert "history_whole_show" not in manualAPI._title_targets({}, [ROW], None, media_type="movie")[0]


def test_quick_add_blocks_read_only_and_unsupported_types_per_feature(monkeypatch) -> None:
    _rules(monkeypatch, caps={"history": {"write": False}, "watchlist": {"write": True, "types": {"movies": True, "shows": False}}})
    assert manualAPI._title_targets({}, [ROW], None, media_type="show") == []
    rows = manualAPI._title_targets({}, [ROW], None, media_type="movie")
    assert rows[0]["history_enabled"] is False and rows[0]["watchlist_enabled"] is True


@pytest.mark.parametrize(("anime", "listed"), [(False, False), (True, True), (None, True)])
def test_anime_only_providers_stay_unless_the_title_is_known_not_to_be_anime(monkeypatch, anime, listed) -> None:
    _rules(monkeypatch, anime=anime)
    assert bool(manualAPI._title_targets({}, [ANILIST], None, media_type="movie", tmdb_id=238)) is listed


def test_manual_watched_sends_picked_episodes_with_their_air_dates(calls) -> None:
    res = manualAPI.api_manual_watched(payload={
        "item": SHOW,
        "providers": [SIMKL],
        "actions": {"history": True},
        "date_mode": "release",
        "episodes": [{"season": 1, "episode": 2, "air_date": "1987-09-28"}],
        "episode_source": "SIMKL:default",
    })

    body = _body(res)
    assert res.status_code == 200 and body["ok"] is True
    assert body["episodes"] == 1
    provider, feature, items = calls[0]
    assert (provider, feature) == ("SIMKL", "history")
    assert items == [{
        "type": "episode",
        "title": "Star Trek: The Next Generation",
        "series_title": "Star Trek: The Next Generation",
        "show_ids": {"tmdb": 655, "tvdb": 71470},
        "ids": {},
        "season": 1,
        "episode": 2,
        "watched_at": "1987-09-28T12:00:00Z",
        "year": 1987,
    }]


def test_manual_watched_keeps_the_whole_show_without_episodes(calls) -> None:
    res = manualAPI.api_manual_watched(payload={"item": SHOW, "providers": [TRAKT], "actions": {"history": True}})

    assert _body(res)["episodes"] == 0
    assert calls[0][2][0]["type"] == "show"


def test_manual_collection_is_sent_only_where_it_is_supported(calls) -> None:
    res = manualAPI.api_manual_watched(payload={
        "item": SHOW,
        "providers": [SIMKL, TRAKT],
        "actions": {"history": False, "collection": True},
        "date_mode": "custom",
        "watched_on": "2024-05-06",
    })

    body = _body(res)
    assert res.status_code == 200 and body["ok"] is True
    assert [(provider, feature) for provider, feature, _items in calls] == [("TRAKT", "collection")]
    sent = calls[0][2][0]
    assert sent["type"] == "show" and sent["collected_at"] == "2024-05-06T12:00:00Z" and "watched_at" not in sent
    assert body["results"][0]["collection_skipped"] == "collection_not_supported"


def test_provider_episode_list_only_goes_to_that_provider(calls) -> None:
    res = manualAPI.api_manual_watched(payload={
        "item": SHOW,
        "providers": [SIMKL, TRAKT],
        "actions": {"history": True},
        "episodes": [{"season": 1, "episode": 2}],
        "episode_source": "SIMKL:default",
    })

    assert res.status_code == 400
    assert _body(res)["error"] == "episode_source_mismatch"
    assert calls == []


def test_tmdb_episode_list_may_go_to_every_provider(calls) -> None:
    res = manualAPI.api_manual_watched(payload={
        "item": SHOW,
        "providers": [SIMKL, TRAKT],
        "actions": {"history": True},
        "episodes": [{"season": 1, "episode": 1}],
        "episode_source": "tmdb",
    })

    assert _body(res)["ok"] is True
    assert [provider for provider, _feature, _items in calls] == ["SIMKL", "TRAKT"]


def test_invalid_episode_picks_are_refused(calls) -> None:
    res = manualAPI.api_manual_watched(payload={
        "item": SHOW,
        "providers": [SIMKL],
        "actions": {"history": True},
        "episodes": [{"season": "x", "episode": 1}],
    })

    assert res.status_code == 400
    assert _body(res)["error"] == "invalid_episodes"


def test_episode_list_route_uses_tmdb_or_an_allowed_provider(calls, monkeypatch) -> None:
    monkeypatch.setattr(manualAPI, "_tmdb_season_list", lambda tmdb: [manualAPI._season_row(1, "Season 1", None, 25)])
    monkeypatch.setattr(manualAPI, "_provider_episode_list", lambda _cfg, provider, instance, tmdb: [manualAPI._season_row(1, "", [{"episode": 1, "name": "", "air_date": ""}])])

    tmdb = _body(manualAPI.api_manual_episodes(request=None, tmdb=655, source="tmdb", season=None))
    simkl = _body(manualAPI.api_manual_episodes(request=None, tmdb=655, source="SIMKL:default", season=None))
    trakt = manualAPI.api_manual_episodes(request=None, tmdb=655, source="TRAKT:default", season=None)

    assert tmdb["seasons"][0]["episode_count"] == 25
    assert simkl["source"] == "SIMKL:default"
    assert simkl["seasons"][0]["episodes"][0]["episode"] == 1
    assert trakt.status_code == 400
    assert _body(trakt)["error"] == "episode_list_not_available"
