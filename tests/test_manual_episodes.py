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
        {**TRAKT, "history_enabled": True, "ratings_enabled": True, "watchlist_enabled": True, "episode_list": False},
    ]


class _Ops:
    def __init__(self, calls: list[tuple[str, str, list[dict[str, Any]]]], provider: str) -> None:
        self.calls = calls
        self.provider = provider

    def add(self, _cfg: Any, items: list[dict[str, Any]], feature: str = "") -> dict[str, Any]:
        self.calls.append((self.provider, feature, items))
        return {"ok": True, "count": len(items)}


@pytest.fixture()
def calls(monkeypatch) -> list[tuple[str, str, list[dict[str, Any]]]]:
    seen: list[tuple[str, str, list[dict[str, Any]]]] = []
    monkeypatch.setattr("cw_platform.config_base.load_config", lambda: {})
    monkeypatch.setattr(manualAPI, "_manual_history_targets", _targets)
    monkeypatch.setattr(manualAPI, "request_user", lambda _request: None)
    monkeypatch.setattr(manualAPI, "load_sync_ops", lambda provider: _Ops(seen, provider))
    monkeypatch.setattr(manualAPI, "build_provider_config_view", lambda _cfg, provider, instance: {})
    monkeypatch.setattr(manualAPI, "_manual_external_ids", lambda _type, tmdb: {"tmdb": int(tmdb), "tvdb": 71470})
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
