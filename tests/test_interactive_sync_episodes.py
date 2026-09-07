# tests/test_interactive_sync_episodes.py
# CrossWatch - Episode suggestion regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from copy import deepcopy

import pytest
import requests

from services.interactive_sync_episodes import match_episode, suggest_episodes


CFG = {"mdblist": {"api_key": "destination"}, "tmdb": {"api_key": "metadata"}}


def row(n=1):
    return dict(id=str(n), provider="MDBLIST", instance="default", item=dict(type="episode", season=3, episode=n,
                ids={"tvdb": str(1000+n)}, show_ids={"tmdb": "10"}, watched_at="2025-10-03T08:00:00Z"))


def mock_api(monkeypatch, payloads):
    calls = []
    class Response:
        status_code = 200
        def __init__(self, body): self.body = body
        def json(self): return self.body
    def get(self, url, **kwargs):
        path = url.split("/3/", 1)[1]
        calls.append(path)
        assert kwargs["params"]["api_key"] == "metadata"
        return Response(payloads[path])
    monkeypatch.setattr(requests.Session, "get", get)
    return calls


def test_ten_exact_episode_ids_correct_split_show_and_coordinates_without_writes(monkeypatch):
    rows = [row(n) for n in range(1,11)]
    before = deepcopy(rows)
    payloads = {f"find/{1000+n}": {"tv_episode_results": [dict(show_id=20, season_number=1, episode_number=n+2)]} for n in range(1,11)}
    payloads["tv/20"] = {"name": "Monster: Another Story"}
    calls = mock_api(monkeypatch, payloads)
    result = suggest_episodes(CFG, [(r, r["item"]) for r in rows])
    assert rows == before
    assert calls.count("tv/20") == 1
    assert [r["match"]["episode"] for r in result["results"]] == list(range(3,13))
    assert all(r["match"]["season"] == 1 and r["match"]["ids"] == {"tmdb": "20"} for r in result["results"])


def test_conflicting_episode_identifiers_do_not_guess(monkeypatch):
    r = row()
    r["item"]["ids"]["imdb"] = "tt123"
    mock_api(monkeypatch, {"find/1001": {"tv_episode_results": [dict(show_id=20,season_number=1,episode_number=1)]},
                           "find/tt123": {"tv_episode_results": [dict(show_id=30,season_number=1,episode_number=1)]}})
    result = suggest_episodes(CFG, [(r, r["item"])])["results"][0]
    assert result["match"] is None and "disagree" in result["reason"]


def test_title_and_air_date_match_across_seasons(monkeypatch):
    r = row()
    r["item"]["ids"] = {}
    draft = {**r["item"], "show_ids": {"tmdb": "20"}}
    mock_api(monkeypatch, {
        "tv/10/season/3/episode/1": {"name": "A New Beginning", "air_date": "2025-10-03"},
        "tv/20": {"name": "Monster", "seasons": [{"season_number": 1}, {"season_number": 2}]},
        "tv/20/season/1": {"episodes": [{"name": "A New Beginning", "air_date": "2025-10-03", "season_number": 1,"episode_number": 7}]},
        "tv/20/season/2": {"episodes": [{"name": "Unrelated", "season_number": 2, "episode_number": 1}]},
    })
    match = suggest_episodes(CFG, [(r, draft)])["results"][0]["match"]
    assert match["season"] == 1 and match["episode"] == 7


@pytest.mark.parametrize("name", ["Episode 3", "S03E03", ""])
def test_generic_episode_names_are_not_evidence(name):
    assert match_episode({"name": name}, [{"name": name, "season_number": 1, "episode_number": 3}]) is None


def test_duplicate_titles_are_ambiguous_and_watched_dates_are_not_air_dates():
    candidates = [{"name": "Pilot", "air_date": "2024-01-01"}, {"name": "Pilot", "air_date": "2025-01-01"}]
    assert match_episode({"name": "Pilot", "watched_at": "2025-01-01"}, candidates) is None
    assert match_episode({"name": "Pilot", "air_date": "2025-01-01"}, candidates) == candidates[1]


def test_missing_configuration_does_not_search(monkeypatch):
    def fail(*args, **kwargs): pytest.fail("No request expected")
    monkeypatch.setattr(requests.Session, "get", fail)
    r = row()
    assert suggest_episodes({"tmdb": {"api_key": "metadata"}}, [(r,r["item"])])["results"][0]["match"] is None


def test_failed_lookup_keeps_draft_and_hides_credentials(monkeypatch):
    def fail(*args, **kwargs): raise requests.Timeout("api_key=SECRET")
    monkeypatch.setattr(requests.Session, "get", fail)
    r = row()
    result = suggest_episodes(CFG, [(r,r["item"])])["results"][0]
    assert result["match"] is None and "SECRET" not in str(result)
