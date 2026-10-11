# tests/test_catalog_lookup.py
# CrossWatch - Provider season and episode layout lookups
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from types import SimpleNamespace

import pytest

from services import catalog_lookup as lookup


def reply(body, status=200):
    return SimpleNamespace(status_code=status, json=lambda: body)


class Client:
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs.get("params") or {}))
        for part, body in self.routes.items():
            if url.endswith(part):
                return reply(body) if body is not None else reply({}, 404)
        return reply({}, 404)

    def mount(self, *args):
        pass


def test_tmdb_shaped_layout_resolves_an_external_id_and_reads_only_wanted_seasons():
    calls = []
    def get(path, **params):
        calls.append(path)
        return {"find/tt1": {"tv_results": [{"id": 7}]},
                "tv/7": {"name": "Show", "seasons": [{"season_number": 0, "episode_count": 2}, {"season_number": 1, "episode_count": 2},
                                                      {"season_number": 2, "episode_count": 1}]},
                "tv/7/season/2": {"episodes": [{"episode_number": 1, "name": "Finale", "id": 55, "runtime": 44}]},
                "tv/7/season/1": {"episodes": [{"episode_number": 1, "name": "Pilot", "id": 51, "runtime": 22}]}}.get(path)
    layout = lookup._tmdb_shaped(get, {"imdb": "tt1"}, {2})
    assert layout["title"] == "Show" and layout["counts"] == {0: 2, 1: 2, 2: 1}
    assert layout["seasons"][2] == [dict(episode=1, title="Finale", ids={"tmdb": "55"}, runtime=44)]
    assert calls[:3] == ["find/tt1", "tv/7", "tv/7/season/2"] and "tv/7/season/0" not in calls


def test_simkl_layout_uses_the_show_lookup_and_keeps_titles_without_external_ids(monkeypatch):
    from services import interactive_sync_mapping

    monkeypatch.setattr(interactive_sync_mapping, "_simkl_headers", lambda block, instance: {})
    monkeypatch.setattr("cw_platform.simkl_http.pace_session", lambda client: None)
    client = Client({"search/id": [{"type": "tv", "title": "Monster", "ids": {"simkl": 1446614}}],
                     "tv/episodes/1446614": [{"type": "episode", "season": 4, "episode": 1, "title": "Bloodbath", "ids": {"simkl_id": 1}},
                                             {"type": "special", "season": 0, "episode": 1, "title": "Extra"},
                                             {"type": "episode", "season": 1, "episode": 1, "title": "Episode One"}]})
    cfg = {"simkl": {"client_id": "id", "access_token": "token"}}
    layout = lookup._simkl(client, cfg, "default", {"tmdb": "299939"}, set())
    assert layout["title"] == "Monster" and layout["counts"] == {4: 1, 1: 1}
    assert layout["seasons"][4] == [dict(episode=1, title="Bloodbath", ids={}, runtime=None)]
    assert client.calls[0][1]["tmdb"] == "299939"


def test_trakt_layout_reads_every_season_in_one_call(monkeypatch):
    from services import interactive_sync_mapping

    monkeypatch.setattr(interactive_sync_mapping, "_trakt_headers", lambda key, block, instance: {})
    monkeypatch.setattr("cw_platform.trakt_http.paced_request", lambda sender, method, url, **kwargs: sender(url, **kwargs))
    client = Client({"search/tmdb/299939": [{"type": "show", "show": {"title": "Story", "ids": {"trakt": 9}}}],
                     "shows/9/seasons": [{"number": 1, "episodes": [{"number": 1, "title": "Bloodbath",
                                                                    "ids": {"tmdb": 6506516, "tvdb": 11934436, "trakt": 1}}]}]})
    layout = lookup._trakt(client, {"trakt": {"client_id": "id"}}, "default", {"tmdb": "299939"}, set())
    assert layout["seasons"] == {1: [dict(episode=1, title="Bloodbath", ids={"tmdb": "6506516", "tvdb": "11934436"}, runtime=None)]}
    assert client.calls[-1][1] == {"extended": "episodes"} and len(client.calls) == 2


def test_wetrakr_layout_needs_the_season_id_and_has_no_titles(monkeypatch):
    import providers.auth._auth_WETRAKR as auth

    client = Client({"/media/external/tmdb/299939": {"id": 1519586, "type": "show"},
                     "/shows/1519586": {"type": "show", "title": "Story", "number_of_seasons": 1},
                     "/shows/1519586/seasons/1": {"id": 350742, "type": "season"},
                     "/seasons/350742/episodes": [{"number": 1, "ids": {"tmdb": 6506516, "imdb": "tt37538664"}}]})
    monkeypatch.setattr(auth, "request_with_auth", lambda session, method, url, **kwargs: session.get(url, params=kwargs.get("params")))
    layout = lookup._wetrakr(client, {}, "default", {"tmdb": "299939"}, {1})
    assert layout["title"] == "Story" and layout["counts"] == {1: 1}
    assert layout["seasons"][1] == [dict(episode=1, title="", ids={"tmdb": "6506516", "imdb": "tt37538664"}, runtime=None)]


def test_mdblist_layout_only_knows_season_sizes(monkeypatch):
    import providers.sync.mdblist._auth as auth

    client = Client({"/tmdb/show/299939": {"title": "Story", "seasons": [{"season_number": 1, "episode_count": 8}]}})
    monkeypatch.setattr(auth, "request_with_auth", lambda session, method, url, **kwargs: session.get(url))
    assert lookup._mdblist(client, {}, "default", {"tmdb": "299939"}, set()) == dict(title="Story", counts={1: 8}, seasons={})


@pytest.mark.parametrize("case", ["unconfigured", "unsupported", "failing", "empty"])
def test_show_layout_returns_nothing_when_a_provider_cannot_answer(monkeypatch, case):
    cfg = {"tmdb": {"api_key": "" if case == "unconfigured" else "key"}}
    def broken(*args):
        raise RuntimeError("offline")
    monkeypatch.setitem(lookup.FETCHERS, "TMDB", broken if case == "failing" else lambda *args: dict(title="", counts={}, seasons={}))
    provider = "PUNCHPLAY" if case == "unsupported" else "TMDB"
    assert lookup.show_layout(cfg, provider, "default", {"tmdb": "1"}) is None


def test_show_layout_labels_a_successful_lookup(monkeypatch):
    monkeypatch.setitem(lookup.FETCHERS, "TMDB", lambda *args: dict(title="Show", counts={1: 2}, seasons={}))
    layout = lookup.show_layout({"tmdb": {"api_key": "key"}}, "tmdb", "default", {"tmdb": 1}, [1])
    assert layout["provider"] == "TMDB" and layout["label"] == "TMDb" and layout["counts"] == {1: 2}


def test_tmdb_shaped_movie_lookup_resolves_an_imdb_id():
    routes = {"find/tt1": {"movie_results": [{"id": 5}]}, "movie/5": {"id": 5, "title": "Film", "release_date": "2026-08-21", "imdb_id": "tt1"}}
    assert lookup._tmdb_shaped_movie(lambda path, **params: routes.get(path), {"imdb": "tt1"}) == dict(
        title="Film", year=2026, ids={"tmdb": "5", "imdb": "tt1"})
    assert lookup._tmdb_shaped_movie(lambda path, **params: None, {"imdb": "tt404"}) is None


def test_simkl_movie_lookup_reads_the_ids_from_the_detail_call(monkeypatch):
    from services import interactive_sync_mapping

    monkeypatch.setattr(interactive_sync_mapping, "_simkl_headers", lambda block, instance: {})
    monkeypatch.setattr("cw_platform.simkl_http.pace_session", lambda client: None)
    client = Client({"search/id": [{"type": "movie", "ids": {"simkl": 2467847}}],
                     "movies/2467847": {"title": "Film", "year": 2026, "ids": {"simkl": 2467847, "imdb": "tt1", "tmdb": "5", "slug": "film"}}})
    found = lookup._simkl_movie(client, {"simkl": {"client_id": "id"}}, "default", {"imdb": "tt1"})
    assert found == dict(title="Film", year=2026, ids={"simkl": "2467847", "imdb": "tt1", "tmdb": "5"})


def test_movie_lookup_reports_unsupported_providers_and_failures(monkeypatch):
    assert lookup.movie_by_id({}, "PUNCHPLAY", "default", {"imdb": "tt1"}) == "unsupported"
    def broken(*args):
        raise RuntimeError("offline")
    monkeypatch.setitem(lookup.MOVIES, "TMDB", broken)
    assert lookup.movie_by_id({"tmdb": {"api_key": "key"}}, "TMDB", "default", {"imdb": "tt1"}) == "unsupported"
    monkeypatch.setitem(lookup.MOVIES, "TMDB", lambda *args: None)
    assert lookup.movie_by_id({"tmdb": {"api_key": "key"}}, "TMDB", "default", {"imdb": "tt1"}) is None
