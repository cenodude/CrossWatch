# tests/test_tmdb_shared_responses.py
# CrossWatch - Shared TMDb response freshness and strict lookup regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from concurrent.futures import Future, ThreadPoolExecutor
import json
import threading
from types import SimpleNamespace

import pytest
import requests

from providers.metadata import _meta_TMDB as tmdb


URL = "https://api.themoviedb.org/3/tv/7/season/1"


def response(body, status=200):
    result = SimpleNamespace(status_code=status, headers={}, json=lambda: body)

    def check():
        if status >= 400:
            raise requests.HTTPError(response=result)

    result.raise_for_status = check
    return result


@pytest.fixture
def providers(tmp_path, monkeypatch):
    def make(root=tmp_path, key="test"):
        provider = tmdb.TmdbProvider(lambda: {"tmdb": {"api_key": key}, "metadata": {"backoff_max_retries": 0}}, lambda _: None)
        root.mkdir(parents=True, exist_ok=True)
        provider._disk_dir = root
        return provider

    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: pytest.fail("Unexpected network request"))
    return make


def test_existing_disk_record_keeps_original_fetch_time(providers, monkeypatch, tmp_path):
    provider = providers()
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response({"episodes": []}))
    provider.get_response(URL)
    path = next(tmp_path.glob("*.json"))
    path.write_text(json.dumps({"ts": 123, "data": {"episodes": []}}), encoding="utf-8")
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: pytest.fail("Cache should not expire"))
    cached = providers().get_response(URL)
    assert (cached.fetched_at, cached.source, cached.data) == (123, "cache", {"episodes": []})
    assert json.loads(path.read_text("utf-8"))["ts"] == 123


def test_status_metadata_peek_never_fetches_and_rejects_stale_data(providers, monkeypatch):
    provider = providers()
    assert provider.peek_show("7") is None
    assert provider.peek_show("invalid") is None
    detail = {"id": 7, "name": "Example", "status": "Ended", "seasons": [{"season_number": 1, "episode_count": 2}]}
    provider.get_response("https://api.themoviedb.org/3/tv/7", {"language": "en-US"}, request=lambda *a, **k: response(detail))
    assert provider.peek_show("7").data == detail
    assert providers().peek_show("7").data == detail
    monkeypatch.setattr(tmdb.time, "time", lambda: 99999999999)
    assert provider.peek_show("7") is None


def test_refresh_updates_other_live_instances_without_touching_other_scopes(providers, monkeypatch, tmp_path):
    first, second = providers(), providers()
    other = providers(tmp_path / "other")
    bodies = iter([{"version": 1}, {"version": 10}, {"version": 2}])
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response(next(bodies)))
    first.get_response(URL)
    second.get_response(URL)
    other.get_response(URL)
    refreshed = first.get_response(URL, fresh=True)
    shared = second.get_response(URL)
    assert shared.data == {"version": 2}
    assert shared.fetched_at == refreshed.fetched_at
    assert shared.source == "cache"
    assert other.get_response(URL).data == {"version": 10}
    shared.data["version"] = 999
    assert first.get_response(URL).data == {"version": 2}
    assert second.get_response(URL).data == {"version": 2}


def test_language_and_credentials_remain_distinct(providers, monkeypatch):
    calls = []

    def get(url, params, **kwargs):
        calls.append(params)
        return response({"name": params["language"] + params["api_key"]})

    monkeypatch.setattr(tmdb.requests, "get", get)
    first, second = providers(), providers(key="other")
    for language in ("en-US", "nl-NL"):
        first.get_response(URL, {"language": language})
        second.get_response(URL, {"language": language})
    first.get_response(URL, {"language": "en-US"}, fresh=True)
    assert second.get_response(URL, {"language": "en-US"}).data == {"name": "en-USother"}
    assert first.get_response(URL, {"language": "nl-NL"}).data == {"name": "nl-NLtest"}
    assert len(calls) == 5


def test_max_age_refreshes_only_when_requested(providers, monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(tmdb.time, "time", lambda: clock[0])
    calls = []

    def get(*args, **kwargs):
        calls.append(1)
        return response({"version": len(calls)})

    monkeypatch.setattr(tmdb.requests, "get", get)
    provider = providers()
    assert provider.get_response(URL).fetched_at == 100
    clock[0] = 200
    assert provider.get_response(URL).data == {"version": 1}
    assert provider.get_response(URL, max_age=150).source == "cache"
    assert provider.get_response(URL, max_age=50).data == {"version": 2}


@pytest.mark.parametrize("fresh", [False, True])
def test_overlapping_requests_share_the_same_fetch(providers, monkeypatch, fresh):
    first, second = providers(), providers()
    entered, release, second_started = threading.Event(), threading.Event(), threading.Event()
    calls = []

    def get(*args, **kwargs):
        calls.append(1)
        entered.set()
        assert release.wait(5)
        return response({"episodes": []})

    class ObservedFuture(Future):
        def result(self, timeout=None):
            second_started.set()
            return super().result(timeout)

    monkeypatch.setattr(tmdb, "Future", ObservedFuture)
    monkeypatch.setattr(tmdb.requests, "get", get)
    with ThreadPoolExecutor(max_workers=2) as pool:
        one = pool.submit(first.get_response, URL, fresh=fresh)
        assert entered.wait(5)
        two = pool.submit(second.get_response, URL, fresh=fresh)
        assert second_started.wait(5)
        release.set()
        owner, joined = one.result(), two.result()
        assert (owner.source, joined.source) == ("network", "shared")
        assert joined.fetched_at == owner.fetched_at
    assert len(calls) == 1


@pytest.mark.parametrize("status", [404, 429, 503])
def test_failed_strict_refresh_preserves_last_success_without_returning_it(providers, monkeypatch, status):
    provider = providers()
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response({"id": 7, "name": "Show"}))
    before = provider.lookup("show", 7)
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response({}, status))
    with pytest.raises(requests.HTTPError) as error:
        provider.lookup("show", 7, fresh=True)
    assert error.value.response.status_code == status
    after = providers().lookup("show", 7)
    assert after.data == before.data and after.fetched_at == before.fetched_at


def test_strict_movie_lookup_never_tries_the_show_namespace(providers, monkeypatch):
    calls = []

    def get(url, **kwargs):
        calls.append(url)
        return response({}, 404)

    monkeypatch.setattr(tmdb.requests, "get", get)
    with pytest.raises(requests.HTTPError):
        providers().lookup("movie", 7)
    assert calls == ["https://api.themoviedb.org/3/movie/7"]


@pytest.mark.parametrize("body", [{}, {"id": 1, "season_number": 1},
                                  {"id": 1, "season_number": 2, "episodes": []},
                                  {"id": 1, "season_number": 1, "episodes": [{}]}])
def test_incomplete_or_wrong_season_is_not_a_success(providers, monkeypatch, body):
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response(body))
    with pytest.raises(ValueError):
        providers().lookup("show", 7, season=1)


def test_specials_and_empty_seasons_are_supported(providers, monkeypatch):
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response({"id": 10, "season_number": 0, "episodes": []}))
    assert providers().lookup("show", 7, season=0).data["episodes"] == []


@pytest.mark.parametrize(("entity", "coordinates", "body", "suffix"), [
    ("movie", {}, {"id": 7, "title": "Movie"}, "movie/7"),
    ("show", {}, {"id": 7, "name": "Show"}, "tv/7"),
    ("show", {"season": 1, "episode": 2}, {"id": 90, "season_number": 1, "episode_number": 2}, "tv/7/season/1/episode/2"),
])
def test_strict_lookup_uses_the_requested_resource(providers, monkeypatch, entity, coordinates, body, suffix):
    calls = []

    def get(url, **kwargs):
        calls.append(url)
        return response(body)

    monkeypatch.setattr(tmdb.requests, "get", get)
    result = providers().lookup(entity, 7, **coordinates)
    assert result.data == body and result.source == "network"
    assert calls == [f"https://api.themoviedb.org/3/{suffix}"]


@pytest.mark.parametrize("kwargs", [{"entity": "episode"}, {"tmdb_id": "../7"}, {"tmdb_id": True},
                                    {"entity": "movie", "season": 1}, {"season": -1},
                                    {"episode": 1}, {"season": 1, "episode": 0}])
def test_invalid_lookup_coordinates_do_not_make_requests(providers, kwargs):
    args = {"entity": "show", "tmdb_id": 7, **kwargs}
    with pytest.raises(ValueError):
        providers().lookup(**args)


@pytest.mark.parametrize("consumer", ["alignment", "catalogue", "manual"])
def test_live_consumers_populate_shared_cache_and_remain_live(providers, monkeypatch, tmp_path, consumer):
    from api import manualAPI
    from services import catalog_lookup, episode_alignment

    calls = []
    body = {"id": 7, "title": "Movie", "release_date": "2020-01-01", "episodes": []}

    def root(provider):
        provider._disk_dir = tmp_path
        return tmp_path

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return response(body)

    monkeypatch.setattr(tmdb.TmdbProvider, "_disk_cache_dir", root)
    monkeypatch.setattr(tmdb.requests, "get", get)
    cfg = {"tmdb": {"api_key": "test"}}
    actions = {
        "alignment": lambda: episode_alignment.tmdb_season("test", 7, 1),
        "catalogue": lambda: catalog_lookup._tmdb_movie(SimpleNamespace(get=get), cfg, "default", {"tmdb": "7"}),
        "manual": lambda: manualAPI._tmdb_release_date(cfg, "movie", 7),
    }
    actions[consumer]()
    assert len(calls) == 1
    assert providers().get_response(calls[0][0]).data == body
    assert len(calls) == 1
    actions[consumer]()
    assert len(calls) == 2


def test_shared_failed_request_is_not_cached_and_next_lookup_recovers(providers, monkeypatch):
    provider = providers()
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response({}, 503))
    with pytest.raises(requests.HTTPError):
        provider.get_response(URL)
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response({"episodes": []}))
    assert provider.get_response(URL).source == "network"
    assert not tmdb._INFLIGHT


def test_cached_reads_stay_available_during_a_failed_background_refresh(providers, monkeypatch):
    first, second = providers(), providers()
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response({"episodes": []}))
    original = first.get_response(URL)
    entered, release = threading.Event(), threading.Event()

    def down(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        raise requests.ConnectionError("offline")

    monkeypatch.setattr(tmdb.requests, "get", down)
    with ThreadPoolExecutor(max_workers=1) as pool:
        refresh = pool.submit(first.get_response, URL, fresh=True)
        assert entered.wait(5)
        try:
            cached = second.get_response(URL)
            assert cached.data == original.data and cached.fetched_at == original.fetched_at
        finally:
            release.set()
        with pytest.raises(requests.ConnectionError):
            refresh.result()


@pytest.mark.parametrize("policy", [{"timeout": 10}, {"max_retries": 2}, {"quiet_404": True}])
def test_callers_with_another_request_policy_never_inherit_a_failure(providers, monkeypatch, policy):
    calls, entered, release = [], threading.Event(), threading.Event()

    def get(*args, timeout, **kwargs):
        calls.append(timeout)
        if len(calls) == 1:
            entered.set()
            assert release.wait(5)
            raise requests.Timeout("slow")
        return response({"episodes": []})

    monkeypatch.setattr(tmdb.requests, "get", get)
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(providers().get_response, URL, timeout=1, max_retries=0)
        assert entered.wait(5)
        try:
            result = providers().get_response(URL, **{"timeout": 1, "max_retries": 0, **policy})
            assert (result.source, result.data) == ("network", {"episodes": []})
        finally:
            release.set()
        with pytest.raises(requests.Timeout):
            first.result()
    assert calls == [1, policy.get("timeout", 1)]


def test_loaded_disk_record_stays_in_memory_when_the_cache_is_full(providers, monkeypatch, tmp_path):
    monkeypatch.setattr(tmdb.TmdbProvider, "CACHE_MAX_ENTRIES", 2)
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response({"episodes": []}))
    provider = providers()
    provider.get_response(URL)
    path = next(tmp_path.glob("*.json"))
    for season in (2, 3):
        provider.get_response(URL[:-1] + str(season))
    path.write_text(json.dumps({"ts": 1, "data": {"old": True}}), encoding="utf-8")
    assert provider.get_response(URL).fetched_at == 1
    monkeypatch.setattr(tmdb.TmdbProvider, "_disk_read", lambda *a: pytest.fail("Record should stay in memory"))
    assert provider.get_response(URL).data == {"old": True}


def test_a_failed_disk_write_never_reverts_a_newer_refresh(providers, monkeypatch):
    first, second = providers(), providers()
    bodies = iter([{"version": 1}, {"version": 2}])
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response(next(bodies)))
    first.get_response(URL)
    with monkeypatch.context() as patch:
        patch.setattr(tmdb.TmdbProvider, "_disk_write", lambda *a: None)
        first.get_response(URL, fresh=True)
    assert second.get_response(URL).data == {"version": 1}
    assert first.get_response(URL).data == {"version": 2}


def test_strict_lookup_can_reuse_a_season_fetched_without_a_language(providers, monkeypatch, tmp_path):
    from services import episode_alignment

    body = {"id": 70, "season_number": 1, "episodes": [{"episode_number": 1, "season_number": 1}]}
    monkeypatch.setattr(tmdb.TmdbProvider, "_disk_cache_dir", lambda provider: tmp_path)
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response(body))
    episode_alignment.tmdb_season("test", 7, 1)
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: pytest.fail("Unexpected network request"))
    result = providers().lookup("show", 7, season=1, locale=None)
    assert (result.source, result.data) == ("cache", body)
