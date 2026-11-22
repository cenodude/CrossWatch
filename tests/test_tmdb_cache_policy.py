# tests/test_tmdb_cache_policy.py
# CrossWatch - TMDb cache freshness rules and stale fallback regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import requests

from providers.metadata import _meta_TMDB as tmdb

API = "https://api.themoviedb.org/3"
NOW = datetime(2026, 11, 22, 12, tzinfo=timezone.utc).timestamp()
DAY = tmdb.DAY
SETTLED = tmdb.SETTLED_MAX_AGE


def response(body, status=200):
    result = SimpleNamespace(status_code=status, headers={}, json=lambda: body)

    def check():
        if status >= 400:
            raise requests.HTTPError(response=result)

    result.raise_for_status = check
    return result


@pytest.fixture
def provider(tmp_path, monkeypatch):
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: pytest.fail("Unexpected network request"))
    item = tmdb.TmdbProvider(lambda: {"tmdb": {"api_key": "test"}, "metadata": {"backoff_max_retries": 0}}, lambda _: None)
    item._disk_dir = tmp_path
    return item


@pytest.mark.parametrize(("path", "data", "expected"), [
    ("search/tv", {"results": []}, DAY),
    ("find/tt1", {"tv_results": [{"id": 1}]}, float("inf")),
    ("find/tt1", {"tv_results": [], "movie_results": []}, DAY),
    ("movie/1", {"release_date": "2010-05-01"}, SETTLED),
    ("movie/1", {"release_date": "2026-11-10"}, DAY),
    ("movie/1", {"release_date": ""}, DAY),
    ("tv/1", {"status": "Ended"}, SETTLED),
    ("tv/1", {"status": "Canceled"}, SETTLED),
    ("tv/1", {"status": "Returning Series", "next_episode_to_air": None}, DAY),
    ("tv/1", {"status": "Returning Series", "next_episode_to_air": {"air_date": "2026-11-20"}}, DAY),
    ("tv/1", {"status": "Returning Series", "next_episode_to_air": {"air_date": "2026-11-25"}}, 3.5 * DAY),
    ("tv/1", {"status": "Returning Series", "next_episode_to_air": {"air_date": "2027-06-01"}}, SETTLED),
    ("tv/1/season/1", {"episodes": [{"air_date": "2020-01-01"}, {"air_date": "2020-01-08"}]}, SETTLED),
    ("tv/1/season/1", {"episodes": [{"air_date": "2026-11-15"}, {"air_date": "2026-11-29"}]}, 7.5 * DAY),
    ("tv/1/season/1", {"episodes": [{"air_date": "2026-11-01"}, {"air_date": "2026-11-15"}]}, DAY),
    ("tv/1/season/1", {"episodes": [{"air_date": "2020-01-01"}, {"air_date": None}]}, DAY),
    ("tv/1/season/1", {"episodes": []}, DAY),
    ("tv/1/season/1/episode/2", {"air_date": "2020-01-01"}, SETTLED),
    ("tv/1/season/1/episode/2/images", {"stills": []}, SETTLED),
    ("tv/1/external_ids", {"imdb_id": "tt1", "tvdb_id": 2}, SETTLED),
    ("tv/1/external_ids", {"imdb_id": None, "tvdb_id": 2}, DAY),
    ("movie/1/images", {"posters": []}, SETTLED),
    ("collection/1", {"parts": []}, SETTLED),
    ("watch/providers/movie", {"results": []}, SETTLED),
    ("tv/abc", {"status": "Ended"}, DAY),
    ("trending/all/day", {"results": []}, DAY),
])
def test_cache_age_follows_the_kind_of_data(path, data, expected):
    assert tmdb.policy_max_age(f"{API}/{path}", data, NOW) == pytest.approx(expected)


def test_other_hosts_never_get_a_long_cache_age():
    assert tmdb.policy_max_age("https://example.test/3/tv/1", {"status": "Ended"}, NOW) == DAY


def test_airing_show_is_refetched_once_the_next_episode_has_aired(provider, monkeypatch):
    clock = [NOW]
    monkeypatch.setattr(tmdb.time, "time", lambda: clock[0])
    bodies = iter([
        {"id": 1, "status": "Returning Series", "next_episode_to_air": {"air_date": "2026-11-25"}},
        {"id": 1, "status": "Returning Series", "next_episode_to_air": {"air_date": "2026-12-02"}},
    ])
    calls = []

    def get(*args, **kwargs):
        calls.append(1)
        return response(next(bodies))

    monkeypatch.setattr(tmdb.requests, "get", get)
    url = f"{API}/tv/1"
    provider.get_response(url)
    clock[0] = NOW + 3 * DAY
    assert provider.get_response(url).source == "cache"
    clock[0] = NOW + 4 * DAY
    refreshed = provider.get_response(url)
    assert refreshed.source == "network"
    assert refreshed.data["next_episode_to_air"]["air_date"] == "2026-12-02"
    assert len(calls) == 2


@pytest.mark.parametrize("failure", [requests.ConnectionError("offline"), response({}, 503), response({}, 429)])
def test_expired_cache_is_used_when_tmdb_fails(provider, monkeypatch, failure):
    clock = [NOW]
    monkeypatch.setattr(tmdb.time, "time", lambda: clock[0])
    url = f"{API}/search/tv"
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response({"results": [1]}))
    provider.get_response(url, {"query": "x"})
    clock[0] = NOW + 2 * DAY

    def down(*args, **kwargs):
        if isinstance(failure, Exception):
            raise failure
        return failure

    monkeypatch.setattr(tmdb.requests, "get", down)
    result = provider.get_response(url, {"query": "x"})
    assert (result.source, result.data, result.fetched_at) == ("cache", {"results": [1]}, NOW)


def test_removed_title_is_not_hidden_by_an_expired_cache(provider, monkeypatch):
    clock = [NOW]
    monkeypatch.setattr(tmdb.time, "time", lambda: clock[0])
    url = f"{API}/search/tv"
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response({"results": [1]}))
    provider.get_response(url)
    clock[0] = NOW + 2 * DAY
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response({}, 404))
    with pytest.raises(requests.HTTPError):
        provider.get_response(url)


def test_strict_and_bounded_reads_never_fall_back_to_expired_data(provider, monkeypatch):
    clock = [NOW]
    monkeypatch.setattr(tmdb.time, "time", lambda: clock[0])
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response({"id": 7, "name": "Show", "status": "Returning Series"}))
    provider.lookup("show", 7)
    clock[0] = NOW + 2 * DAY
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: (_ for _ in ()).throw(requests.ConnectionError("offline")))
    with pytest.raises(requests.ConnectionError):
        provider.lookup("show", 7)
    with pytest.raises(requests.ConnectionError):
        provider.get_response(f"{API}/tv/7", {"language": "en-US"}, max_age=60)
    with pytest.raises(requests.ConnectionError):
        provider.get_response(f"{API}/tv/7", {"language": "en-US"}, fresh=True)


def test_latest_read_falls_back_to_cache_without_a_second_request(provider, monkeypatch):
    clock = [NOW]
    monkeypatch.setattr(tmdb.time, "time", lambda: clock[0])
    url = f"{API}/tv/7/season/1"
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response({"episodes": []}))
    provider.get_response(url)
    clock[0] = NOW + 2 * DAY
    calls = []

    def down(*args, **kwargs):
        calls.append(1)
        raise requests.ConnectionError("offline")

    monkeypatch.setattr(tmdb.requests, "get", down)
    assert provider.get_latest(url) == {"episodes": []}
    assert len(calls) == 1


def test_records_without_a_fetch_time_are_refreshed(provider, monkeypatch, tmp_path):
    import json

    url = f"{API}/movie/1"
    monkeypatch.setattr(tmdb.requests, "get", lambda *a, **k: response({"id": 1, "release_date": "2000-01-01"}))
    provider.get_response(url)
    path = next(tmp_path.glob("*.json"))
    path.write_text(json.dumps({"ts": 0, "data": {"id": 1, "old": True}}), encoding="utf-8")
    provider.clear_cache()
    assert provider.get_response(url).data == {"id": 1, "release_date": "2000-01-01"}
