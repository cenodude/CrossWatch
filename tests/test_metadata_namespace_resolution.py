from __future__ import annotations

import json

import pytest
import requests

from api import metaAPI
from cw_platform.metadata_cache import (
    prune_metadata_cache,
    read_resolution_cache,
    read_resolution_index,
    resolution_cache_key,
    write_metadata_cache,
    write_resolution_index,
)
from providers.metadata._meta_TMDB import TmdbProvider

TOTORO = {"id": 8392, "title": "My Neighbor Totoro", "original_title": "となりのトトロ", "release_date": "1988-04-16"}
POPEYE = {"id": 8392, "name": "Popeye", "original_name": "Popeye", "first_air_date": "1960-09-10"}


def _http_error(status: int) -> requests.exceptions.RequestException:
    response = requests.Response()
    response.status_code = status
    return requests.exceptions.HTTPError(response=response)


class FakeTmdb(TmdbProvider):
    def __init__(self, routes: dict[str, object]) -> None:
        super().__init__(lambda: {}, lambda cfg: None)
        self.routes = routes
        self.calls: list[str] = []

    def _get(self, url: str, params: dict | None = None) -> object:
        path = url.replace("https://api.themoviedb.org/3", "")
        self.calls.append(path)
        if path not in self.routes:
            raise _http_error(404)
        value = self.routes[path]
        if isinstance(value, Exception):
            raise value
        return value


def _provider(routes: dict[str, object]) -> FakeTmdb:
    return FakeTmdb(routes)


def _install(monkeypatch, tmp_path, provider, resolve_result=None):
    class Manager:
        providers = {"TMDB": provider}

        def resolve(self, *, entity, ids, locale=None, need=None, strategy=None):
            return dict(resolve_result or {"type": entity, "ids": dict(ids)})

    monkeypatch.setattr(metaAPI, "_env", lambda: (Manager(), tmp_path, lambda: {}))
    monkeypatch.setattr(metaAPI, "_meta_cache_dir", lambda: tmp_path)
    monkeypatch.setattr(metaAPI, "_meta_cache_enabled", lambda: True)
    monkeypatch.setattr(metaAPI, "_cfg_meta_ttl_secs", lambda: 720 * 3600)
    monkeypatch.setattr(metaAPI, "_cfg_ui_locale", lambda: "en-US")
    metaAPI._resolve_tmdb_cached.cache_clear()


def test_wrong_tv_namespace_resolves_to_movie() -> None:
    provider = _provider({"/movie/8392": TOTORO, "/tv/8392": POPEYE})
    outcome = provider.resolve_namespace(
        tmdb_id="8392", requested_type="show", title="My Neighbor Totoro", year=1988
    )
    assert outcome == {
        "resolved_type": "movie",
        "status": "resolved",
        "reason": "alternate_title_year_match",
    }


def test_title_with_year_suffix_still_matches() -> None:
    paprika = {"id": 1, "title": "Paprika", "original_title": "パプリカ", "release_date": "2006-11-25"}
    makin_it = {"id": 1, "name": "Makin' It", "first_air_date": "1979-02-01"}
    provider = _provider({"/movie/1": paprika, "/tv/1": makin_it})
    outcome = provider.resolve_namespace(
        tmdb_id="1", requested_type="show", title="Paprika 2006", year=2006
    )
    assert outcome["resolved_type"] == "movie"
    assert outcome["reason"] == "alternate_title_year_match"


def test_year_suffix_stripping_does_not_break_numeric_titles() -> None:
    provider = TmdbProvider(lambda: {}, lambda cfg: None)
    assert provider._expected_titles("1917", 1917) == {"1917"}
    assert provider._expected_titles("Blade Runner 2049", 2017) == {"blade runner 2049"}
    assert provider._expected_titles("Paprika 2006", 2006) == {"paprika 2006", "paprika"}


def test_genuine_show_at_same_id_stays_tv() -> None:
    provider = _provider({"/movie/8392": TOTORO, "/tv/8392": POPEYE})
    outcome = provider.resolve_namespace(
        tmdb_id="8392", requested_type="show", title="Popeye", year=1960
    )
    assert outcome["resolved_type"] == "tv"
    assert outcome["reason"] == "requested_title_year_match"


def test_matching_requested_namespace_skips_alternate_lookup() -> None:
    provider = _provider({"/movie/550": {"id": 550, "title": "Fight Club", "release_date": "1999-10-15"}})
    outcome = provider.resolve_namespace(
        tmdb_id="550", requested_type="movie", title="Fight Club", year=1999
    )
    assert outcome["resolved_type"] == "movie"
    assert provider.calls == ["/movie/550"]


def test_external_id_used_only_when_both_details_inconclusive() -> None:
    routes = {
        "/movie/8392": {"id": 8392, "title": "Unrelated", "release_date": "2001-01-01"},
        "/tv/8392": {"id": 8392, "name": "Also Unrelated", "first_air_date": "2002-01-01"},
        "/find/tt0096283": {"movie_results": [{"id": 8392}], "tv_results": []},
    }
    provider = _provider(routes)
    outcome = provider.resolve_namespace(
        tmdb_id="8392",
        requested_type="movie",
        title="My Neighbor Totoro",
        year=1988,
        imdb_id="tt0096283",
    )
    assert outcome["resolved_type"] == "movie"
    assert outcome["reason"] == "external_id_imdb_id"
    assert provider.calls.index("/movie/8392") < provider.calls.index("/find/tt0096283")


def test_external_id_skipped_when_direct_match_succeeds() -> None:
    provider = _provider({"/movie/8392": TOTORO, "/tv/8392": POPEYE, "/find/tt0096283": {}})
    provider.resolve_namespace(
        tmdb_id="8392",
        requested_type="show",
        title="My Neighbor Totoro",
        year=1988,
        imdb_id="tt0096283",
    )
    assert "/find/tt0096283" not in provider.calls


def test_search_is_last_and_only_confirms_matching_id() -> None:
    routes = {
        "/movie/8392": {"id": 8392, "title": "Unrelated", "release_date": "2001-01-01"},
        "/tv/8392": {"id": 8392, "name": "Also Unrelated", "first_air_date": "2002-01-01"},
        "/search/movie": {"results": [{"id": 8392, "title": "My Neighbor Totoro", "release_date": "1988-04-16"}]},
    }
    provider = _provider(routes)
    outcome = provider.resolve_namespace(
        tmdb_id="8392", requested_type="movie", title="My Neighbor Totoro", year=1988
    )
    assert outcome["reason"] == "search_title_year_match"
    assert provider.calls[-1] == "/search/movie"


def test_search_hit_at_different_id_does_not_confirm() -> None:
    routes = {
        "/movie/8392": {"id": 8392, "title": "Unrelated", "release_date": "2001-01-01"},
        "/tv/8392": {"id": 8392, "name": "Also Unrelated", "first_air_date": "2002-01-01"},
        "/search/movie": {"results": [{"id": 99999, "title": "My Neighbor Totoro", "release_date": "1988-04-16"}]},
        "/search/tv": {"results": []},
    }
    provider = _provider(routes)
    outcome = provider.resolve_namespace(
        tmdb_id="8392", requested_type="movie", title="My Neighbor Totoro", year=1988
    )
    assert outcome["status"] == "unresolved"


def test_transient_failure_returns_none_and_404_returns_unresolved() -> None:
    flaky = _provider({"/movie/8392": _http_error(500), "/tv/8392": POPEYE})
    assert flaky.resolve_namespace(
        tmdb_id="8392", requested_type="movie", title="My Neighbor Totoro", year=1988
    ) is None

    missing = _provider({})
    outcome = missing.resolve_namespace(
        tmdb_id="8392", requested_type="movie", title="Nothing Here", year=1999
    )
    assert outcome["status"] == "unresolved"


def test_resolution_is_cached_and_second_call_skips_detail_requests(tmp_path, monkeypatch) -> None:
    provider = _provider({"/movie/8392": TOTORO, "/tv/8392": POPEYE})
    _install(monkeypatch, tmp_path, provider)

    first = metaAPI.get_meta(
        "k", "show", "8392", tmp_path, need={"title": True}, title="My Neighbor Totoro", year=1988
    )
    assert first["resolved_type"] == "movie"
    detail_calls = [c for c in provider.calls if c in {"/movie/8392", "/tv/8392"}]
    assert len(detail_calls) == 2

    key = resolution_cache_key("8392", title="My Neighbor Totoro", year=1988)
    record = read_resolution_cache(tmp_path, key)
    assert record["status"] == "resolved"
    assert record["resolved_type"] == "movie"

    provider.calls.clear()
    metaAPI._resolve_tmdb_cached.cache_clear()
    second = metaAPI.get_meta(
        "k", "show", "8392", tmp_path, need={"title": True}, title="My Neighbor Totoro", year=1988
    )
    assert second["resolved_type"] == "movie"
    assert [c for c in provider.calls if c in {"/movie/8392", "/tv/8392"}] == []


def test_movie_and_show_metadata_files_stay_separate(tmp_path) -> None:
    write_metadata_cache(tmp_path, "movie", "8392", "en-US", {"title": "My Neighbor Totoro"})
    write_metadata_cache(tmp_path, "show", "8392", "en-US", {"title": "Popeye"})

    movie = json.loads((tmp_path / "movie" / "8392.en-US.json").read_text("utf-8"))
    show = json.loads((tmp_path / "show" / "8392.en-US.json").read_text("utf-8"))
    assert movie["title"] == "My Neighbor Totoro"
    assert show["title"] == "Popeye"


def test_legacy_caller_without_evidence_still_works(tmp_path, monkeypatch) -> None:
    provider = _provider({})
    _install(monkeypatch, tmp_path, provider, resolve_result={"type": "movie", "title": "Fight Club"})

    meta = metaAPI.get_meta("k", "movie", "550", tmp_path, need={"title": True})
    assert meta["title"] == "Fight Club"
    assert meta["resolved_type"] == "movie"
    assert provider.calls == []


def test_index_lets_evidence_free_callers_reuse_resolution(tmp_path) -> None:
    assert write_resolution_index(tmp_path, "show", "8392", "movie") is True
    assert read_resolution_index(tmp_path, "show", "8392") == "movie"
    assert read_resolution_index(tmp_path, "movie", "8392") is None


def test_index_is_memoized_and_reparsed_only_after_writes(tmp_path, monkeypatch) -> None:
    from cw_platform import metadata_cache

    write_resolution_index(tmp_path, "show", "8392", "movie")

    reads: list[str] = []
    original = metadata_cache.Path.read_text

    def counting_read_text(self, *args, **kwargs):
        if self.name == metadata_cache.RESOLUTION_INDEX_NAME:
            reads.append(self.name)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(metadata_cache.Path, "read_text", counting_read_text)

    for _ in range(20):
        assert read_resolution_index(tmp_path, "show", "8392") == "movie"
    assert len(reads) == 1

    write_resolution_index(tmp_path, "show", "999", "movie")
    after_write = len(reads)
    assert read_resolution_index(tmp_path, "show", "8392") == "movie"
    assert len(reads) == after_write + 1

    for _ in range(10):
        read_resolution_index(tmp_path, "show", "999")
    assert len(reads) == after_write + 1


def test_stale_resolver_version_is_ignored(tmp_path) -> None:
    from cw_platform import metadata_cache

    key = resolution_cache_key("8392", title="Paprika 2006", year=2006)
    path = metadata_cache.resolution_cache_path(tmp_path, key)
    path.write_text(
        json.dumps({
            "status": "unresolved",
            "resolved_type": "show",
            "resolved_at": __import__("time").time(),
            "resolver_version": metadata_cache.RESOLVER_VERSION - 1,
        }),
        encoding="utf-8",
    )
    assert read_resolution_cache(tmp_path, key) is None

    write_resolution_index(tmp_path, "show", "8392", "movie")
    index_path = tmp_path / "resolution" / "_index.json"
    stale = json.loads(index_path.read_text("utf-8"))
    stale["_version"] = metadata_cache.RESOLVER_VERSION - 1
    index_path.write_text(json.dumps(stale), encoding="utf-8")
    metadata_cache._INDEX_MEMO.clear()
    assert read_resolution_index(tmp_path, "show", "8392") is None


def test_index_conflict_becomes_ambiguous_and_falls_back(tmp_path) -> None:
    write_resolution_index(tmp_path, "show", "8392", "movie")
    write_resolution_index(tmp_path, "show", "8392", "show")
    assert read_resolution_index(tmp_path, "show", "8392") is None


def test_artwork_uses_resolved_namespace_and_distinct_cache_file(tmp_path, monkeypatch) -> None:
    seen: list[str] = []
    downloaded: list[str] = []

    def fake_get_meta(api_key, typ, tmdb_id, cache_dir, *, need=None, locale=None, **kwargs):
        seen.append(typ)
        return {"images": {"poster": [{"url": "https://image.tmdb.org/t/p/w780/totoro.jpg", "lang": "en"}]}}

    def fake_download(url, dest_path, timeout=15.0):
        downloaded.append(dest_path.name)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_bytes(b"x")
        return dest_path, "image/jpeg"

    monkeypatch.setattr(metaAPI, "_meta_cache_dir", lambda: tmp_path)
    monkeypatch.setattr(metaAPI, "_cfg_ui_locale", lambda: "en-US")
    monkeypatch.setattr(metaAPI, "get_meta", fake_get_meta)
    monkeypatch.setattr(metaAPI, "_cache_download", fake_download)
    write_resolution_index(tmp_path, "show", "8392", "movie")

    metaAPI.get_art_file("k", "tv", "8392", "w342", tmp_path / "cache", locale="en-US")

    assert seen == ["movie"]
    assert downloaded and downloaded[0].startswith("movie_8392_poster_")


def test_artwork_resolves_on_first_request_with_no_prior_index(tmp_path, monkeypatch) -> None:
    seen: list[str] = []
    downloaded: list[str] = []
    provider = _provider({"/movie/8392": TOTORO, "/tv/8392": POPEYE})
    _install(monkeypatch, tmp_path, provider)

    def fake_get_meta(api_key, typ, tmdb_id, cache_dir, *, need=None, locale=None, **kwargs):
        seen.append(typ)
        return {"images": {"poster": [{"url": "https://image.tmdb.org/t/p/w780/totoro.jpg", "lang": "en"}]}}

    def fake_download(url, dest_path, timeout=15.0):
        downloaded.append(dest_path.name)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_bytes(b"x")
        return dest_path, "image/jpeg"

    monkeypatch.setattr(metaAPI, "get_meta", fake_get_meta)
    monkeypatch.setattr(metaAPI, "_cache_download", fake_download)

    assert read_resolution_index(tmp_path, "show", "8392") is None

    metaAPI.get_art_file(
        "k", "tv", "8392", "w342", tmp_path / "cache",
        locale="en-US", title="My Neighbor Totoro", year=1988,
    )

    assert seen == ["movie"]
    assert downloaded and downloaded[0].startswith("movie_8392_poster_")
    assert read_resolution_index(tmp_path, "show", "8392") == "movie"


def test_artwork_without_resolution_keeps_requested_namespace(tmp_path, monkeypatch) -> None:
    seen: list[str] = []

    def fake_get_meta(api_key, typ, tmdb_id, cache_dir, *, need=None, locale=None, **kwargs):
        seen.append(typ)
        return {"images": {"poster": [{"url": "https://image.tmdb.org/t/p/w780/popeye.jpg", "lang": "en"}]}}

    monkeypatch.setattr(metaAPI, "_meta_cache_dir", lambda: tmp_path)
    monkeypatch.setattr(metaAPI, "_cfg_ui_locale", lambda: "en-US")
    monkeypatch.setattr(metaAPI, "get_meta", fake_get_meta)
    monkeypatch.setattr(metaAPI, "_cache_download", lambda url, dest, timeout=15.0: (dest, "image/jpeg"))

    metaAPI.get_art_file("k", "tv", "8392", "w342", tmp_path / "cache", locale="en-US")
    assert seen == ["tv"]


def test_prune_preserves_resolution_records(tmp_path) -> None:
    for i in range(40):
        write_metadata_cache(tmp_path, "movie", str(1000 + i), "en-US", {"blob": "x" * 40000})
    write_resolution_index(tmp_path, "show", "8392", "movie")

    removed = prune_metadata_cache(tmp_path, max_mb=1)
    assert removed > 0
    assert read_resolution_index(tmp_path, "show", "8392") == "movie"
