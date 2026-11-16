# tests/test_fanart_metadata.py
# CrossWatch - Fanart.tv artwork provider, art route and key settings
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from api import metaAPI
from cw_platform.config_base import redact_config
from cw_platform.metadata import MetadataManager
from providers.metadata import _meta_FANART
from providers.metadata._meta_FANART import PROJECT_KEY, FanartProvider

ON = {"enabled": True}
FAKE_REQUEST: Any = SimpleNamespace(headers={}, client=None, url=SimpleNamespace(path="/art/tmdb"))
ASSET = "https://assets.fanart.tv/fanart/movies/603"

MOVIE = {
    "name": "The Matrix",
    "hdmovielogo": [
        {"id": "1", "url": f"{ASSET}/hdmovielogo/a.png", "lang": "de", "likes": "9"},
        {"id": "2", "url": f"{ASSET}/hdmovielogo/b.png", "lang": "en", "likes": "2"},
    ],
    "movielogo": [{"id": "3", "url": f"{ASSET}/movielogo/c.png", "lang": "en", "likes": "50"}],
    "moviedisc": [
        {"id": "4", "url": f"{ASSET}/moviedisc/dvd.png", "lang": "en", "likes": "20", "disc": "1", "disc_type": "dvd"},
        {"id": "5", "url": f"{ASSET}/moviedisc/bd2.png", "lang": "en", "likes": "30", "disc": "2", "disc_type": "bluray"},
        {"id": "6", "url": f"{ASSET}/moviedisc/bd1.png", "lang": "en", "likes": "1", "disc": "1", "disc_type": "bluray"},
        {"id": "7", "url": "https://evil.example/moviedisc/x.png", "lang": "en", "likes": "99", "disc": "1", "disc_type": "bluray"},
    ],
}


class FakeResponse:
    def __init__(self, status: int, data: Any = None) -> None:
        self.status_code = status
        self._data = data

    def json(self) -> Any:
        return self._data


def url_of(row: dict[str, Any] | None) -> str:
    assert row is not None
    return str(row["url"])


def path_of(res: Any) -> str:
    return str(getattr(res, "path", ""))


def make_provider(tmp_path: Path, cfg: dict[str, Any] | None = None) -> FanartProvider:
    prov = FanartProvider(lambda: cfg if cfg is not None else {"fanart": {**ON, "api_key": "k"}}, lambda _cfg: None)
    prov._disk_dir = tmp_path
    return prov


def test_pick_prefers_requested_disc_type_first_disc_and_hd_logo(tmp_path, monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(_meta_FANART.requests, "get", lambda url, **kw: (calls.append(url), FakeResponse(200, MOVIE))[1])
    prov = make_provider(tmp_path)

    disc = prov.pick("movie", {"tmdb": 603}, "disc", disc_type="bluray")
    assert disc and disc["url"].endswith("/bd1.png")
    assert url_of(prov.pick("movie", {"tmdb": 603}, "disc", disc_type="dvd")).endswith("/dvd.png")
    assert url_of(prov.pick("movie", {"tmdb": 603}, "logo", lang="de")).endswith("/hdmovielogo/a.png")
    assert url_of(prov.pick("movie", {"tmdb": 603}, "logo", lang="nl")).endswith("/hdmovielogo/b.png")
    assert all("evil.example" not in row["url"] for row in prov.images("movie", {"tmdb": 603})["disc"])
    assert calls == ["https://webservice.fanart.tv/v3/movies/603"]


def test_raw_uses_disk_cache_and_remembers_missing(tmp_path, monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(_meta_FANART.requests, "get", lambda url, **kw: (calls.append(url), FakeResponse(404, {"status": "error"}))[1])
    prov = make_provider(tmp_path)
    assert prov.images("movie", {"tmdb": 1}) == {}
    assert make_provider(tmp_path).images("movie", {"tmdb": 1}) == {}
    assert len(calls) == 1


def test_lookup_ids_and_disabled(tmp_path, monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(_meta_FANART.requests, "get", lambda url, **kw: (calls.append(url), FakeResponse(200, {}))[1])
    prov = make_provider(tmp_path)
    prov.raw("movie", {"imdb": "tt0133093"})
    prov.raw("show", {"tmdb": 1399})
    prov.raw("show", {"tvdb": 121361})
    assert calls == ["https://webservice.fanart.tv/v3/movies/tt0133093", "https://webservice.fanart.tv/v3/tv/121361"]
    assert make_provider(tmp_path, {"fanart": {"api_key": "k"}}).raw("movie", {"tmdb": 603}) == {}
    assert len(calls) == 2


def test_builtin_project_key_and_overrides(monkeypatch) -> None:
    monkeypatch.delenv("CW_FANART_PROJECT_KEY", raising=False)
    assert _meta_FANART.project_key({}) == PROJECT_KEY
    assert _meta_FANART.project_key({"fanart": {"api_key": "own"}}) == "own"
    monkeypatch.setenv("CW_FANART_PROJECT_KEY", "fork")
    assert _meta_FANART.project_key({}) == "fork"
    assert _meta_FANART.project_key({"fanart": {"api_key": "own"}}) == "own"
    assert not _meta_FANART.enabled({"fanart": {"api_key": "own"}})
    assert _meta_FANART.enabled({"fanart": ON})


def test_check_key_maps_errors(monkeypatch) -> None:
    monkeypatch.delenv("CW_FANART_PROJECT_KEY", raising=False)
    prov = FanartProvider(lambda: {}, lambda _cfg: None)
    sent: list[dict[str, Any]] = []
    monkeypatch.setattr(_meta_FANART.requests, "get", lambda url, **kw: (sent.append(kw["params"]), FakeResponse(401, {"error": "invalid API key"}))[1])
    assert prov.check_key("bad", "mine") == (False, "Invalid fanart.tv project API key")
    assert sent == [{"api_key": "bad"}]

    def by_key(url, **kw):
        sent.append(kw["params"])
        return FakeResponse(401, {"error": "invalid API key"}) if kw["params"].get("client_key") == "wrong" else FakeResponse(200, MOVIE)

    sent.clear()
    monkeypatch.setattr(_meta_FANART.requests, "get", by_key)
    assert prov.check_key("good", "wrong") == (False, "Invalid fanart.tv personal API key")
    assert sent == [{"api_key": "good"}, {"api_key": "good", "client_key": "wrong"}]
    sent.clear()
    assert prov.check_key("", "mine") == (True, "")
    assert sent == [{"api_key": PROJECT_KEY, "client_key": "mine"}]
    assert prov.check_key("", "") == (True, "")
    monkeypatch.setattr(_meta_FANART.requests, "get", lambda url, **kw: FakeResponse(200, {}))
    assert prov.check_key("good") == (False, "fanart.tv returned an unexpected response")


def test_resolve_skips_art_only_providers() -> None:
    manager = MetadataManager.__new__(MetadataManager)
    manager.load_cfg = lambda: {}
    manager.save_cfg = lambda _cfg: None
    art = SimpleNamespace(art_only=True, fetch=lambda **kw: {"images": {"logo": [{"url": "x"}]}})
    tmdb = SimpleNamespace(fetch=lambda **kw: {"title": "The Matrix"})
    manager.providers = {"FANART": art, "TMDB": tmdb}
    assert manager.resolve(entity="movie", ids={"tmdb": 603})["title"] == "The Matrix"


def test_redact_config_masks_fanart_keys() -> None:
    out = redact_config({"fanart": {"api_key": "abc", "personal_api_key": "def"}})
    assert out["fanart"]["api_key"] != "abc"
    assert out["fanart"]["personal_api_key"] != "def"


def test_art_route_downloads_picked_asset(tmp_path, monkeypatch) -> None:
    picked: list[tuple[Any, ...]] = []

    class Prov:
        def configured(self) -> bool:
            return True

        def lookup(self, entity, ids):
            picked.append((entity, ids))
            return MOVIE, True

        def pick_from(self, entity, data, kind, **kw):
            picked.append((kind, kw))
            return {"url": f"{ASSET}/moviedisc/bd1.png"}

    downloads: list[str] = []

    def download(url: str, dest: Path, timeout: float = 15.0):
        downloads.append(url)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"png")
        return dest, "image/png"

    monkeypatch.setattr(metaAPI, "_fanart_provider", lambda: Prov())
    monkeypatch.setattr(metaAPI, "_env", lambda: (None, tmp_path, lambda: {}))
    monkeypatch.setattr(metaAPI, "_cfg_ui_locale", lambda: "nl-NL")
    monkeypatch.setattr(metaAPI, "_cache_download", download)

    res = metaAPI.api_fanart_art(typ="movie", tmdb_id=603, kind="disc", disc="bluray", season=None, tvdb=None, imdb=None, locale=None)
    assert res.status_code == 200
    assert downloads == [f"{ASSET}/moviedisc/bd1.png"]
    assert picked[0] == ("movie", {"tmdb": "603"})
    assert picked[1][0] == "disc" and picked[1][1]["lang"] == "nl" and picked[1][1]["disc_type"] == "bluray"
    assert res.headers["cache-control"] == "public, max-age=86400"


def test_art_route_rejects_urls_outside_fanart_assets(tmp_path, monkeypatch) -> None:
    prov = SimpleNamespace(configured=lambda: True, lookup=lambda *a: ({}, True), pick_from=lambda *a, **k: {"url": "https://assets.fanart.tv/preview/movies/603/x.png"})
    monkeypatch.setattr(metaAPI, "_fanart_provider", lambda: prov)
    res = metaAPI.api_fanart_art(typ="movie", tmdb_id=603, kind="logo", disc=None, season=None, tvdb=None, imdb=None, locale=None)
    assert res.status_code == 404


def test_art_route_resolves_tvdb_and_rejects_bad_input(tmp_path, monkeypatch) -> None:
    seen: list[dict[str, str]] = []
    lookups: list[int] = []
    prov = make_provider(tmp_path)
    monkeypatch.setattr(prov, "lookup", lambda entity, ids: (seen.append(ids), ({}, True))[1])
    monkeypatch.setattr(metaAPI, "_fanart_provider", lambda: prov)
    monkeypatch.setattr(metaAPI, "_tmdb_tvdb_lookup", lambda tmdb_id: (lookups.append(tmdb_id), "121361")[1])
    for _ in range(2):
        res = metaAPI.api_fanart_art(typ="show", tmdb_id=1399, kind="logo", disc=None, season=None, tvdb=None, imdb=None, locale="en")
        assert res.status_code == 404
    assert seen == [{"tvdb": "121361"}, {"tvdb": "121361"}]
    assert lookups == [1399]
    assert make_provider(tmp_path).tvdb_for_tmdb(1399, lambda: "999") == ("121361", True)
    bad = metaAPI.api_fanart_art(typ="movie", tmdb_id=603, kind="nope", disc=None, season=None, tvdb=None, imdb=None, locale=None)
    assert bad.status_code == 400


def test_art_route_is_off_until_enabled(tmp_path, monkeypatch) -> None:
    prov = make_provider(tmp_path, {"fanart": {"api_key": "k"}})
    monkeypatch.setattr(metaAPI, "_fanart_provider", lambda: prov)
    off = metaAPI.api_fanart_art(typ="movie", tmdb_id=603, kind="disc", disc=None, season=None, tvdb=None, imdb=None, locale=None)
    assert off.status_code == 404
    assert not prov.prefers("logos")


def test_art_prefs_and_version() -> None:
    assert _meta_FANART.art_version({}) == "t"
    assert _meta_FANART.art_version({"fanart": {"api_key": "k", "logos": True}}) == "t"
    assert _meta_FANART.art_version({"fanart": ON}) == "fl1b1"
    assert _meta_FANART.art_version({"fanart": {**ON, "personal_api_key": "p"}}) == "fl1b1"
    assert _meta_FANART.art_version({"fanart": {**ON, "logos": False}}) == "fb1"
    assert _meta_FANART.art_version({"fanart": {**ON, "logos": "tmdb_first", "backgrounds": "tmdb"}}) == "fl2"
    assert _meta_FANART.art_version({"fanart": {**ON, "logos": "tmdb", "backgrounds": False}}) == "t"
    assert _meta_FANART.art_mode(True) == "fanart" and _meta_FANART.art_mode("bogus") == "fanart"
    assert _meta_FANART.art_mode(False) == "tmdb" and _meta_FANART.art_mode("tmdb_first") == "tmdb_first"


def test_background_pick_prefers_textless(tmp_path, monkeypatch) -> None:
    data = {"moviebackground": [
        {"url": f"{ASSET}/moviebackground/en.jpg", "lang": "en", "likes": "40"},
        {"url": f"{ASSET}/moviebackground/none.jpg", "lang": "", "likes": "1"},
    ]}
    monkeypatch.setattr(_meta_FANART.requests, "get", lambda url, **kw: FakeResponse(200, data))
    prov = make_provider(tmp_path)
    assert url_of(prov.pick("movie", {"tmdb": 603}, "background", lang="en", textless=True)).endswith("/none.jpg")
    assert url_of(prov.pick("movie", {"tmdb": 603}, "background", lang="en")).endswith("/en.jpg")


def test_raw_separates_errors_from_missing(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("CW_FANART_PROJECT_KEY", raising=False)
    monkeypatch.setattr(_meta_FANART.requests, "get", lambda url, **kw: FakeResponse(503))
    assert make_provider(tmp_path).raw("movie", {"tmdb": 5}) is None
    monkeypatch.setattr(_meta_FANART.requests, "get", lambda url, **kw: FakeResponse(404))
    assert make_provider(tmp_path).raw("movie", {"tmdb": 6}) == {}
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(_meta_FANART.requests, "get", lambda url, **kw: (calls.append(kw["params"]), FakeResponse(200, {}))[1])
    prov = make_provider(tmp_path, {"fanart": {**ON, "personal_api_key": "p"}})
    assert prov.raw("movie", {"tmdb": 7}) == {}
    assert make_provider(tmp_path, {"fanart": {**ON, "personal_api_key": "p"}}).raw("movie", {"tmdb": 7}) == {}
    assert calls == [{"api_key": PROJECT_KEY, "client_key": "p"}]
    assert (tmp_path / "movie_7.json").read_text("utf-8").count("_missing") == 1


def test_clear_memory_caches_includes_fanart(tmp_path, monkeypatch) -> None:
    prov = make_provider(tmp_path)
    prov._remember("movie_1", (1.0, {"moviebackground": [{"url": "x"}]}))
    monkeypatch.setattr(metaAPI, "_fanart_provider", lambda: prov)
    monkeypatch.setattr(metaAPI, "_tmdb_provider", lambda: None)
    metaAPI.clear_memory_caches()
    assert not prov._cache


def test_found_cache_is_shorter_with_personal_key(tmp_path) -> None:
    now = 1_000_000.0
    record = (now - 2 * 86400, {"moviebackground": [{"url": "x"}]})
    assert make_provider(tmp_path, {"fanart": ON})._fresh(record, now)
    assert not make_provider(tmp_path, {"fanart": {**ON, "personal_api_key": "p"}})._fresh(record, now)


def _tmdb_route(monkeypatch, tmp_path, cfg: dict[str, Any], **kw: Any):
    monkeypatch.setattr(metaAPI, "load_config", lambda: cfg)
    monkeypatch.setattr(metaAPI, "_env", lambda: (None, tmp_path, lambda: {}))
    tmdb_file = tmp_path / "art" / "tmdb.jpg"
    tmdb_file.parent.mkdir(parents=True, exist_ok=True)
    tmdb_file.write_bytes(b"jpg")
    monkeypatch.setattr(metaAPI, "get_art_file", lambda *a, **k: (str(tmdb_file), "image/jpeg"))
    args = {"typ": "movie", "tmdb_id": 603, "size": "w1280", "kind": "backdrop", "season": None, "episode": None, "locale": "en-US", "title": None, "year": None}
    args.update(kw)
    return metaAPI.api_tmdb_art(FAKE_REQUEST, **args)


def test_tmdb_route_serves_fanart_background_when_enabled(tmp_path, monkeypatch) -> None:
    downloads: list[str] = []

    def download(url: str, dest: Path, timeout: float = 15.0):
        downloads.append(url)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"jpg")
        return dest, "image/jpeg"

    data = {"moviebackground": [{"url": f"{ASSET}/moviebackground/none.jpg", "lang": "", "likes": "1"}]}
    monkeypatch.setattr(_meta_FANART.requests, "get", lambda url, **kw: FakeResponse(200, data))
    monkeypatch.setattr(metaAPI, "_cache_download", download)
    cfg: dict[str, Any] = {"tmdb": {"api_key": "t"}, "fanart": dict(ON)}
    prov = make_provider(tmp_path, cfg)
    monkeypatch.setattr(metaAPI, "_fanart_provider", lambda: prov)

    res = _tmdb_route(monkeypatch, tmp_path, cfg)
    assert downloads == [f"{ASSET}/moviebackground/none.jpg"]
    assert path_of(res).endswith(".jpg") and "tmdb.jpg" not in path_of(res)
    assert res.headers["cache-control"] == "public, max-age=86400"

    _tmdb_route(monkeypatch, tmp_path, cfg, size="w185")
    assert downloads[-1] == f"{ASSET}/moviebackground/none.jpg"

    cfg["fanart"]["backgrounds"] = False
    off = _tmdb_route(monkeypatch, tmp_path, cfg)
    assert path_of(off).endswith("tmdb.jpg") and "immutable" in off.headers["cache-control"]

    cfg["fanart"] = {"enabled": False}
    disabled = _tmdb_route(monkeypatch, tmp_path, cfg)
    assert path_of(disabled).endswith("tmdb.jpg") and "immutable" in disabled.headers["cache-control"]


def test_tmdb_first_uses_fanart_only_when_tmdb_has_nothing(tmp_path, monkeypatch) -> None:
    downloads: list[str] = []

    def download(url: str, dest: Path, timeout: float = 15.0):
        downloads.append(url)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"png")
        return dest, "image/png"

    data = {"hdmovielogo": [{"url": f"{ASSET}/hdmovielogo/a.png", "lang": "en", "likes": "1"}]}
    calls: list[str] = []
    monkeypatch.setattr(_meta_FANART.requests, "get", lambda url, **kw: (calls.append(url), FakeResponse(200, data))[1])
    monkeypatch.setattr(metaAPI, "_cache_download", download)
    cfg: dict[str, Any] = {"tmdb": {"api_key": "t"}, "fanart": {**ON, "logos": "tmdb_first"}}
    prov = make_provider(tmp_path, cfg)
    monkeypatch.setattr(metaAPI, "_fanart_provider", lambda: prov)

    found = _tmdb_route(monkeypatch, tmp_path, cfg, kind="logo", size="w500")
    assert path_of(found).endswith("tmdb.jpg") and "immutable" in found.headers["cache-control"]
    assert calls == [] and downloads == []

    monkeypatch.setattr(metaAPI, "get_art_file", lambda *a, **k: (str(metaAPI._placeholder_poster()), "image/svg+xml"))
    args = {"typ": "movie", "tmdb_id": 603, "size": "w500", "kind": "logo", "season": None, "episode": None, "locale": "en-US", "title": None, "year": None}
    gap = metaAPI.api_tmdb_art(FAKE_REQUEST, **args)
    assert downloads == [f"{ASSET}/hdmovielogo/a.png"]
    assert gap.headers["cache-control"] == "public, max-age=86400"


def test_tmdb_fallback_is_bounded_when_fanart_has_no_art(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(_meta_FANART.requests, "get", lambda url, **kw: FakeResponse(200, {}))
    cfg: dict[str, Any] = {"tmdb": {"api_key": "t"}, "fanart": dict(ON)}
    prov = make_provider(tmp_path, cfg)
    monkeypatch.setattr(metaAPI, "_fanart_provider", lambda: prov)
    res = _tmdb_route(monkeypatch, tmp_path, cfg)
    assert path_of(res).endswith("tmdb.jpg")
    assert res.headers["cache-control"] == "public, max-age=86400"


def test_tmdb_route_uses_resolved_namespace_for_fanart(tmp_path, monkeypatch) -> None:
    seen: list[tuple[str, dict[str, str]]] = []
    cfg: dict[str, Any] = {"tmdb": {"api_key": "t"}, "fanart": dict(ON)}
    prov = make_provider(tmp_path, cfg)
    monkeypatch.setattr(prov, "lookup", lambda entity, ids: (seen.append((entity, ids)), ({}, True))[1])
    monkeypatch.setattr(metaAPI, "_fanart_provider", lambda: prov)
    monkeypatch.setattr(metaAPI, "_resolve_entity", lambda entity, tmdb_id, **kw: "show")
    monkeypatch.setattr(metaAPI, "_tmdb_tvdb_lookup", lambda tmdb_id: "121361")
    _tmdb_route(monkeypatch, tmp_path, cfg, kind="logo", size="w500", title="Game of Thrones", year=2011)
    assert seen == [("tv", {"tvdb": "121361"})]


def test_tmdb_route_falls_back_with_short_cache_on_fanart_error(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(_meta_FANART.requests, "get", lambda url, **kw: FakeResponse(503))
    cfg: dict[str, Any] = {"tmdb": {"api_key": "t"}, "fanart": dict(ON)}
    prov = make_provider(tmp_path, cfg)
    monkeypatch.setattr(metaAPI, "_fanart_provider", lambda: prov)
    res = _tmdb_route(monkeypatch, tmp_path, cfg, kind="logo", size="w500")
    assert path_of(res).endswith("tmdb.jpg")
    assert res.headers["cache-control"] == "public, max-age=3600"


def test_expired_missing_with_fetch_error_stays_uncertain(tmp_path, monkeypatch) -> None:
    prov = make_provider(tmp_path)
    prov._disk_write("movie_8", {"_missing": True}, time.time() - 3 * 86400)
    monkeypatch.setattr(_meta_FANART.requests, "get", lambda url, **kw: FakeResponse(503))
    assert prov.lookup("movie", {"tmdb": 8}) == ({}, False)
    assert prov.raw("movie", {"tmdb": 8}) is None
    cfg: dict[str, Any] = {"tmdb": {"api_key": "t"}, "fanart": dict(ON)}
    monkeypatch.setattr(metaAPI, "_fanart_provider", lambda: prov)
    res = _tmdb_route(monkeypatch, tmp_path, cfg, tmdb_id=8)
    assert res.headers["cache-control"] == "public, max-age=3600"


def test_failed_tvdb_lookup_is_not_cached_as_missing(tmp_path) -> None:
    prov = make_provider(tmp_path)

    def fail() -> str:
        raise RuntimeError("down")

    assert prov.tvdb_for_tmdb(1399, fail) == ("", False)
    assert not (tmp_path / "tmdbtv_1399.json").exists()
    assert prov.tvdb_for_tmdb(1399, lambda: "121361") == ("121361", True)
    prov._remember("tmdbtv_1399", (time.time() - 40 * 86400, {"tvdb": "121361"}))
    prov._disk_write("tmdbtv_1399", {"tvdb": "121361"}, time.time() - 40 * 86400)
    assert prov.tvdb_for_tmdb(1399, fail) == ("121361", False)
    assert prov.tvdb_for_tmdb(1400, lambda: "") == ("", True)


def test_concurrent_misses_share_one_request(tmp_path, monkeypatch) -> None:
    calls: list[str] = []

    def slow(url, **kw):
        calls.append(url)
        time.sleep(0.2)
        return FakeResponse(200, MOVIE)

    monkeypatch.setattr(_meta_FANART.requests, "get", slow)
    prov = make_provider(tmp_path)
    results: list[Any] = []
    threads = [threading.Thread(target=lambda: results.append(prov.lookup("movie", {"tmdb": 603}))) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(calls) == 1
    assert all(certain and data.get("name") == "The Matrix" for data, certain in results)


def test_disc_one_beats_language(tmp_path) -> None:
    prov = make_provider(tmp_path)
    data = {"moviedisc": [
        {"url": f"{ASSET}/moviedisc/de1.png", "lang": "de", "likes": "1", "disc": "1", "disc_type": "bluray"},
        {"url": f"{ASSET}/moviedisc/en2.png", "lang": "en", "likes": "9", "disc": "2", "disc_type": "bluray"},
    ]}
    assert url_of(prov.pick_from("movie", data, "disc", lang="en", disc_type="bluray")).endswith("/de1.png")


def test_outdated_art_version_is_never_cached(tmp_path, monkeypatch) -> None:
    cfg: dict[str, Any] = {"tmdb": {"api_key": "t"}, "fanart": {"enabled": False}}
    prov = make_provider(tmp_path, cfg)
    monkeypatch.setattr(metaAPI, "_fanart_provider", lambda: prov)
    stale = _tmdb_route(monkeypatch, tmp_path, cfg, av="fl1b1")
    assert stale.headers["cache-control"] == "no-store"
    current = _tmdb_route(monkeypatch, tmp_path, cfg, av="t")
    assert "immutable" in current.headers["cache-control"]


def test_failed_fetch_is_shared_by_waiting_callers(tmp_path, monkeypatch) -> None:
    calls: list[str] = []

    def slow_fail(url, **kw):
        calls.append(url)
        time.sleep(0.2)
        return FakeResponse(503)

    monkeypatch.setattr(_meta_FANART.requests, "get", slow_fail)
    prov = make_provider(tmp_path)
    results: list[Any] = []

    def call() -> None:
        results.append(prov.lookup("movie", {"tmdb": 9}))

    first = [threading.Thread(target=call) for _ in range(3)]
    for thread in first:
        thread.start()
    time.sleep(0.05)
    late = threading.Thread(target=call)
    late.start()
    for thread in [*first, late]:
        thread.join()
    assert len(calls) <= 2
    assert all(result == ({}, False) for result in results)
    assert not prov._inflight


def test_failed_tvdb_lookups_are_shared(tmp_path) -> None:
    prov = make_provider(tmp_path)
    calls: list[int] = []

    def slow_fail() -> str:
        calls.append(1)
        time.sleep(0.2)
        raise RuntimeError("down")

    results: list[Any] = []
    threads = [threading.Thread(target=lambda: results.append(prov.tvdb_for_tmdb(1399, slow_fail))) for _ in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert calls == [1]
    assert results == [("", False)] * 3


def test_uncertain_lookups_get_short_cache_even_with_art(tmp_path, monkeypatch) -> None:
    def download(url: str, dest: Path, timeout: float = 15.0):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"jpg")
        return dest, "image/jpeg"

    monkeypatch.setattr(metaAPI, "_cache_download", download)
    cfg: dict[str, Any] = {"tmdb": {"api_key": "t"}, "fanart": dict(ON)}
    prov = make_provider(tmp_path, cfg)
    monkeypatch.setattr(metaAPI, "_fanart_provider", lambda: prov)
    data = {"moviebackground": [{"url": f"{ASSET}/moviebackground/none.jpg", "lang": "", "likes": "1"}]}
    prov._disk_write("movie_603", data, time.time() - 30 * 86400)
    monkeypatch.setattr(_meta_FANART.requests, "get", lambda url, **kw: FakeResponse(503))
    stale_art = _tmdb_route(monkeypatch, tmp_path, cfg)
    assert path_of(stale_art).endswith(".jpg") and "tmdb.jpg" not in path_of(stale_art)
    assert stale_art.headers["cache-control"] == "public, max-age=3600"

    show = {"showbackground": [{"url": "https://assets.fanart.tv/fanart/got.jpg", "lang": "", "likes": "1"}]}
    prov._disk_write("tv_121361", show, time.time())
    prov._disk_write("tmdbtv_1399", {"tvdb": "121361"}, time.time() - 40 * 86400)

    def fail(tmdb_id: int) -> str:
        raise RuntimeError("down")

    monkeypatch.setattr(metaAPI, "_tmdb_tvdb_lookup", fail)
    monkeypatch.setattr(metaAPI, "_resolve_entity", lambda entity, tmdb_id, **kw: entity)
    stale_map = _tmdb_route(monkeypatch, tmp_path, cfg, typ="tv", tmdb_id=1399)
    assert path_of(stale_map).endswith(".jpg") and "tmdb.jpg" not in path_of(stale_map)
    assert stale_map.headers["cache-control"] == "public, max-age=3600"


def test_settings_endpoint_saves_switches(monkeypatch) -> None:
    store: dict[str, Any] = {"fanart": {"api_key": "k"}}
    monkeypatch.setattr(metaAPI, "load_config", lambda: {k: dict(v) for k, v in store.items()})
    monkeypatch.setattr(metaAPI, "save_config", lambda cfg: store.update(cfg))
    assert metaAPI.api_fanart_settings({"logos": False, "backgrounds": "yes"}) == {"ok": True, "enabled": False, "logos": "tmdb", "backgrounds": "fanart"}
    assert store["fanart"] == {"api_key": "k", "logos": "tmdb"}
    assert metaAPI.api_fanart_settings({"logos": "tmdb_first", "backgrounds": "tmdb"})["logos"] == "tmdb_first"
    assert store["fanart"]["backgrounds"] == "tmdb"
    assert metaAPI.api_fanart_settings({"enabled": True})["enabled"] is True
    assert store["fanart"]["enabled"] is True


def test_save_keeps_masked_keys_and_disconnect_clears(monkeypatch) -> None:
    store: dict[str, Any] = {"fanart": {"api_key": "old", "personal_api_key": "mine"}}
    checked: list[tuple[str, str]] = []
    monkeypatch.setattr(metaAPI, "load_config", lambda: {k: dict(v) for k, v in store.items()})
    monkeypatch.setattr(metaAPI, "save_config", lambda cfg: store.update(cfg))
    monkeypatch.setattr(metaAPI, "_fanart_provider", lambda: SimpleNamespace(check_key=lambda k, p: (checked.append((k, p)), (k != "bad", "Invalid fanart.tv project API key" if k == "bad" else ""))[1]))

    assert metaAPI.api_fanart_save({"api_key": "new", "personal_api_key": "********"}) == {"ok": True}
    assert store["fanart"] == {"enabled": True, "api_key": "new", "personal_api_key": "mine"}
    assert metaAPI.api_fanart_save({"api_key": "bad"}) == {"ok": False, "error": "Invalid fanart.tv project API key"}
    assert store["fanart"]["api_key"] == "new"
    assert metaAPI.api_fanart_save({"api_key": "", "personal_api_key": ""}) == {"ok": True}
    assert checked == [("new", "mine"), ("bad", ""), ("", "")]
    assert metaAPI.api_fanart_disconnect() == {"ok": True}
    assert store["fanart"] == {"enabled": False, "api_key": "", "personal_api_key": ""}
