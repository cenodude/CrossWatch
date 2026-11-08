# tests/test_meta_api_logos.py
# CrossWatch - Title logo selection, caching and missing artwork responses
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from pathlib import Path
from types import SimpleNamespace

import pytest

from api import metaAPI
from providers.metadata._meta_TMDB import TmdbProvider


@pytest.fixture
def artwork(monkeypatch, tmp_path):
    calls = []
    downloads = []
    images = [
        {"url": "https://image.tmdb.org/t/p/w500/english.png", "lang": "en"},
        {"url": "https://image.tmdb.org/t/p/w500/dutch.png", "lang": "nl"},
    ]

    def metadata(*args, **kwargs):
        calls.append(kwargs["need"])
        return {"images": {"logo": images}}

    def download(url, destination):
        downloads.append(url)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"png")
        return destination, "image/png"

    monkeypatch.setattr(metaAPI, "_resolve_entity", lambda entity, *a, **kw: entity)
    monkeypatch.setattr(metaAPI, "get_meta", metadata)
    monkeypatch.setattr(metaAPI, "_cache_download", download)
    monkeypatch.setattr(metaAPI, "_cfg_ui_locale", lambda: "nl-NL")
    return images, calls, downloads


def test_logo_uses_locale_and_caches_png_separately_from_other_art(artwork, tmp_path):
    images, calls, downloads = artwork
    path, mime = metaAPI.get_art_file("key", "tv", 42, "w500", tmp_path, kind="logo")
    assert mime == "image/png"
    assert Path(path).read_bytes() == b"png"
    assert downloads == [images[1]["url"]]
    assert calls == [{"logo": True}]
    assert metaAPI.get_art_file("key", "tv", 42, "w500", tmp_path, kind="logo") == (path, mime)
    assert len(calls) == len(downloads) == 1
    english, _ = metaAPI.get_art_file("key", "tv", 42, "w500", tmp_path, locale="en-US", kind="logo")
    assert english != path
    assert downloads[-1] == images[0]["url"]


def test_svg_logo_downloads_png_variant(artwork, tmp_path):
    images, _, downloads = artwork
    images[:] = [{"url": "https://image.tmdb.org/t/p/w500/title.svg", "lang": "en"}]
    path, mime = metaAPI.get_art_file("key", "movie", 7, "w500", tmp_path, kind="logo")
    assert downloads == ["https://image.tmdb.org/t/p/w500/title.png"]
    assert path.endswith(".png")
    assert mime == "image/png"


def test_missing_logos_are_cached_then_retried(artwork, tmp_path, monkeypatch):
    images, calls, downloads = artwork
    images.clear()
    now = 1000000
    monkeypatch.setattr(metaAPI.time, "time", lambda: now)
    for _ in range(2):
        path, _ = metaAPI.get_art_file("key", "movie", 7, "w500", tmp_path, kind="logo")
        assert metaAPI._is_placeholder_art(path)
    assert len(calls) == 1
    assert not downloads
    now += 86401
    images.append({"url": "https://image.tmdb.org/t/p/w500/new.png", "lang": "en"})
    path, mime = metaAPI.get_art_file("key", "movie", 7, "w500", tmp_path, kind="logo")
    assert mime == "image/png"
    assert len(calls) == 2


def test_metadata_failure_does_not_cache_absence(artwork, tmp_path, monkeypatch):
    monkeypatch.setattr(metaAPI, "get_meta", lambda *a, **kw: {})
    metaAPI.get_art_file("key", "movie", 7, "w500", tmp_path, kind="logo")
    assert not list((tmp_path / "art").glob("*.json"))


def test_missing_logo_response_does_not_send_poster_placeholder(artwork, tmp_path, monkeypatch):
    artwork[0].clear()
    monkeypatch.setattr(metaAPI, "load_config", lambda: {"tmdb": {"api_key": "key"}})
    monkeypatch.setattr(metaAPI, "_env", lambda: (None, tmp_path, lambda: {}))
    request = SimpleNamespace(headers={}, url=SimpleNamespace(path="/art/tmdb/tv/42"))
    response = metaAPI.api_tmdb_art(request, typ="tv", tmdb_id=42, size="w500", kind="logo", locale="nl-NL", title=None, year=None)
    assert response.status_code == 404
    assert response.body == b"Logo not available"


def test_logo_provider_includes_english_fallback(monkeypatch):
    provider = TmdbProvider(lambda: {"tmdb": {"api_key": "key"}}, lambda _: None)
    requests = []
    monkeypatch.setattr(provider, "_get", lambda url, params: requests.append((url, params)) or {"logos": [{"file_path": "/english.png", "iso_639_1": "en"}]})
    result = provider._images("42", "show", "nl-NL", {"logo": True})
    assert set(requests[0][1]["include_image_language"].split(",")) == {"nl", "nl-NL", "null", "en"}
    assert result["logo"][0]["lang"] == "en"
