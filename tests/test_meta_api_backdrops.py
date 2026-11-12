# tests/test_meta_api_backdrops.py
# CrossWatch - Backdrop selection, cache migration and missing artwork responses
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from pathlib import Path
from types import SimpleNamespace

import pytest

from api import metaAPI
from providers.metadata._meta_TMDB import TmdbProvider


@pytest.fixture
def artwork(monkeypatch):
    images = [
        {"url": "https://image.tmdb.org/t/p/w1280/key-art.jpg", "lang": "en", "w": 1920, "h": 1080, "vote_average": 9},
        {"url": "https://image.tmdb.org/t/p/w1280/scene.jpg", "lang": None, "w": 1920, "h": 1080, "vote_average": 7},
        {"url": "https://image.tmdb.org/t/p/w780/poster.jpg", "lang": None, "w": 780, "h": 1170, "vote_average": 10},
    ]
    downloads = []

    def download(url, destination):
        downloads.append(url)
        destination.write_bytes(b"image")
        return destination, "image/jpeg"

    monkeypatch.setattr(metaAPI, "_resolve_entity", lambda entity, *a, **kw: entity)
    monkeypatch.setattr(metaAPI, "_cfg_ui_locale", lambda: "en-US")
    monkeypatch.setattr(metaAPI, "get_meta", lambda *a, **kw: {"images": {"backdrop": images, "poster": images[:1]}})
    monkeypatch.setattr(metaAPI, "_cache_download", download)
    return images, downloads


@pytest.mark.parametrize("locale", ["en-US", "nl-NL", None])
def test_backdrop_prefers_neutral_landscape_without_changing_poster_ranking(artwork, locale):
    images, _ = artwork
    assert metaAPI._pick_best_image(images, locale, kind="backdrop") == images[1]
    assert metaAPI._pick_best_image(images, locale) == images[0]


def test_backdrop_falls_back_to_localized_landscape_and_rejects_portraits(artwork):
    images, _ = artwork
    assert metaAPI._pick_best_image([images[0], images[2]], "en-US", kind="backdrop") == images[0]
    assert metaAPI._pick_best_image([images[2]], "en-US", kind="backdrop") is None


def test_backdrop_uses_quality_among_neutral_candidates(artwork):
    images, _ = artwork
    better = {**images[1], "url": "https://image.tmdb.org/t/p/w1280/better-scene.jpg", "vote_average": 8}
    assert metaAPI._pick_best_image([*images, better], "en-US", kind="backdrop") == better


def test_backdrop_replaces_old_cached_selection_and_reuses_new_art(artwork, tmp_path):
    images, downloads = artwork
    cache = tmp_path / "art"
    cache.mkdir()
    old_stem = metaAPI._safe_cache_digest_stem("tmdb_art", "tv", "42", "backdrop", "en-US", "w1280")
    old = cache / f"{old_stem}.jpg"
    old.write_bytes(b"old promotional artwork")
    metaAPI._write_json(old.with_suffix(".json"), {"url": images[0]["url"]})
    result = metaAPI.get_art_file("key", "tv", 42, "w1280", tmp_path, kind="backdrop")
    assert Path(result[0]) != old
    assert downloads == [images[1]["url"]]
    assert metaAPI.get_art_file("key", "tv", 42, "w1280", tmp_path, kind="backdrop") == result
    assert len(downloads) == 1
    assert old.read_bytes() == b"old promotional artwork"


def test_missing_backdrop_does_not_serve_poster_placeholder(artwork, tmp_path, monkeypatch):
    artwork[0].clear()
    monkeypatch.setattr(metaAPI, "load_config", lambda: {"tmdb": {"api_key": "key"}})
    monkeypatch.setattr(metaAPI, "_env", lambda: (None, tmp_path, lambda: {}))
    request = SimpleNamespace(headers={}, url=SimpleNamespace(path="/art/tmdb/tv/42"))
    response = metaAPI.api_tmdb_art(request, typ="tv", tmdb_id=42, size="w1280", kind="backdrop", locale="en-US", title=None, year=None)
    assert response.status_code == 404
    assert response.body == b"Backdrop not available"
    assert response.headers["cache-control"] == "no-store"


def test_provider_preserves_backdrop_quality_and_separates_posters(monkeypatch):
    provider = TmdbProvider(lambda: {"tmdb": {"api_key": "key"}}, lambda _: None)
    monkeypatch.setattr(provider, "_get", lambda *a: {
        "posters": [{"file_path": "/poster.jpg"}],
        "backdrops": [{"file_path": "/scene.jpg", "width": 1920, "height": 1080, "iso_639_1": None, "vote_average": 8, "vote_count": 12}],
    })
    result = provider._images("42", "show", "en-US", {"poster": True, "backdrop": True})
    assert result["poster"][0]["url"].endswith("/poster.jpg")
    assert result["backdrop"] == [{"url": "https://image.tmdb.org/t/p/w1280/scene.jpg", "w": 1920, "h": 1080, "lang": None, "vote_average": 8, "vote_count": 12}]
