# /tests/test_playback_progress_imdb.py
# CrossWatch - IMDb playback title and artwork enrichment tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from copy import deepcopy

import pytest

from services.playback_progress.adapters.nuvio import NuvioPlaybackAdapter
from services.playback_progress.adapters.stremio import StremioPlaybackAdapter
from services.playback_progress.models import PlaybackCapabilities


@pytest.fixture(params=[StremioPlaybackAdapter, NuvioPlaybackAdapter], ids=["stremio", "nuvio"])
def adapter(request):
    return request.param()


class Metadata:
    def __init__(self, title="The Dark Knight", *, fail=False):
        self.title = title
        self.fail = fail
        self.calls = []

    def fetch(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail:
            raise TimeoutError("Metadata unavailable")
        return {
            "title": self.title,
            "year": 2008,
            "images": {
                "poster": [{"url": "https://example.test/poster.jpg"}],
                "backdrop": [{"url": "https://example.test/backdrop.jpg"}],
            },
        }


def record_for(adapter, item, metadata):
    caps = PlaybackCapabilities(
        provider=adapter.provider,
        provider_label=adapter.provider_label,
        instance_id="default",
        instance_label="Default",
        configured=True,
    )
    return adapter._record("imdb:tt0468569", item, "default", "Default", caps, metadata)


def movie(**overrides):
    return {
        "type": "movie",
        "ids": {"imdb": "tt0468569"},
        "title": "imdb:tt0468569",
        "progress_ms": 638400,
        "duration_ms": 9120000,
        **overrides,
    }


@pytest.mark.parametrize("title", ["imdb:tt0468569", "tt0468569"])
def test_imdb_movie_placeholder_gets_title_and_artwork(adapter, title):
    item = movie(title=title)
    original = deepcopy(item)
    metadata = Metadata()
    record = record_for(adapter, item, metadata)

    assert record is not None
    assert record.title == "The Dark Knight"
    assert record.year == 2008
    assert record.poster_url == "https://example.test/poster.jpg"
    assert record.backdrop_url == "https://example.test/backdrop.jpg"
    assert record.progress_percent == 7
    assert metadata.calls[0]["ids"] == {"imdb": "tt0468569"}
    assert metadata.calls[0]["entity"] == "movie"
    assert record.provider_metadata["progress_item"] == original
    assert item == original


def test_imdb_show_placeholder_preserves_episode_title(adapter):
    item = movie(
        type="episode", ids={}, show_ids={"imdb": "tt0903747"},
        series_title="imdb:tt0903747", title="Pilot", season=1, episode=1,
    )
    metadata = Metadata("Breaking Bad")
    record = record_for(adapter, item, metadata)

    assert record is not None
    assert record.title == record.series_title == "Breaking Bad"
    assert record.episode_title == "Pilot"
    assert record.backdrop_url == "https://example.test/backdrop.jpg"
    assert metadata.calls[0]["entity"] == "tv"
    assert metadata.calls[0]["ids"] == {"imdb": "tt0903747"}


def test_existing_poster_does_not_skip_missing_backdrop(adapter):
    record = record_for(adapter, movie(title="Local title", poster="https://example.test/local.jpg"), Metadata())

    assert record is not None
    assert record.title == "Local title"
    assert record.poster_url == "https://example.test/local.jpg"
    assert record.backdrop_url == "https://example.test/backdrop.jpg"


def test_complete_metadata_does_not_fetch_again(adapter):
    metadata = Metadata()
    record = record_for(adapter, movie(
        title="Local title", poster_url="https://example.test/local.jpg",
        backdrop_url="https://example.test/local-backdrop.jpg",
    ), metadata)

    assert record is not None
    assert record.title == "Local title"
    assert metadata.calls == []


def test_metadata_failure_keeps_playback_record(adapter):
    item = movie()
    record = record_for(adapter, item, Metadata(fail=True))

    assert record is not None
    assert record.ids == item["ids"]
    assert record.progress_percent == 7
    assert record.provider_metadata["progress_item"] == item
