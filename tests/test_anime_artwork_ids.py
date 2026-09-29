# tests/test_anime_artwork_ids.py
# CrossWatch test scripts
from __future__ import annotations

from typing import Any

import pytest

import cw_platform.anime_mapping as anime_mapping
import cw_platform.anime_mapping.service as service
import cw_platform.config_base as config_base
from services import dashboard_widgets

ON = {"anime_mapping": {"enabled": True}}


class _FakeService:
    calls: list[tuple[dict[str, Any], str | None]] = []
    table = {
        ("anilist", "21519"): {"tmdb": "372058", "imdb": "tt5311514", "tvdb": "197"},
        ("anidb", "9541"): {"tmdb": "1429", "imdb": "tt2560140", "tvdb": "267440"},
    }

    def __init__(self, cfg: Any = None) -> None:
        self.cfg = cfg

    def ready(self) -> bool:
        return True

    def enrich_ids(self, ids: dict[str, Any], *, media_type: str | None = None) -> dict[str, Any]:
        _FakeService.calls.append((dict(ids), media_type))
        for key, value in ids.items():
            found = self.table.get((key, str(value)))
            if found:
                return {"ids": {**ids, **found}}
        return {"ids": dict(ids)}


@pytest.fixture(autouse=True)
def _fake_service(monkeypatch: Any) -> None:
    _FakeService.calls = []
    monkeypatch.setattr(service, "AnimeMappingService", _FakeService)


def test_artwork_ids_adds_missing_aired_ids() -> None:
    assert service.artwork_ids(ON, {"anilist": 21519}, media_type="movie") == {"tmdb": "372058", "imdb": "tt5311514", "tvdb": "197"}
    assert _FakeService.calls == [({"anilist": "21519"}, "movie")]


def test_artwork_ids_keeps_existing_ids() -> None:
    assert service.artwork_ids(ON, {"anidb": "9541", "tvdb": "999"}, media_type="show") == {"tmdb": "1429", "imdb": "tt2560140"}


def test_artwork_ids_skips_when_tmdb_known() -> None:
    assert service.artwork_ids(ON, {"anidb": "9541", "tmdb": "1429"}) == {}
    assert _FakeService.calls == []


def test_artwork_ids_skips_without_anime_ids() -> None:
    assert service.artwork_ids(ON, {"tvdb": "267440", "imdb": "tt2560140"}) == {}
    assert _FakeService.calls == []


def test_artwork_ids_respects_mapping_switch() -> None:
    assert service.artwork_ids({"anime_mapping": {"enabled": False}}, {"anidb": "9541"}) == {}
    assert service.artwork_ids({}, {"anidb": "9541"}) == {}
    assert _FakeService.calls == []


def _no_metadata(monkeypatch: Any) -> list[Any]:
    lookups: list[Any] = []

    class _Manager:
        def resolve(self, **kwargs: Any) -> dict[str, Any]:
            lookups.append(kwargs)
            return {}

    monkeypatch.setattr(dashboard_widgets, "_metadata_manager", lambda: _Manager())
    monkeypatch.setattr(config_base, "load_config", lambda: ON)
    return lookups


def test_dashboard_movie_with_only_anime_ids_gets_tmdb_art(monkeypatch: Any) -> None:
    lookups = _no_metadata(monkeypatch)
    row = {"type": "movie", "title": "Your Name.", "year": 2016, "ids": {"anilist": "21519"}}

    dashboard_widgets._resolve_missing_art(row, size="w300")

    assert row["tmdb"] == "372058"
    assert row["poster"].startswith("/art/tmdb/movie/372058")
    assert row["art_reason"] == "anime_mapping_resolved"
    assert lookups == []


def test_dashboard_episode_uses_show_level_anime_ids(monkeypatch: Any) -> None:
    lookups = _no_metadata(monkeypatch)
    row = {"type": "episode", "title": "Attack on Titan", "season": 3, "episode": 15, "ids": {"show_ids": {"anidb": "9541"}}}

    dashboard_widgets._resolve_missing_art(row, size="w300", episode_still=True)

    assert row["tmdb"] == "1429"
    assert row["ids"]["show_ids"]["tmdb"] == "1429"
    assert row["poster"] == "/art/tmdb/tv/1429?kind=still&season=3&episode=15&size=w300&artv=2"
    assert lookups == []


def test_dashboard_episode_ignores_episode_level_anime_ids(monkeypatch: Any) -> None:
    lookups = _no_metadata(monkeypatch)
    row = {"type": "episode", "title": "Attack on Titan", "season": 3, "episode": 15, "ids": {"anidb": "9541"}}

    dashboard_widgets._resolve_missing_art(row, size="w300", episode_still=True)

    assert _FakeService.calls == []
    assert "tmdb" not in row
    assert len(lookups) == 1


def test_dashboard_mapping_off_falls_back_to_metadata(monkeypatch: Any) -> None:
    lookups = _no_metadata(monkeypatch)
    monkeypatch.setattr(config_base, "load_config", lambda: {"anime_mapping": {"enabled": False}})
    row = {"type": "movie", "title": "Your Name.", "year": 2016, "ids": {"anilist": "21519"}}

    dashboard_widgets._resolve_missing_art(row, size="w300")

    assert "tmdb" not in row
    assert len(lookups) == 1


def test_package_exports_artwork_ids() -> None:
    assert anime_mapping.artwork_ids is service.artwork_ids
