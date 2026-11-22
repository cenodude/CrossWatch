# CrossWatch test scripts
from __future__ import annotations

from typing import Any

import pytest
import requests

from providers.scrobble import _show_tmdb
from providers.scrobble.emby import watch as emby_watch

CFG = {"tmdb": {"api_key": "key"}}


class ResponseStub:
    def __init__(self, status_code: int = 200, payload: Any = None) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers: dict[str, str] = {}

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)


@pytest.fixture(autouse=True)
def clear_cache() -> None:
    _show_tmdb._CACHE.clear()


def _install(monkeypatch: pytest.MonkeyPatch, responses: dict[tuple[str, str], Any]) -> list[tuple[str, str]]:
    calls: list[tuple[str, str]] = []

    def fake_get(url: str, params: dict[str, Any], **_: Any) -> ResponseStub:
        key = (url.rsplit("/", 1)[-1], params["external_source"])
        calls.append(key)
        hit = responses.get(key)
        if isinstance(hit, Exception):
            raise hit
        if isinstance(hit, ResponseStub):
            return hit
        return ResponseStub(200, hit or {"tv_results": [], "tv_episode_results": []})

    monkeypatch.setattr(_show_tmdb.requests, "get", fake_get)
    return calls


def test_show_id_lookup_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install(monkeypatch, {("393159", "tvdb_id"): {"tv_results": [{"id": 196322}]}})
    assert _show_tmdb.show_tmdb_id(CFG, {"tvdb": 393159}, {"tvdb": "11728983"}) == 196322
    assert calls == [("393159", "tvdb_id")]


def test_episode_lookup_used_when_show_has_no_tmdb_link(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install(monkeypatch, {("7974291", "tvdb_id"): {"tv_episode_results": [{"id": 3959040, "show_id": 113988}]}})
    assert _show_tmdb.show_tmdb_id(CFG, {"tvdb": 389492}, {"tvdb": "7974291"}) == 113988
    assert calls == [("389492", "tvdb_id"), ("7974291", "tvdb_id")]


def test_episode_id_is_never_returned_as_show_id(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, {("tt1480055", "imdb_id"): {"tv_episode_results": [{"id": 63056, "show_id": 1399}]}})
    assert _show_tmdb.show_tmdb_id(CFG, {}, {"imdb": "tt1480055"}) == 1399


def test_no_key_or_no_usable_ids_makes_no_request(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install(monkeypatch, {})
    assert _show_tmdb.show_tmdb_id({}, {"tvdb": 393159}, {}) is None
    assert _show_tmdb.show_tmdb_id(CFG, {"tvdb": "abc", "imdb": "1480055"}, None) is None
    assert calls == []


def test_hits_and_misses_are_cached_but_failures_are_not(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install(monkeypatch, {("75897", "tvdb_id"): {"tv_results": [{"id": 2190}]}, ("5", "tvdb_id"): ResponseStub(500)})
    assert _show_tmdb.show_tmdb_id(CFG, {"tvdb": 75897}, {}) == 2190
    assert _show_tmdb.show_tmdb_id(CFG, {"tvdb": 75897}, {}) == 2190
    assert _show_tmdb.show_tmdb_id(CFG, {"tvdb": 404404}, {}) is None
    assert _show_tmdb.show_tmdb_id(CFG, {"tvdb": 404404}, {}) is None
    assert _show_tmdb.show_tmdb_id(CFG, {"tvdb": 5}, {}) is None
    assert _show_tmdb.show_tmdb_id(CFG, {"tvdb": 5}, {}) is None
    assert calls == [("75897", "tvdb_id"), ("404404", "tvdb_id"), ("5", "tvdb_id"), ("5", "tvdb_id")]


def test_emby_watch_fills_tmdb_show_from_tvdb_only_series(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, {("7974291", "tvdb_id"): {"tv_episode_results": [{"id": 3959040, "show_id": 113988}]}})
    monkeypatch.setattr(emby_watch, "_cfg", lambda: CFG)
    item = {"Type": "Episode", "ProviderIds": {"Tvdb": "7974291"}, "SeriesProviderIds": {"Tvdb": "389492"}}
    ids = emby_watch._enrich_episode_ids(item, {}, emby_watch._map_provider_ids(item))
    assert ids["tvdb_show"] == "389492"
    assert ids["tmdb_show"] == 113988
    assert ids["tvdb"] == "7974291"


def test_kodi_watch_fills_tmdb_show_from_tvdb_only_show(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.kodi.watch import KodiWatchService

    calls = _install(monkeypatch, {("389492", "tvdb_id"): {"tv_results": []}, ("7974291", "tvdb_id"): {"tv_episode_results": [{"show_id": 113988}]}})
    cfg = {**CFG, "kodi": {"server": "http://kodi.local:8080", "connection_verified": True}, "scrobble": {"enabled": True, "sources": {"watcher": True}}}
    service = KodiWatchService(dispatcher=object(), cfg_provider=lambda: cfg, instance_id="default", quiet_startup=True)
    meta = {"media_type": "episode", "ids": {"tvdb_episode": "7974291", "tvdb_show": "389492"}}
    service._enrich_episode_show_ids({}, meta)
    assert meta["ids"]["tmdb_show"] == "113988"
    assert calls == [("389492", "tvdb_id"), ("7974291", "tvdb_id")]

    meta = {"media_type": "episode", "ids": {"tvdb_episode": "1", "tmdb_show": "2190"}}
    service._enrich_episode_show_ids({}, meta)
    assert meta["ids"]["tmdb_show"] == "2190"
    assert len(calls) == 2


def test_plex_watch_fills_tmdb_show_from_tvdb_only_show(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    from providers.scrobble.plex import watch as plex_watch
    from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent

    _install(monkeypatch, {("389492", "tvdb_id"): {"tv_results": []}, ("7974291", "tvdb_id"): {"tv_episode_results": [{"show_id": 113988}]}})
    cfg = {**CFG, "plex": {"server_url": "http://plex.test", "account_token": "token", "username": "owner"}, "scrobble": {"enabled": True, "sources": {"watcher": True}, "watch": {"filters": {}}}}
    show = SimpleNamespace(title="Monster (2022)", guids=[SimpleNamespace(id="tvdb://389492")])
    episode = SimpleNamespace(type="episode", title="Episode One", year=2022, guids=[SimpleNamespace(id="tvdb://7974291")], seasonNumber=1, index=1, media=[], show=lambda: show)
    service = plex_watch.WatchService(dispatcher=Dispatcher([], cfg_provider=lambda: cfg), cfg_provider=lambda: cfg, quiet_startup=True)
    setattr(service, "_plex", SimpleNamespace(fetchItem=lambda _rk: episode))
    ev = ScrobbleEvent(action="start", media_type="episode", ids={}, title="x", year=None, season=None, number=None, progress=1, account="owner", server_uuid="server-1", session_key="1", raw={"item": {"ratingKey": "42"}})
    out = service._enrich_event_with_plex(ev)
    assert out is not None
    assert out.ids["tvdb_show"] == "389492"
    assert out.ids["tmdb_show"] == "113988"
    assert out.title == "Monster (2022)"


def test_imdb_only_episode_gets_its_show_tmdb_id(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, {("tt4063800", "imdb_id"): {"tv_results": [{"id": 62476}]}})
    ids, show_ids = {"imdb": "tt4063800"}, {}
    assert _show_tmdb.fill_display_tmdb(CFG, "episode", ids, show_ids) is True
    assert show_ids == {"tmdb": "62476"}
    assert ids == {"imdb": "tt4063800"}


def test_imdb_only_movie_gets_its_tmdb_id(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install(monkeypatch, {("tt33764258", "imdb_id"): {"movie_results": [{"id": 1368337}]}})
    ids: dict[str, Any] = {"imdb": "tt33764258"}
    assert _show_tmdb.fill_display_tmdb(CFG, "movie", ids, {}) is True
    assert ids["tmdb"] == "1368337"
    assert _show_tmdb.fill_display_tmdb(CFG, "movie", ids, {}) is False
    assert calls == [("tt33764258", "imdb_id")]


def test_fill_never_runs_without_a_tmdb_key_or_with_known_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install(monkeypatch, {})
    assert _show_tmdb.fill_display_tmdb({}, "movie", {"imdb": "tt1"}, {}) is False
    assert _show_tmdb.fill_display_tmdb(CFG, "episode", {"imdb": "tt1"}, {"tmdb": "5"}) is False
    assert _show_tmdb.fill_display_tmdb(CFG, "movie", {"imdb": "tt1", "tmdb": "7"}, {}) is False
    assert _show_tmdb.tmdb_title({}, "movie", 7) == ("", None)
    assert calls == []


def test_tmdb_title_reads_movie_and_show_names(monkeypatch: pytest.MonkeyPatch) -> None:
    bodies = {"movie/1368337": {"title": "The Odyssey", "release_date": "2026-07-15"},
              "tv/62476": {"name": "The Bureau", "first_air_date": "2015-04-27"}}
    monkeypatch.setattr(_show_tmdb.requests, "get", lambda url, **_: ResponseStub(200, bodies[url.split("/3/", 1)[1]]))
    assert _show_tmdb.tmdb_title(CFG, "movie", "1368337") == ("The Odyssey", 2026)
    assert _show_tmdb.tmdb_title(CFG, "show", 62476) == ("The Bureau", 2015)


def test_hub_history_rows_get_display_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import profileAPI

    _install(monkeypatch, {("tt4063800", "imdb_id"): {"tv_results": [{"id": 62476}]},
                           ("tt33764258", "imdb_id"): {"movie_results": [{"id": 1368337}]}})
    rows = [{"type": "episode", "season": 1, "episode": 2, "ids": {"imdb": "tt4063800"}},
            {"type": "movie", "ids": {"imdb": "tt33764258"}},
            {"type": "movie", "ids": {"tmdb": "9"}}]
    profileAPI._fill_history_tmdb(CFG, rows)
    assert rows[0]["tmdb_show"] == "62476" and rows[0]["ids"] == {"imdb": "tt4063800"}
    assert rows[1]["tmdb"] == "1368337" and rows[1]["ids"] == {"imdb": "tt33764258"}
    assert rows[2] == {"type": "movie", "ids": {"tmdb": "9"}}
    untouched = [{"type": "movie", "ids": {"imdb": "tt33764258"}}]
    profileAPI._fill_history_tmdb({}, untouched)
    assert untouched == [{"type": "movie", "ids": {"imdb": "tt33764258"}}]


def test_tracker_rows_get_display_ids_and_missing_titles(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import trackerAPI

    finds = {("tt33764258", "imdb_id"): {"movie_results": [{"id": 1368337}]}}
    details = {"movie/1368337": {"title": "The Odyssey", "release_date": "2026-07-15"}}

    def get(url: str, params: dict[str, Any], **_: Any) -> ResponseStub:
        path = url.split("/3/", 1)[1]
        if path.startswith("find/"):
            return ResponseStub(200, finds.get((path[5:], params["external_source"])) or {})
        return ResponseStub(200, details[path])

    monkeypatch.setattr(_show_tmdb.requests, "get", get)
    monkeypatch.setattr(trackerAPI, "_cfg", lambda: CFG)
    rows = [{"type": "movie", "title": None, "year": None, "ids": {"imdb": "tt33764258"}, "show_ids": {}, "art": None}]
    trackerAPI._fill_display_tmdb(rows)
    assert rows[0]["ids"] == {"imdb": "tt33764258"}
    assert rows[0]["art"] == {"kind": "movie", "tmdb": "1368337"}
    assert (rows[0]["title"], rows[0]["year"]) == ("The Odyssey", 2026)
    monkeypatch.setattr(trackerAPI, "_cfg", lambda: {})
    bare = [{"type": "movie", "title": None, "year": None, "ids": {"imdb": "tt33764258"}, "show_ids": {}, "art": None}]
    trackerAPI._fill_display_tmdb(bare)
    assert bare[0]["title"] is None and bare[0]["art"] is None


def test_tracker_episode_keeps_its_stored_show_key_but_gets_art(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import trackerAPI

    _install(monkeypatch, {("tt4063800", "imdb_id"): {"tv_results": [{"id": 62476}]}})
    monkeypatch.setattr(trackerAPI, "_cfg", lambda: CFG)
    row = {"type": "episode", "title": "Episode 2", "series_title": "The Bureau", "series_year": 2015, "season": 1, "episode": 2,
           "ids": {}, "show_ids": {"imdb": "tt4063800"}, "art": None}
    trackerAPI._fill_display_tmdb([row])
    assert row["show_ids"] == {"imdb": "tt4063800"}
    assert row["art"]["kind"] == "tv" and row["art"]["tmdb"] == "62476"


def test_stremio_progress_episode_gets_show_tmdb_for_artwork(monkeypatch: pytest.MonkeyPatch) -> None:
    from services.playback_progress.adapters import stremio
    from services.playback_progress.models import PlaybackCapabilities

    _install(monkeypatch, {("tt4063800", "imdb_id"): {"tv_results": [{"id": 62476}]}})
    caps = PlaybackCapabilities(provider="stremio", provider_label="Stremio", instance_id="default", instance_label="Default", configured=True, read=True)
    item = {"type": "episode", "title": "The Bureau", "series_title": "The Bureau", "season": 1, "episode": 2,
            "ids": {"imdb": "tt4063800"}, "show_ids": {"imdb": "tt4063800"}, "poster": "https://images.example/p.jpg",
            "progress_percent": 100}
    record = stremio.StremioPlaybackAdapter()._record("k", item, "default", "Default", caps, object(), CFG)
    assert record is not None
    assert record.provider_metadata["show_ids"]["tmdb"] == "62476"
    assert "tmdb" not in record.ids
