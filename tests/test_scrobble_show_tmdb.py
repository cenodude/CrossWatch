# CrossWatch test scripts
from __future__ import annotations

from typing import Any

import pytest

from providers.scrobble import _show_tmdb
from providers.scrobble.emby import watch as emby_watch

CFG = {"tmdb": {"api_key": "key"}}


class ResponseStub:
    def __init__(self, status_code: int = 200, payload: Any = None) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> Any:
        return self._payload


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
