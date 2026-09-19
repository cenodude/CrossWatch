# tests/test_tracearr_history.py
# CrossWatch - Tracearr History Import Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from providers.sync.tracearr import _history as history


SERVER = "1636639d-9326-4f21-9409-4d02386e87f7"
SHOW_MEDIA_ID = "1c00cb97-83e7-434b-b405-c57ca8f0d671"
SHOW_IDS = {"imdb": "tt12327578", "tmdb": "103516", "tvdb": "382389"}


def _movie(**over: Any) -> dict[str, Any]:
    row = {
        "id": "m1",
        "server_id": SERVER,
        "server_type": "plex",
        "media_type": "movie",
        "media_title": "Interstellar",
        "year": 2014,
        "imdb_id": "tt0816692",
        "tmdb_id": 157336,
        "tvdb_id": 131079,
        "rating_key": "45120",
        "started_at": "2026-09-18T18:00:00.000Z",
        "stopped_at": "2026-09-18T20:25:22.123Z",
        "watched": True,
        "percent_complete": 100,
    }
    row.update(over)
    return row


def _episode(**over: Any) -> dict[str, Any]:
    row = {
        "id": "e1",
        "server_id": SERVER,
        "server_type": "plex",
        "media_type": "episode",
        "media_title": "Once La'an a Time",
        "show_title": "Star Trek: Strange New Worlds",
        "season_number": 4,
        "episode_number": 9,
        "year": 2026,
        "imdb_id": "tt37231069",
        "tmdb_id": 7193803,
        "tvdb_id": 11760816,
        "rating_key": "48793",
        "grandparent_rating_key": "43892",
        "show_media_id": SHOW_MEDIA_ID,
        "started_at": "2026-09-19T08:46:33.097Z",
        "stopped_at": "2026-09-19T08:57:43.052Z",
        "watched": True,
        "percent_complete": 100,
    }
    row.update(over)
    return row


class _Client:
    def __init__(self, pages: list[list[dict[str, Any]]], shows: dict[str, dict[str, str]] | None = None):
        self.pages = pages
        self.shows = {SHOW_MEDIA_ID: SHOW_IDS} if shows is None else shows
        self.calls: list[dict[str, Any]] = []

    def get(self, path: str, **params: Any) -> Any:
        self.calls.append({"path": path, **params})
        assert path == "/history"
        idx = int(params.get("cursor") or 0)
        nxt = str(idx + 1) if idx + 1 < len(self.pages) else None
        return {"data": list(self.pages[idx]), "meta": {"nextCursor": nxt, "pageSize": params.get("pageSize")}}

    def show_ids(self, show_media_id: Any) -> dict[str, str]:
        return dict(self.shows.get(str(show_media_id or ""), {}))


def _adapter(client: _Client, cfg: dict[str, Any] | None = None, **root: Any) -> SimpleNamespace:
    return SimpleNamespace(client=client, cfg={"tracearr": {"history": dict(cfg or {})}, **root})


def test_movie_uses_external_ids_and_stop_time() -> None:
    idx = history.build_index(_adapter(_Client([[_movie()]])))

    assert list(idx) == ["tmdb:157336"]
    item = idx["tmdb:157336"]
    assert item["ids"] == {"imdb": "tt0816692", "tmdb": "157336", "tvdb": "131079", "plex": "45120"}
    assert item["watched_at"] == "2026-09-18T20:25:22Z"


def test_episode_keys_on_show_ids_not_episode_ids() -> None:
    idx = history.build_index(_adapter(_Client([[_episode()]])))

    assert list(idx) == ["tmdb:103516#s04e09"]
    item = idx["tmdb:103516#s04e09"]
    assert item["show_ids"] == SHOW_IDS
    assert item["ids"]["plex"] == "43892"
    assert "7193803" not in item["ids"].values()
    assert item["series_title"] == "Star Trek: Strange New Worlds"


def test_partial_plays_are_skipped_by_default() -> None:
    idx = history.build_index(_adapter(_Client([[_movie(watched=False, percent_complete=95)]])))

    assert idx == {}


def test_watched_only_disabled_keeps_partial_plays() -> None:
    idx = history.build_index(_adapter(_Client([[_movie(watched=False, percent_complete=10)]]), {"watched_only": False}))

    assert list(idx) == ["tmdb:157336"]


@pytest.mark.parametrize(("percent", "kept"), [(84, False), (85, True)])
def test_default_threshold_is_the_fallback_when_watched_is_absent(percent: int, kept: bool) -> None:
    row = _movie(percent_complete=percent)
    row.pop("watched")

    idx = history.build_index(_adapter(_Client([[row]])))

    assert bool(idx) is kept


def test_episode_threshold_is_configurable_separately() -> None:
    row = _episode(percent_complete=70)
    row.pop("watched")

    assert history.build_index(_adapter(_Client([[row]]))) == {}
    assert history.build_index(_adapter(_Client([[row]]), {"min_percent_episode": 70}))


def test_tracks_and_live_rows_are_ignored() -> None:
    idx = history.build_index(_adapter(_Client([[_movie(media_type="track"), _movie(media_type="live")]])))

    assert idx == {}


def test_cursor_pagination_reads_every_page_and_forwards_filters() -> None:
    client = _Client([[_movie()], [_movie(id="m2", tmdb_id=550, imdb_id="tt0137523", tvdb_id=None, rating_key="1")]])

    idx = history.build_index(_adapter(client, {"user_id": "u-1", "server_id": SERVER, "per_page": 500}))

    assert sorted(idx) == ["tmdb:157336", "tmdb:550"]
    assert [c.get("cursor") for c in client.calls] == [None, "1"]
    assert all(c["pageSize"] == 100 and c["user_id"] == "u-1" and c["server_id"] == SERVER for c in client.calls)


def test_latest_play_wins_without_rewatches() -> None:
    older = _movie(id="m0", stopped_at="2025-01-01T10:00:00Z")

    idx = history.build_index(_adapter(_Client([[_movie(), older]])))

    assert idx["tmdb:157336"]["watched_at"] == "2026-09-18T20:25:22Z"


def test_rewatch_mode_keeps_each_play() -> None:
    older = _movie(id="m0", stopped_at="2025-01-01T10:00:00Z")

    idx = history.build_index(_adapter(_Client([[_movie(), older]]), _cw_history_rewatches=True))

    assert len(idx) == 2
    assert all(item.get("_cw_rewatch_sync") is True for item in idx.values())


def test_unlinked_episode_borrows_show_ids_from_same_title_on_same_server() -> None:
    unlinked = _episode(id="e2", episode_number=8, show_media_id=None, grandparent_rating_key=None,
                        imdb_id=None, tmdb_id=None, tvdb_id=None)

    idx = history.build_index(_adapter(_Client([[unlinked, _episode()]])))

    assert sorted(idx) == ["tmdb:103516#s04e08", "tmdb:103516#s04e09"]


def test_unlinked_episode_is_skipped_when_the_title_is_ambiguous() -> None:
    other = "2f7a0c0e-0000-4000-8000-000000000000"
    twin = _episode(id="e3", show_media_id=other, season_number=1, episode_number=1)
    unlinked = _episode(id="e2", episode_number=8, show_media_id=None, grandparent_rating_key=None)
    client = _Client([[unlinked, _episode(), twin]], {SHOW_MEDIA_ID: SHOW_IDS, other: {"tmdb": "999"}})

    idx = history.build_index(_adapter(client))

    assert "tmdb:103516#s04e08" not in idx
    assert not any(k.endswith("#s04e08") for k in idx)


def test_unlinked_episode_does_not_borrow_across_servers() -> None:
    unlinked = _episode(id="e2", server_id="other-server", episode_number=8, show_media_id=None, grandparent_rating_key=None)

    idx = history.build_index(_adapter(_Client([[unlinked, _episode()]])))

    assert sorted(idx) == ["tmdb:103516#s04e09"]


def test_unlinked_episode_falls_back_to_the_local_show_key() -> None:
    unlinked = _episode(show_media_id=None, show_title="Unknown Show")

    idx = history.build_index(_adapter(_Client([[unlinked]])))

    assert list(idx) == ["plex:43892#s04e09"]
    assert "show_ids" not in idx["plex:43892#s04e09"]


def test_jellyfin_rows_use_the_jellyfin_local_key() -> None:
    row = _movie(server_type="jellyfin", imdb_id=None, tmdb_id=None, tvdb_id=None, rating_key="abc123")

    idx = history.build_index(_adapter(_Client([[row]])))

    assert list(idx.values())[0]["ids"] == {"jellyfin": "abc123"}


def test_writes_are_read_only_noops() -> None:
    assert history.add(None, [{"type": "movie"}])["reason"] == "read_only"
    assert history.remove(None, [{"type": "movie"}])["reason"] == "read_only"


class _Resp:
    def __init__(self, status: int, body: Any = None):
        self.status_code = status
        self._body = body

    def json(self) -> Any:
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (200, {"data": [], "meta": {"nextCursor": None}}, (True, "")),
        (401, None, (False, "invalid_api_key")),
        (404, None, (False, "api_v2_unavailable")),
        (500, None, (False, "validation_http_500")),
        (200, ValueError("bad"), (False, "validation_bad_response")),
    ],
)
def test_auth_validation_maps_status_codes(monkeypatch: pytest.MonkeyPatch, status: int, body: Any, expected: tuple[bool, str]) -> None:
    from providers.auth import _auth_TRACEARR as auth

    seen: dict[str, Any] = {}

    def fake_get(url: str, **kwargs: Any) -> _Resp:
        seen.update(url=url, **kwargs)
        return _Resp(status, body)

    monkeypatch.setattr(auth.requests, "get", fake_get)

    assert auth.validate_credentials("tracearr:3000/", "trr_pub_x") == expected
    assert seen["url"] == "http://tracearr:3000/api/v2/public/users"
    assert seen["headers"]["Authorization"] == "Bearer trr_pub_x"


def test_auth_lists_users_across_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_TRACEARR as auth

    pages = {
        None: {"data": [{"id": "b", "username": "Zed", "accounts": [{"server_type": "plex"}]}], "meta": {"nextCursor": "c1"}},
        "c1": {"data": [{"id": "a", "username": "amy", "accounts": [{"server_type": "jellyfin"}, {"server_type": "plex"}]}], "meta": {"nextCursor": None}},
    }
    monkeypatch.setattr(auth.requests, "get", lambda url, params, **_: _Resp(200, pages[params.get("cursor")]))

    users, reason = auth.list_users("http://t", "k")

    assert reason == ""
    assert users == [
        {"id": "a", "username": "amy", "servers": ["jellyfin", "plex"]},
        {"id": "b", "username": "Zed", "servers": ["plex"]},
    ]


def test_module_client_sends_bearer_and_caches_show_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync import _mod_TRACEARR as mod

    monkeypatch.setattr(mod, "_SHOW_IDS_CACHE", {})
    monkeypatch.setattr(mod._LIMITER, "wait", lambda key: 0.0)
    calls: list[str] = []

    def fake_request(session: Any, method: str, url: str, **kwargs: Any) -> Any:
        calls.append(url)
        assert session.headers["Authorization"] == "Bearer k"
        return SimpleNamespace(status_code=200, json=lambda: {"imdb_id": "tt1", "tmdb_id": 5, "tvdb_id": None})

    monkeypatch.setattr(mod, "request_with_retries", fake_request)
    monkeypatch.setattr(mod, "safe_json", lambda r: r.json())
    client = mod.TRACEARRModule({"tracearr": {"server_url": "http://t:3000", "api_key": "k"}}).client

    assert client.show_ids("s1") == {"imdb": "tt1", "tmdb": "5"}
    assert client.show_ids("s1") == {"imdb": "tt1", "tmdb": "5"}
    assert calls == ["http://t:3000/api/v2/public/media/s1"]
