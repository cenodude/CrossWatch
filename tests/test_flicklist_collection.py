# tests/test_flicklist_collection.py
# CrossWatch - FlickList collection sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from typing import Any

import pytest

from providers.sync.flicklist import _collection as coll

OWN = "crosswatch_923f4294"


class _Resp:
    def __init__(self, status_code: int = 200, payload: Any = None) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload)
        self.headers: dict[str, str] = {}

    def json(self) -> Any:
        return self._payload


class _Adapter:
    config = {"flicklist": {}}
    instance_id = "default"


def _meta(fmt: str, **extra: Any) -> dict[str, Any]:
    return {"media_type": fmt, "resolution": None, "hdr": None, "audio": None, "audio_channels": None, "3d": None, **extra}


def _copy(fmt: str, source: str = OWN, **extra: Any) -> dict[str, Any]:
    return {"id": 1, "metadata": _meta(fmt, **extra), "edition": None, "source": source, "collected_at": "2024-01-01T10:00:00.000Z"}


MOVIES = [
    {
        "collected_at": "2020-05-01T10:00:00.000Z",
        "movie": {"title": "The Dark Knight", "year": 2008, "ids": {"fldb": "flm_1tk", "slug": "the-dark-knight-2008", "tmdb": 155, "imdb": "tt0468569", "tvdb": None, "anilist": None}},
        "metadata": _meta("digital", resolution="uhd_4k", hdr="dolby_vision", audio="dolby_atmos", audio_channels="7.1"),
        "copies": [_copy("digital", resolution="uhd_4k"), _copy("bluray", source="web")],
    }
]
SHOWS = [
    {
        "collected_at": "2021-01-01T00:00:00.000Z",
        "show": {"title": "Breaking Bad", "year": 2008, "ids": {"fldb": "flt_5b", "slug": "breaking-bad-2008", "tmdb": 1396, "imdb": "tt0903747", "tvdb": 81189, "anilist": None}},
        "scope": "partial",
        "metadata": None,
        "copies": [],
        "seasons": [
            {
                "number": 1,
                "scope": "partial",
                "metadata": None,
                "copies": [],
                "episodes": [{"number": 1, "title": "Pilot", "collected_at": "2021-01-01T00:00:00.000Z", "metadata": _meta("digital", resolution="hd_1080p", **{"3d": True}), "copies": [_copy("digital")]}],
            },
            {"number": 2, "scope": "season", "metadata": _meta("bluray", resolution="hd_1080p"), "copies": [_copy("bluray")], "episodes": []},
        ],
    },
    {
        "collected_at": "2026-10-20T23:24:06.572Z",
        "show": {"title": "Game of Thrones", "year": 2011, "ids": {"fldb": "flt_6s", "tmdb": 1399, "imdb": "tt0944947", "tvdb": 121361}},
        "scope": "show",
        "metadata": _meta("4k_bluray", resolution="uhd_4k", hdr="hdr10"),
        "copies": [_copy("4k_bluray", source="web")],
        "seasons": [],
    },
]


def _reads(monkeypatch: pytest.MonkeyPatch, calls: list[dict[str, Any]], writes: list[_Resp] | None = None) -> None:
    pending = list(writes or [])

    def fake(adapter: Any, method: str, url: str, **kwargs: Any) -> _Resp:
        calls.append({"method": method, "url": url, **kwargs})
        if method == "GET":
            return _Resp(200, MOVIES if url.endswith("/movies") else SHOWS)
        return pending.pop(0) if pending else _Resp(200, {"added": 0, "updated": 0, "removed": 0, "not_found": []})

    monkeypatch.setattr(coll, "flicklist_request", fake)


def test_index_reads_movies_owned_shows_seasons_and_episodes(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    _reads(monkeypatch, calls)

    idx = coll.build_index(_Adapter())

    assert sorted(idx) == ["tmdb:1396#s01e01", "tmdb:1396#season:2", "tmdb:1399#show", "tmdb:155"]
    movie = idx["tmdb:155"]
    assert movie["format"] == "digital"
    assert movie["media_info"] == {"resolution": "uhd_4k", "hdr": "dolby_vision", "audio": "dolby_atmos", "audio_channels": "7.1"}
    assert movie["collected_at"] == "2020-05-01T10:00:00.000Z"
    assert movie["_flicklist_fldb"] == "flm_1tk"
    assert movie["_flicklist_copies"] == [{"format": "digital", "source": OWN}, {"format": "bluray", "source": "web"}]
    episode = idx["tmdb:1396#s01e01"]
    assert episode["show_ids"]["tvdb"] == "81189"
    assert episode["media_info"] == {"resolution": "hd_1080p", "is_3d": True}
    assert idx["tmdb:1396#season:2"]["format"] == "bluray"
    assert idx["tmdb:1396#season:2"]["collected_at"] == "2024-01-01T10:00:00.000Z"
    assert idx["tmdb:1399#show"]["format"] == "4k_bluray"
    assert [c["url"].rsplit("/", 1)[-1] for c in calls] == ["movies", "shows"]


def test_index_skips_fldb_only_rows_and_keeps_slug_scoped_seasons(monkeypatch: pytest.MonkeyPatch) -> None:
    movies = [{"movie": {"title": "Local Only", "year": 2020, "ids": {"fldb": "flm_x"}}, "metadata": _meta("digital"), "copies": [_copy("digital")]}]
    seasons = [{"number": n, "scope": "season", "metadata": _meta("bluray"), "copies": [_copy("bluray")], "episodes": [{"number": 1, "metadata": _meta("digital"), "copies": [_copy("digital")]}]} for n in (1, 2)]
    shows = [
        {"show": {"title": "Fldb Show", "year": 2020, "ids": {"fldb": "flt_x"}}, "scope": "partial", "seasons": seasons},
        {"show": {"title": "Slug Show", "year": 2020, "ids": {"fldb": "flt_y", "slug": "slug-show-2020"}}, "scope": "partial", "seasons": seasons},
    ]
    monkeypatch.setattr(coll, "flicklist_request", lambda adapter, method, url, **kwargs: _Resp(200, movies if url.endswith("/movies") else shows))

    idx = coll.build_index(_Adapter())

    assert sorted(idx) == ["slug:slug-show-2020#s01e01", "slug:slug-show-2020#s02e01", "slug:slug-show-2020#season:1", "slug:slug-show-2020#season:2"]


def test_season_not_found_is_matched_by_season(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    _reads(monkeypatch, calls, [_Resp(200, {"added": 2, "updated": 0, "not_found": [{"ids": {"tmdb": 1396}, "season": 2}]})])
    items = [
        {"type": "show", "ids": {"tmdb": "1396"}},
        {"type": "season", "show_ids": {"tmdb": "1396"}, "season": 1},
        {"type": "season", "show_ids": {"tmdb": "1396"}, "season": 2},
    ]

    res = coll.add(_Adapter(), items)

    assert res["confirmed_keys"] == ["tmdb:1396#show", "tmdb:1396#season:1"]
    assert res["unresolved_keys"] == ["tmdb:1396#season:2"]


def test_index_raises_when_a_read_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(coll, "flicklist_request", lambda *args, **kwargs: _Resp(503, {"error": "unavailable"}))
    with pytest.raises(coll.FlickListError):
        coll.build_index(_Adapter())


def test_add_sends_media_info_dates_and_container_scopes(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    _reads(monkeypatch, calls, [_Resp(200, {"added": 4, "updated": 0, "not_found": []})])
    items = [
        {"type": "movie", "ids": {"tmdb": "603"}, "format": "Blu-ray", "collected_at": "2020-01-01T00:00:00Z", "media_info": {"resolution": "uhd_4k", "audio": "TrueHD Atmos", "audio_channels": "7.1"}},
        {"type": "episode", "ids": {"tmdb": "62085"}, "show_ids": {"tmdb": "1396", "tvdb": "81189"}, "season": 1, "episode": 2, "media_info": {"resolution": "1440p"}},
        {"type": "season", "show_ids": {"tmdb": "1396"}, "season": 2},
        {"type": "show", "ids": {"tmdb": "1399"}},
    ]

    res = coll.add(_Adapter(), items)

    assert res["ok"] is True
    assert res["count"] == 4
    body = calls[0]["json"]["items"]
    assert body[0] == {"ids": {"tmdb": 603}, "media_type": "movie", "format": "4k_bluray", "resolution": "uhd_4k", "audio": "dolby_atmos", "audio_channels": "7.1", "collected_at": "2020-01-01T00:00:00.000Z"}
    assert body[1] == {"ids": {"tmdb": 1396, "tvdb": 81189}, "media_type": "show", "season": 1, "episode": 2}
    assert body[2] == {"ids": {"tmdb": 1396}, "media_type": "show", "season": 2}
    assert body[3] == {"ids": {"tmdb": 1399}, "media_type": "show"}


def test_media_update_refreshes_without_collected_at(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    _reads(monkeypatch, calls, [_Resp(200, {"added": 0, "updated": 1, "not_found": []})])
    item = {"type": "movie", "ids": {"tmdb": "155"}, "format": "digital", "collected_at": "2020-05-01T10:00:00.000Z", "media_info": {"hdr": "hdr10_plus"}, "_cw_media_update": True}

    res = coll.add(_Adapter(), [item])

    assert res["confirmed_keys"] == ["tmdb:155"]
    assert calls[0]["json"]["items"] == [{"ids": {"tmdb": 155}, "media_type": "movie", "format": "digital", "hdr": "hdr10_plus"}]


def test_rejected_item_is_dropped_and_the_batch_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    bad = _Resp(400, {"error": "bad_request", "detail": "items[1].audio: unknown value 'x'"})
    _reads(monkeypatch, calls, [bad, _Resp(200, {"added": 2, "updated": 0, "not_found": []})])
    items = [{"type": "movie", "ids": {"tmdb": str(n)}} for n in (11, 12, 13)]

    res = coll.add(_Adapter(), items)

    assert res["ok"] is False
    assert res["confirmed_keys"] == ["tmdb:11", "tmdb:13"]
    assert res["unresolved_keys"] == ["tmdb:12"]
    assert [len(c["json"]["items"]) for c in calls] == [3, 2]


def test_not_found_items_stay_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    _reads(monkeypatch, calls, [_Resp(200, {"added": 1, "updated": 0, "not_found": [{"ids": {"tmdb": 999999}}]})])

    res = coll.add(_Adapter(), [{"type": "movie", "ids": {"tmdb": "603"}}, {"type": "movie", "ids": {"tmdb": "999999"}}])

    assert res["confirmed_keys"] == ["tmdb:603"]
    assert res["unresolved_keys"] == ["tmdb:999999"]


def test_remove_deletes_only_crosswatch_copies(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    _reads(monkeypatch, calls, [_Resp(200, {"removed": 2, "not_found": []})])
    items = [
        {"type": "movie", "ids": {"imdb": "tt0468569"}},
        {"type": "episode", "show_ids": {"tvdb": "81189"}, "season": 1, "episode": 1},
        {"type": "show", "ids": {"tmdb": "1399"}},
        {"type": "movie", "ids": {"tmdb": "13"}},
    ]

    res = coll.remove(_Adapter(), items)

    deletes = [c for c in calls if c["method"] == "DELETE"]
    assert len(deletes) == 1
    assert deletes[0]["json"]["items"] == [
        {"ids": {"imdb": "tt0468569"}, "media_type": "movie", "format": "digital"},
        {"ids": {"tvdb": 81189}, "media_type": "show", "season": 1, "episode": 1, "format": "digital"},
    ]
    assert sorted(res["confirmed_keys"]) == ["imdb:tt0468569", "tmdb:13", "tvdb:81189#s01e01"]
    assert res["skipped_keys"] == ["tmdb:1399#show"]


def test_module_routes_collection_feature(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync._mod_FLICKLIST import FLICKLISTModule

    calls: list[dict[str, Any]] = []
    _reads(monkeypatch, calls, [_Resp(200, {"added": 1, "updated": 0, "not_found": []})])
    module = FLICKLISTModule({"flicklist": {"api_key": "fs_live_test"}})

    assert "tmdb:155" in module.build_index("collection")
    res = module.add("collection", [{"type": "movie", "ids": {"tmdb": "603"}}])
    assert res["confirmed_keys"] == ["tmdb:603"]
