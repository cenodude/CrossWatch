from __future__ import annotations

import json
from typing import Any

import pytest


class _Resp:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.ok = True
        self.status_code = 200
        self.headers = {"content-type": "application/json"}
        self._payload = payload

    def json(self) -> dict[str, Any]:
        return self._payload

    @property
    def text(self) -> str:
        return json.dumps(self._payload)


MOVIE_ROWS = [
    {
        "ratingKey": "11",
        "guid": "plex://movie/5d776b59ad5437001f79c6f8",
        "Guid": [{"id": "imdb://tt0071562"}, {"id": "tmdb://240"}, {"id": "tvdb://780"}],
    },
]
SHOW_ROWS = [
    {"ratingKey": "21", "guid": "plex://show/5d9c08254eefaa001f5d6f1c", "Guid": [{"id": "tmdb://1399"}]},
]


class _Obj:
    def __init__(self, rating_key: str, type_: str, section_id: str) -> None:
        self.ratingKey = rating_key
        self.type = type_
        self.librarySectionID = section_id
        self.title = "stub"


class _Section:
    def __init__(self, key: str, type_: str) -> None:
        self.key = key
        self.type = type_


class _Session:
    headers: dict[str, str] = {}

    def get(self, url, params=None, headers=None, timeout=None):
        sid = url.rsplit("/all", 1)[0].rsplit("/", 1)[-1]
        rows = MOVIE_ROWS if sid == "1" else SHOW_ROWS
        start = int((params or {}).get("X-Plex-Container-Start") or 0)
        return _Resp({"MediaContainer": {"Metadata": rows[start:], "totalSize": len(rows)}})


class _Server:
    _token = "TOK"

    def __init__(self) -> None:
        self._session = _Session()
        self.library = None

    def url(self, path):
        return f"http://pms{path}"

    def fetchItem(self, rating_key):
        table = {
            11: _Obj("11", "movie", "1"),
            21: _Obj("21", "show", "2"),
            215: _Obj("215", "episode", "2"),
        }
        obj = table.get(int(rating_key))
        if obj is None:
            raise LookupError(rating_key)
        return obj

    def search(self, *a, **k):
        raise AssertionError("title fallback must not run under strict id matching")


class _Adapter:
    def __init__(self, srv: _Server) -> None:
        self.client = type("C", (), {"server": srv})()

    def libraries(self, types=()):
        return [_Section("1", "movie"), _Section("2", "show")]


@pytest.fixture
def plex(monkeypatch):
    from providers.sync.plex import _history as h
    from providers.sync.plex import _progress as pr

    srv = _Server()
    adapter = _Adapter(srv)

    monkeypatch.setattr(h, "_as_base_url", lambda _s: "http://pms")
    monkeypatch.setattr(h, "_load_guid_index", lambda *a, **k: False)
    monkeypatch.setattr(h, "_save_guid_index", lambda *a, **k: None)
    h._clear_guid_index()
    h._GUID_INDEX_KEY = None

    monkeypatch.setattr(pr, "server_find_rating_key_by_guid", lambda *a, **k: None)
    monkeypatch.setattr(pr, "plex_feature_library_ids", lambda *a, **k: set())
    monkeypatch.setattr(
        pr,
        "plex_cfg_get",
        lambda _adapter, key, default=None: True if key == "strict_id_matching" else default,
    )
    return pr, adapter


def test_movie_resolves_from_the_guid_index_when_the_server_guid_filter_misses(plex) -> None:
    pr, adapter = plex

    item = {
        "type": "movie",
        "title": "The Godfather Part II",
        "year": 1974,
        "ids": {"tmdb": "240"},
        "progress_ms": 1_800_000,
        "duration_ms": 12_000_000,
    }

    assert pr._resolve_rating_key(adapter, item) == "11"


def test_episode_resolves_through_the_show_guid_index(plex, monkeypatch) -> None:
    pr, adapter = plex
    monkeypatch.setattr(pr, "episode_rating_key_from_show", lambda obj, season, episode: "215")

    item = {
        "type": "episode",
        "series_title": "Game of Thrones",
        "show_ids": {"tmdb": "1399"},
        "season": 1,
        "episode": 2,
        "progress_ms": 600_000,
        "duration_ms": 3_400_000,
    }

    assert pr._resolve_rating_key(adapter, item) == "215"


def test_unknown_id_still_reports_not_found(plex) -> None:
    pr, adapter = plex

    item = {
        "type": "movie",
        "title": "Not In The Library",
        "ids": {"tmdb": "999999"},
        "progress_ms": 60_000,
    }

    assert pr._resolve_rating_key(adapter, item) is None
