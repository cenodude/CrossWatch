from __future__ import annotations

import json
from typing import Any

import pytest

MOVIE_ROWS = [
    {
        "ratingKey": "11",
        "guid": "plex://movie/5d776b59ad5437001f79c6f8",
        "Guid": [{"id": "imdb://tt0071562"}, {"id": "tmdb://240"}],
    },
]
SHOW_ROWS = [
    {"ratingKey": "21", "guid": "plex://show/5d9c08254eefaa001f5d6f1c", "Guid": [{"id": "tmdb://1399"}]},
]


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


class _Session:
    headers: dict[str, str] = {}

    def get(self, url, params=None, headers=None, timeout=None):
        sid = url.rsplit("/all", 1)[0].rsplit("/", 1)[-1]
        rows = MOVIE_ROWS if sid == "1" else SHOW_ROWS
        start = int((params or {}).get("X-Plex-Container-Start") or 0)
        return _Resp({"MediaContainer": {"Metadata": rows[start:], "totalSize": len(rows)}})


class _Obj:
    def __init__(self, rating_key: str, type_: str, section_id: str, title: str) -> None:
        self.ratingKey = rating_key
        self.type = type_
        self.librarySectionID = section_id
        self.title = title
        self.year = 1974
        self.guid = f"plex://{type_}/{rating_key}"


class _Playlist:
    def __init__(self) -> None:
        self.ratingKey = "10234"
        self.title = "Weekend"
        self.playlistType = "video"
        self.smart = False
        self.added: list[Any] = []

    def items(self):
        return []

    def addItems(self, objs):
        self.added.extend(objs)


class _Section:
    def __init__(self, key: str, type_: str) -> None:
        self.key = key
        self.type = type_


class _Server:
    _token = "TOK"
    machineIdentifier = "MID"

    def __init__(self, playlist: _Playlist) -> None:
        self._session = _Session()
        self._playlist = playlist

    def url(self, path):
        return f"http://pms{path}"

    def playlists(self):
        return [self._playlist]

    def query(self, *_a, **_k):
        return None

    def fetchItem(self, rating_key):
        table = {
            11: _Obj("11", "movie", "1", "The Godfather Part II"),
            21: _Obj("21", "show", "2", "Game of Thrones"),
        }
        obj = table.get(int(rating_key))
        if obj is None:
            raise LookupError(rating_key)
        return obj


class _Adapter:
    def __init__(self, srv: _Server) -> None:
        self.client = type("C", (), {"server": srv})()
        self.instance_id = "default"

    def libraries(self, types=()):
        return [_Section("1", "movie"), _Section("2", "show")]


@pytest.fixture
def plex(monkeypatch, tmp_path):
    from providers.sync.plex import _history as h
    from providers.sync.plex import _playlists as pl

    monkeypatch.setattr(h, "_as_base_url", lambda _s: "http://pms")
    monkeypatch.setattr(h, "_load_guid_index", lambda *a, **k: False)
    monkeypatch.setattr(h, "_save_guid_index", lambda *a, **k: None)
    monkeypatch.setattr(pl, "server_find_rating_key_by_guid", lambda *a, **k: None)
    monkeypatch.setattr(pl, "plex_feature_library_ids", lambda *a, **k: set())
    h._clear_guid_index()
    h._GUID_INDEX_KEY = None

    playlist = _Playlist()
    return pl, _Adapter(_Server(playlist)), playlist


def test_movie_is_added_when_only_the_guid_children_carry_the_external_id(plex) -> None:
    pl, adapter, playlist = plex

    res = pl.add(adapter, "10234", [{"type": "movie", "title": "The Godfather Part II", "ids": {"tmdb": "240"}}])

    assert res["count"] == 1, "the guid index must resolve what the guid query misses"
    assert res["unresolved"] == []
    assert [o.ratingKey for o in playlist.added] == ["11"]


def test_show_against_a_video_playlist_reports_the_type_not_a_missing_item(plex) -> None:
    pl, adapter, playlist = plex

    res = pl.add(adapter, "10234", [{"type": "show", "title": "Game of Thrones", "ids": {"tmdb": "1399"}}])

    assert res["count"] == 0
    assert [u["hint"] for u in res["unresolved"]] == ["unsupported_type"]
    assert res["unresolved"][0]["matched_type"] == "show"
    assert any("wrong type" in w for w in res["warnings"])
    assert playlist.added == []


def test_item_that_is_not_in_the_library_is_reported_as_such(plex) -> None:
    pl, adapter, _playlist = plex

    res = pl.add(adapter, "10234", [{"type": "movie", "title": "Nope", "ids": {"tmdb": "999999"}}])

    assert res["count"] == 0
    assert [u["hint"] for u in res["unresolved"]] == ["not_in_library"]


def test_create_without_items_is_refused_with_a_typed_error(plex) -> None:
    pl, adapter, _playlist = plex

    with pytest.raises(pl.PlaylistItemsRequired):
        pl.create(adapter, "Weekend", items=[])


def test_create_seeds_the_playlist_with_the_resolved_items(plex, monkeypatch) -> None:
    pl, adapter, _playlist = plex
    created: dict[str, Any] = {}

    def _create(title, items=None):
        created["title"] = title
        created["items"] = list(items or [])
        return _Playlist()

    monkeypatch.setattr(adapter.client.server, "createPlaylist", _create, raising=False)

    res = pl.create(adapter, "Weekend", items=[{"type": "movie", "title": "x", "ids": {"tmdb": "240"}}])

    assert created["title"] == "Weekend"
    assert [o.ratingKey for o in created["items"]] == ["11"]
    assert res.id == "10234"


def test_collections_cannot_be_created_from_crosswatch(plex) -> None:
    pl, adapter, _playlist = plex

    with pytest.raises(pl.PlaylistUnsupported):
        pl.create(adapter, "Marvel", media_type="collection", items=[{"ids": {"tmdb": "240"}}])
