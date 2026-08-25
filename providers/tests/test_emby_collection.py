from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from providers.sync.emby import _collection as collection
from providers.sync._mod_EMBY import OPS


class Resp:
    def __init__(self, status: int, payload: Any) -> None:
        self.status_code = status
        self._payload = payload

    def json(self) -> Any:
        return self._payload


class FakeEmbyClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, path: str, params: dict[str, Any] | None = None) -> Resp:
        params = dict(params or {})
        self.calls.append((path, params))
        if path == "/Users/user-1/Views":
            return Resp(
                200,
                {
                    "Items": [
                        {"Id": "lib-tv", "Name": "TV", "CollectionType": "tvshows"},
                        {"Id": "lib-movies", "Name": "Movies", "CollectionType": "movies"},
                    ]
                },
            )
        if path == "/Users/user-1/Items" and params.get("Ids") == "show-1":
            return Resp(
                200,
                {
                    "Items": [
                        {
                            "Id": "show-1",
                            "Name": "The Show",
                            "Type": "Series",
                            "ProviderIds": {"Tmdb": "12345", "Tvdb": "67890"},
                        }
                    ]
                },
            )
        if path == "/Users/user-1/Items":
            return Resp(
                200,
                {
                    "Items": [
                        {
                            "Id": "show-1",
                            "Type": "Series",
                            "Name": "The Show",
                            "AncestorIds": ["lib-tv"],
                            "ProviderIds": {"Tmdb": "12345", "Tvdb": "67890"},
                            "DateCreated": "2026-01-01T03:04:05Z",
                        },
                        {
                            "Id": "season-1",
                            "Type": "Season",
                            "Name": "Season 1",
                            "SeriesName": "The Show",
                            "SeriesId": "show-1",
                            "IndexNumber": 1,
                            "AncestorIds": ["lib-tv", "show-1"],
                            "ProviderIds": {},
                            "DateCreated": "2026-01-01T04:04:05Z",
                        },
                        {
                            "Id": "ep-1",
                            "Type": "Episode",
                            "Name": "Pilot",
                            "SeriesName": "The Show",
                            "SeriesId": "show-1",
                            "ParentIndexNumber": 1,
                            "IndexNumber": 1,
                            "AncestorIds": ["lib-tv", "show-1"],
                            "ProviderIds": {"Tvdb": "111"},
                            "DateCreated": "2026-01-02T03:04:05Z",
                        }
                    ],
                    "TotalRecordCount": 1,
                },
            )
        return Resp(404, {})


def test_collection_resolves_library_id_like_history() -> None:
    adapter = SimpleNamespace(
        client=FakeEmbyClient(),
        cfg=SimpleNamespace(user_id="user-1"),
    )

    index = collection.build_index(adapter)

    item_query = next(params for path, params in adapter.client.calls if path == "/Users/user-1/Items" and params.get("Recursive"))
    assert item_query["IncludeItemTypes"] == "Movie,Series,Season,Episode"
    assert sorted(index) == ["tmdb:12345", "tmdb:12345#s01e01", "tmdb:12345#season:1"]
    show = index["tmdb:12345"]
    assert show["type"] == "show"
    assert show["library_id"] == "lib-tv"
    season = index["tmdb:12345#season:1"]
    assert season["type"] == "season"
    assert season["library_id"] == "lib-tv"
    assert season["show_ids"] == {"tmdb": "12345", "tvdb": "67890"}
    assert season["season"] == 1
    assert season["collected_at"] == "2026-01-01T04:04:05Z"
    item = index["tmdb:12345#s01e01"]
    assert item["type"] == "episode"
    assert item["library_id"] == "lib-tv"
    assert item["show_ids"] == {"tmdb": "12345", "tvdb": "67890"}
    assert item["season"] == 1
    assert item["episode"] == 1
    assert item["collected_at"] == "2026-01-02T03:04:05Z"


def test_collection_capabilities_cover_show_season_episode_scope() -> None:
    assert OPS.capabilities()["collection"]["types"] == {
        "movies": True,
        "shows": True,
        "seasons": True,
        "episodes": True,
    }
