from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from providers.sync.jellyfin import _collection as collection


class Resp:
    def __init__(self, status: int, payload: Any) -> None:
        self.status_code = status
        self._payload = payload

    def json(self) -> Any:
        return self._payload


class FakeJellyfinClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, path: str, params: dict[str, Any] | None = None) -> Resp:
        params = dict(params or {})
        self.calls.append((path, params))
        if path == "/UserViews":
            return Resp(
                200,
                {
                    "Items": [
                        {"Id": "lib-tv", "Name": "TV", "CollectionType": "tvshows"},
                        {"Id": "lib-movies", "Name": "Movies", "CollectionType": "movies"},
                    ]
                },
            )
        if path == "/Items" and params.get("Ids") == "show-1":
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
        if path == "/Items":
            return Resp(
                200,
                {
                    "Items": [
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
                        }
                    ],
                    "TotalRecordCount": 1,
                },
            )
        return Resp(404, {})


def test_collection_resolves_library_id_like_history() -> None:
    adapter = SimpleNamespace(
        client=FakeJellyfinClient(),
        cfg=SimpleNamespace(user_id="user-1"),
    )

    index = collection.build_index(adapter)

    assert sorted(index) == ["tmdb:12345#s01e01"]
    item = index["tmdb:12345#s01e01"]
    assert item["type"] == "episode"
    assert item["library_id"] == "lib-tv"
    assert item["show_ids"] == {"tmdb": "12345", "tvdb": "67890"}
    assert item["season"] == 1
    assert item["episode"] == 1
