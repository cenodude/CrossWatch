# CrossWatch test scripts
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

import pytest

SERVER = "http://silo.local:8090"
CFG = {"silo": {"server_url": SERVER, "access_token": "a", "refresh_token": "r", "profile_id": "p-1", "user_id": "1"}, "_cw_provider_instance": "default"}


@dataclass
class ResponseStub:
    status_code: int = 200
    payload: Any = None
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def text(self) -> str:
        return "" if self.payload is None else "x"

    def json(self) -> Any:
        return self.payload


def _page(items: list[dict[str, Any]]) -> ResponseStub:
    return ResponseStub(200, {"items": items, "page": {"has_more": False}})


COLLECTIONS = [
    {"id": "c1", "name": "Mine", "collection_type": "manual", "creator_profile_id": "p-1", "item_count": 2},
    {"id": "c2", "name": "Smart one", "collection_type": "smart", "creator_profile_id": "p-1", "item_count": 9},
    {"id": "c3", "name": "Kid list", "collection_type": "manual", "creator_profile_id": "p-kid", "item_count": 1},
]


class FakeServer:
    def __init__(self) -> None:
        self.routes: dict[tuple[str, str], Callable[[dict[str, Any]], ResponseStub]] = {}
        self.calls: list[dict[str, Any]] = []
        self.members = [{"media_item_id": "series-tvdb-300", "position": 1}, {"media_item_id": "movie-tmdb-100", "position": 0}]
        self.favorites = [{"content_id": "movie-tmdb-100"}]
        catalog = {
            "movie": [{"content_id": "movie-tmdb-100", "title": "Alpha", "year": 2024}, {"content_id": "movie-tmdb-200", "title": "Beta", "year": 2021}],
            "series": [{"content_id": "series-tvdb-300", "title": "Gamma", "year": 2020}],
            "episode": [],
        }
        self.on("GET", "/catalog", lambda kw: _page(catalog[kw["params"]["type"]]))
        self.on("GET", "/collections", lambda kw: ResponseStub(200, {"groups": [], "items": COLLECTIONS, "page": {"has_more": False}}))
        for row in COLLECTIONS:
            self.on("GET", f"/collections/{row['id']}", lambda kw, row=row: ResponseStub(200, row, {"ETag": f'"{row["id"]}-v1"'}))
        self.on("GET", "/collections/c1/items", lambda kw: _page(list(self.members)))
        self.on("GET", "/favorites", lambda kw: _page(list(self.favorites)))

    def on(self, method: str, path: str, handler: Callable[[dict[str, Any]], ResponseStub]) -> None:
        self.routes[(method, path)] = handler

    def __call__(self, session: Any, method: str, url: str, **kwargs: Any) -> ResponseStub:
        path = url.split("/api/v2", 1)[1]
        self.calls.append({"method": method, "path": path, **kwargs})
        handler = self.routes.get((method, path))
        if handler is None:
            raise AssertionError(f"unexpected {method} {path}")
        return handler(kwargs)


@pytest.fixture()
def server(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> FakeServer:
    from providers.sync.silo import _common

    fake = FakeServer()
    monkeypatch.setattr(_common, "request_with_auth", fake)
    monkeypatch.setattr(_common, "STATE_DIR", tmp_path)
    _common._LIBRARIES.clear()
    yield fake
    _common._LIBRARIES.clear()


def _ops() -> Any:
    from providers.sync._mod_SILO import OPS

    return OPS


def test_resources_list_favorites_own_shared_and_smart(server: FakeServer) -> None:
    from cw_platform.playlists import playlist_capabilities, supports_playlists

    assert supports_playlists(_ops())
    assert playlist_capabilities(_ops())["unordered_endpoint_types"] == ["favorites"]
    res = {r.id: r for r in _ops().list_playlist_resources(CFG, instance="default")}

    assert list(res)[0] == "favorites" and res["favorites"].writable and not res["favorites"].can_reorder
    assert res["collection:c1"].writable and res["collection:c1"].can_reorder
    assert res["collection:c2"].is_smart and not res["collection:c2"].writable
    assert not res["collection:c3"].writable and res["collection:c3"].extra["shared"] is True


def test_snapshot_orders_collection_by_position_and_maps_ids(server: FakeServer) -> None:
    snap = _ops().get_playlist_snapshot(CFG, "collection:c1", instance="default")

    assert snap.ordered_keys() == ["tmdb:100", "tvdb:300"]
    assert snap.items[0].item["title"] == "Alpha" and snap.items[0].position == 0
    fav = _ops().get_playlist_snapshot(CFG, "favorites", instance="default")
    assert fav.ordered_keys() == ["tmdb:100"] and fav.items[0].position is None


def test_add_appends_new_items_and_skips_existing(server: FakeServer) -> None:
    sent: list[Any] = []
    server.on("PUT", "/collections/c1/items/movie-tmdb-200", lambda kw: sent.append(kw["json"]) or ResponseStub(204, None))

    res = _ops().add_playlist_items(CFG, "collection:c1", [
        {"type": "movie", "ids": {"tmdb": "100"}}, {"type": "movie", "ids": {"tmdb": "200"}},
        {"type": "movie", "ids": {"tmdb": "999"}}, {"type": "episode", "show_ids": {"tvdb": "300"}, "season": 1, "episode": 1},
    ], instance="default")

    assert res["confirmed_keys"] == ["tmdb:100", "tmdb:200"]
    assert sent == [{"position": 2}]
    assert {u["hint"] for u in res["unresolved"]} == {"not_in_library", "unsupported_type"}


def test_remove_and_favorites_writes(server: FakeServer) -> None:
    server.on("DELETE", "/collections/c1/items/series-tvdb-300", lambda kw: ResponseStub(204, None))
    server.on("PUT", "/favorites/movie-tmdb-200", lambda kw: ResponseStub(204, None))
    server.on("DELETE", "/favorites/movie-tmdb-100", lambda kw: ResponseStub(204, None))

    assert _ops().remove_playlist_items(CFG, "collection:c1", [{"type": "show", "ids": {"tvdb": "300"}}], instance="default")["confirmed_keys"] == ["tvdb:300"]
    assert _ops().add_playlist_items(CFG, "favorites", [{"type": "movie", "ids": {"tmdb": "200"}}], instance="default")["confirmed_keys"] == ["tmdb:200"]
    assert _ops().remove_playlist_items(CFG, "favorites", [{"type": "movie", "ids": {"tmdb": "100"}}], instance="default")["confirmed_keys"] == ["tmdb:100"]


def test_read_only_collections_reject_writes(server: FakeServer) -> None:
    from providers.sync.silo._playlists import SiloPlaylistError

    with pytest.raises(SiloPlaylistError, match="read-only"):
        _ops().add_playlist_items(CFG, "collection:c3", [{"type": "movie", "ids": {"tmdb": "100"}}], instance="default")
    with pytest.raises(SiloPlaylistError, match="read-only"):
        _ops().remove_playlist_items(CFG, "collection:c2", [{"type": "movie", "ids": {"tmdb": "100"}}], instance="default")


def test_reorder_rename_create_use_validators(server: FakeServer) -> None:
    got: dict[str, Any] = {}
    server.on("GET", "/collections/c1/items/order", lambda kw: ResponseStub(200, {"ordered_ids": [], "has_more": False}, {"ETag": '"order-v3"'}))
    server.on("PUT", "/collections/c1/items/order", lambda kw: got.update(order=(kw["json"], kw["headers"])) or ResponseStub(200, {"ordered_ids": kw["json"]["ordered_ids"], "has_more": False}))
    server.on("PATCH", "/collections/c1", lambda kw: got.update(rename=(kw["json"], kw["headers"])) or ResponseStub(200, {**COLLECTIONS[0], "name": kw["json"]["name"]}))
    server.on("POST", "/collections", lambda kw: got.update(create=kw["json"]) or ResponseStub(201, {"id": "c9", "name": kw["json"]["name"], "collection_type": "manual", "creator_profile_id": "p-1"}))

    res = _ops().reorder_playlist_items(CFG, "collection:c1", ["tvdb:300", "tmdb:100"], instance="default")
    assert res["reordered"] == 2
    assert got["order"][0] == {"ordered_ids": ["series-tvdb-300", "movie-tmdb-100"]}
    assert got["order"][1]["If-Match"] == '"order-v3"'
    assert _ops().reorder_playlist_items(CFG, "collection:c1", ["tmdb:100", "tvdb:300"], instance="default")["reordered"] == 0

    renamed = _ops().rename_playlist(CFG, "collection:c1", "Renamed", instance="default")
    assert renamed.name == "Renamed" and got["rename"][1]["If-Match"] == '"c1-v1"'

    created = _ops().create_playlist(CFG, "From Trakt", instance="default")
    assert created.id == "collection:c9" and got["create"] == {"name": "From Trakt", "collection_type": "manual"}
    assert _ops().reorder_playlist_items(CFG, "favorites", ["tmdb:100"], instance="default")["unsupported"] is True
