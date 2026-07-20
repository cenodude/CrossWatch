from __future__ import annotations

import importlib
from typing import Any

pl = importlib.import_module("providers.sync.publicmetadb._playlists")


class FakeResp:
    def __init__(self, status: int, payload: Any = None):
        self.status_code = status
        self._payload = payload
        self.text = "x" if payload is not None else ""

    def json(self) -> Any:
        return self._payload


class FakeCfg:
    watchlist_page_size = 2


class FakeClient:
    def __init__(self, responses: dict[tuple[str, str, int | None], Any] | None = None):
        self.responses = responses or {}
        self.calls: list[dict[str, Any]] = []

    def get_json(self, path: str, **kw: Any) -> dict[str, Any]:
        page = None
        params = kw.get("params")
        if isinstance(params, dict):
            page = int(params.get("page") or 1)
        self.calls.append({"method": "GET", "path": path, "params": params})
        return dict(self.responses.get(("GET", path, page), {"items": []}))

    def post_json(self, path: str, **kw: Any) -> dict[str, Any]:
        self.calls.append({"method": "POST_JSON", "path": path, "json": kw.get("json")})
        return dict(self.responses.get(("POST_JSON", path, None), {}))

    def post(self, path: str, **kw: Any) -> FakeResp:
        self.calls.append({"method": "POST", "path": path, "json": kw.get("json")})
        return FakeResp(200, self.responses.get(("POST", path, None), {"item": {"id": "x"}}))

    def delete(self, path: str, **kw: Any) -> FakeResp:
        self.calls.append({"method": "DELETE", "path": path, "json": kw.get("json")})
        return FakeResp(200, self.responses.get(("DELETE", path, None), {}))


class FakeAdapter:
    def __init__(self, client: FakeClient):
        self.cfg = FakeCfg()
        self.client = client
        self.instance_id = "default"
        self.config = {"publicmetadb": {"api_key": "k"}}


def _movie(tmdb: int, title: str = "Movie") -> dict[str, Any]:
    return {"type": "movie", "title": title, "ids": {"tmdb": str(tmdb)}}


def _show(tmdb: int, title: str = "Show") -> dict[str, Any]:
    return {"type": "show", "title": title, "ids": {"tmdb": str(tmdb)}}


def test_list_resources_uses_exact_ids_and_pagination():
    client = FakeClient(
        {
            ("GET", "/api/external/lists", 1): {
                "items": [
                    {"id": "list-a", "name": "Weekend", "is_public": False, "items": 2},
                    {"id": "watch-1", "name": "Watchlist", "type": "watchlist", "is_public": False},
                ],
                "totalPages": 2,
            },
            ("GET", "/api/external/lists", 2): {
                "items": [{"id": "list-b", "name": "Shows", "is_public": True, "item_count": 1}],
                "totalPages": 2,
            },
        }
    )

    resources = pl.list_resources(FakeAdapter(client))
    by_id = {r.id: r for r in resources}
    assert set(by_id) == {"list-a", "watch-1", "list-b"}
    assert by_id["list-a"].name == "Weekend"
    assert by_id["list-a"].can_reorder is False
    assert by_id["list-a"].media_types == ("movie", "show")
    assert by_id["list-a"].extra["raw_id"] == "list-a"
    assert by_id["list-b"].extra["private"] is False


def test_snapshot_reads_movies_and_shows_with_pagination():
    client = FakeClient(
        {
            ("GET", "/api/external/lists", 1): {"items": [{"id": "list-a", "name": "Weekend"}]},
            ("GET", "/api/external/lists/list-a/items", 1): {
                "items": [
                    {"id": "i1", "tmdb_id": 438631, "media_type": "movie", "title": "Dune", "year": 2021},
                    {"id": "i2", "tmdb_id": 95396, "media_type": "tv", "title": "Severance", "year": 2022},
                ],
                "totalPages": 2,
            },
            ("GET", "/api/external/lists/list-a/items", 2): {
                "items": [{"id": "i3", "tmdb_id": None, "media_type": "movie", "title": "Sparse"}],
                "totalPages": 2,
            },
        }
    )

    snap = pl.get_snapshot(FakeAdapter(client), "list-a")
    assert snap.resource.id == "list-a"
    assert snap.ordered_keys() == ["tmdb:438631", "tmdb:95396"]
    assert snap.items[0].playlist_item_id == "i1"
    assert snap.items[0].position is None
    assert snap.items[1].item["type"] == "show"


def test_create_private_list_and_add_payloads_require_tmdb():
    client = FakeClient(
        {
            ("POST_JSON", "/api/external/lists", None): {"item": {"id": "created-1", "name": "Private"}},
        }
    )
    ad = FakeAdapter(client)

    res = pl.create(ad, "Private")
    assert res.id == "created-1"
    create_call = next(c for c in client.calls if c["method"] == "POST_JSON")
    assert create_call["json"] == {"name": "Private", "description": "", "is_public": False, "type": "list"}

    add = pl.add(ad, "created-1", [_movie(438631, "Dune"), _show(95396, "Severance"), {"type": "movie", "ids": {}}])
    assert add["count"] == 2
    assert add["confirmed_keys"] == ["tmdb:438631", "tmdb:95396"]
    assert add["unresolved"][0]["hint"] == "missing_tmdb_id"
    post_calls = [c for c in client.calls if c["method"] == "POST" and c["path"].endswith("/items")]
    assert post_calls[0]["json"] == {"tmdb_id": 438631, "media_type": "movie"}
    assert post_calls[1]["json"] == {"tmdb_id": 95396, "media_type": "tv"}


def test_remove_fetches_selected_list_item_ids():
    client = FakeClient(
        {
            ("GET", "/api/external/lists/list-a/items", 1): {
                "items": [{"id": "a-item", "tmdb_id": 1, "media_type": "movie"}],
            },
            ("GET", "/api/external/lists/list-b/items", 1): {
                "items": [{"id": "b-item", "tmdb_id": 1, "media_type": "movie"}],
            },
        }
    )
    ad = FakeAdapter(client)

    res_a = pl.remove(ad, "list-a", [_movie(1)])
    res_b = pl.remove(ad, "list-b", [_movie(1)])
    assert res_a["count"] == 1 and res_b["count"] == 1
    deletes = [c["path"] for c in client.calls if c["method"] == "DELETE"]
    assert "/api/external/lists/list-a/items/a-item" in deletes
    assert "/api/external/lists/list-b/items/b-item" in deletes


def test_reorder_is_unsupported():
    assert pl.reorder(FakeAdapter(FakeClient()), "list-a", ["tmdb:1"])["unsupported"] is True
