from __future__ import annotations

import importlib
from typing import Any

import pytest

pl = importlib.import_module("providers.sync.trakt._playlists")


class FakeResp:
    def __init__(self, status: int, payload: Any = None, headers: dict[str, str] | None = None):
        self.status_code = status
        self._payload = payload
        self.headers = headers or {}
        self.text = "x" if payload is not None else ""

    def json(self) -> Any:
        return self._payload


class FakeCfg:
    client_id = "cid"
    access_token = "tok"
    timeout = 15.0
    max_retries = 1


class FakeSession:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}


class FakeClient:
    def __init__(self, settings: Any) -> None:
        self.session = FakeSession()
        self._settings = settings

    def get(self, url: str, **kw: Any) -> FakeResp:
        return FakeResp(200, self._settings)


class FakeAdapter:
    def __init__(self, settings: Any = None) -> None:
        self.cfg = FakeCfg()
        self.client = FakeClient(settings or {"limits": {"list": {"count": 30, "item_count": 100}}})
        self.instance_id = "default"


LIST_ROW = {"name": "Weekend", "ids": {"trakt": 123, "slug": "weekend"}}
ITEM_ROWS = [
    {"id": 9001, "rank": 1, "type": "movie", "movie": {"title": "Dune", "year": 2021, "ids": {"trakt": 1, "tmdb": 438631, "slug": "dune-2021"}}},
    {"id": 9002, "rank": 2, "type": "show", "show": {"title": "Severance", "year": 2022, "ids": {"trakt": 2, "tmdb": 95396, "slug": "severance"}}},
    {"id": 9003, "rank": 3, "type": "person", "person": {"name": "Nobody", "ids": {"trakt": 3}}},
]


def _router(monkeypatch, handlers):
    captured: list[dict[str, Any]] = []

    def fake_rwr(sess, method, url, **kw):
        captured.append({"method": method, "url": url, "json": kw.get("json"), "params": kw.get("params")})
        for (m, needle), resp in handlers.items():
            if method == m and needle in url:
                return resp() if callable(resp) else resp
        return FakeResp(404, None)

    monkeypatch.setattr(pl, "request_with_retries", fake_rwr)
    pl._SETTINGS_MEMO = (0.0, None)
    return captured


def test_list_resources_and_normalization(monkeypatch):
    _router(monkeypatch, {("GET", "/users/me/lists"): FakeResp(200, [LIST_ROW])})
    ad = FakeAdapter()
    res = pl.list_resources(ad)
    by_id = {r.id: r for r in res}
    watchlist = by_id[pl.WATCHLIST_ID]
    assert watchlist.name == "Watchlist"
    assert watchlist.can_add and watchlist.can_remove
    assert watchlist.can_reorder is False
    r = by_id["123"]
    assert r.provider == "TRAKT"
    assert r.id == "123"
    assert r.name == "Weekend"
    assert r.can_add and r.can_remove and r.can_reorder
    assert r.is_smart is False
    discovery = by_id["discovery:trakt:movies:trending"]
    assert discovery.name == "Trending Movies"
    assert discovery.is_smart is True
    assert discovery.is_discovery is True
    assert discovery.can_read is True
    assert discovery.can_add is False and discovery.can_remove is False
    assert discovery.media_types == ("movie",)


def test_discovery_snapshot_reads_wrapped_and_direct_rows(monkeypatch):
    rows = [
        {"watchers": 12, "movie": {"title": "Dune", "year": 2021, "ids": {"trakt": 1, "tmdb": 438631}}},
        {"title": "Arrival", "year": 2016, "ids": {"trakt": 2, "tmdb": 329865}},
    ]
    captured = _router(
        monkeypatch,
        {("GET", "/movies/trending"): FakeResp(200, rows, headers={"X-Pagination-Page-Count": "1"})},
    )
    snap = pl.get_snapshot(FakeAdapter(), "discovery:trakt:movies:trending")

    assert snap.resource.is_discovery is True
    assert snap.resource.writable is False
    assert snap.ordered_keys() == ["tmdb:438631", "tmdb:329865"]
    call = captured[0]
    assert call["params"]["extended"] == "full"
    assert call["params"]["limit"] == 100


def test_discovery_feeds_are_read_only(monkeypatch):
    _router(monkeypatch, {})
    with pytest.raises(RuntimeError):
        pl.add(FakeAdapter(), "discovery:trakt:movies:popular", [{"type": "movie", "ids": {"tmdb": "1"}}])
    assert pl.reorder(FakeAdapter(), "discovery:trakt:movies:popular", ["tmdb:1"])["unsupported"] is True


def test_watchlist_snapshot_and_writes_delegate(monkeypatch):
    monkeypatch.setattr(
        pl.feat_watchlist,
        "build_index",
        lambda ad: {"tmdb:438631": {"type": "movie", "title": "Dune", "ids": {"tmdb": "438631"}}},
    )
    monkeypatch.setattr(pl.feat_watchlist, "add", lambda ad, items: (len(list(items)), []))
    monkeypatch.setattr(pl.feat_watchlist, "remove", lambda ad, items: (len(list(items)), []))

    snap = pl.get_snapshot(FakeAdapter(), pl.WATCHLIST_ID)
    assert snap.resource.name == "Watchlist"
    assert snap.ordered_keys() == ["tmdb:438631"]

    item = {"type": "movie", "title": "Dune", "ids": {"tmdb": "438631"}}
    add_res = pl.add(FakeAdapter(), pl.WATCHLIST_ID, [item])
    rm_res = pl.remove(FakeAdapter(), pl.WATCHLIST_ID, [item])
    assert add_res["count"] == 1 and add_res["confirmed_keys"] == ["tmdb:438631"]
    assert rm_res["count"] == 1 and rm_res["confirmed_keys"] == ["tmdb:438631"]
    assert pl.reorder(FakeAdapter(), pl.WATCHLIST_ID, ["tmdb:438631"])["unsupported"] is True


def test_snapshot_drops_unsupported_and_maps_keys(monkeypatch):
    def items_resp():
        return FakeResp(200, ITEM_ROWS, headers={"X-Pagination-Page-Count": "1"})

    _router(
        monkeypatch,
        {
            ("GET", "/users/me/lists/123/items"): items_resp,
            ("GET", "/users/me/lists"): FakeResp(200, [LIST_ROW]),
        },
    )
    snap = pl.get_snapshot(FakeAdapter(), "123")
    keys = snap.ordered_keys()
    assert keys == ["tmdb:438631", "tmdb:95396"]
    first = snap.items[0]
    assert first.playlist_item_id == "9001"
    assert first.position == 1


def test_add_and_remove_mixed_media(monkeypatch):
    captured = _router(
        monkeypatch,
        {
            ("GET", "/users/me/lists/123/items"): FakeResp(200, [], headers={"X-Pagination-Page-Count": "1"}),
            ("GET", "/users/me/lists"): FakeResp(200, [LIST_ROW]),
            ("POST", "/items/remove"): FakeResp(200, {"deleted": {"movies": 1}}),
            ("POST", "/users/me/lists/123/items"): FakeResp(200, {"added": {"movies": 1, "shows": 1}, "not_found": {}}),
        },
    )
    items = [
        {"type": "movie", "title": "Dune", "year": 2021, "ids": {"tmdb": "438631"}},
        {"type": "show", "title": "Severance", "year": 2022, "ids": {"tmdb": "95396"}},
    ]
    add_res = pl.add(FakeAdapter(), "123", items)
    assert add_res["ok"] is True
    assert add_res["count"] == 2
    assert set(add_res["confirmed_keys"]) == {"tmdb:438631", "tmdb:95396"}

    rm_res = pl.remove(FakeAdapter(), "123", items[:1])
    assert rm_res["count"] == 1
    assert any("/items/remove" in c["url"] for c in captured)


def test_reorder_uses_list_item_ids(monkeypatch):
    captured = _router(
        monkeypatch,
        {
            ("GET", "/users/me/lists/123/items"): FakeResp(200, ITEM_ROWS, headers={"X-Pagination-Page-Count": "1"}),
            ("GET", "/users/me/lists"): FakeResp(200, [LIST_ROW]),
            ("POST", "/items/reorder"): FakeResp(200, {"updated": 2}),
        },
    )
    res = pl.reorder(FakeAdapter(), "123", ["tmdb:95396", "tmdb:438631"])
    assert res["ok"] is True
    reorder_call = next(c for c in captured if "/items/reorder" in c["url"])
    assert reorder_call["json"] == {"rank": ["9002", "9001"]}


def test_add_http_420_is_capacity_failure(monkeypatch):
    _router(
        monkeypatch,
        {
            ("GET", "/users/me/lists/123/items"): FakeResp(200, [], headers={"X-Pagination-Page-Count": "1"}),
            ("GET", "/users/me/lists"): FakeResp(200, [LIST_ROW]),
            ("POST", "/users/me/lists/123/items"): FakeResp(420, {}, headers={"X-Upgrade-URL": "https://trakt.tv/vip"}),
        },
    )
    res = pl.add(FakeAdapter(), "123", [{"type": "movie", "title": "Dune", "ids": {"tmdb": "438631"}}])
    assert res.get("capacity") == "list_item_count"
    assert any(u.get("hint") == "trakt_limit" for u in res["unresolved"])


def test_create_respects_list_count_limit(monkeypatch):
    _router(monkeypatch, {("GET", "/users/me/lists"): FakeResp(200, [LIST_ROW])})
    ad = FakeAdapter(settings={"limits": {"list": {"count": 1, "item_count": 100}}})
    with pytest.raises(pl.TraktCapacityError):
        pl.create(ad, "New List")


def test_rename_and_delete_user_list(monkeypatch):
    captured = _router(
        monkeypatch,
        {
            ("PUT", "/users/me/lists/123"): FakeResp(200, {"name": "Renamed", "ids": {"trakt": 123}}),
            ("DELETE", "/users/me/lists/123"): FakeResp(204),
        },
    )
    ad = FakeAdapter()

    renamed = pl.rename(ad, "123", "Renamed")
    deleted = pl.delete(ad, "123")

    assert renamed.id == "123"
    assert renamed.name == "Renamed"
    assert deleted["ok"] is True
    assert any(c["method"] == "PUT" and c["json"] == {"name": "Renamed"} for c in captured)
    assert any(c["method"] == "DELETE" and c["url"].endswith("/users/me/lists/123") for c in captured)


def test_built_in_lists_cannot_be_renamed_or_deleted(monkeypatch):
    _router(monkeypatch, {})
    with pytest.raises(RuntimeError):
        pl.rename(FakeAdapter(), pl.WATCHLIST_ID, "Nope")
    with pytest.raises(RuntimeError):
        pl.delete(FakeAdapter(), "discovery:trakt:movies:popular")
