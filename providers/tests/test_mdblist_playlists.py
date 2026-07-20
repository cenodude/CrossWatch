from __future__ import annotations

import importlib
from typing import Any

import pytest

pl = importlib.import_module("providers.sync.mdblist._playlists")


class FakeResp:
    def __init__(self, status: int, payload: Any = None, headers: dict[str, str] | None = None):
        self.status_code = status
        self._payload = payload
        self.headers = headers or {}
        self.text = "x" if payload is not None else ""

    def json(self) -> Any:
        return self._payload


class FakeCfg:
    timeout = 15.0
    max_retries = 1


class FakeSession:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}


class FakeClient:
    def __init__(self) -> None:
        self.session = FakeSession()


class FakeAdapter:
    def __init__(self) -> None:
        self.cfg = FakeCfg()
        self.client = FakeClient()
        self.instance_id = "default"
        self.config = {"mdblist": {"api_key": "k"}}
        self.raw_cfg = self.config


USER_LISTS = [
    {"id": 101, "name": "Weekend", "slug": "weekend", "mediatype": "movie", "items": 2, "dynamic": False, "private": True, "user_name": "me"},
    {"id": 202, "name": "Top Trending", "slug": "trending", "mediatype": "show", "items": 5, "dynamic": True, "private": False, "user_name": "me"},
]

LIST_ITEMS = {
    "movies": [
        {"id": 9001, "rank": 1, "title": "Dune", "imdb_id": "tt1160419", "tmdb_id": 438631, "mediatype": "movie", "release_year": 2021},
    ],
    "shows": [
        {"id": 9002, "rank": 2, "title": "Severance", "imdb_id": "tt11280740", "tvdb_id": 371980, "mediatype": "show", "release_year": 2022},
    ],
}


def _router(monkeypatch, handlers):
    captured: list[dict[str, Any]] = []

    def fake_req(adapter, method, url, **kw):
        captured.append({"method": method, "url": url, "json": kw.get("json"), "params": kw.get("params")})
        for (m, needle), resp in handlers.items():
            if method == m and needle in url:
                return resp() if callable(resp) else resp
        return FakeResp(404, None)

    monkeypatch.setattr(pl, "mdblist_request", fake_req)
    monkeypatch.setattr(pl, "has_auth", lambda *_a, **_k: True)
    return captured


def test_dynamic_list_is_read_only_static_is_writable(monkeypatch):
    _router(monkeypatch, {("GET", "/lists/user"): FakeResp(200, USER_LISTS)})
    res = pl.list_resources(FakeAdapter())
    by_id = {r.id: r for r in res}
    watchlist = by_id[pl.WATCHLIST_ID]
    assert watchlist.name == "Watchlist"
    assert watchlist.can_add and watchlist.can_remove
    assert watchlist.can_reorder is False
    static = by_id["101"]
    dynamic = by_id["202"]
    assert static.can_add and static.can_remove and static.can_reorder is False
    assert static.media_types == ("movie", "show", "season", "episode")
    assert static.is_smart is False
    assert dynamic.is_smart is True
    assert dynamic.can_add is False and dynamic.can_remove is False
    assert dynamic.media_types == ("show",)
    assert static.extra["item_count"] == 2 and static.extra["private"] is True
    assert dynamic.extra["dynamic"] is True


def test_watchlist_snapshot_and_writes_delegate(monkeypatch):
    monkeypatch.setattr(
        pl.feat_watchlist,
        "build_index",
        lambda ad: {"tmdb:438631": {"type": "movie", "title": "Dune", "ids": {"tmdb": "438631"}}},
    )
    monkeypatch.setattr(pl.feat_watchlist, "add", lambda ad, items: (len(list(items)), []))
    monkeypatch.setattr(pl.feat_watchlist, "remove", lambda ad, items: (len(list(items)), []))

    ad = FakeAdapter()
    snap = pl.get_snapshot(ad, pl.WATCHLIST_ID)
    assert snap.resource.name == "Watchlist"
    assert snap.ordered_keys() == ["tmdb:438631"]

    item = {"type": "movie", "title": "Dune", "ids": {"tmdb": "438631"}}
    add_res = pl.add(ad, pl.WATCHLIST_ID, [item])
    rm_res = pl.remove(ad, pl.WATCHLIST_ID, [item])
    assert add_res["count"] == 1 and add_res["confirmed_keys"] == ["tmdb:438631"]
    assert rm_res["count"] == 1 and rm_res["confirmed_keys"] == ["tmdb:438631"]
    assert pl.reorder(ad, pl.WATCHLIST_ID, ["tmdb:438631"])["unsupported"] is True


def test_snapshot_normalizes_movies_and_shows_with_rank_and_ids(monkeypatch):
    _router(
        monkeypatch,
        {
            ("GET", "/lists/user"): FakeResp(200, USER_LISTS),
            ("GET", "/lists/101/items"): FakeResp(200, LIST_ITEMS, headers={"X-Has-More": "false"}),
        },
    )
    snap = pl.get_snapshot(FakeAdapter(), "101")
    assert len(snap.items) == 2
    first = snap.items[0]
    assert first.item["ids"].get("imdb") == "tt1160419"
    assert first.item["ids"].get("tmdb") == "438631"
    assert first.item["ids"].get("mdblist") is None
    assert first.playlist_item_id == "9001"
    assert first.position == 1
    show = snap.items[1]
    assert show.item["type"] == "show"
    assert show.item["ids"].get("imdb") == "tt11280740"
    assert show.item["ids"].get("tvdb") == "371980"
    assert show.item["ids"].get("tmdb") is None


def test_snapshot_reads_nested_id_blocks(monkeypatch):
    rows = {
        "movies": [
            {
                "id": 9001,
                "rank": 1,
                "title": "Dune",
                "ids": {"imdb": "tt1160419", "tmdb": 438631, "mdblist": "mdb-1"},
                "mediatype": "movie",
            },
        ],
    }
    _router(
        monkeypatch,
        {
            ("GET", "/lists/user"): FakeResp(200, USER_LISTS),
            ("GET", "/lists/101/items"): FakeResp(200, rows, headers={"X-Has-More": "false"}),
        },
    )
    snap = pl.get_snapshot(FakeAdapter(), "101")
    assert snap.items[0].key == "tmdb:438631"
    assert snap.items[0].item["ids"]["imdb"] == "tt1160419"
    assert snap.items[0].item["ids"]["mdblist"] == "mdb-1"


def test_add_and_remove_payload_shapes(monkeypatch):
    captured = _router(
        monkeypatch,
        {
            ("GET", "/lists/user"): FakeResp(200, USER_LISTS),
            ("POST", "/lists/101/items/add"): FakeResp(200, {"added": {"movies": 1, "shows": 1}}),
            ("POST", "/lists/101/items/remove"): FakeResp(200, {"deleted": {"movies": 1}}),
        },
    )
    items = [
        {"type": "movie", "title": "Dune", "ids": {"imdb": "tt1160419", "tmdb": "438631"}},
        {"type": "show", "title": "Severance", "ids": {"imdb": "tt11280740"}},
    ]
    add_res = pl.add(FakeAdapter(), "101", items)
    assert add_res["ok"] is True and add_res["count"] == 2
    add_call = next(c for c in captured if c["url"].endswith("/items/add"))
    assert add_call["json"] == {"movies": [{"imdb": "tt1160419", "tmdb": 438631}], "shows": [{"imdb": "tt11280740"}]}

    rm_res = pl.remove(FakeAdapter(), "101", items[:1])
    assert rm_res["count"] == 1


def test_add_existing_items_are_not_reported_as_new_additions(monkeypatch):
    _router(
        monkeypatch,
        {
            ("GET", "/lists/user"): FakeResp(200, USER_LISTS),
            ("POST", "/lists/101/items/add"): FakeResp(200, {"existing": {"movies": 1}}),
        },
    )
    res = pl.add(FakeAdapter(), "101", [{"type": "movie", "title": "Dune", "ids": {"tmdb": "438631"}}])
    assert res["ok"] is True
    assert res["count"] == 0
    assert res["confirmed_keys"] == ["tmdb:438631"]


def test_create_static_list_uses_current_user_add_route(monkeypatch):
    captured = _router(
        monkeypatch,
        {("POST", "/lists/user/add"): FakeResp(200, {"id": 303, "name": "Short"})},
    )
    res = pl.create(FakeAdapter(), "Short", media_type="show")
    assert res.id == "303"
    call = next(c for c in captured if c["url"].endswith("/lists/user/add"))
    assert call["json"] == {"name": "Short"}


def test_seasons_and_episodes_use_tmdb_buckets(monkeypatch):
    captured = _router(
        monkeypatch,
        {
            ("GET", "/lists/user"): FakeResp(200, USER_LISTS),
            ("POST", "/lists/101/items/add"): FakeResp(200, {"added": {"seasons": 1, "episodes": 1}}),
        },
    )
    items = [
        {"type": "episode", "title": "Ep", "ids": {"tmdb": "2001", "imdb": "tt0000001"}},
        {"type": "season", "title": "S1", "ids": {"tmdb": 1001, "tvdb": "111"}},
    ]
    res = pl.add(FakeAdapter(), "101", items)
    assert res["count"] == 2
    add_call = next(c for c in captured if c["url"].endswith("/items/add"))
    assert add_call["json"] == {"seasons": [{"tmdb": 1001}], "episodes": [{"tmdb": 2001}]}


def test_seasons_and_episodes_without_tmdb_rejected_before_request(monkeypatch):
    captured = _router(monkeypatch, {("GET", "/lists/user"): FakeResp(200, USER_LISTS)})
    items = [
        {"type": "episode", "title": "Ep", "ids": {"imdb": "tt0000001"}},
        {"type": "season", "title": "S1", "ids": {"tvdb": "111"}},
    ]
    res = pl.add(FakeAdapter(), "101", items)
    assert res["count"] == 0
    assert all(u["hint"] == "missing_supported_ids" for u in res["unresolved"])
    assert not any("/items/add" in c["url"] for c in captured)


def test_write_to_dynamic_list_rejected(monkeypatch):
    _router(monkeypatch, {("GET", "/lists/user"): FakeResp(200, USER_LISTS)})
    with pytest.raises(pl.MDBListDynamicListError):
        pl.add(FakeAdapter(), "202", [{"type": "movie", "ids": {"imdb": "tt1"}}])


def test_write_to_unowned_list_rejected(monkeypatch):
    _router(monkeypatch, {("GET", "/lists/user"): FakeResp(200, USER_LISTS)})
    with pytest.raises(pl.MDBListNotOwnedError):
        pl.add(FakeAdapter(), "999", [{"type": "movie", "ids": {"imdb": "tt1"}}])


def test_reorder_is_noop_unsupported(monkeypatch):
    _router(monkeypatch, {("GET", "/lists/user"): FakeResp(200, USER_LISTS)})
    res = pl.reorder(FakeAdapter(), "101", ["imdb:tt1"])
    assert res["reordered"] == 0 and res["unsupported"] is True
