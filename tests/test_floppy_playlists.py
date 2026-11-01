# /tests/test_floppy_playlists.py
# Floppy custom lists as playlists: seasons, episodes, custom order and confirmed writes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from cw_platform.playlists import PlaylistAuthError, PlaylistReadOnlyError, PlaylistUnavailableError, playlist_capabilities, supports_playlists
from providers.sync import _mod_FLOPPY as mod
from providers.sync.floppy import _playlists as pl

API = "http://floppy.test/api/v1"
ME = {"id": 1, "username": "cenodude"}
WATCHLIST = {"id": 1, "name": "Watchlist", "owner": ME, "collaborators": [], "items_count": 0, "latest_update": None, "is_public": False}
MINE = {"id": 2, "name": "Heists", "owner": ME, "collaborators": [], "items_count": 4, "latest_update": "2026-11-01T02:32:20Z", "is_public": False}
SHARED = {"id": 3, "name": "Family", "owner": {"id": 9, "username": "mum"}, "collaborators": ["cenodude"], "items_count": 0, "latest_update": None, "is_public": False}
ROWS = [
    {"id": 7623, "item_id": "movie/tmdb/949", "item": {"title": "Heat"}},
    {"id": 7624, "item_id": "tv/tmdb/1396", "item": {"title": "Breaking Bad"}},
    {"id": 7700, "item_id": "tv/tmdb/1396/0", "item": {"title": "Breaking Bad"}},
    {"id": 1800, "item_id": "tv/tmdb/1396/1/1", "item": {"title": "Breaking Bad"}},
    {"id": 5, "item_id": "book/openlibrary/OL1W", "item": {"title": "A book"}},
]


class _Resp:
    def __init__(self, status: int, data: Any = None) -> None:
        self.status_code, self._data = status, data
        self.text = json.dumps(data) if data is not None else ""
        self.headers: dict[str, str] = {}

    def json(self) -> Any:
        return self._data


def _page(rows: list[dict[str, Any]], total: int | None = None) -> dict[str, Any]:
    return {"pagination": {"total": len(rows) if total is None else total, "limit": 200, "offset": 0}, "results": rows}


class _Session:
    def __init__(self, routes: dict[tuple[str, str], Any]) -> None:
        self.routes, self.calls = routes, []

    def request(self, method: str, url: str, **kwargs: Any) -> _Resp:
        path = url[len(API) + 1:].strip("/")
        self.calls.append((method, path, kwargs.get("json"), kwargs.get("params")))
        body = self.routes.get((method, path))
        if callable(body):
            body = body(kwargs)
        if body is None:
            return _Resp(404, {"detail": "Not found."})
        return body if isinstance(body, _Resp) else _Resp(200, body)


def _adapter(monkeypatch: pytest.MonkeyPatch, routes: dict[tuple[str, str], Any]):
    table = {("GET", "lists"): _page([WATCHLIST, MINE, SHARED]), ("GET", "lists/2/items"): _page(ROWS[:4] + ROWS[4:], total=5), **routes}
    session = _Session(table)
    monkeypatch.setattr(pl, "write_bucket", lambda adapter, tmdb, item=None: "")
    client = SimpleNamespace(api_base=API, api_token="t", timeout=5, verify_ssl=False, session=session)
    return SimpleNamespace(client=client, config={"floppy": {"server_url": "http://floppy.test", "api_token": "t"}}, instance_id="default"), session


@pytest.fixture(autouse=True)
def _clear_memo():
    pl._SNAPSHOT_MEMO.clear()
    yield
    pl._SNAPSHOT_MEMO.clear()


def test_manifest_declares_playlists():
    assert supports_playlists(mod.OPS)
    caps = playlist_capabilities(mod.OPS)
    assert caps["reorder"] and caps["resolve"] is False and "seasons" in caps["media_types"]
    assert mod.get_manifest()["features"]["playlists"] is True


def test_resources_mark_watchlist_builtin_and_shared_lists(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {})
    res = {r.id: r for r in pl.list_resources(adapter)}
    assert res["1"].extra["builtin"] and not res["1"].extra["can_delete"] and res["1"].can_add
    assert res["2"].extra["can_rename"] and res["2"].extra["can_delete"] and res["2"].can_reorder
    assert res["3"].name == "Family by mum" and res["3"].extra["endpoint_type"] == "collaboration" and res["3"].can_add and not res["3"].extra["can_delete"]


def test_snapshot_maps_movies_shows_seasons_and_episodes_in_order(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {})
    snap = pl.get_snapshot(adapter, "2")
    assert [it.key for it in snap.items] == ["tmdb:949", "tmdb:1396#show", "tmdb:1396#season:0", "tmdb:1396#s01e01"]
    assert [it.playlist_item_id for it in snap.items] == ["7623", "7624", "7700", "1800"]
    assert [it.position for it in snap.items] == [0, 1, 2, 3]


@pytest.mark.parametrize("bad", ["tv/tmdb/1396/x", "tv/tmdb/1396/1/2/3", "movie/tmdb/949/1", "movie/tmdb/abc", "movie", ""])
def test_unparseable_tmdb_rows_fail_the_snapshot(monkeypatch, bad):
    rows = [ROWS[0], {"id": 9, "item_id": bad}]
    adapter, _ = _adapter(monkeypatch, {("GET", "lists/2/items"): _page(rows)})
    with pytest.raises(pl.FloppyPlaylistError):
        pl.get_snapshot(adapter, "2")


def test_short_read_fails(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("GET", "lists/2/items"): _page(ROWS[:1], total=3)})
    with pytest.raises(RuntimeError):
        pl.get_snapshot(adapter, "2")


def test_rejected_token_is_an_auth_error(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("GET", "lists"): _Resp(401, {"detail": "bad token"})})
    with pytest.raises(PlaylistAuthError):
        pl.list_resources(adapter)


def test_unknown_list_is_unavailable_and_not_writable(monkeypatch):
    adapter, session = _adapter(monkeypatch, {})
    with pytest.raises(PlaylistUnavailableError):
        pl.get_snapshot(adapter, "99")
    with pytest.raises(PlaylistReadOnlyError):
        pl.add(adapter, "99", [{"type": "movie", "ids": {"tmdb": 949}}])
    assert not [c for c in session.calls if c[0] != "GET"]


def test_add_uses_media_paths_and_confirms_duplicates(monkeypatch):
    routes = {
        ("PUT", "media/movie/tmdb/949/lists/2"): [{"list_id": 2, "list_item_id": None}],
        ("PUT", "media/movie/tmdb/807/lists/2"): _Resp(409, {"detail": "Media already in the list."}),
        ("PUT", "media/tv/tmdb/1396/1/2/lists/2"): [],
        ("PUT", "media/tv/tmdb/1396/2/lists/2"): [],
    }
    adapter, session = _adapter(monkeypatch, routes)
    items = [
        {"type": "movie", "ids": {"tmdb": 949}},
        {"type": "movie", "ids": {"tmdb": 807}},
        {"type": "episode", "show_ids": {"tmdb": 1396}, "season": 1, "episode": 2, "ids": {}},
        {"type": "season", "show_ids": {"tmdb": 1396}, "season": 2, "ids": {}},
        {"type": "movie", "ids": {"tmdb": 999999999}},
        {"type": "movie", "ids": {"imdb": "tt0000001"}},
    ]
    monkeypatch.setattr(pl, "tmdb_enriched_item", lambda adapter, item, episode_show=False: dict(item))
    out = pl.add(adapter, "2", items)
    assert out["confirmed_keys"] == ["tmdb:949", "tmdb:807", "tmdb:1396#s01e02", "tmdb:1396#season:2"]
    assert sorted(u["hint"] for u in out["unresolved"]) == ["missing_tmdb_id", "not_found"]
    assert out["ok"] is False


def test_remove_uses_media_paths(monkeypatch):
    adapter, session = _adapter(monkeypatch, {("DELETE", "media/movie/tmdb/949/lists/2"): _Resp(204)})
    out = pl.remove(adapter, "2", [{"type": "movie", "ids": {"tmdb": 949}}])
    assert out["confirmed_keys"] == ["tmdb:949"]
    assert ("DELETE", "media/movie/tmdb/949/lists/2", None, None) in session.calls


def test_reorder_sends_item_ids_in_order(monkeypatch):
    adapter, session = _adapter(monkeypatch, {("PUT", "lists/2/items/order"): _Resp(204)})
    out = pl.reorder(adapter, "2", ["tmdb:1396#s01e01", "tmdb:949"])
    assert out["reordered"] == 4
    assert [c[2] for c in session.calls if c[0] == "PUT"] == [{"item_ids": [1800, 7623, 7624, 7700]}]
    session.calls.clear()
    assert pl.reorder(adapter, "2", ["tmdb:949", "tmdb:1396#show", "tmdb:1396#season:0", "tmdb:1396#s01e01"])["count"] == 0
    assert not [c for c in session.calls if c[0] == "PUT"]


def test_unchanged_list_is_not_read_again_and_writes_drop_the_memo(monkeypatch):
    adapter, session = _adapter(monkeypatch, {("PUT", "media/movie/tmdb/807/lists/2"): []})
    monkeypatch.setattr(pl, "tmdb_enriched_item", lambda adapter, item, episode_show=False: dict(item))
    pl.get_snapshot(adapter, "2")
    pl.get_snapshot(adapter, "2")
    assert len([c for c in session.calls if c[1] == "lists/2/items"]) == 1
    pl.add(adapter, "2", [{"type": "movie", "ids": {"tmdb": 807}}])
    pl.get_snapshot(adapter, "2")
    assert len([c for c in session.calls if c[1] == "lists/2/items"]) == 2


def test_create_is_private_and_rename_delete_follow_ownership(monkeypatch):
    remembered: list[str] = []
    monkeypatch.setattr(pl, "remember_resource", lambda reader, ad, res: remembered.append(res.id))
    created = {"id": 5, "name": "New", "owner": ME, "items_count": 0, "is_public": False}
    routes = {("POST", "lists"): _Resp(201, created), ("PATCH", "lists/2"): {**MINE, "name": "Heists 2"}, ("DELETE", "lists/2"): _Resp(204)}
    adapter, session = _adapter(monkeypatch, routes)
    res = pl.create(adapter, "New")
    assert res.id == "5" and remembered == ["5"]
    assert ("POST", "lists", {"name": "New", "is_public": False}, None) in session.calls
    assert pl.rename(adapter, "2", "Heists 2").name == "Heists 2"
    assert pl.delete(adapter, "2") == {"ok": True, "count": 1}
    for lid in ("1", "3"):
        with pytest.raises(PlaylistReadOnlyError):
            pl.delete(adapter, lid)
        with pytest.raises(PlaylistReadOnlyError):
            pl.rename(adapter, lid, "x")


ALICE = {"id": 1, "username": "alice"}
BOB = {"id": 2, "username": "bob"}


def _lists(monkeypatch, rows):
    adapter, session = _adapter(monkeypatch, {("GET", "lists"): _page(rows)})
    return {r.id: r for r in pl.list_resources(adapter)}, adapter, session


def test_someone_elses_shared_watchlist_does_not_decide_who_you_are(monkeypatch):
    rows = [
        {"id": 10, "name": "Watchlist", "owner": BOB, "collaborators": ["alice"], "items_count": 0},
        {"id": 11, "name": "Mine", "owner": ALICE, "collaborators": [], "items_count": 0},
        {"id": 12, "name": "Bob extra", "owner": BOB, "collaborators": ["alice"], "items_count": 0},
        {"id": 13, "name": "Mine shared", "owner": ALICE, "collaborators": ["bob"], "items_count": 0},
    ]
    res, _, _ = _lists(monkeypatch, rows)
    assert not res["10"].extra["builtin"] and res["10"].name == "Watchlist by bob"
    for lid in ("10", "12"):
        assert not res[lid].extra["can_rename"] and not res[lid].extra["can_delete"] and not res[lid].can_reorder and res[lid].can_add
    for lid in ("11", "13"):
        assert res[lid].extra["can_rename"] and res[lid].extra["can_delete"] and res[lid].can_reorder


def test_unknown_owner_never_gets_owner_only_operations(monkeypatch):
    rows = [
        {"id": 20, "name": "Family", "owner": BOB, "collaborators": ["alice"], "items_count": 0},
        {"id": 21, "name": "Ours", "owner": ALICE, "collaborators": ["bob"], "items_count": 0},
    ]
    res, adapter, session = _lists(monkeypatch, rows)
    for r in res.values():
        assert r.can_add and r.can_remove and not r.can_reorder
        assert not r.extra["can_rename"] and not r.extra["can_delete"]
    with pytest.raises(PlaylistReadOnlyError):
        pl.rename(adapter, "21", "x")
    with pytest.raises(PlaylistReadOnlyError):
        pl.delete(adapter, "21")


def test_reorder_is_refused_on_shared_lists_before_any_call(monkeypatch):
    adapter, session = _adapter(monkeypatch, {})
    with pytest.raises(PlaylistReadOnlyError):
        pl.reorder(adapter, "3", ["tmdb:949"])
    assert not [c for c in session.calls if c[0] != "GET"]
    assert not [c for c in session.calls if c[1] == "lists/3/items"]
