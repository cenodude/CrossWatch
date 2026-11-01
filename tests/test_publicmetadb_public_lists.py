# /tests/test_publicmetadb_public_lists.py
# PublicMetaDB playlists: public lists by link, read only and confirmed writes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlparse

import pytest

from cw_platform.playlists import PlaylistLinkError, PlaylistReadOnlyError, PlaylistUnavailableError, playlist_capabilities
from providers.sync import _mod_PUBLICMETADB as mod
from providers.sync.publicmetadb import _playlists as pl

OWN = {"id": "e5aza1fwa3bkbvj", "name": "CrossWatch Watchlist", "slug": "crosswatch-watchlist", "is_public": False, "type": "watchlist"}
MINE = {"id": "mine00000000001", "name": "Heists", "slug": "heists", "is_public": False, "type": "custom"}
PUBLIC = {"id": "jut2nqaatkp7u70", "name": "Movie Leaks - Latest Releases", "slug": "movie-leaks-latest-releases", "is_public": True, "type": "custom"}
ITEM = {"id": "1wosqjom97fmnpo", "list": "jut2nqaatkp7u70", "media_type": "movie", "tmdb_id": 1383750}


class _Resp:
    def __init__(self, status: int, data: Any) -> None:
        self.status_code, self._data = status, data
        self.text = json.dumps(data) if data is not None else ""

    def json(self) -> Any:
        return self._data


class _Client:
    def __init__(self, routes: dict[tuple[str, str], Any]) -> None:
        self.routes, self.calls = routes, []

    def _answer(self, method: str, path: str, **kw: Any) -> _Resp:
        self.calls.append((method, path, kw.get("params"), kw.get("json")))
        body = self.routes.get((method, path))
        if callable(body):
            body = body(kw)
        if body is None:
            return _Resp(404, {"error": "List not found"})
        return body if isinstance(body, _Resp) else _Resp(200, body)

    def get(self, path: str, **kw: Any) -> _Resp:
        return self._answer("GET", path, **kw)

    def post(self, path: str, **kw: Any) -> _Resp:
        return self._answer("POST", path, **kw)

    def delete(self, path: str, **kw: Any) -> _Resp:
        return self._answer("DELETE", path, **kw)

    def read_json(self, path: str, **kw: Any) -> Any:
        r = self.get(path, **kw)
        if not (200 <= r.status_code < 300):
            raise mod.PUBLICMETADBError(f"publicmetadb_read_http_{r.status_code}:{path}")
        return r.json()

    def post_json(self, path: str, **kw: Any) -> Any:
        return self.post(path, **kw).json()

    @staticmethod
    def safe_json(resp: Any) -> Any:
        return resp.json()


def _page(rows: list[dict[str, Any]], head: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"items": rows, "total": len(rows), "page": 1, "perPage": 500, "totalPages": 1 if rows else 0, **({"list": head} if head else {})}


def _adapter(routes: dict[tuple[str, str], Any]):
    table = {("GET", "/api/external/lists"): _page([OWN, MINE]), **routes}
    client = _Client(table)
    return SimpleNamespace(client=client, cfg=SimpleNamespace(watchlist_page_size=500), instance_id="default"), client


def test_capabilities_offer_link_resolve():
    caps = playlist_capabilities(mod.OPS)
    assert caps["resolve"] and urlparse(caps["link_example"]).hostname == "publicmetadb.com"
    assert callable(getattr(mod.OPS, "resolve_playlist", None))


@pytest.mark.parametrize("ref", [
    "https://publicmetadb.com/lists/u/sactest834/movie-leaks-latest-releases",
    "publicmetadb.com/lists/u/sactest834/movie-leaks-latest-releases?x=1",
])
def test_resolve_share_link(ref):
    head = {**PUBLIC, "contributor": "sactest834"}
    adapter, _ = _adapter({("GET", "/api/external/lists/u/sactest834/movie-leaks-latest-releases/items"): _page([ITEM], head)})
    res = pl.resolve(adapter, ref)
    assert res.id == "jut2nqaatkp7u70" and res.name == "Movie Leaks - Latest Releases by sactest834"
    assert res.extra["endpoint_type"] == "public_list" and not res.can_add and not res.extra["can_delete"]


def test_resolve_list_id_and_own_links():
    adapter, client = _adapter({("GET", "/api/external/lists/jut2nqaatkp7u70/items"): _page([ITEM], PUBLIC)})
    assert pl.resolve(adapter, "jut2nqaatkp7u70").extra["endpoint_type"] == "public_list"
    assert pl.resolve(adapter, "mine00000000001").can_add
    assert pl.resolve(adapter, "https://publicmetadb.com/lists/my/heists").id == "mine00000000001"
    assert pl.resolve(adapter, "https://publicmetadb.com/lists/my/watchlist").id == "e5aza1fwa3bkbvj"
    assert not [c for c in client.calls if c[1].startswith("/api/external/lists/mine")]


def test_resolve_share_link_of_own_list_returns_own():
    head = {**MINE, "contributor": "me"}
    adapter, _ = _adapter({("GET", "/api/external/lists/u/me/heists/items"): _page([], head)})
    assert pl.resolve(adapter, "publicmetadb.com/lists/u/me/heists").can_add


def test_resolve_missing_or_private():
    adapter, _ = _adapter({})
    with pytest.raises(PlaylistUnavailableError):
        pl.resolve(adapter, "publicmetadb.com/lists/u/nobody/nothing")
    with pytest.raises(PlaylistUnavailableError):
        pl.resolve(adapter, "zzzzzzzzzzzzzzz")
    with pytest.raises(PlaylistUnavailableError):
        pl.resolve(adapter, "publicmetadb.com/lists/my/not-mine")


@pytest.mark.parametrize("ref", ["https://trakt.tv/lists/1", "https://publicmetadb.com/lists", "publicmetadb.com/movie/550", "short", ""])
def test_resolve_rejects_other_links(ref):
    adapter, _ = _adapter({})
    with pytest.raises(PlaylistLinkError):
        pl.resolve(adapter, ref)


def test_public_snapshot_is_read_only_and_writes_are_refused():
    routes = {("GET", "/api/external/lists/jut2nqaatkp7u70/items"): _page([ITEM], PUBLIC)}
    adapter, client = _adapter(routes)
    snap = pl.get_snapshot(adapter, "jut2nqaatkp7u70")
    assert not snap.resource.can_add and snap.resource.extra["public_list"]
    assert [it.item["ids"]["tmdb"] for it in snap.items] == ["1383750"]
    for op in (pl.add, pl.remove):
        with pytest.raises(PlaylistReadOnlyError):
            op(adapter, "jut2nqaatkp7u70", [{"type": "movie", "ids": {"tmdb": 949}}])
    with pytest.raises(PlaylistReadOnlyError):
        pl.delete(adapter, "jut2nqaatkp7u70")
    assert not [c for c in client.calls if c[0] != "GET"]


def test_unknown_list_is_unavailable_not_writable():
    adapter, _ = _adapter({})
    with pytest.raises(PlaylistUnavailableError):
        pl.get_snapshot(adapter, "zzzzzzzzzzzzzzz")
    with pytest.raises(PlaylistUnavailableError):
        pl.get_snapshot(adapter, "not-an-id")


def test_add_confirms_only_answered_items():
    def add(kw):
        tmdb = kw["json"]["tmdb_id"]
        if tmdb == 949:
            return {"success": True, "item": {"id": "a", "tmdb_id": 949, "media_type": "movie"}}
        if tmdb == 807:
            return {"success": True, "already_existed": True, "message": "Item already in list", "item": {"id": "b", "tmdb_id": 807, "media_type": "movie"}}
        if tmdb == 1396:
            return {"success": True, "item": {"id": "c", "tmdb_id": 1396, "media_type": "movie"}}
        return {}

    adapter, _ = _adapter({("POST", "/api/external/lists/mine00000000001/items"): add})
    items = [{"type": "movie", "ids": {"tmdb": t}} for t in (949, 807, 155)] + [{"type": "show", "ids": {"tmdb": 1396}}]
    out = pl.add(adapter, "mine00000000001", items)
    assert out["confirmed_keys"] == ["tmdb:949", "tmdb:807"]
    assert [u["hint"] for u in out["unresolved"]] == ["write_not_verified", "write_not_verified"]


def test_created_list_is_private_and_remembered(monkeypatch):
    remembered: list[str] = []
    monkeypatch.setattr(pl, "remember_resource", lambda reader, ad, res: remembered.append(res.id))
    created = {"success": True, "item": {"id": "new000000000001", "name": "Synced", "slug": "synced", "is_public": False, "type": "custom"}}
    adapter, client = _adapter({("POST", "/api/external/lists"): created})
    res = pl.create(adapter, "Synced")
    assert res.id == "new000000000001" and remembered == ["new000000000001"]
    body = [c[3] for c in client.calls if c[1] == "/api/external/lists" and c[0] == "POST"][0]
    assert body["is_public"] is False
