from __future__ import annotations

import importlib
from typing import Any

from cw_platform.id_map import canonical_key
from cw_platform.playlists import supports_playlists

pl = importlib.import_module("providers.sync.jellyfin._playlists")
mod = importlib.import_module("providers.sync._mod_JELLYFIN")


class FakeResp:
    def __init__(self, status: int, payload: Any = None):
        self.status_code = status
        self._payload = payload
        self.text = "x" if payload is not None else ""

    def json(self) -> Any:
        return self._payload


class FakeHttp:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def get(self, path: str, *, params: dict[str, Any] | None = None) -> FakeResp:
        params = params or {}
        self.calls.append({"method": "GET", "path": path, "params": dict(params)})
        if path == "/Items" and params.get("ParentId") == "C1":
            return FakeResp(200, {"Items": [row("I2", "Series", "Show", tvdb="42")], "TotalRecordCount": 1})
        if path == "/Items" and params.get("IncludeItemTypes") == "Playlist":
            return FakeResp(200, {"Items": [{"Id": "P1", "Name": "Weekend", "Type": "Playlist", "ChildCount": 1}], "TotalRecordCount": 1})
        if path == "/Items" and params.get("IncludeItemTypes") == "BoxSet":
            return FakeResp(200, {"Items": [{"Id": "C1", "Name": "Favorites", "Type": "BoxSet", "ChildCount": 1}], "TotalRecordCount": 1})
        if path == "/Playlists/P1/Items":
            return FakeResp(200, {"Items": [row("I1", "Movie", "Dune", tmdb="438631", playlist_item_id="E1")], "TotalRecordCount": 1})
        return FakeResp(404, {})

    def post(self, path: str, *, params: dict[str, Any] | None = None, json: Any = None) -> FakeResp:
        self.calls.append({"method": "POST", "path": path, "params": dict(params or {}), "json": json})
        if path in ("/Playlists/P1/Items", "/Collections/C1/Items"):
            return FakeResp(204, {})
        if path == "/Playlists":
            return FakeResp(200, {"Id": "P2"})
        if path == "/Collections":
            return FakeResp(200, {"Id": "C2"})
        return FakeResp(404, {})

    def delete(self, path: str, *, params: dict[str, Any] | None = None) -> FakeResp:
        self.calls.append({"method": "DELETE", "path": path, "params": dict(params or {})})
        if path in ("/Playlists/P1/Items", "/Collections/C1/Items"):
            return FakeResp(204, {})
        return FakeResp(404, {})


class FakeCfg:
    user_id = "U1"
    watchlist_query_limit = 25
    watchlist_write_delay_ms = 0
    strict_id_matching = False
    watchlist_guid_priority = None


class FakeAdapter:
    def __init__(self) -> None:
        self.client = FakeHttp()
        self.cfg = FakeCfg()
        self.instance_id = "default"


def row(iid: str, typ: str, name: str, *, tmdb: str | None = None, tvdb: str | None = None, playlist_item_id: str | None = None) -> dict[str, Any]:
    provider_ids: dict[str, str] = {}
    if tmdb:
        provider_ids["Tmdb"] = tmdb
    if tvdb:
        provider_ids["Tvdb"] = tvdb
    out = {"Id": iid, "Type": typ, "Name": name, "ProviderIds": provider_ids, "ProductionYear": 2021}
    if playlist_item_id:
        out["PlaylistItemId"] = playlist_item_id
    return out


def media(tmdb: str, title: str = "Dune") -> dict[str, Any]:
    return {"type": "movie", "title": title, "year": 2021, "ids": {"tmdb": tmdb}}


def test_jellyfin_ops_are_playlist_capable():
    assert supports_playlists(mod.OPS)
    assert mod.OPS.features()["playlists"] is True
    assert mod.OPS.capabilities()["playlists"]["endpoint_types"] == ["playlist", "collection"]


def test_list_resources_exposes_playlists_and_collections():
    ad = FakeAdapter()
    resources = pl.list_resources(ad)
    by_id = {r.id: r for r in resources}

    assert set(by_id) == {"playlist:P1", "collection:C1"}
    assert by_id["playlist:P1"].extra["endpoint_type"] == "playlist"
    assert by_id["collection:C1"].extra["endpoint_type"] == "collection"
    assert by_id["collection:C1"].can_reorder is False
    assert by_id["collection:C1"].media_types == ("movie", "show")


def test_snapshots_use_explicit_container_ids():
    ad = FakeAdapter()
    playlist = pl.get_snapshot(ad, "playlist:P1")
    collection = pl.get_snapshot(ad, "collection:C1")

    assert playlist.ordered_keys() == ["tmdb:438631"]
    assert playlist.items[0].playlist_item_id == "E1"
    assert collection.ordered_keys() == ["tvdb:42"]
    assert collection.items[0].position is None
    assert any(c["path"] == "/Playlists/P1/Items" for c in ad.client.calls)
    assert any(c["path"] == "/Items" and c["params"].get("ParentId") == "C1" for c in ad.client.calls)


def test_add_reports_missing_library_and_writes_raw_playlist_id(monkeypatch):
    ad = FakeAdapter()

    def fake_resolve(adapter: Any, item: dict[str, Any], *, feature: str = "history") -> str | None:
        return {"tmdb:438631": "I1"}.get(canonical_key(item))

    monkeypatch.setattr(pl, "resolve_item_id", fake_resolve)
    res = pl.add(ad, "playlist:P1", [media("438631"), media("999", "Ghost")])

    assert res["count"] == 1
    assert res["confirmed_keys"] == ["tmdb:438631"]
    assert res["unresolved"][0]["hint"] == "not_in_library"
    call = next(c for c in ad.client.calls if c["method"] == "POST" and c["path"] == "/Playlists/P1/Items")
    assert call["params"]["ids"] == "I1"
    assert call["params"]["userId"] == "U1"


def test_collection_add_and_remove_use_collection_id(monkeypatch):
    ad = FakeAdapter()

    def fake_resolve(adapter: Any, item: dict[str, Any], *, feature: str = "history") -> str | None:
        return {"tvdb:42": "I2"}.get(canonical_key(item))

    monkeypatch.setattr(pl, "resolve_item_id", fake_resolve)
    item = {"type": "show", "title": "Show", "ids": {"tvdb": "42"}}
    add_res = pl.add(ad, "collection:C1", [item])
    rm_res = pl.remove(ad, "collection:C1", [item])

    assert add_res["count"] == 1
    assert rm_res["count"] == 1
    add_call = next(c for c in ad.client.calls if c["method"] == "POST" and c["path"] == "/Collections/C1/Items")
    remove_call = next(c for c in ad.client.calls if c["method"] == "DELETE" and c["path"] == "/Collections/C1/Items")
    assert add_call["params"]["Ids"] == "I2"
    assert remove_call["params"]["Ids"] == "I2"


def test_create_can_make_playlist_or_collection():
    ad = FakeAdapter()
    playlist = pl.create(ad, "NewList")
    collection = pl.create(ad, "NewBox", media_type="collection")

    assert playlist.id == "playlist:P2"
    assert collection.id == "collection:C2"
    assert any(c["path"] == "/Playlists" and c["json"]["Name"] == "NewList" for c in ad.client.calls)
    assert any(c["path"] == "/Collections" and c["params"]["Name"] == "NewBox" for c in ad.client.calls)
