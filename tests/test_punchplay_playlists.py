# /tests/test_punchplay_playlists.py
# PunchPlay lists as playlists: own, shared, imported, dynamic and public lists
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlparse

import pytest

from cw_platform.playlists import PlaylistAuthError, PlaylistLinkError, PlaylistReadOnlyError, PlaylistUnavailableError, playlist_capabilities, supports_playlists
from providers.sync import _mod_PUNCHPLAY as mod
from providers.sync.punchplay import _playlists as pl

B = pl.PLATFORM_BASE
WATCHLIST = {"id": 2198, "name": "Watchlist", "isPublic": False, "isWatchlist": True, "isDynamicList": False, "externalSource": None, "ownerUsername": None, "itemCount": 1}
MINE = {"id": 10, "name": "Heists", "isPublic": False, "isWatchlist": False, "isDynamicList": False, "externalSource": None, "ownerUsername": None, "itemCount": 2}
DYNAMIC = {"id": 11, "name": "New releases", "isPublic": False, "isWatchlist": False, "isDynamicList": True, "externalSource": None, "ownerUsername": None, "itemCount": 2}
IMPORTED = {"id": 12, "name": "From MDBList", "isPublic": False, "isWatchlist": False, "isDynamicList": False, "externalSource": "mdblist", "ownerUsername": None, "itemCount": 0}
SHARED_IN_INDEX = {"id": 13, "name": "Family", "isPublic": False, "isWatchlist": False, "isDynamicList": False, "externalSource": None, "isCollaborative": True, "ownerUsername": "mum", "itemCount": 0}
ROWS = [
    {"id": 1, "tmdbId": 949, "type": "movie", "title": "Heat", "releaseDate": "1995-12-15", "position": None},
    {"id": 2, "tmdbId": 1396, "type": "show", "title": "Breaking Bad", "position": None},
    {"id": 3, "tmdbId": 1396, "type": "season", "title": "Breaking Bad", "part": {"season": 0, "episode": None}},
    {"id": 4, "tmdbId": 1396, "type": "episode", "title": "Breaking Bad", "part": {"season": 1, "episode": 2}},
]
PUBLIC = {"id": 446, "name": "Fantasy/Sci-Fi", "isPublic": True, "isWatchlist": False, "isDynamicList": False, "externalSource": "mdblist",
          "ownerUsername": None, "itemCount": 2, "isOwner": False, "isCollaborator": False,
          "items": [{"id": 9, "tmdbId": 603, "type": "movie", "title": "The Matrix", "position": 0}, {"id": 8, "tmdbId": 1399, "type": "show", "title": "GoT", "position": 1}]}


class _Resp:
    def __init__(self, status: int, data: Any) -> None:
        self.status_code, self._data = status, data
        self.text = json.dumps(data) if data is not None else ""
        self.headers: dict[str, str] = {}

    def json(self) -> Any:
        return self._data


def _adapter(monkeypatch: pytest.MonkeyPatch, routes: dict[tuple[str, str], Any]):
    table = {
        ("GET", f"{B}/me/lists"): {"items": [WATCHLIST, MINE, DYNAMIC, IMPORTED, SHARED_IN_INDEX], "nextCursor": None},
        ("GET", f"{B}/lists/10"): {**MINE, "isOwner": True, "ownerUsername": "cenodude", "itemCount": 4, "items": ROWS},
        **routes,
    }
    calls: list[tuple[str, str, Any, Any]] = []

    def fake(adapter: Any, method: str, url: str, **kwargs: Any) -> _Resp:
        calls.append((method, url, kwargs.get("params"), kwargs.get("json")))
        body = table.get((method, url))
        if callable(body):
            body = body(kwargs)
        if body is None:
            return _Resp(404, {"error": "not_found", "message": "Not found"})
        return body if isinstance(body, _Resp) else _Resp(200, body)

    monkeypatch.setattr(pl, "punchplay_request", fake)
    monkeypatch.setattr(pl, "cfg_section", lambda adapter: {"username": "cenodude"})
    return SimpleNamespace(instance_id="default"), calls


def test_manifest_declares_playlists():
    assert supports_playlists(mod.OPS)
    caps = playlist_capabilities(mod.OPS)
    assert caps["resolve"] and urlparse(caps["link_example"]).hostname == "punchplay.tv"
    assert mod.get_manifest()["features"]["playlists"] is True


def test_resources_classify_own_dynamic_imported_and_shared(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {})
    res = {r.id: r for r in pl.list_resources(adapter)}
    assert list(res) == ["2198", "10", "11", "12", "13"]
    assert res["2198"].can_add and res["2198"].extra["builtin"] and not res["2198"].extra["can_delete"] and not res["2198"].extra["can_rename"]
    assert res["10"].can_add and res["10"].extra["can_rename"] and res["10"].extra["can_delete"]
    assert res["11"].is_smart and not res["11"].can_add
    assert not res["12"].can_add and res["12"].extra["external_source"] == "mdblist" and not res["12"].extra["can_rename"]
    assert res["13"].can_add and res["13"].extra["endpoint_type"] == "collaboration" and res["13"].name == "Family by mum" and not res["13"].extra["can_delete"]


def test_rejected_token_on_the_index_is_an_auth_error(monkeypatch):
    adapter, calls = _adapter(monkeypatch, {("GET", f"{B}/me/lists"): _Resp(401, {"error": "invalid_token"}), ("GET", f"{B}/lists/446"): PUBLIC})
    for call in (lambda: pl.list_resources(adapter), lambda: pl.resolve(adapter, "446"), lambda: pl.get_snapshot(adapter, "446")):
        with pytest.raises(PlaylistAuthError):
            call()
    assert not [c for c in calls if c[1].endswith("/lists/446")]


def test_private_list_401_after_a_good_index_is_unavailable(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/lists/50"): _Resp(401, {"error": "invalid_token"})})
    with pytest.raises(PlaylistUnavailableError) as err:
        pl.resolve(adapter, "https://punchplay.tv/lists/50")
    assert not isinstance(err.value, PlaylistAuthError)


def test_own_snapshot_maps_movies_shows_seasons_and_episodes(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {})
    snap = pl.get_snapshot(adapter, "10")
    assert snap.resource.can_add
    assert [it.key for it in snap.items] == ["tmdb:949", "tmdb:1396#show", "tmdb:1396#season:0", "tmdb:1396#s01e02"]
    assert [it.playlist_item_id for it in snap.items] == ["1", "2", "3", "4"]


@pytest.mark.parametrize("ref", ["https://punchplay.tv/lists/446", "punchplay.tv/lists/446?tab=items", "446"])
def test_resolve_public_list(monkeypatch, ref):
    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/lists/446"): PUBLIC})
    res = pl.resolve(adapter, ref)
    assert res.id == "446" and res.extra["endpoint_type"] == "public_list" and not res.can_add and not res.extra["can_delete"]
    snap = pl.get_snapshot(adapter, "446")
    assert [(it.position, it.item["title"]) for it in snap.items] == [(0, "The Matrix"), (1, "GoT")]


def test_resolve_shared_list_from_detail_is_writable(monkeypatch):
    shared = {**PUBLIC, "id": 77, "externalSource": None, "isCollaborator": True, "ownerUsername": "mum", "items": []}
    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/lists/77"): shared, ("POST", f"{B}/lists/77/items"): _Resp(201, {"id": 5})})
    res = pl.resolve(adapter, "77")
    assert res.extra["endpoint_type"] == "collaboration" and res.can_add and res.name.endswith("by mum")
    assert pl.add(adapter, "77", [{"type": "movie", "ids": {"tmdb": 949}}])["confirmed_keys"] == ["tmdb:949"]


@pytest.mark.parametrize("ref", ["https://trakt.tv/lists/446", "https://punchplay.tv/discover/lists", "punchplay.tv/list/446", "punchplay.tv/lists/abc", ""])
def test_resolve_rejects_other_links(monkeypatch, ref):
    adapter, _ = _adapter(monkeypatch, {})
    with pytest.raises(PlaylistLinkError):
        pl.resolve(adapter, ref)


@pytest.mark.parametrize("lid", ["446", "11", "12"])
def test_writes_to_public_dynamic_and_imported_lists_are_refused(monkeypatch, lid):
    adapter, calls = _adapter(monkeypatch, {("GET", f"{B}/lists/446"): PUBLIC})
    for op in (pl.add, pl.remove):
        with pytest.raises(PlaylistReadOnlyError):
            op(adapter, lid, [{"type": "movie", "ids": {"tmdb": 949}}])
    assert not [c for c in calls if c[0] != "GET"]


def test_add_confirms_new_and_existing_and_skips_unsupported(monkeypatch):
    def add(kwargs):
        tmdb = kwargs["json"]["sourceId"]
        return _Resp(201 if tmdb == 949 else 200, {"id": 100 + tmdb}) if tmdb in (949, 807) else _Resp(200, {})

    adapter, calls = _adapter(monkeypatch, {("POST", f"{B}/lists/10/items"): add})
    items = [
        {"type": "movie", "title": "Heat", "ids": {"tmdb": 949}},
        {"type": "movie", "title": "Se7en", "ids": {"tmdb": 807}},
        {"type": "movie", "title": "Odd", "ids": {"tmdb": 155}},
        {"type": "movie", "title": "No tmdb", "ids": {"imdb": "tt0000001"}},
        {"type": "episode", "show_ids": {"tmdb": 1396}, "season": 1, "episode": 1, "ids": {}},
    ]
    out = pl.add(adapter, "10", items)
    assert out["confirmed_keys"] == ["tmdb:949", "tmdb:807"]
    assert sorted(u["hint"] for u in out["unresolved"]) == ["missing_tmdb_id", "unsupported_type", "write_not_verified"]
    assert [c[3] for c in calls if c[0] == "POST"][0] == {"kind": "movie", "sourceId": 949, "title": "Heat"}


def test_remove_uses_list_item_ids(monkeypatch):
    adapter, calls = _adapter(monkeypatch, {("DELETE", f"{B}/lists/10/items/1"): _Resp(200, None)})
    out = pl.remove(adapter, "10", [{"type": "movie", "ids": {"tmdb": 949}}, {"type": "movie", "ids": {"tmdb": 155}}])
    assert out["confirmed_keys"] == ["tmdb:949"] and out["unresolved"][0]["hint"] == "missing_remote_item_id"


def test_incomplete_or_malformed_reads_fail(monkeypatch):
    short = {**MINE, "isOwner": True, "itemCount": 5, "items": ROWS}
    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/lists/10"): short})
    with pytest.raises(pl.PunchPlayPlaylistError):
        pl.get_snapshot(adapter, "10")
    broken = {**MINE, "isOwner": True, "itemCount": 2, "items": [ROWS[0], None]}
    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/lists/10"): broken})
    with pytest.raises(pl.PunchPlayPlaylistError):
        pl.get_snapshot(adapter, "10")


def test_dynamic_list_pages_items_and_checks_total(monkeypatch):
    def items(kwargs):
        offset = kwargs["params"]["offset"]
        page = ROWS[offset:offset + 1]
        return {"items": page, "total": 2, "nextOffset": offset + 1 if offset + 1 < 2 else None}

    routes = {("GET", f"{B}/lists/11"): {**DYNAMIC, "isOwner": True, "items": []}, ("GET", f"{B}/lists/11/items"): items}
    adapter, calls = _adapter(monkeypatch, routes)
    snap = pl.get_snapshot(adapter, "11")
    assert [it.item["title"] for it in snap.items] == ["Heat", "Breaking Bad"]
    assert [c[2]["offset"] for c in calls if c[1].endswith("/lists/11/items")] == [0, 1]


def test_create_is_private_rename_and_delete_follow_ownership(monkeypatch):
    remembered: list[str] = []
    monkeypatch.setattr(pl, "remember_resource", lambda reader, ad, res: remembered.append(res.id))
    created = {"id": 900, "name": "New", "isPublic": False, "isWatchlist": False, "isDynamicList": False, "externalSource": None, "itemCount": 0}
    routes = {("POST", f"{B}/lists"): _Resp(201, created), ("PATCH", f"{B}/lists/10"): {"id": 10}, ("DELETE", f"{B}/lists/10"): _Resp(200, None)}
    adapter, calls = _adapter(monkeypatch, routes)
    res = pl.create(adapter, "New")
    assert res.id == "900" and remembered == ["900"]
    assert ("POST", f"{B}/lists", None, {"name": "New", "description": "", "isPublic": False}) in calls
    assert pl.rename(adapter, "10", "Heists 2").name == "Heists 2"
    assert pl.delete(adapter, "10") == {"ok": True, "count": 1}
    for lid in ("2198", "12", "13"):
        with pytest.raises(PlaylistReadOnlyError):
            pl.delete(adapter, lid)
    with pytest.raises(PlaylistReadOnlyError):
        pl.rename(adapter, "2198", "x")


@pytest.mark.parametrize("bad", [
    {"id": 7, "tmdbId": 1396, "type": "episode", "title": "Breaking Bad"},
    {"id": 7, "tmdbId": 1396, "type": "episode", "title": "Breaking Bad", "part": {"season": 1, "episode": None}},
    {"id": 7, "tmdbId": 1396, "type": "season", "title": "Breaking Bad", "part": {"season": None}},
    {"id": 7, "tmdbId": None, "type": "movie", "title": "No id"},
])
def test_unparseable_supported_item_fails_the_snapshot(monkeypatch, bad):
    detail = {**MINE, "isOwner": True, "itemCount": 2, "items": [ROWS[0], bad]}
    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/lists/10"): detail})
    with pytest.raises(pl.PunchPlayPlaylistError, match="invalid_list_item"):
        pl.get_snapshot(adapter, "10")


def test_unknown_item_types_are_skipped(monkeypatch):
    detail = {**MINE, "isOwner": True, "itemCount": 2, "items": [ROWS[0], {"id": 8, "tmdbId": 287, "type": "person", "title": "Brad Pitt"}]}
    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/lists/10"): detail})
    assert [it.key for it in pl.get_snapshot(adapter, "10").items] == ["tmdb:949"]
