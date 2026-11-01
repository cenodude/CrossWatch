# /tests/test_scrob_playlists.py
# Scrob lists as playlists: own lists, seasons, public lists on the same server and confirmed writes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from cw_platform.playlists import PlaylistAuthError, PlaylistLinkError, PlaylistReadOnlyError, PlaylistUnavailableError, playlist_capabilities, supports_playlists
from providers.sync import _mod_SCROB as mod
from providers.sync.scrob import _playlists as pl

WATCHLIST = {"id": 4, "name": "Watchlist", "privacy_level": "private", "item_count": 0}
MINE = {"id": 5, "name": "Heists", "privacy_level": "private", "item_count": 4}
ROWS = [
    {"id": 12, "sort_order": 0, "media": {"id": 1, "type": "movie", "tmdb_id": 949, "imdb_id": "tt0113277", "title": "Heat", "release_date": "1995-12-15"}},
    {"id": 13, "sort_order": 0, "media": {"id": 2, "type": "series", "tmdb_id": 1396, "tvdb_id": 81189, "title": "Breaking Bad", "season_number": None}},
    {"id": 14, "sort_order": 0, "media": {"id": 2, "type": "series", "tmdb_id": 1396, "title": "Breaking Bad", "season_number": 2, "show_title": "Breaking Bad"}},
    {"id": 15, "sort_order": 0, "media": {"id": 3, "type": "episode", "tmdb_id": 62085, "title": "Pilot", "season_number": 1, "episode_number": 1, "show_tmdb_id": 1396, "show_title": "Breaking Bad"}},
    {"id": 16, "sort_order": 0, "media": {"id": 4, "type": "person", "tmdb_id": 287, "title": "Brad Pitt"}},
]
PUBLIC = {"id": 77, "name": "Noir", "privacy_level": "public", "item_count": 1, "is_owner": False, "items": [ROWS[0]]}


class _Resp:
    def __init__(self, status: int, data: Any = None) -> None:
        self.status_code, self._data = status, data
        self.text = json.dumps(data) if data is not None else ""

    def json(self) -> Any:
        return self._data


def _adapter(monkeypatch: pytest.MonkeyPatch, routes: dict[tuple[str, str], Any]):
    table = {
        ("GET", "lists"): {"lists": [WATCHLIST, MINE]},
        ("GET", "lists/5"): {**MINE, "item_count": 5, "is_owner": True, "items": ROWS},
        **routes,
    }
    calls: list[tuple[str, str, Any]] = []

    def fake(adapter: Any, method: str, path: str, **kwargs: Any) -> _Resp:
        calls.append((method, path, kwargs.get("json")))
        body = table.get((method, path))
        if callable(body):
            body = body(kwargs)
        if body is None:
            return _Resp(404, {"detail": "List not found"})
        return body if isinstance(body, _Resp) else _Resp(200, body)

    monkeypatch.setattr(pl, "scrob_request", fake)
    monkeypatch.setattr(pl, "cfg_section", lambda adapter: {"server_url": "http://192.168.2.163:7330"})
    monkeypatch.setattr(pl, "watchlist_name", lambda adapter: "Watchlist")
    return SimpleNamespace(instance_id="default"), calls


def test_manifest_declares_playlists():
    assert supports_playlists(mod.OPS)
    caps = playlist_capabilities(mod.OPS)
    assert caps["resolve"] and caps["reorder"] is False
    assert mod.get_manifest()["features"]["playlists"] is True


def test_resources_mark_the_watchlist_builtin(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {})
    res = {r.id: r for r in pl.list_resources(adapter)}
    assert res["4"].extra["builtin"] and not res["4"].extra["can_delete"] and res["4"].can_add
    assert res["5"].extra["can_rename"] and res["5"].extra["can_delete"] and not res["5"].can_reorder


def test_snapshot_maps_movies_shows_seasons_and_episodes(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {})
    snap = pl.get_snapshot(adapter, "5")
    assert [it.key for it in snap.items] == ["tmdb:949", "tmdb:1396#show", "tmdb:1396#season:2", "tmdb:1396#s01e01"]
    assert [it.playlist_item_id for it in snap.items] == ["12", "13", "14", "15"]


@pytest.mark.parametrize("bad", [
    {"id": 20, "media": {"type": "movie", "title": "No ids"}},
    {"id": 20, "media": {"type": "series", "tmdb_id": 1396, "season_number": -1}},
    {"id": 20, "media": {"type": "episode", "show_tmdb_id": 1396, "season_number": 1}},
    {"id": 20, "media": {"type": "episode", "season_number": 1, "episode_number": 2}},
    {"id": 20, "media": None},
    {"id": 20},
    {"id": 20, "media": {"tmdb_id": 949}},
    {"id": 20, "media": {"type": "collection", "tmdb_id": 10}},
])
def test_unparseable_items_fail_the_snapshot(monkeypatch, bad):
    adapter, _ = _adapter(monkeypatch, {("GET", "lists/5"): {**MINE, "item_count": 2, "is_owner": True, "items": [ROWS[0], bad]}})
    with pytest.raises(pl.ScrobPlaylistError, match="invalid_list_item"):
        pl.get_snapshot(adapter, "5")


def test_short_or_malformed_reads_fail(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("GET", "lists/5"): {**MINE, "item_count": 9, "is_owner": True, "items": ROWS}})
    with pytest.raises(pl.ScrobPlaylistError):
        pl.get_snapshot(adapter, "5")
    adapter, _ = _adapter(monkeypatch, {("GET", "lists/5"): {**MINE, "item_count": 2, "is_owner": True, "items": [ROWS[0], None]}})
    with pytest.raises(pl.ScrobPlaylistError):
        pl.get_snapshot(adapter, "5")


@pytest.mark.parametrize("ref", ["http://192.168.2.163:7330/list/77", "http://192.168.2.163:7330/lists/77/", "77"])
def test_resolve_public_list_on_the_same_server(monkeypatch, ref):
    adapter, _ = _adapter(monkeypatch, {("GET", "lists/77"): PUBLIC})
    res = pl.resolve(adapter, ref)
    assert res.id == "77" and res.extra["endpoint_type"] == "public_list" and not res.can_add
    snap = pl.get_snapshot(adapter, "77")
    assert [it.key for it in snap.items] == ["tmdb:949"] and not snap.resource.can_add


@pytest.mark.parametrize("ref", ["http://other-server:7330/list/77", "http://192.168.2.163:7440/list/77", "http://192.168.2.163/list/77", "https://192.168.2.163:7330/list/77", "https://trakt.tv/lists/77", "http://192.168.2.163:7330/movie/77", "list/77", ""])
def test_resolve_rejects_other_links(monkeypatch, ref):
    adapter, _ = _adapter(monkeypatch, {})
    with pytest.raises(PlaylistLinkError):
        pl.resolve(adapter, ref)


@pytest.mark.parametrize("status", [403, 404])
def test_private_or_missing_lists_are_unavailable(monkeypatch, status):
    adapter, _ = _adapter(monkeypatch, {("GET", "lists/88"): _Resp(status, {"detail": "This list is private"})})
    with pytest.raises(PlaylistUnavailableError):
        pl.resolve(adapter, "88")


def test_rejected_login_is_an_auth_error(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("GET", "lists"): _Resp(401, {"detail": "Not authenticated"})})
    with pytest.raises(PlaylistAuthError):
        pl.list_resources(adapter)


def test_failed_list_index_does_not_log_upstream_detail(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("GET", "lists"): _Resp(500, {"detail": "secret upstream url"})})
    logged: list[dict[str, Any]] = []
    monkeypatch.setattr(pl, "_warn", lambda feature, event, **fields: logged.append(fields))
    with pytest.raises(pl.ScrobPlaylistError):
        pl.list_resources(adapter)
    assert logged == [{"status": 500}]


def test_writes_to_public_or_unknown_lists_are_refused(monkeypatch):
    adapter, calls = _adapter(monkeypatch, {("GET", "lists/77"): PUBLIC})
    for lid in ("77", "99"):
        for op in (pl.add, pl.remove):
            with pytest.raises(PlaylistReadOnlyError):
                op(adapter, lid, [{"type": "movie", "ids": {"tmdb": 949}}])
    assert not [c for c in calls if c[0] != "GET"]


def test_add_confirms_matching_answers_and_skips_episodes(monkeypatch):
    def add(kwargs):
        body = kwargs["json"]
        if body.get("tmdb_id") == 807:
            return _Resp(409, {"detail": "Item already in list"})
        if body.get("tmdb_id") == 155:
            return _Resp(201, {"id": 30, "media": {"type": "series", "tmdb_id": 155}})
        if body.get("tmdb_id") == 999:
            return _Resp(404, {"detail": "Media not found: secret url"})
        return _Resp(201, {"id": 31, "media": {"type": body["media_type"], "tmdb_id": body["tmdb_id"], "season_number": body.get("season_number")}})

    adapter, calls = _adapter(monkeypatch, {("POST", "lists/5/items"): add})
    items = [
        {"type": "movie", "ids": {"tmdb": 949}},
        {"type": "movie", "ids": {"tmdb": 807}},
        {"type": "movie", "ids": {"tmdb": 155}},
        {"type": "movie", "ids": {"tmdb": 999}},
        {"type": "season", "show_ids": {"tmdb": 1396}, "season": 2, "ids": {}},
        {"type": "episode", "show_ids": {"tmdb": 1396}, "season": 1, "episode": 1, "ids": {}},
        {"type": "movie", "ids": {"imdb": "tt0000001"}},
    ]
    out = pl.add(adapter, "5", items)
    assert out["confirmed_keys"] == ["tmdb:949", "tmdb:807", "tmdb:1396#season:2"]
    assert sorted(u["hint"] for u in out["unresolved"]) == ["missing_tmdb_id", "not_found", "unsupported_type", "write_not_verified"]
    assert [c[2] for c in calls if c[0] == "POST"][-1] == {"tmdb_id": 1396, "media_type": "series", "season_number": 2}


def test_remove_uses_list_item_ids(monkeypatch):
    adapter, calls = _adapter(monkeypatch, {("DELETE", "lists/5/items/12"): {"message": "Item removed"}})
    out = pl.remove(adapter, "5", [{"type": "movie", "ids": {"tmdb": 949}}, {"type": "movie", "ids": {"tmdb": 155}}])
    assert out["confirmed_keys"] == ["tmdb:949"] and out["unresolved"][0]["hint"] == "missing_remote_item_id"


def test_create_is_private_and_rename_delete_follow_ownership(monkeypatch):
    remembered: list[str] = []
    monkeypatch.setattr(pl, "remember_resource", lambda reader, ad, res: remembered.append(res.id))
    routes = {
        ("POST", "lists"): _Resp(201, {"id": 9, "name": "New", "privacy_level": "private", "item_count": 0}),
        ("PATCH", "lists/5"): {**MINE, "name": "Heists 2"},
        ("DELETE", "lists/5"): {"message": "List deleted"},
    }
    adapter, calls = _adapter(monkeypatch, routes)
    res = pl.create(adapter, "New")
    assert res.id == "9" and remembered == ["9"]
    assert ("POST", "lists", {"name": "New", "privacy_level": "private"}) in calls
    assert pl.rename(adapter, "5", "Heists 2").name == "Heists 2"
    assert pl.delete(adapter, "5") == {"ok": True, "count": 1}
    with pytest.raises(PlaylistReadOnlyError):
        pl.delete(adapter, "4")
    with pytest.raises(PlaylistReadOnlyError):
        pl.rename(adapter, "77", "x")
