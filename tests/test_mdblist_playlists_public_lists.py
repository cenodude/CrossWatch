# /tests/test_mdblist_playlists_public_lists.py
# MDBList playlist snapshots for own, liked and public lists
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from types import SimpleNamespace

import pytest

from cw_platform.playlists import PlaylistLinkError, PlaylistReadOnlyError, PlaylistUnavailableError
from providers.sync.mdblist import _playlists as pl

B = pl.BASE
OWN = [{"id": 37673, "name": "MCU", "slug": "mcu", "dynamic": False, "items": 59, "user_name": "me", "last_updated_at": "2026-10-01T00:00:00.000Z"}]
LIKED_ROW = {"id": 2194, "name": "Latest TV Shows", "slug": "latest-tv-shows", "dynamic": True, "mediatype": "show", "items": 300, "user_name": "gary", "last_updated_at": "2026-10-30T03:28:13.000Z"}
STATIC_LIKED = {"id": 121077, "name": "Noir", "slug": "noir", "dynamic": False, "items": 2, "user_name": "wave", "last_updated_at": "2026-09-01T00:00:00.000Z"}
PUBLIC = {"id": 555, "name": "Heists", "slug": "heists", "dynamic": False, "items": 1, "private": False, "user_name": "snoak", "updated": "2026-10-02T00:00:00.000Z"}
ITEM = {"id": 949, "rank": 1, "mediatype": "movie", "title": "Heat", "release_year": 1995, "ids": {"imdb": "tt0113277", "tmdb": 949}}


class _Resp:
    def __init__(self, body, status=200, headers=None):
        self._body, self.status_code, self.text, self.headers = body, status, "x", dict(headers or {})

    def json(self):
        return self._body


def _liked(*rows):
    return {"lists": list(rows), "pagination": {"page": 1, "limit": 100, "offset": 0, "total": len(rows), "has_more": False}}


@pytest.fixture(autouse=True)
def _clear_memo():
    pl._SNAPSHOT_MEMO.clear()
    yield
    pl._SNAPSHOT_MEMO.clear()


def _adapter(monkeypatch, routes):
    table = {("GET", f"{B}/lists/user"): OWN, ("GET", f"{B}/lists/liked"): _liked(LIKED_ROW, STATIC_LIKED, OWN[0]), **routes}
    calls = []

    def fake(adapter, method, url, **kwargs):
        calls.append((method, url))
        body = table.get((method, url))
        if isinstance(body, _Resp):
            return body
        if body is None:
            if method != "GET":
                raise AssertionError(f"unexpected write {method} {url}")
            return _Resp({"error": "Not found"}, 404)
        return _Resp(body)

    monkeypatch.setattr(pl, "mdblist_request", fake)
    monkeypatch.setattr(pl, "has_auth", lambda cfg: True)
    monkeypatch.setattr(pl, "cfg_section", lambda adapter: {})
    return SimpleNamespace(instance_id="default"), calls


def test_liked_lists_are_listed_read_only(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {})
    res = {r.id: r for r in pl.list_resources(adapter)}
    assert list(res) == ["__watchlist__", "37673", "2194", "121077"]
    own = res["37673"]
    assert own.can_add and own.name == "MCU" and own.extra["updated_at"] == "2026-10-01T00:00:00.000Z"
    static = res["121077"]
    assert static.name == "Noir by wave" and not static.can_add and not static.can_remove
    assert static.extra["endpoint_type"] == "liked_list" and static.extra["public_list"] and not static.extra["can_delete"]
    dynamic = res["2194"]
    assert dynamic.is_smart and dynamic.is_discovery and dynamic.media_types == ("show",)
    assert dynamic.extra["endpoint_type"] == "liked_list" and not dynamic.can_add


def test_own_list_discovery_failure_raises(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/lists/user"): _Resp({}, 500)})
    with pytest.raises(pl.MDBListPlaylistError):
        pl.list_resources(adapter)


def test_liked_failure_keeps_own_lists(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/lists/liked"): _Resp({}, 500)})
    assert [r.id for r in pl.list_resources(adapter)] == ["__watchlist__", "37673"]


@pytest.mark.parametrize("ref", ["https://mdblist.com/lists/snoak/heists", "mdblist.com/lists/snoak/heists/", "snoak/heists"])
def test_resolve_website_link(monkeypatch, ref):
    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/lists/snoak/heists"): [PUBLIC]})
    res = pl.resolve(adapter, ref)
    assert res.id == "555" and res.name == "Heists by snoak"
    assert res.extra["endpoint_type"] == "public_list" and not res.can_add


def test_resolve_numeric_id_and_own_list(monkeypatch):
    adapter, calls = _adapter(monkeypatch, {("GET", f"{B}/lists/555"): [PUBLIC]})
    assert pl.resolve(adapter, "555").name == "Heists by snoak"
    own = pl.resolve(adapter, "https://mdblist.com/lists/37673")
    assert own.can_add and own.name == "MCU"
    assert ("GET", f"{B}/lists/37673") not in calls


def test_resolve_liked_link_returns_listed_resource(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/lists/gary/latest-tv-shows"): [LIKED_ROW]})
    assert pl.resolve(adapter, "https://mdblist.com/lists/gary/latest-tv-shows").extra["endpoint_type"] == "liked_list"


def test_resolve_private_or_missing(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/lists/snoak/secret"): [{**PUBLIC, "id": 9, "private": True}]})
    with pytest.raises(PlaylistUnavailableError):
        pl.resolve(adapter, "mdblist.com/lists/snoak/secret")
    with pytest.raises(PlaylistUnavailableError):
        pl.resolve(adapter, "mdblist.com/lists/snoak/gone")


@pytest.mark.parametrize("ref", ["https://trakt.tv/lists/555", "https://mdblist.com/lists/user/snoak", "mdblist.com/lists/official/popular", "a/b/c", ""])
def test_resolve_rejects_unsupported_links(monkeypatch, ref):
    adapter, _ = _adapter(monkeypatch, {})
    with pytest.raises(PlaylistLinkError):
        pl.resolve(adapter, ref)


def test_snapshot_of_public_list_by_id(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/lists/555"): [PUBLIC], ("GET", f"{B}/lists/555/items"): [ITEM]})
    snap = pl.get_snapshot(adapter, "555")
    assert snap.resource.name == "Heists by snoak" and not snap.resource.can_add
    assert [it.item.get("title") for it in snap.items] == ["Heat"]


def test_snapshot_unavailable_list(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {})
    with pytest.raises(PlaylistUnavailableError):
        pl.get_snapshot(adapter, "999")


def test_unchanged_list_is_not_read_again(monkeypatch):
    adapter, calls = _adapter(monkeypatch, {("GET", f"{B}/lists/121077/items"): [ITEM]})
    first = pl.get_snapshot(adapter, "121077")
    second = pl.get_snapshot(adapter, "121077")
    assert [it.item.get("title") for it in second.items] == [it.item.get("title") for it in first.items] == ["Heat"]
    assert calls.count(("GET", f"{B}/lists/121077/items")) == 1


def test_write_drops_memo(monkeypatch):
    routes = {("GET", f"{B}/lists/37673/items"): [ITEM], ("POST", f"{B}/lists/37673/items/add"): {"added": {"movies": 1}}}
    adapter, calls = _adapter(monkeypatch, routes)
    pl.get_snapshot(adapter, "37673")
    pl.add(adapter, "37673", [{"type": "movie", "title": "Se7en", "ids": {"tmdb": 807}}])
    pl.get_snapshot(adapter, "37673")
    assert calls.count(("GET", f"{B}/lists/37673/items")) == 2


@pytest.mark.parametrize("lid", ["121077", "2194"])
def test_writes_to_liked_lists_are_refused(monkeypatch, lid):
    adapter, _ = _adapter(monkeypatch, {})
    with pytest.raises(PlaylistReadOnlyError):
        pl.add(adapter, lid, [{"type": "movie", "ids": {"tmdb": 807}}])
    with pytest.raises(pl.MDBListNotOwnedError):
        pl.rename(adapter, lid, "Mine")
