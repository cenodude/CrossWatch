# /tests/test_flicklist_public_lists.py
# FlickList playlists: public lists by link, read only, completeness and unchanged-list memo
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import json
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest

from cw_platform.playlists import PlaylistLinkError, PlaylistReadOnlyError, PlaylistUnavailableError, playlist_capabilities
from providers.sync import _mod_FLICKLIST as mod
from providers.sync.flicklist import _playlists as pl

B = pl.API_V3_BASE
OWN = {"id": 10, "name": "Mine", "slug": "mine", "privacy": "private", "is_smart": False, "item_count": 1, "updated_at": "2026-11-01T00:00:00Z"}
SMART = {"id": 11, "name": "Smart", "slug": "smart", "is_smart": True, "item_count": 0}
PUBLIC = {"id": 2398, "name": "DC Extended Universe", "slug": "dc-extended-universe", "privacy": "public", "is_smart": False, "item_count": 2,
          "updated_at": "2026-10-27T19:47:27.788Z", "owner": {"username": "journeyover"}}


def _row(n, title):
    return {"position": n, "title": title, "year": 2000 + n, "media_type": "movie", "ids": {"fldb": f"flm_{n}", "tmdb": 100 + n, "imdb": f"tt{1000000 + n}"}}


class _Resp:
    def __init__(self, status, payload=None, headers=None):
        self.status_code, self._payload, self.headers = status, payload, dict(headers or {})
        self.text = json.dumps(payload) if payload is not None else ""

    def json(self):
        return self._payload


def _pages(rows, per_page=1):
    def answer(params):
        page = int(params.get("page") or 1)
        chunk = rows[(page - 1) * per_page: page * per_page]
        count = max(1, -(-len(rows) // per_page))
        return _Resp(200, chunk, {"x-flicklist-page-count": str(count), "x-flicklist-item-count": str(len(rows))})
    return answer


@pytest.fixture(autouse=True)
def _clear_memo():
    pl._SNAPSHOT_MEMO.clear()
    yield
    pl._SNAPSHOT_MEMO.clear()


def _adapter(monkeypatch, routes):
    table = {("GET", f"{B}/sync/lists"): [OWN, SMART], **routes}
    calls = []

    def fake(adapter, method, url, **kwargs):
        calls.append((method, url, dict(kwargs.get("params") or {}), kwargs.get("json")))
        body = table.get((method, url))
        if callable(body):
            body = body(kwargs.get("params") or {})
        if body is None:
            return _Resp(404, {"error": "not_found", "detail": "List not found"})
        return body if isinstance(body, _Resp) else _Resp(200, body)

    monkeypatch.setattr(pl, "flicklist_request", fake)
    monkeypatch.setattr(pl, "cfg_section", lambda adapter: {"username": "pascalvandenberg"})
    return SimpleNamespace(instance_id="default"), calls


def test_capabilities_offer_link_resolve():
    caps = playlist_capabilities(mod.OPS)
    assert caps["resolve"] and urlparse(caps["link_example"]).hostname == "flicklist.tv"
    assert callable(getattr(mod.OPS, "resolve_playlist", None))


@pytest.mark.parametrize("ref", [
    "https://flicklist.tv/u/journeyover/lists/dc-extended-universe",
    "flicklist.tv/u/journeyover/lists/dc-extended-universe?x=1",
    "https://www.flicklist.tv/u/journeyover/lists/dc-extended-universe/",
])
def test_resolve_profile_link(monkeypatch, ref):
    def user_lists(params):
        return _Resp(200, [{**PUBLIC, "id": 1, "slug": "other"}, PUBLIC], {"x-flicklist-page-count": "1"})

    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/users/journeyover/lists"): user_lists})
    res = pl.resolve(adapter, ref)
    assert res.id == "2398" and res.name == "DC Extended Universe by journeyover"
    assert res.extra["endpoint_type"] == "public_list" and not res.can_add and not res.extra["can_delete"]


@pytest.mark.parametrize("ref", ["2398", "https://flicklist.tv/lists/2398"])
def test_resolve_numeric_id(monkeypatch, ref):
    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/lists/2398"): PUBLIC})
    assert pl.resolve(adapter, ref).name == "DC Extended Universe by journeyover"


def test_resolve_own_links_return_own_lists(monkeypatch):
    adapter, calls = _adapter(monkeypatch, {})
    assert pl.resolve(adapter, "https://flicklist.tv/u/PascalvandenBerg/lists/mine").can_add
    assert pl.resolve(adapter, "10").can_add
    assert not [c for c in calls if "/users/" in c[1] or c[1].endswith("/lists/10")]


def test_resolve_missing_private_or_unknown(monkeypatch):
    routes = {("GET", f"{B}/users/journeyover/lists"): _Resp(200, [PUBLIC], {"x-flicklist-page-count": "1"})}
    adapter, _ = _adapter(monkeypatch, routes)
    with pytest.raises(PlaylistUnavailableError):
        pl.resolve(adapter, "flicklist.tv/u/journeyover/lists/secret")
    with pytest.raises(PlaylistUnavailableError):
        pl.resolve(adapter, "flicklist.tv/u/nobody/lists/x")
    with pytest.raises(PlaylistUnavailableError):
        pl.resolve(adapter, "99999")


@pytest.mark.parametrize("ref", ["https://trakt.tv/lists/2398", "https://flicklist.tv/journeyover/dc-extended-universe", "flicklist.tv/lists/abc", "flicklist.tv/movies/1", ""])
def test_resolve_rejects_other_links(monkeypatch, ref):
    adapter, _ = _adapter(monkeypatch, {})
    with pytest.raises(PlaylistLinkError):
        pl.resolve(adapter, ref)


def test_public_snapshot_pages_in_order_and_is_read_only(monkeypatch):
    rows = [_row(0, "Wonder Woman"), _row(1, "Aquaman")]
    adapter, calls = _adapter(monkeypatch, {("GET", f"{B}/lists/2398"): PUBLIC, ("GET", f"{B}/lists/2398/items"): _pages(rows)})
    snap = pl.get_snapshot(adapter, "2398")
    assert [(it.position, it.item["title"]) for it in snap.items] == [(0, "Wonder Woman"), (1, "Aquaman")]
    assert snap.resource.name == "DC Extended Universe by journeyover" and not snap.resource.can_add
    assert [c[2]["page"] for c in calls if c[1].endswith("/lists/2398/items")] == [1, 2]
    assert not [c for c in calls if "/sync/lists/2398" in c[1]]


def test_unknown_list_is_never_writable(monkeypatch):
    adapter, calls = _adapter(monkeypatch, {})
    with pytest.raises(PlaylistUnavailableError):
        pl.get_snapshot(adapter, "555")
    for op in (pl.add, pl.remove):
        with pytest.raises(PlaylistReadOnlyError):
            op(adapter, "2398", [{"type": "movie", "ids": {"tmdb": 949}}])
    with pytest.raises(PlaylistReadOnlyError):
        pl.add(adapter, "11", [{"type": "movie", "ids": {"tmdb": 949}}])
    with pytest.raises(PlaylistReadOnlyError):
        pl.delete(adapter, "2398")
    assert not [c for c in calls if c[0] != "GET"]


@pytest.mark.parametrize("bad", [
    _Resp(200, [None], {"x-flicklist-page-count": "2", "x-flicklist-item-count": "2"}),
    _Resp(200, {"items": []}, {"x-flicklist-page-count": "2"}),
    _Resp(200, [], {"x-flicklist-page-count": "3", "x-flicklist-item-count": "3"}),
])
def test_incomplete_public_reads_fail_and_are_not_cached(monkeypatch, bad):
    rows = [_row(0, "A"), _row(1, "B")]
    state = {"broken": True}
    good = _pages(rows)

    def items(params):
        if int(params.get("page") or 1) == 2 and state["broken"]:
            return bad
        return good(params)

    adapter, calls = _adapter(monkeypatch, {("GET", f"{B}/lists/2398"): PUBLIC, ("GET", f"{B}/lists/2398/items"): items})
    with pytest.raises(pl.FlickListPlaylistError):
        pl.get_snapshot(adapter, "2398")
    state["broken"] = False
    assert [it.item["title"] for it in pl.get_snapshot(adapter, "2398").items] == ["A", "B"]


def test_short_read_against_item_count_fails(monkeypatch):
    short = _Resp(200, [_row(0, "A")], {"x-flicklist-page-count": "1", "x-flicklist-item-count": "2"})
    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/lists/2398"): PUBLIC, ("GET", f"{B}/lists/2398/items"): short})
    with pytest.raises(pl.FlickListPlaylistError):
        pl.get_snapshot(adapter, "2398")


def test_private_public_list_is_unavailable(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/lists/2398"): PUBLIC})
    with pytest.raises(PlaylistUnavailableError):
        pl.get_snapshot(adapter, "2398")


def test_unchanged_list_is_not_read_again_and_writes_drop_the_memo(monkeypatch):
    own_items = [_row(0, "Heat")]
    routes = {
        ("GET", f"{B}/lists/2398"): PUBLIC,
        ("GET", f"{B}/lists/2398/items"): _pages([_row(0, "A")]),
        ("GET", f"{B}/sync/lists/10/items"): own_items,
        ("POST", f"{B}/sync/lists/10/items"): {"added": 1, "existing": 0, "not_found": []},
    }
    adapter, calls = _adapter(monkeypatch, routes)
    pl.get_snapshot(adapter, "2398")
    pl.get_snapshot(adapter, "2398")
    assert len([c for c in calls if c[1].endswith("/lists/2398/items")]) == 1
    pl.get_snapshot(adapter, "10")
    pl.get_snapshot(adapter, "10")
    assert len([c for c in calls if c[1].endswith("/sync/lists/10/items") and c[0] == "GET"]) == 1
    pl.add(adapter, "10", [{"type": "movie", "ids": {"tmdb": 807}}])
    pl.get_snapshot(adapter, "10")
    assert len([c for c in calls if c[1].endswith("/sync/lists/10/items") and c[0] == "GET"]) == 2


def test_own_list_that_disappeared_is_unavailable(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {})
    with pytest.raises(PlaylistUnavailableError):
        pl.get_snapshot(adapter, "10")


@pytest.mark.parametrize("bad", [[None], ["x"], {"error": "boom"}])
def test_malformed_own_list_fails_and_is_not_cached(monkeypatch, bad):
    state = {"broken": True}

    def own(params):
        return _Resp(200, bad) if state["broken"] else _Resp(200, [_row(0, "Heat")])

    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/sync/lists/10/items"): own})
    with pytest.raises((pl.FlickListPlaylistError, pl.FlickListError)):
        pl.get_snapshot(adapter, "10")
    state["broken"] = False
    assert [it.item["title"] for it in pl.get_snapshot(adapter, "10").items] == ["Heat"]


def _headerless(rows):
    def answer(params):
        page, limit = int(params.get("page") or 1), int(params.get("limit") or 100)
        return _Resp(200, rows[(page - 1) * limit: page * limit])
    return answer


def test_missing_paging_headers_keep_paging_until_a_short_page(monkeypatch):
    rows = [_row(n, f"T{n}") for n in range(101)]
    adapter, calls = _adapter(monkeypatch, {("GET", f"{B}/lists/2398"): {**PUBLIC, "item_count": 101}, ("GET", f"{B}/lists/2398/items"): _headerless(rows)})
    assert len(pl.get_snapshot(adapter, "2398").items) == 101
    assert [c[2]["page"] for c in calls if c[1].endswith("/lists/2398/items")] == [1, 2]


def test_missing_paging_headers_and_short_total_fail(monkeypatch):
    rows = [_row(n, f"T{n}") for n in range(100)]
    adapter, _ = _adapter(monkeypatch, {("GET", f"{B}/lists/2398"): {**PUBLIC, "item_count": 101}, ("GET", f"{B}/lists/2398/items"): _headerless(rows)})
    with pytest.raises(pl.FlickListPlaylistError):
        pl.get_snapshot(adapter, "2398")


def test_created_list_is_remembered_before_seeding(monkeypatch):
    created = {"id": 900, "name": "New", "slug": "new", "is_smart": False, "item_count": 0}
    routes = {("POST", f"{B}/sync/lists"): created, ("POST", f"{B}/sync/lists/900/items"): {"added": 1, "existing": 0, "not_found": []}}
    adapter, calls = _adapter(monkeypatch, routes)
    remembered = []

    def remember(reader, ad, resource):
        remembered.append(resource.id)
        table_rows = [OWN, SMART, created]
        monkeypatch.setattr(pl, "list_resources", lambda a: [r for r in (pl._resource_from_row(a, row) for row in table_rows) if r])

    monkeypatch.setattr(pl, "remember_resource", remember)
    res = pl.create(adapter, "New", items=[{"type": "movie", "ids": {"tmdb": 949}}])
    assert res.id == "900" and remembered == ["900"]
    assert ("POST", f"{B}/sync/lists/900/items") in [(c[0], c[1]) for c in calls]
