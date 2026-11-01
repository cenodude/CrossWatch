# /tests/test_tmdb_public_lists.py
# TMDb v4 lists: public lists by link, read only, duplicates, strict reads and unchanged-list memo
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

import pytest

from cw_platform.playlists import PlaylistLinkError, PlaylistReadOnlyError, PlaylistUnavailableError, playlist_capabilities
from providers.sync import _mod_TMDB as mod
from providers.sync.tmdb import _playlists as pl

OWN = {"page": 1, "total_pages": 1, "total_results": 1, "results": [
    {"id": 11, "name": "Faves", "number_of_items": 2, "public": 0, "updated_at": "2026-11-01 01:22:42 UTC"},
]}
PUBLIC = {"id": 1, "name": "The Marvel Universe", "public": True, "item_count": 2, "total_pages": 1, "total_results": 2,
          "created_by": {"id": "4bc8892a017a3c0f92000002", "username": "travisbell"},
          "results": [{"media_type": "movie", "id": 986056, "title": "Thunderbolts*", "release_date": "2025-04-30"},
                      {"media_type": "tv", "id": 1396, "name": "Breaking Bad", "first_air_date": "2008-01-20"}]}


class _Resp:
    def __init__(self, status: int, data: Any) -> None:
        self.status_code, self._data = status, data
        self.content = b"x" if data is not None else b""
        self.text = "x" if data is not None else ""
        self.headers: dict[str, str] = {}

    def json(self) -> Any:
        return self._data


@pytest.fixture(autouse=True)
def _setup(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(pl, "enrich_ids_dict", lambda *args, **kwargs: None)
    pl._SNAPSHOT_MEMO.clear()
    yield
    pl._SNAPSHOT_MEMO.clear()


def _cfg() -> dict[str, Any]:
    return {"tmdb_sync": {"api_key": "a" * 32, "session_id": "sess", "account_id": "7", "access_token": "user-access",
                          "account_object_id": "obj-1", "v4_session_ref": mod.session_ref("sess")}}


def _adapter(monkeypatch: pytest.MonkeyPatch, routes: dict[tuple[str, str], Any]):
    adapter = mod.TMDBModule(_cfg())
    table = {("GET", "/account/obj-1/lists"): OWN, **routes}
    calls: list[tuple[str, str, Any, Any]] = []

    def v4(method: str, path: str, *, params: Any = None, json_body: Any = None, timeout: Any = None) -> _Resp:
        calls.append((method, path, dict(params or {}), json_body))
        body = table.get((method, path))
        if callable(body):
            body = body(dict(params or {}))
        if body is None:
            return _Resp(404, {"success": False, "status_code": 34, "status_message": "The resource you requested could not be found."})
        return body if isinstance(body, _Resp) else _Resp(200, body)

    monkeypatch.setattr(adapter.client, "v4", v4)
    return adapter, calls


def test_capabilities_offer_link_resolve():
    caps = playlist_capabilities(mod.OPS)
    assert caps["resolve"] and urlparse(caps["link_example"]).hostname == "www.themoviedb.org"
    assert callable(getattr(mod.OPS, "resolve_playlist", None))


@pytest.mark.parametrize("ref", [
    "https://www.themoviedb.org/list/1-the-marvel-universe",
    "https://www.themoviedb.org/list/1",
    "themoviedb.org/list/1?language=en",
    "1",
])
def test_resolve_public_link(monkeypatch, ref):
    adapter, _ = _adapter(monkeypatch, {("GET", "/list/1"): PUBLIC})
    res = pl.resolve(adapter, ref)
    assert res.id == "1" and res.name == "The Marvel Universe by travisbell"
    assert res.extra["endpoint_type"] == "public_list" and not res.can_add and not res.extra["can_delete"]


def test_resolve_own_list_returns_writable_own(monkeypatch):
    adapter, calls = _adapter(monkeypatch, {})
    res = pl.resolve(adapter, "https://www.themoviedb.org/list/11-faves")
    assert res.can_add and res.name == "Faves"
    assert not [c for c in calls if c[1] == "/list/11"]


def test_own_list_found_by_owner_id_is_not_marked_public(monkeypatch):
    mine = {**PUBLIC, "id": 5, "created_by": {"id": "obj-1", "username": "cenodude"}}
    adapter, _ = _adapter(monkeypatch, {("GET", "/list/5"): mine})
    assert "public_list" not in pl.resolve(adapter, "5").extra


@pytest.mark.parametrize("ref", ["https://trakt.tv/lists/1", "https://www.themoviedb.org/u/travisbell/lists", "themoviedb.org/list/abc", "themoviedb.org/movie/550", ""])
def test_resolve_rejects_other_links(monkeypatch, ref):
    adapter, _ = _adapter(monkeypatch, {})
    with pytest.raises(PlaylistLinkError):
        pl.resolve(adapter, ref)


@pytest.mark.parametrize("status", [401, 404])
def test_private_or_missing_lists_are_unavailable(monkeypatch, status):
    adapter, _ = _adapter(monkeypatch, {("GET", "/list/9"): _Resp(status, {"success": False, "status_code": 34})})
    with pytest.raises(PlaylistUnavailableError):
        pl.resolve(adapter, "9")
    with pytest.raises(PlaylistUnavailableError):
        pl.get_snapshot(adapter, "9")


def test_public_snapshot_is_read_only_and_writes_are_refused(monkeypatch):
    adapter, calls = _adapter(monkeypatch, {("GET", "/list/1"): PUBLIC})
    snap = pl.get_snapshot(adapter, "1")
    assert snap.resource.name == "The Marvel Universe by travisbell" and not snap.resource.can_add
    assert [(it.item["type"], it.item["title"]) for it in snap.items] == [("movie", "Thunderbolts*"), ("show", "Breaking Bad")]
    for op in (pl.add, pl.remove):
        with pytest.raises(PlaylistReadOnlyError):
            op(adapter, "1", [{"type": "movie", "ids": {"tmdb": 949}}])
    assert not [c for c in calls if c[0] != "GET"]


def _paged(rows: list[dict[str, Any]], total: int | None = None, broken: dict[int, Any] | None = None):
    def answer(params: dict[str, Any]) -> Any:
        page = int(params.get("page") or 1)
        if broken and page in broken:
            return broken[page]
        chunk = rows[(page - 1) * 1: page * 1]
        return {**PUBLIC, "page": page, "total_pages": len(rows), "total_results": len(rows) if total is None else total, "results": chunk}
    return answer


@pytest.mark.parametrize("bad", [
    {**PUBLIC, "page": 2, "total_pages": 2, "results": [None]},
    {**PUBLIC, "page": 2, "total_pages": 2, "results": None},
    {**PUBLIC, "page": 2, "total_pages": 3, "results": []},
])
def test_incomplete_reads_fail(monkeypatch, bad):
    adapter, _ = _adapter(monkeypatch, {("GET", "/list/1"): _paged(PUBLIC["results"], broken={2: bad})})
    with pytest.raises(pl.TMDBPlaylistError):
        pl.get_snapshot(adapter, "1")


def test_short_read_against_total_results_fails(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("GET", "/list/1"): _paged(PUBLIC["results"], total=3)})
    with pytest.raises(pl.TMDBPlaylistError):
        pl.get_snapshot(adapter, "1")


def test_duplicate_add_is_confirmed_and_unknown_media_is_rejected(monkeypatch):
    monkeypatch.setattr(pl, "resolve_tmdb_id", lambda adapter, item, want: (item.get("ids") or {}).get("tmdb"))
    result = {"success": True, "status_code": 1, "results": [
        {"media_id": 949, "media_type": "movie", "error": ["Media has already been taken"], "success": False},
        {"media_id": 999999999, "media_type": "movie", "error": ["Media is required"], "success": False},
    ]}
    adapter, _ = _adapter(monkeypatch, {("POST", "/list/11/items"): result})
    res = pl.add(adapter, "11", [{"type": "movie", "ids": {"tmdb": 949}}, {"type": "movie", "ids": {"tmdb": 999999999}}])
    assert res["confirmed_keys"] == ["tmdb:949"] and res["unresolved"][0]["hint"] == "rejected"


@pytest.mark.parametrize("response", [{"success": True}, {"success": True, "results": []}, {}])
def test_write_without_per_item_results_confirms_nothing(monkeypatch, response):
    monkeypatch.setattr(pl, "resolve_tmdb_id", lambda adapter, item, want: (item.get("ids") or {}).get("tmdb"))
    adapter, _ = _adapter(monkeypatch, {("POST", "/list/11/items"): response})
    res = pl.add(adapter, "11", [{"type": "movie", "ids": {"tmdb": 949}}])
    assert res["ok"] is False and res["confirmed_keys"] == [] and res["unresolved"][0]["hint"] == "write_not_verified"


def test_unchanged_own_list_is_not_read_again_and_writes_drop_the_memo(monkeypatch):
    monkeypatch.setattr(pl, "resolve_tmdb_id", lambda adapter, item, want: (item.get("ids") or {}).get("tmdb"))
    own_list = {**PUBLIC, "id": 11, "name": "Faves", "created_by": {"id": "obj-1", "username": "cenodude"}}
    routes = {("GET", "/list/11"): own_list, ("POST", "/list/11/items"): {"success": True, "results": [{"media_id": 807, "media_type": "movie", "success": True}]}}
    adapter, calls = _adapter(monkeypatch, routes)
    pl.get_snapshot(adapter, "11")
    pl.get_snapshot(adapter, "11")
    assert len([c for c in calls if c[1] == "/list/11"]) == 1
    pl.add(adapter, "11", [{"type": "movie", "ids": {"tmdb": 807}}])
    pl.get_snapshot(adapter, "11")
    assert len([c for c in calls if c[1] == "/list/11"]) == 2


def test_public_lists_are_always_read(monkeypatch):
    adapter, calls = _adapter(monkeypatch, {("GET", "/list/1"): PUBLIC})
    pl.get_snapshot(adapter, "1")
    pl.get_snapshot(adapter, "1")
    assert len([c for c in calls if c[1] == "/list/1"]) == 2


def test_created_list_is_private_and_remembered(monkeypatch):
    remembered: list[str] = []
    monkeypatch.setattr(pl, "remember_resource", lambda reader, ad, res: remembered.append(res.id))
    adapter, calls = _adapter(monkeypatch, {("POST", "/list"): _Resp(201, {"success": True, "id": 77})})
    res = pl.create(adapter, "Synced")
    assert res.id == "77" and remembered == ["77"]
    body = [c[3] for c in calls if c[1] == "/list"][0]
    assert type(body["public"]) is int and body["public"] == 0


@pytest.mark.parametrize("status", [401, 403])
def test_rejected_token_is_an_auth_error_not_a_missing_list(monkeypatch, status):
    from cw_platform.playlists import PlaylistAuthError

    rejected = _Resp(status, {"success": False, "status_code": 3, "status_message": "Authentication failed"})
    adapter, calls = _adapter(monkeypatch, {("GET", "/account/obj-1/lists"): rejected, ("GET", "/list/1"): PUBLIC})
    for call in (lambda: pl.resolve(adapter, "1"), lambda: pl.get_snapshot(adapter, "1"), lambda: pl.list_resources(adapter)):
        with pytest.raises(PlaylistAuthError):
            call()
    assert not [c for c in calls if c[1] == "/list/1"]


def test_private_list_after_a_good_index_stays_unavailable(monkeypatch):
    from cw_platform.playlists import PlaylistAuthError

    adapter, _ = _adapter(monkeypatch, {("GET", "/list/9"): _Resp(401, {"success": False, "status_code": 3})})
    with pytest.raises(PlaylistUnavailableError) as err:
        pl.resolve(adapter, "9")
    assert not isinstance(err.value, PlaylistAuthError)
