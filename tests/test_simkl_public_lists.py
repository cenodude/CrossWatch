# /tests/test_simkl_public_lists.py
# SIMKL custom lists: followed, shared and public lists by link
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from types import SimpleNamespace

import pytest

from cw_platform.playlists import PlaylistLinkError, PlaylistPlanError, PlaylistUnavailableError, playlist_capabilities
from providers.sync import _mod_SIMKL as mod
from providers.sync.simkl import _playlists as pl

ME = {"id": 77, "name": "Me"}
OTHER = {"id": 5, "name": "SIMKL"}
OWN = {"id": 10, "name": "Heist", "media_type": "movies", "type": "regular", "privacy": "private", "role": "owner", "user": ME, "counts": {"items": 1}, "updated_at": "2026-10-01T00:00:00Z"}
FOLLOWED = {"id": 2733, "name": "Star Wars (TV Shows)", "media_type": "tv", "type": "regular", "privacy": "public", "role": "follower", "user": OTHER, "counts": {"items": 28}, "updated_at": "2026-09-22T11:10:01Z"}
SHARED = {"id": 3000, "name": "Family", "media_type": "anime", "type": "regular", "privacy": "private", "role": "collaborator", "user": OTHER, "counts": {"items": 3}}
DETAIL = {"id": 2731, "name": "Star Wars (Movies)", "media_type": "movies", "type": "regular", "privacy": "public", "user": OTHER, "counts": {"items": 19},
          "updated_at": "2026-09-22T11:10:01Z", "pagination": {"page": 1, "total_pages": 1},
          "items": [{"title": "A New Hope", "year": 1977, "type": "movie", "position": 1, "ids": {"simkl_id": 1, "tmdb": "11"}}]}


class _Resp:
    def __init__(self, body, status=200):
        self._body, self.status_code, self.text = body, status, "x"

    def json(self):
        return self._body


class _Session:
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def get(self, url, **kwargs):
        params = dict(kwargs.get("params") or {})
        self.calls.append((url, params))
        body = self.routes.get(url)
        if body is None:
            return _Resp({"error": "url_failed", "message": "List not found"}, 404)
        body = body(params) if callable(body) else body
        return body if isinstance(body, _Resp) else _Resp(body)

    def post(self, url, **kwargs):
        raise AssertionError("custom lists must never be written")


@pytest.fixture(autouse=True)
def _clear_memo():
    pl._SNAPSHOT_MEMO.clear()
    yield
    pl._SNAPSHOT_MEMO.clear()


def _user_lists(params):
    if params.get("followed") == "true":
        return {"pagination": {"total_pages": 1}, "lists": [OWN, FOLLOWED]}
    if params.get("collaborants") == "true":
        return {"pagination": {"total_pages": 1}, "lists": [SHARED]}
    return {"pagination": {"total_pages": 1}, "lists": [OWN]}


def _adapter(monkeypatch, routes):
    session = _Session({pl.URL_USER_LISTS.format(user_id="77"): _user_lists, **routes})
    monkeypatch.setattr(pl, "adapter_headers", lambda adapter: {"simkl-api-key": "cid"})
    monkeypatch.setattr(pl, "fetch_user_settings", lambda *a, **k: {"account": {"id": 77, "type": "vip"}})
    return SimpleNamespace(client=SimpleNamespace(session=session), cfg=SimpleNamespace(timeout=5.0), instance_id="default"), session


def _custom(resources):
    return {r.id: r for r in resources if r.id.startswith("list:")}


def test_capabilities_offer_link_resolve():
    caps = playlist_capabilities(mod.OPS)
    assert caps["resolve"] and "simkl.com/" in caps["link_example"]
    assert callable(getattr(mod.OPS, "resolve_playlist", None))


def test_followed_and_shared_lists_are_listed_read_only(monkeypatch):
    adapter, session = _adapter(monkeypatch, {})
    res = _custom(pl.list_resources(adapter))
    assert list(res) == ["list:10", "list:2733", "list:3000"]
    assert res["list:10"].name == "Heist" and res["list:10"].extra["endpoint_type"] == "custom_list"
    followed = res["list:2733"]
    assert followed.name == "Star Wars (TV Shows) by SIMKL" and followed.extra["endpoint_type"] == "followed_list" and followed.extra["public_list"]
    shared = res["list:3000"]
    assert shared.extra["endpoint_type"] == "collaboration" and shared.media_types == ("anime",)
    assert not any(r.can_add or r.can_remove or r.can_reorder for r in res.values())
    assert {p.get("followed") or p.get("collaborants") or "own" for _url, p in session.calls} == {"own", "true"}


def test_extra_scopes_failing_keeps_own_lists(monkeypatch):
    def lists(params):
        if params.get("followed") or params.get("collaborants"):
            return _Resp({}, 500)
        return {"pagination": {"total_pages": 1}, "lists": [OWN]}

    adapter, _ = _adapter(monkeypatch, {pl.URL_USER_LISTS.format(user_id="77"): lists})
    assert list(_custom(pl.list_resources(adapter))) == ["list:10"]


@pytest.mark.parametrize("ref", [
    "https://simkl.com/5/list/2731/star-wars-franchise-movies",
    "simkl.com/5/list/2731",
    "https://www.simkl.com/5/list/2731/star-wars-franchise-movies?ref=x",
    "2731",
    "list:2731",
])
def test_resolve_public_list_link(monkeypatch, ref):
    adapter, _ = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="2731"): DETAIL})
    res = pl.resolve(adapter, ref)
    assert res.id == "list:2731" and res.name == "Star Wars (Movies) by SIMKL"
    assert res.extra["endpoint_type"] == "public_list" and not res.can_add


def test_resolve_prefers_listed_lists(monkeypatch):
    adapter, session = _adapter(monkeypatch, {})
    assert pl.resolve(adapter, "https://simkl.com/5/list/2733/star-wars").extra["endpoint_type"] == "followed_list"
    assert pl.resolve(adapter, "simkl.com/77/list/10/heist").name == "Heist"
    assert not [url for url, _p in session.calls if url == pl.URL_LIST.format(list_id="2733")]


@pytest.mark.parametrize("ref", ["https://trakt.tv/lists/2731", "https://simkl.com/lists/official/", "simkl.com/movies/11/star-wars", ""])
def test_resolve_rejects_other_links(monkeypatch, ref):
    adapter, _ = _adapter(monkeypatch, {})
    with pytest.raises(PlaylistLinkError):
        pl.resolve(adapter, ref)


def test_private_and_missing_lists_are_unavailable(monkeypatch):
    routes = {pl.URL_LIST.format(list_id="102500"): _Resp({"error": "private_list"}, 403)}
    adapter, _ = _adapter(monkeypatch, routes)
    with pytest.raises(PlaylistUnavailableError):
        pl.resolve(adapter, "simkl.com/1/list/102500")
    with pytest.raises(PlaylistUnavailableError):
        pl.resolve(adapter, "simkl.com/1/list/999999")
    with pytest.raises(PlaylistUnavailableError):
        pl.get_snapshot(adapter, "list:102500")
    with pytest.raises(pl.SIMKLPlaylistNotFound):
        pl.get_snapshot(adapter, "list:999999")


def test_snapshot_of_another_users_list_is_public_read_only(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="2731"): DETAIL})
    snap = pl.get_snapshot(adapter, "list:2731")
    assert snap.resource.name == "Star Wars (Movies) by SIMKL" and snap.resource.extra["endpoint_type"] == "public_list"
    assert [it.item["title"] for it in snap.items] == ["A New Hope"]


def _paged(kind, updated="2026-09-22T11:10:01Z"):
    def page(params):
        n = int(params.get("page") or 1)
        return {**DETAIL, "type": kind, "updated_at": updated, "counts": {"items": 2}, "pagination": {"page": n, "total_pages": 2},
                "items": [{"title": f"T{n}", "year": 2000 + n, "type": "movie", "position": n, "ids": {"simkl_id": n, "tmdb": str(100 + n)}}]}
    return page


def test_unchanged_regular_list_skips_later_pages(monkeypatch):
    adapter, session = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="2731"): _paged("regular")})
    first = pl.get_snapshot(adapter, "list:2731")
    second = pl.get_snapshot(adapter, "list:2731")
    assert [it.item["title"] for it in second.items] == [it.item["title"] for it in first.items] == ["T1", "T2"]
    pages = [p["page"] for url, p in session.calls if url == pl.URL_LIST.format(list_id="2731")]
    assert pages == [1, 2, 1]


def test_auto_and_recommendation_lists_are_always_read(monkeypatch):
    adapter, session = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="2731"): _paged("recommendations")})
    pl.get_snapshot(adapter, "list:2731")
    pl.get_snapshot(adapter, "list:2731")
    assert [p["page"] for url, p in session.calls if url == pl.URL_LIST.format(list_id="2731")] == [1, 2, 1, 2]


def test_changed_list_is_read_again(monkeypatch):
    stamps = iter(["2026-09-22T11:10:01Z", "2026-09-22T11:10:01Z", "2026-10-01T00:00:00Z", "2026-10-01T00:00:00Z"])

    def page(params):
        return {**_paged("regular", next(stamps))(params)}

    adapter, session = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="2731"): page})
    pl.get_snapshot(adapter, "list:2731")
    pl.get_snapshot(adapter, "list:2731")
    assert [p["page"] for url, p in session.calls if url == pl.URL_LIST.format(list_id="2731")] == [1, 2, 1, 2]


def test_own_list_failure_keeps_status_buckets_but_fails_resolve(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {pl.URL_USER_LISTS.format(user_id="77"): _Resp({}, 500)})
    assert [r.id for r in pl.list_resources(adapter)] == [row[0] for row in pl._STATUS_ROWS]
    with pytest.raises(pl.SIMKLFetchError):
        pl.resolve(adapter, "simkl.com/5/list/2731")


def test_unreadable_account_keeps_status_buckets_but_fails_resolve(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {})
    monkeypatch.setattr(pl, "fetch_user_settings", lambda *a, **k: None)
    assert [r.id for r in pl.list_resources(adapter)] == [row[0] for row in pl._STATUS_ROWS]
    with pytest.raises(pl.SIMKLFetchError):
        pl.resolve(adapter, "simkl.com/5/list/2731")


@pytest.mark.parametrize("page_two", [
    {},
    {"items": None, "pagination": {"total_pages": 2}},
    {"items": [], "pagination": {"page": 2, "total_pages": 3}},
    {"items": [None], "pagination": {"page": 2, "total_pages": 2, "total_items": 2}},
    {"items": ["x"], "pagination": {"page": 2, "total_pages": 2}},
])
def test_incomplete_pages_fail_and_are_not_cached(monkeypatch, page_two):
    state = {"broken": True}

    def page(params):
        n = int(params.get("page") or 1)
        if n == 2 and state["broken"]:
            return page_two
        return _paged("regular")(params)

    adapter, session = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="2731"): page})
    with pytest.raises(pl.SIMKLFetchError):
        pl.get_snapshot(adapter, "list:2731")
    state["broken"] = False
    assert [it.item["title"] for it in pl.get_snapshot(adapter, "list:2731").items] == ["T1", "T2"]
    assert [p["page"] for url, p in session.calls if url == pl.URL_LIST.format(list_id="2731")] == [1, 2, 1, 2]


def test_short_read_against_total_items_fails(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="2731"): {**DETAIL, "pagination": {"page": 1, "total_pages": 1, "total_items": 3}}})
    with pytest.raises(pl.SIMKLFetchError):
        pl.get_snapshot(adapter, "list:2731")


def test_snapshot_without_role_keeps_saved_relationship(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="2731"): DETAIL})
    snap = pl.get_snapshot(adapter, "list:2731")
    assert snap.resource.extra["unresolved"] is True
    own = {**DETAIL, "id": 10, "user": ME, "name": "Heist"}
    adapter, _ = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="10"): own})
    snap = pl.get_snapshot(adapter, "list:10")
    assert "unresolved" not in snap.resource.extra and snap.resource.extra["endpoint_type"] == "custom_list"


@pytest.mark.parametrize("error", ["oauth2_token_required", "user_token_required"])
def test_token_without_list_access_keeps_status_buckets(monkeypatch, error):
    adapter, _ = _adapter(monkeypatch, {pl.URL_USER_LISTS.format(user_id="77"): _Resp({"error": error}, 403)})
    assert [r.id for r in pl.list_resources(adapter)] == [row[0] for row in pl._STATUS_ROWS]


def test_resolve_on_a_free_account_needs_a_paid_plan(monkeypatch):
    adapter, session = _adapter(monkeypatch, {})
    monkeypatch.setattr(pl, "fetch_user_settings", lambda *a, **k: {"account": {"id": 77, "type": "free"}})
    with pytest.raises(PlaylistPlanError):
        pl.resolve(adapter, "https://simkl.com/5/list/2731/star-wars-franchise-movies")
    assert session.calls == []


def test_premium_only_answer_is_a_plan_error(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="2731"): {"error": "premium_only"}})
    with pytest.raises(PlaylistPlanError):
        pl.get_snapshot(adapter, "list:2731")
