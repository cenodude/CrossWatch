# /tests/test_wetrakr_playlists.py
# WeTrakr lists as playlists: own, followed, liked, public and shared lists
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from types import SimpleNamespace

import pytest

from cw_platform.playlists import PlaylistLinkError, PlaylistReadOnlyError, PlaylistUnavailableError, playlist_capabilities, supports_playlists
from providers.sync import _mod_WETRAKR as mod
from providers.sync.wetrakr import _playlists as pl
from providers.sync.wetrakr._common import WeTrakrSyncError

OWNER = {"id": 44, "info": {"username": "walter", "display_name": "Walter"}}
OWN = {"id": 101, "name": "Heist", "privacy": "private", "items_count": 2, "list_updated_at": "2026-10-01T00:00:00Z", "permissions": {"is_owner": True, "is_collaborator": False}}
FOLLOWED = [{"id": 202, "name": "Top 250", "items_count": 250, "owner": OWNER}, {"id": 203, "name": "Locked", "owner": OWNER, "locked": True, "locked_reason": "followed"}]
LIKED = [{"list": {"id": 303, "name": "Noir"}, "like_added_at": "2026-09-01T10:00:00Z"}]
PUBLIC = {"id": 555, "name": "Heists", "privacy": "public", "items_count": 1, "owner": OWNER, "permissions": {"is_owner": False, "is_collaborator": False}}
SHARED = {"id": 777, "name": "Family", "privacy": "private", "items_count": 0, "owner": OWNER, "permissions": {"is_owner": False, "is_collaborator": True}}
SHOW = {"id": 1391953, "type": "show", "title": "Breaking Bad", "ids": {"tmdb": 1396, "tvdb": 81189}}
ROWS = [
    {"id": 790, "type": "movie", "title": "Heat", "release_date": "1995-12-15", "ids": {"tmdb": 949, "imdb": "tt0113277"}, "list_item_id": "a1", "list_item_rank": 2},
    {"id": 653, "type": "movie", "title": "Se7en", "release_date": "1995-09-22", "ids": {"tmdb": 807}, "list_item_id": "a2", "list_item_rank": 1},
    {"id": 174658, "type": "episode", "title": "Pilot", "season_number": 1, "number": 1, "ids": {"tmdb": 62085}, "show": SHOW, "list_item_id": "a3", "list_item_rank": 3},
    {"id": 9, "type": "person", "name": "Al Pacino", "list_item_id": "a4", "list_item_rank": 4},
]


class _Resp:
    def __init__(self, body, headers=None):
        self._body, self.headers = body, dict(headers or {})

    def json(self):
        return self._body


def _adapter(monkeypatch, routes):
    table = {
        ("GET", "/sync/lists"): [OWN],
        ("GET", "/sync/lists/followed"): FOLLOWED,
        ("GET", "/sync/likes/lists"): LIKED,
        ("GET", "/lists/101/items/all"): ROWS,
        **routes,
    }
    calls = []

    def fake(adapter, method, path, **kwargs):
        calls.append((method, path, kwargs.get("json"), kwargs.get("params")))
        body = table.get((method, path))
        if callable(body):
            body = body(kwargs)
        if isinstance(body, WeTrakrSyncError):
            raise body
        if body is None:
            raise WeTrakrSyncError("request_rejected", status=404)
        return body if isinstance(body, _Resp) else _Resp(body)

    monkeypatch.setattr(pl, "request", fake)
    return SimpleNamespace(config={}, instance_id="default"), calls


def _err(status):
    return WeTrakrSyncError("request_rejected", status=status)


def test_manifest_declares_playlists():
    assert supports_playlists(mod.OPS)
    caps = playlist_capabilities(mod.OPS)
    assert caps["resolve"] and caps["reorder"] and caps["link_example"].startswith("https://wetrakr.com/lists/")
    assert mod.get_manifest()["features"]["playlists"] is True


def test_resources_own_followed_and_liked(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {})
    res = {r.id: r for r in pl.list_resources(adapter)}
    assert list(res) == ["101", "202", "303"]
    own = res["101"]
    assert own.name == "Heist" and own.can_add and own.can_remove and own.can_reorder and own.extra["updated_at"]
    followed = res["202"]
    assert followed.name == "Top 250 by Walter" and not followed.can_add and followed.extra["endpoint_type"] == "followed_list"
    liked = res["303"]
    assert liked.name == "Noir" and liked.extra["endpoint_type"] == "liked_list" and liked.extra["public_list"]


@pytest.mark.parametrize("reason", ["lists", "items"])
def test_followed_list_locked_by_owner_plan_stays_listed_as_locked(monkeypatch, reason):
    followed = [{"id": 204, "name": "Owner over limit", "owner": OWNER, "locked": True, "locked_reason": reason}]
    adapter, calls = _adapter(monkeypatch, {("GET", "/sync/lists/followed"): followed})
    res = {r.id: r for r in pl.list_resources(adapter)}["204"]
    assert res.extra["locked"] and res.extra["locked_reason"] == reason and not res.can_read
    with pytest.raises(PlaylistUnavailableError, match="locked"):
        pl.get_snapshot(adapter, "204")
    assert not [c for c in calls if "/lists/204" in c[1]]


def test_own_list_failure_raises_and_extras_are_optional(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("GET", "/sync/lists"): _err(500)})
    with pytest.raises(WeTrakrSyncError):
        pl.list_resources(adapter)
    adapter, _ = _adapter(monkeypatch, {("GET", "/sync/lists/followed"): _err(500), ("GET", "/sync/likes/lists"): _err(403)})
    assert [r.id for r in pl.list_resources(adapter)] == ["101"]


def test_daily_quota_is_not_swallowed(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("GET", "/sync/lists/followed"): WeTrakrSyncError("daily_quota_exceeded", status=429)})
    with pytest.raises(WeTrakrSyncError):
        pl.list_resources(adapter)


def test_snapshot_reads_pages_in_list_order(monkeypatch):
    def items(kwargs):
        page = kwargs["params"]["page"]
        rows = ROWS[:2] if page == 1 else ROWS[2:]
        return _Resp(rows, {"X-Pagination-Page-Count": "2"})

    adapter, calls = _adapter(monkeypatch, {("GET", "/lists/101/items/all"): items})
    snap = pl.get_snapshot(adapter, "101")
    assert [(it.position, it.item.get("title")) for it in snap.items] == [(1, "Se7en"), (2, "Heat"), (3, "Pilot")]
    assert snap.items[2].key == "tmdb:1396#s01e01" and snap.items[1].playlist_item_id == "a1"
    assert [c[3]["page"] for c in calls if c[1] == "/lists/101/items/all"] == [1, 2]


@pytest.mark.parametrize("status, text", [(403, "private"), (404, "no longer exists"), (420, "plan limit")])
def test_snapshot_unavailable(monkeypatch, status, text):
    adapter, _ = _adapter(monkeypatch, {("GET", "/lists/101/items/all"): _err(status)})
    with pytest.raises(PlaylistUnavailableError, match=text if status != 403 else "no longer exists"):
        pl.get_snapshot(adapter, "101")


def test_snapshot_of_unknown_public_list(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("GET", "/lists/555"): PUBLIC, ("GET", "/lists/555/items/all"): ROWS[:1]})
    snap = pl.get_snapshot(adapter, "555")
    assert snap.resource.name == "Heists by Walter" and not snap.resource.can_add and len(snap.items) == 1


@pytest.mark.parametrize("ref", ["https://wetrakr.com/lists/555", "wetrakr.com/lists/555?tab=items", "555", "lists/555"])
def test_resolve_public_link(monkeypatch, ref):
    adapter, _ = _adapter(monkeypatch, {("GET", "/lists/555"): PUBLIC})
    res = pl.resolve(adapter, ref)
    assert res.id == "555" and res.extra["endpoint_type"] == "public_list" and not res.can_add and not res.extra["can_delete"]


def test_resolve_shared_and_own(monkeypatch):
    adapter, calls = _adapter(monkeypatch, {("GET", "/lists/777"): SHARED})
    shared = pl.resolve(adapter, "https://wetrakr.com/lists/777")
    assert shared.extra["endpoint_type"] == "collaboration" and shared.can_add and not shared.can_remove and not shared.can_reorder
    assert pl.resolve(adapter, "https://wetrakr.com/lists/101").can_reorder
    assert ("GET", "/lists/101", None, None) not in calls


@pytest.mark.parametrize("ref", ["https://trakt.tv/lists/555", "wetrakr.com/lists/popular", "https://wetrakr.com/movies/126", ""])
def test_resolve_rejects_other_links(monkeypatch, ref):
    adapter, _ = _adapter(monkeypatch, {})
    with pytest.raises(PlaylistLinkError):
        pl.resolve(adapter, ref)


def test_resolve_missing_or_private(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("GET", "/lists/9"): _err(403)})
    with pytest.raises(PlaylistUnavailableError):
        pl.resolve(adapter, "wetrakr.com/lists/9")
    with pytest.raises(PlaylistUnavailableError):
        pl.resolve(adapter, "wetrakr.com/lists/10")


def test_add_dedupes_shared_ids_and_reads_results(monkeypatch):
    result = {"added": {"total": 2}, "notFound": {"movies": [{"ids": {"tmdb": 999}}]}, "errored": {"movies": [{"ids": {"imdb": "tt0114369"}, "error": "Already added!"}]}}
    adapter, calls = _adapter(monkeypatch, {("POST", "/sync/lists/101/items"): result})
    monkeypatch.setattr(pl, "resolve_child", lambda ad, item: {**item, "ids": {"wetrakr": "174658"}})
    items = [
        {"type": "movie", "ids": {"tmdb": 949, "imdb": "tt0113277"}},
        {"type": "movie", "ids": {"imdb": "tt0113277"}},
        {"type": "movie", "ids": {"imdb": "tt0114369"}},
        {"type": "movie", "ids": {"tmdb": 999}},
        {"type": "episode", "show_ids": {"tmdb": 1396}, "season": 1, "episode": 1, "ids": {"tvdb": 349232}},
    ]
    out = pl.add(adapter, "101", items)
    body = [c[2] for c in calls if c[0] == "POST"][0]
    assert body == {"movies": [{"ids": {"tmdb": 949}}, {"ids": {"imdb": "tt0114369"}}, {"ids": {"tmdb": 999}}], "episodes": [{"id": 174658}]}
    assert out["confirmed_keys"] == ["tmdb:949", "imdb:tt0113277", "imdb:tt0114369", "tmdb:1396#s01e01"]
    assert out["unresolved_keys"] == ["tmdb:999"] and out["unresolved"][0]["hint"] == "not_found"


def test_full_list_reports_capacity(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("POST", "/sync/lists/101/items"): _err(420)})
    out = pl.add(adapter, "101", [{"type": "movie", "ids": {"tmdb": 949}}])
    assert out["ok"] is False and out["capacity"] == "list_full" and out["unresolved_keys"] == ["tmdb:949"]


def test_shared_list_can_add_but_not_remove(monkeypatch):
    routes = {("GET", "/sync/lists/777"): SHARED, ("POST", "/sync/lists/777/items"): {"added": {"total": 1}}}
    adapter, _ = _adapter(monkeypatch, routes)
    assert pl.add(adapter, "777", [{"type": "movie", "ids": {"tmdb": 949}}])["confirmed_keys"] == ["tmdb:949"]
    with pytest.raises(PlaylistReadOnlyError):
        pl.remove(adapter, "777", [{"type": "movie", "ids": {"tmdb": 949}}])
    with pytest.raises(PlaylistReadOnlyError):
        pl.reorder(adapter, "777", ["tmdb:949"])


@pytest.mark.parametrize("lid", ["202", "303", "555"])
def test_writes_to_other_lists_are_refused(monkeypatch, lid):
    adapter, _ = _adapter(monkeypatch, {})
    with pytest.raises(PlaylistReadOnlyError):
        pl.add(adapter, lid, [{"type": "movie", "ids": {"tmdb": 949}}])
    with pytest.raises(PlaylistReadOnlyError):
        pl.rename(adapter, lid, "Mine")
    with pytest.raises(PlaylistReadOnlyError):
        pl.delete(adapter, lid)


def test_reorder_sends_list_item_ids(monkeypatch):
    adapter, calls = _adapter(monkeypatch, {("POST", "/sync/lists/101/items/reorder"): {"ok": True, "moved": 3, "total": 3}})
    out = pl.reorder(adapter, "101", ["tmdb:1396#s01e01", "tmdb:949"])
    assert out == {"ok": True, "count": 3, "reordered": 3}
    assert [c[2] for c in calls if c[0] == "POST"] == [{"items": ["a3", "a1", "a2"]}]
    calls.clear()
    assert pl.reorder(adapter, "101", ["tmdb:807", "tmdb:949", "tmdb:1396#s01e01"])["count"] == 0
    assert not [c for c in calls if c[0] == "POST"]


def test_create_is_private_and_rename_delete_own(monkeypatch):
    created = {"id": 900, "name": "New", "privacy": "private", "items_count": 0}
    routes = {("POST", "/sync/lists"): created, ("PUT", "/sync/lists/101"): {**OWN, "name": "Heist 2"}, ("DELETE", "/sync/lists/101"): {"message": "List removed successfully"}}
    adapter, calls = _adapter(monkeypatch, routes)
    res = pl.create(adapter, "New")
    assert res.id == "900" and res.can_reorder
    assert ("POST", "/sync/lists", {"name": "New", "privacy": "private"}, None) in calls
    assert pl.rename(adapter, "101", "Heist 2").name == "Heist 2"
    assert pl.delete(adapter, "101") == {"ok": True, "count": 1}


def test_create_over_quota(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {("POST", "/sync/lists"): _err(420)})
    with pytest.raises(RuntimeError, match="quota"):
        pl.create(adapter, "New")


def test_malformed_list_item_fails_the_snapshot(monkeypatch):
    broken = {**ROWS[2], "show": None}
    adapter, _ = _adapter(monkeypatch, {("GET", "/lists/101/items/all"): [ROWS[0], broken]})
    with pytest.raises(WeTrakrSyncError, match="invalid_list_item"):
        pl.get_snapshot(adapter, "101")


@pytest.mark.parametrize("response", [{}, {"added": {"total": 1}}, {"added": "2"}, {"removed": {"movies": 2}}])
def test_unverified_add_confirms_nothing(monkeypatch, response):
    adapter, _ = _adapter(monkeypatch, {("POST", "/sync/lists/101/items"): response})
    out = pl.add(adapter, "101", [{"type": "movie", "ids": {"tmdb": 949}}, {"type": "movie", "ids": {"tmdb": 807}}])
    assert out["ok"] is False and out["confirmed_keys"] == []
    assert {u["hint"] for u in out["unresolved"]} == {"write_not_verified"}


def test_remove_counts_per_type(monkeypatch):
    response = {"added": {"total": 0}, "removed": {"movies": 1, "episodes": 0, "lists": 0}, "notFound": {"movies": [{"ids": {"tmdb": 155}}]}}
    adapter, _ = _adapter(monkeypatch, {("POST", "/sync/lists/101/items/remove"): response})
    out = pl.remove(adapter, "101", [{"type": "movie", "ids": {"tmdb": 949}}, {"type": "movie", "ids": {"tmdb": 155}}])
    assert out["ok"] is True and out["confirmed_keys"] == ["tmdb:949"] and out["unresolved_keys"] == ["tmdb:155"]


def test_already_added_only_response_is_confirmed(monkeypatch):
    response = {"errored": {"movies": [{"ids": {"tmdb": 949}, "error": "Already added!"}]}}
    adapter, _ = _adapter(monkeypatch, {("POST", "/sync/lists/101/items"): response})
    out = pl.add(adapter, "101", [{"type": "movie", "ids": {"tmdb": 949}}])
    assert out["ok"] is True and out["confirmed_keys"] == ["tmdb:949"]


def test_followed_shared_list_keeps_collaborator_access(monkeypatch):
    followed = [{**SHARED, "permissions": {"is_owner": False, "is_collaborator": True}}]
    routes = {("GET", "/sync/lists/followed"): followed, ("POST", "/sync/lists/777/items"): {"added": {"total": 1}}}
    adapter, _ = _adapter(monkeypatch, routes)
    res = {r.id: r for r in pl.list_resources(adapter)}["777"]
    assert res.extra["endpoint_type"] == "collaboration" and res.can_add and not res.can_remove
    assert pl.add(adapter, "777", [{"type": "movie", "ids": {"tmdb": 949}}])["confirmed_keys"] == ["tmdb:949"]


def test_liked_shared_list_is_checked_before_refusing(monkeypatch):
    liked = [{"list": {"id": 777, "name": "Family"}}]
    routes = {
        ("GET", "/sync/likes/lists"): liked,
        ("GET", "/sync/lists/777"): SHARED,
        ("GET", "/lists/777"): SHARED,
        ("POST", "/sync/lists/777/items"): {"added": {"total": 1}},
    }
    adapter, _ = _adapter(monkeypatch, routes)
    assert pl.resolve(adapter, "wetrakr.com/lists/777").extra["endpoint_type"] == "collaboration"
    assert pl.add(adapter, "777", [{"type": "movie", "ids": {"tmdb": 949}}])["confirmed_keys"] == ["tmdb:949"]
    with pytest.raises(PlaylistReadOnlyError):
        pl.remove(adapter, "777", [{"type": "movie", "ids": {"tmdb": 949}}])


def test_liked_list_without_access_stays_read_only(monkeypatch):
    adapter, calls = _adapter(monkeypatch, {("GET", "/lists/303"): {"id": 303, "name": "Noir", "owner": OWNER, "permissions": {"is_owner": False, "is_collaborator": False}}})
    assert pl.resolve(adapter, "wetrakr.com/lists/303").extra["endpoint_type"] == "liked_list"
    with pytest.raises(PlaylistReadOnlyError):
        pl.add(adapter, "303", [{"type": "movie", "ids": {"tmdb": 949}}])
    assert ("GET", "/sync/lists/303", None, None) in calls
