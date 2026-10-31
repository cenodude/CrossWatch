# /tests/test_trakt_playlists_public_lists.py
# Trakt playlist snapshots for own, liked, shared, smart and public lists
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from types import SimpleNamespace

import pytest

from cw_platform.playlists import PlaylistUnavailableError
from providers.sync.trakt import _playlists as pl


class _Resp:
    def __init__(self, body, status=200, headers=None):
        self._body, self.status_code, self.text, self.headers = body, status, "x", dict(headers or {})

    def json(self):
        return self._body


OWN = [{"name": "Heist", "ids": {"trakt": 101, "slug": "heist"}, "updated_at": "2026-10-01T00:00:00Z", "item_count": 1}]
LIKED = [
    {"type": "list", "list": {"name": "Top 250", "ids": {"trakt": 777, "slug": "top-250"}, "user": {"username": "snoak"}}},
    {"type": "list", "list": {"name": "Heist", "ids": {"trakt": 101, "slug": "heist"}, "user": {"username": "me"}}},
]
COLLAB = [{"name": "Family", "ids": {"trakt": 303, "slug": "family"}, "user": {"username": "Mum", "ids": {"slug": "mum"}}}]
SMART = [{"name": "Thrillers", "ids": {"trakt": 9, "slug": "thrillers"}, "media_type": "movies"}]
ROW = {"id": 9, "rank": 1, "type": "movie", "movie": {"title": "Heat", "year": 1995, "ids": {"trakt": 1, "tmdb": 949}}}
SMART_ROW = {"rank": 1, "type": "movie", "movie": {"title": "Se7en", "year": 1995, "ids": {"trakt": 2, "tmdb": 807}}}


class _Session:
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url))
        body = self.routes.get((method, url), self.routes.get(url) if method == "GET" else None)
        if isinstance(body, _Resp):
            return body
        if body is None:
            if method != "GET":
                raise AssertionError(f"unexpected write {method} {url}")
            return _Resp({}, 404)
        page = int((kwargs.get("params") or {}).get("page") or 1)
        return _Resp(body if page == 1 or method != "GET" or url.endswith("/users/me/lists") else [])


@pytest.fixture(autouse=True)
def _clear_memo():
    pl._SNAPSHOT_MEMO.clear()
    yield
    pl._SNAPSHOT_MEMO.clear()


def _adapter(monkeypatch, routes):
    session = _Session({f"{pl.BASE}/users/me/lists": OWN, **routes})
    monkeypatch.setattr(pl, "headers_for_adapter", lambda adapter: {})
    monkeypatch.setattr(pl, "_list_item_limit", lambda adapter: None)
    cfg = SimpleNamespace(timeout=5.0, max_retries=1)
    return SimpleNamespace(client=SimpleNamespace(session=session), cfg=cfg, instance_id="default"), session


def _gets(session, url):
    return [c for c in session.calls if c == ("GET", url)]


def test_own_list_keeps_personal_route_and_stays_writable(monkeypatch):
    adapter, session = _adapter(monkeypatch, {f"{pl.BASE}/users/me/lists/101/items": [ROW]})
    snap = pl.get_snapshot(adapter, "Heist")
    assert [it.item.get("title") for it in snap.items] == ["Heat"]
    assert snap.resource.can_add and snap.resource.can_remove and snap.resource.can_reorder
    assert _gets(session, f"{pl.BASE}/users/me/lists/101/items")
    assert not any(url.startswith(f"{pl.BASE}/lists/") for _, url in session.calls)


def test_unknown_list_id_reads_public_route_read_only(monkeypatch):
    adapter, session = _adapter(monkeypatch, {
        f"{pl.BASE}/lists/555": {"name": "Noir", "ids": {"trakt": 555}},
        f"{pl.BASE}/lists/555/items": [ROW],
    })
    snap = pl.get_snapshot(adapter, "555")
    assert [it.item.get("title") for it in snap.items] == ["Heat"]
    assert snap.resource.id == "555" and snap.resource.can_read
    assert not (snap.resource.can_add or snap.resource.can_remove or snap.resource.can_reorder)
    assert snap.resource.extra.get("public_list") is True
    assert not any("/users/me/lists/555" in url for _, url in session.calls)


def test_unconfirmed_list_id_is_never_treated_as_public(monkeypatch):
    adapter, session = _adapter(monkeypatch, {})
    with pytest.raises(PlaylistUnavailableError, match="could not be read"):
        pl.get_snapshot(adapter, "555")
    assert _gets(session, f"{pl.BASE}/users/me/lists/555/items")
    assert not _gets(session, f"{pl.BASE}/lists/555/items")


def test_failed_own_list_discovery_raises_instead_of_rerouting(monkeypatch):
    adapter, session = _adapter(monkeypatch, {
        f"{pl.BASE}/users/me/lists": _Resp({}, 500),
        f"{pl.BASE}/users/me/lists/101/items": [ROW],
    })
    with pytest.raises(RuntimeError, match="discovery failed"):
        pl.get_snapshot(adapter, "101")
    assert not any("/lists/101/items" in url for _, url in session.calls)


def test_exact_id_wins_over_a_list_named_like_that_id(monkeypatch):
    adapter, session = _adapter(monkeypatch, {
        f"{pl.BASE}/users/me/lists": [{"name": "303", "ids": {"trakt": 101}}],
        f"{pl.BASE}/users/me/lists/collaborations": COLLAB,
        ("POST", f"{pl.BASE}/users/mum/lists/303/items"): {"added": {"movies": 1}},
        f"{pl.BASE}/users/mum/lists/303/items": [ROW],
    })
    assert pl.resolve(adapter, "303").extra["endpoint_type"] == "collaboration"
    pl.add(adapter, "303", [{"type": "movie", "ids": {"tmdb": 949}}])
    posts = [url for m, url in session.calls if m == "POST"]
    assert posts == [f"{pl.BASE}/users/mum/lists/303/items"]


def test_writes_to_unknown_lists_are_refused(monkeypatch):
    adapter, session = _adapter(monkeypatch, {})
    with pytest.raises(RuntimeError, match="not found"):
        pl.add(adapter, "555", [{"type": "movie", "ids": {"tmdb": 949}}])
    assert not any(m == "POST" for m, _ in session.calls)


def test_public_list_id_gets_real_name_and_owner(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {
        f"{pl.BASE}/lists/555": {"name": "Noir", "ids": {"trakt": 555}, "user": {"username": "snoak"}},
        f"{pl.BASE}/lists/555/items": [ROW],
    })
    snap = pl.get_snapshot(adapter, "555")
    assert snap.resource.name == "Noir by snoak"
    assert snap.resource.extra["endpoint_type"] == "public_list"


def test_liked_lists_are_listed_read_only_without_own_duplicates(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {f"{pl.BASE}/users/me/likes/lists": LIKED})
    resources = {r.id: r for r in pl.list_resources(adapter)}
    assert resources["101"].can_add and resources["101"].name == "Heist"
    liked = resources["777"]
    assert liked.name == "Top 250 by snoak" and liked.extra["endpoint_type"] == "liked_list"
    assert not (liked.can_add or liked.can_remove or liked.can_reorder)
    assert liked.extra["can_rename"] is False and liked.extra["can_delete"] is False


def test_liked_list_snapshot_uses_public_route(monkeypatch):
    adapter, session = _adapter(monkeypatch, {
        f"{pl.BASE}/users/me/likes/lists": LIKED,
        f"{pl.BASE}/lists/777/items": [ROW],
    })
    snap = pl.get_snapshot(adapter, "777")
    assert [it.item.get("title") for it in snap.items] == ["Heat"]
    assert snap.resource.name == "Top 250 by snoak"
    assert not any(url.endswith("/lists/777") for _, url in session.calls)


def test_optional_list_failures_keep_own_lists(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {
        f"{pl.BASE}/users/me/likes/lists": _Resp({}, 500),
        f"{pl.BASE}/users/me/lists/collaborations": _Resp({}, 500),
        f"{pl.BASE}/users/me/smart-lists": _Resp({}, 500),
    })
    assert "101" in {r.id for r in pl.list_resources(adapter)}


def test_collaboration_lists_are_writable_through_owner_route(monkeypatch):
    added = {"added": {"movies": 1}, "not_found": {}}
    adapter, session = _adapter(monkeypatch, {
        f"{pl.BASE}/users/me/lists/collaborations": COLLAB,
        f"{pl.BASE}/users/mum/lists/303/items": [ROW],
        ("POST", f"{pl.BASE}/users/mum/lists/303/items"): added,
    })
    shared = {r.id: r for r in pl.list_resources(adapter)}["303"]
    assert shared.name == "Family by Mum" and shared.can_add and shared.extra["endpoint_type"] == "collaboration"
    assert shared.extra["can_rename"] is False and shared.extra["can_delete"] is False
    assert [it.item.get("title") for it in pl.get_snapshot(adapter, "303").items] == ["Heat"]
    res = pl.add(adapter, "303", [{"type": "movie", "title": "Heat", "year": 1995, "ids": {"tmdb": 949}}])
    assert res["count"] == 1
    assert ("POST", f"{pl.BASE}/users/mum/lists/303/items") in session.calls
    assert not any(m == "POST" and "/users/me/" in url for m, url in session.calls)


def test_smart_lists_are_read_only_sources(monkeypatch):
    adapter, session = _adapter(monkeypatch, {
        f"{pl.BASE}/users/me/smart-lists": SMART,
        f"{pl.BASE}/smart-lists/thrillers/items": [SMART_ROW],
    })
    smart = {r.id: r for r in pl.list_resources(adapter)}["smart:trakt:thrillers"]
    assert smart.is_smart and smart.name == "Thrillers" and smart.media_types == ("movies",)
    assert not (smart.can_add or smart.can_remove)
    snap = pl.get_snapshot(adapter, "smart:trakt:thrillers")
    assert [it.item.get("title") for it in snap.items] == ["Se7en"]
    with pytest.raises(PlaylistUnavailableError):
        pl.add(adapter, "smart:trakt:thrillers", [{"type": "movie", "ids": {"tmdb": 807}}])
    assert not any(m == "POST" for m, _ in session.calls)


def test_writes_to_public_lists_are_refused(monkeypatch):
    adapter, session = _adapter(monkeypatch, {f"{pl.BASE}/users/me/likes/lists": LIKED})
    with pytest.raises(PlaylistUnavailableError, match="read only"):
        pl.add(adapter, "777", [{"type": "movie", "ids": {"tmdb": 949}}])
    assert not any(m == "POST" for m, _ in session.calls)


def test_missing_public_list_raises_clear_error(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {f"{pl.BASE}/users/me/likes/lists": LIKED})
    with pytest.raises(PlaylistUnavailableError, match="Top 250 by snoak is private or no longer exists"):
        pl.get_snapshot(adapter, "777")


def test_lost_collaboration_access_raises_clear_error(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {
        f"{pl.BASE}/users/me/lists/collaborations": COLLAB,
        f"{pl.BASE}/users/mum/lists/303/items": _Resp({}, 403),
    })
    with pytest.raises(PlaylistUnavailableError, match="no longer have access"):
        pl.get_snapshot(adapter, "303")


def test_unchanged_list_is_served_from_memo(monkeypatch):
    adapter, session = _adapter(monkeypatch, {f"{pl.BASE}/users/me/lists/101/items": [ROW]})
    items_url = f"{pl.BASE}/users/me/lists/101/items"
    pl.get_snapshot(adapter, "101")
    first = len(_gets(session, items_url))
    again = pl.get_snapshot(adapter, "101")
    assert [it.item.get("title") for it in again.items] == ["Heat"]
    assert len(_gets(session, items_url)) == first
    session.routes[f"{pl.BASE}/users/me/lists"] = [{**OWN[0], "item_count": 2}]
    pl.get_snapshot(adapter, "101")
    assert len(_gets(session, items_url)) > first


def test_writes_drop_the_memo(monkeypatch):
    adapter, session = _adapter(monkeypatch, {
        f"{pl.BASE}/users/me/lists/101/items": [ROW],
        ("POST", f"{pl.BASE}/users/me/lists/101/items"): {"added": {"movies": 1}},
    })
    pl.get_snapshot(adapter, "101")
    assert pl._SNAPSHOT_MEMO
    pl.add(adapter, "101", [{"type": "movie", "ids": {"tmdb": 949}}])
    assert not pl._SNAPSHOT_MEMO


def test_page_count_header_stops_paging(monkeypatch):
    adapter, session = _adapter(monkeypatch, {
        f"{pl.BASE}/users/me/lists/101/items": _Resp([ROW], headers={"X-Pagination-Page-Count": "1"}),
    })
    pl.get_snapshot(adapter, "101")
    assert len(_gets(session, f"{pl.BASE}/users/me/lists/101/items")) == 1


@pytest.mark.parametrize("ref", [
    "https://trakt.tv/users/snoak/lists/noir?sort=rank,asc",
    "trakt.tv/users/snoak/lists/noir",
    "https://app.trakt.tv/users/snoak/lists/noir",
])
def test_resolve_user_list_links(monkeypatch, ref):
    adapter, _ = _adapter(monkeypatch, {
        f"{pl.BASE}/users/snoak/lists/noir": {"name": "Noir", "privacy": "public", "ids": {"trakt": 555}, "user": {"username": "snoak"}},
    })
    res = pl.resolve(adapter, ref)
    assert (res.id, res.name, res.extra["endpoint_type"]) == ("555", "Noir by snoak", "public_list")


def test_resolve_prefers_own_list_and_rejects_private(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {
        f"{pl.BASE}/users/me2/lists/heist": {"name": "Heist", "privacy": "private", "ids": {"trakt": 101}},
        f"{pl.BASE}/users/other/lists/secret": {"name": "Secret", "privacy": "private", "ids": {"trakt": 404}},
    })
    assert pl.resolve(adapter, "https://trakt.tv/users/me2/lists/heist").can_add
    with pytest.raises(PlaylistUnavailableError, match="not public"):
        pl.resolve(adapter, "https://trakt.tv/users/other/lists/secret")


def test_resolve_numeric_and_smart_links(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {
        f"{pl.BASE}/lists/555": {"name": "Noir", "ids": {"trakt": 555}, "user": {"username": "snoak"}},
        f"{pl.BASE}/smart-lists/new-noir": {"name": "New noir", "ids": {"slug": "new-noir"}, "media_type": "media", "user": {"username": "snoak"}},
    })
    assert pl.resolve(adapter, "https://trakt.tv/lists/555").id == "555"
    assert pl.resolve(adapter, "555").name == "Noir by snoak"
    smart = pl.resolve(adapter, "https://trakt.tv/users/snoak/smart-lists/new-noir")
    assert (smart.id, smart.name, smart.is_smart) == ("smart:trakt:new-noir", "New noir by snoak", True)
    assert pl.resolve(adapter, "smart:trakt:new-noir").id == "smart:trakt:new-noir"


@pytest.mark.parametrize("ref", ["https://example.com/users/x/lists/y", "https://trakt.tv/movies/heat-1995", "999"])
def test_resolve_rejects_bad_or_missing_links(monkeypatch, ref):
    adapter, _ = _adapter(monkeypatch, {})
    with pytest.raises(PlaylistUnavailableError):
        pl.resolve(adapter, ref)


def test_numeric_id_never_falls_back_to_a_list_name(monkeypatch):
    adapter, session = _adapter(monkeypatch, {
        f"{pl.BASE}/users/me/lists": [{"name": "303", "ids": {"trakt": 101}}],
        f"{pl.BASE}/users/me/lists/collaborations": _Resp({}, 500),
    })
    with pytest.raises(RuntimeError, match="not found"):
        pl.add(adapter, "303", [{"type": "movie", "ids": {"tmdb": 949}}])
    with pytest.raises(PlaylistUnavailableError):
        pl.resolve(adapter, "https://trakt.tv/lists/303")
    assert not any(m == "POST" for m, _ in session.calls)
    assert not any("/users/me/lists/101" in url for _, url in session.calls)


def test_list_names_still_resolve_for_non_numeric_refs(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {f"{pl.BASE}/users/me/lists/101/items": [ROW]})
    assert pl.get_snapshot(adapter, "Heist").resource.id == "101"


def test_create_capacity_counts_only_personal_lists(monkeypatch):
    adapter, session = _adapter(monkeypatch, {
        f"{pl.BASE}/users/me/likes/lists": LIKED,
        f"{pl.BASE}/users/me/lists/collaborations": COLLAB,
        f"{pl.BASE}/users/me/smart-lists": SMART,
        ("POST", f"{pl.BASE}/users/me/lists"): {"name": "New", "ids": {"trakt": 202}},
    })
    monkeypatch.setattr(pl, "_list_count_limit", lambda adapter: 2)
    assert pl.create(adapter, "New").id == "202"
    assert ("POST", f"{pl.BASE}/users/me/lists") in session.calls
