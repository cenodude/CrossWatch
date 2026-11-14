from types import SimpleNamespace

import pytest

from providers.sync.simkl import _playlists as pl


class _Resp:
    def __init__(self, body, status=200):
        self._body, self.status_code, self.text = body, status, "x"

    def json(self):
        return self._body


class _Session:
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def get(self, url, **kwargs):
        self.calls.append(("GET", url, dict(kwargs.get("params") or {})))
        body = self.routes[url]
        return body if isinstance(body, _Resp) else _Resp(body(kwargs.get("params") or {}) if callable(body) else body)

    def _write(self, method, url, kwargs):
        self.calls.append((method, url, dict(kwargs.get("params") or {}), kwargs.get("json")))
        body = self.routes.get((method, url))
        if body is None:
            raise AssertionError(f"unexpected {method} {url}")
        if isinstance(body, list):
            body = body.pop(0)
        return body if isinstance(body, _Resp) else _Resp(body(kwargs.get("json")) if callable(body) else body)

    def post(self, url, **kwargs):
        return self._write("POST", url, kwargs)

    def delete(self, url, **kwargs):
        return self._write("DELETE", url, kwargs)


LISTS = {"pagination": {"page": 1, "total_pages": 1}, "lists": [
    {"id": 5, "name": "Heist", "media_type": "movies", "type": "regular", "privacy": "private", "role": "owner",
     "counts": {"items": 2}, "updated_at": "2026-09-16T05:47:25Z"},
    {"id": 7, "name": "Picks", "media_type": "movies", "type": "recommendations", "role": "owner", "counts": {"items": 9}},
    {"id": 8, "name": "Shared", "media_type": "tv", "type": "regular", "role": "collaborator", "user": {"id": 9, "name": "Ann"}, "counts": {"items": 1}},
    {"id": 6, "name": "Odd", "media_type": "games", "counts": {"items": 1}},
]}
HEAD = {"id": 5, "name": "Heist", "media_type": "movies", "type": "regular", "privacy": "private", "user": {"id": 77, "name": "Me"},
        "counts": {"items": 2}, "updated_at": "2026-09-16T05:47:25Z", "pagination": {"page": 1, "total_pages": 1, "total_items": 0}, "items": []}


def _adapter(monkeypatch, routes, tier="vip"):
    session = _Session(routes)
    monkeypatch.setattr(pl, "adapter_headers", lambda adapter: {"simkl-api-key": "cid"})
    monkeypatch.setattr(pl, "fetch_user_settings", lambda *a, **k: {"account": {"id": 77, "type": tier}})
    return SimpleNamespace(client=SimpleNamespace(session=session), cfg=SimpleNamespace(timeout=5.0), instance_id="default"), session


def test_only_own_regular_lists_are_writable(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {pl.URL_USER_LISTS.format(user_id="77"): LISTS})
    custom = {r.id: r for r in pl.list_resources(adapter) if str(r.id).startswith("list:")}
    assert [(r.id, r.name, r.media_types, r.extra["item_count"]) for r in custom.values()] == [
        ("list:5", "Heist", ("movie",), 2), ("list:7", "Picks", ("movie",), 9), ("list:8", "Shared by Ann", ("show",), 1)]
    own = custom["list:5"]
    assert own.can_add and own.can_remove and own.can_reorder
    assert own.extra["can_rename"] is True and own.extra["can_delete"] is True and own.extra["warnings"] == []
    for rid in ("list:7", "list:8"):
        res = custom[rid]
        assert not (res.can_add or res.can_remove or res.can_reorder)
        assert res.extra["can_rename"] is False and res.extra["read_only"] is True


def test_free_accounts_get_no_custom_lists_and_no_request(monkeypatch):
    adapter, session = _adapter(monkeypatch, {}, tier="free")
    assert [r.id for r in pl.list_resources(adapter)] == [row[0] for row in pl._STATUS_ROWS]
    assert session.calls == []


def test_snapshot_pages_keeps_order_and_maps_simkl_ids(monkeypatch):
    def page(params):
        first = int(params.get("page") or 1) == 1
        return {"id": 5, "name": "Heist", "media_type": "movies", "counts": {"items": 2}, "updated_at": "2026-09-16T05:47:25Z",
                "pagination": {"page": 1 if first else 2, "total_pages": 2},
                "items": [{"title": "Heat", "year": 1995, "type": "movie", "position": 1,
                           "ids": {"simkl_id": 11, "imdb": "tt0113277", "tmdb": "949"}}] if first else
                         [{"title": "Ronin", "year": 1998, "type": "movie", "position": 2, "ids": {"simkl_id": 12, "tmdb": "8195"}}]}
    adapter, session = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="5"): page})
    snap = pl.get_snapshot(adapter, "list:5")
    assert [(item.item["title"], item.item["type"], str(item.item["ids"]["simkl"]), item.position) for item in snap.items] == [
        ("Heat", "movie", "11", 0), ("Ronin", "movie", "12", 1)]
    assert snap.checkpoint == "2026-09-16T05:47:25Z"
    assert [call[2]["page"] for call in session.calls] == [1, 2]


def test_premium_only_answer_on_http_200_is_an_error_not_an_empty_list(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="5"): {"error": "premium_only", "message": "upgrade"}})
    with pytest.raises(pl.SIMKLPlaylistError, match="PRO or VIP"):
        pl.get_snapshot(adapter, "list:5")


def test_writes_to_a_read_only_list_never_post(monkeypatch):
    shared = {**HEAD, "id": 8, "user": {"id": 9, "name": "Ann"}, "role": "collaborator"}
    adapter, session = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="8"): shared})
    item = {"type": "movie", "ids": {"imdb": "tt0113277"}}
    for result in (pl.add(adapter, "list:8", [item]), pl.remove(adapter, "list:8", [item])):
        assert result["ok"] is False and result["count"] == 0 and result["confirmed_keys"] == []
        assert result["unresolved"][0]["hint"] == "read_only"
    assert all(call[0] == "GET" for call in session.calls)


HEAT = {"type": "movie", "title": "Heat", "ids": {"imdb": "tt0113277", "tmdb": "949"}}
RONIN = {"type": "movie", "title": "Ronin", "ids": {"tmdb": "8195"}}
GHOST = {"type": "movie", "title": "Ghost", "ids": {"imdb": "tt9999999"}}
BB = {"type": "show", "title": "Breaking Bad", "ids": {"tmdb": "1396"}}


def test_add_uses_the_list_media_key_and_reports_not_found_and_wrong_type(monkeypatch):
    items_url = pl.URL_LIST_ITEMS.format(list_id="5")
    answer = {"added": 2, "updated": 0, "existing": 0, "not_found": [{"ids": {"imdb": "tt9999999"}}],
              "wrong_type": [{"ids": {"imdb": "tt0113277", "simkl": 11121}, "type": "tv"}], "list": {"id": 5}}
    adapter, session = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="5"): HEAD, ("POST", items_url): _Resp(answer, 201)})
    res = pl.add(adapter, "list:5", [HEAT, RONIN, GHOST, BB])
    post = [c for c in session.calls if c[0] == "POST"]
    assert len(post) == 1 and list(post[0][3]) == ["movies"] and len(post[0][3]["movies"]) == 3
    assert res["ok"] is True and res["count"] == 1
    assert sorted(u["hint"] for u in res["unresolved"]) == ["not_found", "wrong_media_type", "wrong_media_type"]


def test_titles_of_the_other_kind_never_reach_simkl(monkeypatch):
    adapter, session = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="5"): HEAD})
    res = pl.add(adapter, "list:5", [BB])
    assert res["count"] == 0 and res["unresolved"][0]["hint"] == "wrong_media_type"
    assert all(c[0] == "GET" for c in session.calls)


def test_a_2xx_answer_with_an_error_is_a_failure(monkeypatch):
    items_url = pl.URL_LIST_ITEMS.format(list_id="5")
    adapter, _ = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="5"): HEAD, ("POST", items_url): {"error": "something_new"}})
    with pytest.raises(pl.SIMKLPlaylistError, match="something_new"):
        pl.add(adapter, "list:5", [HEAT])


def test_create_keeps_the_new_list_when_seeding_fails(monkeypatch):
    created = {**HEAD, "id": 9, "name": "New"}
    items_url = pl.URL_LIST_ITEMS.format(list_id="9")
    routes = {("POST", pl.URL_LISTS): _Resp(created, 201), pl.URL_LIST.format(list_id="9"): created,
              ("POST", items_url): _Resp({"error": "max_items", "message": "full"}, 403)}
    adapter, _ = _adapter(monkeypatch, routes)
    assert pl.create(adapter, "New", media_type="movie", items=[HEAT]).id == "list:9"


def test_remove_counts_absent_titles_as_removed(monkeypatch):
    url = pl.URL_LIST_ITEMS_REMOVE.format(list_id="5")
    adapter, _ = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="5"): HEAD,
                                        ("POST", url): {"deleted": 1, "not_found": [{"ids": {"tmdb": "8195"}}]}})
    res = pl.remove(adapter, "list:5", [HEAT, RONIN])
    assert res["ok"] is True and res["count"] == 2 and res["unresolved"] == []
    assert "warnings" not in res


ROWS = {"11": HEAT, "12": RONIN}


def _list_with(order):
    items = [{**ROWS[x], "ids": {**ROWS[x]["ids"], "simkl_id": int(x)}} for x in order]
    return {**HEAD, "pagination": {"page": 1, "total_pages": 1, "total_items": len(items)}, "items": items}


def test_reorder_posts_the_full_order_with_allow_reorder_and_checks_it(monkeypatch):
    state = {"order": ["11", "12"]}

    def apply(body):
        state["order"] = [str(row["ids"]["simkl"]) for row in body["movies"]]
        return {"added": 0, "updated": 1, "existing": 1}

    url = pl.URL_LIST_ITEMS.format(list_id="5")
    adapter, session = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="5"): lambda p: _list_with(state["order"]), ("POST", url): apply})
    keys = [item.key for item in reversed(pl.get_snapshot(adapter, "list:5").items)]
    res = pl.reorder(adapter, "list:5", keys)
    post = [c for c in session.calls if c[0] == "POST"]
    assert res == {"ok": True, "count": 2, "reordered": 2}
    assert post[0][2]["allow_reorder"] == "yes" and [r["ids"]["simkl"] for r in post[0][3]["movies"]] == [12, 11]
    assert pl.reorder(adapter, "list:5", keys) == {"ok": True, "count": 0, "reordered": 0}


def test_reorder_reports_an_order_simkl_did_not_apply(monkeypatch):
    url = pl.URL_LIST_ITEMS.format(list_id="5")
    adapter, _ = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="5"): lambda p: _list_with(["11", "12"]), ("POST", url): {"updated": 0}})
    keys = [item.key for item in reversed(pl.get_snapshot(adapter, "list:5").items)]
    assert pl.reorder(adapter, "list:5", keys)["error"] == "playlist_order_unconfirmed"


def test_reorder_batches_overlap_by_one_title(monkeypatch):
    monkeypatch.setattr(pl, "_WRITE_CHUNK", 2)
    rows = {100 + i: {"type": "movie", "title": f"M{i}", "ids": {"tmdb": str(i), "simkl_id": 100 + i}} for i in range(5)}
    state = {"order": sorted(rows)}
    sent = []

    def apply(body):
        chunk = [row["ids"]["simkl"] for row in body["movies"]]
        sent.append(chunk)
        order = list(state["order"])
        for prev, cur in zip(chunk, chunk[1:]):
            if order.index(cur) < order.index(prev):
                order.remove(cur)
                order.insert(order.index(prev) + 1, cur)
        state["order"] = order
        return {"updated": len(chunk)}

    def head(params):
        return {**HEAD, "pagination": {"page": 1, "total_pages": 1, "total_items": 5}, "items": [rows[x] for x in state["order"]]}

    url = pl.URL_LIST_ITEMS.format(list_id="5")
    adapter, _ = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="5"): head, ("POST", url): apply})
    keys = [item.key for item in reversed(pl.get_snapshot(adapter, "list:5").items)]
    assert pl.reorder(adapter, "list:5", keys)["ok"] is True
    assert sent == [[104, 103], [103, 102], [102, 101], [101, 100]]
    assert state["order"] == [104, 103, 102, 101, 100]


def test_create_defaults_to_public_and_seeds_items(monkeypatch):
    created = {**HEAD, "id": 9, "name": "New", "media_type": "tv", "privacy": "public"}
    items_url = pl.URL_LIST_ITEMS.format(list_id="9")
    routes = {("POST", pl.URL_LISTS): _Resp(created, 201), pl.URL_LIST.format(list_id="9"): created,
              ("POST", items_url): {"added": 1, "not_found": [], "wrong_type": []}}
    adapter, session = _adapter(monkeypatch, routes)
    res = pl.create(adapter, "New", media_type="show", items=[BB])
    post = [c for c in session.calls if c[0] == "POST"]
    assert post[0][3] == {"name": "New", "media_type": "tv", "privacy": "public"}
    assert post[1][1] == items_url and list(post[1][3]) == ["shows"]
    assert res.id == "list:9" and res.can_add and res.media_types == ("show",)
    pl.create(adapter, "New", media_type="anime", privacy="private")
    assert [c for c in session.calls if c[0] == "POST"][-1][3]["privacy"] == "private"
    with pytest.raises(pl.SIMKLPlaylistError, match="movies, shows or anime"):
        pl.create(adapter, "New", media_type="playlist")


def test_create_is_refused_for_free_accounts(monkeypatch):
    adapter, session = _adapter(monkeypatch, {}, tier="free")
    with pytest.raises(pl.SIMKLPremiumRequired):
        pl.create(adapter, "New", media_type="movie")
    assert session.calls == []


@pytest.mark.parametrize("status,error,exc", [
    (403, "premium_only", "SIMKLPremiumRequired"),
    (403, "insufficient_scope", "SIMKLListReconnectRequired"),
    (403, "oauth2_token_required", "SIMKLListReconnectRequired"),
    (403, "max_lists", "SIMKLListLimitReached"),
    (403, "no_access", "PlaylistReadOnlyError"),
])
def test_write_errors_are_typed(monkeypatch, status, error, exc):
    adapter, _ = _adapter(monkeypatch, {("POST", pl.URL_LISTS): _Resp({"error": error, "message": "nope"}, status)})
    with pytest.raises(getattr(pl, exc)):
        pl.create(adapter, "New", media_type="movie")


def test_busy_writes_are_retried(monkeypatch):
    monkeypatch.setattr(pl.time, "sleep", lambda s: None)
    created = {**HEAD, "id": 9, "name": "New"}
    busy = _Resp({"error": "rate_limit"}, 400)
    adapter, session = _adapter(monkeypatch, {("POST", pl.URL_LISTS): [busy, busy, _Resp(created, 201)]})
    assert pl.create(adapter, "New", media_type="movie").id == "list:9"
    assert len([c for c in session.calls if c[0] == "POST"]) == 3


def test_rename_and_delete_only_touch_own_lists(monkeypatch):
    url = pl.URL_LIST.format(list_id="5")
    routes = {url: HEAD, ("POST", url): {**HEAD, "name": "Renamed"}, ("DELETE", url): _Resp(None, 204)}
    adapter, session = _adapter(monkeypatch, routes)
    assert pl.rename(adapter, "list:5", "Renamed").name == "Renamed"
    assert pl.delete(adapter, "list:5") == {"ok": True, "count": 1}
    assert [(c[0], c[3]) for c in session.calls if c[0] != "GET"] == [("POST", {"name": "Renamed"}), ("DELETE", None)]
    with pytest.raises(pl.SIMKLPlaylistError):
        pl.rename(adapter, "plantowatch", "x")


def test_privacy_change_posts_to_the_list_and_skips_status_buckets(monkeypatch):
    url = pl.URL_LIST.format(list_id="5")
    adapter, session = _adapter(monkeypatch, {url: HEAD, ("POST", url): {**HEAD, "privacy": "unlisted"}})
    assert pl.set_privacy(adapter, "list:5", "unlisted").extra["privacy"] == "unlisted"
    assert [c[3] for c in session.calls if c[0] == "POST"] == [{"privacy": "unlisted"}]
    with pytest.raises(pl.SIMKLPlaylistError):
        pl.set_privacy(adapter, "plantowatch", "private")
    assert pl.list_resources(SimpleNamespace(client=SimpleNamespace(session=_Session({pl.URL_USER_LISTS.format(user_id="77"): LISTS})),
                                             cfg=SimpleNamespace(timeout=5.0), instance_id="default"))[0].extra["can_edit_privacy"] is False


def test_a_batch_still_busy_after_retries_is_unresolved_not_raised(monkeypatch):
    monkeypatch.setattr(pl.time, "sleep", lambda s: None)
    items_url = pl.URL_LIST_ITEMS.format(list_id="5")
    busy = _Resp({"error": "rate_limit"}, 400)
    adapter, _ = _adapter(monkeypatch, {pl.URL_LIST.format(list_id="5"): HEAD, ("POST", items_url): [busy] * 4})
    res = pl.add(adapter, "list:5", [HEAT])
    assert res["ok"] is False and res["count"] == 0 and res["unresolved"][0]["hint"] == "http:400"
