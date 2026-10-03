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

    def post(self, url, **kwargs):
        raise AssertionError("custom lists must never be written")


LISTS = {"pagination": {"page": 1, "total_pages": 1}, "lists": [
    {"id": 5, "name": "Heist", "media_type": "movies", "type": "regular", "privacy": "private",
     "counts": {"items": 2}, "updated_at": "2026-09-16T05:47:25Z"},
    {"id": 6, "name": "Odd", "media_type": "games", "counts": {"items": 1}},
]}


def _adapter(monkeypatch, routes, tier="vip"):
    session = _Session(routes)
    monkeypatch.setattr(pl, "adapter_headers", lambda adapter: {"simkl-api-key": "cid"})
    monkeypatch.setattr(pl, "fetch_user_settings", lambda *a, **k: {"account": {"id": 77, "type": tier}})
    return SimpleNamespace(client=SimpleNamespace(session=session), cfg=SimpleNamespace(timeout=5.0), instance_id="default"), session


def test_custom_lists_are_listed_read_only_for_pro_and_vip(monkeypatch):
    adapter, _ = _adapter(monkeypatch, {pl.URL_USER_LISTS.format(user_id="77"): LISTS})
    custom = [r for r in pl.list_resources(adapter) if r.extra.get("endpoint_type") == "custom_list"]
    assert [(r.id, r.name, r.media_types, r.extra["item_count"]) for r in custom] == [("list:5", "Heist", ("movie",), 2)]
    assert not (custom[0].can_add or custom[0].can_remove or custom[0].can_reorder)
    assert custom[0].extra["can_rename"] is False and custom[0].extra["can_delete"] is False


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


def test_writes_to_a_custom_list_are_refused_without_calling_simkl(monkeypatch):
    adapter, session = _adapter(monkeypatch, {})
    item = {"type": "movie", "ids": {"imdb": "tt0113277"}}
    for result in (pl.add(adapter, "list:5", [item]), pl.remove(adapter, "list:5", [item])):
        assert result["ok"] is False and result["count"] == 0 and result["confirmed_keys"] == []
        assert result["unresolved"][0]["hint"] == "read_only"
    assert session.calls == []
