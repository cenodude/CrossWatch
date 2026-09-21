from __future__ import annotations

import importlib
from typing import Any

_common = importlib.import_module("providers.sync.trakt._common")


class FakeResp:
    def __init__(self, status: int, payload: Any = None):
        self.status_code = status
        self._payload = payload
        self.text = "x" if payload is not None else ""
        self.headers: dict[str, str] = {}

    def json(self) -> Any:
        return self._payload


class FakeCfg:
    client_id = "cid"
    access_token = "tok"
    timeout = 15.0
    max_retries = 1


class FakeSession:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}


class FakeClient:
    def __init__(self) -> None:
        self.session = FakeSession()


class FakeAdapter:
    def __init__(self) -> None:
        self.cfg = FakeCfg()
        self.client = FakeClient()


def _patch_settings(monkeypatch, payload, status=200):
    _common._SETTINGS_MEMO = (0.0, None)

    def fake_rwr(sess, method, url, **kw):
        return FakeResp(status, payload)

    monkeypatch.setattr(_common, "request_with_retries", fake_rwr)


def test_api_supplied_limit(monkeypatch):
    _patch_settings(monkeypatch, {"user": {"vip": False}, "limits": {"watchlist": {"item_count": 400}}})
    assert _common.resolve_watchlist_limit(FakeAdapter(), {}) == 400


def test_config_override(monkeypatch):
    _patch_settings(monkeypatch, {"user": {"vip": False}, "limits": {"watchlist": {"item_count": 400}}})
    assert _common.resolve_watchlist_limit(FakeAdapter(), {"watchlist_limit": 42}) == 42


def test_free_account_fallback(monkeypatch):
    _patch_settings(monkeypatch, {"user": {"vip": False}, "limits": {}})
    assert _common.resolve_watchlist_limit(FakeAdapter(), {}) == 250


def _wl_row(n: int) -> dict[str, Any]:
    return {"type": "movie", "movie": {"title": f"M{n}", "year": 2000, "ids": {"trakt": n, "tmdb": 10000 + n}}}


def test_watchlist_index_follows_pages(monkeypatch):
    _wl = importlib.import_module("providers.sync.trakt._watchlist")
    rows = [_wl_row(n) for n in range(1, 251)]
    calls: list[dict[str, Any]] = []

    def fake_rwr(sess, method, url, **kw):
        params = kw.get("params") or {}
        calls.append(dict(params))
        page, limit = int(params["page"]), int(params["limit"])
        resp = FakeResp(200, rows[(page - 1) * limit : page * limit])
        resp.headers = {"X-Pagination-Page-Count": str((len(rows) + limit - 1) // limit)}
        return resp

    monkeypatch.setattr(_wl, "request_with_retries", fake_rwr)
    monkeypatch.setattr(_wl, "fetch_last_activities", lambda *a, **k: None)
    monkeypatch.setattr(_wl, "update_watermarks_from_last_activities", lambda acts: {})
    monkeypatch.setattr(_wl, "_shadow_load", lambda: {"etag": None, "ts": 0, "items": {}})
    monkeypatch.setattr(_wl, "_shadow_save", lambda etag, items: None)
    monkeypatch.setattr(_wl, "headers_for_adapter", lambda adapter: {})

    idx = _wl.build_index(FakeAdapter())

    assert len(idx) == 250
    assert [c["page"] for c in calls] == [1, 2, 3, 4]
    assert all(c["limit"] == 100 for c in calls)


def test_ratings_bucket_uses_applied_limit(monkeypatch):
    _rat = importlib.import_module("providers.sync.trakt._ratings")
    rows = [{"type": "movie", "rating": 8, "rated_at": "2026-01-01T00:00:00.000Z", **_wl_row(n)} for n in range(1, 601)]
    pages: list[int] = []

    class Sess:
        def get(self, url, headers=None, params=None, timeout=None):
            page, limit = int(params["page"]), min(int(params["limit"]), 250)
            pages.append(page)
            resp = FakeResp(200, rows[(page - 1) * limit : page * limit])
            resp.headers = {"X-Pagination-Limit": str(limit)}
            return resp

    out = _rat._fetch_bucket(Sess(), {}, _rat.URL_RAT_MOV, "movie", 1000, 50, 5.0, 0)

    assert len(out) == 600
    assert pages == [1, 2, 3, 4]
