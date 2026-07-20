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
