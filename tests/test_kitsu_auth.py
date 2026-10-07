# tests/test_kitsu_auth.py
# CrossWatch - Kitsu tokens, account isolation and auth API tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import json
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import requests
from fastapi import FastAPI
from fastapi.testclient import TestClient

from cw_platform.config_base import DEFAULT_CFG, redact_config
from cw_platform.provider_instances import build_provider_config_view, resolve_provider_block
from providers.auth import _auth_KITSU as auth
from providers.sync import _mod_KITSU as module


def response(code, data):
    result = requests.Response()
    result.status_code = code
    result._content = json.dumps(data).encode()
    result.headers["Content-Type"] = "application/json"
    return result


def tokens():
    return {"access_token": "new-access", "refresh_token": "new-refresh", "created_at": int(time.time()), "expires_in": 3600}


@pytest.fixture()
def account(monkeypatch):
    cfg = {"kitsu": {"instances": {"P01": {"access_token": "old-access", "refresh_token": "old-refresh", "expires_at": 1,
                                             "user": {"id": "7", "name": "Test"}}}}}
    monkeypatch.setattr(auth, "load_config", lambda: copy.deepcopy(cfg))

    def save(value):
        cfg.clear()
        cfg.update(copy.deepcopy(value))

    monkeypatch.setattr(auth, "save_config", save)
    return cfg


def test_password_exchange_form_encoded_and_never_retained(monkeypatch, caplog):
    calls = []

    def post(url, **kwargs):
        calls.append(copy.deepcopy((url, kwargs)))
        return response(200, tokens())

    monkeypatch.setattr(auth.requests, "post", post)
    monkeypatch.setattr(auth, "request_with_auth", lambda *args, **kwargs: response(200,
        {"data": [{"id": "7", "type": "users", "attributes": {"name": "Test", "email": "private@example.com"}}]}))
    result = auth.login("tester", "p+&= secret")
    assert calls[0][0] == auth.TOKEN_URL
    assert calls[0][1]["data"] == {"grant_type": "password", "username": "tester", "password": "p+&= secret"}
    assert calls[0][1]["allow_redirects"] is False
    assert set(result) == {"access_token", "refresh_token", "expires_at", "scope", "reauth_required", "user"}
    assert result["user"] == {"id": "7", "name": "Test"}
    assert "secret" not in caplog.text


def test_server_error_body_never_exposes_credentials(monkeypatch):
    monkeypatch.setattr(auth.requests, "post", lambda *a, **kw: response(400, {"error": "invalid_grant", "error_description": "password: secret"}))
    with pytest.raises(auth.KitsuAuthError) as exc:
        auth.login("tester", "secret")
    assert str(exc.value) == "invalid_grant"


def test_refresh_single_flight_for_narrowed_named_account(account, monkeypatch):
    calls = []

    def post(url, **kwargs):
        calls.append(copy.deepcopy(kwargs))
        return response(200, tokens())

    monkeypatch.setattr(auth.requests, "post", post)
    view = build_provider_config_view(account, "kitsu", "P01")
    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(lambda _: auth.refresh_token(view, instance_id="P01"), range(5)))
    assert len(calls) == 1
    assert all(result["access_token"] == "new-access" for result in results)
    assert "access_token" not in account["kitsu"]
    assert account["kitsu"]["instances"]["P01"]["refresh_token"] == "new-refresh"
    assert calls[0]["data"] == {"grant_type": "refresh_token", "refresh_token": "old-refresh"}


def test_terminal_refresh_clears_only_named_profile(account, monkeypatch):
    account["kitsu"].update({"access_token": "default-token", "refresh_token": "default-refresh"})
    monkeypatch.setattr(auth.requests, "post", lambda *a, **kw: response(400, {"error": "invalid_grant"}))
    with pytest.raises(auth.KitsuAuthError):
        auth.refresh_token(account, instance_id="P01")
    block = account["kitsu"]["instances"]["P01"]
    assert block["access_token"] == block["refresh_token"] == ""
    assert block["reauth_required"]
    assert account["kitsu"]["access_token"] == "default-token"


def test_transient_refresh_preserves_tokens(account, monkeypatch):
    monkeypatch.setattr(auth.requests, "post", lambda *a, **kw: response(503, {"error": "unavailable"}))
    with pytest.raises(auth.KitsuAuthError):
        auth.refresh_token(account, instance_id="P01")
    assert account["kitsu"]["instances"]["P01"]["refresh_token"] == "old-refresh"


def test_authenticated_request_refreshes_and_retries_once(account, monkeypatch):
    from providers.sync import _mod_common

    account["kitsu"]["instances"]["P01"]["expires_at"] = int(time.time()) + 3600
    headers = []
    monkeypatch.setattr(auth.requests, "post", lambda *a, **kw: response(200, tokens()))

    def request(session, method, url, **kwargs):
        headers.append(dict(kwargs["headers"]))
        return response(401, {})

    monkeypatch.setattr(_mod_common, "request_with_retries", request)
    view = build_provider_config_view(account, "kitsu", "P01")
    result = auth.request_with_auth(requests.Session(), "GET", auth.IDENTITY_URL, cfg=view, instance_id="P01")
    assert result.status_code == 401
    assert [h["Authorization"] for h in headers] == ["Bearer old-access", "Bearer new-access"]
    with pytest.raises(auth.KitsuAuthError, match="Invalid Kitsu"):
        auth.request_with_auth(requests.Session(), "GET", "https://example.com", cfg=view)


def test_profile_only_configuration_and_redaction(account):
    view = build_provider_config_view(account, "kitsu", "P01")
    view["_cw_provider_instance"] = "P01"
    assert module.OPS.is_configured(view)
    assert resolve_provider_block(view, "kitsu", "P01")["access_token"] == "old-access"
    assert not auth.is_configured(DEFAULT_CFG["kitsu"])
    assert "password" not in DEFAULT_CFG["kitsu"]
    masked = json.dumps(redact_config(copy.deepcopy(account)))
    assert "old-access" not in masked and "old-refresh" not in masked


def test_rate_limited_reads_retry_but_writes_are_not_replayed(monkeypatch):
    from providers.sync import _mod_common

    cfg = {"kitsu": {"access_token": "token"}}
    waits = []
    calls = []
    monkeypatch.setattr(_mod_common, "_sleep_cancellable", waits.append)
    session = requests.Session()

    def request(method, url, **kwargs):
        calls.append(method)
        result = response(429 if len(calls) == 1 else 200, {"data": []})
        result.headers["Retry-After"] = "2"
        return result

    monkeypatch.setattr(session, "request", request)
    assert auth.request_with_auth(session, "GET", auth.IDENTITY_URL, cfg=cfg).status_code == 200
    assert calls == ["GET", "GET"] and waits == [2]
    calls.clear()
    waits.clear()
    assert auth.request_with_auth(session, "POST", auth.API_URL + "/library-entries", cfg=cfg).status_code == 429
    assert calls == ["POST"] and not waits


def test_auth_routes_stage_tokens_and_disconnect_named_profile(account, monkeypatch):
    from api import authenticationAPI as api
    from api import probesAPI

    calls = []
    monkeypatch.setattr(auth, "login", lambda username, password: {"access_token": "new-access", "refresh_token": "new-refresh", "user": {"id": "7"}})
    monkeypatch.setattr(api, "load_config", lambda: copy.deepcopy(account))
    monkeypatch.setattr(api, "save_config", lambda cfg: calls.append(cfg))
    monkeypatch.setattr(probesAPI, "invalidate_provider_caches", lambda *args: None)
    app = FastAPI()
    api.register_auth(app)
    client = TestClient(app)
    result = client.post("/api/kitsu/connect?instance=P01", json={"username": "test", "password": "secret"})
    assert result.status_code == 200 and result.json()["ok"]
    assert result.headers["cache-control"] == "no-store"
    assert "secret" not in result.text and not calls
    assert client.get("/api/kitsu/status?instance=P01").json()["connected"]
    assert not client.get("/api/kitsu/status").json()["connected"]
    assert client.post("/api/kitsu/disconnect?instance=P01").json()["ok"]
    assert calls[-1]["kitsu"]["instances"]["P01"]["access_token"] == ""
    assert "access_token" not in calls[-1]["kitsu"]


def test_probe_identity_body_cache_and_secret_key(account, monkeypatch):
    from api import probesAPI as probes

    view = build_provider_config_view(account, "kitsu", "P01")
    probes.invalidate_provider_caches("kitsu")
    monkeypatch.setattr(probes, "_authenticated_account", lambda *args: (200, '{"data":[]}'))
    assert not probes._probe_kitsu_detail(view, max_age_sec=0)[0]
    monkeypatch.setattr(probes, "_authenticated_account", lambda *args: (200, '{"data":[{"id":"7","type":"users","attributes":{"name":"Test"}}]}'))
    assert probes._probe_kitsu_detail(view, max_age_sec=0)[0]
    assert "old-access" not in probes._probe_key("kitsu", view)
    assert "old-refresh" not in probes._probe_key("kitsu", view)
    assert probes.kitsu_user_info(view, max_age_sec=0)["user"]["id"] == "7"


def test_status_discovers_profile_without_default(account, monkeypatch):
    from api import probesAPI as probes

    probes.invalidate_provider_caches("kitsu")
    monkeypatch.setitem(probes.DETAIL_PROBES, "KITSU", lambda cfg, **kw: (True, ""))
    monkeypatch.setattr(probes, "kitsu_user_info", lambda cfg, **kw: {"user": {"id": "7"}})
    monkeypatch.setitem(probes.USERINFO_FNS, "KITSU", probes.kitsu_user_info)
    app = FastAPI()
    probes.register_probes(app, lambda: account)
    data = TestClient(app).get("/api/status?fresh=1").json()
    assert data["providers"]["KITSU"]["connected"]
    assert data["providers"]["KITSU"]["rep_instance"] == "P01"
    assert data["kitsu_connected"]
