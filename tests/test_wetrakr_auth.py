# tests/test_wetrakr_auth.py
# CrossWatch - WeTrakr authentication, profile isolation, and connection integration tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import base64
import hashlib
import copy
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest

from providers.auth import _auth_WETRAKR as wt


@dataclass
class Response:
    status_code: int = 200
    payload: Any = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)

    def json(self):
        return self.payload

    @property
    def text(self):
        return json.dumps(self.payload)


@pytest.fixture
def store(monkeypatch, config_base):
    state = {"cfg": {"wetrakr": {"instances": {"P01": {}, "P02": {"access_token": "other"}}}}}
    monkeypatch.setattr(wt, "_load_full_cfg", lambda: copy.deepcopy(state["cfg"]))
    monkeypatch.setattr(wt, "_save_full_cfg", lambda cfg: state.update(cfg=copy.deepcopy(cfg)))
    monkeypatch.setattr(wt.time, "time", lambda: 1000)
    monkeypatch.delenv("CW_UA", raising=False)
    monkeypatch.delenv("CW_WETRAKR_UA", raising=False)
    wt._REFRESH_RETRY_AT.clear()
    wt._PENDING.clear()

    def unexpected(*args, **kwargs):
        raise AssertionError("Unexpected network request")

    monkeypatch.setattr(wt.requests.sessions.Session, "request", unexpected)
    return state


def block(store):
    return store["cfg"]["wetrakr"]["instances"]["P01"]


def connected(store, expires=900):
    block(store).update(access_token="old-access", refresh_token="old-refresh", expires_at=expires)


def test_pkce_start_generates_s256_and_oob_without_network_or_persisted_secrets(store):
    result = wt.start_oauth(instance_id="P01")
    query = parse_qs(urlsplit(result["authorization_url"]).query)
    pending = wt._PENDING["P01"]
    verifier = pending["verifier"]
    expected = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    assert result["ok"]
    assert query["code_challenge"] == [expected]
    assert query["code_challenge_method"] == ["S256"]
    assert query["redirect_uri"] == ["urn:ietf:wg:oauth:2.0:oob"]
    assert query["client_id"] == [wt.app_client_id()]
    assert query["state"] == [result["flow_id"]]
    assert 43 <= len(verifier) <= 128
    assert verifier not in json.dumps(result) + json.dumps(store)
    assert not block(store)
    next_flow = wt.start_oauth(instance_id="P01")
    assert next_flow["flow_id"] != result["flow_id"]
    assert wt._PENDING["P01"]["verifier"] != verifier


def test_exchange_omits_client_secret_validates_account_and_persists_selected_profile(store, monkeypatch):
    flow = wt.start_oauth(instance_id="P01")
    verifier = wt._PENDING["P01"]["verifier"]
    sent = []

    def post(url, **kwargs):
        sent.append((url, kwargs))
        return Response(payload={"access_token": "access", "refresh_token": "refresh", "expires_in": 604800})

    monkeypatch.setattr(wt.requests, "post", post)
    monkeypatch.setattr(wt.requests, "get", lambda *a, **k: Response(payload={"id": 42, "info": {"username": "alice"}, "plan": "vip"}))
    result = wt.finish_oauth(instance_id="P01", flow_id=flow["flow_id"], code="auth-code")
    assert result["ok"] and result["vip"]
    assert sent[0][0] == wt.TOKEN_URL
    assert sent[0][1]["json"] == {"client_id": wt.app_client_id(), "code": "auth-code", "code_verifier": verifier}
    headers = sent[0][1]["headers"]
    assert headers["wetrakr-api-key"] == wt.app_client_id()
    assert headers["wetrakr-api-version"] == "1"
    assert headers["User-Agent"].startswith("CrossWatch/")
    assert "Mozilla" not in headers["User-Agent"]
    assert sent[0][1]["allow_redirects"] is False
    assert block(store)["refresh_token"] == "refresh"
    assert block(store)["expires_at"] == 605800
    assert "access_token" not in store["cfg"]["wetrakr"]
    assert store["cfg"]["wetrakr"]["instances"]["P02"] == {"access_token": "other"}
    assert "P01" not in wt._PENDING
    assert "access_token" not in result and "refresh_token" not in result


def test_flow_is_bound_to_profile_and_cannot_be_replayed(store, monkeypatch):
    flow = wt.start_oauth(instance_id="P01")
    assert wt.finish_oauth(instance_id="P02", flow_id=flow["flow_id"], code="c")["error"] == "invalid_flow"
    assert wt.finish_oauth(instance_id="P01", flow_id="wrong", code="c")["error"] == "invalid_flow"
    wt.cancel_oauth(instance_id="P01", flow_id=flow["flow_id"])
    assert wt.finish_oauth(instance_id="P01", flow_id=flow["flow_id"], code="c")["error"] == "invalid_flow"


def test_stale_cancel_cannot_cancel_new_login(store):
    old = wt.start_oauth(instance_id="P01")
    new = wt.start_oauth(instance_id="P01")
    wt.cancel_oauth(instance_id="P01", flow_id=old["flow_id"])
    assert wt._PENDING["P01"]["state"] == new["flow_id"]


def test_expiry_and_reconnect_cancellation_preserve_existing_credentials(store):
    connected(store)
    flow = wt.start_oauth(instance_id="P01")
    assert block(store)["access_token"] == "old-access"
    wt._PENDING["P01"]["expires_at"] = 999
    assert wt.finish_oauth(instance_id="P01", flow_id=flow["flow_id"], code="c")["error"] == "expired_flow"
    flow = wt.start_oauth(instance_id="P01")
    wt.cancel_oauth(instance_id="P01", flow_id=flow["flow_id"])
    assert block(store)["access_token"] == "old-access"


@pytest.mark.parametrize("code", ["", "with spaces", "x" * 2049])
def test_invalid_code_is_rejected_without_network(store, code):
    flow = wt.start_oauth(instance_id="P01")
    result = wt.finish_oauth(instance_id="P01", flow_id=flow["flow_id"], code=code)
    assert result["error"] == "invalid_code" and result["retryable"]


@pytest.mark.parametrize("response", [Response(400), Response(403, {"cloudflare_error": "browser_signature_banned"}), Response(503)])
def test_failed_exchange_is_not_retried_automatically(store, monkeypatch, response):
    flow = wt.start_oauth(instance_id="P01")
    calls = []
    monkeypatch.setattr(wt.requests, "post", lambda *a, **k: (calls.append(k), response)[1])
    assert not wt.finish_oauth(instance_id="P01", flow_id=flow["flow_id"], code="c")["ok"]
    assert wt.finish_oauth(instance_id="P01", flow_id=flow["flow_id"], code="c")["error"] == "invalid_flow"
    assert len(calls) == 1


def test_exchange_rate_limit_retains_verifier_and_honors_retry_after(store, monkeypatch):
    flow = wt.start_oauth(instance_id="P01")
    calls = []
    monkeypatch.setattr(wt.requests, "post", lambda *a, **k: (calls.append(k), Response(429, {}, {"Retry-After": "120"}))[1])
    assert wt.finish_oauth(instance_id="P01", flow_id=flow["flow_id"], code="c")["retryable"]
    assert wt.finish_oauth(instance_id="P01", flow_id=flow["flow_id"], code="c")["retry_after"] >= 120
    assert len(calls) == 1


@pytest.mark.parametrize("identity", [Response(401), Response(payload={"info": {"username": "alice"}})])
def test_invalid_account_cannot_become_connected(store, monkeypatch, identity):
    flow = wt.start_oauth(instance_id="P01")
    monkeypatch.setattr(wt.requests, "post", lambda *a, **k: Response(payload={"access_token": "access", "refresh_token": "refresh"}))
    monkeypatch.setattr(wt.requests, "get", lambda *a, **k: identity)
    assert not wt.finish_oauth(instance_id="P01", flow_id=flow["flow_id"], code="c")["ok"]
    assert not wt.is_configured(block(store))


def test_identity_retry_does_not_exchange_code_twice(store, monkeypatch):
    flow = wt.start_oauth(instance_id="P01")
    calls = []
    monkeypatch.setattr(wt.requests, "post", lambda *a, **k: (calls.append(k), Response(payload={"access_token": "access", "refresh_token": "refresh"}))[1])
    monkeypatch.setattr(wt.requests, "get", lambda *a, **k: Response(503))
    assert wt.finish_oauth(instance_id="P01", flow_id=flow["flow_id"], code="c")["retryable"]
    wt._PENDING["P01"]["retry_at"] = 0
    monkeypatch.setattr(wt.requests, "get", lambda *a, **k: Response(payload={"id": 42, "plan": "free"}))
    assert wt.finish_oauth(instance_id="P01", flow_id=flow["flow_id"], code="c")["ok"]
    assert len(calls) == 1


def test_refresh_rotation_is_single_flight_for_narrowed_profile(store, monkeypatch):
    from cw_platform.provider_instances import build_provider_config_view

    connected(store)
    calls = []
    monkeypatch.setattr(wt.requests, "post", lambda *a, **k: (calls.append(k), Response(payload={"access_token": "new-access", "new_refresh_token": "new-refresh", "expires_in": 3600}))[1])
    view = build_provider_config_view(store["cfg"], "wetrakr", "P01")
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: wt.refresh_token(view, instance_id="P01", force=True, rejected_token="old-access"), range(6)))
    assert all(result["ok"] for result in results)
    assert len(calls) == 1
    assert calls[0]["json"] == {"refresh_token": "old-refresh"}
    assert block(store)["refresh_token"] == "new-refresh"
    assert "access_token" not in store["cfg"]["wetrakr"]


@pytest.mark.parametrize("response", [Response(400, {"error": "invalid_grant"}), Response(401)])
def test_terminal_refresh_requires_reconnect(store, monkeypatch, response):
    connected(store)
    monkeypatch.setattr(wt.requests, "post", lambda *a, **k: response)
    assert not wt.refresh_token(instance_id="P01")["ok"]
    assert block(store)["reauth_required"]
    assert not block(store)["access_token"] and not block(store)["refresh_token"]
    assert store["cfg"]["wetrakr"]["instances"]["P02"]["access_token"] == "other"


@pytest.mark.parametrize("response", [Response(403, {"cloudflare_error": "browser_signature_banned", "error_code": 1010}), Response(429, {}, {"Retry-After": "120"}), Response(503)])
def test_transient_refresh_preserves_tokens_and_backs_off(store, monkeypatch, response):
    connected(store)
    calls = []
    monkeypatch.setattr(wt.requests, "post", lambda *a, **k: (calls.append(k), response)[1])
    first = wt.refresh_token(instance_id="P01")
    second = wt.refresh_token(instance_id="P01")
    assert not first["ok"] and not second["ok"]
    assert len(calls) == 1
    assert block(store)["refresh_token"] == "old-refresh"
    if response.status_code == 429:
        assert second["retry_after"] >= 120


def test_request_retries_once_and_rejected_rotated_token_requires_reconnect(store, monkeypatch):
    connected(store, expires=5000)
    monkeypatch.setattr(wt.requests, "post", lambda *a, **k: Response(payload={"access_token": "new", "new_refresh_token": "rotated", "expires_in": 3600}))
    sent = []

    def call(*args, **kwargs):
        sent.append(dict(kwargs["headers"]))
        return Response(401)

    response = wt.request_with_auth(None, "GET", wt.ME_URL, cfg=store["cfg"], instance_id="P01", request_func=call)
    assert response.status_code == 401
    assert [headers["Authorization"] for headers in sent] == ["Bearer old-access", "Bearer new"]
    assert block(store)["reauth_required"]


def test_proactive_refresh_failure_does_not_retry_again_on_401(store, monkeypatch):
    connected(store)
    calls = []
    monkeypatch.setattr(wt.requests, "post", lambda *a, **k: (calls.append(k), Response(503))[1])
    wt.request_with_auth(None, "GET", wt.ME_URL, cfg=store["cfg"], instance_id="P01", request_func=lambda *a, **k: Response(401))
    assert len(calls) == 1
    assert block(store)["refresh_token"] == "old-refresh"


def test_request_does_not_send_token_to_other_hosts(store):
    connected(store)
    with pytest.raises(wt.WeTrakrAuthError, match="invalid_api_url"):
        wt.request_with_auth(None, "GET", "https://other.test/", cfg=store["cfg"], instance_id="P01")


def test_status_and_redacted_config_never_expose_credentials(store):
    from cw_platform.config_base import redact_config

    connected(store)
    wt.start_oauth(instance_id="P01")
    wt._PENDING["P01"]["token_response"] = {"access_token": "candidate"}
    text = json.dumps(wt.status_for_block(block(store))) + json.dumps(redact_config(store["cfg"]))
    for secret in ("old-access", "old-refresh", "private-device", "candidate"):
        assert secret not in text


def test_account_status_exposes_plan_and_quota_details(store, monkeypatch):
    connected(store)
    responses = iter([Response(payload={"id": 42, "info": {"username": "alice"}, "plan": "vip"}), Response(payload={"plan": "VIP", "tier": "vip", "features": [{"key": "lists", "type": "quota", "used": 3, "limit": 5}]})])
    monkeypatch.setattr(wt, "request_with_auth", lambda *a, **k: next(responses))
    status = wt.account_status(store["cfg"], instance_id="P01")
    assert status["connected"] and status["vip"] and status["username"] == "alice"
    assert status["plan_usage"]["features"][0]["used"] == 3


def test_optional_quota_failure_does_not_disconnect_verified_account(store, monkeypatch):
    connected(store)

    def request(session, method, url, **kwargs):
        if url == wt.PLAN_URL:
            raise wt.requests.Timeout()
        return Response(payload={"id": 42, "plan": "free"})

    monkeypatch.setattr(wt, "request_with_auth", request)
    status = wt.account_status(store["cfg"], instance_id="P01")
    assert status["connected"] and status["plan"] == "free" and not status["vip"]
    assert "plan_usage" not in status


def test_routes_use_oauth_not_device_endpoints(store, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import authenticationAPI as api

    monkeypatch.setattr(api, "load_config", lambda: copy.deepcopy(store["cfg"]))
    app = FastAPI()
    api.register_auth(app)
    client = TestClient(app)
    assert client.post("/api/wetrakr/device/start").status_code == 404
    flow = client.post("/api/wetrakr/oauth/start?instance=P01").json()
    assert flow["ok"]
    result = client.post("/api/wetrakr/oauth/finish?instance=P02", json={"flow_id": flow["flow_id"], "code": "c"})
    assert result.json()["error"] == "invalid_flow"
    assert client.post("/api/wetrakr/oauth/cancel?instance=P01", json={"flow_id": flow["flow_id"]}).json()["ok"]
    assert client.get("/api/wetrakr/status?instance=P01").json()["connected"] is False
    assert store["cfg"]["wetrakr"]["instances"]["P02"]["access_token"] == "other"


def test_disconnect_revokes_selected_profile_and_busts_probe_cache(store, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import authenticationAPI as api

    connected(store)
    sent = []
    monkeypatch.setattr(wt.requests, "post", lambda url, **k: (sent.append((url, k)), Response())[1])
    monkeypatch.setattr(api, "load_config", lambda: copy.deepcopy(store["cfg"]))
    app = FastAPI()
    cache = {"wetrakr": (123, True)}
    api.register_auth(app, probe_cache=cache)
    assert TestClient(app).post("/api/wetrakr/disconnect?instance=P01").json()["ok"]
    assert sent[0][0] == wt.REVOKE_URL
    assert sent[0][1]["headers"]["Authorization"] == "Bearer old-access"
    assert not block(store)["access_token"]
    assert cache["wetrakr"] == (0, False)


def test_registered_as_tracker_without_sync_features(store):
    from cw_platform.modules_registry import MODULES, sync_provider_names
    from providers.auth import runtime
    from providers.auth.registry import auth_providers_html

    assert MODULES["AUTH"]["_auth_WETRAKR"] == "providers.auth._auth_WETRAKR"
    assert "WETRAKR" not in sync_provider_names(upper=True)
    assert runtime._backend("wetrakr") is wt
    assert wt.PROVIDER.manifest().fields == []
    html = auth_providers_html()
    assert html.index('id="sec-auth-trackers"') < html.index('id="sec-wetrakr"') < html.index('id="sec-auth-clients"')


def test_probe_validates_identity_and_uses_profile_instance(store, monkeypatch):
    from api import probesAPI as probes
    from providers.auth import runtime

    connected(store)
    calls = []

    def request(provider, session, method, url, **kwargs):
        calls.append((provider, url, kwargs))
        return Response(payload={"id": 42, "info": {"username": "alice"}, "plan": "vip"})

    monkeypatch.setattr(runtime, "request_with_auth", request)
    probes.invalidate_provider_caches("wetrakr")
    view = probes._cfg_view_for(store["cfg"], "WETRAKR", "P01")
    assert probes._probe_wetrakr_detail(view, max_age_sec=0) == (True, "")
    assert probes.wetrakr_user_info(view, max_age_sec=0)["vip"]
    assert calls[0][0:2] == ("wetrakr", wt.ME_URL)
    assert calls[0][2]["instance_id"] == "P01"
    assert "old-access" not in probes._probe_key("wetrakr", view)


@pytest.mark.parametrize("response, reason", [(Response(401), "WeTrakr: reconnect required"), (Response(payload={"info": {"username": "alice"}}), "WeTrakr: invalid response")])
def test_probe_rejects_unauthorized_or_invalid_identity(store, monkeypatch, response, reason):
    from api import probesAPI as probes
    from providers.auth import runtime

    connected(store)
    monkeypatch.setattr(runtime, "request_with_auth", lambda *a, **k: response)
    probes.invalidate_provider_caches("wetrakr")
    assert probes._probe_wetrakr_detail(probes._cfg_view_for(store["cfg"], "WETRAKR", "P01"), max_age_sec=0) == (False, reason)
