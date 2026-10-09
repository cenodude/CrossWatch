# tests/test_myanimelist_auth.py
# CrossWatch - MyAnimeList authentication and profile integration tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

import pytest

from providers.auth import _auth_MYANIMELIST as mal


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
    state = {"cfg": {"myanimelist": {"instances": {"P01": {}, "P02": {"access_token": "other"}}}}}
    monkeypatch.setattr(mal, "load_config", lambda: copy.deepcopy(state["cfg"]))
    monkeypatch.setattr(mal, "save_config", lambda cfg: state.update(cfg=copy.deepcopy(cfg)))
    monkeypatch.setattr(mal.time, "time", lambda: 1000)
    mal._PENDING.clear()
    mal._REFRESH_RETRY_AT.clear()
    def unexpected(*args, **kwargs):
        raise AssertionError("Unexpected network request")
    monkeypatch.setattr(mal.requests.sessions.Session, "request", unexpected)
    return state


def block(store):
    return store["cfg"]["myanimelist"]["instances"]["P01"]


def tokens():
    return {"ok": True, "status": "authorized", "access_token": "new-access", "refresh_token": "new-refresh", "expires_in": 3600, "token_type": "Bearer"}


def connected(store, expires=900):
    block(store).update(access_token="old-access", refresh_token="old-refresh", expires_at=expires, auth_broker="https://auth.crosswatch.app")


def start(monkeypatch):
    monkeypatch.setattr(mal.requests, "post", lambda *a, **k: Response(payload={"ok": True, "session_id": "session", "poll_token": "p" * 43, "authorization_url": "https://myanimelist.net/v1/oauth2/authorize?state=oauth-state", "expires_in": 600, "interval": 5}))
    return mal.start_oauth(instance_id="P01")


def test_broker_is_fixed_and_ignores_environment(store, monkeypatch):
    monkeypatch.setenv("CROSSWATCH_MAL_AUTH_BROKER", "https://different.example.com")
    calls = []
    monkeypatch.setattr(mal.requests, "post", lambda url, **kwargs: calls.append(url) or Response(503))
    mal.start_oauth(instance_id="P01")
    assert calls == ["https://auth.crosswatch.app/mal/start"]
    assert mal.status_for_block({})["login_available"]


def test_start_keeps_poll_secret_out_of_browser_and_config(store, monkeypatch):
    before = copy.deepcopy(store)
    result = start(monkeypatch)
    assert result["ok"]
    assert "poll_token" not in result and "session_id" not in result
    assert result["flow_id"] != "oauth-state"
    assert store == before
    assert mal._PENDING["P01"]["poll_token"] == "p" * 43


def test_stale_cancel_and_other_profile_cannot_use_flow(store, monkeypatch):
    result = start(monkeypatch)
    mal.cancel_oauth(flow_id="stale", instance_id="P01")
    mal.cancel_oauth(flow_id="é", instance_id="P01")
    assert mal.poll_oauth(flow_id="é", instance_id="P01")["error"] == "invalid_flow"
    assert "P01" in mal._PENDING
    assert mal.poll_oauth(flow_id=result["flow_id"], instance_id="P02")["error"] == "invalid_flow"
    assert mal.poll_oauth(flow_id="stale", instance_id="P01")["error"] == "invalid_flow"


def test_poll_verifies_identity_saves_only_selected_profile(store, monkeypatch):
    result = start(monkeypatch)
    calls = []
    def post(url, **kwargs):
        calls.append((url, kwargs))
        return Response(payload=tokens())
    monkeypatch.setattr(mal.requests, "post", post)
    monkeypatch.setattr(mal.requests, "get", lambda *a, **k: Response(payload={"id": 42, "name": "animefan"}))
    response = mal.poll_oauth(flow_id=result["flow_id"], instance_id="P01")
    assert response["status"] == "authorized"
    assert "access_token" not in response and "refresh_token" not in response
    assert block(store)["username"] == "animefan"
    assert block(store)["expires_at"] == 4600
    assert store["cfg"]["myanimelist"]["instances"]["P02"] == {"access_token": "other"}
    assert not store["cfg"]["myanimelist"].get("access_token")
    assert calls[0][1]["headers"]["Authorization"] == "Bearer " + "p" * 43
    assert calls[0][1]["allow_redirects"] is False
    assert "P01" not in mal._PENDING


def test_identity_retry_does_not_redeem_tokens_again(store, monkeypatch):
    result = start(monkeypatch)
    calls = []
    monkeypatch.setattr(mal.requests, "post", lambda *a, **k: calls.append(a) or Response(payload=tokens()))
    monkeypatch.setattr(mal.requests, "get", lambda *a, **k: Response(503))
    assert mal.poll_oauth(flow_id=result["flow_id"], instance_id="P01")["retryable"]
    assert block(store) == {}
    monkeypatch.setattr(mal.time, "time", lambda: 1031)
    monkeypatch.setattr(mal.requests, "get", lambda *a, **k: Response(payload={"id": 42, "name": "animefan"}))
    assert mal.poll_oauth(flow_id=result["flow_id"], instance_id="P01")["ok"]
    assert len(calls) == 1
    assert block(store)["expires_at"] == 4600


@pytest.mark.parametrize("account", [{}, {"id": 42}, {"id": 0, "name": "bad"}, {"id": 42, "name": ""}])
def test_malformed_identity_cannot_connect(store, monkeypatch, account):
    result = start(monkeypatch)
    monkeypatch.setattr(mal.requests, "post", lambda *a, **k: Response(payload=tokens()))
    monkeypatch.setattr(mal.requests, "get", lambda *a, **k: Response(payload=account))
    assert mal.poll_oauth(flow_id=result["flow_id"], instance_id="P01")["error"] == "invalid_account"
    assert block(store) == {}


def test_pending_poll_throttled_and_expired(store, monkeypatch):
    result = start(monkeypatch)
    calls = []
    monkeypatch.setattr(mal.requests, "post", lambda *a, **k: calls.append(a) or Response(payload={"ok": True, "status": "pending"}))
    for _ in range(3):
        assert mal.poll_oauth(flow_id=result["flow_id"], instance_id="P01")["status"] == "pending"
    assert len(calls) == 1
    monkeypatch.setattr(mal.time, "time", lambda: 1601)
    assert mal.poll_oauth(flow_id=result["flow_id"], instance_id="P01")["error"] == "expired_flow"


def test_refresh_is_single_flight(store, monkeypatch):
    connected(store)
    calls = []
    monkeypatch.setattr(mal.requests, "post", lambda *a, **k: calls.append((a, k)) or Response(payload=tokens()))
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: mal.refresh_token(instance_id="P01", force=True, rejected_token="old-access"), range(4)))
    assert all(r["ok"] for r in results)
    assert len(calls) == 1
    assert block(store)["refresh_token"] == "new-refresh"
    assert "other" not in json.dumps(calls)


def test_foreign_broker_credentials_are_not_forwarded(store, monkeypatch):
    connected(store)
    block(store)["auth_broker"] = "https://different.example.com"
    assert mal.refresh_token(instance_id="P01")["error"] == "broker_unavailable"
    assert block(store)["refresh_token"] == "old-refresh"


def test_terminal_refresh_clears_only_selected_profile(store, monkeypatch):
    connected(store)
    monkeypatch.setattr(mal.requests, "post", lambda *a, **k: Response(400, {"error": "invalid_grant"}))
    assert not mal.refresh_token(instance_id="P01")["ok"]
    assert block(store)["access_token"] == ""
    assert block(store)["refresh_token"] == ""
    assert block(store)["reauth_required"] is True
    assert store["cfg"]["myanimelist"]["instances"]["P02"]["access_token"] == "other"


def test_refresh_rate_limit_preserves_credentials_and_backs_off(store, monkeypatch):
    connected(store)
    calls = []
    monkeypatch.setattr(mal.requests, "post", lambda *a, **k: calls.append(a) or Response(429, {}, {"Retry-After": "120"}))
    assert not mal.refresh_token(instance_id="P01")["ok"]
    assert not mal.refresh_token(instance_id="P01")["ok"]
    assert len(calls) == 1
    assert block(store)["access_token"] == "old-access"
    assert mal._REFRESH_RETRY_AT["P01"] == 1120


def test_request_refreshes_on_401_only_once(store, monkeypatch):
    connected(store, expires=5000)
    monkeypatch.setattr(mal.requests, "post", lambda *a, **k: Response(payload=tokens()))
    seen = []
    def request(*args, **kwargs):
        seen.append(dict(kwargs["headers"]))
        assert kwargs["allow_redirects"] is False
        return Response(401)
    response = mal.request_with_auth(None, "GET", mal.ME_URL, cfg=store["cfg"], instance_id="P01", request_func=request)
    assert response.status_code == 401
    assert [h["Authorization"] for h in seen] == ["Bearer old-access", "Bearer new-access"]
    assert block(store)["reauth_required"] is True


@pytest.mark.parametrize("url", ["http://api.myanimelist.net/v2/users/@me", "https://evil.example/v2/users/@me", "https://api.myanimelist.net.evil.example/v2/users/@me"])
def test_api_host_is_pinned(store, url):
    with pytest.raises(mal.MyAnimeListAuthError, match="invalid_api_url"):
        mal.request_with_auth(None, "GET", url, cfg=store["cfg"], instance_id="P01")


def test_profile_resolver_and_redaction(store):
    from cw_platform.config_base import DEFAULT_CFG, redact_config
    from cw_platform.provider_instances import build_provider_config_view
    connected(store)
    view = build_provider_config_view(store["cfg"], "myanimelist", "P01")
    assert mal.provider_block(view, "P01")["access_token"] == "old-access"
    assert not mal.provider_block(store["cfg"], "missing").get("access_token")
    redacted = redact_config(store["cfg"])
    assert "old-access" not in json.dumps(redacted)
    assert "old-refresh" not in json.dumps(redacted)
    assert not mal.status_for_block(DEFAULT_CFG["myanimelist"])["connected"]


def test_registration_routes_and_probe(store, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import authenticationAPI as api
    from api import probesAPI as probes
    from cw_platform.modules_registry import MODULES, sync_provider_names
    from providers.auth.registry import auth_providers_manifests
    from providers.auth.runtime import _backend
    assert _backend("myanimelist") is mal
    assert "_auth_MYANIMELIST" in MODULES["AUTH"]
    assert "MYANIMELIST" in sync_provider_names()
    assert any(m["name"] == "MYANIMELIST" for m in auth_providers_manifests())
    monkeypatch.setattr(api, "load_config", lambda: copy.deepcopy(store["cfg"]))
    app = FastAPI()
    api.register_auth(app)
    client = TestClient(app)
    assert client.get("/api/myanimelist/status?instance=P01").json()["connected"] is False
    connected(store, expires=5000)
    monkeypatch.setattr(probes, "_authenticated_account", lambda *a, **k: (200, json.dumps({"id": 42, "name": "animefan"})))
    assert probes._probe_myanimelist_detail.__wrapped__({"myanimelist": block(store)}, max_age_sec=0) == (True, "")
    assert "old-access" not in probes._probe_key("myanimelist", {"myanimelist": block(store)})
    assert client.post("/api/myanimelist/disconnect?instance=P01").json()["ok"]
    assert block(store)["access_token"] == ""


def test_empty_profile_never_inherits_default_credentials(store):
    from cw_platform.provider_instances import build_provider_config_view
    store["cfg"]["myanimelist"].update(access_token="default-access", refresh_token="default-refresh", username="default-user")
    for cfg in (store["cfg"], build_provider_config_view(store["cfg"], "myanimelist", "P01")):
        assert mal.provider_block(cfg, "P01") == {}
        assert not mal.PROVIDER.get_status(cfg, instance_id="P01").connected
    assert mal.provider_block(store["cfg"], "default")["access_token"] == "default-access"


@pytest.mark.parametrize("exception,reason", [
    (mal.requests.exceptions.SSLError("private-secret"), "network_tls"),
    (mal.requests.exceptions.ProxyError("private-secret"), "network_proxy"),
    (mal.requests.exceptions.Timeout("private-secret"), "network_timeout"),
    (mal.requests.exceptions.ConnectionError(mal.socket.gaierror(-2, "private-secret")), "network_dns"),
    (mal.requests.exceptions.ConnectionError("private-secret"), "network_connection"),
])
def test_network_failure_is_diagnostic_without_leaking_exception_details(store, monkeypatch, exception, reason):
    logs = []
    monkeypatch.setattr(mal, "_app_log", lambda message, **kwargs: logs.append(message))
    def fail(*args, **kwargs):
        raise exception
    monkeypatch.setattr(mal.requests, "post", fail)
    result = mal.start_oauth(instance_id="P01")
    assert result["error"] == "network_error"
    assert result["reason"] == reason
    assert reason in logs[0]
    assert "auth.crosswatch.app" in logs[0]
    assert "private-secret" not in json.dumps([logs, result])


def test_broker_http_failure_logs_status_without_response_credentials(store, monkeypatch):
    logs = []
    monkeypatch.setattr(mal, "_app_log", lambda message, **kwargs: logs.append(message))
    monkeypatch.setattr(mal.requests, "post", lambda *args, **kwargs: Response(403, {"error": "private-secret", "access_token": "private-token"}))
    result = mal.start_oauth(instance_id="P01")
    assert result["error"] == "broker_error"
    assert "HTTP 403" in logs[0]
    assert "private" not in json.dumps([logs, result])


@pytest.mark.parametrize("action", ["start", "poll", "cancel", "refresh"])
def test_broker_requests_include_crosswatch_marker(store, monkeypatch, action):
    calls = []
    monkeypatch.setattr(mal.requests, "post", lambda url, **kwargs: calls.append(kwargs) or Response(payload={"ok": True}))
    mal._post(mal.BROKER_URL, action, {}, credential="poll-secret")
    assert calls[0]["headers"]["X-CrossWatch-Client"] == "CrossWatch"
    assert calls[0]["headers"]["Authorization"] == "Bearer poll-secret"
    assert "X-CrossWatch-Client" not in mal._headers("mal-token")
