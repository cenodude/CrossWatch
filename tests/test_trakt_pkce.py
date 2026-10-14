# tests/test_trakt_pkce.py
# CrossWatch - Trakt PKCE authorization and token persistence regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import base64
from copy import deepcopy
import hashlib
from unittest.mock import Mock
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import authenticationAPI
from providers.auth import _auth_TRAKT as auth


@pytest.fixture
def oauth(monkeypatch):
    store = {"trakt": {"client_id": "own", "client_secret": "legacy", "access_token": "old", "instances": {
        "second": {"client_id": "second-client", "access_token": "second-old", "label": "Second"},
    }}}
    monkeypatch.setattr(auth, "_OAUTH_PENDING", {})
    monkeypatch.setattr(auth, "_load_config", lambda: deepcopy(store))

    def save(cfg):
        store.clear()
        store.update(deepcopy(cfg))
        return True

    monkeypatch.setattr(auth, "_save_config", save)
    post = Mock(return_value=Mock(status_code=200, json=lambda: {
        "access_token": "new", "refresh_token": "refresh", "expires_in": 3600,
    }))
    monkeypatch.setattr(auth.requests, "post", post)
    return store, post


@pytest.mark.parametrize("instance", ["default", "second"])
def test_pkce_oob_exchange_persists_tokens_and_refreshes_without_secret(oauth, instance):
    store, post = oauth
    before = deepcopy(store)
    started = auth.start_oauth(instance_id=instance)
    pending = auth._OAUTH_PENDING[instance]
    verifier = pending["verifier"]
    params = parse_qs(urlsplit(started["authorize_url"]).query)
    assert 43 <= len(verifier) <= 128
    assert params["code_challenge"] == [base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()]
    assert params["code_challenge_method"] == ["S256"]
    assert params["response_type"] == ["code"]
    assert params["redirect_uri"] == ["urn:ietf:wg:oauth:2.0:oob"]
    assert params["state"] == [started["flow_id"]]
    assert verifier not in str(started)
    assert store == before
    assert auth.finish_oauth(code=" code ", flow_id=started["flow_id"], instance_id=instance) == {"ok": True}
    block = store["trakt"] if instance == "default" else store["trakt"]["instances"][instance]
    assert post.call_args.args == (auth.OAUTH_TOKEN,)
    assert post.call_args.kwargs["json"] == {
        "code": "code", "client_id": block["client_id"], "redirect_uri": auth.OAUTH_REDIRECT_URI,
        "grant_type": "authorization_code", "code_verifier": verifier,
    }
    assert post.call_args.kwargs["allow_redirects"] is False
    assert block["access_token"] == "new"
    assert block["refresh_token"] == "refresh"
    assert block["auth_method"] == "app"
    assert block["auth_flow"] == "pkce"
    assert block["expires_at"] > auth._now()
    assert block["auth_completed_at"]
    assert verifier not in str(store)
    assert instance not in auth._OAUTH_PENDING
    if instance == "second":
        assert store["trakt"]["access_token"] == "old"
        assert block["label"] == "Second"
    else:
        assert block["client_secret"] == "legacy"
        assert block["instances"] == before["trakt"]["instances"]
    block["expires_at"] = 1
    assert auth.PROVIDER.refresh(instance_id=instance)["ok"]
    assert "client_secret" not in post.call_args.kwargs["json"]
    post.reset_mock()
    assert auth.finish_oauth(code="code", flow_id=started["flow_id"], instance_id=instance)["error"] == "invalid_flow"
    post.assert_not_called()


def test_pkce_rejects_wrong_profile_stale_cancelled_and_expired_flows(oauth, monkeypatch):
    store, post = oauth
    first = auth.start_oauth()
    second = auth.start_oauth()
    assert first["flow_id"] != second["flow_id"]
    assert auth.finish_oauth(code="code", flow_id=first["flow_id"])["error"] == "invalid_flow"
    assert auth.finish_oauth(code="code", flow_id=second["flow_id"], instance_id="second")["error"] == "invalid_flow"
    auth.cancel_oauth(flow_id=first["flow_id"])
    assert auth._OAUTH_PENDING["default"]["flow_id"] == second["flow_id"]
    expires = auth._OAUTH_PENDING["default"]["expires_at"]
    monkeypatch.setattr(auth, "_now", lambda: expires)
    assert auth.finish_oauth(code="code", flow_id=second["flow_id"])["error"] == "expired_flow"
    third = auth.start_oauth()
    auth.cancel_oauth(flow_id=third["flow_id"])
    assert auth.finish_oauth(code="code", flow_id=third["flow_id"])["error"] == "invalid_flow"
    post.assert_not_called()
    assert store["trakt"]["access_token"] == "old"


@pytest.mark.parametrize("failure", ["http", "json", "missing_token", "network", "empty_code"])
def test_failed_pkce_exchange_preserves_existing_connection(oauth, failure):
    store, post = oauth
    before = deepcopy(store)
    started = auth.start_oauth()
    if failure == "http":
        post.return_value.status_code = 400
    elif failure == "json":
        post.return_value.json = Mock(side_effect=ValueError("bad JSON"))
    elif failure == "missing_token":
        post.return_value.json = lambda: {"refresh_token": "refresh"}
    elif failure == "network":
        post.side_effect = auth.requests.RequestException("private details")
    result = auth.finish_oauth(code="" if failure == "empty_code" else "code", flow_id=started["flow_id"])
    assert result["ok"] is False
    assert "private details" not in str(result)
    assert store == before


def test_pkce_save_failure_can_retry_without_reusing_authorization_code(oauth, monkeypatch):
    store, post = oauth
    save = auth._save_config
    started = auth.start_oauth(client_id="replacement")
    monkeypatch.setattr(auth, "_save_config", lambda cfg: False)
    assert auth.finish_oauth(code="code", flow_id=started["flow_id"])["error"] == "save_failed"
    assert store["trakt"]["access_token"] == "old"
    monkeypatch.setattr(auth, "_save_config", save)
    assert auth.finish_oauth(code="code", flow_id=started["flow_id"])["ok"]
    post.assert_called_once()
    assert store["trakt"]["client_id"] == "replacement"
    assert store["trakt"]["client_secret"] == ""


def test_pkce_api_uses_saved_client_id_and_returns_no_tokens(oauth, monkeypatch):
    store, post = oauth
    from api import probesAPI

    monkeypatch.setattr(probesAPI, "invalidate_provider_caches", lambda *a: None)
    app = FastAPI()
    authenticationAPI.register_auth(app)
    client = TestClient(app)
    started = client.post("/api/trakt/oauth/start?instance=second", json={"client_id": "********"}).json()
    assert parse_qs(urlsplit(started["authorize_url"]).query)["client_id"] == ["second-client"]
    result = client.post("/api/trakt/oauth/finish?instance=second", json={"flow_id": started["flow_id"], "code": "code"})
    assert result.json() == {"ok": True}
    assert store["trakt"]["instances"]["second"]["access_token"] == "new"
    started = client.post("/api/trakt/oauth/start?instance=second", json={}).json()
    assert client.post("/api/trakt/oauth/cancel?instance=second", json={"flow_id": started["flow_id"]}).json()["ok"]
    assert "second" not in auth._OAUTH_PENDING


def test_pkce_tokens_survive_config_save_and_reload(config_base, monkeypatch):
    monkeypatch.setattr(auth, "_OAUTH_PENDING", {})
    cfg = auth.load_config()
    cfg["trakt"]["client_id"] = "own"
    auth.save_config(cfg)
    monkeypatch.setattr(auth.requests, "post", Mock(return_value=Mock(status_code=200, json=lambda: {
        "access_token": "persisted-access", "refresh_token": "persisted-refresh", "expires_in": 3600,
    })))
    flow = auth.start_oauth()
    assert auth.finish_oauth(code="code", flow_id=flow["flow_id"])["ok"]
    from cw_platform import config_base as config

    config.invalidate_config_cache()
    stored = auth.load_config()["trakt"]
    assert stored["access_token"] == "persisted-access"
    assert stored["refresh_token"] == "persisted-refresh"
    assert stored["auth_flow"] == "pkce"
    assert stored["auth_completed_at"]
