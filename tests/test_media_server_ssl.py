# /tests/test_media_server_ssl.py
# CrossWatch - Emby and Jellyfin SSL checkbox regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from urllib.parse import urlparse

import pytest
import requests

from providers.auth._auth_EMBY import EmbyAuth
from providers.auth._auth_JELLYFIN import JellyfinAuth
from providers.sync._mod_EMBY import EMBYClient, EMBYConfig
from providers.sync._mod_JELLYFIN import JFClient, JFConfig
from providers.sync.jellyfin._auth_http import JellyfinAuthSession


@pytest.fixture(params=[None, "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE"])
def transport(request, monkeypatch):
    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
    monkeypatch.delenv("CURL_CA_BUNDLE", raising=False)
    bundle = "test-ca.pem" if request.param else None
    if request.param:
        monkeypatch.setenv(request.param, bundle)
    calls = []

    def send(session, prepared, **kwargs):
        path = urlparse(prepared.url).path
        calls.append((prepared.method, path, kwargs["verify"]))
        body = {
            "AccessToken": "test-token", "User": {"Id": "test-user", "Name": "Test"},
            "Id": "test-user", "Name": "Test", "Version": "10.10.0",
            "Secret": "test-secret", "Code": "123456", "Authenticated": True,
        }
        response = requests.Response()
        response.status_code = 200
        response.url = prepared.url
        response.request = prepared
        response.headers["Content-Type"] = "application/json"
        response._content = json.dumps(True if path.endswith("QuickConnect/Enabled") else body).encode()
        return response

    monkeypatch.setattr(requests.Session, "send", send)
    monkeypatch.setattr("providers.auth._auth_EMBY.log", lambda *args, **kwargs: None)
    monkeypatch.setattr("providers.auth._auth_JELLYFIN.log", lambda *args, **kwargs: None)
    return calls, bundle


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("provider,auth,count", [("emby", EmbyAuth, 2), ("jellyfin", JellyfinAuth, 3)])
@pytest.mark.parametrize("instance", ["default", "alternate"])
def test_login_uses_selected_instance_ssl_setting(transport, enabled, provider, auth, count, instance):
    calls, bundle = transport
    block = {"server": "https://media.test", "username": "Test", "password": "test", "verify_ssl": enabled}
    cfg = {provider: block if instance == "default" else {
        "verify_ssl": not enabled, "instances": {instance: block},
    }}
    result = auth().start(cfg, instance_id=instance)
    assert result["ok"]
    assert len(calls) == count
    assert all(verify == (bundle or True if enabled else False) for _, _, verify in calls)


@pytest.mark.parametrize("enabled", [False, True])
def test_jellyfin_quick_connect_uses_ssl_setting(transport, enabled):
    calls, bundle = transport
    with JellyfinAuthSession("https://media.test", verify_ssl=enabled) as session:
        assert session.quick_connect_enabled()
        session.quick_connect_initiate()
        session.quick_connect_state("test-secret")
        session.authenticate_with_quick_connect("test-secret")
    assert len(calls) == 4
    assert all(verify == (bundle or True if enabled else False) for _, _, verify in calls)


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("client,config", [(EMBYClient, EMBYConfig), (JFClient, JFConfig)])
def test_sync_reads_and_writes_use_ssl_setting(transport, enabled, client, config):
    calls, bundle = transport
    http = client(config(server="https://media.test", access_token="test", user_id="test", verify_ssl=enabled))
    try:
        http.get("/Items")
        http.post("/Items")
        http.delete("/Items")
    finally:
        http.session.close()
    assert [method for method, _, _ in calls] == ["GET", "POST", "DELETE"]
    assert all(verify == (bundle or True if enabled else False) for _, _, verify in calls)
