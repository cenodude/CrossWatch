# /tests/test_plex_sync_ssl.py
# CrossWatch - Plex sync SSL verification regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import pytest
import requests

from providers.sync import _mod_PLEX as plex


@pytest.mark.parametrize(
    ("settings", "env", "expected"),
    [
        ({"plex": {"verify_ssl": False}}, None, False),
        ({"plex": {"verify_ssl": True}}, None, True),
        ({"plex": {"verify_ssl": "false"}}, None, False),
        ({"plex": {"verify_ssl": "true"}}, None, True),
        ({"plex": {}}, None, True),
        ({"plex": {}, "verify_ssl": False}, None, False),
        ({"plex": {"verify_ssl": False}}, "true", True),
        ({"plex": {"verify_ssl": True}}, "false", False),
    ],
)
def test_ssl_setting_applies_to_initial_and_subsequent_pms_requests(monkeypatch, settings, env, expected):
    monkeypatch.delenv("CW_PLEX_VERIFY", raising=False)
    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
    monkeypatch.delenv("CURL_CA_BUNDLE", raising=False)
    if env is not None:
        monkeypatch.setenv("CW_PLEX_VERIFY", env)
    monkeypatch.setattr(plex, "configure_plex_context", lambda **kwargs: None)
    monkeypatch.setattr(plex.PLEXClient, "_post_connect_user_scope", lambda *args: None)
    calls = []

    def send(session, request, **kwargs):
        calls.append((request.url, kwargs["verify"]))
        response = requests.Response()
        response.status_code = 200
        response._content = b'<MediaContainer machineIdentifier="test-server"/>'
        response.url = request.url
        response.request = request
        return response

    monkeypatch.setattr(requests.Session, "send", send)
    settings["plex"].update(server_url="https://plex.test:32400", pms_token="test-token")

    module = plex.PLEXModule(settings)

    assert module.client.server is not None
    module.client.server.query("/identity")
    assert calls == [
        ("https://plex.test:32400/", expected),
        ("https://plex.test:32400/identity", expected),
    ]


def test_changing_ssl_setting_invalidates_adapter_cache(monkeypatch):
    monkeypatch.delenv("CW_PLEX_VERIFY", raising=False)
    cfg = {"plex": {"server_url": "https://plex.test:32400", "verify_ssl": True}}
    verified = plex._adapter_identity(cfg)

    cfg["plex"]["verify_ssl"] = False

    assert plex._adapter_identity(cfg) != verified


def test_changing_ssl_environment_override_invalidates_adapter_cache(monkeypatch):
    cfg = {"plex": {"server_url": "https://plex.test:32400", "verify_ssl": True}}
    monkeypatch.setenv("CW_PLEX_VERIFY", "true")
    verified = plex._adapter_identity(cfg)

    monkeypatch.setenv("CW_PLEX_VERIFY", "false")

    assert plex._adapter_identity(cfg) != verified
