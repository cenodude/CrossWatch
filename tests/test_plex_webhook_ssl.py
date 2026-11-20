# /tests/test_plex_webhook_ssl.py
# CrossWatch - Plex webhook SSL verification regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import pytest
import requests

from providers.webhooks import plex


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
@pytest.mark.parametrize("lookup", ["show_ids", "progress", "played"])
def test_plex_webhook_lookups_respect_ssl_setting(monkeypatch, settings, env, expected, lookup):
    monkeypatch.delenv("CW_PLEX_VERIFY", raising=False)
    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
    monkeypatch.delenv("CURL_CA_BUNDLE", raising=False)
    if env is not None:
        monkeypatch.setenv("CW_PLEX_VERIFY", env)
    settings["plex"].update(server_url="https://plex.test:32400", account_token="test-token")
    calls = []

    def send(session, request, **kwargs):
        calls.append((request.url, kwargs["verify"], request.headers["X-Plex-Token"]))
        response = requests.Response()
        response.status_code = 200
        response._content = (
            b'<MediaContainer><Video ratingKey="48970" duration="1000" viewOffset="500" viewCount="1">'
            b'<Guid id="tmdb://123"/></Video></MediaContainer>'
        )
        response.url = request.url
        response.request = request
        return response

    monkeypatch.setattr(requests.Session, "send", send)

    if lookup == "show_ids":
        assert plex._plex_show_ids_from_metadata(settings, {"grandparentRatingKey": "48970"}) == {"tmdb": 123}
    elif lookup == "progress":
        assert plex._probe_active_progress(settings, "48970") == (50, 1000)
    else:
        assert plex._probe_played_status(settings, "48970") is True

    path = "/status/sessions" if lookup == "progress" else "/library/metadata/48970"
    assert calls == [(f"https://plex.test:32400{path}", expected, "test-token")]


def test_show_id_lookup_keeps_ssl_verification_when_trying_parent(monkeypatch):
    monkeypatch.delenv("CW_PLEX_VERIFY", raising=False)
    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
    monkeypatch.delenv("CURL_CA_BUNDLE", raising=False)
    cfg = {"plex": {"server_url": "https://plex.test:32400", "account_token": "test-token", "verify_ssl": True}}
    calls = []

    def send(session, request, **kwargs):
        calls.append((request.url, kwargs["verify"]))
        raise requests.exceptions.SSLError("certificate verify failed")

    monkeypatch.setattr(requests.Session, "send", send)

    assert plex._plex_show_ids_from_metadata(cfg, {"grandparentRatingKey": "48970", "parentRatingKey": "48971"}) == {}
    assert calls == [
        ("https://plex.test:32400/library/metadata/48970", True),
        ("https://plex.test:32400/library/metadata/48971", True),
    ]
