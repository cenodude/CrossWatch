# tests/test_status_mapping_transport.py
# CrossWatch - Status mutations must not retry uncertain transport outcomes.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import importlib
from urllib.parse import urlsplit

import pytest
import requests

from cw_platform.status_mapping import Observation, ReadState, Status
from providers.sync._status_mapping import ADAPTERS, Record, adapter_class


@pytest.mark.parametrize("provider", [name for name in ADAPTERS if name != "crosswatch"])
@pytest.mark.parametrize("failure", ["timeout", 500, 503])
def test_status_mutation_has_one_transport_attempt(monkeypatch, config_base, provider, failure):
    block = {"access_token": "test-token", "api_key": "test-key", "api_token": "test-key", "client_id": "test-client",
             "base_url": "https://example.invalid", "server_url": "https://example.invalid", "url": "https://example.invalid",
             "expires_at": 4102444800, "token_expires_at": 4102444800, "max_retries": 3, "timeout": 1,
             "auth_method": "api_key", "credential": "key", "user_id": "1", "user": {"id": "1"},
             "username": "test-user", "password": "test-password"}
    cfg = {provider: block}
    auth = importlib.import_module(f"providers.auth._auth_{provider.upper()}")
    if hasattr(auth, "_load_full_cfg"):
        monkeypatch.setattr(auth, "_load_full_cfg", lambda: cfg)
    if hasattr(auth, "load_config"):
        monkeypatch.setattr(auth, "load_config", lambda: cfg)
    calls = []
    def send(self, request, **kwargs):
        response = requests.Response()
        response.request = request
        response.url = request.url
        if request.method == "GET" and urlsplit(request.url).path.endswith("auth/settings"):
            response.status_code = 200
            response._content = json.dumps({name + "_push_dropped": False for name in ("trakt", "simkl", "mdblist")}).encode()
            return response
        assert request.method in {"POST", "PATCH", "PUT", "DELETE"}
        calls.append((request.method, request.url))
        if failure == "timeout":
            raise requests.Timeout("lost response")
        response.status_code = failure
        response._content = b'{}'
        return response
    monkeypatch.setattr(requests.Session, "send", send)
    cls = adapter_class(provider)
    monkeypatch.setattr(cls, "blockers", lambda *args: ())
    adapter = cls(cfg)
    raw = {"entry": {"id": "1", "status": "watching"}, "consumption_id": "1", "id": 1, "bucket": "shows"}
    record = Record("shows:1" if provider == "simkl" else "1", "shows", {"type": "show", "ids": {"tmdb": "1", "trakt": "1", "simkl": "1"}},
                    Observation(ReadState.PRESENT, Status.WATCHING, "watching", "protected", False, False), raw)
    with pytest.raises(Exception) as error:
        adapter.write(record, Status.DROPPED)
    assert len(calls) == 1, str(error.value)
