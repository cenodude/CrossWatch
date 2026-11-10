# tests/test_mdblist_quota.py
# CrossWatch - MDBList quota capture and cached status regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
from types import SimpleNamespace

import pytest
import requests

from providers.sync.mdblist import _auth as auth


@pytest.fixture(autouse=True)
def isolated(monkeypatch, config_base):
    monkeypatch.setattr(auth, "_QUOTA_SEEN", {})

    def unexpected(*args, **kwargs):
        raise AssertionError("Unexpected network request")

    monkeypatch.setattr(requests.sessions.Session, "request", unexpected)


@pytest.mark.parametrize("block", [{"api_key": "key"}, {"access_token": "token", "expires_at": 9999999999}])
@pytest.mark.parametrize("status,remaining", [(200, "42"), (429, "0")])
def test_requests_capture_quota_without_extra_calls(block, status, remaining):
    calls = []
    response = SimpleNamespace(status_code=status, headers={
        "X-RateLimit-Limit": "1000", "X-RateLimit-Remaining": remaining, "X-RateLimit-Reset": "2000000000",
    })

    def send(*args, **kwargs):
        calls.append((args, kwargs))
        return response

    cfg = {"mdblist": {"api_key": "default", "instances": {"P01": block}}}
    result = auth.request_with_auth(None, "POST", f"{auth.API_BASE}/sync/history", cfg=cfg, instance_id="P01", request_func=send)
    assert result is response
    assert len(calls) == 1
    quota = auth.latest_quota(block)
    assert quota["daily_remaining"] == int(remaining)
    assert quota["daily_limit"] == 1000
    assert quota["daily_resets_at"] == 2000000000
    assert auth.latest_quota(cfg["mdblist"]) == {}


def test_quota_preserves_newest_valid_observation(monkeypatch):
    block = {"api_key": "key"}
    monkeypatch.setattr(auth, "time", SimpleNamespace(time=lambda: 1000))
    auth.record_quota(block, {"x-ratelimit-remaining": "12", "x-ratelimit-reset": "1060"}, started_at=10)
    auth.record_quota(block, {"x-ratelimit-remaining": "15"}, started_at=9)
    for headers in [{}, {"X-RateLimit-Remaining": "invalid"}, {"X-RateLimit-Remaining": "-1"}]:
        auth.record_quota(block, headers, started_at=11)
    assert auth.latest_quota(block) == {
        "daily_remaining": 12, "daily_limit": None, "daily_resets_at": 1060, "daily_seen_at": 1000,
    }
    assert auth.latest_quota({"api_key": "other"}) == {}
    auth.record_quota(block, {"x-ratelimit-remaining": "0", "x-ratelimit-limit": "bad", "x-ratelimit-reset": "-1"}, started_at=12)
    assert auth.latest_quota(block)["daily_remaining"] == 0
    assert auth.latest_quota(block)["daily_resets_at"] is None


def test_auth_retry_records_quota_for_the_rotated_token(monkeypatch):
    old = {"access_token": "old", "expires_at": 9999999999}
    new = {"access_token": "new", "expires_at": 9999999999}
    monkeypatch.setattr(auth, "refresh_token", lambda *a, **k: {"ok": True})
    monkeypatch.setattr(auth, "_load_full_cfg", lambda: {"mdblist": new})
    calls = []

    def send(*args, **kwargs):
        token = kwargs["headers"]["Authorization"]
        calls.append(token)
        return SimpleNamespace(status_code=401 if token == "Bearer old" else 200, headers={"X-RateLimit-Remaining": "42"})

    result = auth.request_with_auth(None, "GET", f"{auth.API_BASE}/user", cfg={"mdblist": old}, request_func=send)
    assert result.status_code == 200
    assert calls == ["Bearer old", "Bearer new"]
    assert auth.latest_quota(new)["daily_remaining"] == 42


@pytest.mark.parametrize("body_quota", [True, False])
def test_normal_activity_updates_cached_status_without_reprobing(monkeypatch, body_quota):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import probesAPI as probes
    from providers.sync import _mod_common

    cfg = {"mdblist": {"api_key": "status-key"}, "pairs": []}
    clock = [time.time()]
    monkeypatch.setattr(auth, "time", SimpleNamespace(time=lambda: clock[0], monotonic=lambda: clock[0]))
    probes.invalidate_provider_caches("mdblist")
    calls = []
    remaining = [100]

    def send(session, method, url, **kwargs):
        calls.append(url)
        response = requests.Response()
        response.status_code = 200
        response.headers.update({"X-RateLimit-Remaining": str(remaining[0]), "X-RateLimit-Limit": "1000",
                                 "X-RateLimit-Reset": str(int(clock[0]) + 3600)})
        payload = {"user_id": 12, "username": "tester"}
        if body_quota:
            payload.update(rate_limit_remaining=remaining[0], rate_limit=1000, rate_limit_reset=int(clock[0]) + 3600)
        response._content = json.dumps(payload).encode()
        return response

    monkeypatch.setattr(_mod_common, "request_with_retries", send)
    app = FastAPI()
    probes.register_probes(app, lambda: cfg)
    client = TestClient(app)
    first = client.get("/api/status?fresh=1").json()["providers"]["MDBLIST"]
    assert first["daily_remaining"] == 100
    assert calls == [f"{auth.API_BASE}/user"]
    assert client.get("/api/status").json()["providers"]["MDBLIST"]["daily_remaining"] == 100
    assert len(calls) == 1
    clock[0] += 10
    remaining[0] = 99
    auth.request_with_auth(None, "GET", f"{auth.API_BASE}/sync/last_activities", cfg=cfg)
    updated = client.get("/api/status").json()["providers"]["MDBLIST"]
    assert updated["daily_remaining"] == 99
    assert updated["username"] == "tester"
    assert updated["connected"] is True
    assert calls == [f"{auth.API_BASE}/user", f"{auth.API_BASE}/sync/last_activities"]
    newer = {**probes.mdblist_user_info(cfg), "daily_remaining": 98, "daily_seen_at": clock[0] + 1}
    probes.connection_status.update("mdblist", cfg, userinfo=newer)
    assert client.get("/api/status").json()["providers"]["MDBLIST"]["daily_remaining"] == 98
    assert len(calls) == 2
