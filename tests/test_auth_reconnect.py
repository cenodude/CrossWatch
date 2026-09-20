# /tests/test_auth_reconnect.py
# CrossWatch - OAuth reconnect completion and preserved connection settings
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import authenticationAPI as auth
from cw_platform.provider_instances import ensure_instance_block


@pytest.fixture(autouse=True)
def isolate_probe_invalidation(monkeypatch):
    from api import probesAPI

    monkeypatch.setattr(probesAPI, "invalidate_provider_caches", lambda *args: None)


@pytest.mark.parametrize("provider", ["simkl", "anilist"])
@pytest.mark.parametrize("instance", ["default", "alternate"])
@pytest.mark.parametrize("succeeds", [False, True])
def test_oauth_reconnect_preserves_profile_and_only_marks_success(monkeypatch, provider, instance, succeeds):
    block = {
        "access_token": "old-token", "client_id": "client", "client_secret": "secret",
        "label": "My account", "history": {"libraries": ["one"]}, "auth_completed_at": "before",
    }
    store = {
        provider: deepcopy(block) if instance == "default" else {"access_token": "other-token", "instances": {instance: deepcopy(block)}},
        "pairs": [{"source": provider.upper(), "source_instance": instance, "target": "PLEX"}],
    }
    before = deepcopy(store)
    monkeypatch.setattr(auth, "load_config", lambda: deepcopy(store))

    def save(cfg):
        store.clear()
        store.update(deepcopy(cfg))

    monkeypatch.setattr(auth, "save_config", save)

    def exchange_simkl(cfg, inst, *args):
        if not succeeds:
            return None
        ensure_instance_block(cfg, "simkl", inst)["access_token"] = "new-token"
        return {"access_token": "new-token"}

    monkeypatch.setattr(auth, "simkl_exchange_code", exchange_simkl)
    monkeypatch.setattr(auth, "anilist_exchange_code_for_token", lambda **kwargs: {"access_token": "new-token"} if succeeds else None)
    state = {
        "test-state": {"instance": instance, "created_at": int(time.time()), "redirect_uri": f"http://testserver/callback/{provider}", "code_verifier": "verifier"},
    }
    monkeypatch.setattr(auth, "SIMKL_STATE" if provider == "simkl" else "ANILIST_STATE", state)
    app = FastAPI()
    auth.register_auth(app)
    callback = "/callback" if provider == "simkl" else "/callback/anilist"
    response = TestClient(app).get(callback, params={"state": "test-state", "code": "code"})
    assert response.status_code == (200 if succeeds else 400)
    result = ensure_instance_block(store, provider, instance)
    if succeeds:
        assert result["access_token"] == "new-token"
        assert result["auth_completed_at"] != "before"
        assert result["label"] == block["label"]
        assert result["history"] == block["history"]
        assert store["pairs"] == before["pairs"]
        if instance != "default":
            assert store[provider]["access_token"] == "other-token"
    else:
        assert store == before


def test_failed_anilist_exchange_does_not_report_the_existing_token_as_new(monkeypatch):
    cfg = {"anilist": {"access_token": "old-token", "client_id": "client", "client_secret": "secret"}}

    def fail(*args, **kwargs):
        raise RuntimeError("Authorization denied")

    monkeypatch.setattr(auth, "load_config", lambda: deepcopy(cfg))
    monkeypatch.setattr(auth, "_import_provider", lambda *args: SimpleNamespace(finish=fail))
    assert auth.anilist_exchange_code_for_token(code="bad-code", redirect_uri="http://testserver/callback/anilist") is None
    assert cfg["anilist"]["access_token"] == "old-token"


def test_plex_reconnect_polls_new_pin_before_accepting_existing_token(monkeypatch):
    block = {
        "account_token": "old-token", "server_url": "http://plex.test", "username": "user",
        "account_id": "42", "pms_token": "server-token", "machine_id": "server", "label": "My Plex",
    }
    store = {"plex": {"client_id": "client", "instances": {"alternate": block}}, "pairs": [{"source": "PLEX"}]}
    calls = []
    monkeypatch.setattr(auth, "load_config", lambda: deepcopy(store))

    def save(cfg):
        store.clear()
        store.update(deepcopy(cfg))

    def start(cfg, **kwargs):
        ensure_instance_block(cfg, "plex", kwargs["instance_id"])["_pending_pin"] = {"id": 123, "code": "ABCD"}
        return {"pin": "ABCD"}

    def finish(cfg, **kwargs):
        current = ensure_instance_block(cfg, "plex", kwargs["instance_id"])
        calls.append(deepcopy(current))
        current["account_token"] = "new-token"
        current.pop("_pending_pin")

    class ImmediateThread:
        def __init__(self, target, args, daemon):
            self.target, self.args = target, args

        def start(self):
            self.target(*self.args)

    monkeypatch.setattr(auth, "save_config", save)
    monkeypatch.setattr(auth, "_import_provider", lambda *args: SimpleNamespace(start=start, finish=finish))
    monkeypatch.setattr(auth, "threading", SimpleNamespace(Thread=ImmediateThread))
    monkeypatch.setattr(auth, "time", SimpleNamespace(time=time.time, sleep=lambda *args: None))
    app = FastAPI()
    auth.register_auth(app)
    response = TestClient(app).post("/api/plex/pin/new?instance=alternate")
    assert response.json()["ok"] is True
    assert len(calls) == 1
    assert calls[0]["account_token"] == "old-token"
    assert calls[0]["_pending_pin"]["id"] == 123
    result = store["plex"]["instances"]["alternate"]
    assert result == {**block, "account_token": "new-token", "client_id": "client"}
    assert store["pairs"] == [{"source": "PLEX"}]


def test_tmdb_reconnect_keeps_old_session_until_new_authorization(monkeypatch):
    block = {"api_key": "key", "session_id": "old-session", "_pending_request_token": "new-request", "label": "My TMDb"}
    store = {"tmdb_sync": {"instances": {"alternate": deepcopy(block)}}, "pairs": [{"source": "TMDB"}]}
    approved = False
    exchanges = []
    monkeypatch.setattr(auth, "load_config", lambda: deepcopy(store))

    def save(cfg):
        store.clear()
        store.update(deepcopy(cfg))

    def post(url, **kwargs):
        exchanges.append(kwargs["json"]["request_token"])
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"session_id": "new-session"} if approved else {})

    def get(url, **kwargs):
        assert kwargs["params"]["session_id"] == "new-session"
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"id": 42, "username": "user"})

    monkeypatch.setattr(auth, "save_config", save)
    monkeypatch.setattr(auth, "requests", SimpleNamespace(post=post, get=get))
    app = FastAPI()
    auth.register_auth(app)
    client = TestClient(app)
    pending = client.get("/api/tmdb_sync/verify?instance=alternate").json()
    assert pending["connected"] is False
    assert pending["pending"] is True
    assert store["tmdb_sync"]["instances"]["alternate"] == block
    approved = True
    result = client.get("/api/tmdb_sync/verify?instance=alternate").json()
    assert result["connected"] is True
    assert result["pending"] is False
    assert exchanges == ["new-request", "new-request"]
    current = store["tmdb_sync"]["instances"]["alternate"]
    assert current["session_id"] == "new-session"
    assert current["label"] == "My TMDb"
    assert "_pending_request_token" not in current
    assert store["pairs"] == [{"source": "TMDB"}]
