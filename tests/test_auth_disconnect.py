# tests/test_auth_disconnect.py
# CrossWatch - Offline disconnect and in-flight authentication isolation
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import importlib
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest


@pytest.mark.parametrize("provider", ["wetrakr", "myanimelist", "kitsu"])
@pytest.mark.parametrize("failed", [False, True])
def test_disconnect_does_not_wait_for_refresh_or_allow_it_to_restore_tokens(monkeypatch, provider, failed):
    auth = importlib.import_module(f"providers.auth._auth_{provider.upper()}")
    state = {"cfg": {provider: {"instances": {
        "P01": {"access_token": "old-access", "refresh_token": "old-refresh", "expires_at": 1},
        "P02": {"access_token": "other-account"},
    }}}}
    read = lambda: copy.deepcopy(state["cfg"])
    save = lambda cfg: state.update(cfg=copy.deepcopy(cfg))
    monkeypatch.setattr(auth, "load_config", read)
    monkeypatch.setattr(auth, "save_config", save)
    if hasattr(auth, "_load_full_cfg"):
        monkeypatch.setattr(auth, "_load_full_cfg", read)
        monkeypatch.setattr(auth, "_save_full_cfg", save)
    if hasattr(auth, "cleanup_remote"):
        monkeypatch.setattr(auth, "cleanup_remote", lambda *a, **kw: None)
    if hasattr(auth, "_REFRESH_RETRY_AT"):
        auth._REFRESH_RETRY_AT.clear()
    started, release = threading.Event(), threading.Event()

    def response(*args, **kwargs):
        started.set()
        assert release.wait(5)
        if provider == "kitsu" and failed:
            raise auth.KitsuAuthError("invalid_grant")
        data = {"ok": not failed, "error": "invalid_grant", "access_token": "stale-access",
                "refresh_token": "stale-refresh", "token_type": "bearer", "expires_in": 3600}
        return (data, not failed) if provider == "wetrakr" else data

    monkeypatch.setattr(auth, "_exchange" if provider == "kitsu" else "_post", response)

    def disconnect():
        if provider == "kitsu":
            with auth._CONFIG_LOCK:
                cfg = read()
                auth.PROVIDER.disconnect(cfg, instance_id="P01")
                save(cfg)
        else:
            auth.PROVIDER.disconnect(instance_id="P01")

    with ThreadPoolExecutor(max_workers=2) as pool:
        refresh = pool.submit(auth.refresh_token, instance_id="P01")
        try:
            assert started.wait(2)
            pool.submit(disconnect).result(timeout=2)
            assert not state["cfg"][provider]["instances"]["P01"].get("access_token")
            state["cfg"][provider]["instances"]["P01"].update(access_token="new-connection", refresh_token="new-refresh")
        finally:
            release.set()
        if provider == "kitsu":
            with pytest.raises(auth.KitsuAuthError, match="connection changed"):
                refresh.result(timeout=2)
        else:
            assert refresh.result(timeout=2)["error"] == "connection_changed"
    assert state["cfg"][provider]["instances"]["P01"]["access_token"] == "new-connection"
    assert state["cfg"][provider]["instances"]["P02"]["access_token"] == "other-account"


@pytest.mark.parametrize("provider", ["punchplay", "simkl", "wetrakr", "myanimelist"])
def test_disconnect_saves_before_remote_cleanup_and_captures_old_credentials(monkeypatch, provider):
    auth = importlib.import_module(f"providers.auth._auth_{provider.upper()}")
    cfg = {provider: {"access_token": "old-access", "refresh_token": "old-refresh", "client_id": "test-client", "auth_version": 2}}
    state = {"cfg": copy.deepcopy(cfg)}
    save = lambda value: state.update(cfg=copy.deepcopy(value))
    monkeypatch.setattr(auth, "load_config", lambda: copy.deepcopy(state["cfg"]))
    monkeypatch.setattr(auth, "save_config", save)
    if hasattr(auth, "_load_full_cfg"):
        monkeypatch.setattr(auth, "_load_full_cfg", lambda: copy.deepcopy(state["cfg"]))
        monkeypatch.setattr(auth, "_save_full_cfg", save)
    if provider == "myanimelist":
        auth._PENDING["default"] = {"origin": "https://example.test", "session_id": "old-session", "poll_token": "old-poll"}
    cleanup = []

    def schedule(callback, *args, **kwargs):
        assert not state["cfg"][provider].get("access_token")
        cleanup.append((callback, copy.deepcopy(args), copy.deepcopy(kwargs)))

    monkeypatch.setattr(auth, "cleanup_remote", schedule)
    auth.PROVIDER.disconnect(cfg)
    assert len(cleanup) == 1
    cfg[provider]["access_token"] = "new-access"
    assert "new-access" not in repr(cleanup)
    assert ("old-poll" if provider == "myanimelist" else "old-access") in repr(cleanup)


def test_remote_cleanup_does_not_wait_for_unreachable_service():
    from providers.auth._cleanup import cleanup_remote

    started, release, finished = threading.Event(), threading.Event(), threading.Event()

    def remote():
        started.set()
        try:
            assert release.wait(5)
            raise ConnectionError("offline")
        finally:
            finished.set()

    try:
        cleanup_remote(remote)
        assert started.wait(2)
        assert not finished.is_set()
    finally:
        release.set()
    assert finished.wait(2)


@pytest.mark.parametrize("provider", ["simkl", "kitsu"])
def test_disconnect_api_clears_local_state_without_waiting(monkeypatch, provider):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import authenticationAPI as api
    from providers.auth import _cleanup

    auth = importlib.import_module(f"providers.auth._auth_{provider.upper()}")
    state = {"cfg": {provider: {"access_token": "old-access", "refresh_token": "old-refresh", "client_id": "test-client"}}}
    read = lambda: copy.deepcopy(state["cfg"])
    save = lambda cfg: state.update(cfg=copy.deepcopy(cfg))
    monkeypatch.setattr(api, "load_config", read)
    monkeypatch.setattr(api, "save_config", save)
    queued = []
    monkeypatch.setattr(_cleanup, "cleanup_remote", lambda fn, *args, **kwargs: queued.append((fn, args, kwargs)))
    app = FastAPI()
    api.register_auth(app)
    started, release = threading.Event(), threading.Event()

    def busy_refresh():
        with auth.instance_lock("default") if provider == "kitsu" else threading.Lock():
            started.set()
            assert release.wait(5)

    with ThreadPoolExecutor(max_workers=2) as pool:
        busy = pool.submit(busy_refresh)
        try:
            assert started.wait(2)
            path = "/api/simkl/token/delete" if provider == "simkl" else "/api/kitsu/disconnect"
            result = pool.submit(TestClient(app).post, path).result(timeout=2)
            assert result.json()["ok"]
            assert not state["cfg"][provider].get("access_token")
        finally:
            release.set()
        busy.result(timeout=2)
    if provider == "simkl":
        assert queued[0][1][0]["access_token"] == "old-access"


@pytest.mark.parametrize("provider", ["wetrakr", "myanimelist"])
def test_disconnect_invalidates_login_already_verifying_account(monkeypatch, provider):
    from types import SimpleNamespace

    auth = importlib.import_module(f"providers.auth._auth_{provider.upper()}")
    state = {"cfg": {provider: {}}}
    read = lambda: copy.deepcopy(state["cfg"])
    save = lambda cfg: state.update(cfg=copy.deepcopy(cfg))
    monkeypatch.setattr(auth, "load_config", read)
    monkeypatch.setattr(auth, "save_config", save)
    if hasattr(auth, "_load_full_cfg"):
        monkeypatch.setattr(auth, "_load_full_cfg", read)
        monkeypatch.setattr(auth, "_save_full_cfg", save)
    monkeypatch.setattr(auth, "cleanup_remote", lambda *args, **kwargs: None)
    auth._PENDING["default"] = {"flow_id": "flow", "state": "flow", "expires_at": float("inf"), "next_poll": 0,
                                "interval": 5, "origin": "https://example.test", "session_id": "session", "poll_token": "poll",
                                "token_response": {"access_token": "stale", "refresh_token": "stale-refresh"},
                                "tokens": {"access_token": "stale", "refresh_token": "stale-refresh"}}
    started, release = threading.Event(), threading.Event()

    def identity(*args, **kwargs):
        started.set()
        assert release.wait(5)
        account = {"id": 1, "name": "Test", "info": {"username": "Test"}}
        return (account, True) if provider == "wetrakr" else SimpleNamespace(status_code=200, json=lambda: account)

    if provider == "wetrakr":
        monkeypatch.setattr(auth, "_identity", identity)
        login = lambda: auth.finish_oauth(flow_id="flow", code="code")
    else:
        monkeypatch.setattr(auth.requests, "get", identity)
        login = lambda: auth.poll_oauth(flow_id="flow")
    with ThreadPoolExecutor(max_workers=2) as pool:
        attempt = pool.submit(login)
        try:
            assert started.wait(2)
            pool.submit(auth.PROVIDER.disconnect).result(timeout=2)
        finally:
            release.set()
        assert attempt.result(timeout=2)["error"] == "connection_changed"
    assert not state["cfg"][provider].get("access_token")
