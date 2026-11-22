# tests/test_view_cache.py
# CrossWatch - Background view snapshot lifecycle tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
import copy
import threading

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from services.view_cache import ViewCache


@pytest.fixture
def cache():
    service = ViewCache()
    yield service
    service.stop()


def _data(response):
    return json.loads(response.body)


def _idle(cache):
    cache.pool.submit(lambda: None).result(timeout=5)


def _expire_check(cache):
    with cache.lock:
        for entry in cache.entries.values():
            entry["checked"] = 0


def test_cold_requests_share_one_background_build(tmp_path, cache):
    started, release = threading.Event(), threading.Event()
    calls = []

    def build():
        calls.append(1)
        started.set()
        assert release.wait(5)
        return {"ok": True, "items": [{"key": "one"}], "total": 1}

    try:
        first = _data(cache.get(tmp_path, "alice", "collection", lambda: 1, build))
        assert first["_view"]["ready"] is False
        assert started.wait(5)
        second = _data(cache.get(tmp_path, "alice", "collection", lambda: 1, build))
        assert second["_view"]["refreshing"] is True
        assert "items" not in second
        assert len(calls) == 1
    finally:
        release.set()
    _idle(cache)
    ready = _data(cache.get(tmp_path, "alice", "collection", lambda: 1, build))
    assert ready["items"] == [{"key": "one"}]
    assert ready["_view"]["ready"] is True
    assert ready["_view"]["refreshing"] is False


def test_stale_response_does_not_wait_for_revision_or_build(tmp_path, cache):
    cache.get(tmp_path, "alice", "history", lambda: 1, lambda: {"items": ["old"]})
    _idle(cache)
    _expire_check(cache)
    started, release = threading.Event(), threading.Event()

    def revision():
        started.set()
        assert release.wait(5)
        return 2

    try:
        response = _data(cache.get(tmp_path, "alice", "history", revision, lambda: {"items": ["new"]}))
        assert response["items"] == ["old"]
        assert response["_view"]["refreshing"]
        assert started.wait(5)
        assert _data(cache.get(tmp_path, "alice", "history", revision, lambda: {}))["items"] == ["old"]
    finally:
        release.set()
    _idle(cache)
    assert _data(cache.get(tmp_path, "alice", "history", lambda: 2, lambda: {}))["items"] == ["new"]


def test_snapshots_survive_restart_and_unchanged_revision_skips_build(tmp_path, cache):
    cache.get(tmp_path, "alice", "ratings", lambda: 1, lambda: {"items": ["saved"]})
    _idle(cache)
    restarted = ViewCache()
    try:
        calls = []
        data = _data(restarted.get(tmp_path, "alice", "ratings", lambda: 1, lambda: calls.append(1)))
        assert data["items"] == ["saved"]
        _idle(restarted)
        assert not calls
        other = _data(restarted.get(tmp_path, "bob", "ratings", lambda: 1, lambda: {"items": []}))
        assert other["_view"]["ready"] is False
        assert "items" not in other
        _idle(restarted)
    finally:
        restarted.stop()


@pytest.mark.parametrize("result", [JSONResponse({"error": "unavailable"}, status_code=503), {"ok": False}, None])
def test_failed_rebuild_keeps_snapshot_and_can_retry(tmp_path, cache, result):
    cache.get(tmp_path, "alice", "watchlist", lambda: 1, lambda: {"items": ["saved"]})
    _idle(cache)
    _expire_check(cache)
    cache.get(tmp_path, "alice", "watchlist", lambda: 2, lambda: result)
    _idle(cache)
    failed = _data(cache.get(tmp_path, "alice", "watchlist", lambda: 2, lambda: {}))
    assert failed["items"] == ["saved"]
    assert failed["_view"]["error"] == "refresh_failed"
    assert not failed["_view"]["refreshing"]
    cache.get(tmp_path, "alice", "watchlist", lambda: 2, lambda: {"items": []}, retry=True)
    _idle(cache)
    assert _data(cache.get(tmp_path, "alice", "watchlist", lambda: 2, lambda: {}))["items"] == []


def test_invalidated_job_cannot_publish_and_disk_snapshot_is_removed(tmp_path, cache):
    cache.get(tmp_path, "alice", "collection", lambda: 1, lambda: {"items": ["old"]})
    _idle(cache)
    _expire_check(cache)
    started, release = threading.Event(), threading.Event()

    def build():
        started.set()
        assert release.wait(5)
        return {"items": ["obsolete"]}

    try:
        cache.get(tmp_path, "alice", "collection", lambda: 2, build)
        assert started.wait(5)
        cache.invalidate(tmp_path)
        assert not list((tmp_path / ".cw_cache" / "views").glob("*.json"))
    finally:
        release.set()
    _idle(cache)
    assert not cache.entries
    assert not cache.pending
    assert not list((tmp_path / ".cw_cache" / "views").glob("*.json"))


def test_changed_revision_during_build_is_not_published(tmp_path, cache):
    revision = [1]

    def build():
        revision[0] = 2
        return {"items": ["outdated"]}

    cache.get(tmp_path, "alice", "history", lambda: revision[0], build)
    _idle(cache)
    assert not cache.entries
    assert not cache.failures


def test_route_opt_in_keeps_sync_contract_and_checks_scope_on_every_read(tmp_path, cache, monkeypatch):
    from api import view_cache

    allowed = [True]
    scope = ["alice"]
    monkeypatch.setattr(view_cache, "CACHE", cache)
    monkeypatch.setattr(view_cache, "_context", lambda request, arguments: (tmp_path, scope[0], lambda: 1) if allowed[0] else JSONResponse({"ok": False}, status_code=403))
    app = FastAPI()

    @app.get("/items")
    @view_cache.background_view
    def items(request: Request, page: int = 1):
        return {"ok": True, "items": [page]}

    client = TestClient(app)
    assert client.get("/items?page=3").json() == {"ok": True, "items": [3]}
    assert client.get("/items?page=3&background=1").json()["_view"]["ready"] is False
    _idle(cache)
    assert client.get("/items?page=3&background=1").json()["items"] == [3]
    assert client.get("/items?page=4&background=1").json()["_view"]["ready"] is False
    _idle(cache)
    third = client.get("/items?page=3&background=1").json()["_view"]
    fourth = client.get("/items?page=4&background=1").json()["_view"]
    assert third["revision"] == fourth["revision"]
    assert third["version"] != fourth["version"]
    allowed[0] = False
    assert client.get("/items?page=3&background=1").status_code == 403
    allowed[0] = True
    scope[0] = "bob"
    assert client.get("/items?page=3&background=1").json()["_view"]["ready"] is False
    _idle(cache)


def test_explicit_refresh_checks_revision_even_when_recently_checked(tmp_path, cache):
    cache.get(tmp_path, "alice", "history", lambda: 1, lambda: {"items": ["old"]})
    _idle(cache)
    old = _data(cache.get(tmp_path, "alice", "history", lambda: 2, lambda: {"items": ["new"]}, retry=True))
    assert old["items"] == ["old"]
    assert old["_view"]["refreshing"]
    _idle(cache)
    new = _data(cache.get(tmp_path, "alice", "history", lambda: 2, lambda: {}))
    assert new["items"] == ["new"]
    assert new["_view"]["revision"] != old["_view"]["revision"]


def test_snapshot_persistence_does_not_block_memory_reads(tmp_path, cache, monkeypatch):
    started, release = threading.Event(), threading.Event()

    def persist(*args):
        started.set()
        assert release.wait(5)

    monkeypatch.setattr(cache, "_persist", persist)
    try:
        cache.get(tmp_path, "alice", "collection", lambda: 1, lambda: {"items": ["available"]})
        assert started.wait(5)
        assert _data(cache.get(tmp_path, "alice", "collection", lambda: 1, lambda: {}))["items"] == ["available"]
    finally:
        release.set()
    _idle(cache)


def test_corrupt_snapshot_is_rebuilt(tmp_path, cache):
    cache.get(tmp_path, "alice", "collection", lambda: 1, lambda: {"items": ["old"]})
    _idle(cache)
    path = next((tmp_path / ".cw_cache" / "views").glob("*.json"))
    stored = json.loads(path.read_text("utf-8"))
    stored["body"] = "{invalid"
    path.write_text(json.dumps(stored), "utf-8")
    cache.entries.clear()
    assert not _data(cache.get(tmp_path, "alice", "collection", lambda: 1, lambda: {"items": ["recovered"]}))["_view"]["ready"]
    _idle(cache)
    assert _data(cache.get(tmp_path, "alice", "collection", lambda: 1, lambda: {}))["items"] == ["recovered"]


def test_routine_write_retains_all_snapshots_and_rebuilds_only_changed_data(tmp_path, cache):
    calls = []
    for user in ("alice", "bob"):
        cache.get(tmp_path, user, "history", lambda: 1, lambda user=user: calls.append(user) or {"items": [user]})
    _idle(cache)
    cache.mark_stale()
    assert len(list((tmp_path / ".cw_cache" / "views").glob("*.json"))) == 2
    started, release = threading.Event(), threading.Event()

    def build():
        started.set()
        assert release.wait(5)
        return {"items": ["updated"]}

    try:
        alice = _data(cache.get(tmp_path, "alice", "history", lambda: 2, build))
        assert alice["items"] == ["alice"]
        assert alice["_view"]["refreshing"]
        assert started.wait(5)
        bob = _data(cache.get(tmp_path, "bob", "history", lambda: 1, lambda: calls.append("unnecessary rebuild")))
        assert bob["items"] == ["bob"]
    finally:
        release.set()
    _idle(cache)
    assert calls == ["alice", "bob"]
    assert _data(cache.get(tmp_path, "alice", "history", lambda: 2, lambda: {}))["items"] == ["updated"]


def test_config_changes_keep_snapshots_but_access_changes_isolate_them(tmp_path, cache, monkeypatch):
    from api import profileAPI, view_cache, appAuthAPI
    from cw_platform import config_base
    from cw_platform.provider_instances import list_user_profiles

    cfg = {"plex": {}, "trakt": {}, "user_profiles": {"alice": {"instances": {"PLEX": ["default"]}}}}
    list_user_profiles(cfg)
    user = {"permissions": {"watchlist": True}}
    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    monkeypatch.setattr(config_base, "load_config", lambda: copy.deepcopy(cfg))
    monkeypatch.setattr(profileAPI, "_profile_context", lambda request: (copy.deepcopy(cfg), {}, "alice", {}, user, "session"))
    monkeypatch.setattr(profileAPI, "collection_cache_fingerprint", lambda: 1)
    monkeypatch.setattr(appAuthAPI, "effective_request_profile_id", lambda *args: "alice")
    monkeypatch.setattr(view_cache, "CACHE", cache)
    app = FastAPI()

    @app.get("/api/profile/collection")
    @view_cache.background_view
    def collection(request: Request):
        return {"items": ["available"]}

    client = TestClient(app)
    url = "/api/profile/collection?background=1"
    assert not client.get(url).json()["_view"]["ready"]
    _idle(cache)
    cfg.update(ui={"theme": "flat-dark"}, app_auth={"sessions": [{"token_hash": "new session"}]})
    cfg["plex"]["access_token"] = "rotated"
    cache.mark_stale()
    assert client.get(url).json()["items"] == ["available"]
    _idle(cache)
    user["permissions"]["watchlist"] = False
    assert not client.get(url).json()["_view"]["ready"]
    _idle(cache)
    from cw_platform.provider_instances import upsert_user_profile
    upsert_user_profile(cfg, "alice", label="Alice", instances={"TRAKT": ["default"]})
    assert not client.get(url).json()["_view"]["ready"]
    _idle(cache)


@pytest.mark.parametrize("warm", [False, True])
@pytest.mark.parametrize("stage", ["queued", "running"])
def test_config_change_supersedes_refresh_without_stopping_polling(tmp_path, cache, monkeypatch, warm, stage):
    from api import profileAPI, view_cache, appAuthAPI
    from cw_platform import config_base

    cfg = {"ui": {"theme": "original"}}
    revision = [1]
    blocking = [False]
    started, release = threading.Event(), threading.Event()
    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    monkeypatch.setattr(config_base, "load_config", lambda: copy.deepcopy(cfg))
    monkeypatch.setattr(profileAPI, "_profile_context", lambda request: (copy.deepcopy(cfg), {}, profileAPI.ADMIN_USER_ID, {}, {}, "session"))
    monkeypatch.setattr(profileAPI, "collection_cache_fingerprint", lambda: revision[0])
    monkeypatch.setattr(appAuthAPI, "effective_request_profile_id", lambda *args: "")
    monkeypatch.setattr(view_cache, "CACHE", cache)
    app = FastAPI()

    def block():
        started.set()
        assert release.wait(5)
        return {"items": []}

    @app.get("/api/profile/collection")
    @view_cache.background_view
    def collection(request: Request):
        theme = cfg["ui"]["theme"]
        if blocking[0] and stage == "running":
            block()
        return {"items": [theme]}

    client = TestClient(app)
    url = "/api/profile/collection?background=1"
    if warm:
        client.get(url)
        _idle(cache)
    revision[0] = 2
    blocking[0] = True
    cache.mark_stale()
    try:
        if stage == "queued":
            cache.get(tmp_path, "blocker", "blocker", lambda: 1, block)
            assert started.wait(5)
        client.get(url)
        assert started.wait(5)
        cfg["ui"]["theme"] = "flat-dark"
    finally:
        release.set()
    _idle(cache)
    assert not cache.failures
    data = client.get(url).json()
    assert data["_view"]["error"] is None
    assert data["_view"]["refreshing"]
    assert data["_view"]["ready"] == warm
    assert data.get("items") == (["original"] if warm else None)
    _idle(cache)
    assert client.get(url).json()["items"] == ["flat-dark"]


def test_collection_background_api_scopes_snapshots_and_summary_skips_page_work(tmp_path, cache, monkeypatch):
    from api import profileAPI, view_cache, appAuthAPI
    from cw_platform import config_base
    from cw_platform.local_db import state
    from cw_platform.provider_instances import list_user_profiles

    cfg = {"plex": {}, "trakt": {}, "user_profiles": {"alice": {"label": "Alice", "instances": {"PLEX": ["default"]}}, "bob": {"label": "Bob", "instances": {"TRAKT": ["default"]}}}}
    list_user_profiles(cfg)
    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    monkeypatch.setattr(config_base, "load_config", lambda: copy.deepcopy(cfg))
    monkeypatch.setattr(profileAPI, "CONFIG_DIR", tmp_path)
    monkeypatch.setattr(profileAPI, "_profile_context", lambda request: (copy.deepcopy(cfg), {}, request.headers["x-user"], {}, {}, request.headers["x-user"]))
    monkeypatch.setattr(profileAPI, "effective_user_profile_id", lambda cfg, token, requested: token)
    monkeypatch.setattr(appAuthAPI, "effective_request_profile_id", lambda cfg, request, requested: request.headers["x-user"])
    monkeypatch.setattr(view_cache, "CACHE", cache)
    for provider, title in (("PLEX", "Alice's movie"), ("TRAKT", "Bob's movie")):
        state.save_feature_baseline(tmp_path, provider=provider, feature="collection", items={provider: {"type": "movie", "title": title, "ids": {"tmdb": "1" if provider == "PLEX" else "2"}}})
    app = FastAPI()
    app.include_router(profileAPI.router)
    client = TestClient(app)
    url = "/api/profile/collection?background=1"
    assert not client.get(url, headers={"x-user": "alice"}).json()["_view"]["ready"]
    _idle(cache)
    assert client.get(url, headers={"x-user": "alice"}).json()["items"][0]["title"] == "Alice's movie"
    assert not client.get(url, headers={"x-user": "bob"}).json()["_view"]["ready"]
    _idle(cache)
    assert client.get(url, headers={"x-user": "bob"}).json()["items"][0]["title"] == "Bob's movie"
    monkeypatch.setattr(profileAPI, "build_profile_collection_payload", lambda *args, **kwargs: pytest.fail("Summary must not prepare a page"))
    summary_url = url + "&summary_only=true"
    assert not client.get(summary_url, headers={"x-user": "alice"}).json()["_view"]["ready"]
    _idle(cache)
    summary = client.get(summary_url, headers={"x-user": "alice"}).json()
    assert summary["counts"]["all"] == summary["total"] == 1
    assert "items" not in summary
