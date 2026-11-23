# tests/test_hub_airing.py
# CrossWatch - Hub episode dates, deduplication and profile isolation
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from datetime import date
import json
from types import SimpleNamespace
from threading import Event

import pytest

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from api import profileAPI
from services import hub_airing


class Metadata:
    def __init__(self):
        self.calls = []

    def lookup(self, kind, tmdb, *, season=None, **kwargs):
        self.calls.append((tmdb, season))
        data = {"name": "Example", "status": "Returning Series", "seasons": [{"season_number": 1, "episode_count": 4, "air_date": "2026-11-01"}]}
        if season:
            data = {"episodes": [{"episode_number": n, "name": f"Episode {n}", "air_date": day, "still_path": "/still.jpg"} for n, day in enumerate(["2026-11-21", "2026-11-22", "2026-11-25", "2026-12-22"], 1)]}
        return SimpleNamespace(data=data, fetched_at=9999999999)


def row(number, ids=None):
    return {"_type": "episode", "title": "Example", "season": 1, "episode": number, "ids": {"tmdb": "99999", "show_ids": ids or {"tmdb": "12"}}}


def test_hub_airing_merges_provider_histories_and_excludes_watched_episodes(monkeypatch):
    resolutions = []

    def resolve(cfg, ids, episode_ids, *, strict):
        assert strict
        resolutions.append(dict(ids))
        return 12 if ids.get("tvdb") else None

    monkeypatch.setattr(hub_airing, "show_tmdb_id", resolve)
    provider = Metadata()
    indices = [{"rows": [row(1)] * 1000}, {"rows": [row(2, {"tvdb": "44"}), {"_type": "movie", "ids": {"tmdb": "77"}}]}]
    result = hub_airing.airing_payload(indices, {}, provider, date(2026, 11, 22))
    assert result["total"] == 1
    assert len(resolutions) == 1
    assert [(r["show_ids"], r["episode"]) for r in result["airing"]] == [({"tmdb": "12"}, 3)]
    assert provider.calls == [("12", None), ("12", 1)]


def test_hub_airing_reports_unavailable_metadata(monkeypatch):
    monkeypatch.setattr(hub_airing, "show_tmdb_id", lambda *args, **kwargs: None)
    result = hub_airing.airing_payload([{"rows": [row(1, {"tvdb": "44"})]}], {}, Metadata(), date(2026, 11, 22))
    assert result["unavailable"] == 1
    assert result["airing"] == []


@pytest.mark.parametrize("failed_season", [None, 1])
def test_hub_airing_rejects_partial_metadata_failures(monkeypatch, failed_season):
    provider = Metadata()
    lookup = provider.lookup

    def unavailable(kind, tmdb, *, season=None, **kwargs):
        if tmdb == "12" and season == failed_season:
            raise TimeoutError("Metadata request timed out")
        return lookup(kind, tmdb, season=season, **kwargs)

    monkeypatch.setattr(provider, "lookup", unavailable)
    indices = [{"rows": [row(1), row(1, {"tmdb": "13"})]}]
    result = hub_airing.airing_payload(indices, {}, provider, date(2026, 11, 22))
    assert result["ok"] is False
    assert result["error"] == "episode_metadata_unavailable"
    assert result["total"] == 2
    assert result["unavailable"] == 1
    assert {entry["show_ids"]["tmdb"] for entry in result["airing"]} == {"13"}


def test_hub_identity_timeout_preserves_snapshot_and_recovers(tmp_path, monkeypatch):
    from services.view_cache import ViewCache

    failure = False
    revision = 1
    indices = [{"rows": [row(1, {"tvdb": "44"}), row(1, {"tmdb": "13"})]}]

    def resolve(cfg, ids, episode_ids, *, strict):
        assert strict
        if failure:
            raise TimeoutError("Identity lookup timed out")
        return 12

    monkeypatch.setattr(hub_airing, "show_tmdb_id", resolve)
    cache = ViewCache()

    def read(*, retry=False):
        return json.loads(bytes(cache.get(tmp_path, "hub", "airing", lambda: revision,
                                         lambda: hub_airing.airing_payload(indices, {}, Metadata(), date(2026, 11, 22)), retry=retry).body))

    try:
        read()
        cache.pool.submit(lambda: None).result(timeout=5)
        saved = read()
        assert {entry["show_ids"]["tmdb"] for entry in saved["airing"]} == {"12", "13"}
        revision += 1
        indices[0]["rows"].append(row(2, {"tmdb": "13"}))
        failure = True
        cache.mark_stale()
        read()
        cache.pool.submit(lambda: None).result(timeout=5)
        failed = read()
        assert failed["airing"] == saved["airing"]
        assert failed["_view"]["error"] == "refresh_failed"
        failure = False
        read(retry=True)
        cache.pool.submit(lambda: None).result(timeout=5)
        recovered = read()
        assert recovered["_view"]["error"] is None
        assert recovered["_view"]["version"] != saved["_view"]["version"]
        assert not any(entry["show_ids"]["tmdb"] == "13" and entry["episode"] == 2 for entry in recovered["airing"])
    finally:
        cache.stop()


def test_hub_airing_uses_effective_profile_for_both_history_sources(monkeypatch):
    from providers.metadata import _meta_TMDB

    cfg = {"tmdb": {"api_key": "test"}}
    calls = []
    monkeypatch.setattr(profileAPI, "_profile_context", lambda request: (cfg, None, "user", {}, {}, "token"))
    monkeypatch.setattr(profileAPI, "effective_user_profile_id", lambda cfg, token, requested: "family")

    def index(cfg, profile, source):
        calls.append((profile, source))
        return {"rows": [row(1)]}, "revision"

    monkeypatch.setattr(profileAPI, "_profile_history_index", index)
    monkeypatch.setattr(_meta_TMDB, "TmdbProvider", lambda *args: Metadata())
    app = FastAPI()
    app.include_router(profileAPI.router)
    client = TestClient(app)
    data = client.get("/api/profile/airing?day=2026-11-22&user_profile=other").json()
    assert calls == [("family", "synced"), ("family", "scrobble")]
    assert [r["episode"] for r in data["airing"]] == [2, 3]
    calls.clear()
    cfg.clear()
    assert client.get("/api/profile/airing?day=2026-11-22").json()["metadata_available"] is False
    assert calls == []
    assert client.get("/api/profile/airing?day=invalid").status_code == 400


@pytest.mark.parametrize("path", ["/api/profile/airing", "/api/tracker/overview/shows"])
def test_airing_build_does_not_block_other_cached_pages_and_survives_restart(tmp_path, monkeypatch, path):
    from api import view_cache
    from services.view_cache import ViewCache

    regular, airing = ViewCache(), ViewCache()
    monkeypatch.setattr(view_cache, "CACHE", regular)
    monkeypatch.setattr(view_cache, "AIRING_CACHE", airing)
    monkeypatch.setattr(view_cache, "_context", lambda request, arguments: (tmp_path, arguments.get("user_profile", ""), lambda: 1))
    entered, release = Event(), Event()
    app = FastAPI()

    @app.get(path)
    @view_cache.background_view
    def slow(request: Request, user_profile: str = "family"):
        entered.set()
        assert release.wait(5)
        return {"ok": True, "airing": [user_profile]}

    @app.get("/api/profile/history")
    @view_cache.background_view
    def fast(request: Request):
        return {"ok": True, "items": ["ready"]}

    client = TestClient(app)
    try:
        assert not client.get(path + "?background=1").json()["_view"]["ready"]
        assert entered.wait(3)
        client.get("/api/profile/history?background=1")
        assert regular.pool is not None
        regular.pool.submit(lambda: None).result(timeout=3)
        assert client.get("/api/profile/history?background=1").json()["items"] == ["ready"]
        release.set()
        assert airing.pool is not None
        airing.pool.submit(lambda: None).result(timeout=3)
        airing.entries.clear()
        assert client.get(path + "?background=1").json()["airing"] == ["family"]
        assert not client.get(path + "?background=1&user_profile=other").json()["_view"]["ready"]
        airing.pool.submit(lambda: None).result(timeout=3)
        assert client.get(path + "?background=1&user_profile=other").json()["airing"] == ["other"]
    finally:
        release.set()
        regular.stop()
        airing.stop()


def test_hub_and_tracker_episode_builds_can_run_together(tmp_path, monkeypatch):
    from api import view_cache
    from services.view_cache import ViewCache

    cache = ViewCache(workers=2)
    monkeypatch.setattr(view_cache, "AIRING_CACHE", cache)
    monkeypatch.setattr(view_cache, "_context", lambda request, arguments: (tmp_path, "owner", lambda: 1))
    hub_started, tracker_started, release = Event(), Event(), Event()
    app = FastAPI()

    @app.get("/api/profile/airing")
    @view_cache.background_view
    def hub(request: Request):
        hub_started.set()
        assert release.wait(5)
        return {"ok": True, "airing": ["hub"]}

    @app.get("/api/tracker/overview/shows")
    @view_cache.background_view
    def tracker(request: Request):
        tracker_started.set()
        assert release.wait(5)
        return {"ok": True, "airing": ["tracker"]}

    client = TestClient(app)
    try:
        assert not client.get("/api/profile/airing?background=1").json()["_view"]["ready"]
        assert hub_started.wait(3)
        assert not client.get("/api/tracker/overview/shows?background=1").json()["_view"]["ready"]
        assert tracker_started.wait(3)
    finally:
        release.set()
        cache.pool.shutdown(wait=True)
        cache.stop()
