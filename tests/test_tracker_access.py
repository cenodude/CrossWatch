# tests/test_tracker_access.py
# CrossWatch - Managed users reach only the tracker instances assigned to their profile
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

import crosswatch
import ui_frontend
from api import appAuthAPI, trackerAPI
from cw_platform import access_policy
from cw_platform.tracker_store import TrackerStore
from services import tracker_import

HEAT = {"type": "movie", "title": "Heat", "year": 1995, "ids": {"tmdb": "949"}, "watched_at": "2026-11-01T10:00:00.000Z"}
ANN = {"username": "ann", "profile_id": "ann", "is_admin": False, "permissions": {"write": True}}
ASSIGNED = {"ann": {"CROSSWATCH": ["CW-P01"], "TRAKT": ["TRAKT-P01"]}}


@pytest.fixture()
def cfg(tmp_path, monkeypatch):
    conf = {"crosswatch": {"root_dir": str(tmp_path / "default"), "instances": {
        "CW-P01": {"label": "Ann", "root_dir": str(tmp_path / "ann")},
        "CW-P02": {"label": "Bob", "root_dir": str(tmp_path / "bob")},
    }}}
    for name in ("default", "ann", "bob"):
        TrackerStore(tmp_path / name).write_state("history", {"tmdb:949": {**HEAT, "title": f"Heat {name}"}})
    monkeypatch.setattr(access_policy, "instances_for_user_profile", lambda _cfg, pid: ASSIGNED.get(pid, {}))
    monkeypatch.setattr(trackerAPI, "load_config", lambda: conf)
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    return conf


def _client(user: dict[str, Any] | None) -> TestClient:
    app = FastAPI()

    @app.middleware("http")
    async def as_user(request: Request, call_next):
        request.state.cw_user = user
        return await call_next(request)

    app.include_router(trackerAPI.router)
    return TestClient(app)


def test_managed_user_sees_and_edits_only_their_tracker(cfg):
    api = _client(ANN)

    summary = api.get("/api/tracker/summary?provider_instance=default").json()
    assert summary["provider_instance"] == "CW-P01"
    assert [i["id"] for i in summary["instances"]] == ["CW-P01"]
    assert api.get("/api/tracker/items?provider_instance=CW-P01").json()["items"][0]["title"] == "Heat ann"

    for path in ("/api/tracker/items?provider_instance=CW-P02", "/api/tracker/items?provider_instance=default", "/api/tracker/stats?provider_instance=CW-P02"):
        res = api.get(path)
        assert (res.status_code, res.json()["detail"]) == (403, "profile_scope_denied"), path
    blocked = api.post("/api/tracker/delete", json={"feature": "history", "provider_instance": "CW-P02", "keys": ["tmdb:949"]})
    assert blocked.status_code == 403
    assert TrackerStore(trackerAPI.tracker_context(cfg, trackerAPI.CONFIG, "CW-P02")[1]).read_current("history")["items"]


def test_admin_still_sees_every_tracker(cfg):
    api = _client({"username": "admin", "is_admin": True})

    assert [i["id"] for i in api.get("/api/tracker/summary").json()["instances"]] == ["default", "CW-P01", "CW-P02"]
    assert api.get("/api/tracker/items?provider_instance=CW-P02").json()["items"][0]["title"] == "Heat bob"
    assert api.get("/api/tracker/import/sources").json()["can_create"] is True


def test_admin_viewing_as_a_profile_gets_that_scope(cfg, monkeypatch):
    from cw_platform import config_base

    monkeypatch.setattr(access_policy, "requested_view_as_profile", lambda _request: "ann")
    monkeypatch.setattr(access_policy, "valid_user_profile_id", lambda _cfg, pid: pid == "ann")
    monkeypatch.setattr(config_base, "load_config", lambda: cfg)
    api = _client({"username": "admin", "is_admin": True})

    assert [i["id"] for i in api.get("/api/tracker/summary").json()["instances"]] == ["CW-P01"]


def test_bell_and_import_only_show_the_users_own_instances(cfg, monkeypatch):
    sources = [
        {"id": "TRAKT", "label": "Trakt", "instances": [{"id": "default", "label": "Default"}, {"id": "TRAKT-P01", "label": "Ann"}], "features": ["history"]},
        {"id": "PLEX", "label": "Plex", "instances": [{"id": "default", "label": "Default"}], "features": ["history"]},
    ]
    monkeypatch.setattr(tracker_import, "sources", lambda _cfg: sources)
    bob = TrackerStore(trackerAPI.tracker_context(cfg, trackerAPI.CONFIG, "CW-P02")[1])
    bob.save_job("metadata_check", {"id": "c1", "state": "completed", "finished_at": 9, "attention": 3})
    api = _client(ANN)

    listed = api.get("/api/tracker/import/sources").json()
    assert listed["sources"] == [{**sources[0], "instances": [{"id": "TRAKT-P01", "label": "Ann"}]}]
    assert [i["id"] for i in listed["instances"]] == ["CW-P01"]
    assert listed["can_create"] is False
    assert api.get("/api/tracker/check/current").json()["job"] is None

    other = api.post("/api/tracker/import", json={"source": "TRAKT", "source_instance": "default", "provider_instance": "CW-P01", "features": ["history"]})
    assert (other.status_code, other.json()["error"]) == (403, "profile_scope_denied")
    target = api.post("/api/tracker/import", json={"source": "TRAKT", "source_instance": "TRAKT-P01", "provider_instance": "CW-P02", "features": ["history"]})
    assert target.status_code == 403


@pytest.mark.parametrize(("permissions", "allowed"), [({"write": True}, True), ({"write": False}, False), ({"monitor": True}, False)])
def test_tracker_api_is_open_to_managed_users_with_write_access(permissions, allowed):
    user = {"is_admin": False, "permissions": permissions}
    for method, path in (("GET", "/api/tracker/summary"), ("POST", "/api/tracker/edit")):
        assert appAuthAPI.non_admin_api_allowed(path, method)
        assert crosswatch._non_admin_permission_allowed(user, path, method) is allowed


def test_managed_shell_has_the_tracker_tab_only_with_an_assigned_instance(monkeypatch):
    from cw_platform import config_base

    monkeypatch.setattr(config_base, "load_config", lambda: {})
    monkeypatch.setattr(ui_frontend, "managed_profile_instances", lambda _cfg, user: ASSIGNED.get(str((user or {}).get("profile_id")), {}))
    with_tracker = ui_frontend.get_index_html(include_admin=False, user=ANN)
    assert 'id="tab-tracker"' in with_tracker and 'id="page-tracker"' in with_tracker
    assert 'data-cw-perm-tracker="on"' in with_tracker
    assert "tracker-notifications.js" in with_tracker and "month-bar.js" in with_tracker
    assert with_tracker.index('id="tab-playlists"') < with_tracker.index('id="tab-tracker"') < with_tracker.index('id="tab-editor"')

    for user in ({**ANN, "profile_id": "bob"}, {**ANN, "permissions": {"write": False}}):
        html = ui_frontend.get_index_html(include_admin=False, user=user)
        assert 'id="tab-tracker"' not in html and 'id="page-tracker"' not in html
        assert 'data-cw-perm-tracker="off"' in html
        assert "tracker-notifications.js" not in html
