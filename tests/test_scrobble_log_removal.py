# tests/test_scrobble_log_removal.py
# CrossWatch - Scoped cleanup of individual Hub scrobble log entries
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import profileAPI
from cw_platform.local_db import close_conn
from cw_platform.local_db.db import get_conn
from services import activity, profile_history
from services.view_cache import CACHE


@pytest.fixture
def log(tmp_path, monkeypatch):
    close_conn()
    monkeypatch.setattr(activity, "state_dir", lambda: tmp_path)
    profile_history.clear_history_cache()
    yield tmp_path
    close_conn()
    profile_history.clear_history_cache()


def add(key, **values):
    return activity.add_event({
        "id": key, "kind": "scrobble", "method": "watcher", "event": "scrobble_stop",
        "source": "plex", "source_instance": "alice", "target": "trakt", "target_instance": "alice",
        "media_type": "movie", "title": "Arrival", "account": "alice", "ids": {"tmdb": "329865"},
        "progress": 100, "watched_at": 1794000000, "captured_at": 1794000000, **values,
    })


def test_removes_one_group_and_its_ids_but_keeps_other_plays_and_activity(log):
    add("first-trakt")
    add("first-simkl", target="simkl")
    add("rewatch", captured_at=1794000600, watched_at=1794000600)
    add("other", source_instance="bob", target_instance="bob", account="bob")
    add("rating", kind="rating")
    selected = next(row["id"] for row in activity.list_events()["items"] if len(row["targets"]) == 2)
    result = activity.remove_scrobble_events([selected], allowed=lambda row: row.get("account") == "alice")
    assert result == {"ok": True, "removed": 1, "events_removed": 2}
    assert {row["id"] for row in activity.list_events(group_routes=False)["items"]} == {"rewatch", "other", "rating"}
    conn = get_conn(activity._activity_base_path())
    assert conn.execute("SELECT COUNT(*) FROM activity_event_ids WHERE event_id IN ('first-trakt','first-simkl')").fetchone()[0] == 0
    assert activity.remove_scrobble_events([selected], allowed=lambda row: True)["removed"] == 0


def test_unauthorized_selection_is_atomic(log):
    add("mine")
    add("theirs", source_instance="bob", target_instance="bob", account="bob")
    with pytest.raises(PermissionError):
        activity.remove_scrobble_events(["mine", "theirs"], allowed=lambda row: row.get("account") == "alice")
    assert activity.list_events(group_routes=False)["total"] == 2


def test_removes_group_when_new_delivery_changed_its_representative(log):
    add("original")
    add("new-delivery", target="simkl", captured_at=1794000005)
    assert activity.list_events()["items"][0]["id"] == "new-delivery"
    result = activity.remove_scrobble_events(["original"], allowed=lambda row: True)
    assert result == {"ok": True, "removed": 1, "events_removed": 2}
    assert activity.list_events()["total"] == 0


def test_removes_history_sync_log_without_media_ids(log):
    add("synced", kind="history_sync", ids={})
    result = activity.remove_scrobble_events(["synced"], allowed=lambda row: True)
    assert result["events_removed"] == 1
    assert activity.list_events()["total"] == 0


def test_shared_group_only_removes_authorized_deliveries(log):
    add("owned", source_instance="shared", target_instance="alice")
    add("foreign", source_instance="shared", target_instance="bob")
    selected = activity.list_events()["items"][0]["id"]
    result = activity.remove_scrobble_events([selected], allowed=lambda row: row.get("target_instance") == "alice")
    assert result["events_removed"] == 1
    assert activity.list_events(group_routes=False)["items"][0]["id"] == "foreign"


def test_scrobble_selection_uses_event_identity_even_with_equal_title_and_time(log):
    add("mine")
    add("theirs", account="bob", source_instance="bob", target_instance="bob")
    payload = profile_history.build_history_payload(profile_history.build_history_index("scrobble"), include_keys=True, resolve_art=False)
    assert {row["key"] for row in payload["items"]} == {"scrobble:mine", "scrobble:theirs"}
    assert {row["id"] for row in payload["selection"]} == {"mine", "theirs"}


@pytest.fixture
def client(log, monkeypatch):
    user = {"is_admin": False, "profile_id": "alice", "permissions": {"write": True}}
    monkeypatch.setattr(profileAPI, "_profile_context", lambda request: ({}, {}, "alice", {}, user, "session"))
    monkeypatch.setattr(profileAPI, "_origin_allowed", lambda request: True)
    monkeypatch.setattr(profileAPI, "effective_user_profile_id", lambda cfg, token, requested: "alice")
    monkeypatch.setattr(profileAPI, "instances_for_user_profile", lambda cfg, profile: {"TRAKT": ["alice"]})
    invalidations = []
    monkeypatch.setattr(CACHE, "invalidate", lambda base: invalidations.append(base))
    app = FastAPI()
    app.include_router(profileAPI.router)
    with TestClient(app) as session:
        yield session, user, invalidations


def test_endpoint_enforces_scope_and_invalidates_deleted_results(client):
    session, user, invalidations = client
    add("mine")
    add("theirs", source_instance="bob", target_instance="bob", account="bob")
    assert session.post("/api/profile/history/scrobbles/remove", json={"ids": ["theirs"], "user_profile": "bob"}).status_code == 403
    response = session.post("/api/profile/history/scrobbles/remove", json={"ids": ["mine"]})
    assert response.status_code == 200
    assert response.json()["removed"] == 1
    assert len(invalidations) == 1
    assert activity.list_events(group_routes=False)["items"][0]["id"] == "theirs"
    user["permissions"]["write"] = False
    assert session.post("/api/profile/history/scrobbles/remove", json={"ids": ["theirs"]}).status_code == 403


@pytest.mark.parametrize("ids", [[], "mine", [None], [""], ["x"] * 1001])
def test_endpoint_rejects_invalid_selection(client, ids):
    session, _, _ = client
    assert session.post("/api/profile/history/scrobbles/remove", json={"ids": ids}).status_code == 400


def test_endpoint_rejects_cross_origin_write(client, monkeypatch):
    session, _, _ = client
    monkeypatch.setattr(profileAPI, "_origin_allowed", lambda request: False)
    assert session.post("/api/profile/history/scrobbles/remove", json={"ids": ["mine"]}).status_code == 403


def test_endpoint_denies_profile_without_assigned_instances(client, monkeypatch):
    session, _, _ = client
    add("mine")
    monkeypatch.setattr(profileAPI, "instances_for_user_profile", lambda cfg, profile: {})
    assert session.post("/api/profile/history/scrobbles/remove", json={"ids": ["mine"]}).status_code == 403
    assert activity.list_events()["total"] == 1


def test_history_endpoint_returns_ids_for_all_results_selection(client, monkeypatch):
    session, _, _ = client
    add("mine")
    def build(cfg, **kwargs):
        return profile_history.build_history_payload(
            profile_history.build_history_index(kwargs["source"]),
            include_keys=kwargs["include_keys"], resolve_art=False,
        )
    monkeypatch.setattr(profileAPI, "build_profile_history_payload", build)
    response = session.get("/api/profile/history?source=scrobble&include_keys=true&background=1")
    assert response.status_code == 200
    assert response.json()["selection"][0]["id"] == "mine"
