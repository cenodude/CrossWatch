# tests/test_action_route_scope.py
# CrossWatch - Profile scope checks for playback, activity and permission gating

from __future__ import annotations

import json

from starlette.requests import Request


def _request(path: str, method: str = "POST") -> Request:
    return Request(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": method,
            "scheme": "http",
            "path": path,
            "raw_path": path.encode("latin-1"),
            "query_string": b"",
            "headers": [(b"host", b"testserver")],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
        }
    )


def _with_user(request: Request, user: dict | None) -> Request:
    request.state.cw_user = user
    return request


def test_playback_action_denies_foreign_instance() -> None:
    from services.playback_progress import get_service

    result = get_service().remove(
        {"provider": "plex", "instance_id": "PLEX-P01", "record": {"remote_id": "1"}},
        user_filter={"PLEX": ["PLEX-P02"]},
    )

    assert result["ok"] is False
    assert result["error_code"] == "profile_scope_denied"


def test_playback_action_denies_omitted_instance_that_defaults_to_owner() -> None:
    from services.playback_progress import get_service

    result = get_service().mark_watched(
        {"provider": "plex", "record": {"remote_id": "1", "can_mark_watched": True}},
        user_filter={"PLEX": ["PLEX-P02"]},
    )

    assert result["ok"] is False
    assert result["error_code"] == "profile_scope_denied"


def test_playback_bulk_scopes_every_item(monkeypatch) -> None:
    from services.playback_progress import get_service

    service = get_service()
    allowed_calls: list = []
    monkeypatch.setattr(
        service,
        "_adapter_for_action",
        lambda cfg, provider, inst: allowed_calls.append((provider, inst)) or (None, {}, inst),
    )

    out = service.bulk(
        {
            "action": "mark_watched",
            "items": [
                {"provider": "plex", "instance_id": "PLEX-P02", "record": {"can_mark_watched": True}},
                {"provider": "plex", "instance_id": "PLEX-P01", "record": {"can_mark_watched": True}},
            ],
        },
        user_filter={"PLEX": ["PLEX-P02"]},
    )

    codes = [r.get("error_code") for r in out["results"]]
    assert codes[1] == "profile_scope_denied"
    assert ("plex", "PLEX-P01") not in allowed_calls


def test_playback_admin_unscoped_is_unaffected(monkeypatch) -> None:
    from services.playback_progress import get_service

    service = get_service()
    seen: list = []
    monkeypatch.setattr(
        service,
        "_adapter_for_action",
        lambda cfg, provider, inst: seen.append((provider, inst)) or (None, {}, inst),
    )

    result = service.remove({"provider": "plex", "instance_id": "PLEX-P01", "record": {}}, user_filter={})

    assert result["error_code"] == "unknown_provider"
    assert seen == [("plex", "PLEX-P01")]


def test_activity_clear_denied_for_managed_user(monkeypatch) -> None:
    from api import activityAPI

    called: list = []
    monkeypatch.setattr(activityAPI, "clear_events", lambda *a, **k: called.append(1) or {"ok": True})

    resp = activityAPI.activity_clear(
        request=_with_user(_request("/api/activity/history", "DELETE"), {"is_admin": False, "username": "bob"})
    )

    assert resp.status_code == 403
    assert json.loads(resp.body.decode("utf-8"))["error"] == "profile_scope_denied"
    assert called == []


def test_activity_clear_allowed_for_admin(monkeypatch) -> None:
    from api import activityAPI

    monkeypatch.setattr(activityAPI, "clear_events", lambda *a, **k: {"ok": True, "removed": 3})

    resp = activityAPI.activity_clear(
        request=_with_user(_request("/api/activity/history", "DELETE"), {"is_admin": True, "username": "admin"})
    )

    assert resp.status_code == 200
    assert json.loads(resp.body.decode("utf-8"))["removed"] == 3


def test_activity_history_delete_not_in_non_admin_allowlist() -> None:
    from api.appAuthAPI import non_admin_api_allowed

    assert non_admin_api_allowed("/api/activity/history", "GET") is True
    assert non_admin_api_allowed("/api/activity/history", "DELETE") is False


def test_write_permission_no_longer_bypasses_feature_permissions() -> None:
    from crosswatch import _non_admin_permission_allowed

    write_only = {"permissions": {"write": True, "watchlist": False, "playback": False, "dashboard": False}}
    full = {"permissions": {"write": True, "watchlist": True, "playback": True, "dashboard": True}}

    assert _non_admin_permission_allowed(write_only, "/api/watchlist/delete", "POST") is False
    assert _non_admin_permission_allowed(write_only, "/api/playback_progress/actions/mark_watched", "POST") is False
    assert _non_admin_permission_allowed(full, "/api/watchlist/delete", "POST") is True
    assert _non_admin_permission_allowed(full, "/api/playback_progress/actions/mark_watched", "POST") is True


def test_write_permission_still_required_and_sufficient_elsewhere() -> None:
    from crosswatch import _non_admin_permission_allowed

    full = {"permissions": {"write": True, "watchlist": True, "playback": True, "dashboard": True}}
    no_write = {"permissions": {"write": False, "watchlist": True, "playback": True, "dashboard": True}}

    assert _non_admin_permission_allowed(full, "/api/pairs/abc", "PUT") is True
    assert _non_admin_permission_allowed(no_write, "/api/pairs/abc", "PUT") is False
