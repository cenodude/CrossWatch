# tests/test_overview_default_instances.py
# CrossWatch - Default instance overview scope tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import activityAPI, appAuthAPI, eventsAPI, insightAPI, watchlistAPI
from cw_platform import access_policy, provider_instances
from services.dashboard_widgets import recent_history_widget


SCOPE = provider_instances.DEFAULT_OVERVIEW_SCOPE


def _cfg():
    return {
        "plex": {"instances": {"PLEX-P01": {}}},
        "crosswatch": {"instances": {"CW-P01": {}}},
        "user_profiles": {"alice": {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"]}}},
    }


def _request(user, query=SCOPE, header=""):
    return SimpleNamespace(state=SimpleNamespace(cw_user=user), cookies={},
                           query_params={"user_profile": query}, headers={"x-cw-view-as": header})


def test_default_scope_does_not_create_or_impersonate_a_profile(monkeypatch):
    cfg = _cfg()
    before = deepcopy(cfg)
    scope = provider_instances.instances_for_user_profile(cfg, SCOPE)
    assert scope["PLEX"] == scope["CROSSWATCH"] == scope["TMDB"] == ["default"]
    assert all(values == ["default"] for values in scope.values())
    assert cfg == before
    assert provider_instances.normalize_user_profile_id(SCOPE) == ""
    assert access_policy.valid_user_profile_id(cfg, SCOPE) == ""
    admin = {"id": "administrator", "is_admin": True}
    monkeypatch.setattr("cw_platform.config_base.load_config", lambda: cfg)
    request = _request(admin, header="alice")
    assert access_policy.request_user(request) is admin
    assert access_policy.view_as_scope_profile(cfg, request) == ""
    assert appAuthAPI.effective_request_profile_id(cfg, request, SCOPE) == SCOPE
    assert appAuthAPI.effective_user_profile_id(cfg, None, SCOPE) == SCOPE


def test_managed_users_cannot_select_default_scope(monkeypatch):
    cfg = _cfg()
    managed = {"is_admin": False, "profile_id": "alice"}
    monkeypatch.setattr(appAuthAPI, "auth_required", lambda cfg: True)
    monkeypatch.setattr(appAuthAPI, "current_user", lambda cfg, token: managed)
    assert appAuthAPI.effective_user_profile_id(cfg, "managed", SCOPE) == "alice"
    request = _request(managed)
    assert appAuthAPI.effective_request_profile_id(cfg, request, SCOPE) == "alice"
    profile, scope = watchlistAPI._effective_user_filter(cfg, request, SCOPE)
    assert profile == "alice"
    assert scope == {"PLEX": ["PLEX-P01"]}


def test_status_pairs_include_only_default_endpoints():
    pairs = [
        {"id": "default", "source": "PLEX", "target": "CROSSWATCH"},
        {"id": "mixed", "source": "PLEX", "target": "CROSSWATCH", "target_instance": "CW-P01"},
        {"id": "named", "source": "PLEX", "source_instance": "PLEX-P01", "target": "CROSSWATCH", "target_instance": "CW-P01"},
    ]
    assert [row["id"] for row in access_policy.filter_pairs_for_profile(_cfg(), SCOPE, pairs)] == ["default"]
    assert len(access_policy.filter_pairs_for_profile(_cfg(), "", pairs)) == 3


def test_default_scope_filters_activity_and_history(monkeypatch):
    monkeypatch.setattr(activityAPI, "load_config", _cfg)
    items = [
        {"source": "PLEX", "source_instance": "default", "title": "Admin"},
        {"source": "PLEX", "source_instance": "PLEX-P01", "title": "Alice"},
    ]
    payload = activityAPI._apply_user_profile({"items": items, "total": 2}, SCOPE)
    assert payload["items"] == [items[0]]
    assert payload["total"] == 1
    lane = lambda title, key: {"history": {"baseline": {"items": {
        key: {"type": "movie", "title": title, "ids": {"tmdb": key}, "watched_at": 1767225600}
    }}}}
    state = {"providers": {"PLEX": {**lane("Admin", "1"), "instances": {"PLEX-P01": lane("Alice", "2")}}}}
    result = recent_history_widget(state, user_filter=provider_instances.instances_for_user_profile(_cfg(), SCOPE))
    assert result["total"] == 1
    assert [item["title"] for item in result["items"]] == ["Admin"]


def test_default_scope_filters_event_details_and_calendar(monkeypatch):
    cfg = _cfg()
    cfg["pairs"] = [
        {"id": "own", "source": "PLEX", "target": "CROSSWATCH"},
        {"id": "other", "source": "PLEX", "source_instance": "PLEX-P01", "target": "CROSSWATCH", "target_instance": "CW-P01"},
    ]
    request = _request({"is_admin": True})
    items = [
        {"source_provider": "PLEX", "source_instance": "default"},
        {"source_provider": "PLEX", "source_instance": "PLEX-P01"},
    ]
    payload = eventsAPI._filter_event_payload(cfg, request, {"items": items, "total": 2})
    assert payload["items"] == [items[0]]
    assert payload["total"] == 1
    calendar = eventsAPI._calendar_scope(cfg, request)
    assert not calendar.unrestricted
    assert calendar.pair_ids == ["own"]
    assert eventsAPI._calendar_scope(cfg, _request({"is_admin": True}, "all")).unrestricted
    monkeypatch.setattr(eventsAPI, "load_config", lambda: cfg)
    monkeypatch.setattr(eventsAPI, "_scoped_statistics", lambda *args: {"scoped": True})
    response = eventsAPI.events_statistics(range="30d", since=1, until=2, bucket=None, request=request)
    assert response.body == b'{"scoped":true}'
    assert not eventsAPI._default_overview_requested(_request({"is_admin": False, "profile_id": "alice"}))


def test_insights_default_totals_exclude_named_instances(monkeypatch):
    cfg = _cfg()
    lane = lambda title: {"watchlist": {"baseline": {"items": {title: {"type": "movie", "title": title}}}}}
    state = {"providers": {"PLEX": {**lane("Admin"), "instances": {"PLEX-P01": lane("Alice")}}}}
    wall = [
        {"key": "admin", "type": "movie", "sources_by_provider": {"plex": ["default"]}},
        {"key": "alice", "type": "movie", "sources_by_provider": {"plex": ["PLEX-P01"]}},
    ]
    events = [
        {"feature": "watchlist", "action": "add", "source": "PLEX", "source_instance": "default", "key": "admin"},
        {"feature": "watchlist", "action": "add", "source": "PLEX", "source_instance": "PLEX-P01", "key": "alice"},
    ]
    cw = SimpleNamespace(STATS=SimpleNamespace(data={"samples": [], "events": events}), REPORT_DIR=None,
                         CACHE_DIR=None, _load_wall_snapshot=lambda: wall, _append_log=lambda *args: None)
    monkeypatch.setattr(insightAPI, "_env", lambda: (cw, lambda: cfg, lambda cfg: None, lambda *a, **k: None))
    monkeypatch.setattr(insightAPI, "_load_state_features", lambda features: state)
    app = FastAPI()
    insightAPI.register_insights(app)
    client = TestClient(app)
    url = "/api/insights?limit_samples=5&history=0"
    all_data = client.get(url).json()
    own = client.get(f"{url}&user_profile={SCOPE}").json()
    assert all_data["features"]["watchlist"]["providers"]["plex"] == 2
    assert own["user_profile"] == SCOPE
    assert own["features"]["watchlist"]["providers"]["plex"] == 1
    assert own["instances_by_provider"]["plex"] == ["default"]
    assert own["watchtime"]["movies"] == 1
    assert [event["key"] for event in own["events"]] == ["admin"]
