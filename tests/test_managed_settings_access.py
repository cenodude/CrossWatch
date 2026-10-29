# tests/test_managed_settings_access.py
# CrossWatch - Settings access for managed users (pairs, scrobbler, schedule, connections)
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from cw_platform.access_policy import (
    clean_managed_permissions,
    managed_reconnect_allowed,
    managed_settings_access,
    reconnect_payload,
    reconnect_route_provider,
    user_can_access_instance,
)

BOB = "22222222222242228222222222222222"
ALICE = "11111111111141118111111111111111"


def _user(**perms: Any) -> dict[str, Any]:
    return {"id": "u1", "is_admin": False, "profile_id": BOB, "permissions": perms}


def test_full_access_gets_every_settings_grant_by_default() -> None:
    perms = clean_managed_permissions({"write": True})
    assert (perms["pairs"], perms["scrobbler"], perms["schedule"], perms["connections"]) == (True, True, True, True)
    assert managed_settings_access(_user(write=True)) is True


def test_grants_can_be_switched_off_one_by_one() -> None:
    perms = clean_managed_permissions({"write": True, "scrobbler": False})
    assert (perms["pairs"], perms["scrobbler"], perms["schedule"], perms["connections"]) == (True, False, True, True)
    assert managed_settings_access(_user(write=True, pairs=False, scrobbler=False, schedule=False)) is True
    assert managed_settings_access(_user(write=True, pairs=False, scrobbler=False, schedule=False, connections=False)) is False


def test_read_only_and_monitor_never_get_settings() -> None:
    for raw in ({"write": False}, {"write": True, "monitor": True}):
        perms = clean_managed_permissions(raw)
        assert not any(perms[key] for key in ("pairs", "scrobbler", "schedule", "connections"))
    assert managed_settings_access({"is_admin": True}) is False


def test_saved_config_keeps_switched_off_grants() -> None:
    from cw_platform.config_base import _normalize_app_auth

    cfg: dict[str, Any] = {
        "app_auth": {
            "users": {
                "a" * 32: {
                    "username": "bob",
                    "profile_id": BOB,
                    "permissions": {"write": True, "pairs": False, "schedule": True, "connections": False},
                    "password": {"scheme": "pbkdf2_sha256", "iterations": 1, "salt": "s", "hash": "h"},
                }
            }
        }
    }
    _normalize_app_auth(cfg)
    perms = cfg["app_auth"]["users"]["a" * 32]["permissions"]
    assert perms["pairs"] is False
    assert perms["connections"] is False
    assert "schedule" not in perms
    assert clean_managed_permissions(perms)["schedule"] is True


@pytest.mark.parametrize(
    ("path", "method", "grant"),
    [
        ("/api/pairs", "POST", "pairs"),
        ("/api/pairs/pair_1", "PUT", "pairs"),
        ("/api/scrobbler/routes", "POST", "scrobbler"),
        ("/api/scrobbler/overview", "GET", "scrobbler"),
        ("/api/scheduling/profiles/" + BOB, "POST", "schedule"),
        ("/api/auth/providers/html", "GET", "connections"),
    ],
)
def test_middleware_checks_each_grant(path: str, method: str, grant: str) -> None:
    from crosswatch import _non_admin_permission_allowed

    assert _non_admin_permission_allowed(_user(write=True), path, method) is True
    assert _non_admin_permission_allowed(_user(write=True, **{grant: False}), path, method) is False
    assert _non_admin_permission_allowed(_user(write=False), path, method) is False


def test_only_scoped_scrobbler_and_schedule_endpoints_open_to_managed_users() -> None:
    from api.appAuthAPI import non_admin_api_allowed

    assert non_admin_api_allowed("/api/scrobbler/routes", "POST")
    assert non_admin_api_allowed("/api/scrobbler/routes/R1", "DELETE")
    assert non_admin_api_allowed("/api/scrobbler/webhooks/profile", "POST")
    assert non_admin_api_allowed("/api/scheduling/profiles/" + BOB, "POST")
    assert not non_admin_api_allowed("/api/scrobbler/settings", "POST")
    assert not non_admin_api_allowed("/api/scrobbler/webhooks/cleanup-legacy", "POST")
    assert not non_admin_api_allowed("/api/scheduling", "POST")
    assert not non_admin_api_allowed("/api/config", "POST")


def _cfg() -> dict[str, Any]:
    from cw_platform import provider_instances

    cfg: dict[str, Any] = {
        "plex": {"instances": {"PLEX-P01": {}, "PLEX-P02": {}}},
        "simkl": {"instances": {"SIMKL-P01": {}}},
        "user_profiles": {
            BOB: {"label": "Bob", "instances": {"PLEX": ["PLEX-P01", "PLEX-P02"], "SIMKL": ["SIMKL-P01"]}},
            ALICE: {"label": "Alice", "instances": {"PLEX": ["PLEX-P02"]}},
        },
    }
    provider_instances.ensure_provider_instance_uids(cfg)
    return cfg


def test_route_scope_rejects_instances_outside_the_profile() -> None:
    import api.scrobblerManagementAPI as scrobbler_api

    cfg = _cfg()
    user = _user(write=True)
    scrobbler_api._require_route_scope(cfg, user, {"provider": "plex", "provider_instance": "PLEX-P01", "sink": "simkl", "sink_instance": "SIMKL-P01"})
    with pytest.raises(scrobbler_api.ValidationFailure):
        scrobbler_api._require_route_scope(cfg, user, {"provider": "plex", "provider_instance": "default", "sink": "simkl", "sink_instance": "SIMKL-P01"})
    scrobbler_api._require_route_scope(cfg, None, {"provider": "plex", "provider_instance": "default", "sink": "simkl", "sink_instance": "default"})


def test_webhook_on_a_shared_media_instance_stays_with_the_admin() -> None:
    import api.scrobblerManagementAPI as scrobbler_api

    cfg = _cfg()
    user = _user(write=True)
    scrobbler_api._require_own_webhook_source(cfg, user, "plex", "PLEX-P01")
    with pytest.raises(scrobbler_api.ValidationFailure):
        scrobbler_api._require_own_webhook_source(cfg, user, "plex", "PLEX-P02")


def test_overview_lists_only_the_users_own_instances() -> None:
    import api.scrobblerManagementAPI as scrobbler_api

    cfg = _cfg()
    groups = [{"provider": "plex", "profiles": [{"instance": "default"}, {"instance": "PLEX-P01"}]}]
    scoped = scrobbler_api._scoped_profile_groups(cfg, _user(write=True), groups)
    assert [p["instance"] for p in scoped[0]["profiles"]] == ["PLEX-P01"]
    assert scrobbler_api._scoped_profile_groups(cfg, {"is_admin": True}, groups) is groups


def test_schedule_endpoints_only_serve_the_users_own_profile(monkeypatch) -> None:
    import api.schedulingAPI as scheduling_api

    request = SimpleNamespace(state=SimpleNamespace(cw_user=_user(write=True)), headers={}, query_params={})
    assert scheduling_api._own_profile_only(request) == BOB
    admin = SimpleNamespace(state=SimpleNamespace(cw_user={"is_admin": True}), headers={}, query_params={})
    assert scheduling_api._own_profile_only(admin) is None


def test_settings_page_contains_only_granted_panes() -> None:
    import ui_frontend

    def panes(user: dict[str, Any]) -> list[str]:
        html = ui_frontend.get_index_html(include_admin=False, user=user)
        return [p for p in ("overview", "providers", "sync", "scheduling", "scrobbler", "app", "maintenance") if f'class="cw-settings-pane' in html and f'data-pane="{p}">' in html]

    assert panes(_user(write=True)) == ["providers", "sync", "scheduling", "scrobbler"]
    assert panes(_user(write=True, pairs=False, scrobbler=False, connections=False)) == ["scheduling"]
    assert panes(_user(write=True, pairs=False, scrobbler=False, schedule=False)) == ["providers"]
    assert panes(_user(write=True, pairs=False, scrobbler=False, schedule=False, connections=False)) == []
    assert panes(_user(write=False)) == []
    html = ui_frontend.get_index_html(include_admin=False, user=_user(write=True))
    assert "app_user_manager" not in html
    assert "app_auth_oidc_client_secret" not in html
    assert 'id="save-fab"' not in html


def test_reconnect_routes_map_to_their_provider() -> None:
    assert reconnect_route_provider("/api/plex/pin/new", "POST") == "plex"
    assert reconnect_route_provider("/api/plex/pin/new?instance=PLEX-P01", "POST") == "plex"
    assert reconnect_route_provider("/api/plex/pin/new", "GET") == ""
    assert reconnect_route_provider("/api/plex/token/delete", "POST") == ""
    assert reconnect_route_provider("/api/config", "POST") == ""


def test_reconnect_only_on_own_named_instances() -> None:
    cfg = _cfg()
    user = _user(write=True)
    assert managed_reconnect_allowed(cfg, user, "plex", "PLEX-P01") is True
    assert managed_reconnect_allowed(cfg, user, "plex", "default") is False
    assert managed_reconnect_allowed(cfg, user, "plex", "") is False
    assert managed_reconnect_allowed(cfg, user, "plex", "PLEX-P09") is False
    assert managed_reconnect_allowed(cfg, _user(write=True, connections=False), "plex", "PLEX-P01") is False
    assert managed_reconnect_allowed(cfg, _user(write=False), "plex", "PLEX-P01") is False
    assert managed_reconnect_allowed(cfg, {"is_admin": True}, "plex", "PLEX-P01") is False


def test_managed_api_gate_sends_reconnect_routes_through_instance_check() -> None:
    from crosswatch import _managed_api_allowed

    cfg = _cfg()
    user = _user(write=True)

    def req(method: str, instance: str | None = None) -> Any:
        return SimpleNamespace(method=method, query_params={"instance": instance} if instance else {})

    assert _managed_api_allowed(cfg, user, req("POST", "PLEX-P01"), "/api/plex/pin/new") is True
    assert _managed_api_allowed(cfg, user, req("POST"), "/api/plex/pin/new") is False
    assert _managed_api_allowed(cfg, user, req("POST", "PLEX-P01"), "/api/plex/token/delete") is False
    assert _managed_api_allowed(cfg, user, req("GET"), "/api/auth/providers/html") is True
    assert _managed_api_allowed(cfg, _user(write=True, connections=False), req("GET"), "/api/auth/providers/html") is False


def test_shared_instances_are_admin_only_for_every_reconnect_route() -> None:
    from crosswatch import _managed_api_allowed

    cfg = _cfg()
    user = _user(write=True)
    assert managed_reconnect_allowed(cfg, user, "plex", "PLEX-P02") is False
    for path, method in (("/api/plex/pin/new", "POST"), ("/api/tmdb_sync/verify", "GET"), ("/api/jellyfin/quickconnect/poll", "GET")):
        request = SimpleNamespace(method=method, query_params={"instance": "PLEX-P02"})
        assert _managed_api_allowed(cfg, user, request, path) is False


def test_default_instance_cannot_be_reached_through_an_alias() -> None:
    from cw_platform import provider_instances

    cfg: dict[str, Any] = {
        "plex": {"account_token": "t", "instances": {"PLEX-P01": {}}},
        "user_profiles": {BOB: {"label": "Bob", "instances": {"PLEX": ["default", "PLEX-P01"]}}},
    }
    provider_instances.ensure_provider_instance_uids(cfg)
    user = _user(write=True)
    assert user_can_access_instance(cfg, user, "plex", "default") is True
    for alias in ("default", "Default", "default:1", " default "):
        assert managed_reconnect_allowed(cfg, user, "plex", alias) is False
    assert managed_reconnect_allowed(cfg, user, "plex", "PLEX-P01:1") is True


def test_reconnect_cannot_change_connection_settings() -> None:
    body = {"server": "http://evil.local", "username": "bob", "password": "pw", "verify_ssl": False}
    managed = SimpleNamespace(state=SimpleNamespace(cw_user=_user(write=True)))
    admin = SimpleNamespace(state=SimpleNamespace(cw_user={"is_admin": True}))
    assert reconnect_payload(managed, "/api/jellyfin/login", body) == {"username": "bob", "password": "pw"}
    assert reconnect_payload(managed, "/api/trakt/pin/new", {"client_id": "x", "client_secret": "y", "auth_method": "pin"}) == {"auth_method": "pin"}
    assert reconnect_payload(admin, "/api/jellyfin/login", body) == body
    assert reconnect_payload(managed, "/api/plex/pin/new", None) is None
    assert reconnect_payload(managed, "/api/silo/device/start", {"server_url": "http://x", "verify_ssl": False}) == {}
    assert reconnect_payload(managed, "/api/flicklist/device/start", {"client_id": "x", "credential": "key"}) == {"credential": "key"}
    assert reconnect_payload(managed, "/api/tmdb_sync/connect/start", {"api_key": "k", "read_token": "t"}) == {}
    assert reconnect_payload(managed, "/api/tmdb_sync/connect/finish", {"api_key": "k", "request_token": "r"}) == {"request_token": "r"}


def test_overview_runtime_only_shows_own_routes() -> None:
    import api.scrobblerManagementAPI as scrobbler_api

    runtime = {
        "running": True,
        "groups": [{"id": "g1", "provider": "plex", "running": True, "routes": ["R1", "R2"]}, {"id": "g2", "provider": "emby", "running": True, "routes": ["R3"]}],
        "routes": [{"id": "R1", "sink": "simkl", "running": True}, {"id": "R2", "sink": "trakt", "running": True}, {"id": "R3", "sink": "trakt", "running": True}],
    }
    scoped = scrobbler_api._scoped_runtime(runtime, {"R1"})
    assert [r["id"] for r in scoped["routes"]] == ["R1"]
    assert scoped["groups"] == [{"id": "g1", "provider": "plex", "running": True, "routes": ["R1"]}]
    assert (scoped["running_route_count"], scoped["active_sinks"], scoped["active_source_providers"]) == (1, ["simkl"], ["plex"])


def test_overview_hides_the_global_ratings_webhook_from_managed_users(monkeypatch) -> None:
    import api.scrobblerManagementAPI as scrobbler_api

    cfg = _cfg()
    cfg["security"] = {"webhook_ids": {"plexwatcher": "secret-token"}}
    monkeypatch.setattr(scrobbler_api, "_runtime_status", lambda request, cfg: {"routes": [], "groups": []})
    monkeypatch.setattr(scrobbler_api, "_webhook_cards", lambda cfg, request: [])
    monkeypatch.setattr(scrobbler_api, "_base_url", lambda request: "http://cw")

    def overview(user: dict[str, Any]) -> dict[str, Any]:
        request = SimpleNamespace(state=SimpleNamespace(cw_user=user), headers={}, query_params={})
        return scrobbler_api.build_overview(cfg, request)["source_state"]["global_plex_ratings"]

    assert overview({"is_admin": True})["endpoint_url"].endswith("secret-token")
    assert overview(_user(write=True))["endpoint_url"] == ""


@pytest.mark.parametrize(
    "replacement",
    [
        '<section class="cw-settings-pane cw-extra" data-pane="scrobbler">',
        "<section class='cw-settings-pane' data-pane='scrobbler'>",
        '<div class="cw-settings-pane" data-pane="scrobbler">',
        '<section data-pane="scrobbler" class="cw-settings-section">',
    ],
)
def test_settings_shell_fails_closed_when_a_denied_pane_drifts(replacement: str) -> None:
    import ui_frontend

    user = {"permissions": {"write": True, "dashboard": True, "scrobbler": False}}
    original = '<section class="cw-settings-pane" data-pane="scrobbler">'
    source = ui_frontend._get_index_html_static()
    assert original in source
    with pytest.raises(RuntimeError, match="settings pane scrobbler"):
        ui_frontend._managed_user_shell(source.replace(original, replacement, 1), user)


def test_settings_shell_keeps_only_granted_panes() -> None:
    import ui_frontend

    user = {"permissions": {"write": True, "dashboard": True, "scrobbler": False}}
    html = ui_frontend._managed_user_shell(ui_frontend._get_index_html_static(), user)
    assert ui_frontend._remaining_settings_panes(html) == {"providers", "sync", "scheduling"}


@pytest.mark.parametrize("path", ["/api/pairs", "/api/sync/providers", "/api/sync/providers/counts"])
def test_pair_settings_reads_follow_the_pairs_grant(path: str) -> None:
    from crosswatch import _non_admin_permission_allowed

    assert _non_admin_permission_allowed(_user(write=True, dashboard=False), path, "GET") is True
    assert _non_admin_permission_allowed(_user(write=True, dashboard=False, pairs=False), path, "GET") is False
    assert _non_admin_permission_allowed(_user(write=False, dashboard=True), path, "GET") is True
    assert _non_admin_permission_allowed(_user(write=True, dashboard=False), "/api/status", "GET") is False


def test_provider_counts_only_cover_the_users_own_instances(monkeypatch) -> None:
    import api.syncAPI as sync_api
    from cw_platform.orchestrator import _state_store

    cfg = _cfg()
    seen: list[Any] = []

    def fake_counts(self: Any, feature: str = "watchlist", *, include_sync_users: bool = False, instances: Any = None) -> dict[str, int]:
        seen.append(instances)
        return {"PLEX": 7} if instances else {"PLEX": 99, "EMBY": 42}

    monkeypatch.setattr(_state_store.StateStore, "provider_feature_counts", fake_counts)
    monkeypatch.setattr(sync_api, "_env", lambda: (lambda: cfg, lambda c: None))
    monkeypatch.setattr(sync_api, "_provider_counts_current", lambda: {"PLEX": 99, "EMBY": 42})

    def counts(user: dict[str, Any]) -> dict[str, int]:
        return sync_api.api_provider_counts(request=SimpleNamespace(state=SimpleNamespace(cw_user=user), headers={}, query_params={}))

    mine = counts(_user(write=True, dashboard=False))
    assert mine["PLEX"] == 7 and mine.get("EMBY", 0) == 0
    assert set(seen[-1]) == {"PLEX", "SIMKL"}
    assert counts({"is_admin": True}) == {"PLEX": 99, "EMBY": 42}
    nobody = {"id": "u2", "is_admin": False, "profile_id": "", "permissions": {"write": True}}
    calls = len(seen)
    assert all(value == 0 for value in counts(nobody).values())
    assert len(seen) == calls


def test_local_tracker_connect_is_a_reconnect_route_and_listed_for_the_ui() -> None:
    import ui_frontend
    from cw_platform.access_policy import reconnect_connection_keys

    assert reconnect_route_provider("/api/crosswatch/connect", "POST") == "crosswatch"
    assert reconnect_route_provider("/api/crosswatch/disconnect", "POST") == ""
    keys = reconnect_connection_keys()
    assert {"CROSSWATCH", "TMDB", "PLEX"} <= set(keys)
    assert not {"TAUTULLI", "TRACEARR", "TMDB_SYNC"} & set(keys)
    for html in (
        ui_frontend.get_index_html(include_admin=True, user={"is_admin": True}),
        ui_frontend.get_index_html(include_admin=False, user=_user(write=True)),
    ):
        assert f'data-cw-reconnect="{" ".join(keys)}"' in html
