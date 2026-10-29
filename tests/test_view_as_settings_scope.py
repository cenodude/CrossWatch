# tests/test_view_as_settings_scope.py
# CrossWatch - Settings scope and auto-assign while viewing as a profile
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from cw_platform import access_policy

BOB = "22222222222242228222222222222222"
ADMIN = {"id": "administrator", "username": "admin", "is_admin": True}
MANAGED = {"id": "u1", "username": "bob", "is_admin": False, "profile_id": BOB}


def _cfg() -> dict[str, Any]:
    from cw_platform import provider_instances

    cfg: dict[str, Any] = {
        "plex": {"instances": {"PLEX-P01": {}}},
        "simkl": {"instances": {"SIMKL-P01": {}}},
        "user_profiles": {BOB: {"label": "Bob", "instances": {"PLEX": ["PLEX-P01"], "SIMKL": ["SIMKL-P01"]}}},
    }
    provider_instances.ensure_provider_instance_uids(cfg)
    return cfg


def _req(user: dict[str, Any] | None, headers: dict[str, str] | None = None, query: dict[str, str] | None = None) -> Any:
    return SimpleNamespace(state=SimpleNamespace(cw_user=user), headers=headers or {}, query_params=query or {})


def test_view_as_scope_profile_only_for_signed_in_admin() -> None:
    cfg = _cfg()
    assert access_policy.view_as_scope_profile(cfg, _req(ADMIN, {"x-cw-view-as": BOB})) == BOB
    assert access_policy.view_as_scope_profile(cfg, _req(ADMIN, {"x-cw-view-as": BOB}, {"user_profile": "all"})) == ""
    assert access_policy.view_as_scope_profile(cfg, _req(ADMIN, {"x-cw-view-as": "f" * 32})) == ""
    assert access_policy.view_as_scope_profile(cfg, _req(MANAGED, {"x-cw-view-as": BOB})) == ""
    assert access_policy.view_as_scope_profile(cfg, _req(None, {"x-cw-view-as": BOB})) == ""


def test_requested_assign_profile_only_for_signed_in_admin() -> None:
    cfg = _cfg()
    assert access_policy.requested_assign_profile(cfg, _req(ADMIN, {"x-cw-assign-profile": BOB})) == BOB
    assert access_policy.requested_assign_profile(cfg, _req(ADMIN)) == ""
    assert access_policy.requested_assign_profile(cfg, _req(ADMIN, {"x-cw-assign-profile": "f" * 32})) == ""
    assert access_policy.requested_assign_profile(cfg, _req(MANAGED, {"x-cw-assign-profile": BOB})) == ""


def test_instance_scope_follows_view_as_for_admin() -> None:
    import api.providerInstancesAPI as provider_api

    cfg = _cfg()
    assert provider_api._managed_user_instance_scope(cfg, _req(ADMIN)) is None
    scope = provider_api._managed_user_instance_scope(cfg, _req(ADMIN, {"x-cw-view-as": BOB}))
    assert scope == {"PLEX": {"PLEX-P01"}, "SIMKL": {"SIMKL-P01"}}
    assert provider_api._managed_user_instance_scope(cfg, _req(ADMIN, {"x-cw-view-as": BOB}, {"user_profile": "all"})) is None


def test_new_pair_is_assigned_to_viewed_profile() -> None:
    import api.syncAPI as sync_api

    cfg = _cfg()
    item: dict[str, Any] = {"source": "PLEX", "target": "SIMKL"}
    sync_api._apply_pair_profile_scope(cfg, _req(ADMIN, {"x-cw-assign-profile": BOB}), item)
    assert item["profile_id"] == BOB

    plain: dict[str, Any] = {"source": "PLEX", "target": "SIMKL"}
    sync_api._apply_pair_profile_scope(cfg, _req(ADMIN), plain)
    assert "profile_id" not in plain


def test_new_webhook_sinks_are_assigned_to_viewed_profile() -> None:
    import api.scrobblerManagementAPI as scrobbler_api
    from cw_platform.user_profile_resources import webhook_assignment_root, webhook_resource_id

    cfg = _cfg()
    scrobbler_api._assign_webhook_sinks(cfg, access_policy.requested_assign_profile(cfg, _req(ADMIN, {"x-cw-assign-profile": BOB})), "plex", "PLEX-P01", ["simkl"])
    sink_instance = scrobbler_api.webhook_sink_instance(scrobbler_api.webhook_settings(cfg, "plex", "PLEX-P01"), "simkl")
    rid = webhook_resource_id("plex", "PLEX-P01", "simkl", sink_instance)
    assert webhook_assignment_root(cfg)[rid] == BOB

    untouched = _cfg()
    scrobbler_api._assign_webhook_sinks(untouched, access_policy.requested_assign_profile(untouched, _req(ADMIN)), "plex", "PLEX-P01", ["simkl"])
    assert webhook_assignment_root(untouched) == {}
