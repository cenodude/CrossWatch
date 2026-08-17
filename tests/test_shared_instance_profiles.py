from __future__ import annotations

from typing import Any

DAD = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
KID = "cccccccccccccccccccccccccccccccc"
UID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def _cfg() -> dict[str, Any]:
    return {
        "plex": {"server_url": "http://plex:32400", "account_token": "t", "instances": {}},
        "trakt": {"client_id": "c", "client_secret": "s", "instances": {}},
        "provider_instance_ids": {UID: {"provider": "PLEX", "instance": "default"}},
        "user_profiles": {
            DAD: {"label": "Dad", "instance_uids": [UID]},
            KID: {"label": "Kid", "instance_uids": [UID]},
        },
        "scrobble": {
            "watch": {
                "routes": [
                    {"id": "R1", "enabled": True, "provider": "plex", "provider_instance": "default",
                     "sink": "trakt", "sink_instance": "default", "profile_id": DAD,
                     "filters": {"username_whitelist": ["dad"]}},
                    {"id": "R2", "enabled": True, "provider": "plex", "provider_instance": "default",
                     "sink": "trakt", "sink_instance": "default", "profile_id": KID,
                     "filters": {"username_whitelist": ["kid"]}},
                ]
            }
        },
    }


def _stream(account: str) -> dict[str, Any]:
    return {"source": "plex", "provider_instance": "default", "account": account, "title": "Movie"}


def _filter_for(cfg: dict[str, Any], pid: str) -> dict[str, Any]:
    from cw_platform.access_policy import media_account_allowlist_for_profile
    from cw_platform.provider_instances import instances_for_user_profile

    return {"instances": instances_for_user_profile(cfg, pid), "accounts": media_account_allowlist_for_profile(cfg, pid)}


def test_two_profiles_share_one_instance() -> None:
    from cw_platform.provider_instances import user_profiles_for_instance

    owners = {row["id"] for row in user_profiles_for_instance(_cfg(), "PLEX", "default")}
    assert owners == {DAD, KID}


def test_shared_instance_resolves_for_both_profiles() -> None:
    from cw_platform.provider_instances import instances_for_user_profile

    cfg = _cfg()
    assert instances_for_user_profile(cfg, DAD) == {"PLEX": ["default"]}
    assert instances_for_user_profile(cfg, KID) == {"PLEX": ["default"]}


def test_each_profile_sees_only_its_own_card() -> None:
    from api.scrobbleAPI import _currently_watching_matches_user

    cfg = _cfg()
    dad, kid = _filter_for(cfg, DAD), _filter_for(cfg, KID)
    assert _currently_watching_matches_user(_stream("dad"), dad) is True
    assert _currently_watching_matches_user(_stream("kid"), dad) is False
    assert _currently_watching_matches_user(_stream("kid"), kid) is True
    assert _currently_watching_matches_user(_stream("dad"), kid) is False


def test_shared_instance_has_no_derived_owner() -> None:
    from cw_platform.access_policy import route_effective_profile_id

    cfg = _cfg()
    route = {"provider": "plex", "provider_instance": "default"}
    assert route_effective_profile_id(cfg, route) == ""
    assert route_effective_profile_id(cfg, dict(route, profile_id=DAD)) == DAD


def test_sole_owner_still_derives() -> None:
    from cw_platform.access_policy import route_effective_profile_id

    cfg = _cfg()
    cfg["user_profiles"].pop(KID)
    assert route_effective_profile_id(cfg, {"provider": "plex", "provider_instance": "default"}) == DAD


def test_upsert_does_not_steal_instance_from_other_profile() -> None:
    from cw_platform.provider_instances import upsert_user_profile, user_profiles_for_instance

    cfg = _cfg()
    cfg["user_profiles"].pop(KID)
    upsert_user_profile(cfg, KID, label="Kid", instances={"PLEX": ["default"]})
    owners = {row["id"] for row in user_profiles_for_instance(cfg, "PLEX", "default")}
    assert owners == {DAD, KID}


def test_instance_assignment_is_additive() -> None:
    from cw_platform.provider_instances import assign_instance_to_user_profile_id, user_profiles_for_instance

    cfg = _cfg()
    cfg["user_profiles"].pop(KID)
    cfg["user_profiles"][KID] = {"label": "Kid", "instance_uids": []}
    assign_instance_to_user_profile_id(cfg, KID, "PLEX", "default")
    owners = {row["id"] for row in user_profiles_for_instance(cfg, "PLEX", "default")}
    assert owners == {DAD, KID}


def test_unassign_still_clears_every_owner() -> None:
    from cw_platform.provider_instances import assign_instance_to_user_profile, user_profiles_for_instance

    cfg = _cfg()
    assign_instance_to_user_profile(cfg, "", "PLEX", "default")
    assert user_profiles_for_instance(cfg, "PLEX", "default") == []


def test_profile_editor_save_path_allows_sharing() -> None:
    from cw_platform.user_profile_resources import apply_profile_resource_assignments
    from cw_platform.provider_instances import user_profiles_for_instance

    cfg = _cfg()
    cfg["user_profiles"] = {DAD: {"label": "Dad", "instance_uids": [UID]}}
    apply_profile_resource_assignments(
        cfg,
        KID,
        label="Kid",
        manual_instances={"PLEX": ["default"]},
        selected_sync_pairs=[],
        selected_watcher_routes=[],
        selected_webhook_routes=[],
    )
    owners = {row["id"] for row in user_profiles_for_instance(cfg, "PLEX", "default")}
    assert owners == {DAD, KID}


def test_resource_assignment_stays_exclusive() -> None:
    from cw_platform.user_profile_resources import apply_profile_resource_assignments

    cfg = _cfg()
    cfg["scrobble"]["watch"]["routes"][1].pop("profile_id")
    apply_profile_resource_assignments(
        cfg,
        KID,
        label="Kid",
        manual_instances={"PLEX": ["default"]},
        selected_sync_pairs=[],
        selected_watcher_routes=["R1"],
        selected_webhook_routes=[],
    )
    routes = {r["id"]: r.get("profile_id") for r in cfg["scrobble"]["watch"]["routes"]}
    assert routes["R1"] == KID
