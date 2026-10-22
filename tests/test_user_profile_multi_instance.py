# tests/test_user_profile_multi_instance.py
# CrossWatch - User profiles holding several connections of one provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

from cw_platform import provider_instances as pi
from cw_platform.access_policy import profile_allows_pair, profile_instances_map
from cw_platform.user_profile_resources import apply_profile_resource_assignments

PID = "user1"


def _cfg() -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "emby": {"instances": {"E1-USER1": {}, "E2-USER1": {}}},
        "jellyfin": {"instances": {"J1-USER1": {}, "J2-USER1": {}}},
        "user_profiles": {PID: {"label": "User1", "instance_uids": []}},
        "pairs": [
            {"id": "p-emby", "source": "EMBY", "source_instance": "E1-USER1", "target": "EMBY", "target_instance": "E2-USER1"},
            {"id": "p-jf", "source": "EMBY", "source_instance": "E1-USER1", "target": "JELLYFIN", "target_instance": "J2-USER1"},
        ],
    }
    pi.ensure_provider_instance_uids(cfg)
    return cfg


def test_upsert_keeps_every_instance_of_a_provider() -> None:
    cfg = _cfg()
    pi.upsert_user_profile(cfg, PID, label="User1", instances={"EMBY": ["E1-USER1", "E2-USER1"], "JELLYFIN": ["J1-USER1", "J2-USER1"]})
    assert pi.instances_for_user_profile(cfg, PID) == {"EMBY": ["E1-USER1", "E2-USER1"], "JELLYFIN": ["J1-USER1", "J2-USER1"]}


def test_assign_adds_instead_of_replacing_same_provider() -> None:
    cfg = _cfg()
    pi.assign_instance_to_user_profile_id(cfg, PID, "EMBY", "E1-USER1")
    pi.assign_instance_to_user_profile_id(cfg, PID, "EMBY", "E2-USER1")
    assert sorted(pi.instances_for_user_profile(cfg, PID)["EMBY"]) == ["E1-USER1", "E2-USER1"]


def test_remove_drops_only_that_instance() -> None:
    cfg = _cfg()
    pi.upsert_user_profile(cfg, PID, label="User1", instances={"EMBY": ["E1-USER1", "E2-USER1"]})
    pi.remove_instance_from_user_profiles(cfg, "EMBY", "E2-USER1")
    assert pi.instances_for_user_profile(cfg, PID) == {"EMBY": ["E1-USER1"]}


def test_same_provider_pair_assignment_grants_both_endpoints() -> None:
    cfg = _cfg()
    apply_profile_resource_assignments(
        cfg,
        PID,
        label="User1",
        manual_instances={},
        selected_sync_pairs=["p-emby", "p-jf"],
        selected_watcher_routes=[],
        selected_webhook_routes=[],
    )
    scope = profile_instances_map(cfg, PID)
    assert scope == {"EMBY": ["E1-USER1", "E2-USER1"], "JELLYFIN": ["J2-USER1"]}
    for pair in cfg["pairs"]:
        assert profile_allows_pair(scope, pair) is True


def test_manual_instances_merge_with_required_ones() -> None:
    cfg = _cfg()
    profile = apply_profile_resource_assignments(
        cfg,
        PID,
        label="User1",
        manual_instances={"JELLYFIN": ["J1-USER1"]},
        selected_sync_pairs=["p-jf"],
        selected_watcher_routes=[],
        selected_webhook_routes=[],
    )
    assert profile["instances"] == {"EMBY": ["E1-USER1"], "JELLYFIN": ["J1-USER1", "J2-USER1"]}
    assert profile["manual_instances"] == {"JELLYFIN": ["J1-USER1"]}


def test_export_all_for_managed_user_covers_every_allowed_instance(monkeypatch) -> None:
    from services import export

    monkeypatch.setattr(export, "request_user", lambda request: {"is_admin": False, "profile_id": PID})
    monkeypatch.setattr(export, "managed_profile_instances", lambda cfg, user: {"EMBY": ["E1-USER1", "E2-USER1"], "JELLYFIN": ["J1-USER1"]})
    assert export._export_instance_for_request({}, None, "EMBY", "all") == ("E1-USER1", "E2-USER1")
    assert export._export_instance_for_request({}, None, "JELLYFIN", "all") == "J1-USER1"


def test_export_bucket_merges_only_scoped_instances() -> None:
    from services import export

    def block(key: str) -> dict[str, Any]:
        return {"watchlist": {"baseline": {"items": {key: {"title": key}}}}}

    state = {"providers": {"EMBY": {**block("default-item"), "instances": {"E1-USER1": block("e1-item"), "E2-USER1": block("e2-item"), "E1-USER2": block("other-item")}}}}
    bucket = export._items_bucket(state, "EMBY", "watchlist", instance_id=("E1-USER1", "E2-USER1"))
    assert sorted(bucket) == ["e1-item", "e2-item"]
