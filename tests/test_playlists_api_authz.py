from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

from api import playlistsAPI as api
from cw_platform import provider_instances


ALICE_PROFILE_ID = "11111111111141118111111111111111"
BOB_PROFILE_ID = "22222222222242228222222222222222"


def _loads(resp) -> dict[str, Any]:
    return json.loads(resp.body.decode("utf-8"))


def _request(user: dict[str, Any]) -> Any:
    return SimpleNamespace(state=SimpleNamespace(cw_user=user))


def _profile_cfg() -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "plex": {"instances": {"PLEX-P01": {}, "PLEX-P02": {}}},
        "trakt": {"instances": {"TRAKT-P01": {}, "TRAKT-P02": {}}},
        "playlists": {"endpoints": [], "mappings": [], "rulesets": []},
    }
    provider_instances.ensure_provider_instance_uids(cfg)
    provider_instances.upsert_user_profile(
        cfg,
        ALICE_PROFILE_ID,
        label="Alice",
        instances={"PLEX": ["PLEX-P01"], "TRAKT": ["TRAKT-P01"]},
    )
    provider_instances.upsert_user_profile(
        cfg,
        BOB_PROFILE_ID,
        label="Bob",
        instances={"PLEX": ["PLEX-P02"], "TRAKT": ["TRAKT-P02"]},
    )
    return cfg


def test_playlist_activity_is_filtered_by_endpoint_and_mapping_scope(monkeypatch) -> None:
    cfg = _profile_cfg()
    endpoints = [
        {"id": "EP-A1", "name": "AlicePlex", "provider": "PLEX", "instance": "PLEX-P01"},
        {"id": "EP-A2", "name": "AliceTrakt", "provider": "TRAKT", "instance": "TRAKT-P01"},
        {"id": "EP-B1", "name": "BobPlex", "provider": "PLEX", "instance": "PLEX-P02"},
        {"id": "EP-B2", "name": "BobTrakt", "provider": "TRAKT", "instance": "TRAKT-P02"},
    ]
    mappings = [
        {"id": "MAP-A", "source_endpoint": "EP-A1", "target_endpoints": ["EP-A2"]},
        {"id": "MAP-B", "source_endpoint": "EP-B1", "target_endpoints": ["EP-B2"]},
    ]
    activity = [
        {"ts": 4, "type": "Run", "label": "MAP-B - Plex to Trakt", "mapping_id": "MAP-B"},
        {"ts": 3, "type": "Run", "label": "MAP-A - Plex to Trakt", "mapping_id": "MAP-A"},
        {"ts": 2, "type": "Sync", "label": "Plex - BobPlex", "endpoint_id": "EP-B1"},
        {"ts": 1, "type": "Sync", "label": "Plex - AlicePlex", "endpoint_id": "EP-A1"},
    ]

    monkeypatch.setattr(api, "load_config", lambda: cfg)
    monkeypatch.setattr(api.svc, "list_endpoints", lambda _cfg: list(endpoints))
    monkeypatch.setattr(api.svc, "get_mapping", lambda _cfg, mid: next((m for m in mappings if m["id"] == mid), None))
    monkeypatch.setattr(api.runner, "get_endpoint", lambda _cfg, eid: next((e for e in endpoints if e["id"] == eid), None))
    monkeypatch.setattr(api.svc, "activity", lambda _cfg: list(activity))

    data = _loads(api.api_playlist_activity(_request({"is_admin": False, "profile_id": ALICE_PROFILE_ID})))

    assert [row["label"] for row in data["activity"]] == [
        "MAP-A - Plex to Trakt",
        "Plex - AlicePlex",
    ]


def test_managed_users_cannot_read_or_mutate_global_custom_rulesets(monkeypatch) -> None:
    from cw_platform.playlists import BUILTIN_RULESETS, BUILTIN_TRAKT_FREE_ACCOUNT_RULESET_ID

    custom = dict(BUILTIN_RULESETS[BUILTIN_TRAKT_FREE_ACCOUNT_RULESET_ID])
    custom.update({"id": "custom-global", "name": "Custom", "built_in": False})
    cfg = _profile_cfg()
    cfg["playlists"]["rulesets"].append(custom)
    managed = _request({"is_admin": False, "profile_id": ALICE_PROFILE_ID})

    monkeypatch.setattr(api, "load_config", lambda: cfg)
    monkeypatch.setattr(api.svc, "_save", lambda _cfg: None)

    listed = _loads(api.api_playlist_rulesets(managed))
    built_in_get = api.api_playlist_ruleset_get(BUILTIN_TRAKT_FREE_ACCOUNT_RULESET_ID, request=managed)
    custom_get = api.api_playlist_ruleset_get("custom-global", request=managed)
    create = api.api_playlist_ruleset_upsert({"ruleset": custom}, request=managed)
    clone = api.api_playlist_ruleset_clone(BUILTIN_TRAKT_FREE_ACCOUNT_RULESET_ID, {"name": "Clone"}, request=managed)
    delete = api.api_playlist_ruleset_delete("custom-global", request=managed)
    admin_get = api.api_playlist_ruleset_get("custom-global", request=_request({"is_admin": True}))

    assert all(row.get("built_in") for row in listed["rulesets"])
    assert "custom-global" not in {row.get("id") for row in listed["rulesets"]}
    assert built_in_get.status_code == 200
    assert custom_get.status_code == 403
    assert admin_get.status_code == 200
    assert create.status_code == 403
    assert clone.status_code == 403
    assert delete.status_code == 403
    assert [row["id"] for row in cfg["playlists"]["rulesets"]] == ["custom-global"]


def test_managed_mapping_responses_do_not_embed_custom_rulesets(monkeypatch) -> None:
    cfg = _profile_cfg()
    cfg["playlists"]["mappings"] = [
        {"id": "MAP-A", "source_endpoint": "EP-A1", "target_endpoints": ["EP-A2"], "ruleset_id": "custom-global"}
    ]
    mapping = {
        "id": "MAP-A",
        "source_endpoint": "EP-A1",
        "target_endpoints": ["EP-A2"],
        "ruleset_id": "custom-global",
        "ruleset": {"id": "custom-global", "name": "Custom", "built_in": False, "per_endpoint_capacity": 250},
    }
    endpoints = [
        {"id": "EP-A1", "provider": "PLEX", "instance": "PLEX-P01"},
        {"id": "EP-A2", "provider": "TRAKT", "instance": "TRAKT-P01"},
    ]
    managed = _request({"is_admin": False, "profile_id": ALICE_PROFILE_ID})

    monkeypatch.setattr(api, "load_config", lambda: cfg)
    monkeypatch.setattr(api.svc, "list_mappings", lambda _cfg: [dict(mapping)])
    monkeypatch.setattr(api.runner, "get_endpoint", lambda _cfg, eid: next((e for e in endpoints if e["id"] == eid), None))

    data = _loads(api.api_playlist_mappings(managed))

    assert data["mappings"][0]["ruleset_id"] == "custom-global"
    assert "ruleset" not in data["mappings"][0]
