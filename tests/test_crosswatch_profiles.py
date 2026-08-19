# tests/test_crosswatch_profiles.py
# CrossWatch - Local tracker profile tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
import io
import json
import zipfile

import api.providerInstancesAPI as provider_api
import api.activityAPI as activity_api
import api.editorAPI as editor_api
import api.eventsAPI as events_api
import api.insightAPI as insight_api
import api.authenticationAPI as auth_api
import api.maintenanceAPI as maintenance_api
import api.playbackProgressAPI as playback_api
import api.scrobbleAPI as scrobble_api
import services.editor as editor_service
import services.export as export_service
import cw_platform.tracker_storage as tracker_storage
from cw_platform.orchestrator._state_store import StateStore
import cw_platform.provider_instances as provider_instances
from cw_platform.provider_instances import build_pair_config_view, get_provider_block
from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

ALICE_PROFILE_ID = "11111111111141118111111111111111"
BOB_PROFILE_ID = "22222222222242228222222222222222"
FRANK_PROFILE_ID = "33333333333343338333333333333333"
PROVIDER_INSTANCE_UID = "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"


def _fake_update(loader, saver):
    def _update(mutator):
        cfg = loader()
        result = mutator(cfg)
        saver(cfg)
        return cfg, result

    return _update


def _admin_req():
    from tests.test_app_auth_api import _request

    return _request("/api/user-profiles")


def _loads_body(body: bytes | memoryview[int]) -> Any:
    return json.loads(bytes(body))


def _provider_key(provider: str) -> str:
    key = provider_instances.provider_key(provider)
    return "tmdb_sync" if key == "tmdb" else key


def _configured_refs(cfg: dict[str, Any], refs: list[tuple[str, str]]) -> dict[str, Any]:
    for provider, instance in refs:
        key = _provider_key(provider)
        block = cfg.setdefault(key, {})
        if not isinstance(block, dict):
            block = {}
            cfg[key] = block
        if instance != "default":
            block.setdefault("instances", {}).setdefault(instance, {})
    provider_instances.ensure_provider_instance_uids(cfg)
    return cfg


def _profile_instances(cfg: dict[str, Any], profile_id: str) -> dict[str, list[str]]:
    for row in provider_instances.list_user_profiles(cfg):
        if row.get("id") == profile_id:
            return dict(row.get("instances") or {})
    return {}


def _currently_watching_state() -> dict[str, Any]:
    return {
        "v": 2,
        "streams": {
            "alice": {
                "source": "plex",
                "provider_instance": "PLEX-P01",
                "state": "playing",
                "title": "Alice Movie",
                "updated": 100,
            },
            "bob": {
                "source": "plex",
                "provider_instance": "PLEX-P02",
                "state": "playing",
                "title": "Bob Movie",
                "updated": 200,
            },
        },
    }


def test_crosswatch_next_profile_uses_cw_prefix_and_label(monkeypatch) -> None:
    store: dict[str, Any] = {"crosswatch": {"root_dir": "/config/.cw_provider"}}

    def fake_load() -> dict[str, Any]:
        return store

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(provider_api, "load_config", fake_load)
    monkeypatch.setattr(provider_api, "save_config", fake_save)
    monkeypatch.setattr(provider_api, "update_config", _fake_update(provider_api.load_config, fake_save))
    monkeypatch.setattr(provider_api, "_invalidate_provider_cache", lambda provider: None)
    monkeypatch.setattr(provider_instances, "generate_provider_instance_uid", lambda _cfg: PROVIDER_INSTANCE_UID)

    res = provider_api.api_provider_instances_create_next("crosswatch", {"label": "Living Room Tracker"}, request=_admin_req())

    assert res == {"ok": True, "id": "CW-P01", "uid": PROVIDER_INSTANCE_UID}
    block = store["crosswatch"]["instances"]["CW-P01"]
    assert block["label"] == "Living Room"
    assert block["root_dir"] == "/config/.cw_provider/profiles/CW-P01"
    assert store["provider_instance_ids"][PROVIDER_INSTANCE_UID] == {"provider": "CROSSWATCH", "instance": "CW-P01"}


def test_provider_instance_update_sets_label_and_user_profile(monkeypatch) -> None:
    store: dict[str, Any] = {
        "jellyfin": {"instances": {"JELLYFIN-P01": {"server": "http://jf"}}},
        "crosswatch": {"instances": {"CW-P01": {"connected": True}}},
    }

    def fake_load() -> dict[str, Any]:
        return json.loads(json.dumps(store))

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(provider_api, "load_config", fake_load)
    monkeypatch.setattr(provider_api, "save_config", fake_save)
    monkeypatch.setattr(provider_api, "update_config", _fake_update(provider_api.load_config, fake_save))
    monkeypatch.setattr(provider_api, "_invalidate_provider_cache", lambda provider: None)
    monkeypatch.setattr(provider_instances, "generate_user_profile_id", lambda _cfg: ALICE_PROFILE_ID)

    res = provider_api.api_provider_instances_update(
        "jellyfin",
        "JELLYFIN-P01",
        {"label": "Alice JF", "user_profile_label": "Alice"},
     request=_admin_req())
    provider_api.api_provider_instances_update("crosswatch", "CW-P01", {"label": "Alice CW", "user_profile_label": "Alice"}, request=_admin_req())

    assert res["ok"] is True
    assert store["jellyfin"]["instances"]["JELLYFIN-P01"]["label"] == "Alice JF"
    assert store["user_profiles"][ALICE_PROFILE_ID]["label"] == "Alice"
    assert "instances" not in store["user_profiles"][ALICE_PROFILE_ID]
    assert len(store["user_profiles"][ALICE_PROFILE_ID]["instance_uids"]) == 2
    assert _profile_instances(store, ALICE_PROFILE_ID) == {
        "CROSSWATCH": ["CW-P01"],
        "JELLYFIN": ["JELLYFIN-P01"],
    }

    rows = provider_api.api_provider_instances_provider("jellyfin").body
    data = _loads_body(rows)
    row = next(item for item in data if item["id"] == "JELLYFIN-P01")

    assert row["friendly_label"] == "Alice JF"
    assert row["display_label"] == "Alice JF"
    assert row["user_profile_id"] == ALICE_PROFILE_ID
    assert row["user_profile_label"] == "Alice"


def test_provider_instance_update_by_user_profile_id_preserves_label(monkeypatch) -> None:
    store: dict[str, Any] = {
        "jellyfin": {"instances": {"JELLYFIN-P01": {"server": "http://jf"}}},
        "user_profiles": {ALICE_PROFILE_ID: {"label": "Alice Smith", "instances": {}}},
    }

    def fake_load() -> dict[str, Any]:
        return json.loads(json.dumps(store))

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(provider_api, "load_config", fake_load)
    monkeypatch.setattr(provider_api, "save_config", fake_save)
    monkeypatch.setattr(provider_api, "update_config", _fake_update(provider_api.load_config, fake_save))
    monkeypatch.setattr(provider_api, "_invalidate_provider_cache", lambda provider: None)

    res = provider_api.api_provider_instances_update("jellyfin", "JELLYFIN-P01", {"user_profile_id": ALICE_PROFILE_ID}, request=_admin_req())

    assert res["ok"] is True
    assert store["user_profiles"][ALICE_PROFILE_ID]["label"] == "Alice Smith"
    assert "instances" not in store["user_profiles"][ALICE_PROFILE_ID]
    assert _profile_instances(store, ALICE_PROFILE_ID) == {"JELLYFIN": ["JELLYFIN-P01"]}


def test_user_profile_crud_api(monkeypatch) -> None:
    store: dict[str, Any] = {
        "jellyfin": {"instances": {"JELLYFIN-P02": {"server": "http://jf"}}},
        "scrob": {"instances": {"SCROB-P02": {"token": "x"}}},
    }

    def fake_load() -> dict[str, Any]:
        return json.loads(json.dumps(store))

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(provider_api, "load_config", fake_load)
    monkeypatch.setattr(provider_api, "save_config", fake_save)
    monkeypatch.setattr(provider_api, "update_config", _fake_update(provider_api.load_config, fake_save))
    monkeypatch.setattr(provider_api, "generate_user_profile_id", lambda _cfg: BOB_PROFILE_ID)

    created = provider_api.api_user_profiles_create({"label": "Bob", "instances": {"JELLYFIN": "JELLYFIN-P02"}}, request=_admin_req())
    updated = provider_api.api_user_profiles_update(BOB_PROFILE_ID, {"label": "Bobby", "instances": {"SCROB": "SCROB-P02"}}, request=_admin_req())
    listed = _loads_body(provider_api.api_user_profiles_all().body)
    deleted = provider_api.api_user_profiles_delete(BOB_PROFILE_ID, request=_admin_req())

    assert created["profile"]["id"] == BOB_PROFILE_ID
    assert created["profile"]["id"] != "bob"
    assert updated["profile"]["label"] == "Bobby"
    assert listed["items"][0]["id"] == BOB_PROFILE_ID
    assert listed["items"][0]["label"] == "Bobby"
    assert listed["items"][0]["instances"] == {"SCROB": ["SCROB-P02"]}
    assert len(listed["items"][0]["instance_uids"]) == 1
    assert deleted == {"ok": True, "deleted": True}


def test_user_profile_validation_errors_do_not_expose_exception_text(monkeypatch) -> None:
    store: dict[str, Any] = {
        "user_profiles": {BOB_PROFILE_ID: {"label": "Bob", "instances": {}}},
    }

    def unsafe_update(_mutator):
        raise ValueError("Traceback: leaked internal path C:\\config\\secret.json")

    monkeypatch.setattr(provider_api, "load_config", lambda: json.loads(json.dumps(store)))
    monkeypatch.setattr(provider_api, "update_config", unsafe_update)

    created = provider_api.api_user_profiles_create({"label": "Alice"}, request=_admin_req())
    updated = provider_api.api_user_profiles_update(BOB_PROFILE_ID, {"label": "Bobby"}, request=_admin_req())

    assert created == {"ok": False, "error": "invalid_user_profile"}
    assert updated == {"ok": False, "error": "invalid_user_profile"}


def test_user_profile_delete_blocks_when_assigned_to_user(monkeypatch) -> None:
    store: dict[str, Any] = {
        "user_profiles": {BOB_PROFILE_ID: {"label": "Bob", "instances": {"PLEX": ["PLEX-P01"]}}},
        "app_auth": {
            "users": {
                "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa": {
                    "username": "pascal",
                    "profile_id": BOB_PROFILE_ID,
                }
            }
        },
    }

    monkeypatch.setattr(provider_api, "load_config", lambda: json.loads(json.dumps(store)))
    monkeypatch.setattr(provider_api, "save_config", lambda cfg: store.update(cfg))
    monkeypatch.setattr(provider_api, "update_config", _fake_update(provider_api.load_config, lambda cfg: store.update(cfg)))

    response = provider_api.api_user_profiles_delete(BOB_PROFILE_ID, request=_admin_req())

    assert response.status_code == 409
    assert _loads_body(response.body) == {
        "ok": False,
        "error": "Cannot delete this profile because it is assigned to user account: pascal. Reassign or delete the user first.",
    }
    assert BOB_PROFILE_ID in store["user_profiles"]


def test_user_profile_delete_clears_resource_assignments(monkeypatch) -> None:
    store: dict[str, Any] = {
        "user_profiles": {ALICE_PROFILE_ID: {"label": "Pascal", "instances": {"PLEX": ["default"], "SIMKL": ["default"]}}},
        "pairs": [{"id": "pair-1", "source": "PLEX", "target": "SIMKL", "profile_id": ALICE_PROFILE_ID}],
        "scrobble": {
            "watch": {"routes": [{"id": "route-1", "provider": "plex", "sink": "simkl", "profile_id": ALICE_PROFILE_ID}]},
            "webhook": {"user_profile_assignments": {"plex:default:simkl:default": ALICE_PROFILE_ID}},
        },
    }

    def fake_load() -> dict[str, Any]:
        return json.loads(json.dumps(store))

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(provider_api, "load_config", fake_load)
    monkeypatch.setattr(provider_api, "save_config", fake_save)
    monkeypatch.setattr(provider_api, "update_config", _fake_update(provider_api.load_config, fake_save))

    deleted = provider_api.api_user_profiles_delete(ALICE_PROFILE_ID, request=_admin_req())

    assert deleted == {"ok": True, "deleted": True}
    assert "profile_id" not in store["pairs"][0]
    assert "profile_id" not in store["scrobble"]["watch"]["routes"][0]
    assert "user_profile_assignments" not in store["scrobble"]["webhook"]


def test_user_profile_resource_assignments_round_trip(monkeypatch) -> None:
    store: dict[str, Any] = {
        "user_profiles": {ALICE_PROFILE_ID: {"label": "Pascal", "instances": {}}},
        "plex": {"pms_token": "plex-token"},
        "mdblist": {"api_key": "mdblist-token"},
        "simkl": {"access_token": "simkl-token"},
        "pairs": [
            {"id": "pair-1", "source": "PLEX", "source_instance": "default", "target": "MDBLIST", "target_instance": "default"},
        ],
        "scrobble": {
            "enabled": True,
            "sources": {"watcher": True, "webhook": True},
            "watch": {"routes": [{"id": "route-1", "provider": "plex", "provider_instance": "default", "sink": "simkl", "sink_instance": "default"}]},
            "webhook": {"profiles": {"plex": {"default": {"enabled": True, "sinks": ["simkl"], "sink_instances": {"simkl": "default"}}}}},
        },
    }
    _configured_refs(store, [("PLEX", "default"), ("MDBLIST", "default"), ("SIMKL", "default")])

    def fake_load() -> dict[str, Any]:
        return json.loads(json.dumps(store))

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(provider_api, "load_config", fake_load)
    monkeypatch.setattr(provider_api, "save_config", fake_save)
    monkeypatch.setattr(provider_api, "update_config", _fake_update(provider_api.load_config, fake_save))

    saved = provider_api.api_user_profiles_update(
        ALICE_PROFILE_ID,
        {
            "label": "Pascal",
            "instances": {},
            "resources": {
                "sync_pairs": ["pair-1"],
                "watcher_routes": ["route-1"],
                "webhook_routes": ["plex:default:simkl:default"],
            },
        },
     request=_admin_req())
    listed = _loads_body(provider_api.api_user_profiles_all().body)

    assert saved["ok"] is True
    assert _profile_instances(store, ALICE_PROFILE_ID) == {"MDBLIST": ["default"], "PLEX": ["default"], "SIMKL": ["default"]}
    assert store["pairs"][0]["profile_id"] == ALICE_PROFILE_ID
    assert store["scrobble"]["watch"]["routes"][0]["profile_id"] == ALICE_PROFILE_ID
    assert store["scrobble"]["webhook"]["user_profile_assignments"]["plex:default:simkl:default"] == ALICE_PROFILE_ID
    assert listed["items"][0]["resources"] == {
        "sync_pairs": ["pair-1"],
        "watcher_routes": ["route-1"],
        "webhook_routes": ["plex:default:simkl:default"],
    }
    assert listed["items"][0]["resource_counts"] == {"providers": 3, "resources": 3}

    provider_api.api_user_profiles_update(ALICE_PROFILE_ID, {"label": "Pascal", "instances": {}, "resources": {"sync_pairs": [], "watcher_routes": [], "webhook_routes": []}}, request=_admin_req())

    assert _profile_instances(store, ALICE_PROFILE_ID) == {}
    assert "profile_id" not in store["pairs"][0]
    assert "profile_id" not in store["scrobble"]["watch"]["routes"][0]
    assert "user_profile_assignments" not in store["scrobble"]["webhook"]


def test_user_profile_rejects_duplicate_labels(monkeypatch) -> None:
    store: dict[str, Any] = {
        "user_profiles": {ALICE_PROFILE_ID: {"label": "Pascal", "instances": {}}},
    }

    monkeypatch.setattr(provider_api, "load_config", lambda: json.loads(json.dumps(store)))
    monkeypatch.setattr(provider_api, "save_config", lambda cfg: store.update(cfg))
    monkeypatch.setattr(provider_api, "update_config", _fake_update(provider_api.load_config, lambda cfg: store.update(cfg)))
    monkeypatch.setattr(provider_api, "generate_user_profile_id", lambda _cfg: BOB_PROFILE_ID)

    created = provider_api.api_user_profiles_create({"label": "pascal"}, request=_admin_req())
    renamed = provider_api.api_user_profiles_update(ALICE_PROFILE_ID, {"label": "  PASCAL  "}, request=_admin_req())

    assert created == {"ok": False, "error": "duplicate_user_profile_label"}
    assert renamed["ok"] is True


def test_user_profile_allows_one_instance_per_provider(monkeypatch) -> None:
    store: dict[str, Any] = {
        "user_profiles": {
            ALICE_PROFILE_ID: {"label": "Alice", "instances": {"JELLYFIN": ["JELLYFIN-P01"]}},
            FRANK_PROFILE_ID: {"label": "Frank", "instances": {"JELLYFIN": ["JELLYFIN-P02"]}},
        }
    }
    _configured_refs(store, [("JELLYFIN", "JELLYFIN-P01"), ("JELLYFIN", "JELLYFIN-P02"), ("CROSSWATCH", "CW-P01")])

    def fake_load() -> dict[str, Any]:
        return json.loads(json.dumps(store))

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(provider_api, "load_config", fake_load)
    monkeypatch.setattr(provider_api, "save_config", fake_save)
    monkeypatch.setattr(provider_api, "update_config", _fake_update(provider_api.load_config, fake_save))

    updated = provider_api.api_user_profiles_update(
        ALICE_PROFILE_ID,
        {"label": "Alice", "instances": {"JELLYFIN": ["JELLYFIN-P01", "JELLYFIN-P02"], "CROSSWATCH": ["CW-P01"]}},
     request=_admin_req())

    assert updated["profile"]["instances"] == {
        "CROSSWATCH": ["CW-P01"],
        "JELLYFIN": ["JELLYFIN-P01"],
    }
    assert _profile_instances(store, FRANK_PROFILE_ID) == {"JELLYFIN": ["JELLYFIN-P02"]}


def test_user_profiles_list_is_scoped_for_managed_user(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from tests.test_app_auth_api import _auth_cfg, _request

    store: dict[str, Any] = _auth_cfg()
    store["user_profiles"] = {
        ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"]}},
        BOB_PROFILE_ID: {"label": "Bob", "instances": {"PLEX": ["PLEX-P02"]}},
    }
    _configured_refs(store, [("PLEX", "PLEX-P01"), ("PLEX", "PLEX-P02")])
    store["app_auth"]["users"] = {
        "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa": {
            "username": "bob",
            "enabled": True,
            "role": "user",
            "profile_id": BOB_PROFILE_ID,
            "permissions": {"dashboard": True},
            "password": auth._password_hash("secrett2"),
        }
    }
    user = auth._public_user("aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa", store["app_auth"]["users"]["aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"])
    token, _exp = auth._issue_session(store, _request("/api/app-auth/login"), user)
    request = _request("/api/user-profiles", method="GET", headers={"cookie": f"{auth.COOKIE_NAME}={token}"})

    monkeypatch.setattr(provider_api, "load_config", lambda: json.loads(json.dumps(store)))

    data = _loads_body(provider_api.api_user_profiles_all(request).body)

    assert data["items"][0]["id"] == BOB_PROFILE_ID
    assert data["items"][0]["label"] == "Bob"
    assert data["items"][0]["instances"] == {"PLEX": ["PLEX-P02"]}


def test_provider_instances_are_scoped_for_managed_user(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from tests.test_app_auth_api import _auth_cfg, _request

    store: dict[str, Any] = _auth_cfg()
    store["user_profiles"] = {
        ALICE_PROFILE_ID: {"label": "Alice", "instances": {"CROSSWATCH": ["CW-P01"], "PLEX": ["PLEX-P01"]}},
        BOB_PROFILE_ID: {"label": "Bob", "instances": {"CROSSWATCH": ["CW-P02"]}},
    }
    _configured_refs(store, [("CROSSWATCH", "CW-P01"), ("CROSSWATCH", "CW-P02"), ("PLEX", "PLEX-P01"), ("PLEX", "PLEX-P02")])
    store["app_auth"]["users"] = {
        "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa": {
            "username": "alice",
            "enabled": True,
            "role": "user",
            "profile_id": ALICE_PROFILE_ID,
            "permissions": {"dashboard": True},
            "password": auth._password_hash("secrett2"),
        }
    }
    user = auth._public_user("aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa", store["app_auth"]["users"]["aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"])
    token, _exp = auth._issue_session(store, _request("/api/app-auth/login"), user)
    request = _request("/api/provider-instances/CROSSWATCH", method="GET", headers={"cookie": f"{auth.COOKIE_NAME}={token}"})

    monkeypatch.setattr(provider_api, "load_config", lambda: json.loads(json.dumps(store)))

    crosswatch_rows = _loads_body(provider_api.api_provider_instances_provider("CROSSWATCH", request).body)
    all_rows = _loads_body(provider_api.api_provider_instances_all(_request("/api/provider-instances", method="GET", headers={"cookie": f"{auth.COOKIE_NAME}={token}"})).body)

    assert [row["id"] for row in crosswatch_rows] == ["CW-P01"]
    assert [row["id"] for row in all_rows["CROSSWATCH"]] == ["CW-P01"]
    assert [row["id"] for row in all_rows["PLEX"]] == ["PLEX-P01"]


def test_user_profile_detail_is_scoped_for_managed_user(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from tests.test_app_auth_api import _auth_cfg, _request

    store: dict[str, Any] = _auth_cfg()
    store["user_profiles"] = {
        ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"]}},
        BOB_PROFILE_ID: {"label": "Bob", "instances": {"PLEX": ["PLEX-P02"]}},
    }
    _configured_refs(store, [("PLEX", "PLEX-P01"), ("PLEX", "PLEX-P02")])
    store["app_auth"]["users"] = {
        "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa": {
            "username": "bob",
            "enabled": True,
            "role": "user",
            "profile_id": BOB_PROFILE_ID,
            "permissions": {"dashboard": True},
            "password": auth._password_hash("secrett2"),
        }
    }
    user = auth._public_user("aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa", store["app_auth"]["users"]["aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"])
    token, _exp = auth._issue_session(store, _request("/api/app-auth/login"), user)
    request = _request(f"/api/user-profiles/{ALICE_PROFILE_ID}", method="GET", headers={"cookie": f"{auth.COOKIE_NAME}={token}"})

    monkeypatch.setattr(provider_api, "load_config", lambda: json.loads(json.dumps(store)))

    blocked = provider_api.api_user_profiles_get(ALICE_PROFILE_ID, request)
    own = provider_api.api_user_profiles_get(BOB_PROFILE_ID, _request(f"/api/user-profiles/{BOB_PROFILE_ID}", method="GET", headers={"cookie": f"{auth.COOKIE_NAME}={token}"}))

    assert blocked.status_code == 404
    assert _loads_body(own.body)["profile"]["id"] == BOB_PROFILE_ID


def test_currently_watching_filters_requested_user_profile(monkeypatch) -> None:
    store: dict[str, Any] = {
        "user_profiles": {
            ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"]}},
            BOB_PROFILE_ID: {"label": "Bob", "instances": {"PLEX": ["PLEX-P02"]}},
        }
    }
    _configured_refs(store, [("PLEX", "PLEX-P01"), ("PLEX", "PLEX-P02")])

    monkeypatch.setattr(scrobble_api, "load_config", lambda: json.loads(json.dumps(store)))
    monkeypatch.setattr(scrobble_api, "_cw_load_state", _currently_watching_state)

    data = _loads_body(scrobble_api.api_currently_watching(cast(Any, SimpleNamespace(cookies={})), user_profile=ALICE_PROFILE_ID).body)

    assert data["streams_count"] == 1
    assert [row["title"] for row in data["streams"]] == ["Alice Movie"]
    assert data["currently_watching"]["provider_instance"] == "PLEX-P01"


def test_currently_watching_filters_shared_instance_by_route_account_allowlist(monkeypatch) -> None:
    store: dict[str, Any] = {
        "user_profiles": {
            ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["default"]}},
            BOB_PROFILE_ID: {"label": "Bob", "instances": {"PLEX": ["default"]}},
        },
        "scrobble": {
            "watch": {
                "routes": [
                    {
                        "id": "R1",
                        "provider": "plex",
                        "provider_instance": "default",
                        "sink": "crosswatch",
                        "sink_instance": "default",
                        "profile_id": ALICE_PROFILE_ID,
                        "filters": {"username_whitelist": ["Alice"]},
                    },
                    {
                        "id": "R2",
                        "provider": "plex",
                        "provider_instance": "default",
                        "sink": "crosswatch",
                        "sink_instance": "default",
                        "profile_id": BOB_PROFILE_ID,
                        "filters": {"username_whitelist": ["id:42"]},
                    },
                ]
            }
        },
    }
    _configured_refs(store, [("PLEX", "default"), ("CROSSWATCH", "default")])
    state = {
        "v": 2,
        "streams": {
            "a": {"source": "PLEX", "provider_instance": "default", "account": "Alice", "state": "playing", "updated": 2, "title": "Alice Movie"},
            "b": {"source": "PLEX", "provider_instance": "default", "account": "Bob", "account_id": "42", "state": "playing", "updated": 1, "title": "Bob Movie"},
        },
    }

    monkeypatch.setattr(scrobble_api, "load_config", lambda: json.loads(json.dumps(store)))
    monkeypatch.setattr(scrobble_api, "_cw_load_state", lambda: json.loads(json.dumps(state)))

    alice = _loads_body(scrobble_api.api_currently_watching(cast(Any, SimpleNamespace(cookies={})), user_profile=ALICE_PROFILE_ID).body)
    bob = _loads_body(scrobble_api.api_currently_watching(cast(Any, SimpleNamespace(cookies={})), user_profile=BOB_PROFILE_ID).body)

    assert [row["title"] for row in alice["streams"]] == ["Alice Movie"]
    assert [row["title"] for row in bob["streams"]] == ["Bob Movie"]


def test_currently_watching_managed_user_forces_own_profile(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from tests.test_app_auth_api import _auth_cfg, _request

    store: dict[str, Any] = _auth_cfg()
    store["user_profiles"] = {
        ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"]}},
        BOB_PROFILE_ID: {"label": "Bob", "instances": {"PLEX": ["PLEX-P02"]}},
    }
    _configured_refs(store, [("PLEX", "PLEX-P01"), ("PLEX", "PLEX-P02")])
    store["app_auth"]["users"] = {
        "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa": {
            "username": "bob",
            "enabled": True,
            "role": "user",
            "profile_id": BOB_PROFILE_ID,
            "permissions": {"dashboard": True},
            "password": auth._password_hash("secrett2"),
        }
    }
    user = auth._public_user("aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa", store["app_auth"]["users"]["aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"])
    token, _exp = auth._issue_session(store, _request("/api/app-auth/login"), user)

    monkeypatch.setattr(scrobble_api, "load_config", lambda: json.loads(json.dumps(store)))
    monkeypatch.setattr(scrobble_api, "_cw_load_state", _currently_watching_state)

    data = _loads_body(scrobble_api.api_currently_watching(cast(Any, SimpleNamespace(cookies={auth.COOKIE_NAME: token})), user_profile=ALICE_PROFILE_ID).body)

    assert data["streams_count"] == 1
    assert [row["title"] for row in data["streams"]] == ["Bob Movie"]
    assert data["currently_watching"]["provider_instance"] == "PLEX-P02"


def test_playback_progress_filter_for_managed_user_forces_own_profile(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from tests.test_app_auth_api import _auth_cfg, _request

    store: dict[str, Any] = _auth_cfg()
    store["user_profiles"] = {
        ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"]}},
        BOB_PROFILE_ID: {"label": "Bob", "instances": {"PLEX": ["PLEX-P02"]}},
    }
    _configured_refs(store, [("PLEX", "PLEX-P01"), ("PLEX", "PLEX-P02")])
    store["app_auth"]["users"] = {
        "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa": {
            "username": "bob",
            "enabled": True,
            "role": "user",
            "profile_id": BOB_PROFILE_ID,
            "permissions": {"dashboard": True},
            "password": auth._password_hash("secrett2"),
        }
    }
    user = auth._public_user("aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa", store["app_auth"]["users"]["aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"])
    token, _exp = auth._issue_session(store, _request("/api/app-auth/login"), user)

    monkeypatch.setattr(playback_api, "load_config", lambda: json.loads(json.dumps(store)))

    filt = playback_api._playback_user_filter(cast(Any, SimpleNamespace(cookies={auth.COOKIE_NAME: token})), ALICE_PROFILE_ID)

    assert filt == {"PLEX": ["PLEX-P02"]}


def test_activity_recent_filters_by_user_profile(monkeypatch) -> None:
    cfg = _configured_refs(
        {"user_profiles": {BOB_PROFILE_ID: {"label": "Bob", "instances": {"PLEX": ["PLEX-P02"]}}}},
        [("PLEX", "PLEX-P02")],
    )
    monkeypatch.setattr(
        activity_api,
        "load_config",
        lambda: json.loads(json.dumps(cfg)),
    )
    monkeypatch.setattr(
        activity_api,
        "list_events",
        lambda **_kwargs: {
            "ok": True,
            "total": 2,
            "items": [
                {"id": "alice", "source": "plex", "source_instance": "default"},
                {"id": "bob", "source": "plex", "source_instance": "PLEX-P02"},
            ],
        },
    )

    data = _loads_body(activity_api.activity_recent(limit=10, user_profile=BOB_PROFILE_ID).body)

    assert data["total"] == 1
    assert [item["id"] for item in data["items"]] == ["bob"]


def test_user_profile_manager_screen_is_mounted() -> None:
    settings_html = Path("ui_frontend.py").read_text("utf-8")
    providers_js = Path("assets/helpers/providers-ui.js").read_text("utf-8")
    manager_js = Path("assets/js/user-profiles.js").read_text("utf-8")
    app_users_js = Path("assets/js/app-users.js").read_text("utf-8")
    app_users_css = Path("assets/css/app-users.css").read_text("utf-8")
    css = Path("assets/css/auth-providers.css").read_text("utf-8")

    assert "user-profiles.js" in settings_html
    assert "cwUserProfilesNew" in settings_html
    assert "sec-user-profiles" not in settings_html
    assert "cw-user-profile-manager" in providers_js
    assert "sec-user-profiles" in providers_js
    assert "Manage people and provider instances linked to Managed User accounts." in providers_js
    assert "/api/user-profiles" in manager_js
    assert "/api/provider-instances" in manager_js
    assert "configured_only=true" in manager_js
    assert "Create Alice" not in manager_js
    assert "cw-upm-summary-row" not in manager_js
    assert "cw-upm-instance-user" not in manager_js
    assert "cw-upm-table" not in manager_js
    assert "cw-upm-service-grid" in manager_js
    assert "cw-upm-overlay" in manager_js
    assert "cw-auth-provider-mark cw-upm-card-icon" in manager_js
    assert "cw-auth-add-mark cw-upm-add-mark" in manager_js
    assert "cw-auth-service-copy cw-upm-card-copy" in manager_js
    assert "providerMark(provider)" in manager_js
    assert "cw-upm-provider-logo" in manager_js
    assert "cw-upm-provider-head" in manager_js
    assert "cw-upm-provider-grid" in manager_js
    assert "cw-upm-instance-box" not in manager_js
    assert "permissions: { dashboard: true, watchlist: true, playback: true, write: true }" in app_users_js
    assert "permissions: { dashboard: true, watchlist: true, playback: true, write: false }" not in app_users_js
    assert "#page-settings .cw-app-user-create .cw-icon-select-btn" in app_users_css
    assert "#page-settings .cw-app-user-create.has-no-profiles .cw-icon-select" in app_users_css
    assert "cw-app-user-profile-select-wrap-hidden" in app_users_js
    assert "function createProfileWrap(profileSelect)" in app_users_js
    assert "function normalizeCreateProfileWrap(profileSelect)" in app_users_js
    assert "function enhanceCreateProfileSelect(profileSelect)" in app_users_js
    assert "(profileWrap || profileSelect).insertAdjacentElement(\"afterend\", addBtn);" in app_users_js
    assert "profileWrap = enhanceCreateProfileSelect(profileSelect);" in app_users_js
    assert "setTimeout(() => refresh(), 160);" in app_users_js
    assert "profileWrap.hidden = true;" in app_users_js
    assert "height: 44px !important;" in app_users_css
    assert "max-height: 44px !important;" in app_users_css
    assert '#page-settings #app_auth_fields :is(input:not([type="checkbox"]):not([type="radio"]):not([type="range"]), select, .cw-icon-select, .cw-icon-select-btn)' in app_users_css
    assert "#page-settings .cw-app-user-controls .cw-icon-select-btn" in app_users_css
    assert "height: 42px !important;" in app_users_css
    assert "max-height: 42px !important;" in app_users_css
    assert "#page-settings #app_auth_fields .cw-auth-totp-field" in app_users_css
    assert "#page-settings #app_auth_fields .cw-auth-plex-field" in app_users_css
    assert "#page-settings #app_auth_fields .cw-auth-totp-field > .cw-settings-inline-action" in app_users_css
    assert "#page-settings #app_auth_fields .cw-auth-plex-field > .cw-settings-inline-action" in app_users_css
    assert "#page-settings #app_auth_fields :is(.cw-auth-totp-field, .cw-auth-plex-field) > .sub" in app_users_css
    assert "grid-template-columns: repeat(2, minmax(0, 1fr));" in app_users_css
    assert "grid-template-columns: repeat(2, minmax(0, 1fr)) !important;" in app_users_css
    assert "cw-danger-confirm" in manager_js
    assert "Confirm delete" in manager_js
    assert "cwUserProfilesOpenNew" in manager_js
    assert "cw-upm-floating-host" in manager_js
    assert "w.confirm" not in manager_js
    assert "manage_accounts" not in manager_js
    assert 'type="radio"' not in manager_js
    assert 'type="checkbox" name="cw-upm-provider-${esc(provider)}"' in manager_js
    assert 'style="position:absolute;opacity:0;pointer-events:none"' in manager_js
    assert "input:checked" in manager_js
    assert ".cw-user-profile-manager" in css
    assert ".cw-upm-service-grid" in css
    assert ".cw-upm-dialog" in css
    assert "flex:0 0 36px;width:36px;height:36px" in css
    assert ".cw-upm-add-mark .material-symbols-rounded{font-size:24px" in css
    assert ".cw-upm-provider-mark" in css
    assert ".cw-upm-provider-grid" in css
    assert "grid-template-columns:repeat(auto-fit,minmax(280px,1fr))" in css
    assert "grid-template-columns:repeat(auto-fill,minmax(178px,1fr))" in css
    assert ".cw-upm-instance-box" in css
    assert "function createReady()" in app_users_js
    assert "btn-app-user-profile-create" in app_users_js
    assert "w.cwUserProfilesOpenNew?.({ stay: true });" in app_users_js
    assert "cwAppUsersSavePending" in app_users_js
    assert 'data-user-action="save"' not in app_users_js
    assert 'id="app_user_create_form"' in settings_html
    assert 'autocomplete="username"' in settings_html
    assert "cwAppUsersSavePending" in Path("assets/helpers/settings-save.js").read_text("utf-8")
    assert "#page-settings .cw-app-user-add-profile" in app_users_css
    assert ".cw-app-user-delete.is-confirming" in app_users_css


def test_events_audit_domain_is_admin_only_for_managed_users(monkeypatch) -> None:
    req = cast(Any, SimpleNamespace(state=SimpleNamespace(cw_user={"id": "u1", "username": "pascal", "is_admin": False})))
    monkeypatch.setattr(events_api, "load_config", lambda: {})

    blocked = events_api.events_groups(domain="audit", request=req)

    assert blocked.status_code == 403
    assert _loads_body(blocked.body)["error"] == "profile_scope_denied"


def test_events_modal_has_admin_only_audits_tab() -> None:
    js = Path("assets/js/modals/events/index.js").read_text("utf-8")

    assert 'value: "scrobble", label: "Scrobble"' in js
    assert 'value: "audit", label: "Audits"' in js
    assert 'value: "statistics", label: "Statistics"' in js
    assert js.index('value: "scrobble", label: "Scrobble"') < js.index('value: "audit", label: "Audits"') < js.index('value: "statistics", label: "Statistics"')
    assert "...(isAdmin ? [{ value: \"audit\", label: \"Audits\", icon: \"admin_panel_settings\" }] : [])" in js
    assert 'if (view === "audit" && !isAdmin) view = "sync"' in js
    assert 'domain === "audit"' in js


def test_overview_profile_avatar_link_replaces_selector() -> None:
    html = Path("ui_frontend.py").read_text("utf-8")
    js = Path("assets/js/overview-profile.js").read_text("utf-8")
    css = Path("assets/crosswatch.css").read_text("utf-8")

    assert "cw-nav-profile-link" in html
    assert "cw-nav-profile-avatar" in html
    assert "overview-profile-link" not in html
    assert "overview-profile-avatar" not in html
    assert "overview-profile-select" not in html
    assert "overview-profile-select" not in js
    assert "CW?.IconSelect?.enhance" not in js
    assert "#stats-card.card,#stats-card.cw-main-card{overflow:visible}" in css
    assert ".cw-nav-profile-link" in css
    assert ".cw-nav-profile-avatar img" in css
    assert "overview-profile-control" not in css
    assert "cw-overview-profile-menu" not in css


def test_provider_instances_all_can_return_configured_only(monkeypatch) -> None:
    store: dict[str, Any] = {
        "anilist": {"access_token": "ani"},
        "emby": {},
        "jellyfin": {
            "instances": {
                "JELLYFIN-P01": {"server": "http://jf", "access_token": "token"},
                "JELLYFIN-P02": {"server": "http://jf"},
            }
        },
    }

    monkeypatch.setattr(provider_api, "load_config", lambda: json.loads(json.dumps(store)))

    data = _loads_body(provider_api.api_provider_instances_all(configured_only=True).body)

    assert "ANILIST" in data
    assert "EMBY" not in data
    assert [row["id"] for row in data["JELLYFIN"]] == ["JELLYFIN-P01"]


def test_provider_instances_rows_expose_stable_uid(monkeypatch) -> None:
    store: dict[str, Any] = {
        "plex": {"instances": {"PLEX-P01": {"server_url": "http://plex", "label": "Desk"}}},
        "provider_instance_ids": {
            PROVIDER_INSTANCE_UID: {"provider": "PLEX", "instance": "PLEX-P01"},
        },
    }

    monkeypatch.setattr(provider_api, "load_config", lambda: json.loads(json.dumps(store)))
    monkeypatch.setattr(provider_api, "save_config", lambda cfg: store.update(json.loads(json.dumps(cfg))))
    monkeypatch.setattr(provider_api, "update_config", _fake_update(provider_api.load_config, lambda cfg: store.update(json.loads(json.dumps(cfg)))))

    data = _loads_body(provider_api.api_provider_instances_provider("plex").body)
    row = next(item for item in data if item["id"] == "PLEX-P01")

    assert row["uid"] == PROVIDER_INSTANCE_UID
    assert provider_instances.provider_instance_ref(store, "plex", PROVIDER_INSTANCE_UID) == ("PLEX", "PLEX-P01")
    assert provider_instances.provider_instance_ref(store, "plex", "PLEX-P01") == ("PLEX", "PLEX-P01")


def test_provider_instance_uid_migration_backfills_existing_refs() -> None:
    cfg: dict[str, Any] = {
        "plex": {"server_url": "http://plex", "instances": {"PLEX-P01": {"server_url": "http://plex-1"}}},
        "jellyfin": {"server": "http://jf"},
        "user_profiles": {
            ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"], "JELLYFIN": ["default"]}},
        },
        "pairs": [
            {"source": "PLEX", "source_instance": "PLEX-P01", "target": "JELLYFIN", "target_instance": "default"},
        ],
        "provider_instance_ids": {
            "bbbbbbbbbbbb4bbb8bbbbbbbbbbbbbbb": {"provider": "PLEX", "instance": "PLEX-P99"},
        },
    }

    changed = provider_instances.ensure_provider_instance_uids(cfg)
    records = set((row["provider"], row["instance"]) for row in cfg["provider_instance_ids"].values())

    assert changed is True
    assert records == {("JELLYFIN", "default"), ("PLEX", "PLEX-P01")}
    assert "instances" not in cfg["user_profiles"][ALICE_PROFILE_ID]
    assert len(cfg["user_profiles"][ALICE_PROFILE_ID]["instance_uids"]) == 2
    assert _profile_instances(cfg, ALICE_PROFILE_ID) == {"JELLYFIN": ["default"], "PLEX": ["PLEX-P01"]}
    assert cfg["pairs"][0]["source_instance"] == "PLEX-P01"


def test_deleted_recreated_instance_does_not_keep_old_profile_assignment() -> None:
    cfg: dict[str, Any] = {
        "jellyfin": {"instances": {"JELLYFIN-P02": {"server": "http://old"}}},
        "user_profiles": {
            ALICE_PROFILE_ID: {"label": "Alice", "instances": {"JELLYFIN": ["JELLYFIN-P02"]}},
        },
    }
    provider_instances.ensure_provider_instance_uids(cfg)
    old_uid = cfg["user_profiles"][ALICE_PROFILE_ID]["instance_uids"][0]

    provider_instances.remove_instance_from_user_profiles(cfg, "JELLYFIN", "JELLYFIN-P02")
    provider_instances.remove_provider_instance_uid(cfg, "JELLYFIN", "JELLYFIN-P02")
    cfg["jellyfin"]["instances"].pop("JELLYFIN-P02")
    cfg["jellyfin"]["instances"]["JELLYFIN-P02"] = {"server": "http://new"}
    new_uid = provider_instances.provider_instance_uid_for(cfg, "JELLYFIN", "JELLYFIN-P02", create=True)

    assert new_uid
    assert new_uid != old_uid
    assert _profile_instances(cfg, ALICE_PROFILE_ID) == {}
    assert provider_instances.user_profile_for_instance(cfg, "JELLYFIN", "JELLYFIN-P02") is None


def test_crosswatch_provider_block_derives_profile_root_when_missing(tmp_path: Path) -> None:
    root = tmp_path / "cw_provider"
    cfg = {"crosswatch": {"root_dir": str(root), "instances": {"CW-P01": {"label": "Kids"}}}}

    block = get_provider_block(cfg, "CROSSWATCH", "CW-P01")

    assert block["label"] == "Kids"
    assert block["root_dir"].replace("\\", "/").endswith("/cw_provider/profiles/CW-P01")


def test_crosswatch_pair_config_view_selects_profile_root(tmp_path: Path) -> None:
    root = tmp_path / "cw_provider"
    cfg = {
        "simkl": {"access_token": "token"},
        "crosswatch": {"root_dir": str(root), "instances": {"CW-P02": {"retention_days": 7}}},
    }

    view = build_pair_config_view(cfg, "SIMKL", "default", "CROSSWATCH", "CW-P02")

    assert view["crosswatch"]["retention_days"] == 7
    assert view["crosswatch"]["root_dir"].replace("\\", "/").endswith("/cw_provider/profiles/CW-P02")


def test_crosswatch_module_uses_profile_root_from_config_view(tmp_path: Path) -> None:
    root = tmp_path / "cw_provider"
    cfg = {"crosswatch": {"root_dir": str(root), "instances": {"CW-P03": {}}}}
    view = build_pair_config_view(cfg, "SIMKL", "default", "CROSSWATCH", "CW-P03")

    mod = CROSSWATCHModule(view)

    assert str(mod.cfg.base_path).replace("\\", "/").endswith("/cw_provider/profiles/CW-P03")


def test_editor_removes_local_tracker_source() -> None:
    editor_js = Path("assets/js/editor.js").read_text("utf-8")
    sources_js = Path("assets/js/editor/sources.js").read_text("utf-8")
    chrome_js = Path("assets/js/editor/chrome.js").read_text("utf-8")
    load_js = Path("assets/js/editor/load-controller.js").read_text("utf-8")

    assert 'option value="tracker"' not in editor_js
    assert 'const SOURCES = ["state", "manual", "playlist"];' in sources_js
    assert "sourceSel.querySelector('option[value=\"tracker\"]')?.remove();" in sources_js
    assert "sourceSelect.querySelector('option[value=\"tracker\"]')?.remove();" in chrome_js
    assert "/api/editor/tracker/workspaces" not in sources_js
    assert "state.workspace" not in load_js
    assert "if (ctx.snapLabel) ctx.snapLabel.textContent = providerPicker ? \"Provider\" : \"Endpoint\";" in sources_js


def test_editor_rejects_local_tracker_source() -> None:
    assert not hasattr(editor_api, "api_editor_tracker_workspaces")

    try:
        editor_api.api_editor_get_state(kind="watchlist", source="tracker")
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 400
    else:
        raise AssertionError("tracker source should not load")

    try:
        editor_api.api_editor_save_state({"kind": "watchlist", "source": "tracker", "items": {}})
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 400
    else:
        raise AssertionError("tracker source should not save")


def test_tracker_archive_json_import_uses_crosswatch_profile_snapshot_dir(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "cw_provider"
    monkeypatch.setattr(editor_service, "load_config", lambda: {
        "crosswatch": {"root_dir": str(root), "instances": {"CW-P04": {"retention_days": 0}}},
    })

    stats = editor_service.import_tracker_json(
        b'{"items":{"movie:x":{"title":"X"}}}',
        "20260101T000000Z-watchlist.json",
        "CW-P04",
    )

    assert stats["target"] == "snapshot"
    assert (root / "profiles" / "CW-P04" / "snapshots" / "20260101T000000Z-watchlist.json").exists()
    assert not (root / "snapshots" / "20260101T000000Z-watchlist.json").exists()


def test_tracker_archive_ignores_unconfigured_profile_path_input(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "cw_provider"
    outside = tmp_path / "outside"
    monkeypatch.setattr(editor_service, "load_config", lambda: {
        "crosswatch": {"root_dir": str(root), "instances": {"CW-P04": {"retention_days": 0}}},
    })

    stats = editor_service.import_tracker_json(
        b'{"items":{"movie:x":{"title":"X"}}}',
        "watchlist.json",
        "../outside",
    )

    assert stats["target"] == "state"
    assert (root / "watchlist.json").exists()
    assert not outside.exists()


def test_status_probes_include_crosswatch_profiles(monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from api import probesAPI as probes

    cfg = {
        "crosswatch": {
            "connected": True,
            "root_dir": "/config/.cw_provider",
            "instances": {
                "CW-P01": {"connected": True, "label": "Desk"},
                "CW-P02": {"connected": True, "enabled": False, "label": "Old"},
            },
        }
    }

    probes.invalidate_provider_caches("crosswatch")
    app = FastAPI()
    probes.register_probes(app, lambda: cfg)

    data = TestClient(app).get("/api/status?fresh=1").json()
    body = json.dumps(data)
    cw = data["providers"]["CROSSWATCH"]

    assert data["crosswatch_connected"] is True
    assert cw["connected"] is True
    assert cw["vip"] is True
    assert cw["vip_text"] == "You've earned it"
    assert cw["instances_summary"]["total"] == 3
    assert cw["instances"]["default"]["configured"] is True
    assert cw["instances"]["CW-P01"]["configured"] is True
    assert cw["instances"]["CW-P02"]["connected"] is False
    assert "Desk" not in body


def test_status_probes_are_scoped_to_managed_user_profile() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from api import probesAPI as probes

    cfg: dict[str, Any] = {
        "crosswatch": {
            "connected": True,
            "root_dir": "/config/.cw_provider",
            "instances": {
                "CW-P01": {"connected": True, "label": "Alice"},
                "CW-P02": {"connected": True, "label": "Bob"},
            },
        },
        "plex": {"account_token": "plex-token", "server_url": "http://plex"},
        "simkl": {"access_token": "simkl-token", "client_id": "cid"},
        "scrobble": {
            "watch": {
                "routes": [
                    {
                        "id": "route-1",
                        "provider": "plex",
                        "provider_instance": "default",
                        "sink": "simkl",
                        "sink_instance": "default",
                        "profile_id": ALICE_PROFILE_ID,
                    }
                ]
            }
        },
        "user_profiles": {
            ALICE_PROFILE_ID: {"label": "Alice", "instances": {"CROSSWATCH": ["CW-P01"], "PLEX": ["default"], "SIMKL": ["default"]}},
            BOB_PROFILE_ID: {"label": "Bob", "instances": {"CROSSWATCH": ["CW-P02"]}},
        },
    }
    _configured_refs(cfg, [("CROSSWATCH", "CW-P01"), ("CROSSWATCH", "CW-P02"), ("PLEX", "default"), ("SIMKL", "default")])

    probes.invalidate_provider_caches("crosswatch")
    app = FastAPI()

    @app.middleware("http")
    async def _as_managed_user(request, call_next):
        request.state.cw_user = {"id": "u-bob", "username": "bob", "is_admin": False, "profile_id": BOB_PROFILE_ID}
        return await call_next(request)

    probes.register_probes(app, lambda: cfg)

    providers = TestClient(app).get("/api/status?fresh=1").json()["providers"]

    assert set(providers) == {"CROSSWATCH"}
    assert set(providers["CROSSWATCH"]["instances"]) == {"CW-P02"}


def test_main_status_crosswatch_vip_copy() -> None:
    text = Path("assets/js/main-status.js").read_text("utf-8")
    provider_meta = Path("assets/helpers/provider-meta.js").read_text("utf-8")
    css = Path("assets/crosswatch.css").read_text("utf-8")

    assert 'case "CROSSWATCH"' in text
    assert "Plan: VIP" in text
    assert "Plan: Free" in text
    assert "Plan: Premiere" in text
    assert "You've earned it" in text
    assert "SIMKL plan:" not in text
    assert "VIP status" not in text
    assert "Free account" not in text
    assert 'info.key !== "CROSSWATCH"' not in provider_meta
    assert 'CROSSWATCH: { key: "CROSSWATCH"' in provider_meta
    assert "max-width:520px !important" in css
    assert "max-width:340px !important" not in css


def test_crosswatch_tracker_settings_live_only_in_connection_modal() -> None:
    settings_html = Path("ui_frontend.py").read_text("utf-8")
    settings_ui = Path("assets/helpers/settings-ui.js").read_text("utf-8")
    settings_save = Path("assets/helpers/settings-save.js").read_text("utf-8")
    auth_html = Path("providers/auth/_auth_CROSSWATCH.py").read_text("utf-8")
    auth_js = Path("assets/auth/auth.crosswatch.js").read_text("utf-8")
    auth_shared = Path("assets/auth/auth.shared.js").read_text("utf-8")
    auth_css = Path("assets/css/auth-providers.css").read_text("utf-8")
    providers_ui = Path("assets/helpers/providers-ui.js").read_text("utf-8")

    assert 'data-target="tracker"' not in settings_html
    assert 'data-tab="tracker"' not in settings_html
    assert "Retention, capture and restore snapshots" not in settings_html
    assert "Settings (UI / Security / Local Tracker)" not in settings_html
    assert "loadCrossWatchSnapshots" not in settings_ui
    assert "cw_restore_watchlist" not in settings_save

    assert "cw_tracker_label" in auth_html
    assert 'maxlength="12"' in auth_html
    assert "cw-profile-label-field" in auth_shared
    assert "Display name" in auth_shared
    assert "Max 12" not in auth_shared
    assert "Internal ID stays unchanged" in auth_shared
    assert ".cw-profile-label-field" in auth_css
    assert ".cw-profile-label-input" in auth_css
    assert 'data-sub="auth"' in auth_html
    assert "cw_crosswatch_connect" in auth_html
    assert "Manage Local Tracker Profiles" not in auth_html
    assert "cw-field-help material-symbols-rounded" in auth_html
    assert "cw-tracker-settings-stack" in auth_html
    assert "Local tracker storage" in auth_html
    assert "Restore snapshots" in auth_html
    assert "Local tracker label setting help" in auth_html
    assert "Local tracker progress restore help" in auth_html
    assert "/api/crosswatch/connect" in auth_js
    assert "cw_tracker_msg" not in auth_html
    assert "cw_tracker_msg" not in auth_js
    assert ">Ready<" not in auth_html
    assert 'data-sub="restore"' not in auth_html
    assert "cw_tracker_enabled" not in auth_html
    assert "cw_tracker_enabled" not in auth_js
    assert 'set("enabled"' not in settings_save
    assert "#cw_crosswatch_disconnect" in providers_ui
    assert 'restore: ["restore_page"' not in providers_ui
    assert "Local Tracker Restore" not in providers_ui
    assert "Storage and restore" in providers_ui
    assert "Use profiles when you want separate local watchlist, ratings, history or progress data." in providers_ui
    assert 'introSubs: ["auth"]' in providers_ui
    assert 'introSubs: ["auth", "settings"]' not in providers_ui
    assert "cw_crosswatch_disconnect" in auth_html
    assert "cw_tracker_retention_days" in auth_html
    assert "cw_tracker_auto_snapshot" in auth_html
    assert "cw_tracker_max_snapshots" in auth_html
    assert "cw_tracker_root_dir" not in auth_html
    assert "Storage root" not in auth_html
    assert "cw_tracker_root_dir" not in auth_js
    assert "cw_tracker_root_dir" not in settings_save
    assert "cw_tracker_restore_progress" in auth_html


def test_tmdb_metadata_modal_refreshes_saved_key_before_opening() -> None:
    settings_ui = Path("assets/helpers/settings-ui.js").read_text("utf-8")
    settings_save = Path("assets/helpers/settings-save.js").read_text("utf-8")
    providers_ui = Path("assets/helpers/providers-ui.js").read_text("utf-8")

    assert "async function cwRefreshTmdbMetadataState" in settings_ui
    assert "cfg?.tmdb?.api_key || cfg?.metadata?.tmdb_api_key" in settings_ui
    assert "window.CW?.AuthShared?.maskSecret" in settings_ui
    assert "window.CW.AuthShared.maskSecret(input, hasKey)" in settings_ui
    assert "window.cwRefreshTmdbMetadataState = cwRefreshTmdbMetadataState" in settings_ui
    assert "cwRefreshTmdbMetadataState," in settings_ui
    assert "serverCfg?.tmdb?.api_key || serverCfg?.metadata?.tmdb_api_key" in settings_save

    opener = providers_ui.split("async function openMetadataProviderForm", 1)[1].split("const overlay = ensureAuthOverlay", 1)[0]
    assert 'if (info.key === "TMDB_METADATA")' in opener
    assert "await window.cwRefreshTmdbMetadataState?.({ force: true })" in opener
    assert opener.index("window.cwMetaProviderEnsure") < opener.index("window.cwRefreshTmdbMetadataState")

    after_enhance = providers_ui.split("enhanceConnectionModal(section, overlay, info.key);", 1)[1].split("requestAnimationFrame", 1)[0]
    assert "window.cwRefreshTmdbMetadataState?.()" in after_enhance


def test_connection_modals_mount_and_bind_before_opening() -> None:
    providers_ui = Path("assets/helpers/providers-ui.js").read_text("utf-8")

    ensure_overlay = providers_ui.split("function ensureAuthOverlay", 1)[1].split("function providerHome", 1)[0]
    assert "slot.appendChild(overlay);" in ensure_overlay
    assert "bindAuthPresentation(slot);" in ensure_overlay
    assert ensure_overlay.index("slot.appendChild(overlay);") < ensure_overlay.index("bindAuthPresentation(slot);")

    add_provider = providers_ui.split("async function openAddConnection", 1)[1].split("async function openAddMetadata", 1)[0]
    assert "bindAuthPresentation(slot);" in add_provider
    assert "await mountAuthProviders();" in add_provider
    assert 'openAuthOverlay("picker", "", "provider")' in add_provider
    assert add_provider.index("await mountAuthProviders();") < add_provider.index('openAuthOverlay("picker", "", "provider")')

    add_metadata = providers_ui.split("async function openAddMetadata", 1)[1].split("const ProvidersUI", 1)[0]
    assert "bindAuthPresentation(slot);" in add_metadata
    assert "await mountMetadataProviders();" in add_metadata
    assert 'openAuthOverlay("picker", "", "metadata")' in add_metadata
    assert add_metadata.index("await mountMetadataProviders();") < add_metadata.index('openAuthOverlay("picker", "", "metadata")')

    metadata_form = providers_ui.split("async function openMetadataProviderForm", 1)[1].split("function pruneEmptyProfileOnClose", 1)[0]
    assert "window.cwBuildTmdbPanel?.();" in metadata_form
    assert "await mountMetadataProviders(true);" in metadata_form
    assert metadata_form.index("window.cwBuildTmdbPanel?.();") < metadata_form.index("await mountMetadataProviders(true);")

    auth_form = providers_ui.split("async function openAuthProviderForm", 1)[1].split("async function openMetadataProviderForm", 1)[0]
    assert "await mountAuthProviders(true);" in auth_form


def test_maintenance_tracker_archive_uses_profile_selector_toolbar() -> None:
    root = Path(__file__).resolve().parents[1]
    modal_js = (root / "assets" / "js" / "modals" / "maintenance" / "index.js").read_text("utf-8")
    modal_css = (root / "assets" / "js" / "modals" / "maintenance" / "styles.css").read_text("utf-8")
    icon_select_js = (root / "assets" / "helpers" / "icon-select.js").read_text("utf-8")
    profile_select_js = (root / "assets" / "helpers" / "profile-select.js").read_text("utf-8")

    assert "tracker-archive-options" in modal_js
    assert "tracker-profile-control" in modal_js
    assert "archive-btn icon-only secondary" in modal_js
    assert "aria-label=\"Download tracker archive\"" in modal_js
    assert "aria-label=\"Import tracker archive\"" in modal_js
    assert "CW?.ProfileSelect?.enhanceProfile" in modal_js
    assert "cxm-tracker-profile-select" in modal_js
    assert "menuClassName: \"cxm-tracker-profile-menu\"" in modal_js
    assert "menuMinWidth: 220" in modal_js
    assert "button, input, label, a, summary, .cw-icon-select" in modal_js
    assert "tracker-archive-options { align-items: center; flex-wrap: nowrap" in modal_css
    assert "archive-btn.icon-only" in modal_css
    assert ".tracker-profile-control .cxm-tracker-profile-select" in modal_css
    assert "cxm-tracker-profile-menu" in modal_css
    assert "menuMinWidth" in icon_select_js
    assert "menuClassName" in icon_select_js
    assert "{ ...cfg, className:" in profile_select_js


def test_maintenance_tracker_archive_exports_and_imports_profile_storage(tmp_path: Path, monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    config_dir = tmp_path / "config"
    cache_dir = tmp_path / "cache"
    state_dir = tmp_path / "state"
    root = config_dir / ".cw_provider"
    profile_root = root / "profiles" / "CW-P01"
    snapshot_root = profile_root / "snapshots"
    snapshot_root.mkdir(parents=True)
    cfg = {"crosswatch": {"root_dir": str(root), "instances": {"CW-P01": {"label": "Desk"}}}}
    (config_dir / "config.json").write_text(json.dumps(cfg), "utf-8")
    (profile_root / "watchlist.json").write_text('{"items":{"tmdb:1":{"title":"One"}}}', "utf-8")
    (profile_root / "progress.json").write_text('{"items":{"tmdb:2":{"title":"Two"}}}', "utf-8")
    (snapshot_root / "20260101T000000Z-watchlist.json").write_text('{"items":{}}', "utf-8")

    class Stats:
        path = config_dir / "statistics.json"

    monkeypatch.setattr(
        maintenance_api,
        "_cw",
        lambda: (cache_dir, config_dir, state_dir, Stats(), lambda: {}, lambda *_args, **_kwargs: None),
    )
    monkeypatch.setattr(editor_service, "load_config", lambda: cfg)
    app = FastAPI()
    app.include_router(maintenance_api.router)
    client = TestClient(app)

    status = client.get("/api/maintenance/crosswatch-tracker", params={"provider_instance": "CW-P01"}).json()
    exported = client.get("/api/maintenance/crosswatch-tracker/export", params={"provider_instance": "CW-P01"})

    assert status["counts"]["state_files"] == 2
    assert exported.status_code == 200
    with zipfile.ZipFile(io.BytesIO(exported.content)) as archive:
        assert set(archive.namelist()) == {
            "watchlist.json",
            "progress.json",
            "snapshots/20260101T000000Z-watchlist.json",
        }

    for path in profile_root.rglob("*.json"):
        path.unlink()

    imported = client.post(
        "/api/maintenance/crosswatch-tracker/import",
        params={"provider_instance": "CW-P01"},
        files={"file": ("crosswatch-tracker.zip", exported.content, "application/zip")},
    ).json()

    assert imported["ok"] is True
    assert imported["states"] == 2
    assert imported["snapshots"] == 1
    assert (profile_root / "watchlist.json").exists()
    assert (profile_root / "progress.json").exists()
    assert (snapshot_root / "20260101T000000Z-watchlist.json").exists()


def test_crosswatch_profile_delete_removes_profile_storage(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "cw_provider"
    profile_root = root / "profiles" / "CW-P04"
    profile_root.mkdir(parents=True)
    (profile_root / "watchlist.json").write_text("{}", "utf-8")
    store: dict[str, Any] = {"crosswatch": {"root_dir": str(root), "instances": {"CW-P04": {"label": "Desk"}}}}
    monkeypatch.setattr(tracker_storage, "_DEFAULT_TRACKER_ROOT", str(root))

    def fake_load() -> dict[str, Any]:
        return json.loads(json.dumps(store))

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(provider_api, "load_config", fake_load)
    monkeypatch.setattr(provider_api, "save_config", fake_save)
    monkeypatch.setattr(provider_api, "update_config", _fake_update(provider_api.load_config, fake_save))
    monkeypatch.setattr(provider_api, "_invalidate_provider_cache", lambda provider: None)

    res = provider_api.api_provider_instances_delete("crosswatch", "CW-P04", request=_admin_req())

    assert res["ok"] is True
    assert res["storage"]["removed"] is True
    assert "CW-P04" not in store["crosswatch"]["instances"]
    assert not profile_root.exists()


def test_provider_instance_delete_removes_stable_uid(monkeypatch) -> None:
    store: dict[str, Any] = {
        "plex": {"instances": {"PLEX-P01": {"server_url": "http://plex"}}},
        "provider_instance_ids": {
            PROVIDER_INSTANCE_UID: {"provider": "PLEX", "instance": "PLEX-P01"},
        },
        "user_profiles": {
            ALICE_PROFILE_ID: {"label": "Alice", "instance_uids": [PROVIDER_INSTANCE_UID]},
        },
    }

    def fake_load() -> dict[str, Any]:
        return json.loads(json.dumps(store))

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(provider_api, "load_config", fake_load)
    monkeypatch.setattr(provider_api, "save_config", fake_save)
    monkeypatch.setattr(provider_api, "update_config", _fake_update(provider_api.load_config, fake_save))
    monkeypatch.setattr(provider_api, "_invalidate_provider_cache", lambda provider: None)

    res = provider_api.api_provider_instances_delete("plex", "PLEX-P01", request=_admin_req())

    assert res["ok"] is True
    assert "PLEX-P01" not in store["plex"]["instances"]
    assert store["provider_instance_ids"] == {}
    assert _profile_instances(store, ALICE_PROFILE_ID) == {}

    recreated = provider_api.api_provider_instances_create("plex", "PLEX-P01", {}, request=_admin_req())

    assert recreated["ok"] is True
    assert recreated["uid"] != PROVIDER_INSTANCE_UID
    assert provider_instances.user_profile_for_instance(store, "PLEX", "PLEX-P01") is None


def test_provider_instance_delete_blocks_sync_pair_reference(monkeypatch) -> None:
    store: dict[str, Any] = {
        "jellyfin": {"instances": {"JELLYFIN-P02": {"server": "http://jf"}}},
        "pairs": [
            {
                "id": "alice-sync",
                "enabled": False,
                "source": "CROSSWATCH",
                "source_instance": "CW-P01",
                "target": "JELLYFIN",
                "target_instance": "JELLYFIN-P02",
            }
        ],
    }

    def fake_load() -> dict[str, Any]:
        return json.loads(json.dumps(store))

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(provider_api, "load_config", fake_load)
    monkeypatch.setattr(provider_api, "save_config", fake_save)
    monkeypatch.setattr(provider_api, "update_config", _fake_update(provider_api.load_config, fake_save))
    monkeypatch.setattr(provider_api, "_invalidate_provider_cache", lambda provider: None)

    res = provider_api.api_provider_instances_delete("jellyfin", "JELLYFIN-P02", request=_admin_req())
    data = _loads_body(res.body)

    assert res.status_code == 409
    assert data["error"] == "provider_in_use"
    assert data["usages"][0]["feature"] == "sync_pair"
    assert "JELLYFIN-P02" in store["jellyfin"]["instances"]


def test_crosswatch_disconnect_removes_connection_and_storage(tmp_path: Path, monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    root = tmp_path / "cw_provider"
    root.mkdir()
    (root / "watchlist.json").write_text("{}", "utf-8")
    store: dict[str, Any] = {"crosswatch": {"root_dir": str(root), "retention_days": 30}}
    monkeypatch.setattr(tracker_storage, "_DEFAULT_TRACKER_ROOT", str(root))

    def fake_load() -> dict[str, Any]:
        return json.loads(json.dumps(store))

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(auth_api, "load_config", fake_load)
    monkeypatch.setattr(auth_api, "save_config", fake_save)

    app = FastAPI()
    auth_api.register_auth(app)
    data = TestClient(app).post("/api/crosswatch/disconnect").json()

    assert data["ok"] is True
    assert data["storage"]["removed"] is True
    assert "crosswatch" not in store
    assert not root.exists()


def test_crosswatch_connect_creates_connection_config(monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    store: dict[str, Any] = {}

    def fake_load() -> dict[str, Any]:
        return json.loads(json.dumps(store))

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(auth_api, "load_config", fake_load)
    monkeypatch.setattr(auth_api, "save_config", fake_save)

    app = FastAPI()
    auth_api.register_auth(app)
    data = TestClient(app).post("/api/crosswatch/connect").json()

    assert data == {"ok": True, "instance": "default"}
    assert store["crosswatch"]["root_dir"] == "/config/.cw_provider"
    assert store["crosswatch"]["connected"] is True
    assert store["crosswatch"]["retention_days"] == 30
    assert store["crosswatch"]["auto_snapshot"] is True
    assert store["crosswatch"]["max_snapshots"] == 64


def test_crosswatch_default_config_is_not_connected() -> None:
    from providers.auth._auth_CROSSWATCH import PROVIDER
    from providers.sync._mod_CROSSWATCH import OPS

    cfg = {"crosswatch": {"root_dir": "/config/.cw_provider", "connected": False}}

    assert PROVIDER.get_status(cfg).connected is False
    assert OPS.is_configured(cfg) is False


def test_crosswatch_config_default_does_not_auto_connect(tmp_path: Path, monkeypatch) -> None:
    import cw_platform.config_base as config_base

    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    cfg = config_base.load_config()

    assert cfg["crosswatch"]["connected"] is False

    (tmp_path / "config.json").write_text(json.dumps({"crosswatch": {"root_dir": "/tmp/cw"}}), "utf-8")
    migrated = config_base.load_config()

    assert migrated["crosswatch"]["connected"] is True


def test_analyzer_treats_crosswatch_as_tracker_provider() -> None:
    import services.analyzer as analyzer

    assert analyzer._is_tracker_to_media_server("CROSSWATCH@CW-P01", ["PLEX"])


def test_profile_labels_are_used_in_analyzer_and_events_modals() -> None:
    analyzer_js = Path("assets/js/modals/analyzer/index.js").read_text("utf-8")
    events_js = Path("assets/js/modals/events/index.js").read_text("utf-8")
    scrobbler_js = Path("assets/js/scrobbler.js").read_text("utf-8")
    dashboard_js = Path("assets/js/dashboard-widgets.js").read_text("utf-8")

    assert "/api/provider-instances" in analyzer_js
    assert "row.label" in analyzer_js
    assert "/api/provider-instances" in events_js
    assert "PROFILE_LABELS" in events_js
    assert "row.label" in events_js
    assert "x.profile_label || x.source_label" in scrobbler_js
    assert "x.sink_profile_label" in scrobbler_js
    assert "r.source_label" in scrobbler_js
    assert "r.sink_label" in scrobbler_js
    assert "configuredInstanceLabel(provider, instance)" in dashboard_js
    assert "currentConfig = cfg" in dashboard_js


def test_exporter_options_label_crosswatch_profiles_from_config(tmp_path: Path, monkeypatch) -> None:
    StateStore(tmp_path).save_state(
        {
            "providers": {
                "CROSSWATCH": {
                    "instances": {
                        "CW-P01": {
                            "history": {
                                "baseline": {
                                    "items": {
                                        "tmdb:1": {
                                            "type": "movie",
                                            "title": "Desk Movie",
                                            "ids": {"tmdb": "1"},
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    )
    monkeypatch.setattr(export_service, "CONFIG_DIR", tmp_path)
    monkeypatch.setattr(
        export_service,
        "load_config",
        lambda: {"crosswatch": {"instances": {"CW-P01": {"label": "Desk"}}}},
    )

    opts = export_service.api_export_options()

    assert {"id": "CW-P01", "label": "CW-P01 - Desk"} in opts["instances"]["CROSSWATCH"]


def test_editor_import_provider_options_include_instance_labels() -> None:
    editor_api = Path("api/editorAPI.py").read_text("utf-8")
    editor_importers = Path("assets/js/editor/importers.js").read_text("utf-8")

    assert "_provider_instance_label" in editor_api
    assert '"instance_label": instance_label' in editor_api
    assert '"display": label if instance == "default" else f"{label} ({instance_label})"' in editor_api
    assert '"instances": [' in editor_api
    assert '"label": _provider_instance_label(cfg, name, inst)' in editor_api
    assert 'typeof x === "object"' in editor_importers
    assert "x.label || x.display_label || id" in editor_importers


def test_non_admin_tabs_are_guarded_in_spa_router() -> None:
    core_js = Path("assets/helpers/core.js").read_text("utf-8")
    css = Path("assets/crosswatch.css").read_text("utf-8")

    assert "function canUseRouteTab(tab)" in core_js
    assert "window.CW?.AuthState?.read?.()" in core_js
    assert 'document.documentElement?.dataset?.cwRole === "user"' in core_js
    assert 'normalized === "watchlist"' in core_js
    assert 'normalized === "playback_progress"' in core_js
    assert "perms.dashboard !== false" in core_js
    assert "perms.playback !== false" in core_js
    assert "perms.watchlist !== false" in core_js
    assert "perms.write === true" in core_js
    assert "let tab = allowedRouteTab(name);" in core_js
    assert '!(tab === "playback_progress" && playbackAllowed)' in core_js
    assert 'window.addEventListener("cw:overview-profile-changed"' in core_js
    assert ".cw-nav-profile-link" in css
    assert "html[data-cw-role=user][data-cw-perm-write=off] #tab-snapshots" in css
    assert "html[data-cw-role=user][data-cw-perm-dashboard=off] #tab-main:not([data-cw-profile-home])" in css
    assert "html[data-cw-role=user][data-cw-perm-write=off] #ops-card .action-buttons" in css
    assert "html[data-cw-role=user][data-cw-perm-write=off] #cw-quick-add" in css
    assert "html[data-cw-role=user] #tab-playback_progress" not in css
    assert "html[data-cw-role=user][data-cw-perm-playback=off] #tab-playback_progress" in css
    assert "html[data-cw-role=user][data-cw-perm-write=off] #tab-snapshots" in css


def test_insights_footer_requires_stats_card() -> None:
    js = Path("assets/js/insights.js").read_text("utf-8")

    assert 'const stats = $("#stats-card");' in js
    assert 'if (!stats) {' in js
    assert '$("#insights-footer")?.remove();' in js
    assert 'stats.appendChild(foot);' in js
    assert '($("#stats-card") || d.body).appendChild(foot);' not in js


def test_non_admin_shell_omits_admin_only_modules() -> None:
    import ui_frontend

    html = ui_frontend.get_index_html(include_admin=False)

    assert '<html lang="en" data-cw-role="user"' in html
    assert "/assets/js/modals.js" not in html
    assert "/assets/auth/auth_loader.js" not in html
    assert "/assets/js/user-profiles.js" not in html
    assert "/assets/js/app-users.js" not in html
    assert "/assets/js/overview-profile.js" in html
    assert "/assets/js/dashboard-widgets.js" in html
    assert 'id="tab-snapshots"' not in html
    assert 'id="tab-playlists"' not in html
    assert 'id="tab-editor"' not in html
    assert 'id="tab-settings-menu"' not in html
    assert 'id="ops-card"' not in html
    assert 'id="stats-card"' not in html
    assert 'id="dashboard-widgets-card"' not in html
    assert 'id="page-snapshots"' not in html
    assert 'id="page-playlists"' not in html
    assert 'id="page-editor"' not in html
    assert 'id="page-settings"' not in html
    assert "runSync()" not in html
    assert "openAnalyzer()" not in html
    assert "openEvents()" not in html
    assert "openExporter()" not in html
    assert 'id="tab-main"' in html
    assert 'id="tab-main" class="tab" data-cw-profile-home="1" type="button"' in html
    assert '<a id="tab-main"' not in html
    assert 'id="tab-watchlist"' in html
    assert 'id="tab-playback_progress"' in html
    assert 'id="cw-managed-logout"' in html
    assert 'id="tab-about-menu"' not in html


def test_non_admin_shell_uses_initial_permissions() -> None:
    import ui_frontend

    watchlist_only = ui_frontend.get_index_html(
        include_admin=False,
        user={"is_admin": False, "profile_id": ALICE_PROFILE_ID, "permissions": {"dashboard": False, "watchlist": True, "playback": False, "write": False}},
    )
    dashboard_only = ui_frontend.get_index_html(
        include_admin=False,
        user={"is_admin": False, "profile_id": ALICE_PROFILE_ID, "permissions": {"dashboard": True, "watchlist": False, "playback": True, "write": False}},
    )
    full_access = ui_frontend.get_index_html(
        include_admin=False,
        user={"is_admin": False, "profile_id": ALICE_PROFILE_ID, "permissions": {"dashboard": True, "watchlist": True, "playback": True, "write": True}},
    )

    assert 'data-cw-perm-dashboard="off"' in watchlist_only
    assert 'data-cw-perm-watchlist="on"' in watchlist_only
    assert 'data-cw-perm-playback="off"' in watchlist_only
    assert 'data-cw-perm-write="off"' in watchlist_only
    assert f'data-cw-profile-id="{ALICE_PROFILE_ID}"' in watchlist_only
    assert 'id="tab-main"' in watchlist_only
    assert 'id="tab-main" class="tab" data-cw-profile-home="1" type="button"' in watchlist_only
    assert 'id="tab-playback_progress"' not in watchlist_only
    assert 'id="ops-card"' not in watchlist_only
    assert 'id="stats-card"' not in watchlist_only
    assert 'id="dashboard-widgets-card"' not in watchlist_only
    assert 'id="tab-watchlist"' in watchlist_only
    assert 'id="page-watchlist"' in watchlist_only

    assert 'data-cw-perm-dashboard="off"' in dashboard_only
    assert 'data-cw-perm-watchlist="off"' in dashboard_only
    assert 'data-cw-perm-playback="on"' in dashboard_only
    assert 'id="tab-main"' in dashboard_only
    assert 'id="tab-main" class="tab" data-cw-profile-home="1" type="button"' in dashboard_only
    assert 'id="tab-playback_progress"' in dashboard_only
    assert 'id="tab-watchlist"' not in dashboard_only
    assert 'id="ops-card"' not in dashboard_only
    assert 'id="stats-card"' not in dashboard_only
    assert 'id="dashboard-widgets-card"' not in dashboard_only
    assert 'id="page-watchlist"' not in dashboard_only

    assert 'data-cw-perm-write="on"' in full_access
    assert 'id="ops-card"' in full_access
    assert 'id="tab-snapshots"' in full_access
    assert 'id="tab-playlists"' in full_access
    assert 'id="tab-editor"' in full_access
    assert 'id="cw-managed-logout"' in full_access
    assert "/assets/js/modals.js" in full_access
    assert 'id="tab-settings-menu"' not in full_access
    assert 'id="tab-about-menu"' not in full_access
    assert 'id="tab-about"' not in full_access
    assert full_access.index('id="tab-main"') < full_access.index('id="tab-watchlist"')
    assert full_access.index('id="tab-watchlist"') < full_access.index('id="tab-playback_progress"')
    assert full_access.index('id="tab-playback_progress"') < full_access.index('id="tab-snapshots"')
    assert full_access.index('id="tab-snapshots"') < full_access.index('id="tab-playlists"')
    assert full_access.index('id="tab-playlists"') < full_access.index('id="tab-editor"')
    assert full_access.index('id="tab-editor"') < full_access.index('id="cw-managed-logout"')


def test_profile_nav_keeps_read_only_managed_links() -> None:
    import ui_frontend

    html = ui_frontend.get_profile_html(
        user={"is_admin": False, "profile_id": ALICE_PROFILE_ID, "username": "pascal", "permissions": {"dashboard": True, "watchlist": True, "playback": True, "write": False}},
    )

    assert 'class="tab active" href="/profile">Main</a>' in html
    assert 'class="tab" href="/?view=watchlist#watchlist">Watchlist</a>' in html
    assert 'class="tab" href="/?view=playback_progress#playback_progress">Playback</a>' in html
    assert 'href="/?view=watchlist#watchlist">View all</a>' in html
    assert 'href="/?view=playback_progress#playback_progress">View all</a>' in html
    assert 'id="cw-profile-logout"' in html
    assert 'href="/?main=1#main">Main</a>' not in html
    assert 'href="/?main=1#snapshots">Captures</a>' not in html


def test_profile_nav_uses_full_user_links_for_write_managed_user() -> None:
    import ui_frontend

    html = ui_frontend.get_profile_html(
        user={"is_admin": False, "profile_id": ALICE_PROFILE_ID, "username": "pascal", "permissions": {"dashboard": True, "watchlist": True, "playback": True, "write": True}},
    )

    assert 'class="tab active" href="/?main=1#main">Main</a>' in html
    assert 'href="/?main=1#watchlist">Watchlist</a>' in html
    assert 'href="/?main=1#playback_progress">Playback</a>' in html
    assert 'href="/?main=1#snapshots">Captures</a>' in html
    assert 'href="/?main=1#playlists">Playlists</a>' in html
    assert 'href="/?main=1#editor">Editor</a>' in html
    assert 'href="/?main=1#watchlist">View all</a>' in html
    assert 'href="/?main=1#playback_progress">View all</a>' in html
    assert 'href="/?main=1#settings">Settings</a>' not in html
    assert 'id="cw-profile-logout"' in html


def test_profile_page_redirects_stale_app_hashes_to_the_app_shell() -> None:
    js = Path("assets/js/profile-page.js").read_text("utf-8")

    assert 'window.location?.pathname !== "/profile"' in js
    assert '"playback_progress"' in js
    assert 'window.location.replace(`/?main=1${raw}`)' in js
    assert 'window.location.replace(`/?view=${encodeURIComponent(tab)}${raw}`)' in js


def test_profile_page_supports_admin_account() -> None:
    import ui_frontend

    html = ui_frontend.get_profile_html(
        user={"is_admin": True, "username": "admin", "display_name": "Administrator"},
    )

    assert 'data-cw-role="admin"' in html
    assert 'data-cw-profile-id=""' in html
    assert 'id="profile-role" class="cw-profile-role">Administrator</span>' in html
    assert 'class="tab active" href="/">Main</a>' in html
    assert 'href="/#watchlist">Watchlist</a>' in html
    assert 'href="/#playback_progress">Playback</a>' in html
    assert 'href="/#snapshots">Captures</a>' in html
    assert 'href="/#playlists">Playlists</a>' in html
    assert 'href="/#editor">Editor</a>' in html
    assert 'id="tab-settings" class="tab"' in html
    assert '<span>Settings</span><span class="tab-caret"' in html
    assert 'id="tab-about" class="tab"' in html
    assert '<span>About</span><span class="tab-caret"' in html
    assert 'id="cw-profile-logout"' not in html


def test_profile_admin_menu_opens_local_modals_without_main_flash() -> None:
    import subprocess
    import sys
    from html.parser import HTMLParser

    import ui_frontend

    class ScriptCollector(HTMLParser):
        def __init__(self) -> None:
            super().__init__()
            self.in_script = False
            self.scripts: list[str] = []

        def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
            if tag.lower() == "script":
                self.in_script = True
                self.scripts.append("")

        def handle_endtag(self, tag: str) -> None:
            if tag.lower() == "script":
                self.in_script = False

        def handle_data(self, data: str) -> None:
            if self.in_script and self.scripts:
                self.scripts[-1] += data

    html = ui_frontend.get_profile_html(
        user={"is_admin": True, "username": "admin", "display_name": "Administrator"},
    )

    assert "/assets/js/modals/core/styles.css" in html
    assert "/assets/js/modals.js" in html
    assert 'id="cw-help-overlay"' in html
    assert 'window.openHelp = () => window.cwOpenHelp?.();' in html
    assert 'else if (await ensureModals()) window.openAbout?.();' in html
    assert 'window.location.href = "/#" + (paths[pane] || "settings");' in html
    assert "window.__CW_VERSION__" not in html
    assert 'window.location.href = "/?main=1#"' not in html
    assert 'window.location.href = "/?main=1#about"' not in html

    parser = ScriptCollector()
    parser.feed(html)
    menu_script = next(script for script in parser.scripts if "cwToggleSettingsMenu" in script)
    script_path = Path(".pytest_tmp") / "profile-menu-script.js"
    script_path.parent.mkdir(exist_ok=True)
    script_path.write_text(menu_script, encoding="utf-8")
    result = subprocess.run(["node", "--check", str(script_path)], text=True, capture_output=True)
    if result.returncode != 0 and sys.platform.startswith("win"):
        result = subprocess.run(["cmd", "/c", "node", "--check", str(script_path)], text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_main_shell_has_early_hash_route_hint() -> None:
    import ui_frontend

    html = ui_frontend.get_index_html(include_admin=True)
    core_js = Path("assets/helpers/core.js").read_text("utf-8")

    assert "data-cw-initial-tab" in html
    assert 'const tabs = new Set(["watchlist", "playback_progress", "snapshots", "playlists", "editor", "settings"]);' in html
    assert 'html[data-cw-initial-tab]:not([data-cw-initial-tab="main"]) #ops-card' in html
    assert 'html[data-cw-initial-tab="settings"] #page-settings' in html
    assert "delete document.documentElement.dataset.cwInitialTab;" in core_js


def test_profile_2fa_setup_uses_spacious_qr_layout() -> None:
    js = Path("assets/js/profile-page.js").read_text("utf-8")
    css = Path("assets/css/profile-page.css").read_text("utf-8")

    assert "cw-profile-qr-code" in js
    assert "cw-profile-qr-copy" in js
    assert "cw-profile-qr-verify" in js
    assert "Scan with your authenticator app</strong><span>Or enter this setup key manually." in js
    assert ".cw-profile-qr{display:grid;grid-template-columns:auto minmax(0,1fr)" in css
    assert ".cw-profile-qr-code{display:grid;place-items:center;width:184px;height:184px" in css
    assert ".cw-profile-qr-copy{display:grid;gap:9px" in css
    assert ".cw-profile-qr-verify{display:grid;grid-template-columns:minmax(120px,1fr) auto" in css


def test_profile_last_watched_uses_provider_branding_icons() -> None:
    js = Path("assets/js/profile-page.js").read_text("utf-8")
    css = Path("assets/css/profile-page.css").read_text("utf-8")

    assert "window.CW?.ProviderMeta?.logoPath?.(provider)" in js
    assert "route.sinks.map(providerIconHtml)" in js
    assert "providerNode.innerHTML = badges;" in js
    assert "cw-profile-provider-badge" in css
    assert "cw-profile-provider-logo" in css
    assert "body.cw-profile-page #dashboard-widgets-card .cw-dash-layout-controls{right:104px;top:14px;gap:10px}" in css
    assert 'const art = watchlistPreviewArt(item) || poster(item, "w780");' in js
    helper = js[js.index("const watchlistPreviewArt ="):js.index("const heroBackdrop =")]
    assert "backdrop_url" not in helper and "background_url" not in helper and "fanart" not in helper
    assert 'return `/art/tmdb/${kind}/${encodeURIComponent(String(id))}?kind=backdrop&size=${encodeURIComponent(size)}&locale=${locale}${watchlistArtEvidence(item)}`;' in js
    assert "updated ${relTime(when)}" in js
    assert "cw-profile-watchlist-status" in js
    assert "cw-profile-watchlist-sync" in js
    assert "function syncedEpoch(item, fallbackEpoch = 0)" in js
    assert "watchlistRow(item, wall?.last_sync_epoch)" in js
    assert "#profile-watchlist .cw-profile-row{grid-template-columns:clamp(150px,28%,270px) minmax(0,1fr) auto;grid-template-rows:76px" in css
    assert "height:76px;min-height:76px;padding:0 14px 0 0;overflow:hidden;border-radius:14px;align-items:center;text-decoration:none!important" in css
    assert "#profile-watchlist .cw-profile-watchlist-art{position:relative;display:block;align-self:stretch;width:100%;height:100%;min-height:0;overflow:hidden;border-radius:13px 0 0 13px" in css
    assert "#profile-watchlist .cw-profile-watchlist-art img{display:block;width:100%;height:100%;min-height:0;object-fit:cover;object-position:center}" in css
    assert "#profile-watchlist .cw-profile-watchlist-sync{position:absolute;left:8px;top:8px;z-index:2" in css
    assert "#profile-watchlist .cw-profile-watchlist-copy{display:flex;flex-direction:column;justify-content:center;align-self:center;justify-self:start;min-width:0;min-height:0;height:100%;text-decoration:none!important}" in css
    assert "#profile-watchlist .cw-profile-watchlist-copy strong{font-size:15px;line-height:1.18;color:var(--profile-text);text-decoration:none!important}" in css
    assert "#profile-watchlist .cw-profile-watchlist-status{display:inline-flex;align-items:center;justify-content:center;align-self:center;justify-self:center;height:28px;min-height:28px" in css


def test_profile_watchlist_rows_reuse_widget_art_url() -> None:
    js = Path("assets/js/profile-page.js").read_text("utf-8")
    helper = Path("assets/helpers/watchlist-preview.js").read_text("utf-8")
    css = Path("assets/css/profile-page.css").read_text("utf-8")

    assert "artUrl,\n    gridArtUrl," in helper
    assert 'const cover = preview?.artUrl?.(item, "w342") || "/assets/img/placeholder_poster.svg";' in js
    assert "return preview?.gridArtUrl?.(item, size) || cover;" in js
    row = js[js.index("function watchlistRow(item, fallbackSyncEpoch = 0)"):js.index("function posterCard(item)")]
    assert "const art = watchlistWidgetArt(item);" in row
    assert "backdrop(item)" not in row
    assert "this.src='/assets/img/placeholder_poster.svg'" in row
    assert "body.cw-profile-page #dashboard-widgets-card #placeholder-card .poster *{text-decoration:none!important}" in css


def test_watchlist_widget_stacks_updated_stamp_on_profile_only() -> None:
    helper = Path("assets/helpers/watchlist-preview.js").read_text("utf-8")
    shared_css = Path("assets/crosswatch.css").read_text("utf-8")
    css = Path("assets/css/profile-page.css").read_text("utf-8")

    assert '<small class="wl-meta-stacked">${esc(stackedMeta)}</small>' in helper
    assert 'const stackedMeta = [typeLabel, item.year || ""].filter(Boolean).join(" - ");' in helper
    assert 'timeLabel ? `<small class="wl-meta-stacked-updated">${esc(`updated ${timeLabel}`)}</small>` : ""' in helper
    assert "#placeholder-card .poster .cap small.wl-meta-stacked,#placeholder-card .poster .cap small.wl-meta-stacked-updated{display:none !important}" in shared_css
    assert '[data-widget-view="grid"] .poster .wl-meta-compact{\n  display:none!important}' in css
    assert '.poster :is(.wl-meta-stacked,.wl-meta-stacked-updated){\n  display:block!important}' in css


def test_dashboard_layout_tools_are_profile_page_only() -> None:
    shared_css = Path("assets/crosswatch.css").read_text("utf-8")

    assert "body:not(.cw-profile-page) .cw-dashboard-layout-toolbar{display:none}" in shared_css


def test_quick_stats_counts_movies_and_shows_from_history_breakdown() -> None:
    js = Path("assets/js/profile-page.js").read_text("utf-8")

    stats = js[js.index("function renderQuickStats("):js.index('$("#profile-quick-stats").innerHTML')]
    assert "const movies = Number(breakdown.movies ?? watchtime.movies ?? sampleStats.movies) || 0;" in stats
    assert "const shows = Number(breakdown.shows ?? watchtime.shows ?? sampleStats.shows) || 0;" in stats
    assert "const anime = Number(breakdown.anime) || 0;" in stats
    assert "const episodes = Number(breakdown.episodes ?? sampleStats.episodes) || 0;" in stats
    assert 'const breakdown = insights?.features?.history?.breakdown || {};' in stats
    assert '["anime", "animation", "auto_awesome", "Anime", numberFmt.format(anime), "Total anime in your syncs"],' in stats
    assert "cw-profile-stat--anime" in Path("assets/css/profile-page.css").read_text("utf-8")


def test_profile_overview_paints_shimmer_skeletons_before_data_lands() -> None:
    js = Path("assets/js/profile-page.js").read_text("utf-8")
    css = Path("assets/css/profile-page.css").read_text("utf-8")

    assert "async function init() {\n    paintOverviewSkeletons();" in js
    assert "posterItems.clear();\n    paintOverviewSkeletons();" in js
    for host in ("#profile-progress", "#profile-watchlist", "#profile-quick-stats"):
        assert host in js[js.index("function paintOverviewSkeletons()"):js.index("function setAvatar(url)")]
    for primitive in ("cw-dash-skeleton", "cw-dash-skeleton-row", "cw-skel-block", "cw-skel-line--title", "cw-skel-line--meta", "cw-skel-dot"):
        assert primitive in js, primitive
    assert ".cw-profile-skel{position:relative;pointer-events:none;cursor:default}" in css
    assert "#profile-watchlist .cw-profile-skel>.cw-skel-block{align-self:stretch" in css
    assert "#profile-quick-stats .cw-profile-skel-icon{width:56px;height:56px;border-radius:50%}" in css


def test_overview_profile_helper_locks_managed_shell_to_profile() -> None:
    js = Path("assets/js/overview-profile.js").read_text("utf-8")

    assert "dataset?.cwProfileId" in js
    assert 'status?.is_admin || authUser?.is_admin' in js
    assert 'activeId = String(state?.profileId || authUser?.profile_id || SHELL_PROFILE_ID || "").trim();' in js
    assert 'else activeId = isAdmin ? storedId() : "";' in js
    assert 'if (!isAdmin) return false;' in js
    assert 'host.classList.toggle("hidden", !isAdmin);' in js
    assert 'isAdmin = Boolean(state.isAdmin) && !SHELL_MANAGED;' in js
    assert 'await window.__cwAuthBootstrapPromise;' in js
    assert 'cw-auth-setup-pending' in js
    assert "overview-profile-select" not in js


def test_quick_add_uses_authenticated_write_gate() -> None:
    js = Path("assets/js/main.js").read_text("utf-8")

    assert 'fetch("/api/app-auth/status"' in js
    assert "window.CW?.AuthState?.read?.()" in js
    assert "window.CW?.AuthState?.apply?.(status)" in js
    assert "authWriteAllowed" in js
    assert "auth.permissions.write === true" in js
    assert 'window.addEventListener("cw:auth-state-changed", syncVisibility);' in js
    assert "renderAll();\n  queuePairsRefresh();" in js


def test_managed_pair_policy_filters_to_profile_instances() -> None:
    from cw_platform.access_policy import filter_pairs_for_user, user_can_access_pair

    cfg = {
        "user_profiles": {
            ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"], "CROSSWATCH": ["CW-P01"]}},
        },
        "pairs": [
            {"id": "own", "profile_id": ALICE_PROFILE_ID, "source": "PLEX", "source_instance": "PLEX-P01", "target": "CROSSWATCH", "target_instance": "CW-P01"},
            {"id": "other", "profile_id": ALICE_PROFILE_ID, "source": "PLEX", "source_instance": "PLEX-P02", "target": "CROSSWATCH", "target_instance": "CW-P01"},
            {"id": "unassigned", "source": "PLEX", "source_instance": "PLEX-P01", "target": "CROSSWATCH", "target_instance": "CW-P01"},
        ],
    }
    _configured_refs(cfg, [("PLEX", "PLEX-P01"), ("PLEX", "PLEX-P02"), ("CROSSWATCH", "CW-P01")])
    user = {"is_admin": False, "profile_id": ALICE_PROFILE_ID, "permissions": {"write": True}}

    rows = filter_pairs_for_user(cfg, user, list(cfg["pairs"]))

    assert [row["id"] for row in rows] == ["own"]
    assert rows[0]["profile_label"] == "Alice"
    assert user_can_access_pair(cfg, user, cfg["pairs"][0]) is True
    assert user_can_access_pair(cfg, user, cfg["pairs"][1]) is False
    assert user_can_access_pair(cfg, user, cfg["pairs"][2]) is False


def test_managed_pair_endpoints_are_profile_scoped(monkeypatch) -> None:
    import api.syncAPI as sync_api

    cfg = {
        "user_profiles": {
            ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"], "CROSSWATCH": ["CW-P01"]}},
        },
        "pairs": [
            {"id": "own", "profile_id": ALICE_PROFILE_ID, "source": "PLEX", "source_instance": "PLEX-P01", "target": "CROSSWATCH", "target_instance": "CW-P01", "enabled": True},
            {"id": "other", "profile_id": ALICE_PROFILE_ID, "source": "PLEX", "source_instance": "PLEX-P02", "target": "CROSSWATCH", "target_instance": "CW-P01", "enabled": True},
        ],
    }
    _configured_refs(cfg, [("PLEX", "PLEX-P01"), ("PLEX", "PLEX-P02"), ("CROSSWATCH", "CW-P01"), ("CROSSWATCH", "CW-P02")])
    request = SimpleNamespace(state=SimpleNamespace(cw_user={"is_admin": False, "profile_id": ALICE_PROFILE_ID, "permissions": {"write": True}}))

    monkeypatch.setattr(sync_api, "_env", lambda: (lambda: json.loads(json.dumps(cfg)), lambda next_cfg: cfg.update(next_cfg)))

    listed = _loads_body(sync_api.api_pairs_list(cast(Any, request)).body)
    blocked = sync_api.api_pairs_update(
        "own",
        sync_api.PairPatch(target_instance="CW-P02"),
        cast(Any, request),
    )
    denied_delete = sync_api.api_pairs_delete("other", request=cast(Any, request))

    assert [row["id"] for row in listed] == ["own"]
    assert blocked == {"ok": False, "error": "profile_scope_denied"}
    assert denied_delete == {"ok": False, "error": "profile_scope_denied"}


def test_managed_events_are_profile_scoped_to_assigned_pairs() -> None:
    cfg = {
        "user_profiles": {
            ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"], "MDBLIST": ["MDBLIST-P01"]}},
        },
        "pairs": [
            {"id": "pair_plex_mdblist", "profile_id": ALICE_PROFILE_ID, "source": "PLEX", "source_instance": "PLEX-P01", "target": "MDBLIST", "target_instance": "MDBLIST-P01"},
            {"id": "pair_trakt_scrob", "source": "TRAKT", "source_instance": "default", "target": "SCROB", "target_instance": "default"},
        ],
    }
    _configured_refs(cfg, [("PLEX", "PLEX-P01"), ("PLEX", "PLEX-P02"), ("MDBLIST", "MDBLIST-P01"), ("TRAKT", "default"), ("SCROB", "default")])
    request = SimpleNamespace(state=SimpleNamespace(cw_user={"is_admin": False, "profile_id": ALICE_PROFILE_ID, "permissions": {"write": True}}))

    assert events_api._event_row_allowed(
        cfg,
        cast(Any, request),
        {"pair_key": "MDBLIST-PLEX", "source_provider": "PLEX", "destination_provider": "MDBLIST"},
        False,
    ) is True
    assert events_api._event_row_allowed(
        cfg,
        cast(Any, request),
        {"event_type": "plan_created", "source_provider": "PLEX", "source_instance": "PLEX-P01", "destination_provider": "MDBLIST", "destination_instance": "MDBLIST-P01"},
        False,
    ) is True
    assert events_api._event_row_allowed(
        cfg,
        cast(Any, request),
        {"event_type": "plan_created", "source_provider": "PLEX", "source_instance": "PLEX-P02", "destination_provider": "MDBLIST", "destination_instance": "MDBLIST-P01"},
        False,
    ) is False
    assert events_api._event_row_allowed(
        cfg,
        cast(Any, request),
        {"pair_key": "SCROB-TRAKT", "source_provider": "TRAKT", "destination_provider": "SCROB"},
        False,
    ) is False


def test_pair_endpoints_persist_profile_assignment(monkeypatch) -> None:
    import api.syncAPI as sync_api

    cfg = {
        "user_profiles": {
            ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"], "CROSSWATCH": ["CW-P01"]}},
            BOB_PROFILE_ID: {"label": "Bob", "instances": {"PLEX": ["PLEX-P02"], "MDBLIST": ["MDBLIST-P01"]}},
        },
        "pairs": [],
    }
    _configured_refs(cfg, [("PLEX", "PLEX-P01"), ("PLEX", "PLEX-P02"), ("CROSSWATCH", "CW-P01"), ("MDBLIST", "MDBLIST-P01")])
    saved: list[dict[str, Any]] = []

    def save(next_cfg: dict[str, Any]) -> None:
        data = json.loads(json.dumps(next_cfg))
        saved.append(data)
        cfg.clear()
        cfg.update(data)

    monkeypatch.setattr(sync_api, "_env", lambda: (lambda: cfg, save))

    admin_request = SimpleNamespace(state=SimpleNamespace(cw_user={"is_admin": True}))
    managed_request = SimpleNamespace(state=SimpleNamespace(cw_user={"is_admin": False, "profile_id": BOB_PROFILE_ID, "permissions": {"write": True}}))

    admin_res = sync_api.api_pairs_add(
        sync_api.PairIn(source="PLEX", source_instance="PLEX-P01", target="CROSSWATCH", target_instance="CW-P01", profile_id=ALICE_PROFILE_ID),
        request=cast(Any, admin_request),
    )
    managed_res = sync_api.api_pairs_add(
        sync_api.PairIn(source="PLEX", source_instance="PLEX-P02", target="MDBLIST", target_instance="MDBLIST-P01", profile_id=ALICE_PROFILE_ID),
        request=cast(Any, managed_request),
    )
    listed = _loads_body(sync_api.api_pairs_list(cast(Any, admin_request)).body)

    assert admin_res["ok"] is True
    assert managed_res["ok"] is True
    assert [row["profile_id"] for row in listed] == [ALICE_PROFILE_ID, BOB_PROFILE_ID]
    assert [row["profile_label"] for row in listed] == ["Alice", "Bob"]
    assert saved


def test_managed_request_user_uses_cw_user_state() -> None:
    from cw_platform.access_policy import request_user, user_can_access_instance

    user = {"is_admin": False, "profile_id": ALICE_PROFILE_ID, "permissions": {"write": True}}
    request = SimpleNamespace(state=SimpleNamespace(cw_user=user))
    cfg = {
        "user_profiles": {
            ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"]}},
        }
    }
    _configured_refs(cfg, [("PLEX", "PLEX-P01"), ("PLEX", "PLEX-P02")])

    assert request_user(request) is user
    assert user_can_access_instance(cfg, user, "PLEX", "PLEX-P01") is True
    assert user_can_access_instance(cfg, user, "PLEX", "PLEX-P02") is False


def test_managed_export_options_are_profile_scoped(tmp_path: Path, monkeypatch) -> None:
    StateStore(tmp_path).save_state(
        {
            "providers": {
                "PLEX": {
                    "watchlist": {"baseline": {"items": {"tmdb:1": {"type": "movie", "title": "Other"}}}},
                    "instances": {
                        "PLEX-P01": {"watchlist": {"baseline": {"items": {"tmdb:2": {"type": "movie", "title": "Alice"}}}}},
                        "PLEX-P02": {"watchlist": {"baseline": {"items": {"tmdb:3": {"type": "movie", "title": "Bob"}}}}},
                    },
                }
            }
        }
    )
    cfg = {
        "user_profiles": {
            ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"]}},
        },
        "plex": {"instances": {"PLEX-P01": {"label": "Alice"}, "PLEX-P02": {"label": "Bob"}}},
    }
    provider_instances.ensure_provider_instance_uids(cfg)
    request = SimpleNamespace(state=SimpleNamespace(cw_user={"is_admin": False, "profile_id": ALICE_PROFILE_ID, "permissions": {"write": True}}))

    monkeypatch.setattr(export_service, "CONFIG_DIR", tmp_path)
    monkeypatch.setattr(export_service, "load_config", lambda: cfg)

    opts = export_service.api_export_options(request=cast(Any, request))
    sample = export_service.api_export_sample(
        provider="PLEX",
        provider_instance="all",
        feature="watchlist",
        format="letterboxd",
        media_types="movie",
        request=cast(Any, request),
    )

    assert opts["providers"] == ["PLEX"]
    assert opts["instances"]["PLEX"] == [{"id": "PLEX-P01", "label": "PLEX-P01 - Alice"}]
    assert [row["title"] for row in sample["items"]] == ["Alice"]


def test_managed_import_targets_are_profile_scoped(monkeypatch) -> None:
    import services.importer as importer_service

    cfg = {
        "user_profiles": {
            ALICE_PROFILE_ID: {"label": "Alice", "instances": {"CROSSWATCH": ["CW-P01"]}},
        },
        "crosswatch": {"connected": True, "instances": {"CW-P01": {"label": "Alice"}, "CW-P02": {"label": "Bob"}}},
    }
    provider_instances.ensure_provider_instance_uids(cfg)
    request = SimpleNamespace(state=SimpleNamespace(cw_user={"is_admin": False, "profile_id": ALICE_PROFILE_ID, "permissions": {"write": True}}))

    monkeypatch.setattr(importer_service, "load_config", lambda: cfg)
    monkeypatch.setattr(importer_service, "_target_connected", lambda _cfg, _instance: True)

    opts = importer_service.api_import_options(request=cast(Any, request))

    assert opts["targets"] == [{"id": "CW-P01", "label": "Alice", "connected": True}]


def test_managed_editor_send_targets_are_profile_scoped(monkeypatch) -> None:
    cfg = {
        "user_profiles": {
            ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"]}},
        }
    }
    _configured_refs(cfg, [("PLEX", "PLEX-P01"), ("PLEX", "PLEX-P02")])
    request = SimpleNamespace(state=SimpleNamespace(cw_user={"is_admin": False, "profile_id": ALICE_PROFILE_ID, "permissions": {"write": True}}))
    targets = [
        {"provider": "PLEX", "instance": "PLEX-P01", "label": "Plex"},
        {"provider": "PLEX", "instance": "PLEX-P02", "label": "Plex"},
    ]

    monkeypatch.setattr(editor_api, "load_config", lambda: cfg)
    monkeypatch.setattr(editor_api, "_editor_send_targets", lambda _cfg, _feature: list(targets))

    data = editor_api.api_editor_send_providers(kind="watchlist", request=cast(Any, request))

    assert data["providers"] == [targets[0]]


def test_playing_card_uses_overview_profile_scope() -> None:
    js = Path("assets/js/playingcard.js").read_text("utf-8")

    assert "window.CW?.OverviewProfile?.id" in js
    assert "user_profile=${encodeURIComponent(id)}" in js
    assert "window.CW?.OverviewProfile?.ready" in js
    assert 'window.addEventListener("cw:overview-profile-changed"' in js
    assert "CARD.cacheScope !== scope" in js


def test_insights_snapshot_selector_saves_crosswatch_profile_choice(monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    store: dict[str, Any] = {
        "crosswatch": {
            "root_dir": "/config/.cw_provider",
            "instances": {"CW-P01": {"label": "Desk"}},
        }
    }

    def fake_load() -> dict[str, Any]:
        return json.loads(json.dumps(store))

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(insight_api, "_env", lambda: (None, fake_load, fake_save, lambda *a, **k: None))

    app = FastAPI()
    insight_api.register_insights(app)
    res = TestClient(app).post(
        "/api/crosswatch/select-snapshot",
        params={"feature": "ratings", "snapshot": "20260101T000000Z-ratings.json", "provider_instance": "CW-P01"},
    ).json()

    assert res == {
        "ok": True,
        "feature": "ratings",
        "snapshot": "20260101T000000Z-ratings.json",
        "provider_instance": "CW-P01",
    }
    assert store["crosswatch"]["instances"]["CW-P01"]["restore_ratings"] == "20260101T000000Z-ratings.json"
    assert "restore_ratings" not in store["crosswatch"]


def test_insights_crosswatch_snapshots_include_profiles(tmp_path: Path, monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    root = tmp_path / "cw_provider"
    profile_root = root / "profiles" / "CW-P01"
    (root / "snapshots").mkdir(parents=True)
    (profile_root / "snapshots").mkdir(parents=True)
    (root / "snapshots" / "20260101T000000Z-ratings.json").write_text("{}", "utf-8")
    (profile_root / "snapshots" / "20260202T000000Z-ratings.json").write_text("{}", "utf-8")
    cfg = {
        "crosswatch": {
            "root_dir": str(root),
            "instances": {"CW-P01": {"label": "Desk", "restore_ratings": "20260202T000000Z-ratings.json"}},
        }
    }

    monkeypatch.setattr(insight_api, "_env", lambda: (None, lambda: cfg, lambda _cfg: None, lambda *a, **k: None))

    app = FastAPI()
    insight_api.register_insights(app)
    snapshots = TestClient(app).get("/api/insights?limit_samples=0&history=0").json()["crosswatch_snapshots"]

    profile_rows = {row["id"]: row for row in snapshots["_profiles"]}
    assert profile_rows["CW-P01"]["label"] == "CW-P01 - Desk"
    assert profile_rows["CW-P01"]["root_dir"].replace("\\", "/").endswith("/cw_provider/profiles/CW-P01")
    assert snapshots["ratings"]["actual"] == "20260101T000000Z-ratings.json"
    assert snapshots["_by_profile"]["CW-P01"]["ratings"]["actual"] == "20260202T000000Z-ratings.json"
    assert snapshots["_by_profile"]["CW-P01"]["ratings"]["provider_instance"] == "CW-P01"


def test_insights_are_profile_scoped(monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    cfg: dict[str, Any] = {
        "user_profiles": {
            ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"], "MDBLIST": ["MDBLIST-P01"]}},
        },
        "pairs": [
            {"id": "own", "source": "PLEX", "source_instance": "PLEX-P01", "target": "MDBLIST", "target_instance": "MDBLIST-P01"},
            {"id": "other", "source": "PLEX", "source_instance": "PLEX-P02", "target": "SIMKL", "target_instance": "SIMKL-P01"},
        ],
    }
    _configured_refs(cfg, [("PLEX", "PLEX-P01"), ("PLEX", "PLEX-P02"), ("MDBLIST", "MDBLIST-P01"), ("SIMKL", "SIMKL-P01")])
    state = {
        "providers": {
            "PLEX": {
                "instances": {
                    "PLEX-P01": {"watchlist": {"baseline": {"items": {"a": {"type": "movie", "title": "Alice"}}}}},
                    "PLEX-P02": {"watchlist": {"baseline": {"items": {"b": {"type": "movie", "title": "Bob"}}}}},
                }
            },
            "MDBLIST": {"instances": {"MDBLIST-P01": {"watchlist": {"baseline": {"items": {"a": {"type": "movie", "title": "Alice"}}}}}}},
            "SIMKL": {"instances": {"SIMKL-P01": {"watchlist": {"baseline": {"items": {"b": {"type": "movie", "title": "Bob"}}}}}}},
        },
        "wall": [
            {"key": "a", "type": "movie", "sources_by_provider": {"plex": ["PLEX-P01"], "mdblist": ["MDBLIST-P01"]}},
            {"key": "b", "type": "movie", "sources_by_provider": {"plex": ["PLEX-P02"], "simkl": ["SIMKL-P01"]}},
        ],
    }

    class Stats:
        data = {
            "samples": [{"ts": 1, "count": 99}],
            "events": [
                {"feature": "watchlist", "action": "add", "source": "PLEX", "source_instance": "PLEX-P01", "key": "a"},
                {"feature": "watchlist", "action": "add", "source": "PLEX", "source_instance": "PLEX-P02", "key": "b"},
            ],
        }

    cw = SimpleNamespace(STATS=Stats(), REPORT_DIR=None, CACHE_DIR=None, _load_wall_snapshot=lambda: state["wall"], _append_log=lambda *_args, **_kwargs: None)
    monkeypatch.setattr(insight_api, "_env", lambda: (cw, lambda: cfg, lambda _cfg: None, lambda *a, **k: None))
    monkeypatch.setattr(insight_api, "_load_state_features", lambda _features: state)

    app = FastAPI()
    insight_api.register_insights(app)
    data = TestClient(app).get(f"/api/insights?limit_samples=5&history=0&user_profile={ALICE_PROFILE_ID}").json()

    assert data["user_profile"] == ALICE_PROFILE_ID
    assert data["watchtime"]["movies"] == 1
    assert data["instances_by_provider"] == {"mdblist": ["MDBLIST-P01"], "plex": ["PLEX-P01"]}
    assert data["features"]["watchlist"]["providers"] == {"mdblist": 1, "plex": 1}
    assert data["events"][0]["key"] == "a"
    assert all("simkl" not in block for block in (data["providers_by_feature"].values()))
    assert data["series"] == []


def test_insights_snapshot_modal_is_crosswatch_profile_aware() -> None:
    insights_js = Path("assets/js/insights.js").read_text("utf-8")

    assert "CW_SNAPSHOT_PROFILE_KEY" in insights_js
    assert "_by_profile" in insights_js
    assert "cw-snap-profile-select" in insights_js
    assert "provider_instance=" in insights_js
    assert "user_profile=${encodeURIComponent(overviewProfileId())}" in insights_js
    assert '"/config/.cw_provider/snapshots"' not in insights_js


def test_status_probes_resolve_provider_without_configured_default() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from api import probesAPI as probes

    cases = [
        ("JELLYFIN", "jellyfin", "JELLYFIN-P01", {"server": "http://jf:8096", "access_token": "t"}),
        ("EMBY", "emby", "EMBY-P01", {"server": "http://emby:8096", "access_token": "t"}),
        ("PLEX", "plex", "PLEX-P01", {"account_token": "t"}),
        ("TRAKT", "trakt", "TRAKT-P01", {"access_token": "t", "client_id": "c"}),
        ("SIMKL", "simkl", "SIMKL-P01", {"access_token": "t", "client_id": "c"}),
        ("MDBLIST", "mdblist", "MDBLIST-P01", {"api_key": "k"}),
        ("TAUTULLI", "tautulli", "TAUTULLI-P01", {"server_url": "http://t:8181", "api_key": "k"}),
    ]

    for prov, ck, inst, blk in cases:
        cfg = {ck: {"instances": {inst: dict(blk)}}}
        probes.invalidate_provider_caches(ck)

        app = FastAPI()
        probes.register_probes(app, lambda cfg=cfg: cfg)
        block = TestClient(app).get("/api/status?fresh=1").json()["providers"][prov]

        summary = block["instances_summary"]
        assert list(block["instances"]) == [inst], prov
        assert summary["total"] == 1, prov
        assert summary["rep"] == inst, prov
        assert block["rep_instance"] == inst, prov
        assert block["instances"][inst]["probed"] is True, prov


def test_status_probes_keep_partially_configured_named_instances() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from api import probesAPI as probes

    cfg = {
        "kodi": {"instances": {"KODI-P01": {"server": "http://kodi:8080"}}},
        "tmdb_sync": {"instances": {"TMDB-P01": {"api_key": "k"}}},
    }
    for ck in ("kodi", "tmdb"):
        probes.invalidate_provider_caches(ck)

    app = FastAPI()
    probes.register_probes(app, lambda: cfg)
    providers = TestClient(app).get("/api/status?fresh=1").json()["providers"]

    assert list(providers["KODI"]["instances"]) == ["KODI-P01"]
    assert list(providers["TMDB"]["instances"]) == ["TMDB-P01"]


def test_connection_cards_show_default_and_friendly_profile_names() -> None:
    ui = Path("assets/helpers/providers-ui.js").read_text("utf-8")

    assert "function profileFriendlyName(cfg, provider, id)" in ui
    assert "PROFILE_PILL_LIMIT" in ui
    assert 'is-overflow' in ui
    # default is no longer stripped from the card badges
    assert '.filter((id) => String(id || "").trim().toLowerCase() !== "default")' not in ui

    css = Path("assets/css/auth-providers.css").read_text("utf-8")
    pill = css.split(".cw-auth-profile-pill{", 1)[1].split("}", 1)[0]
    assert "text-overflow:ellipsis" in pill
    assert "overflow:hidden" in pill
    strip = css.split(".cw-auth-profile-strip{", 1)[1].split("}", 1)[0]
    assert "max-width:" in strip and "overflow:hidden" in strip


def test_connection_delete_guard_ignores_unconfigured_default() -> None:
    ui = Path("assets/helpers/providers-ui.js").read_text("utf-8")
    guard = ui.split("function connectionDeleteBlockedByProfiles(", 1)[1].split("\n  }", 1)[0]

    assert 'if (!ids.includes("default")) return false;' in guard
    assert 'ids.some((id) => id !== "default")' in guard


def test_profile_delete_refreshes_config_cache() -> None:
    shared = Path("assets/auth/auth.shared.js").read_text("utf-8")
    handler = shared.split("btnDel.addEventListener(", 1)[1].split("\n      });", 1)[0]

    assert "invalidateConfigCache" in handler
    assert "Config?.load?.(true)" in handler
    assert "auth-changed" in handler


def test_profile_switcher_skips_unconfigured_default() -> None:
    shared = Path("assets/auth/auth.shared.js").read_text("utf-8")

    assert 'if (want === "default" && defaultRow.configured === false)' in shared
    assert 'item.id !== "default" && item.configured' in shared


def test_auth_remount_preserves_open_connection_overlay() -> None:
    ui = Path("assets/helpers/providers-ui.js").read_text("utf-8")
    body = ui.split("async function mountAuthProviders(", 1)[1].split("initMountedAuthSections(slot);", 1)[0]

    assert 'const overlay = slot.querySelector(":scope > .cw-auth-overlay");' in body
    assert "overlay?.remove();" in body
    assert body.index("overlay?.remove();") < body.index("slot.innerHTML = authHtml;")
    assert "liveSectionIds" in body
    assert "slot.appendChild(overlay);" in body


def test_status_probes_ignore_pair_reference_to_unconfigured_default() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from api import probesAPI as probes

    cfg = {
        "jellyfin": {"instances": {"JELLYFIN-P01": {"server": "http://jf:8096", "access_token": "t"}}},
        "pairs": [{"enabled": True, "source": "JELLYFIN", "target": "TRAKT", "features": {"watchlist": {"enable": True}}}],
        "trakt": {"access_token": "t", "client_id": "c"},
    }
    probes.invalidate_provider_caches("jellyfin")

    app = FastAPI()
    probes.register_probes(app, lambda: cfg)
    block = TestClient(app).get("/api/status?fresh=1").json()["providers"]["JELLYFIN"]

    assert block["rep_instance"] == "JELLYFIN-P01"
    assert block["instances_summary"]["rep"] == "JELLYFIN-P01"
    assert block["instances"]["default"]["configured"] is False
    assert block["instances"]["default"]["used"] is True
    assert block["instances"]["JELLYFIN-P01"]["configured"] is True


def test_status_probes_keep_used_default_when_it_is_configured() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from api import probesAPI as probes

    cfg = {
        "jellyfin": {
            "server": "http://main:8096",
            "access_token": "t",
            "instances": {"JELLYFIN-P01": {"server": "http://jf:8096", "access_token": "t"}},
        },
        "pairs": [{"enabled": True, "source": "JELLYFIN", "source_instance": "default", "target": "TRAKT", "features": {"watchlist": {"enable": True}}}],
        "trakt": {"access_token": "t", "client_id": "c"},
    }
    probes.invalidate_provider_caches("jellyfin")

    app = FastAPI()
    probes.register_probes(app, lambda: cfg)
    block = TestClient(app).get("/api/status?fresh=1").json()["providers"]["JELLYFIN"]

    assert block["rep_instance"] == "default"
    assert block["instances"]["default"]["configured"] is True


def test_sync_visibility_skips_redundant_select_rebuilds() -> None:
    core = Path("assets/helpers/core.js").read_text("utf-8")
    fn = core.split("function applySyncVisibility()", 1)[1].split("\n  }", 1)[0]

    assert "const wanted = PROVIDER_ORDER.filter((key) => allowed.has(key));" in fn
    assert "current.every((value, i) => value === wanted[i])" in fn
    assert "card.dataset?.prov || card.dataset?.syncProv" in fn
    assert "if (card.style.display !== display)" in fn
    assert "const named = host.querySelectorAll(\".prov-card\");" in fn


def test_auth_dots_batch_layout_reads_before_writes() -> None:
    status = Path("assets/js/main-status.js").read_text("utf-8")
    apply_fn = status.split("function applyAuthDots(cfg)", 1)[1].split("\n  }", 1)[0]
    read_fn = status.split("function readDotTargets()", 1)[1].split("\n  }", 1)[0]

    assert "const targets = readDotTargets();" in apply_fn
    assert "const configured = configuredProviderSet(cfg);" in apply_fn
    assert apply_fn.index("readDotTargets()") < apply_fn.index("writeDot(")
    assert "getComputedStyle(head).display" in read_fn
    assert "head.style" not in read_fn


def test_status_provider_visibility_reuses_configured_set() -> None:
    status = Path("assets/js/main-status.js").read_text("utf-8")

    assert "function configuredProviderSet(cfg = getCachedConfig())" in status
    assert "isStatusProviderVisible(key, data, cfg = getCachedConfig(), configured = null)" in status
    assert "isStatusProviderVisible(k, providers[k], cfg, configured)" in status
    assert "typeof set.has === \"function\"" in status


def _profile_scoped_webhook_cfg(assigned: bool = True) -> dict[str, Any]:
    profile_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    instance_uid = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    webhook: dict[str, Any] = {"profiles": {"plex": {"default": {"enabled": True, "sinks": ["trakt"]}}}}
    if assigned:
        webhook["user_profile_assignments"] = {"plex:default:trakt:default": profile_id}
    return {
        "plex": {"server_url": "http://plex:32400", "account_token": "t", "instances": {}},
        "trakt": {"client_id": "c", "client_secret": "s", "access_token": "t", "instances": {}},
        "provider_instance_ids": {instance_uid: {"provider": "PLEX", "instance": "default"}},
        # the profile owns the instance in both cases; only the explicit assignment differs
        "user_profiles": {profile_id: {"label": "Alice", "instance_uids": [instance_uid]}},
        "scrobble": {"webhook": webhook},
    }


def test_watcher_route_rows_expose_user_profile_label() -> None:
    import sys

    sys.path.insert(0, "tests")
    from test_scrobble_account_scope import _cfg

    from api.scrobblerManagementAPI import _normalized_routes

    assigned = _normalized_routes(_cfg(profile=True, whitelist=["dad"]), None)[0]
    assert assigned["user_profile_label"] == "Alice"

    # an admin picking a profile-owned instance must NOT mark the route as that profile
    derived = _normalized_routes(_cfg(profile=True, whitelist=["dad"], assign=False), None)[0]
    assert derived["user_profile_label"] == ""
    assert derived["user_profile_id"] == ""

    plain = _normalized_routes(_cfg(profile=False, whitelist=None), None)[0]
    assert plain["user_profile_label"] == ""
    assert plain["user_profile_id"] == ""


def test_webhook_rows_expose_user_profile_without_touching_instance_label() -> None:
    from api.scrobblerManagementAPI import _webhook_cards

    request = SimpleNamespace(base_url="http://cw:9898/", headers={})

    row = _webhook_cards(_profile_scoped_webhook_cfg(True), request)[0]
    assert row["user_profile_label"] == "Alice"
    assert row["profile_label"] == "Default"

    # owned instance but no explicit assignment must stay unlabelled
    bare = _webhook_cards(_profile_scoped_webhook_cfg(False), request)[0]
    assert bare["user_profile_label"] == ""
    assert bare["profile_label"] == "Default"


def test_route_and_webhook_cards_render_profile_connector() -> None:
    js = Path("assets/js/scrobbler.js").read_text("utf-8")

    assert "function profileConnector(row)" in js
    assert "user_profile_label" in js
    # webhook cards iterate x, watcher routes iterate r
    webhooks = js.split("function renderWebhooks(", 1)[1].split("function renderRoutes(", 1)[0]
    routes = js.split("function renderRoutes(", 1)[1]
    assert "${profileConnector(x)}" in webhooks
    assert "${profileConnector(r)}" in routes
    assert '<div class="sc2-rt-conn"><span class="sc2-rt-conn-line"></span></div>' not in webhooks


def test_route_card_accent_is_purple_only_with_user_profile() -> None:
    css = Path("assets/css/components.css").read_text("utf-8")

    # default and live accents stay green for cards without a user profile
    live = css.split(".sc2-route-card.is-live:not(.sc2-route-add-card)::after{", 1)[1].split("}", 1)[0]
    assert "57d889" in live and "124,92,255" not in live

    # purple is opt-in via the card class, and never overrides the disabled state
    assert ".sc2-route-card.has-user-profile:not(.is-disabled):not(.sc2-route-add-card)::after" in css
    assert ".sc2-route-card.has-user-profile.is-live:not(.is-disabled):not(.sc2-route-add-card)::after" in css

    # the connector still absorbs free space so the sink endpoint stays flush right
    assert ".sc2-rt-conn.has-profile{min-width" not in css
    assert "flex:0 1 150px" not in css

    chip = css.split(".sc2-rt-profile{", 1)[1].split("}", 1)[0]
    assert "max-width:min(100%,150px)" in chip
    name = css.split(".sc2-rt-profile-name{", 1)[1].split("}", 1)[0]
    assert "text-overflow:ellipsis" in name


def test_cards_tag_user_profile_class() -> None:
    js = Path("assets/js/scrobbler.js").read_text("utf-8")
    webhooks = js.split("function renderWebhooks(", 1)[1].split("function renderRoutes(", 1)[0]
    routes = js.split("function renderRoutes(", 1)[1]

    assert '${x.user_profile_label ? "has-user-profile" : ""}' in webhooks
    assert '${r.user_profile_label ? "has-user-profile" : ""}' in routes


def test_pair_config_profile_selector_sits_before_mode_control() -> None:
    js = Path("assets/js/modals/pair-config/index.js").read_text("utf-8")
    css = Path("assets/js/modals/pair-config/styles.css").read_text("utf-8")

    assert "cx-user-profile-slot" in js
    assert js.index("cx-user-profile-slot") < js.index("flow-mode-inline")
    assert "slot.appendChild(row)" in js
    assert ".flow-control-row" in css
    assert "cx-user-profile-select" in js
    assert "resetPairUserProfileControl(state)" in js
    assert 'sel.value=""' in js
    assert "selected_user_profile_id" in js
    assert "applying_user_profile" in js
    assert "eligiblePairUserProfiles(state)" in js
    assert "profileCanOwnCurrentPair(profile,state)" in js
    assert ".cx-user-profile-select" in css
    assert "cx-user-profile-label" not in js
    assert ">User profile<" not in js
    assert "display_label||x.label||x.id" in js
    assert 'title="${escHTML(row.id)}"' in js

def test_scrobbler_modals_have_conditional_user_profile_selector() -> None:
    route_js = Path("assets/js/modals/scrobbler-route/index.js").read_text("utf-8")
    webhook_js = Path("assets/js/modals/scrobbler-webhook/index.js").read_text("utf-8")
    css = Path("assets/css/components.css").read_text("utf-8")

    assert "/api/user-profiles" in route_js
    assert "/api/user-profiles" in webhook_js
    assert "id=\"scr-user-profile\"" in route_js
    assert "id=\"scw-user-profile\"" in webhook_js
    assert "if (!userProfiles.length) return \"\"" in route_js
    assert "if (!userProfiles.length || props.mode === \"edit\") return \"\"" in webhook_js
    assert "selectedUserProfileId" in route_js
    assert "selectedUserProfileId" in webhook_js
    assert 'selectedUserProfileId = applyUserProfile(selected) ? selected : ""' in route_js
    assert 'selectedUserProfileId = applyUserProfile(selected) ? selected : ""' in webhook_js
    assert '${p.id === current ? "selected" : ""}' in route_js
    assert '${p.id === current ? "selected" : ""}' in webhook_js
    assert "profileOptionLabel(profile)" in route_js
    assert "profileOptionLabel(profile)" in webhook_js
    assert "display_label || profile?.label" in route_js
    assert "display_label || profile?.label" in webhook_js
    assert 'title="${esc(p.instance)}"' in route_js
    assert 'title="${esc(p.instance)}"' in webhook_js
    assert ".scrm-profile-row" in css


def test_version_stamp_file_beats_stale_env(tmp_path, monkeypatch) -> None:
    from api import versionAPI

    stamp = tmp_path / "VERSION"
    monkeypatch.setattr(versionAPI, "VERSION_FILE", stamp)

    # a container env pinned by Portainer or Watchtower must not win
    stamp.write_text("v0.11.1", encoding="utf-8")
    monkeypatch.setenv("APP_VERSION", "v0.11.0")
    assert versionAPI.resolve_current_version() == "v0.11.1"

    # without a stamp the env still works, for bare python runs
    stamp.unlink()
    assert versionAPI.resolve_current_version() == "v0.11.0"

    # an empty or truncated stamp must not blank the version
    stamp.write_text("   ", encoding="utf-8")
    assert versionAPI.resolve_current_version() == "v0.11.0"

    monkeypatch.delenv("APP_VERSION", raising=False)
    assert versionAPI.resolve_current_version() == versionAPI.FALLBACK_VERSION

    # dev images build without the arg, so the placeholder must fall through
    stamp.write_text("v0.0.0", encoding="utf-8")
    monkeypatch.setenv("APP_VERSION", "v0.0.0")
    assert versionAPI.resolve_current_version() == versionAPI.FALLBACK_VERSION
    stamp.write_text("0.0.0", encoding="utf-8")
    assert versionAPI.resolve_current_version() == versionAPI.FALLBACK_VERSION


def test_dockerfile_stamps_version_into_the_image() -> None:
    dockerfile = Path("Dockerfile").read_text("utf-8")

    assert 'RUN printf \'%s\' "${APP_VERSION}" > /app/VERSION' in dockerfile
    # the stamp must be written in the runtime stage, after the app is copied
    assert dockerfile.index("COPY --from=appsrc") < dockerfile.index("/app/VERSION")
    assert "VERSION" in Path(".gitignore").read_text("utf-8")


def test_filter_pairs_for_profile_scopes_by_assignment_and_instances() -> None:
    from cw_platform.access_policy import filter_pairs_for_profile

    cfg = {
        "user_profiles": {
            ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"], "CROSSWATCH": ["CW-P01"]}},
            BOB_PROFILE_ID: {"label": "Bob", "instances": {"PLEX": ["PLEX-P02"]}},
        },
    }
    _configured_refs(cfg, [("PLEX", "PLEX-P01"), ("PLEX", "PLEX-P02"), ("CROSSWATCH", "CW-P01")])
    pairs = [
        {"id": "own", "profile_id": ALICE_PROFILE_ID, "source": "PLEX", "source_instance": "PLEX-P01", "target": "CROSSWATCH", "target_instance": "CW-P01"},
        {"id": "wrong_instance", "profile_id": ALICE_PROFILE_ID, "source": "PLEX", "source_instance": "PLEX-P02", "target": "CROSSWATCH", "target_instance": "CW-P01"},
        {"id": "unassigned", "profile_id": "", "source": "PLEX", "source_instance": "PLEX-P01", "target": "CROSSWATCH", "target_instance": "CW-P01"},
        {"id": "other_profile", "profile_id": BOB_PROFILE_ID, "source": "PLEX", "source_instance": "PLEX-P02", "target": "CROSSWATCH", "target_instance": "CW-P01"},
    ]

    kept = [row.get("id") for row in filter_pairs_for_profile(cfg, ALICE_PROFILE_ID, pairs)]
    assert kept == ["own"]

    assert [row.get("id") for row in filter_pairs_for_profile(cfg, "", pairs)] == ["own", "wrong_instance", "unassigned", "other_profile"]


def test_status_probe_scope_accepts_admin_requested_profile(monkeypatch) -> None:
    import api.probesAPI as probes_api

    cfg = {
        "user_profiles": {ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"]}}},
    }
    _configured_refs(cfg, [("PLEX", "PLEX-P01")])

    from api.appAuthAPI import effective_user_profile_id

    assert effective_user_profile_id(cfg, None, ALICE_PROFILE_ID) == ALICE_PROFILE_ID
    assert effective_user_profile_id(cfg, None, "") == ""

    src = Path("api/probesAPI.py").read_text("utf-8")
    assert 'def api_status(request: Request, fresh: int = Query(0), user_profile: str = Query(""))' in src
    assert "scope_profile = _status_scope_profile(cfg0, request, user_profile)" in src
    assert "managed_scope = bool(scope_profile) or bool(scoped_user and not scoped_user.get(\"is_admin\"))" in src
    assert "filter_pairs_for_profile(cfg, scope_profile," in src
    assert probes_api is not None


def test_status_frontend_sends_selected_profile() -> None:
    api_js = Path("assets/helpers/api.js").read_text("utf-8")
    core_js = Path("assets/helpers/core.js").read_text("utf-8")

    assert "`/api/status?user_profile=${encodeURIComponent(profile)}`" in api_js
    assert '`${KEY.status}:${profile || "all"}`' in api_js
    assert "key === KEY.pairs || key === KEY.status" in api_js
    assert "const statusCacheKey = ()" in core_js
    assert "localStorage.setItem(statusCacheKey()" in core_js
    assert 'window.CW?.Cache?.invalidate?.(["status"])' in core_js


def _view_as_request(user: dict[str, Any], *, header: str = "", query: str = "") -> Any:
    return SimpleNamespace(
        state=SimpleNamespace(cw_user=user),
        headers={"x-cw-view-as": header} if header else {},
        query_params={"user_profile": query} if query else {},
    )


def _view_as_cfg() -> dict[str, Any]:
    cfg = {"user_profiles": {ALICE_PROFILE_ID: {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"]}}}}
    _configured_refs(cfg, [("PLEX", "PLEX-P01")])
    return cfg


def test_request_user_impersonates_profile_for_admin(monkeypatch) -> None:
    from cw_platform import access_policy

    cfg = _view_as_cfg()
    monkeypatch.setattr("cw_platform.config_base.load_config", lambda *a, **k: cfg)
    admin = {"id": "administrator", "username": "admin", "is_admin": True}

    plain = access_policy.request_user(_view_as_request(admin))
    assert plain is admin

    scoped = access_policy.request_user(_view_as_request(admin, header=ALICE_PROFILE_ID))
    assert scoped is not None
    assert scoped["is_admin"] is False
    assert scoped["profile_id"] == ALICE_PROFILE_ID
    assert scoped["view_as"] is True
    assert scoped["permissions"]["write"] is True

    via_query = access_policy.request_user(_view_as_request(admin, query=ALICE_PROFILE_ID))
    assert via_query is not None and via_query["profile_id"] == ALICE_PROFILE_ID

    unknown = access_policy.request_user(_view_as_request(admin, header="0" * 32))
    assert unknown is admin


def test_managed_user_cannot_impersonate_another_profile(monkeypatch) -> None:
    from cw_platform import access_policy

    cfg = _view_as_cfg()
    cfg["user_profiles"][BOB_PROFILE_ID] = {"label": "Bob", "instances": {"PLEX": ["PLEX-P01"]}}
    monkeypatch.setattr("cw_platform.config_base.load_config", lambda *a, **k: cfg)
    managed = {"id": "bob", "is_admin": False, "profile_id": BOB_PROFILE_ID, "permissions": {"write": False}}

    scoped = access_policy.request_user(_view_as_request(managed, header=ALICE_PROFILE_ID))
    assert scoped is managed
    assert scoped["profile_id"] == BOB_PROFILE_ID


def test_view_as_leaves_audit_identity_untouched(monkeypatch) -> None:
    from cw_platform import access_policy

    cfg = _view_as_cfg()
    monkeypatch.setattr("cw_platform.config_base.load_config", lambda *a, **k: cfg)
    admin = {"id": "administrator", "username": "admin", "is_admin": True}
    request = _view_as_request(admin, header=ALICE_PROFILE_ID)

    access_policy.request_user(request)
    assert request.state.cw_user is admin
    assert request.state.cw_user["is_admin"] is True


def test_view_as_header_transport_is_scoped_to_safe_and_allowlisted_calls() -> None:
    api_js = Path("assets/helpers/api.js").read_text("utf-8")
    sync_api = Path("api/syncAPI.py").read_text("utf-8")

    assert 'const VIEW_AS_HEADER = "X-CW-View-As";' in api_js
    assert 'const VIEW_AS_WRITE_PATHS = ["/api/run", "/api/export", "/api/import"];' in api_js
    assert 'const viewAsProfile = () => (isManagedUser() ? "" : String(window.CW?.OverviewProfile?.id || "").trim());' in api_js
    assert 'if (method === "GET" || method === "HEAD") return true;' in api_js
    assert "user = request_user(request)" in sync_api


def test_status_badges_hide_out_of_scope_providers() -> None:
    js = Path("assets/js/main-status.js").read_text("utf-8")

    assert "function matchesOverviewProfile(key, data)" in js
    assert "if (!matchesOverviewProfile(key, data)) return false;" in js
    assert 'return instances.some((instance) => profile.matchesEndpoint(key, instance));' in js
    assert 'return profile.matchesEndpoint(key, "default");' in js
    assert 'window.addEventListener("cw:overview-profile-changed", () => {' in js


def test_view_as_query_overrides_ambient_header(monkeypatch) -> None:
    from cw_platform import access_policy

    cfg = _view_as_cfg()
    cfg["user_profiles"][BOB_PROFILE_ID] = {"label": "Bob", "instances": {"PLEX": ["PLEX-P01"]}}
    monkeypatch.setattr("cw_platform.config_base.load_config", lambda *a, **k: cfg)
    admin = {"id": "administrator", "is_admin": True}

    def req(header: str = "", query: str = "") -> Any:
        return SimpleNamespace(
            state=SimpleNamespace(cw_user=admin),
            headers={"x-cw-view-as": header} if header else {},
            query_params={"user_profile": query} if query else {},
        )

    both = access_policy.request_user(req(header=ALICE_PROFILE_ID, query=BOB_PROFILE_ID))
    assert both is not None and both["profile_id"] == BOB_PROFILE_ID

    explicit_all = access_policy.request_user(req(header=ALICE_PROFILE_ID, query="all"))
    assert explicit_all is admin

    header_only = access_policy.request_user(req(header=ALICE_PROFILE_ID))
    assert header_only is not None and header_only["profile_id"] == ALICE_PROFILE_ID


def test_events_modal_has_profile_filter_in_more_filters() -> None:
    js = Path("assets/js/modals/events/index.js").read_text("utf-8")

    assert "function withEventScope(u)" in js
    assert 'if (!url.pathname.startsWith("/api/events/")) return u;' in js
    assert 'url.searchParams.set("user_profile", eventScope || "all");' in js
    assert "const ddProfile = createDropdown({" in js
    assert "const placeProfileDd = () => {" in js
    assert "const host = stats ? tabsRightEl : filtersEl;" in js
    assert "renderStatsRange(); placeProfileDd(); loadStats();" in js


def test_profile_hero_prefers_last_scrobble_over_history() -> None:
    js = Path("assets/js/profile-page.js").read_text("utf-8")

    assert "function renderHero(scrobbleItems, historyItems)" in js
    assert "const item = newestItem(scrobbleItems) || newestItem(historyItems);" in js
    assert "renderHero(scrobble, history);" in js
    assert "const watchedEpoch = (item) =>" in js
    assert "relTime(watchedEpoch(item))" in js
    assert "const providerName = (value) => {" in js
    assert 'return String(value.provider || value.name || value.key || "");' in js
    assert 'const providerOf = (item) => providerName(item?.source) || providerName(item?.provider) || providerName(item?.sources?.[0]) || "";' in js
    assert "const providerRoute = (item) => {" in js
    assert "const providerIconHtml = (provider) => {" in js
    assert "for (const row of Array.isArray(item?.targets) ? item.targets : []) push(row);" in js
    assert "for (const row of Array.isArray(item?.sources) ? item.sources : []) push(row);" in js
    assert "const sinkHtml = route.sinks.map(providerIconHtml).filter(Boolean).join(\"\");" in js
    assert "cw-profile-provider-badge--icon" in js
    assert "function bindLastWatchedPreview(node) {" in js
    assert "bindLastWatchedPreview(last);" in js
    assert 'node.dataset.previewBound === "1"' in js

    css = Path("assets/css/profile-page.css").read_text("utf-8")
    assert "border-radius:0 15px 15px 0" in css
    assert "grid-template-columns:minmax(0,1fr)90px" in css
    assert "#profile-last-provider{flex:1 1 100%;display:flex;flex-wrap:wrap;gap:6px" in css
    assert ".cw-profile-provider-badge--icon{gap:0;width:30px;min-width:30px;padding:0;justify-content:center}" in css


def test_continue_watching_cards_show_provider_icons() -> None:
    js = Path("assets/js/profile-page.js").read_text("utf-8")
    css = Path("assets/css/profile-page.css").read_text("utf-8")

    assert "const progressProviders = (item) => {" in js
    assert "for (const row of Array.isArray(item?.providers) ? item.providers : []) push(row);" in js
    assert 'name.toLowerCase() === "combined"' in js
    assert 'const providerStrip = providerIcons ? `<span class="cw-cw-providers">${providerIcons}</span>` : "";' in js
    assert "${episodeBadge}${providerStrip}<img" in js
    assert ".cw-cw-providers{position:absolute;left:8px;bottom:8px;z-index:2;" in css
