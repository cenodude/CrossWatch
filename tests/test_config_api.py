from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, cast


def _loads_body(body: bytes | memoryview[int]) -> Any:
    return json.loads(bytes(body))


def test_config_migrate_clears_pending_upgrade_marker(monkeypatch) -> None:
    from api import configAPI as cfg_api

    saved: dict = {}

    def load_cfg() -> dict:
      return {
          "version": "0.9.13",
          "ui": {
              "_pending_upgrade_from_version": "0.9.13",
              "_autogen": False,
          },
      }

    def save_cfg(cfg: dict) -> None:
        saved.clear()
        saved.update(cfg)

    monkeypatch.setattr(
        cfg_api,
        "_env",
        lambda: {
            "CW": None,
            "cfg_base": object(),
            "load": load_cfg,
            "save": save_cfg,
            "prune": lambda *_: None,
            "ensure": lambda *_: None,
            "norm_pair": lambda *_: None,
            "probes_cache": None,
            "probes_status_cache": None,
            "scheduler": None,
        },
    )

    res = cfg_api.api_config_migrate()

    assert res["ok"] is True
    assert "_pending_upgrade_from_version" not in (saved.get("ui") or {})


def test_config_migrate_backfills_resource_ids_and_scheduler_refs(monkeypatch) -> None:
    from api import configAPI as cfg_api
    from cw_platform import config_base

    current = {
        "version": "0.10.9",
        "ui": {"_pending_upgrade_from_version": "0.10.9"},
        "pairs": [
            {"source": "plex", "target": "trakt"},
            {"id": "kept_pair", "source": "jellyfin", "target": "simkl"},
            {"id": "kept_pair", "source": "emby", "target": "mdblist"},
        ],
        "scrobble": {
            "enabled": True,
            "sources": {"watcher": True},
            "watch": {
                "autostart": True,
                "routes": [
                    {"provider": "plex", "sink": "trakt"},
                    {"id": "kept_route", "provider": "jellyfin", "sink": "simkl"},
                    {"id": "kept_route", "provider": "emby", "sink": "mdblist"},
                ],
            },
        },
        "scheduling": {
            "advanced": {
                "jobs": [{"id": "job_1", "pair_id": "pair-1"}],
                "workflows": [{"id": "workflow_1", "steps": [{"id": "step_1", "pair_id": "pair-1"}]}],
                "event_rules": [
                    {
                        "id": "event_1",
                        "source": "watcher",
                        "filters": {"route_id": "R1"},
                        "action": {"pair_id": "pair-1"},
                    }
                ],
            }
        },
    }
    saved: dict = {}

    monkeypatch.setattr(
        cfg_api,
        "_env",
        lambda: {
            "CW": None,
            "cfg_base": SimpleNamespace(
                ensure_config_resource_ids=config_base.ensure_config_resource_ids,
                _current_version_norm=lambda: "0.11.0",
            ),
            "load": lambda: json.loads(json.dumps(current)),
            "save": lambda cfg: saved.update(cfg),
            "prune": lambda *_: None,
            "ensure": lambda *_: None,
            "norm_pair": lambda *_: None,
            "probes_cache": None,
            "probes_status_cache": None,
            "scheduler": None,
        },
    )

    res = cfg_api.api_config_migrate()

    assert res["ok"] is True
    first_pair_id = saved["pairs"][0]["id"]
    duplicate_pair_id = saved["pairs"][2]["id"]
    first_route_id = saved["scrobble"]["watch"]["routes"][0]["id"]
    duplicate_route_id = saved["scrobble"]["watch"]["routes"][2]["id"]

    assert first_pair_id.startswith("pair_")
    assert saved["pairs"][1]["id"] == "kept_pair"
    assert duplicate_pair_id.startswith("pair_")
    assert duplicate_pair_id != "kept_pair"
    assert first_route_id.startswith("route_")
    assert saved["scrobble"]["watch"]["routes"][1]["id"] == "kept_route"
    assert duplicate_route_id.startswith("route_")
    assert duplicate_route_id != "kept_route"
    assert saved["scheduling"]["advanced"]["jobs"][0]["pair_id"] == first_pair_id
    assert saved["scheduling"]["advanced"]["workflows"][0]["steps"][0]["pair_id"] == first_pair_id
    assert saved["scheduling"]["advanced"]["event_rules"][0]["action"]["pair_id"] == first_pair_id
    assert saved["scheduling"]["advanced"]["event_rules"][0]["filters"]["route_id"] == first_route_id
    assert "pairs[0].id" in res["resource_id_paths"]
    assert "scrobble.watch.routes[0].id" in res["resource_id_paths"]


def test_config_migrate_cleans_invalid_resource_profile_assignments(monkeypatch) -> None:
    from api import configAPI as cfg_api
    from cw_platform import config_base

    current = {
        "version": "0.10.9",
        "ui": {"_pending_upgrade_from_version": "0.10.9"},
        "user_profiles": {
            "valid-profile": {"label": "Valid"},
        },
        "scrobble": {
            "watch": {
                "routes": [
                    {"id": "R1", "provider": "plex", "sink": "trakt", "profile_id": "VALID-PROFILE"},
                    {"id": "R2", "provider": "plex", "sink": "simkl", "profileId": "missing-profile"},
                ],
            },
            "webhook": {
                "user_profile_assignments": {
                    "plex:default:trakt:default": "VALID-PROFILE",
                    "plex:default:simkl:default": "missing-profile",
                }
            },
        },
    }
    saved: dict = {}

    monkeypatch.setattr(
        cfg_api,
        "_env",
        lambda: {
            "CW": None,
            "cfg_base": SimpleNamespace(
                ensure_config_resource_ids=config_base.ensure_config_resource_ids,
                cleanup_invalid_resource_profile_ids=config_base.cleanup_invalid_resource_profile_ids,
                _current_version_norm=lambda: "0.11.0",
            ),
            "load": lambda: json.loads(json.dumps(current)),
            "save": lambda cfg: saved.update(cfg),
            "prune": lambda *_: None,
            "ensure": lambda *_: None,
            "norm_pair": lambda *_: None,
            "probes_cache": None,
            "probes_status_cache": None,
            "scheduler": None,
        },
    )

    res = cfg_api.api_config_migrate()

    assert res["ok"] is True
    routes = saved["scrobble"]["watch"]["routes"]
    assignments = saved["scrobble"]["webhook"]["user_profile_assignments"]
    assert routes[0]["profile_id"] == "valid-profile"
    assert "profile_id" not in routes[1]
    assert "profileId" not in routes[1]
    assert assignments == {"plex:default:trakt:default": "valid-profile"}
    assert "scrobble.watch.routes[1].profileId" in res["profile_cleanup_paths"]
    assert "scrobble.webhook.user_profile_assignments.plex:default:simkl:default" in res["profile_cleanup_paths"]


def test_config_write_uses_canonical_top_level_order() -> None:
    from cw_platform import config_base

    ordered = config_base._order_config_for_write(
        {
            "ui": {},
            "plex": {},
            "tmdb": {},
            "metadata": {},
            "tmdb_sync": {},
            "anime_mapping": {},
            "version": "0.11.0",
            "pairs": [],
            "custom_future_key": True,
            "app_auth": {},
        }
    )

    assert list(ordered.keys()) == [
        "version",
        "app_auth",
        "plex",
        "tmdb_sync",
        "metadata",
        "tmdb",
        "anime_mapping",
        "pairs",
        "ui",
        "custom_future_key",
    ]


def test_config_migrate_removes_obsolete_mobile_auth(monkeypatch) -> None:
    from api import configAPI as cfg_api
    from cw_platform import config_base

    current = {
        "version": "0.10.9",
        "mobile_auth": {"enabled": True, "token": "legacy"},
        "ui": {"_pending_upgrade_from_version": "0.10.9"},
    }
    saved: dict = {}

    monkeypatch.setattr(
        cfg_api,
        "_env",
        lambda: {
            "CW": None,
            "cfg_base": SimpleNamespace(
                cleanup_obsolete_config_keys=config_base.cleanup_obsolete_config_keys,
                _current_version_norm=lambda: "0.11.0",
            ),
            "load": lambda: json.loads(json.dumps(current)),
            "save": lambda cfg: saved.update(cfg),
            "prune": lambda *_: None,
            "ensure": lambda *_: None,
            "norm_pair": lambda *_: None,
            "probes_cache": None,
            "probes_status_cache": None,
            "scheduler": None,
        },
    )

    res = cfg_api.api_config_migrate()

    assert res["ok"] is True
    assert "mobile_auth" not in saved
    assert res["obsolete_paths"] == ["mobile_auth"]


def test_config_save_preserves_blank_stremio_auth_key(monkeypatch) -> None:
    from api import configAPI as cfg_api

    saved: dict = {}

    monkeypatch.setattr(
        cfg_api,
        "_env",
        lambda: {
            "CW": None,
            "cfg_base": object(),
            "load": lambda: {"stremio": {"auth_key": "real-key"}, "scrobble": {}},
            "save": lambda cfg: saved.update(cfg),
            "prune": lambda *_: None,
            "ensure": lambda *_: None,
            "norm_pair": lambda *_: None,
            "probes_cache": None,
            "probes_status_cache": None,
            "scheduler": None,
        },
    )

    res = cfg_api.api_config_save(cast(Any, SimpleNamespace(app=SimpleNamespace())), {"stremio": {"auth_key": ""}})

    assert res["ok"] is True
    assert saved["stremio"]["auth_key"] == "real-key"


def test_config_save_preserves_masked_totp_secrets(monkeypatch) -> None:
    from api import configAPI as cfg_api

    saved: dict = {}
    current = {
        "app_auth": {
            "totp": {"enabled": True, "secret": "REALADMIN", "pending_secret": "PENDINGADMIN"},
            "users": {
                "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa": {
                    "username": "pascal",
                    "enabled": True,
                    "role": "user",
                    "profile_id": "11111111111141118111111111111111",
                    "permissions": {"dashboard": True},
                    "password": {"salt": "salt", "hash": "hash"},
                    "totp": {"enabled": True, "secret": "REALUSER", "pending_secret": "PENDINGUSER"},
                }
            },
        },
        "scrobble": {},
    }

    monkeypatch.setattr(
        cfg_api,
        "_env",
        lambda: {
            "CW": None,
            "cfg_base": object(),
            "load": lambda: json.loads(json.dumps(current)),
            "save": lambda cfg: saved.update(cfg),
            "prune": lambda *_: None,
            "ensure": lambda *_: None,
            "norm_pair": lambda *_: None,
            "probes_cache": None,
            "probes_status_cache": None,
            "scheduler": None,
        },
    )

    res = cfg_api.api_config_save(
        cast(Any, SimpleNamespace(app=SimpleNamespace())),
        {
            "app_auth": {
                "totp": {"enabled": True, "secret": "••••••••", "pending_secret": "••••••••"},
                "users": {
                    "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa": {
                        "totp": {"enabled": True, "secret": "••••••••", "pending_secret": "••••••••"}
                    }
                },
            }
        },
    )

    assert res["ok"] is True
    assert saved["app_auth"]["totp"]["secret"] == "REALADMIN"
    assert saved["app_auth"]["totp"]["pending_secret"] == "PENDINGADMIN"
    assert saved["app_auth"]["users"]["aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"]["totp"]["secret"] == "REALUSER"
    assert saved["app_auth"]["users"]["aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"]["totp"]["pending_secret"] == "PENDINGUSER"


def test_config_meta_exposes_only_safe_ui_and_tmdb_state(monkeypatch, tmp_path) -> None:
    from api import configAPI as cfg_api

    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "version": "0.11.0",
                "ui": {
                    "show_watchlist_preview": True,
                    "recent_activity_limit": 12,
                    "secret_admin_only": "hidden",
                },
                "tmdb": {"api_key": "real-key"},
                "plex": {"token": "secret-token"},
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        cfg_api,
        "_env",
        lambda: {
            "CW": None,
            "cfg_base": SimpleNamespace(config_path=lambda: path),
            "load": lambda: {},
            "save": lambda *_: None,
            "prune": lambda *_: None,
            "ensure": lambda *_: None,
            "norm_pair": lambda *_: None,
            "probes_cache": None,
            "probes_status_cache": None,
            "scheduler": None,
        },
    )

    res = cfg_api.api_config_meta(cast(Any, SimpleNamespace(cookies={})))
    data = _loads_body(res.body)

    assert data["ui"] == {"show_watchlist_preview": True, "recent_activity_limit": 12}
    assert data["tmdb_configured"] is True
    assert "tmdb" not in data
    assert "plex" not in data


def test_sync_providers_exposes_progress_completion_policy(monkeypatch) -> None:
    from api import syncAPI as sync_api
    from cw_platform import config_base, provider_instances

    dummy = SimpleNamespace(
        get_manifest=lambda: {
            "name": "PUBLICMETADB",
            "label": "PublicMetaDB",
            "features": {"progress": True},
            "capabilities": {
                "bidirectional": True,
                "progress": {
                    "server_completion_percent": 80,
                    "completion_policy": {
                        "progress_write": {
                            "mode": "auto_complete",
                            "percent": 80,
                        }
                    },
                },
            },
        }
    )

    monkeypatch.setattr(config_base, "load_config", lambda: {})
    monkeypatch.setattr(provider_instances, "list_instance_ids", lambda *_: ["default"])
    monkeypatch.setattr(provider_instances, "build_provider_config_view", lambda cfg, *_: cfg)
    monkeypatch.setattr(sync_api, "sync_provider_names", lambda upper=True: ["PUBLICMETADB"])
    monkeypatch.setattr(sync_api, "get_sync_module_path_by_name", lambda name: "dummy.publicmetadb")
    monkeypatch.setattr(sync_api.importlib, "import_module", lambda path: dummy)

    response = sync_api.api_sync_providers()
    data = _loads_body(response.body)

    assert data[0]["capabilities"]["progress"]["server_completion_percent"] == 80
    assert data[0]["capabilities"]["progress"]["completion_policy"]["progress_write"]["percent"] == 80
