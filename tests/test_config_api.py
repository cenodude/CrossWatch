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
