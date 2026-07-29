from __future__ import annotations

import json
from types import SimpleNamespace


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
    data = json.loads(response.body)

    assert data[0]["capabilities"]["progress"]["server_completion_percent"] == 80
    assert data[0]["capabilities"]["progress"]["completion_policy"]["progress_write"]["percent"] == 80
