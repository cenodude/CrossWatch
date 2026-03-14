from __future__ import annotations


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
