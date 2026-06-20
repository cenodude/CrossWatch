from __future__ import annotations

import json
import zipfile
from datetime import datetime
from pathlib import Path


def _patch_config_dir(monkeypatch, tmp_path: Path) -> None:
    import services.backups as backups

    monkeypatch.setattr(backups, "CONFIG", tmp_path)


def test_config_backup_includes_master_key_and_validates(tmp_path: Path, monkeypatch) -> None:
    _patch_config_dir(monkeypatch, tmp_path)
    import services.backups as backups

    (tmp_path / "config.json").write_text('{"plex":{"account_token":"enc:v1:test"}}\n', encoding="utf-8")
    (tmp_path / ".cw_master_key").write_text("test-key", encoding="utf-8")

    res = backups.create_backup(scope="config_only", label="unit", trigger="test")
    validation = backups.validate_backup(res["path"])

    assert validation["ok"] is True
    manifest = validation["manifest"]
    assert manifest["master_key_included"] is True
    assert manifest["external_key_required"] is False
    assert {row["path"] for row in manifest["files"]} == {"config.json", ".cw_master_key"}


def test_restore_config_backup_creates_pre_restore_backup(tmp_path: Path, monkeypatch) -> None:
    _patch_config_dir(monkeypatch, tmp_path)
    import services.backups as backups

    (tmp_path / "config.json").write_text('{"version":"before"}\n', encoding="utf-8")
    (tmp_path / ".cw_master_key").write_text("test-key", encoding="utf-8")
    res = backups.create_backup(scope="config_only", label="restore-src", trigger="test")

    (tmp_path / "config.json").write_text('{"version":"after"}\n', encoding="utf-8")
    restored = backups.restore_backup(res["path"], create_pre_restore=True)

    assert restored["ok"] is True
    assert restored["pre_restore_backup"]["path"]
    assert json.loads((tmp_path / "config.json").read_text(encoding="utf-8"))["version"] == "before"


def test_validate_rejects_archive_member_outside_restore_allowlist(tmp_path: Path, monkeypatch) -> None:
    _patch_config_dir(monkeypatch, tmp_path)
    import services.backups as backups

    target = tmp_path / "backups" / "bad.zip"
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = b"not allowed"
    manifest = {
        "kind": backups.BACKUP_KIND,
        "schema_version": backups.BACKUP_SCHEMA_VERSION,
        "created_at": datetime.utcnow().isoformat(),
        "files": [
            {
                "path": "api/backdoor.py",
                "size": len(payload),
                "sha256": "0" * 64,
            }
        ],
    }
    with zipfile.ZipFile(target, "w") as zf:
        zf.writestr("api/backdoor.py", payload)
        zf.writestr("manifest.json", json.dumps(manifest))

    validation = backups.validate_backup("bad.zip")

    assert validation["ok"] is False
    assert any("Invalid restore target" in err for err in validation["errors"])


def test_scheduler_backup_job_payload(monkeypatch) -> None:
    import services.scheduling as scheduling
    from services.scheduling import SyncScheduler

    now = datetime(2026, 3, 15, 8, 5, 0)
    monkeypatch.setattr(scheduling, "_as_now_in_tz", lambda _tz=None: now)
    monkeypatch.setattr(scheduling, "_now_ts", lambda: int(now.timestamp()))

    seen: list[dict] = []
    config = {
        "scheduling": {
            "enabled": False,
            "mode": "disabled",
            "advanced": {
                "enabled": True,
                "jobs": [],
                "capture_jobs": [],
                "backup_jobs": [
                    {
                        "id": "morning-backup",
                        "scope": "app_state",
                        "at": "08:00",
                        "days": [7],
                        "label_template": "nightly-{scope}-{date}",
                        "retention_days": 30,
                        "max_backups": 10,
                        "auto_delete_old": True,
                        "include_snapshots": True,
                        "include_reports": False,
                        "include_cache": False,
                        "active": True,
                    }
                ],
                "event_rules": [],
            },
        }
    }

    scheduler = SyncScheduler(
        load_config=lambda: config,
        save_config=lambda next_cfg: config.update(next_cfg),
        run_sync_fn=lambda payload=None: seen.append(payload or {}) or True,
        is_sync_running_fn=lambda: False,
    )

    ok = scheduler._adv_run_due(config["scheduling"], None)

    assert ok is True
    assert seen == [
        {
            "source": "scheduler",
            "scheduler_mode": "advanced_backup",
            "backup_job_id": "morning-backup",
            "backup": {
                "scope": "app_state",
                "label_template": "nightly-{scope}-{date}",
                "retention_days": 30,
                "max_backups": 10,
                "auto_delete_old": True,
                "include_snapshots": True,
                "include_reports": False,
                "include_cache": False,
            },
        }
    ]

    status = scheduler.status()
    assert status["last_backup_job_id"] == "morning-backup"
    assert status["last_backup_scope"] == "app_state"
