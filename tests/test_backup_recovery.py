# tests/test_backup_recovery.py
# CrossWatch - Encrypted backup jobs and safe recovery regression checks
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import json
import os
import sqlite3
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

import _backup_crypto as crypto
from _backup_bootstrap import apply_pending
from cw_platform import config_base
from services import backup_manager as manager, backup_restore as recovery, backups

PASSWORD = "a separate recovery password"


@pytest.fixture
def installation(tmp_path, monkeypatch):
    root = tmp_path / "config"
    root.mkdir()
    monkeypatch.setattr(backups, "CONFIG", root)
    monkeypatch.setattr(config_base, "CONFIG", root)
    monkeypatch.setattr(manager, "start", lambda: None)
    monkeypatch.setenv("CW_BACKUP_ROOT", str(tmp_path))
    monkeypatch.delenv("CW_CONFIG_KEY", raising=False)
    monkeypatch.delenv("CROSSWATCH_CONFIG_KEY", raising=False)
    monkeypatch.delenv("CROSSWATCH_DB", raising=False)
    cfg = {"crosswatch": {"instances": {}}, "pairs": []}
    monkeypatch.setattr(config_base, "load_config", lambda: copy.deepcopy(cfg))
    monkeypatch.setattr(manager, "load_config", lambda: copy.deepcopy(cfg))
    (root / "config.json").write_text(json.dumps(cfg), encoding="utf-8")
    return root, cfg


def plan(**changes):
    return {"components": ["application"], "trackers": None, "encrypt_local": True, "password": PASSWORD, "password_saved": True, "destination": {"kind": "none"}, **changes}


def seed_db(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS sample (value TEXT)")
        conn.execute("DELETE FROM sample")
        conn.execute("INSERT INTO sample VALUES (?)", (value,))
        conn.commit()
    finally:
        conn.close()


def test_encryption_round_trip_wrong_password_and_tampering(tmp_path):
    source, encrypted, restored = (tmp_path / name for name in ("source", "encrypted", "restored"))
    source.write_bytes(b"private-config" * 100000)
    crypto.encrypt(source, encrypted, PASSWORD)
    assert b"private-config" not in encrypted.read_bytes()
    crypto.decrypt(encrypted, restored, PASSWORD)
    assert restored.read_bytes() == source.read_bytes()
    restored.unlink()
    with pytest.raises(ValueError, match="incorrect"):
        crypto.decrypt(encrypted, restored, "another password")
    assert not restored.exists()
    data = bytearray(encrypted.read_bytes())
    data[80] ^= 1
    encrypted.write_bytes(data)
    with pytest.raises(ValueError, match="changed"):
        crypto.decrypt(encrypted, restored, PASSWORD)
    assert not restored.exists()


def test_crypto_does_not_remove_existing_destination(tmp_path):
    source, target = tmp_path / "source", tmp_path / "existing"
    source.write_bytes(b"content")
    target.write_bytes(b"keep me")
    with pytest.raises(FileExistsError):
        crypto.encrypt(source, target, PASSWORD)
    assert target.read_bytes() == b"keep me"


def test_components_and_selected_external_tracker_are_independent(installation, tmp_path, monkeypatch):
    root, cfg = installation
    external = tmp_path / "external-tracker"
    cfg["crosswatch"]["instances"]["one"] = {"root_dir": str(external), "label": "My tracker"}
    seed_db(root / ".cw_provider" / "tracker.db", "default")
    seed_db(external / "tracker.db", "selected")
    database = tmp_path / "database" / "custom.db"
    seed_db(database, "application mappings")
    monkeypatch.setenv("CROSSWATCH_DB", str(database))
    (root / "snapshots").mkdir()
    (root / "snapshots" / "capture.json").write_text("{}")
    result = backups.create_backup(components=["trackers", "captures"], trackers=["one"])
    validation = backups.validate_backup(result["path"])
    assert validation["ok"]
    manifest = validation["manifest"]
    assert {r["path"] for r in manifest["files"]} == {".cw_provider/profiles/one/tracker.db", "snapshots/capture.json"}
    assert manifest["trackers"] == [{"id": "one", "root": ".cw_provider/profiles/one", "label": "My tracker"}]
    assert not manifest["external_key_required"]


def test_failed_database_snapshot_never_falls_back_to_raw_copy(installation, monkeypatch):
    root, _ = installation
    seed_db(root / ".cw_databases" / "state.db", "value")
    monkeypatch.setattr(backups, "_snapshot_sqlite", lambda *a: (_ for _ in ()).throw(OSError("busy")))
    with pytest.raises(ValueError, match="consistent database"):
        backups.create_backup(components=["application"])
    assert backups.list_backups() == []


@pytest.mark.parametrize("password", ["", PASSWORD])
@pytest.mark.parametrize("database_location", ["default", "external", "custom_name"])
def test_application_restores_mapping_stores(installation, tmp_path, monkeypatch, password, database_location):
    from cw_platform.anime_mapping import overrides
    from cw_platform.local_db import db, manual_policy

    root, _ = installation
    if database_location != "default":
        database = (tmp_path / "database" if database_location == "external" else root / ".cw_databases") / "custom.db"
        monkeypatch.setenv("CROSSWATCH_DB", str(database))
        seed_db(root / ".cw_databases" / "crosswatch.sqlite3", "unused")
    monkeypatch.setattr(overrides, "CONFIG_BASE", lambda: root)
    db.close_conn()
    overrides.invalidate_cache()
    try:
        rules = {"version": 1, "providers": {"SIMKL": {"history": {
            "adds": {"items": {"tmdb:77#show": {"type": "show", "title": "Saved correction", "ids": {"tmdb": "77"}}}},
            "mappings": {"tmdb:77#show": {"original_key": "tmdb:5#show", "original": {"type": "show", "ids": {"tmdb": "5"}}}},
            "blocks": ["tmdb:5#show", "tmdb:999#show"],
        }}}}
        manual_policy.save_policy(root, {**rules, "pairs": {"saved-pair": copy.deepcopy(rules)}})
        expected_policy = manual_policy.load_policy(root)
        assert expected_policy["providers"]["SIMKL"]["history"]["mappings"]
        assert expected_policy["pairs"]["saved-pair"] == rules
        for enabled in (True, False):
            overrides.upsert_override({"media_type": "show", "title": "Custom anime rule", "match_provider": "tvdb", "match_id": "81472", "match_season": 2,
                                       "target_namespace": "mal", "target_id": "999", "episode_from": 1, "episode_to": 35, "episode_start_at": 500, "enabled": enabled})
        expected_anime = overrides.load_overrides()
        assert [rule["enabled"] for rule in expected_anime] == [True, False]
        source = backups.create_backup(components=["application"], password=password)
        manifest = backups.validate_backup(source["path"], password)["manifest"]
        members = {row["path"] for row in manifest["files"]}
        assert ".cw_databases/crosswatch.sqlite3" in members
        assert ".cw_state/anime_mapping_overrides.json" in members
        assert not any("custom.db" in name or name.endswith(("-wal", "-shm")) for name in members)
        db.close_conn()
        if database_location == "external":
            monkeypatch.setenv("CROSSWATCH_DB", str(tmp_path / "new-installation" / "restored.db"))
        manual_policy.save_policy(root, {"version": 1, "providers": {}, "pairs": {}})
        overrides.save_overrides([])
        db.close_conn()
        recovery.prepare(source["path"], password)
        apply_pending(root)
        assert manager.status()["restore"]["ok"]
        overrides.invalidate_cache()
        assert manual_policy.load_policy(root) == expected_policy
        assert overrides.load_overrides() == expected_anime
        db.close_conn()
        safety = manager.status()["restore"]["safety_backup"]
        recovery.prepare(safety)
        apply_pending(root)
        assert manager.status()["restore"]["ok"]
        overrides.invalidate_cache()
        assert not manual_policy.load_policy(root)["providers"]
        assert overrides.load_overrides() == []
    finally:
        db.close_conn()
        overrides.invalidate_cache()


@pytest.mark.parametrize("scope", ["app_state", "full", "config_only"])
def test_legacy_scope_handles_external_application_database(installation, tmp_path, monkeypatch, scope):
    database = tmp_path / "database" / "custom.db"
    seed_db(database, "saved")
    monkeypatch.setenv("CROSSWATCH_DB", str(database))
    source = backups.create_backup(scope=scope)
    members = {row["path"] for row in backups.validate_backup(source["path"])["manifest"]["files"]}
    assert (".cw_databases/crosswatch.sqlite3" in members) == (scope != "config_only")


def test_password_confirmation_and_public_secret_redaction(installation):
    root, _ = installation
    with pytest.raises(ValueError, match="Confirm"):
        manager.save_plan(plan(password_saved=False))
    public = manager.save_plan(plan())
    assert public["has_password"] and "password" not in public
    assert PASSWORD not in (root / ".cw_backups" / "settings.json").read_text()
    manager.enqueue()
    assert "settings" not in manager.status()["jobs"][0]
    assert PASSWORD not in json.dumps(manager.status())


def test_encrypted_local_job_and_plaintext_cleanup(installation):
    manager.save_plan(plan())
    manager.enqueue()
    manager.run_job(manager._jobs()[0])
    row = manager.status()["jobs"][0]
    assert row["stage"] == "complete" and row["local_status"] == "saved"
    assert row["path"].endswith(".cwbackup")
    assert not list(backups._backups_dir().rglob("*.zip"))
    assert backups.validate_backup(row["path"], PASSWORD)["ok"]


def test_failed_encryption_removes_plaintext_archive(installation, monkeypatch):
    manager.save_plan(plan())
    manager.enqueue()
    monkeypatch.setattr(manager.backup_crypto, "encrypt", lambda *a: (_ for _ in ()).throw(OSError("disk full")))
    manager.run_job(manager._jobs()[0])
    assert manager._jobs()[0]["stage"] == "failed"
    assert not list(backups._backups_dir().rglob("*.zip"))


def test_failed_remote_copy_retries_existing_local_archive(installation, tmp_path, monkeypatch):
    destination = tmp_path / "remote"
    destination.mkdir()
    manager.save_plan(plan(encrypt_local=False, destination={"kind": "folder", "path": str(destination)}))
    manager.enqueue()
    monkeypatch.setattr(manager.destinations, "upload", lambda *a: (_ for _ in ()).throw(ValueError("Offline")))
    manager.run_job(manager._jobs()[0])
    first = manager._jobs()[0]
    assert first["local_status"] == "saved" and first["remote_status"] == "failed"
    assert first["retry_at"] > 0
    uploads = []
    monkeypatch.setattr(manager.destinations, "upload", lambda dest, src, name, cb: uploads.append(src.suffix))
    monkeypatch.setattr(manager.destinations, "verify", lambda *a: None)
    manager.retry(first["id"])
    manager.run_job(manager._jobs()[0])
    row = manager._jobs()[0]
    assert row["stage"] == "complete" and row["remote_status"] == "verified"
    assert row["path"] == first["path"] and uploads == [".cwbackup"]
    assert len(backups.list_backups()) == 1


def test_missing_rclone_keeps_local_backup_without_automatic_retries(installation, tmp_path, monkeypatch):
    destination = tmp_path / "remote"
    destination.mkdir()
    manager.save_plan(plan(destination={"kind": "folder", "path": str(destination)}))
    manager.enqueue()
    monkeypatch.setattr(manager.destinations, "upload", lambda *a: (_ for _ in ()).throw(manager.destinations.RcloneUnavailable("rclone is unavailable")))
    manager.run_job(manager._jobs()[0])
    row = manager._jobs()[0]
    assert row["local_status"] == "saved" and row["remote_status"] == "failed"
    assert row["retry_at"] == 0 and "unavailable" in row["error"]
    assert backups.validate_backup(row["path"], PASSWORD)["ok"]


def test_cleanup_failure_does_not_downgrade_saved_backup(installation, monkeypatch):
    manager.save_plan(plan())
    manager.enqueue("scheduler")
    monkeypatch.setattr(manager, "_local_retention", lambda *a: (_ for _ in ()).throw(OSError("denied")))
    manager.run_job(manager._jobs()[0])
    row = manager._jobs()[0]
    assert row["stage"] == "complete" and row["retry_at"] == 0
    assert "cleaned up" in row["warning"]


def test_restore_waits_for_restart_and_saves_encrypted_safety_copy(installation):
    root, _ = installation
    manager.save_plan(plan())
    result = backups.create_backup(components=["application"])
    (root / "config.json").write_text('{"version":"live"}')
    recovery.prepare(result["path"])
    assert json.loads((root / "config.json").read_text())["version"] == "live"
    apply_pending(root)
    assert "version" not in json.loads((root / "config.json").read_text())
    status = manager.status()
    assert status["restore"]["ok"] and not status["pending_restore"]
    safety = status["restore"]["safety_backup"]
    assert safety.endswith(".cwbackup")
    assert backups.validate_backup(safety, PASSWORD)["ok"]


def test_restore_rejects_changed_staging_without_changing_live_files(installation):
    root, _ = installation
    result = backups.create_backup(components=["application"])
    (root / "config.json").write_text('{"version":"live"}')
    recovery.prepare(result["path"])
    pending = manager.read("pending.json", {})
    (manager.private_dir() / pending["folder"] / "new" / "config.json").unlink()
    apply_pending(root)
    assert json.loads((root / "config.json").read_text())["version"] == "live"
    assert not manager.status()["restore"]["ok"]


def test_interrupted_restore_rolls_back_before_startup(installation):
    root, _ = installation
    folder = manager.private_dir() / "restore-test"
    (folder / "old").mkdir(parents=True)
    (folder / "old" / "config.json").write_text('{"version":"original"}')
    (root / "config.json").write_text('{"version":"partial"}')
    manager.write("pending.json", {"stage": "applying", "folder": folder.name, "originals": {"config.json": True}})
    apply_pending(root)
    assert json.loads((root / "config.json").read_text())["version"] == "original"
    assert not manager.status()["restore"]["ok"]


def test_restore_rolls_back_files_if_a_replacement_fails(installation, monkeypatch):
    import _backup_bootstrap as bootstrap

    root, _ = installation
    seed_db(root / ".cw_databases" / "state.db", "saved")
    source = backups.create_backup(components=["application"])
    seed_db(root / ".cw_databases" / "state.db", "live")
    (root / "config.json").write_text('{"version":"live"}')
    recovery.prepare(source["path"])
    replace = bootstrap.os.replace
    replacements = []

    def fail_second(src, dst):
        if str(src).endswith(".restore.tmp"):
            replacements.append(src)
            if len(replacements) == 2:
                raise OSError("simulated write failure")
        return replace(src, dst)

    monkeypatch.setattr(bootstrap.os, "replace", fail_second)
    apply_pending(root)
    assert not manager.status()["restore"]["ok"]
    assert json.loads((root / "config.json").read_text())["version"] == "live"
    conn = sqlite3.connect(root / ".cw_databases" / "state.db")
    try:
        assert conn.execute("SELECT value FROM sample").fetchone()[0] == "live"
    finally:
        conn.close()


def test_external_config_key_requires_original_but_does_not_block_tracker_validation(installation, monkeypatch):
    root, _ = installation
    monkeypatch.setenv("CW_CONFIG_KEY", "external-passphrase")
    token = config_base._encrypt_secret("provider-token")
    (root / "config.json").write_text(json.dumps({"token": token}))
    (root / ".cw_master_key").write_text(Fernet.generate_key().decode())
    result = backups.create_backup(components=["application"])
    assert not backups.validate_backup(result["path"])["manifest"]["master_key_included"]
    assert recovery.inspect(result["path"])["application_ready"]
    monkeypatch.setenv("CW_CONFIG_KEY", "wrong-key")
    assert not recovery.inspect(result["path"])["application_ready"]
    with pytest.raises(ValueError, match="configuration key"):
        recovery.prepare(result["path"])


def test_restore_preserves_current_key_for_backup_credentials(installation):
    root, _ = installation
    original_key = Fernet.generate_key()
    (root / ".cw_master_key").write_text(original_key.decode())
    token = "enc:v1:" + Fernet(original_key).encrypt(b"provider-token").decode()
    (root / "config.json").write_text(json.dumps({"token": token}))
    source = backups.create_backup(components=["application"])
    current_key = Fernet.generate_key()
    (root / ".cw_master_key").write_text(current_key.decode())
    (root / "config.json").write_text("{}")
    manager.save_plan(plan())
    recovery.prepare(source["path"])
    apply_pending(root)
    assert (root / ".cw_master_key").read_text().encode() == current_key
    restored = json.loads((root / "config.json").read_text())["token"]
    assert Fernet(current_key).decrypt(restored[7:].encode()) == b"provider-token"
    assert manager.reveal(manager.settings())["password"] == PASSWORD


def test_tracker_recovery_creates_new_instance_without_sync_pairs(installation, monkeypatch):
    root, cfg = installation
    seed_db(root / ".cw_provider" / "tracker.db", "original")
    source = backups.create_backup(components=["trackers"])
    monkeypatch.setattr(config_base, "update_config", lambda mutate: mutate(cfg))
    result = recovery.recover_tracker(source["path"], "default", "Recovered")
    assert result["instance"] != "default"
    assert cfg["pairs"] == []
    new_root = Path(cfg["crosswatch"]["instances"][result["instance"]]["root_dir"])
    assert (new_root / "tracker.db").is_file()
    assert (root / ".cw_provider" / "tracker.db").is_file()


def test_retention_preserves_manual_safety_imported_other_jobs_and_pending_copies(installation):
    created = []
    for trigger, job_id in [("manual", "plan"), ("pre_restore", "plan"), ("scheduler", "other"), ("scheduler", "plan"), ("scheduler", "plan"), ("scheduler", "plan")]:
        result = backups.create_backup(components=["application"], trigger=trigger, job_id=job_id)
        created.append(result["path"])
        os.utime(backups._resolve_backup_file(result["path"])[1], (100000 + len(created), 100000 + len(created)))
    imported = backups._backups_dir() / "imported"
    imported.mkdir()
    import shutil

    shutil.copy2(backups._resolve_backup_file(created[3])[1], imported / "automatic.zip")
    result = backups.enforce_backup_retention(max_backups=1, job_id="plan", auto_delete_old=True, protected=[created[4]])
    assert result["deleted"] == [created[3]]
    assert {b["path"] for b in backups.list_backups()} == {created[0], created[1], created[2], created[4], created[5], "imported/automatic.zip"}


@pytest.mark.parametrize("encrypt_local", [False, True])
@pytest.mark.parametrize("error_type", [ValueError, manager.destinations.RcloneUnavailable])
def test_scheduled_local_retention_runs_while_destination_is_unavailable(installation, tmp_path, monkeypatch, encrypt_local, error_type):
    _, cfg = installation
    cfg["scheduling"] = {"advanced": {"backup_jobs": [{"id": manager.PLAN_ID, "max_backups": 2, "auto_delete_old": True}]}}
    destination = tmp_path / "remote"
    destination.mkdir()
    manager.save_plan(plan(encrypt_local=encrypt_local, destination={"kind": "folder", "path": str(destination)}))
    local_counts = []
    remote_cleanups = []

    def upload(*args):
        local_counts.append(len(backups.list_backups()))
        raise error_type("Destination unavailable")

    monkeypatch.setattr(manager.destinations, "upload", upload)
    monkeypatch.setattr(manager, "_remote_retention", lambda *args: remote_cleanups.append(True))
    for _ in range(4):
        manager.enqueue("scheduler")
        manager.run_job(manager._jobs()[-1])
    jobs = manager._jobs()
    assert local_counts == [1, 2, 2, 2]
    assert not remote_cleanups
    assert len(backups.list_backups()) == 2
    assert all(row["local_status"] == "expired" and row["retry_at"] == 0 for row in jobs[:2])
    assert all(row["local_status"] == "saved" for row in jobs[2:])
    assert len(list(manager.private_dir().glob("*.cwbackup"))) == (0 if encrypt_local else 2)
    with pytest.raises(ValueError, match="no longer available"):
        manager.retry(jobs[0]["id"])
    for row in jobs[2:]:
        assert backups.validate_backup(row["path"], PASSWORD if encrypt_local else "")["ok"]


def test_failed_verification_keeps_local_retention_without_remote_deletion(installation, tmp_path, monkeypatch):
    _, cfg = installation
    cfg["scheduling"] = {"advanced": {"backup_jobs": [{"id": manager.PLAN_ID, "max_backups": 1}]}}
    destination = tmp_path / "remote"
    destination.mkdir()
    manager.save_plan(plan(destination={"kind": "folder", "path": str(destination)}, remote_keep=1))
    deletions = []
    monkeypatch.setattr(manager.destinations, "upload", lambda *args: None)
    monkeypatch.setattr(manager.destinations, "verify", lambda *args: (_ for _ in ()).throw(ValueError("Checksum failed")))
    monkeypatch.setattr(manager.destinations, "run", lambda *args: deletions.append(args))
    for _ in range(2):
        manager.enqueue("scheduler")
        manager.run_job(manager._jobs()[-1])
    assert len(backups.list_backups()) == 1
    assert not deletions
    assert manager._jobs()[-1]["remote_status"] == "failed"


def test_remote_retention_only_runs_after_verified_upload(installation, tmp_path, monkeypatch):
    destination = tmp_path / "remote"
    destination.mkdir()
    manager.save_plan(plan(destination={"kind": "folder", "path": str(destination)}, remote_keep=1))
    events = []
    monkeypatch.setattr(manager.destinations, "upload", lambda *args: events.append("upload"))
    monkeypatch.setattr(manager.destinations, "verify", lambda *args: events.append("verify"))
    monkeypatch.setattr(manager.destinations, "run", lambda *args: events.append("delete"))
    for _ in range(2):
        manager.enqueue("scheduler")
        manager.run_job(manager._jobs()[-1])
    assert events == ["upload", "verify", "upload", "verify", "delete"]
    assert manager._jobs()[0]["remote_status"] == "expired"
    assert manager._jobs()[1]["remote_status"] == "verified"


def test_default_retention_includes_legacy_schedules_but_custom_jobs_do_not(installation):
    created = []
    for job_id in ("", "custom", manager.PLAN_ID):
        result = backups.create_backup(components=["application"], trigger="scheduler", job_id=job_id)
        created.append(result["path"])
        os.utime(backups._resolve_backup_file(result["path"])[1], (100000 + len(created), 100000 + len(created)))
    assert backups.enforce_backup_retention(max_backups=1, job_id="custom", auto_delete_old=True)["deleted"] == []
    result = backups.enforce_backup_retention(max_backups=1, job_id=manager.PLAN_ID, auto_delete_old=True)
    assert result["deleted"] == [created[0]]
    assert {row["path"] for row in backups.list_backups()} == set(created[1:])


def test_manual_retention_api_prunes_all_scheduled_groups_and_protects_active_restore(installation):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import backupsAPI

    paths = []
    for trigger, job_id in (("scheduler", ""), ("scheduler", "custom"), ("scheduler", manager.PLAN_ID), ("manual", ""), ("pre_restore", "")):
        result = backups.create_backup(components=["application"], trigger=trigger, job_id=job_id)
        paths.append(result["path"])
        os.utime(backups._resolve_backup_file(result["path"])[1], (100000 + len(paths), 100000 + len(paths)))
    manager.write("pending.json", {"path": paths[0]})
    manager.write("jobs.json", [{"id": "active", "stage": "uploading", "path": paths[1], "local_status": "saved"}])
    app = FastAPI()
    app.include_router(backupsAPI.router)
    with TestClient(app) as client:
        assert client.post("/api/backups/retention", json={"max_backups": 1}).json()["result"]["deleted"] == []
        (manager.private_dir() / "pending.json").unlink()
        manager.write("jobs.json", [])
        result = client.post("/api/backups/retention", json={"max_backups": 1})
        assert result.status_code == 200
        assert set(result.json()["result"]["deleted"]) == set(paths[:2])
    assert {row["path"] for row in backups.list_backups()} == set(paths[2:])


def test_disabled_retention_preserves_failed_jobs_and_transfer_files(installation, tmp_path, monkeypatch):
    _, cfg = installation
    cfg["scheduling"] = {"advanced": {"backup_jobs": [{"id": manager.PLAN_ID, "max_backups": 1, "auto_delete_old": False}]}}
    destination = tmp_path / "remote"
    destination.mkdir()
    manager.save_plan(plan(encrypt_local=False, destination={"kind": "folder", "path": str(destination)}))
    monkeypatch.setattr(manager.destinations, "upload", lambda *args: (_ for _ in ()).throw(ValueError("Offline")))
    for _ in range(2):
        manager.enqueue("scheduler")
        manager.run_job(manager._jobs()[-1])
    assert len(backups.list_backups()) == 2
    assert len(list(manager.private_dir().glob("*.cwbackup"))) == 2
    assert all(row["local_status"] == "saved" and row["retry_at"] > 0 for row in manager._jobs())


def test_saved_password_can_start_with_configuration_encryption_prefix(installation):
    password = "enc:v1:this is a user chosen password"
    manager.save_plan(plan(password=password))
    assert manager.reveal(manager.settings())["password"] == password


@pytest.mark.parametrize("name", ["../config.json", "/settings.json", "sub/settings.json", "sub\\settings.json", "cloud-../settings.json", "unknown.json"])
def test_backup_private_files_reject_unexpected_paths(installation, name):
    with pytest.raises(ValueError, match="settings file"):
        manager.read(name, {})
    with pytest.raises(ValueError, match="settings file"):
        manager.write(name, {})


@pytest.mark.parametrize("suffix", [".zip", ".cwbackup"])
def test_remote_download_uses_generated_local_names(installation, tmp_path, monkeypatch, suffix):
    destination = tmp_path / "remote"
    destination.mkdir()
    manager.save_plan(plan(destination={"kind": "folder", "path": str(destination)}))
    received = []

    def download(destination, args):
        received.append(args)
        Path(args[2]).write_bytes(b"downloaded archive")

    monkeypatch.setattr(manager.destinations, "run", download)
    result = manager.download_remote("installation/crosswatch-backup-original" + suffix)
    local = backups._resolve_backup_file(result["path"])[1]
    assert local.parent.name == "imported" and local.suffix == suffix
    assert len(local.stem) == 32 and "original" not in local.name
    assert received[0][1].endswith("crosswatch-backup-original" + suffix)


@pytest.mark.parametrize("path", ["crosswatch-backup-" * 10000, "crosswatch-backup-.zip", "crosswatch-backup-file.exe", "../crosswatch-backup-file.zip"], ids=["long", "empty", "extension", "traversal"])
def test_remote_download_rejects_invalid_names_before_transfer(installation, tmp_path, monkeypatch, path):
    destination = tmp_path / "remote"
    destination.mkdir()
    manager.save_plan(plan(destination={"kind": "folder", "path": str(destination)}))
    transfers = []
    monkeypatch.setattr(manager.destinations, "run", lambda *args: transfers.append(args))
    with pytest.raises(ValueError):
        manager.download_remote(path)
    assert not transfers


def test_validation_does_not_expose_configuration_key_errors(installation, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import backupsAPI

    backup = backups.create_backup(components=["application"])
    monkeypatch.setattr(recovery, "_check_config_key", lambda *args: (_ for _ in ()).throw(ValueError("sensitive-key-details-and-server-path")))
    app = FastAPI()
    app.include_router(backupsAPI.router)
    with TestClient(app) as client:
        response = client.post("/api/backups/validate", json={"path": backup["path"]})
    assert response.status_code == 200
    assert not response.json()["validation"]["application_ready"]
    assert "original configuration key" in response.text
    assert "sensitive-key-details-and-server-path" not in response.text


def test_deleting_failed_local_archive_stops_automatic_retries(installation, monkeypatch):
    manager.save_plan(plan())
    manager.enqueue()
    manager.run_job(manager._jobs()[0])
    row = manager._jobs()[0]
    manager._update(row["id"], stage="failed", retry_at=12345)
    manager.delete_local(row["path"])
    assert manager._jobs()[0]["retry_at"] == 0
    assert manager._jobs()[0]["local_status"] == "deleted"


def test_api_plan_job_encrypted_import_and_restore_confirmation(installation, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import backupsAPI

    app = FastAPI()
    app.include_router(backupsAPI.router)
    restarts = []
    monkeypatch.setattr(backupsAPI, "_restart_soon", lambda: restarts.append(True))
    with TestClient(app) as client:
        assert client.post("/api/backups/create", json={"managed": True}).status_code == 400
        response = client.post("/api/backups/plan", json=plan())
        assert response.status_code == 200
        assert "password" not in response.json()["plan"]
        assert client.post("/api/backups/create", json={"managed": True}).status_code == 202
        manager.run_job(manager._jobs()[0])
        status = client.get("/api/backups/status").json()
        assert status["jobs"][0]["stage"] == "complete"
        path = status["jobs"][0]["path"]
        download = client.get("/api/backups/download", params={"path": path})
        assert download.status_code == 200 and download.content.startswith(crypto.MAGIC)
        imported = client.post("/api/backups/upload", files={"file": ("backup.cwbackup", download.content)}, data={"password": PASSWORD})
        assert imported.status_code == 200
        imported_path = imported.json()["backup"]["path"]
        assert client.post("/api/backups/validate", json={"path": imported_path, "password": "wrong"}).status_code == 400
        assert client.post("/api/backups/restore", json={"path": imported_path}).status_code == 400
        assert client.post("/api/backups/restore", json={"path": imported_path, "confirm": True, "restart": True}).status_code == 200
        assert restarts == [True]
        assert manager.status()["pending_restore"]


@pytest.mark.parametrize("endpoint,method", [("plan", "GET"), ("plan", "POST"), ("status", "GET"), ("remote", "GET"), ("remote/download", "POST"), ("destination/test", "POST"), ("restore", "POST"), ("retry", "POST"), ("recovery-key", "POST"), ("cloud/connect", "POST"), ("cloud/continue", "POST")])
def test_backup_routes_remain_admin_only(endpoint, method):
    from api.appAuthAPI import non_admin_api_allowed

    assert not non_admin_api_allowed("/api/backups/" + endpoint, method)
