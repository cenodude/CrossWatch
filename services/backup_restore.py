# services/backup_restore.py
# CrossWatch - Validated backup recovery and restart-time replacement
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import hashlib
import os
import shutil
import sqlite3
import tempfile
import uuid
import zipfile
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken

from cw_platform.config_base import _normalize_fernet_key
from services import backup_manager as manager, backups


def _check_config_key(archive: zipfile.ZipFile, manifest: dict[str, Any]) -> None:
    if "config.json" not in archive.namelist():
        return
    key = os.getenv("CW_CONFIG_KEY") or os.getenv("CROSSWATCH_CONFIG_KEY")
    if not key and ".cw_master_key" in archive.namelist():
        key = archive.read(".cw_master_key").decode().strip()
    config = json.loads(archive.read("config.json"))

    def check(value: Any) -> None:
        if isinstance(value, dict):
            for child in value.values():
                check(child)
        elif isinstance(value, list):
            for child in value:
                check(child)
        elif isinstance(value, str) and value.startswith("enc:v1:"):
            if not key:
                raise ValueError("Supply the original CW_CONFIG_KEY before restoring this application backup.")
            try:
                Fernet(_normalize_fernet_key(key)).decrypt(value[7:].encode())
            except (InvalidToken, ValueError) as exc:
                raise ValueError("The configuration key does not unlock this backup. Supply the original key.") from exc

    check(config)


def inspect(path: str, password: str = "") -> dict[str, Any]:
    password = manager.password_for(path, password)
    validation = backups.validate_backup(path, password)
    if not validation["ok"]:
        raise ValueError("Backup validation failed: " + "; ".join(validation["errors"][:3]))
    manifest = validation["manifest"]
    with backups.open_archive(path, password) as (_, target), zipfile.ZipFile(target) as archive:
        try:
            _check_config_key(archive, manifest)
            validation["application_ready"] = True
        except ValueError as exc:
            validation["application_ready"] = False
            validation["application_error"] = str(exc)
        from cw_platform.tracker_store import SCHEMA_VERSION

        for row in manifest["files"]:
            if Path(row["path"]).name != "tracker.db":
                continue
            with tempfile.TemporaryDirectory(dir=manager.private_dir()) as folder:
                db = Path(folder) / "tracker.db"
                with archive.open(row["path"]) as src, db.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
                conn = sqlite3.connect(f"{db.as_uri()}?mode=ro", uri=True)
                try:
                    version = conn.execute("PRAGMA user_version").fetchone()[0]
                    if version > SCHEMA_VERSION:
                        raise ValueError("This tracker needs a newer CrossWatch version.")
                finally:
                    conn.close()
    return validation


def prepare(path: str, password: str = "") -> dict[str, Any]:
    password = manager.password_for(path, password)
    validation = inspect(path, password)
    if not validation["application_ready"]:
        raise ValueError(validation["application_error"])
    with manager._LOCK:
        if any(j["stage"] not in {"complete", "failed"} for j in manager._jobs()):
            raise ValueError("Wait for the current backup or transfer to finish before restoring.")
        if (manager.private_dir() / "pending.json").exists():
            raise ValueError("A restore is already waiting for restart.")
        folder = manager.private_dir() / ("restore-" + uuid.uuid4().hex)
        folder.mkdir(mode=0o700)
        try:
            with backups.open_archive(path, password) as (_, target), zipfile.ZipFile(target) as archive:
                members = [row["path"] for row in validation["manifest"]["files"]]
                for name in members:
                    backups._resolve_restore_target(name)
                    dest = folder / "new" / name
                    dest.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                    with archive.open(name) as src, dest.open("xb") as dst:
                        os.chmod(dest, 0o600)
                        shutil.copyfileobj(src, dst)
                config = folder / "new" / "config.json"
                incoming_key = folder / "new" / ".cw_master_key"
                existing_key = backups.CONFIG / ".cw_master_key"
                if config.exists() and incoming_key.exists() and existing_key.exists() and not backups._is_env_key_configured():
                    old_cipher = Fernet(_normalize_fernet_key(incoming_key.read_text(encoding="utf-8")))
                    current_cipher = Fernet(_normalize_fernet_key(existing_key.read_text(encoding="utf-8")))

                    def rekey(value):
                        if isinstance(value, dict):
                            return {k: rekey(v) for k, v in value.items()}
                        if isinstance(value, list):
                            return [rekey(v) for v in value]
                        if isinstance(value, str) and value.startswith("enc:v1:"):
                            return "enc:v1:" + current_cipher.encrypt(old_cipher.decrypt(value[7:].encode())).decode()
                        return value

                    config.write_text(json.dumps(rekey(json.loads(config.read_text(encoding="utf-8")))), encoding="utf-8")
                    shutil.copy2(existing_key, incoming_key)
                trackers = validation["manifest"].get("trackers") or []
                if config.exists() and trackers:
                    cfg = json.loads(config.read_text(encoding="utf-8"))
                    base = cfg.setdefault("crosswatch", {})
                    for tracker in trackers:
                        block = base if tracker["id"] == "default" else base.setdefault("instances", {}).setdefault(tracker["id"], {})
                        block["root_dir"] = str(backups._resolve_restore_target(tracker["root"]))
                    config.write_text(json.dumps(cfg), encoding="utf-8")
                hashes = {}
                for name in members:
                    with (folder / "new" / name).open("rb") as handle:
                        hashes[name] = hashlib.file_digest(handle, "sha256").hexdigest()
                manager.write("pending.json", {"folder": folder.name, "members": members, "hashes": hashes, "stage": "prepared", "path": path})
        except Exception:
            shutil.rmtree(folder)
            raise
    return {"ok": True, "restart_required": True, "message": "Restore prepared. CW will save a safety backup and apply it during restart."}


def recover_tracker(path: str, instance: str, label: str, password: str = "") -> dict[str, Any]:
    from api.providerInstancesAPI import _create_instance, _invalidate_provider_cache, _next_profile_id
    from cw_platform.config_base import update_config

    password = manager.password_for(path, password)
    validation = inspect(path, password)
    tracker = next((t for t in validation["manifest"].get("trackers", []) if t["id"] == instance), None)
    if not tracker:
        raise ValueError("Choose a tracker recorded in this backup.")
    prefix = tracker["root"].rstrip("/") + "/"
    members = [r["path"] for r in validation["manifest"]["files"] if r["path"].startswith(prefix) and not r["path"][len(prefix):].startswith("profiles/")]
    if not members:
        raise ValueError("The backup contains no data for this tracker.")
    with tempfile.TemporaryDirectory(dir=manager.private_dir()) as folder:
        staging = Path(folder) / "tracker"
        staging.mkdir()
        with backups.open_archive(path, password) as (_, target), zipfile.ZipFile(target) as archive:
            for member in members:
                rel = backups._safe_rel_path(member[len(prefix):])
                dest = staging / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(member) as src, dest.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
        created = {}

        def create(cfg):
            instances = cfg.setdefault("crosswatch", {}).setdefault("instances", {})
            ident = _next_profile_id("CROSSWATCH", instances, cfg)
            if not ident:
                raise ValueError("The tracker instance limit has been reached.")
            root = backups.CONFIG / ".cw_provider" / "profiles" / ident
            if root.exists():
                raise ValueError("The new tracker storage already exists.")
            _create_instance(cfg, "CROSSWATCH", instances, ident, {"template": {"connected": True}, "label": label or "Recovered tracker"})
            instances[ident]["root_dir"] = str(root)
            created.update(id=ident, path=root)
            shutil.copytree(staging, root)

        try:
            update_config(create)
        except Exception:
            if created.get("path") and created["path"].exists():
                shutil.rmtree(created["path"])
            raise
    _invalidate_provider_cache("CROSSWATCH")
    return {"ok": True, "instance": created["id"], "message": "Tracker recovered. No sync pairs were created."}
