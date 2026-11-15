# /_backup_bootstrap.py
# CrossWatch - Restore prepared backups before application imports
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import base64
import json
import os
import shutil
import sqlite3
import time
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath


def application_database_target(name: str) -> Path | None:
    configured = (os.getenv("CROSSWATCH_DB") or "").strip()
    if not configured:
        return None
    for suffix in ("", "-wal", "-shm", "-journal"):
        if name == ".cw_databases/crosswatch.sqlite3" + suffix:
            return Path(configured + suffix).resolve()
    return None


def _encrypt_safety(base: Path, safety: Path) -> Path:
    settings = base / ".cw_backups" / "settings.json"
    if not settings.exists():
        return safety
    plan = json.loads(settings.read_text(encoding="utf-8"))
    if not plan.get("encrypt_local"):
        return safety
    from cryptography.fernet import Fernet
    from _backup_crypto import encrypt

    raw = (os.getenv("CW_CONFIG_KEY") or os.getenv("CROSSWATCH_CONFIG_KEY") or (base / ".cw_master_key").read_text(encoding="utf-8")).strip().encode()
    try:
        valid = len(base64.urlsafe_b64decode(raw)) == 32
    except Exception:
        valid = False
    key = raw if valid else base64.urlsafe_b64encode(hashlib.sha256(raw).digest())
    password = Fernet(key).decrypt(plan["password"][7:].encode()).decode()
    encrypted = safety.with_suffix(".cwbackup")
    encrypt(safety, encrypted, password)
    with zipfile.ZipFile(safety) as archive:
        manifest = json.loads(archive.read("manifest.json"))
    encrypted.with_suffix(".metadata.json").write_text(json.dumps({k: v for k, v in manifest.items() if k != "files"}), encoding="utf-8")
    safety.unlink()
    return encrypted


def apply_pending(config: Path | None = None) -> None:
    base = (config or Path(os.getenv("CONFIG_BASE") or ("/config" if Path("/app").exists() else Path(__file__).resolve().parent))).resolve()
    private = base / ".cw_backups"
    marker = private / "pending.json"
    if not marker.is_file():
        return
    pending = json.loads(marker.read_text(encoding="utf-8"))
    folder = (private / pending["folder"]).resolve()
    if folder.parent != private.resolve() or not folder.name.startswith("restore-"):
        raise ValueError("Invalid restore staging folder")
    allowed_files = {"config.json", ".cw_master_key", "published_lists.json"}
    allowed_dirs = {".cw_state", ".cw_databases", "tls", "snapshots", ".cw_provider", "sync_reports", ".cw_cache", "cache"}

    def target(name):
        parts = PurePosixPath(name).parts
        if not parts or PurePosixPath(name).is_absolute() or ".." in parts or "\\" in name or ":" in name or (name not in allowed_files and parts[0] not in allowed_dirs):
            raise ValueError("Invalid restore target")
        database = application_database_target(name)
        if database is not None:
            return database
        path = base.joinpath(*parts).resolve()
        if not path.is_relative_to(base):
            raise ValueError("Restore target is outside the config directory")
        return path

    def record(name, data):
        dest = private / name
        temp = dest.with_suffix(".tmp")
        with temp.open("w", encoding="utf-8") as handle:
            os.chmod(temp, 0o600)
            json.dump(data, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, dest)

    def rollback():
        for name, exists in pending.get("originals", {}).items():
            dest = target(name)
            if exists:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(folder / "old" / name, dest)
            else:
                dest.unlink(missing_ok=True)

    if pending["stage"] == "applying":
        rollback()
        record("restore-result.json", {"ok": False, "message": "An interrupted restore was rolled back. Review the backup before retrying.", "at": time.time()})
    else:
        try:
            if pending["stage"] != "prepared":
                raise ValueError("Unknown restore state")
            for name in pending["members"]:
                target(name)
                source = folder / "new" / name
                if not source.resolve().is_relative_to((folder / "new").resolve()):
                    raise ValueError("Invalid staged file")
                with source.open("rb") as handle:
                    if hashlib.file_digest(handle, "sha256").hexdigest() != pending["hashes"][name]:
                        raise ValueError("Staged file changed")
            members = list(pending["members"])
            if pending.get("mode") == "mappings":
                from _backup_mapping import merge_mappings

                name = ".cw_databases/crosswatch.sqlite3"
                if members != [name]:
                    raise ValueError("Invalid mapping restore selection")
                merged = folder / "mapping-restore.sqlite3"
                merge_mappings(folder / "new" / name, target(name), merged)
                os.replace(merged, folder / "new" / name)
            for name in pending["members"]:
                if Path(name).suffix in {".db", ".sqlite", ".sqlite3"}:
                    members.extend(name + suffix for suffix in ("-wal", "-shm", "-journal") if name + suffix not in members)
            originals = {}
            for name in members:
                dest = target(name)
                originals[name] = dest.exists()
                if dest.exists():
                    old = folder / "old" / name
                    old.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                    shutil.copy2(dest, old)
            now = datetime.now(timezone.utc)
            archive_dir = base / "backups" / now.strftime("%Y-%m-%d")
            archive_dir.mkdir(parents=True, exist_ok=True)
            safety = archive_dir / f"crosswatch-backup-{now.strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:12]}.zip"
            manifest_files = []
            with zipfile.ZipFile(safety, "x", compression=zipfile.ZIP_DEFLATED) as archive:
                os.chmod(safety, 0o600)
                for name, exists in originals.items():
                    if not exists or name.endswith(("-wal", "-shm", "-journal")):
                        continue
                    source = folder / "old" / name
                    if Path(name).suffix in {".db", ".sqlite", ".sqlite3"}:
                        snapshot = folder / (uuid.uuid4().hex + ".db")
                        src = sqlite3.connect(str(target(name)))
                        dst = sqlite3.connect(str(snapshot))
                        try:
                            src.backup(dst)
                        finally:
                            src.close()
                            dst.close()
                        source = snapshot
                    with source.open("rb") as handle:
                        digest = hashlib.file_digest(handle, "sha256").hexdigest()
                    manifest_files.append({"path": name, "size": source.stat().st_size, "sha256": digest})
                    archive.write(source, name)
                key_included = any(row["path"] == ".cw_master_key" for row in manifest_files)
                archive.writestr("manifest.json", json.dumps({"kind": "crosswatch_backup", "schema_version": 1, "app_version": os.getenv("APP_VERSION", "unknown"), "scope": "full", "label": "pre-restore", "trigger": "pre_restore", "created_at": now.isoformat(), "files": manifest_files, "file_count": len(manifest_files), "master_key_included": key_included, "external_key_required": bool(not key_included and (os.getenv("CW_CONFIG_KEY") or os.getenv("CROSSWATCH_CONFIG_KEY")))}))
            try:
                safety = _encrypt_safety(base, safety)
            except Exception:
                safety.unlink(missing_ok=True)
                raise
            pending.update(stage="applying", originals=originals)
            record("pending.json", pending)
            for name in members:
                dest = target(name)
                source = folder / "new" / name
                if source.exists():
                    if not source.resolve().is_relative_to((folder / "new").resolve()):
                        raise ValueError("Invalid staged file")
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    temp = dest.with_name(dest.name + ".restore.tmp")
                    shutil.copy2(source, temp)
                    os.replace(temp, dest)
                else:
                    dest.unlink(missing_ok=True)
            record("restore-result.json", {"ok": True, "message": "Backup restored during startup.", "safety_backup": safety.relative_to(base / "backups").as_posix(), "at": time.time()})
        except Exception:
            if pending.get("stage") == "applying":
                rollback()
            record("restore-result.json", {"ok": False, "message": "Restore failed. Original files were preserved or restored.", "at": time.time()})
    marker.unlink()
    shutil.rmtree(folder)
