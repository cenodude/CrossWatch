# services/backup_manager.py
# CrossWatch - Backup plans and persistent local and remote jobs
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import json
import os
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from cw_platform.config_base import _decrypt_secret, _get_cipher, load_config
import _backup_crypto as backup_crypto
from services import backup_destinations as destinations, backups

PLAN_ID = "app-backup-default"
_LOCK = threading.RLock()
_WAKE = threading.Event()
_STOP = threading.Event()
_THREAD: threading.Thread | None = None


def private_dir() -> Path:
    path = backups.CONFIG / ".cw_backups"
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    return path


def _private_file(name: str) -> Path:
    if name in {"settings.json", "jobs.json", "pending.json", "restore-result.json"}:
        filename = name
    else:
        raise ValueError("Invalid backup settings file.")
    base = private_dir().resolve()
    path = base / filename
    if path.resolve().parent != base:
        raise ValueError("Invalid backup settings file.")
    return path


def read(name: str, fallback: Any) -> Any:
    path = _private_file(name)
    if not path.exists():
        return copy.deepcopy(fallback)
    return json.loads(path.read_text(encoding="utf-8"))


def write(name: str, value: Any) -> None:
    path = _private_file(name)
    temp = path.with_suffix("." + uuid.uuid4().hex + ".tmp")
    try:
        with temp.open("x", encoding="utf-8") as handle:
            os.chmod(temp, 0o600)
            json.dump(value, handle, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def settings() -> dict[str, Any]:
    with _LOCK:
        value = read("settings.json", {"configured": False, "components": ["application", "trackers", "captures"], "trackers": None, "encrypt_local": False, "password": "", "destination": {"kind": "none"}, "remote_keep": 30, "installation": uuid.uuid4().hex})
        if value["destination"].get("kind") not in destinations.SUPPORTED_KINDS:
            value["destination"] = {"kind": "none"}
            value["destination_notice"] = "The saved destination is no longer supported. Backups are saved locally. Choose Nextcloud / WebDAV or a mounted folder."
        return value


def reveal(raw: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(raw)
    for item in (result, result.get("destination") or {}):
        if item.get("password"):
            item["password"] = _decrypt_secret(item["password"])
    return result


def _encrypt_secret(value: str) -> str:
    cipher = _get_cipher(create=True)
    if cipher is None:
        raise RuntimeError("The configuration encryption key is unavailable.")
    return "enc:v1:" + cipher.encrypt(value.encode()).decode()


def plan() -> dict[str, Any]:
    from cw_platform.tracker_store import tracker_roots

    value = settings()
    value["has_password"] = bool(value.pop("password", ""))
    destination = value["destination"]
    destination["has_password"] = bool(destination.pop("password", ""))
    cfg = load_config()
    value["available_trackers"] = [{"id": inst, "label": str((cfg.get("crosswatch") or {}).get("label") or "Main tracker") if inst == "default" else str(((cfg.get("crosswatch") or {}).get("instances") or {}).get(inst, {}).get("label") or inst)} for inst, root in tracker_roots(cfg, backups.CONFIG) if root.is_dir()]
    value["key_source"] = "external" if backups._is_env_key_configured() else "included" if (backups.CONFIG / ".cw_master_key").is_file() else "not_needed"
    runtime = destinations.runtime_status()
    value["rclone_available"] = runtime["available"]
    value["rclone_error"] = runtime.get("error", "")
    value["mounted_root"] = str(destinations.mounted_root())
    value["capture_count"] = sum(1 for _ in (backups.CONFIG / "snapshots").rglob("*.json"))
    return value


def save_plan(body: dict[str, Any]) -> dict[str, Any]:
    with _LOCK:
        previous = settings()
        components = body.get("components")
        if not isinstance(components, list) or not components or set(components) - {"application", "trackers", "captures"}:
            raise ValueError("Choose at least one backup content type.")
        selected = body.get("trackers")
        if selected is not None and "trackers" in components:
            available = {t["id"] for t in plan()["available_trackers"]}
            if not isinstance(selected, list) or not selected or set(selected) - available:
                raise ValueError("Choose an existing tracker or all trackers.")
        password = str(body.get("password") or "")
        if password and len(password) < 12:
            raise ValueError("Use at least 12 characters for the backup password.")
        destination = copy.deepcopy(body.get("destination") or {"kind": "none"})
        if destination.get("kind") == "webdav" and not destination.get("password"):
            old = previous.get("destination") or {}
            if old.get("url") != destination.get("url") or old.get("username") != destination.get("username"):
                raise ValueError("Enter the app password for this destination.")
            destination["password"] = _decrypt_secret(old.get("password") or "")
        destination = destinations.validate_destination(destination, backups.CONFIG)
        encrypted = bool(body.get("encrypt_local"))
        if (encrypted or destination["kind"] != "none") and not (password or previous.get("password")):
            raise ValueError("Set a backup password and save it outside CW before enabling encryption or an additional copy.")
        if password and body.get("password_saved") is not True:
            raise ValueError("Confirm that you have saved your backup password or recovery key.")
        if destination.get("password"):
            old_password = previous.get("destination", {}).get("password", "")
            destination["password"] = old_password if old_password and _decrypt_secret(old_password) == destination["password"] else _encrypt_secret(destination["password"])
        updated = {"configured": True, "components": sorted(set(components)), "trackers": selected, "encrypt_local": encrypted, "password": _encrypt_secret(password) if password else previous.get("password", ""), "destination": destination, "remote_keep": max(1, min(365, int(body.get("remote_keep") or 30))), "installation": previous["installation"]}
        write("settings.json", updated)
    return plan()


def password_for(path: str, supplied: str = "") -> str:
    if supplied:
        return supplied
    if not path.endswith(".cwbackup"):
        return ""
    try:
        return reveal(settings()).get("password") or ""
    except RuntimeError as exc:
        raise ValueError("Enter the backup password saved outside CW.") from exc


def _jobs() -> list[dict[str, Any]]:
    return read("jobs.json", [])


def status() -> dict[str, Any]:
    with _LOCK:
        rows = _jobs()
        public = [{k: v for k, v in row.items() if k not in {"settings", "remote_local"}} for row in rows]
        return {"jobs": public[-100:][::-1], "pending_restore": (private_dir() / "pending.json").exists(), "restore": read("restore-result.json", None)}


def _update(job_id: str, **fields: Any) -> dict[str, Any]:
    with _LOCK:
        rows = _jobs()
        row = next(r for r in rows if r["id"] == job_id)
        row.update(fields, updated_at=time.time())
        write("jobs.json", rows)
        return copy.deepcopy(row)


def enqueue(trigger: str = "manual") -> dict[str, Any]:
    with _LOCK:
        current = settings()
        if not current.get("configured"):
            raise ValueError("Save your backup settings first.")
        if (private_dir() / "pending.json").exists():
            raise ValueError("A restore is waiting for restart.")
        rows = _jobs()
        if any(row["stage"] in {"queued", "creating", "validating", "encrypting", "uploading", "verifying"} for row in rows):
            raise ValueError("A backup is already queued or running.")
        job = {"id": uuid.uuid4().hex, "stage": "queued", "trigger": trigger, "created_at": time.time(), "updated_at": time.time(), "attempts": 0, "retry_at": 0, "settings": current, "local_status": "pending", "remote_status": "pending" if current["destination"]["kind"] != "none" else "disabled", "bytes": 0, "total_bytes": 0}
        rows.append(job)
        write("jobs.json", rows)
    start()
    _WAKE.set()
    return {"id": job["id"], "stage": "queued"}


def retry(job_id: str) -> None:
    with _LOCK:
        rows = _jobs()
        if any(r["stage"] not in {"complete", "failed"} for r in rows):
            raise ValueError("A backup is already queued or running.")
        row = next((r for r in rows if r["id"] == job_id), None)
        if not row or row["stage"] != "failed":
            raise ValueError("Choose a failed backup job.")
        if row.get("local_status") in {"expired", "deleted"}:
            raise ValueError("This local backup is no longer available. Create a new backup instead.")
        if (private_dir() / "pending.json").exists():
            raise ValueError("A restore is waiting for restart.")
        current = settings()
        old_destination = row["settings"]["destination"]
        new_destination = current["destination"]
        if {k: v for k, v in old_destination.items() if k != "password"} == {k: v for k, v in new_destination.items() if k != "password"}:
            row["settings"]["destination"] = new_destination
        _update(job_id, settings=row["settings"], stage="queued", attempts=0, retry_at=0, error="")
    start()
    _WAKE.set()


def delete_local(path: str) -> dict[str, Any]:
    with _LOCK:
        rows = [r for r in _jobs() if r.get("path") == path]
        if any(r["stage"] not in {"complete", "failed"} for r in rows):
            raise ValueError("Wait for this backup transfer to finish before deleting its local archive.")
        result = backups.delete_backup(path)
        for row in rows:
            _update(row["id"], local_status="deleted", retry_at=0)
        return result


def _encrypt_archive(source: Path, target: Path, password: str, manifest: dict[str, Any]) -> None:
    temp = target.with_suffix(target.suffix + "." + uuid.uuid4().hex + ".tmp")
    try:
        backup_crypto.encrypt(source, temp, password)
        os.replace(temp, target)
        target.with_suffix(".metadata.json").write_text(json.dumps({k: v for k, v in manifest.items() if k != "files"}), encoding="utf-8")
    finally:
        temp.unlink(missing_ok=True)


def run_job(job: dict[str, Any]) -> None:
    ident = job["id"]
    unsaved = None
    phase = "local" if job.get("local_status") != "saved" else "remote"
    remote_kind = ""
    try:
        setup = reveal(job["settings"])
        if job.get("local_status") != "saved":
            _update(ident, stage="creating", error="")
            result = backups.create_backup(scope="selected", components=setup["components"], trackers=setup["trackers"], trigger=job["trigger"], job_id=PLAN_ID, label="automatic" if job["trigger"] == "scheduler" else "manual", password=setup["password"] if setup["encrypt_local"] else "", record_event=False)
            _, unsaved = backups._resolve_backup_file(result["path"])
            _update(ident, stage="validating")
            validation = backups.validate_backup(result["path"], setup["password"] if setup["encrypt_local"] else "")
            if not validation["ok"]:
                raise ValueError("The local backup did not pass validation.")
            manifest = validation["manifest"]
            job = _update(ident, local_status="saved", path=result["path"], local_saved_at=time.time(), manifest={k: v for k, v in manifest.items() if k != "files"})
            unsaved = None
            _record_backup(job["trigger"], ok=True, scope=result.get("scope") or "selected", size=result.get("size"), label=result.get("label"), path=result.get("path"))
            phase = "remote"
        try:
            _local_retention(job)
        except Exception:
            _update(ident, warning="Backup saved. Old automatic local backups could not be cleaned up. Check permissions and storage.")
        _, source = backups._resolve_backup_file(job["path"])
        destination = setup["destination"]
        remote_kind = str(destination.get("kind") or "")
        if destination["kind"] not in destinations.SUPPORTED_KINDS:
            raise destinations.UnsupportedDestination("This destination is no longer supported. Your local backup is available. Choose Nextcloud / WebDAV or a mounted folder.")
        if destination["kind"] != "none":
            if source.suffix != ".cwbackup":
                _update(ident, stage="encrypting")
                remote_copy = private_dir() / (ident + ".cwbackup")
                if not remote_copy.exists():
                    _encrypt_archive(source, remote_copy, setup["password"], job["manifest"])
                source = remote_copy
                _update(ident, remote_local=source.name)
            name = setup["installation"] + "/" + Path(job["path"]).with_suffix(".cwbackup").name
            _update(ident, stage="uploading", remote_status="uploading", total_bytes=source.stat().st_size, bytes=0, remote_path=name)
            def report_progress(values: dict[str, Any]) -> None:
                _update(ident, **values)

            destinations.upload(destination, source, name, report_progress)
            _update(ident, stage="verifying", remote_status="verifying")
            destinations.verify(destination, source, name)
            _update(ident, remote_status="verified", remote_verified_at=time.time(), bytes=source.stat().st_size)
            _record_upload(remote_kind, ok=True, trigger=job["trigger"], path=name)
            if source.parent == private_dir():
                source.unlink(missing_ok=True)
                source.with_suffix(".metadata.json").unlink(missing_ok=True)
        phase = "done"
        _update(ident, stage="complete", retry_at=0)
        try:
            _remote_retention(job, setup)
        except Exception:
            _update(ident, warning="Backup saved. Old automatic copies could not be cleaned up. Check destination permissions and storage.")
    except Exception as exc:
        if unsaved is not None:
            unsaved.unlink(missing_ok=True)
        current = next(r for r in _jobs() if r["id"] == ident)
        attempts = int(current.get("attempts") or 0) + 1
        message = str(exc) if isinstance(exc, ValueError) else "Backup operation failed. Check available storage and connection settings."
        if phase == "local":
            _record_backup(job["trigger"], ok=False, scope="selected", reason=message)
        elif phase == "remote" and remote_kind != "none":
            _record_upload(remote_kind or "remote", ok=False, trigger=job["trigger"], reason=message)
        _update(ident, stage="failed", attempts=attempts, error=message, remote_status="failed" if current.get("local_status") == "saved" and current["remote_status"] != "disabled" else current["remote_status"], retry_at=time.time() + min(3600, 60 * 2 ** attempts) if attempts < 5 and not isinstance(exc, (destinations.RcloneUnavailable, destinations.UnsupportedDestination)) else 0)


def _record_backup(trigger: Any, *, ok: bool, scope: str, **fields: Any) -> None:
    try:
        from cw_platform.event_archive import record_backup

        record_backup(scope, trigger=trigger, ok=ok, **fields)
    except Exception:
        pass


def _record_upload(destination: str, *, ok: bool, trigger: Any, reason: str = "", path: str = "") -> None:
    try:
        from cw_platform.event_archive import record_backup_upload

        record_backup_upload(destination, ok=ok, trigger=trigger, reason=reason, path=path)
    except Exception:
        pass


def prune_local(*, retention_days: int = 0, max_backups: int = 0, auto_delete_old: bool = False, job_id: str | None = None, trigger: str = "manual") -> dict[str, Any]:
    with _LOCK:
        rows = _jobs()
        protected = [row.get("path", "") for row in rows if row["stage"] not in {"complete", "failed"}]
        pending = read("pending.json", {})
        if pending.get("path"):
            protected.append(pending["path"])
        result = backups.enforce_backup_retention(retention_days=retention_days, max_backups=max_backups, auto_delete_old=auto_delete_old, job_id=job_id, protected=protected)
        if not result["applied"]:
            return result
        deleted = set(result["deleted"])
        try:
            from cw_platform.event_archive import record_backup_cleanup

            record_backup_cleanup(len(deleted), trigger=trigger)
        except Exception:
            pass
        for row in rows:
            if row.get("path") in deleted:
                fields: dict[str, Any] = {"local_status": "expired", "retry_at": 0}
                if row.get("remote_status") in {"pending", "failed"}:
                    fields.update(remote_status="expired", error="Local backup expired under the retention limit. Create a new backup to send a destination copy.")
                _update(row["id"], **fields)
                row.update(fields)
            if row.get("local_status") == "expired" and re.fullmatch(r"[0-9a-f]{32}", str(row.get("id") or "")):
                source = private_dir() / (row["id"] + ".cwbackup")
                try:
                    source.unlink(missing_ok=True)
                    source.with_suffix(".metadata.json").unlink(missing_ok=True)
                except OSError:
                    result["errors"].append("Could not remove an expired backup transfer file.")
        result["ok"] = not result["errors"]
        return result


def _local_retention(job: dict[str, Any]) -> None:
    if job["trigger"] != "scheduler":
        return
    cfg = load_config()
    jobs = ((cfg.get("scheduling") or {}).get("advanced") or {}).get("backup_jobs") or []
    schedule = next((j for j in jobs if j.get("id") == PLAN_ID), {})
    result = prune_local(max_backups=int(schedule.get("max_backups") or 7), retention_days=int(schedule.get("retention_days") or 0), auto_delete_old=bool(schedule.get("auto_delete_old", True)), job_id=PLAN_ID, trigger="scheduler")
    if not result["ok"]:
        raise OSError("Automatic local backup cleanup failed.")


def _remote_retention(job: dict[str, Any], setup: dict[str, Any]) -> None:
    if job["trigger"] != "scheduler" or setup["destination"]["kind"] == "none":
        return
    with _LOCK:
        rows = _jobs()
    verified = [r for r in rows if r.get("trigger") == "scheduler" and r.get("remote_status") == "verified" and r["settings"]["destination"] == job["settings"]["destination"]]
    verified.sort(key=lambda row: row["created_at"], reverse=True)
    for row in verified[setup["remote_keep"]:]:
        destinations.run(setup["destination"], ["deletefile", destinations.remote(setup["destination"], row["remote_path"])])
        _update(row["id"], remote_status="expired")


def _work() -> None:
    with _LOCK:
        rows = _jobs()
        for row in rows:
            if row["stage"] not in {"complete", "failed"}:
                row["stage"] = "queued"
        write("jobs.json", rows)
    while not _STOP.is_set():
        with _LOCK:
            job = None if (private_dir() / "pending.json").exists() else next((copy.deepcopy(r) for r in _jobs() if r["stage"] == "queued" or r["stage"] == "failed" and 0 < r.get("retry_at", 0) <= time.time()), None)
            if job:
                job = _update(job["id"], stage="queued")
        if job:
            run_job(job)
        else:
            _WAKE.wait(10)
            _WAKE.clear()


def start() -> None:
    global _THREAD
    with _LOCK:
        if _THREAD and _THREAD.is_alive():
            return
        _STOP.clear()
        _THREAD = threading.Thread(target=_work, name="cw-backups", daemon=True)
        _THREAD.start()


def stop() -> None:
    _STOP.set()
    _WAKE.set()


def remote_list() -> list[dict[str, Any]]:
    destination = reveal(settings())["destination"]
    if destination["kind"] == "none":
        raise ValueError("Configure an additional backup destination first.")
    return destinations.list_remote(destination)


def download_remote(path: str) -> dict[str, Any]:
    destination = reveal(settings())["destination"]
    if destination["kind"] == "none":
        raise ValueError("Configure an additional backup destination first.")
    safe = destinations.safe_remote_path(path)
    name = Path(safe).name
    prefix = "crosswatch-backup-"
    suffix = ".cwbackup" if name.endswith(".cwbackup") else ".zip" if name.endswith(".zip") else ""
    if len(name) > 255 or not suffix or not name.startswith(prefix) or not re.fullmatch(r"[a-zA-Z0-9._-]+", name[len(prefix):-len(suffix)]):
        raise ValueError("Choose a CrossWatch backup archive.")
    folder = backups._backups_dir() / "imported"
    folder.mkdir(exist_ok=True)
    target = folder / (uuid.uuid4().hex + suffix)
    try:
        destinations.run(destination, ["copyto", destinations.remote(destination, safe), str(target), "--max-transfer", str(backups.MAX_ARCHIVE_BYTES), "--cutoff-mode", "hard"])
        if target.stat().st_size > backups.MAX_ARCHIVE_BYTES:
            raise ValueError("The backup archive is too large.")
        return {"path": backups._rel_from_backup_root(target)}
    except Exception:
        target.unlink(missing_ok=True)
        raise
