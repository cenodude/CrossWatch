# services/backups.py
# CrossWatch - Application backup and restore helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Literal

import hashlib
import json
import os
import re
import shutil
import uuid
import zipfile

from _logging import log as BASE_LOG
from cw_platform.config_base import CONFIG, _current_version_norm

BackupScope = Literal["config_only", "app_state", "full"]

LOG = BASE_LOG.child("BACKUP")

BACKUP_KIND = "crosswatch_backup"
BACKUP_SCHEMA_VERSION = 1
BACKUP_SCOPES = {"config_only", "app_state", "full"}
MAX_ARCHIVE_BYTES = 2 * 1024 * 1024 * 1024
MAX_ZIP_MEMBERS = 100_000
MANIFEST_NAME = "manifest.json"

_APP_STATE_FILES = (
    "config.json",
    ".cw_master_key",
    "state.json",
    "last_sync.json",
    "statistics.json",
    "watchlist_hide.json",
)
_APP_STATE_DIRS = (
    ".cw_state",
    "tls",
)
_FULL_DIRS = (
    "snapshots",
    ".cw_provider",
    "sync_reports",
    ".cw_cache",
)
_OPTIONAL_RESTORE_DIRS = (
    "cache",
)


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _backups_dir() -> Path:
    d = CONFIG / "backups"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _safe_backup_label(value: str) -> str:
    raw = re.sub(r"[^a-zA-Z0-9._ -]+", "", str(value or "").strip())
    raw = re.sub(r"\s+", "-", raw).strip("-._ ")
    return raw[:48] or "manual"


def render_backup_label_template(template: str, *, scope: str, ts: datetime | None = None) -> str:
    stamp = ts if isinstance(ts, datetime) else _utc_now()
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    raw = str(template or "").strip() or "scheduled-{scope}-{date}"
    values = {
        "scope": str(scope or "app_state").strip().lower(),
        "date": stamp.strftime("%Y-%m-%d"),
        "time": stamp.strftime("%H-%M"),
        "datetime": stamp.strftime("%Y-%m-%d_%H-%M"),
        "stamp": stamp.strftime("%Y%m%dT%H%M%SZ"),
    }
    try:
        rendered = raw.format_map(values)
    except Exception:
        rendered = raw
    return _safe_backup_label(rendered)


def _normalize_scope(scope: Any) -> BackupScope:
    raw = str(scope or "app_state").strip().lower()
    if raw not in BACKUP_SCOPES:
        raw = "app_state"
    return raw  # type: ignore[return-value]


def _is_env_key_configured() -> bool:
    return bool((os.getenv("CW_CONFIG_KEY") or "").strip() or (os.getenv("CROSSWATCH_CONFIG_KEY") or "").strip())


def _config_has_encrypted_values() -> bool:
    p = CONFIG / "config.json"
    if not p.exists() or not p.is_file():
        return False
    try:
        return "enc:v1:" in p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return False


def _rel_from_config(path: Path) -> str:
    base = CONFIG.resolve()
    target = path.resolve()
    try:
        rel = target.relative_to(base)
    except ValueError as e:
        raise ValueError("Path is outside the config directory") from e
    return rel.as_posix()


def _safe_rel_path(raw: Any) -> str:
    text = str(raw or "").strip().replace("\\", "/")
    if not text:
        raise ValueError("Path is required")
    posix = PurePosixPath(text)
    parts = [part for part in posix.parts if part not in ("", ".")]
    if posix.is_absolute() or not parts or any(part == ".." for part in parts):
        raise ValueError("Invalid path")
    if parts[0] == "backups":
        raise ValueError("Invalid path")
    return "/".join(parts)


def _resolve_backup_file(path: str, *, must_exist: bool = True) -> tuple[str, Path]:
    text = str(path or "").strip().replace("\\", "/")
    if not text:
        raise ValueError("Backup path is required")
    posix = PurePosixPath(text)
    parts = [part for part in posix.parts if part not in ("", ".")]
    if posix.is_absolute() or not parts or any(part == ".." for part in parts):
        raise ValueError("Invalid backup path")
    if Path(parts[-1]).suffix.lower() != ".zip":
        raise ValueError("Invalid backup path")

    base = _backups_dir().resolve()
    target = base.joinpath(*parts).resolve()
    try:
        target.relative_to(base)
    except ValueError as e:
        raise ValueError("Invalid backup path") from e
    if must_exist and (not target.exists() or not target.is_file()):
        raise ValueError("Backup not found")
    return "/".join(parts), target


def _resolve_restore_target(rel_path: str) -> Path:
    rel = _safe_rel_path(rel_path)
    allowed_files = set(_APP_STATE_FILES)
    allowed_dirs = set(_APP_STATE_DIRS) | set(_FULL_DIRS) | set(_OPTIONAL_RESTORE_DIRS)
    first = rel.split("/", 1)[0]
    if rel not in allowed_files and first not in allowed_dirs:
        raise ValueError("Invalid restore target")
    base = CONFIG.resolve()
    target = base.joinpath(*PurePosixPath(rel).parts).resolve()
    try:
        target.relative_to(base)
    except ValueError as e:
        raise ValueError("Invalid restore target") from e
    return target


def _iter_files_under(path: Path) -> Iterable[Path]:
    if path.is_file():
        if not path.is_symlink():
            yield path
        return
    if not path.is_dir() or path.is_symlink():
        return
    for child in sorted(path.rglob("*")):
        try:
            if child.is_file() and not child.is_symlink():
                yield child
        except OSError:
            continue


def _candidate_roots(
    scope: BackupScope,
    *,
    include_snapshots: bool | None = None,
    include_reports: bool | None = None,
    include_cache: bool = False,
) -> list[Path]:
    names: list[str] = []
    if scope == "config_only":
        names.extend(("config.json", ".cw_master_key"))
    else:
        names.extend(_APP_STATE_FILES)
        names.extend(_APP_STATE_DIRS)
        if scope == "full":
            names.extend(_FULL_DIRS)
        if include_snapshots is True and "snapshots" not in names:
            names.append("snapshots")
        if include_reports is True and "sync_reports" not in names:
            names.append("sync_reports")
        if include_cache:
            names.append("cache")

    out: list[Path] = []
    seen: set[str] = set()
    backup_root = _backups_dir().resolve()
    for name in names:
        rel = _safe_rel_path(name)
        p = (CONFIG / rel).resolve()
        try:
            p.relative_to(CONFIG.resolve())
        except ValueError:
            continue
        try:
            p.relative_to(backup_root)
            continue
        except ValueError:
            pass
        key = str(p)
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_zip_member(zf: zipfile.ZipFile, name: str) -> str:
    h = hashlib.sha256()
    with zf.open(name, "r") as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _backup_rel_for_new(ts: datetime, label: str) -> tuple[str, Path]:
    day_dir = _backups_dir() / ts.strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    stamp = ts.strftime("%Y%m%dT%H%M%SZ")
    base_name = f"crosswatch-backup-{stamp}-{uuid.uuid4().hex[:12]}.zip"
    target = day_dir / base_name
    i = 1
    while target.exists():
        target = day_dir / f"crosswatch-backup-{stamp}-{uuid.uuid4().hex[:12]}-{i}.zip"
        i += 1
    return _rel_from_backup_root(target), target


def _rel_from_backup_root(path: Path) -> str:
    base = _backups_dir().resolve()
    target = path.resolve()
    try:
        return target.relative_to(base).as_posix()
    except ValueError as e:
        raise ValueError("Path is outside the backups directory") from e


def _safe_log_text(value: Any) -> str:
    text = str(value or "").strip().replace("\r", " ").replace("\n", " ")
    try:
        text = text.replace(str(CONFIG.resolve()), "<config>")
        text = text.replace(str(_backups_dir().resolve()), "<backups>")
    except Exception:
        pass
    return text[:300] or "no detail"


def _safe_log_errors(errors: list[str]) -> list[str]:
    return [_safe_log_text(err) for err in errors[:10]]


def _error_text(err: Exception) -> str:
    text = _safe_log_text(err)
    return text if text != "no detail" else type(err).__name__


def create_backup(
    *,
    scope: BackupScope | str = "app_state",
    label: str = "manual",
    include_snapshots: bool | None = None,
    include_reports: bool | None = None,
    include_cache: bool = False,
    trigger: str = "manual",
) -> dict[str, Any]:
    sc = _normalize_scope(scope)
    ts = _utc_now()
    safe_label = _safe_backup_label(label or trigger or sc)
    rel, target = _backup_rel_for_new(ts, safe_label)
    tmp = target.with_suffix(target.suffix + f".tmp.{uuid.uuid4().hex[:8]}")

    LOG.info(f"creating backup scope={sc} trigger={trigger or 'manual'} label={safe_label}")
    LOG.debug(
        "backup create options",
        extra={
            "scope": sc,
            "trigger": str(trigger or "manual"),
            "include_snapshots": include_snapshots,
            "include_reports": include_reports,
            "include_cache": bool(include_cache),
        },
    )

    files: list[Path] = []
    seen: set[str] = set()
    for root in _candidate_roots(sc, include_snapshots=include_snapshots, include_reports=include_reports, include_cache=include_cache):
        for file_path in _iter_files_under(root):
            rel_file = _rel_from_config(file_path)
            if rel_file in seen:
                continue
            seen.add(rel_file)
            files.append(file_path)

    LOG.debug("backup file selection completed", extra={"scope": sc, "file_count": len(files)})

    manifest_files: list[dict[str, Any]] = []
    total_size = 0
    try:
        with zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for file_path in files:
                rel_file = _rel_from_config(file_path)
                st = file_path.stat()
                total_size += int(st.st_size)
                digest = _sha256_file(file_path)
                manifest_files.append(
                    {
                        "path": rel_file,
                        "size": int(st.st_size),
                        "sha256": digest,
                    }
                )
                zf.write(file_path, rel_file)

            key_included = any(row.get("path") == ".cw_master_key" for row in manifest_files)
            encrypted = _config_has_encrypted_values()
            manifest = {
                "kind": BACKUP_KIND,
                "schema_version": BACKUP_SCHEMA_VERSION,
                "created_at": ts.isoformat(),
                "app_version": _current_version_norm(),
                "scope": sc,
                "label": safe_label,
                "trigger": str(trigger or "manual"),
                "files": manifest_files,
                "file_count": len(manifest_files),
                "total_size": total_size,
                "config_encrypted": encrypted,
                "master_key_included": key_included,
                "external_key_required": bool(encrypted and not key_included),
                "env_key_configured": _is_env_key_configured(),
            }
            zf.writestr(MANIFEST_NAME, json.dumps(manifest, indent=2, sort_keys=False) + "\n")
        os.replace(tmp, target)
    except Exception as e:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        LOG.error(f"backup create failed scope={sc} trigger={trigger or 'manual'}: {type(e).__name__}")
        LOG.debug(f"backup create failure detail: {_error_text(e)}", extra={"scope": sc, "target": rel})
        raise

    result = {
        "ok": True,
        "path": rel,
        "created_at": ts.isoformat(),
        "scope": sc,
        "label": safe_label,
        "size": int(target.stat().st_size),
        "file_count": len(manifest_files),
        "total_size": total_size,
        "master_key_included": any(row.get("path") == ".cw_master_key" for row in manifest_files),
        "external_key_required": bool(_config_has_encrypted_values() and not any(row.get("path") == ".cw_master_key" for row in manifest_files)),
    }
    LOG.success(
        f"backup created scope={sc} trigger={trigger or 'manual'} path={rel} files={result['file_count']} size={result['size']}"
    )
    LOG.debug(
        "backup manifest summary",
        extra={
            "path": rel,
            "scope": sc,
            "file_count": result["file_count"],
            "total_size": total_size,
            "master_key_included": result["master_key_included"],
            "external_key_required": result["external_key_required"],
        },
    )
    return result


def _read_manifest_from_zip(zf: zipfile.ZipFile) -> dict[str, Any]:
    try:
        info = zf.getinfo(MANIFEST_NAME)
    except KeyError as e:
        raise ValueError("Backup manifest is missing") from e
    if info.file_size > 5 * 1024 * 1024:
        raise ValueError("Backup manifest is too large")
    with zf.open(info, "r") as f:
        data = json.loads(f.read().decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Invalid backup manifest")
    if str(data.get("kind") or "") != BACKUP_KIND:
        raise ValueError("Invalid backup kind")
    if int(data.get("schema_version") or 0) != BACKUP_SCHEMA_VERSION:
        raise ValueError("Unsupported backup schema")
    files = data.get("files")
    if not isinstance(files, list):
        raise ValueError("Invalid backup file list")
    return data


def validate_backup(path: str) -> dict[str, Any]:
    rel, target = _resolve_backup_file(path)
    LOG.debug(f"validating backup path={rel}")
    if target.stat().st_size > MAX_ARCHIVE_BYTES:
        LOG.warn(f"backup validation rejected oversized archive path={rel}")
        raise ValueError("Backup archive is too large")

    errors: list[str] = []
    try:
        with zipfile.ZipFile(target, "r") as zf:
            infos = zf.infolist()
            if len(infos) > MAX_ZIP_MEMBERS:
                raise ValueError("Backup archive contains too many files")
            bad = zf.testzip()
            if bad:
                raise ValueError("Backup archive failed integrity check")
            manifest = _read_manifest_from_zip(zf)
            info_by_name = {info.filename: info for info in infos}
            for row in manifest.get("files") or []:
                if not isinstance(row, Mapping):
                    errors.append("Invalid manifest file entry")
                    continue
                member = _safe_rel_path(row.get("path"))
                try:
                    _resolve_restore_target(member)
                except Exception:
                    errors.append(f"Invalid restore target: {member}")
                    continue
                if member not in info_by_name:
                    errors.append(f"Missing archive member: {member}")
                    continue
                info = info_by_name[member]
                if info.is_dir():
                    errors.append(f"Invalid directory member: {member}")
                    continue
                expected_size = int(row.get("size") or -1)
                if expected_size >= 0 and expected_size != int(info.file_size):
                    errors.append(f"Size mismatch: {member}")
                    continue
                expected_hash = str(row.get("sha256") or "").strip().lower()
                if not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
                    errors.append(f"Invalid hash: {member}")
                    continue
                actual_hash = _sha256_zip_member(zf, member)
                if actual_hash != expected_hash:
                    errors.append(f"Hash mismatch: {member}")
    except zipfile.BadZipFile as e:
        LOG.warn(f"backup validation rejected invalid archive path={rel}")
        raise ValueError("Invalid backup archive") from e

    result = {
        "ok": len(errors) == 0,
        "path": rel,
        "manifest": manifest,
        "errors": errors,
    }
    if result["ok"]:
        LOG.success(f"backup validated path={rel}")
    else:
        LOG.warn(f"backup validation failed path={rel} errors={len(errors)}")
        LOG.debug("backup validation errors", extra={"path": rel, "errors": _safe_log_errors(errors), "error_count": len(errors)})
    return result


def list_backups() -> list[dict[str, Any]]:
    base = _backups_dir()
    out: list[dict[str, Any]] = []
    for p in sorted(base.rglob("*.zip")):
        try:
            rel = _rel_from_backup_root(p)
        except Exception:
            continue
        row: dict[str, Any] = {
            "path": rel,
            "size": int(p.stat().st_size),
            "mtime": int(p.stat().st_mtime),
            "valid": None,
        }
        try:
            with zipfile.ZipFile(p, "r") as zf:
                manifest = _read_manifest_from_zip(zf)
            row.update(
                {
                    "valid": True,
                    "created_at": manifest.get("created_at"),
                    "app_version": manifest.get("app_version"),
                    "scope": manifest.get("scope"),
                    "label": manifest.get("label"),
                    "file_count": manifest.get("file_count"),
                    "total_size": manifest.get("total_size"),
                    "master_key_included": manifest.get("master_key_included"),
                    "external_key_required": manifest.get("external_key_required"),
                }
            )
        except Exception:
            row["valid"] = False
        out.append(row)
    out.sort(key=lambda x: int(x.get("mtime") or 0), reverse=True)
    LOG.debug(f"listed backups count={len(out)}")
    return out


def delete_backup(path: str) -> dict[str, Any]:
    rel, target = _resolve_backup_file(path)
    LOG.info(f"deleting backup path={rel}")
    target.unlink()
    LOG.success(f"backup deleted path={rel}")
    return {"ok": True, "deleted": rel}


def enforce_backup_retention(*, retention_days: int = 0, max_backups: int = 0, auto_delete_old: bool = False) -> dict[str, Any]:
    if not auto_delete_old:
        LOG.debug("backup retention skipped auto_delete_old=false")
        return {"ok": True, "applied": False, "deleted": [], "errors": []}

    keep_days = max(0, int(retention_days or 0))
    keep_count = max(0, int(max_backups or 0))
    LOG.info(f"applying backup retention days={keep_days} max={keep_count}")
    rows = list_backups()
    cutoff = _utc_now() - timedelta(days=keep_days) if keep_days > 0 else None
    to_delete: list[str] = []
    kept: list[dict[str, Any]] = []

    for row in rows:
        path = str(row.get("path") or "")
        created = datetime.fromtimestamp(int(row.get("mtime") or 0), tz=timezone.utc)
        if cutoff is not None and created < cutoff:
            to_delete.append(path)
            continue
        kept.append(row)

    if keep_count > 0 and len(kept) > keep_count:
        to_delete.extend(str(row.get("path") or "") for row in kept[keep_count:])

    LOG.debug(
        "backup retention candidates",
        extra={"total_backups": len(rows), "candidate_count": len(to_delete), "retention_days": keep_days, "max_backups": keep_count},
    )
    deleted: list[str] = []
    errors: list[str] = []
    seen: set[str] = set()
    for rel in to_delete:
        if not rel or rel in seen:
            continue
        seen.add(rel)
        try:
            deleted.append(str(delete_backup(rel).get("deleted") or rel))
        except Exception as e:
            errors.append(f"{rel}: {type(e).__name__}")
    result = {"ok": not errors, "applied": True, "deleted": deleted, "errors": errors}
    if errors:
        LOG.warn(f"backup retention completed with errors deleted={len(deleted)} errors={len(errors)}")
        LOG.debug("backup retention errors", extra={"errors": _safe_log_errors(errors), "error_count": len(errors)})
    else:
        LOG.success(f"backup retention completed deleted={len(deleted)}")
    return result


def restore_backup(path: str, *, create_pre_restore: bool = True) -> dict[str, Any]:
    validation = validate_backup(path)
    if not validation.get("ok"):
        LOG.warn(f"backup restore blocked validation_failed path={validation.get('path') or 'unknown'}")
        return {"ok": False, "error": "backup_validation_failed", "errors": validation.get("errors") or []}

    rel, target = _resolve_backup_file(path)
    LOG.info(f"restoring backup path={rel} pre_restore={bool(create_pre_restore)}")
    raw_manifest = validation.get("manifest")
    manifest: dict[str, Any] = raw_manifest if isinstance(raw_manifest, dict) else {}
    if bool(manifest.get("external_key_required")) and not _is_env_key_configured():
        LOG.warn(f"backup restore blocked external_config_key_required path={rel}")
        return {
            "ok": False,
            "error": "external_config_key_required",
            "path": rel,
        }

    pre_restore = None
    if create_pre_restore:
        pre_restore = create_backup(scope="app_state", label="pre-restore", trigger="pre_restore")
        LOG.debug(
            "pre-restore backup created",
            extra={"source": rel, "pre_restore_path": str((pre_restore or {}).get("path") or "")},
        )

    restored: list[str] = []
    errors: list[str] = []
    try:
        with zipfile.ZipFile(target, "r") as zf:
            raw_files = manifest.get("files")
            files = raw_files if isinstance(raw_files, list) else []
            for row in files:
                if not isinstance(row, Mapping):
                    continue
                member = _safe_rel_path(row.get("path"))
                dst = _resolve_restore_target(member)
                tmp = dst.with_suffix(dst.suffix + f".restore.{uuid.uuid4().hex[:8]}.tmp")
                try:
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    with zf.open(member, "r") as src, tmp.open("wb") as out:
                        shutil.copyfileobj(src, out, length=1024 * 1024)
                    os.replace(tmp, dst)
                    restored.append(member)
                except Exception as e:
                    errors.append(f"{member}: {type(e).__name__}")
                    try:
                        tmp.unlink(missing_ok=True)
                    except Exception:
                        pass
    except zipfile.BadZipFile:
        LOG.warn(f"backup restore failed invalid archive path={rel}")
        return {"ok": False, "error": "invalid_backup_archive", "restored": restored, "errors": errors}

    result = {
        "ok": len(errors) == 0,
        "path": rel,
        "pre_restore_backup": pre_restore,
        "restored": restored,
        "errors": errors,
        "restart_required": True,
    }
    if result["ok"]:
        LOG.success(f"backup restored path={rel} files={len(restored)}")
    else:
        LOG.warn(f"backup restore completed with errors path={rel} restored={len(restored)} errors={len(errors)}")
        LOG.debug("backup restore errors", extra={"path": rel, "errors": _safe_log_errors(errors), "error_count": len(errors)})
    return result


def save_uploaded_backup(upload_path: Path) -> dict[str, Any]:
    if upload_path.stat().st_size > MAX_ARCHIVE_BYTES:
        LOG.warn("uploaded backup rejected oversized archive")
        raise ValueError("Uploaded backup is too large")
    LOG.info("importing uploaded backup")
    ts = _utc_now()
    imports_dir = _backups_dir() / "imported" / ts.strftime("%Y-%m-%d")
    imports_dir.mkdir(parents=True, exist_ok=True)
    target = imports_dir / f"crosswatch-import-{ts.strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}.zip"
    shutil.move(str(upload_path), target)
    rel = _rel_from_backup_root(target)
    validation = validate_backup(rel)
    if not validation.get("ok"):
        target.unlink(missing_ok=True)
        LOG.warn(f"uploaded backup rejected validation_failed path={rel}")
        raise ValueError("Uploaded backup failed validation")
    LOG.success(f"uploaded backup imported path={rel}")
    return {"ok": True, "path": rel, "validation": validation}
