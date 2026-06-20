# /api/backupsAPI.py
# CrossWatch - Backup and restore API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from pathlib import Path
from typing import Any

import os
import tempfile
import threading

from fastapi import APIRouter, Body, File, Query, UploadFile
from fastapi.responses import FileResponse, JSONResponse, Response

from _logging import log as BASE_LOG
from services.backups import (
    MAX_ARCHIVE_BYTES,
    create_backup,
    delete_backup,
    enforce_backup_retention,
    list_backups,
    restore_backup,
    save_uploaded_backup,
    validate_backup,
    _resolve_backup_file,
)

router = APIRouter(prefix="/api/backups", tags=["backups"])
LOG = BASE_LOG.child("BACKUP")

def _ok(payload: dict[str, Any], *, status_code: int = 200) -> JSONResponse:
    payload.setdefault("ok", True)
    return JSONResponse(payload, status_code=status_code, headers={"Cache-Control": "no-store"})


def _err(msg: str, *, status_code: int = 400, extra: dict[str, Any] | None = None) -> JSONResponse:
    payload: dict[str, Any] = {"ok": False, "error": str(msg or "backup_request_failed")}
    if extra:
        payload.update(extra)
    return JSONResponse(payload, status_code=status_code, headers={"Cache-Control": "no-store"})


def _debug_failure(action: str, err: Exception) -> None:
    LOG.debug(
        f"{action} failure detail",
        extra={"error_type": type(err).__name__},
    )


def _refresh_scheduler() -> None:
    try:
        import crosswatch as CW

        scheduler = getattr(CW, "scheduler", None)
        if scheduler is not None:
            getattr(scheduler, "start", lambda: None)()
            getattr(scheduler, "refresh", lambda: None)()
    except Exception:
        pass


def _restart_soon() -> None:
    def _kill() -> None:
        os._exit(0)

    threading.Timer(0.75, _kill).start()


@router.get("/list")
def api_backups_list() -> JSONResponse:
    try:
        LOG.debug("backup list requested")
        return _ok({"backups": list_backups()})
    except Exception as e:
        LOG.warn(f"backup list request failed: {type(e).__name__}")
        _debug_failure("backup list", e)
        return _err("backup_list_failed")


@router.post("/create")
def api_backups_create(body: dict[str, Any] | None = Body(default=None)) -> JSONResponse:
    body = body or {}
    try:
        LOG.debug(
            "manual backup requested",
            extra={
                "scope": str(body.get("scope") or "app_state"),
                "include_snapshots": bool(body.get("include_snapshots")) if "include_snapshots" in body else None,
                "include_reports": bool(body.get("include_reports")) if "include_reports" in body else None,
                "include_cache": bool(body.get("include_cache")),
            },
        )
        res = create_backup(
            scope=str(body.get("scope") or "app_state"),
            label=str(body.get("label") or "manual"),
            include_snapshots=bool(body.get("include_snapshots")) if "include_snapshots" in body else None,
            include_reports=bool(body.get("include_reports")) if "include_reports" in body else None,
            include_cache=bool(body.get("include_cache")),
            trigger="manual",
        )
        return _ok({"backup": res})
    except Exception as e:
        LOG.warn(f"manual backup request failed: {type(e).__name__}")
        _debug_failure("manual backup", e)
        return _err("backup_create_failed")


@router.get("/download", response_model=None)
def api_backups_download(path: str = Query(..., description="Relative path under /config/backups")) -> Response:
    try:
        rel, file_path = _resolve_backup_file(path)
        LOG.info(f"backup download requested path={rel}")
        name = Path(rel).name
        return FileResponse(
            str(file_path),
            media_type="application/zip",
            filename=name,
            headers={
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
            },
        )
    except Exception as e:
        LOG.warn(f"backup download request failed: {type(e).__name__}")
        _debug_failure("backup download", e)
        return _err("backup_download_failed")


@router.post("/validate")
def api_backups_validate(body: dict[str, Any] = Body(...)) -> JSONResponse:
    try:
        path = str(body.get("path") or "").strip()
        LOG.debug("backup validate requested")
        return _ok({"validation": validate_backup(path)})
    except Exception as e:
        LOG.warn(f"backup validate request failed: {type(e).__name__}")
        _debug_failure("backup validate", e)
        return _err("backup_validate_failed")


@router.post("/restore")
def api_backups_restore(body: dict[str, Any] = Body(...)) -> JSONResponse:
    try:
        path = str(body.get("path") or "").strip()
        restart = bool(body.get("restart"))
        LOG.info(f"backup restore requested restart={restart}")
        res = restore_backup(path, create_pre_restore=True)
        if res.get("ok") and restart:
            res["restart_scheduled"] = True
            LOG.info("backup restore requested restart scheduled")
            _restart_soon()
        return _ok({"result": res}, status_code=200 if res.get("ok") else 400)
    except Exception as e:
        LOG.warn(f"backup restore request failed: {type(e).__name__}")
        _debug_failure("backup restore", e)
        return _err("backup_restore_failed")


@router.post("/delete")
def api_backups_delete(body: dict[str, Any] = Body(...)) -> JSONResponse:
    try:
        path = str(body.get("path") or "").strip()
        LOG.debug("backup delete requested")
        return _ok({"result": delete_backup(path)})
    except Exception as e:
        LOG.warn(f"backup delete request failed: {type(e).__name__}")
        _debug_failure("backup delete", e)
        return _err("backup_delete_failed")


@router.post("/upload")
async def api_backups_upload(file: UploadFile = File(...)) -> JSONResponse:
    suffix = Path(str(file.filename or "")).suffix.lower()
    if suffix != ".zip":
        LOG.warn("backup upload rejected invalid extension")
        return _err("Invalid backup path")

    tmp_name = ""
    try:
        LOG.info("backup upload requested")
        fd, tmp_name = tempfile.mkstemp(prefix="crosswatch-backup-upload-", suffix=".zip")
        total = 0
        with os.fdopen(fd, "wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_ARCHIVE_BYTES:
                    raise ValueError("Uploaded backup is too large")
                out.write(chunk)
        res = save_uploaded_backup(Path(tmp_name))
        tmp_name = ""
        return _ok({"backup": res})
    except Exception as e:
        LOG.warn(f"backup upload request failed: {type(e).__name__}")
        _debug_failure("backup upload", e)
        return _err("backup_upload_failed")
    finally:
        try:
            await file.close()
        except Exception:
            pass
        if tmp_name:
            try:
                Path(tmp_name).unlink(missing_ok=True)
            except Exception:
                pass


@router.get("/schedule")
def api_backups_schedule_get() -> JSONResponse:
    try:
        LOG.debug("backup schedule requested")
        from cw_platform.config_base import load_config

        cfg = load_config() or {}
        raw_sch = cfg.get("scheduling")
        sch: dict[str, Any] = raw_sch if isinstance(raw_sch, dict) else {}
        raw_adv = sch.get("advanced")
        adv: dict[str, Any] = raw_adv if isinstance(raw_adv, dict) else {}
        raw_jobs = adv.get("backup_jobs") or adv.get("backupJobs")
        jobs = raw_jobs if isinstance(raw_jobs, list) else []
        job = jobs[0] if isinstance(jobs, list) and jobs and isinstance(jobs[0], dict) else {}
        return _ok({"schedule": job, "advanced_enabled": bool(adv.get("enabled"))})
    except Exception as e:
        LOG.warn(f"backup schedule request failed: {type(e).__name__}")
        _debug_failure("backup schedule", e)
        return _err("backup_schedule_failed")


@router.post("/schedule")
def api_backups_schedule_post(body: dict[str, Any] = Body(...)) -> JSONResponse:
    try:
        from cw_platform.config_base import load_config, save_config

        enabled = bool(body.get("enabled"))
        LOG.info(f"saving backup schedule enabled={enabled}")
        cfg = load_config() or {}
        sch = cfg.setdefault("scheduling", {})
        if not isinstance(sch, dict):
            sch = {}
            cfg["scheduling"] = sch
        adv = sch.setdefault("advanced", {})
        if not isinstance(adv, dict):
            adv = {}
            sch["advanced"] = adv
        adv["enabled"] = bool(adv.get("enabled") or enabled)

        job = {
            "id": "app-backup-default",
            "scope": str(body.get("scope") or "app_state"),
            "at": str(body.get("at") or "03:00"),
            "days": body.get("days") if isinstance(body.get("days"), list) else [],
            "label_template": str(body.get("label_template") or "scheduled-{scope}-{date}"),
            "retention_days": int(body.get("retention_days") or 30),
            "max_backups": int(body.get("max_backups") or 10),
            "auto_delete_old": bool(body.get("auto_delete_old", True)),
            "include_snapshots": bool(body.get("include_snapshots")),
            "include_reports": bool(body.get("include_reports")),
            "include_cache": bool(body.get("include_cache")),
            "active": enabled,
        }

        raw_jobs0 = adv.get("backup_jobs")
        jobs0 = raw_jobs0 if isinstance(raw_jobs0, list) else []
        jobs = [x for x in jobs0 if not (isinstance(x, dict) and str(x.get("id") or "") == "app-backup-default")]
        jobs.insert(0, job)
        adv["backup_jobs"] = jobs
        save_config(cfg)
        _refresh_scheduler()
        LOG.success(f"backup schedule saved enabled={enabled}")
        return _ok({"schedule": job, "advanced_enabled": bool(adv.get("enabled"))})
    except Exception as e:
        LOG.warn(f"backup schedule save failed: {type(e).__name__}")
        _debug_failure("backup schedule save", e)
        return _err("backup_schedule_save_failed")


@router.post("/retention")
def api_backups_retention(body: dict[str, Any] | None = Body(default=None)) -> JSONResponse:
    body = body or {}
    try:
        LOG.info("backup retention requested")
        res = enforce_backup_retention(
            retention_days=int(body.get("retention_days") or 0),
            max_backups=int(body.get("max_backups") or 0),
            auto_delete_old=True,
        )
        return _ok({"result": res})
    except Exception as e:
        LOG.warn(f"backup retention request failed: {type(e).__name__}")
        _debug_failure("backup retention", e)
        return _err("backup_retention_failed")
