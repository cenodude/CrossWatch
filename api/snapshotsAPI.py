# /api/snapshotsAPI.py
# CrossWatch - Snapshots API (watchlist/ratings/history)
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
from typing import Any, Literal, cast

from fastapi import APIRouter, Body, Query, Request
from fastapi.responses import JSONResponse

from cw_platform.access_policy import filter_instances_for_user, request_user, user_can_access_instance
from cw_platform.config_base import load_config
from cw_platform.provider_instances import normalize_instance_id, provider_display_key
from services.snapshots import (
    clear_provider_features,
    create_snapshot,
    get_capture_progress,
    list_snapshots,
    read_snapshot,
    restore_snapshot,
    snapshot_manifest,
    delete_snapshot,
    delete_all_snapshots,
    diff_snapshots,
    diff_snapshots_extended,
    start_capture_job,
    start_provider_cleanup_job,
    start_restore_job,
)

router = APIRouter(prefix="/api/snapshots", tags=["snapshots"])
_LOG = logging.getLogger("crosswatch.api.snapshots")

RestoreMode = Literal["merge", "clear_restore"]
Feature = Literal["watchlist", "ratings", "history", "progress", "all"]
_SAFE_ERROR_PREFIXES = (
    "Snapshot path is required",
    "Invalid snapshot path",
    "Snapshot not found",
    "Unknown provider:",
    "Provider not configured:",
    "Feature not enabled for provider:",
    "Unsupported feature:",
    "No shared child captures found",
    "Feature not available in both full captures:",
    "Compare Captures only supports",
    "Advanced compare supports",
    "Invalid capture contents",
    "Invalid compare kind",
    "snapshot_manifest_failed",
    "snapshot_list_failed",
    "snapshot_read_failed",
    "snapshot_diff_failed",
    "snapshot_create_failed",
    "snapshot_progress_failed",
    "snapshot_restore_failed",
    "snapshot_delete_failed",
    "snapshot_clear_failed",
    "snapshot_tools_clear_failed",
)


def _ok(payload: dict[str, Any], *, status_code: int = 200) -> JSONResponse:
    payload.setdefault("ok", True)
    return JSONResponse(payload, status_code=status_code)


def _public_error(msg: str, default: str = "snapshot_request_failed") -> str:
    text = str(msg or "").strip()
    if text and any(text.startswith(prefix) for prefix in _SAFE_ERROR_PREFIXES):
        return text
    return default


def _err(msg: str, *, status_code: int = 400, extra: dict[str, Any] | None = None) -> JSONResponse:
    payload: dict[str, Any] = {"ok": False, "error": _public_error(msg)}
    if extra:
        payload.update(extra)
    return JSONResponse(payload, status_code=status_code)


def _log_failure(action: str, exc: Exception) -> None:
    _LOG.warning("%s failed", action, exc_info=True)


def _is_admin_request(request: Request | None) -> bool:
    user = request_user(request)
    return not user or bool(user.get("is_admin"))


def _scope_denied() -> JSONResponse:
    return JSONResponse({"ok": False, "error": "profile_scope_denied"}, status_code=403)


def _snapshot_allowed(cfg: dict[str, Any], request: Request | None, snap: dict[str, Any]) -> bool:
    provider = provider_display_key(snap.get("provider"))
    instance = normalize_instance_id(snap.get("instance") or snap.get("instance_id") or snap.get("profile") or "default")
    return user_can_access_instance(cfg, request_user(request), provider, instance)


def _filter_snapshot_rows(cfg: dict[str, Any], request: Request | None, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if _is_admin_request(request):
        return rows
    return [row for row in rows if isinstance(row, dict) and _snapshot_allowed(cfg, request, row)]


@router.get("/manifest")
def api_snapshots_manifest(request: Request = cast(Request, None)) -> JSONResponse:
    try:
        cfg = load_config() or {}
        providers = snapshot_manifest(cfg)
        user = request_user(request)
        if user and not bool(user.get("is_admin")):
            scoped: list[dict[str, Any]] = []
            for provider in providers:
                if not isinstance(provider, dict):
                    continue
                insts = provider.get("instances")
                if not isinstance(insts, list):
                    continue
                allowed_ids = set(filter_instances_for_user(cfg, user, provider.get("id"), [normalize_instance_id(inst.get("id")) for inst in insts if isinstance(inst, dict)]))
                keep = [inst for inst in insts if isinstance(inst, dict) and normalize_instance_id(inst.get("id")) in allowed_ids]
                if not keep:
                    continue
                row = dict(provider)
                row["instances"] = keep
                row["configured"] = any(bool(inst.get("configured")) for inst in keep)
                scoped.append(row)
            providers = scoped
        return _ok({"providers": providers})
    except Exception as e:
        _log_failure("snapshot manifest request", e)
        return _err("snapshot_manifest_failed")


@router.get("/list")
def api_snapshots_list(request: Request = cast(Request, None)) -> JSONResponse:
    try:
        cfg = load_config() or {}
        return _ok({"snapshots": _filter_snapshot_rows(cfg, request, list_snapshots())})
    except Exception as e:
        _log_failure("snapshot list request", e)
        return _err("snapshot_list_failed")


@router.get("/read")
def api_snapshots_read(path: str = Query(..., description="Relative path under /config/snapshots"), request: Request = cast(Request, None)) -> JSONResponse:
    try:
        snap = read_snapshot(path)
        cfg = load_config() or {}
        if not _snapshot_allowed(cfg, request, snap):
            return _scope_denied()
        return _ok({"snapshot": snap})
    except Exception as e:
        _log_failure("snapshot read request", e)
        return _err("snapshot_read_failed")



@router.get("/diff")
def api_snapshots_diff(
    a: str = Query(..., description="Snapshot A path (relative under /config/snapshots)"),
    b: str = Query(..., description="Snapshot B path (relative under /config/snapshots)"),
    limit: int = Query(200, ge=1, le=2000),
    max_changes: int = Query(25, ge=1, le=200),
    request: Request = cast(Request, None),
) -> JSONResponse:
    try:
        cfg = load_config() or {}
        if not _snapshot_allowed(cfg, request, read_snapshot(a)) or not _snapshot_allowed(cfg, request, read_snapshot(b)):
            return _scope_denied()
        res = diff_snapshots(a, b, limit=limit, max_changes=max_changes)
        return _ok({"diff": res})
    except Exception as e:
        _log_failure("snapshot diff request", e)
        return _err("snapshot_diff_failed")


@router.get("/diff/extended")
def api_snapshots_diff_extended(
    a: str = Query(..., description="Snapshot A path (relative under /config/snapshots)"),
    b: str = Query(..., description="Snapshot B path (relative under /config/snapshots)"),
    feature: str = Query("", description="Specific feature to compare when using full captures"),
    kind: str = Query("all", description="all|added|removed|updated|unchanged"),
    q: str = Query("", description="Search query"),
    offset: int = Query(0, ge=0),
    limit: int = Query(5000, ge=1, le=20000),
    max_changes: int = Query(250, ge=1, le=1000),
    max_depth: int = Query(6, ge=1, le=12),
    request: Request = cast(Request, None),
) -> JSONResponse:
    try:
        cfg = load_config() or {}
        if not _snapshot_allowed(cfg, request, read_snapshot(a)) or not _snapshot_allowed(cfg, request, read_snapshot(b)):
            return _scope_denied()
        res = diff_snapshots_extended(
            a,
            b,
            feature=feature,
            kind=kind,
            q=q,
            offset=offset,
            limit=limit,
            max_depth=max_depth,
            max_changes=max_changes,
        )
        return _ok({"diff": res})
    except Exception as e:
        _log_failure("snapshot extended diff request", e)
        return _err("snapshot_diff_failed")

@router.post("/create")
def api_snapshots_create(body: dict[str, Any] = Body(...), request: Request = cast(Request, None)) -> JSONResponse:
    provider = str(body.get("provider") or "").strip()
    instance = str(body.get("instance") or body.get("instance_id") or body.get("profile") or "").strip()
    feature = str(body.get("feature") or "").strip().lower()
    label = str(body.get("label") or "").strip()
    progress_id = str(body.get("progress_id") or body.get("progressId") or "").strip()
    background = bool(body.get("background") or body.get("async") or body.get("async_job"))
    try:
        cfg = load_config() or {}
        if not user_can_access_instance(cfg, request_user(request), provider, instance):
            return _scope_denied()
        if background:
            job = start_capture_job(provider, feature, label=label, instance_id=instance, progress_id=progress_id or None)  # type: ignore[arg-type]
            return _ok({"job": job, "progress_id": job["progress_id"]}, status_code=202)
        res = create_snapshot(provider, feature, label=label, instance_id=instance, progress_id=progress_id or None)  # type: ignore[arg-type]
        return _ok({"snapshot": res})
    except Exception as e:
        _log_failure("snapshot create request", e)
        return _err("snapshot_create_failed")


@router.get("/capture-progress/{progress_id}")
def api_snapshots_capture_progress(progress_id: str) -> JSONResponse:
    try:
        progress = get_capture_progress(progress_id)
        if not progress:
            return _ok({"progress": {"ok": False, "done": False, "stage": "waiting", "message": "Waiting for capture to start.", "percent": 0}})
        return _ok({"progress": progress})
    except Exception as e:
        _log_failure("snapshot capture progress request", e)
        return _err("snapshot_progress_failed")


@router.post("/restore")
def api_snapshots_restore(body: dict[str, Any] = Body(...), request: Request = cast(Request, None)) -> JSONResponse:
    path = str(body.get("path") or "").strip()
    mode = str(body.get("mode") or "merge").strip().lower()
    instance = str(body.get("instance") or body.get("instance_id") or body.get("profile") or "").strip()
    progress_id = str(body.get("progress_id") or body.get("progressId") or "").strip()
    background = bool(body.get("background") or body.get("async") or body.get("async_job"))
    try:
        cfg = load_config() or {}
        snap = read_snapshot(path)
        provider = provider_display_key(snap.get("provider"))
        target_instance = normalize_instance_id(instance or snap.get("instance") or snap.get("instance_id") or snap.get("profile") or "default")
        if not _snapshot_allowed(cfg, request, snap) or not user_can_access_instance(cfg, request_user(request), provider, target_instance):
            return _scope_denied()
        if background:
            job = start_restore_job(path, mode=mode, instance_id=instance, progress_id=progress_id or None)  # type: ignore[arg-type]
            return _ok({"job": job, "progress_id": job["progress_id"]}, status_code=202)
        res = restore_snapshot(path, mode=mode, instance_id=instance)  # type: ignore[arg-type]
        return _ok({"result": res})
    except Exception as e:
        _log_failure("snapshot restore request", e)
        return _err("snapshot_restore_failed")


@router.post("/delete")
def api_snapshots_delete(body: dict[str, Any] = Body(...), request: Request = cast(Request, None)) -> JSONResponse:
    path = str(body.get("path") or "").strip()
    delete_children = bool(body.get("delete_children", True))
    try:
        cfg = load_config() or {}
        if not _snapshot_allowed(cfg, request, read_snapshot(path)):
            return _scope_denied()
        res = delete_snapshot(path, delete_children=delete_children)
        return _ok({"result": res})
    except Exception as e:
        _log_failure("snapshot delete request", e)
        return _err("snapshot_delete_failed")


@router.post("/clear")
def api_snapshots_clear(request: Request = cast(Request, None)) -> JSONResponse:
    try:
        if not _is_admin_request(request):
            return _scope_denied()
        result = delete_all_snapshots()
        if not result.get("ok"):
            return _err("snapshot_clear_failed")
        return _ok({"result": result, "summary": result.get("summary") or {}})
    except Exception as e:
        _log_failure("snapshot clear request", e)
        return _err("snapshot_clear_failed")


@router.post("/tools/clear")
def api_snapshots_tools_clear(body: dict[str, Any] = Body(...), request: Request = cast(Request, None)) -> JSONResponse:
    provider = str(body.get("provider") or "").strip()
    instance = str(body.get("instance") or body.get("instance_id") or body.get("profile") or "").strip()
    progress_id = str(body.get("progress_id") or body.get("progressId") or "").strip()
    background = bool(body.get("background") or body.get("async") or body.get("async_job"))
    feats = body.get("features") or []
    features: list[str] = []
    if isinstance(feats, list):
        for f in feats:
            s = str(f or "").strip().lower()
            if s:
                features.append(s)
    try:
        cfg = load_config() or {}
        if not user_can_access_instance(cfg, request_user(request), provider, instance):
            return _scope_denied()
        if background:
            job = start_provider_cleanup_job(provider, features, instance_id=instance, progress_id=progress_id or None)  # type: ignore[arg-type]
            return _ok({"job": job, "progress_id": job["progress_id"]}, status_code=202)
        res = clear_provider_features(provider, features, instance_id=instance)  # type: ignore[arg-type]
        return _ok({"result": res})
    except Exception as e:
        _log_failure("snapshot tools clear request", e)
        return _err("snapshot_tools_clear_failed")
