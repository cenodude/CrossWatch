# /api/snapshotsAPI.py
# CrossWatch - Snapshots API (watchlist/ratings/history)
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Body, Query
from fastapi.responses import JSONResponse

from services.snapshots import (
    clear_provider_features,
    create_snapshot,
    list_snapshots,
    read_snapshot,
    restore_snapshot,
    snapshot_manifest,
    delete_snapshot,
    diff_snapshots,
)

router = APIRouter(prefix="/api/snapshots", tags=["snapshots"])

RestoreMode = Literal["merge", "clear_restore"]
Feature = Literal["watchlist", "ratings", "history", "all"]


def _ok(payload: dict[str, Any], *, status_code: int = 200) -> JSONResponse:
    payload.setdefault("ok", True)
    return JSONResponse(payload, status_code=status_code)


def _err(msg: str, *, status_code: int = 400, extra: dict[str, Any] | None = None) -> JSONResponse:
    payload: dict[str, Any] = {"ok": False, "error": msg}
    if extra:
        payload.update(extra)
    return JSONResponse(payload, status_code=status_code)


@router.get("/manifest")
def api_snapshots_manifest() -> JSONResponse:
    try:
        return _ok({"providers": snapshot_manifest()})
    except Exception as e:
        return _err(str(e))


@router.get("/list")
def api_snapshots_list() -> JSONResponse:
    try:
        return _ok({"snapshots": list_snapshots()})
    except Exception as e:
        return _err(str(e))


@router.get("/read")
def api_snapshots_read(path: str = Query(..., description="Relative path under /config/snapshots")) -> JSONResponse:
    try:
        snap = read_snapshot(path)
        return _ok({"snapshot": snap})
    except Exception as e:
        return _err(str(e))



@router.get("/diff")
def api_snapshots_diff(
    a: str = Query(..., description="Snapshot A path (relative under /config/snapshots)"),
    b: str = Query(..., description="Snapshot B path (relative under /config/snapshots)"),
    limit: int = Query(200, ge=1, le=2000),
    max_changes: int = Query(25, ge=1, le=200),
) -> JSONResponse:
    try:
        res = diff_snapshots(a, b, limit=limit, max_changes=max_changes)
        return _ok({"diff": res})
    except Exception as e:
        return _err(str(e))

@router.post("/create")
def api_snapshots_create(body: dict[str, Any] = Body(...)) -> JSONResponse:
    provider = str(body.get("provider") or "").strip()
    instance = str(body.get("instance") or body.get("instance_id") or body.get("profile") or "").strip()
    feature = str(body.get("feature") or "").strip().lower()
    label = str(body.get("label") or "").strip()
    try:
        res = create_snapshot(provider, feature, label=label, instance_id=instance)  # type: ignore[arg-type]
        return _ok({"snapshot": res})
    except Exception as e:
        return _err(str(e))


@router.post("/restore")
def api_snapshots_restore(body: dict[str, Any] = Body(...)) -> JSONResponse:
    path = str(body.get("path") or "").strip()
    mode = str(body.get("mode") or "merge").strip().lower()
    instance = str(body.get("instance") or body.get("instance_id") or body.get("profile") or "").strip()
    try:
        res = restore_snapshot(path, mode=mode, instance_id=instance)  # type: ignore[arg-type]
        return _ok({"result": res})
    except Exception as e:
        return _err(str(e))


@router.post("/delete")
def api_snapshots_delete(body: dict[str, Any] = Body(...)) -> JSONResponse:
    path = str(body.get("path") or "").strip()
    delete_children = bool(body.get("delete_children", True))
    try:
        res = delete_snapshot(path, delete_children=delete_children)
        return _ok({"result": res})
    except Exception as e:
        return _err(str(e))


@router.post("/tools/clear")
def api_snapshots_tools_clear(body: dict[str, Any] = Body(...)) -> JSONResponse:
    provider = str(body.get("provider") or "").strip()
    instance = str(body.get("instance") or body.get("instance_id") or body.get("profile") or "").strip()
    feats = body.get("features") or []
    features: list[str] = []
    if isinstance(feats, list):
        for f in feats:
            s = str(f or "").strip().lower()
            if s:
                features.append(s)
    try:
        res = clear_provider_features(provider, features, instance_id=instance)  # type: ignore[arg-type]
        return _ok({"result": res})
    except Exception as e:
        return _err(str(e))
