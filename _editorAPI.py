# _editorAPI.py
# CrossWatch - Tracker editor API for history / ratings / watchlist
# Copyright (c) 2025 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Dict
import io

from fastapi import APIRouter, Body, Query, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse

from _editor import (
    load_state,
    save_state,
    list_snapshots,
    Kind,
    export_tracker_zip,
    import_tracker_upload,
)

router = APIRouter(prefix="/api/editor", tags=["editor"])

def _normalize_kind(val: str | None) -> Kind:
    k = (val or "watchlist").strip().lower()
    if k not in ("watchlist", "history", "ratings"):
        raise HTTPException(status_code=400, detail=f"Unsupported kind: {k}")
    return k  # type: ignore[return-value]

@router.get("")
def api_editor_get_state(
    kind: str = Query("watchlist"),
    snapshot: str | None = Query(None),
) -> Dict[str, Any]:
    k = _normalize_kind(kind)
    state = load_state(k, snapshot=snapshot)
    items = state.get("items") or {}
    if not isinstance(items, dict):
        items = {}
    return {
        "kind": k,
        "snapshot": snapshot,
        "ts": state.get("ts"),
        "count": len(items),
        "items": items,
    }

@router.get("/snapshots")
def api_editor_list_snapshots(
    kind: str = Query("watchlist"),
) -> Dict[str, Any]:
    k = _normalize_kind(kind)
    snaps = list_snapshots(k)
    return {"kind": k, "snapshots": snaps}

def _normalize_items(items: Any) -> Dict[str, Any]:
    if isinstance(items, dict):
        return {str(k): v for k, v in items.items()}
    if isinstance(items, list):
        out: Dict[str, Any] = {}
        for row in items:
            if not isinstance(row, dict):
                continue
            key = str(row.get("key") or "").strip()
            if not key:
                continue
            payload = {k: v for k, v in row.items() if k != "key"}
            out[key] = payload
        return out
    return {}

@router.post("")
def api_editor_save_state(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    kind = _normalize_kind(str(payload.get("kind") or "watchlist"))
    items_raw = payload.get("items")
    items = _normalize_items(items_raw)
    state = save_state(kind, items)
    return {
        "ok": True,
        "kind": kind,
        "count": len(items),
        "ts": state.get("ts"),
    }

@router.get("/export")
def api_editor_export() -> StreamingResponse:
    data = export_tracker_zip()
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=crosswatch-tracker.zip"},
    )

@router.post("/import")
async def api_editor_import(file: UploadFile = File(...)) -> Dict[str, Any]:
    payload = await file.read()
    filename = file.filename or "upload.json"
    try:
        stats = import_tracker_upload(payload, filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    # stats contains: files, overwritten, target, kind, name, states, snapshots, mode
    return {"ok": True, **stats}
