# api/editorAPI.py
# CrossWatch - Tracker editor API for history / ratings / watchlist
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import io
import json
import os
from pathlib import Path

from fastapi import APIRouter, Body, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse

from services.editor import (
    Kind,
    export_tracker_zip,
    import_tracker_upload,
    list_snapshots,
    load_state,
    save_state,
)

router = APIRouter(prefix="/api/editor", tags=["editor"])

_STATE_PATH = Path("/config/state.json")

def _atomic_write_json(path: Path, payload: Any) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), "utf-8")
        os.replace(tmp, path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to write {path}: {e}")

def _load_current_state() -> dict[str, Any]:
    if not _STATE_PATH.exists():
        raise HTTPException(status_code=404, detail=f"Missing state file: {_STATE_PATH}")
    try:
        raw = json.loads(_STATE_PATH.read_text("utf-8"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read state file: {e}")
    return raw if isinstance(raw, dict) else {}

def _state_providers(raw: dict[str, Any]) -> list[str]:
    providers = raw.get("providers") or {}
    if not isinstance(providers, dict):
        return []
    return sorted([str(k) for k in providers.keys() if str(k).strip()])

def _load_state_items(kind: Kind, provider: str) -> dict[str, Any]:
    raw = _load_current_state()
    providers = raw.get("providers") or {}
    if not isinstance(providers, dict):
        return {}
    node = providers.get(provider) or {}
    if not isinstance(node, dict):
        return {}
    feature = node.get(kind) or {}
    if not isinstance(feature, dict):
        return {}
    baseline = feature.get("baseline") or {}
    if not isinstance(baseline, dict):
        return {}
    items = baseline.get("items") or {}
    return items if isinstance(items, dict) else {}

def _save_state_items(kind: Kind, provider: str, items: dict[str, Any]) -> None:
    raw = _load_current_state()
    providers = raw.get("providers")
    if not isinstance(providers, dict):
        providers = {}
        raw["providers"] = providers
    node = providers.get(provider)
    if not isinstance(node, dict):
        node = {}
        providers[provider] = node
    feature = node.get(kind)
    if not isinstance(feature, dict):
        feature = {"baseline": {"items": {}}, "checkpoint": None}
        node[kind] = feature
    baseline = feature.get("baseline")
    if not isinstance(baseline, dict):
        baseline = {"items": {}}
        feature["baseline"] = baseline
    baseline["items"] = dict(items or {})
    _atomic_write_json(_STATE_PATH, raw)

def _normalize_kind(val: str | None) -> Kind:
    k = (val or "watchlist").strip().lower()
    if k not in ("watchlist", "history", "ratings"):
        raise HTTPException(status_code=400, detail=f"Unsupported kind: {k}")
    return k  # type: ignore[return-value]

@router.get("/state/providers")
def api_editor_state_providers() -> dict[str, Any]:
    raw = _load_current_state()
    return {"providers": _state_providers(raw)}

@router.get("")
def api_editor_get_state(
    kind: str = Query("watchlist"),
    snapshot: str | None = Query(None),
    source: str = Query("tracker"),
    provider: str | None = Query(None),
) -> dict[str, Any]:
    k = _normalize_kind(kind)
    src = (source or "tracker").strip().lower()
    if src in ("tracker", "cw", "crosswatch"):
        state = load_state(k, snapshot=snapshot)
        items = state.get("items") or {}
        if not isinstance(items, dict):
            items = {}
        return {
            "kind": k,
            "source": "tracker",
            "snapshot": snapshot,
            "provider": None,
            "ts": state.get("ts"),
            "count": len(items),
            "items": items,
        }
    if src in ("state", "current"):
        raw = _load_current_state()
        providers = _state_providers(raw)
        chosen = (provider or "").strip() or (providers[0] if providers else "")
        if not chosen:
            return {
                "kind": k,
                "source": "state",
                "snapshot": None,
                "provider": None,
                "ts": None,
                "count": 0,
                "items": {},
            }
        items = _load_state_items(k, chosen)
        ts = None
        try:
            ts = int(_STATE_PATH.stat().st_mtime)
        except Exception:
            ts = None
        return {
            "kind": k,
            "source": "state",
            "snapshot": None,
            "provider": chosen,
            "ts": ts,
            "count": len(items),
            "items": items,
        }
    raise HTTPException(status_code=400, detail=f"Unsupported source: {src}")
@router.get("/snapshots")
def api_editor_list_snapshots(
    kind: str = Query("watchlist"),
) -> dict[str, Any]:
    k = _normalize_kind(kind)
    snaps = list_snapshots(k)
    return {"kind": k, "snapshots": snaps}

def _normalize_items(items: Any) -> dict[str, Any]:
    if isinstance(items, dict):
        return {str(k): v for k, v in items.items()}
    if isinstance(items, list):
        out: dict[str, Any] = {}
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
def api_editor_save_state(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    kind = _normalize_kind(str(payload.get("kind") or "watchlist"))
    src = str(payload.get("source") or "tracker").strip().lower()
    items_raw = payload.get("items")
    items = _normalize_items(items_raw)
    if src in ("tracker", "cw", "crosswatch"):
        state = save_state(kind, items)
        return {
            "ok": True,
            "kind": kind,
            "source": "tracker",
            "provider": None,
            "count": len(items),
            "ts": state.get("ts"),
        }
    if src in ("state", "current"):
        provider = str(payload.get("provider") or "").strip()
        if not provider:
            raise HTTPException(status_code=400, detail="Missing provider for source=state")
        _save_state_items(kind, provider, items)
        ts = None
        try:
            ts = int(_STATE_PATH.stat().st_mtime)
        except Exception:
            ts = None
        return {
            "ok": True,
            "kind": kind,
            "source": "state",
            "provider": provider,
            "count": len(items),
            "ts": ts,
        }
    raise HTTPException(status_code=400, detail=f"Unsupported source: {src}")
@router.get("/export")
def api_editor_export() -> StreamingResponse:
    data = export_tracker_zip()
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=crosswatch-tracker.zip"},
    )

@router.post("/import")
async def api_editor_import(file: UploadFile = File(...)) -> dict[str, Any]:
    payload = await file.read()
    filename = file.filename or "upload.json"
    try:
        stats = import_tracker_upload(payload, filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ok": True, **stats}
