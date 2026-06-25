# /api/dashboardAPI.py
# CrossWatch - Main dashboard widget API
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from services.dashboard_widgets import dashboard_widgets_payload

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/widgets")
def dashboard_widgets(
    history_limit: int = Query(8, ge=1, le=24),
    ratings_limit: int = Query(12, ge=1, le=24),
    scrobble_limit: int = Query(8, ge=1, le=24),
) -> JSONResponse:
    try:
        from .syncAPI import _load_state

        state = _load_state() or {}
        payload = dashboard_widgets_payload(
            state,
            history_limit=history_limit,
            ratings_limit=ratings_limit,
            scrobble_limit=scrobble_limit,
        )
        return JSONResponse(payload, headers={"Cache-Control": "no-store"})
    except Exception:
        return JSONResponse(
            {"ok": False, "error": "dashboard_widgets_failed"},
            status_code=200,
            headers={"Cache-Control": "no-store"},
        )
