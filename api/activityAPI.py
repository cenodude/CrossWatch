# /api/activityAPI.py
# CrossWatch - Local Recent Activity API
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from services.activity import clear_events, list_events

router = APIRouter(prefix="/api/activity", tags=["activity"])


@router.get("/recent")
def activity_recent(
    limit: int = Query(10, ge=1, le=50),
    since: int | None = Query(None, ge=0),
) -> JSONResponse:
    return JSONResponse(list_events(limit=limit, offset=0, since=since), headers={"Cache-Control": "no-store"})


@router.get("/history")
def activity_history(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    media_type: str = Query("all"),
    status: str = Query("all"),
    q: str = Query(""),
    since: int | None = Query(None, ge=0),
) -> JSONResponse:
    payload: dict[str, Any] = list_events(
        limit=limit,
        offset=offset,
        media_type=media_type,
        status=status,
        query=q,
        since=since,
    )
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@router.delete("/history")
def activity_clear() -> JSONResponse:
    return JSONResponse(clear_events(), headers={"Cache-Control": "no-store"})
