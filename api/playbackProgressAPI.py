# /api/playbackProgressAPI.py
# CrossWatch - Local Playback Progress API
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Query
from fastapi.responses import JSONResponse

from services.playback_progress import get_service
from services.playback_progress.models import utc_now_iso

router = APIRouter(prefix="/api/playback_progress", tags=["playback_progress"])


@router.get("/providers")
def api_playback_progress_providers() -> dict[str, Any]:
    service = get_service()
    return {
        "providers": [cap.to_dict() for cap in service.capabilities()],
        "refreshed_at": utc_now_iso(),
    }


@router.get("/settings")
def api_playback_progress_settings() -> dict[str, Any]:
    return get_service().settings()


@router.post("/settings")
def api_playback_progress_save_settings(payload: dict[str, Any] = Body(...)) -> JSONResponse:
    result = get_service().save_settings(payload)
    return JSONResponse(result, status_code=200 if result.get("ok") else 400)


@router.get("/items")
def api_playback_progress_items(
    provider: str | None = Query(None),
    instance_id: str | None = Query(None),
    media_type: str | None = Query(None),
    progress_min: float | None = Query(None, ge=0, le=100),
    progress_max: float | None = Query(None, ge=0, le=100),
    age: str | None = Query(None),
    rating_min: float | None = Query(None, ge=0, le=10),
    search: str | None = Query(None),
    sort: str = Query("last_updated"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=250),
    force_refresh: bool = Query(False),
) -> dict[str, Any]:
    service = get_service()
    return service.items(
        provider=provider,
        instance_id=instance_id,
        media_type=media_type,
        progress_min=progress_min,
        progress_max=progress_max,
        age=age,
        rating_min=rating_min,
        search=search,
        sort=sort,
        page=page,
        page_size=page_size,
        force_refresh=force_refresh,
    )


@router.post("/actions/remove")
def api_playback_progress_remove(payload: dict[str, Any] = Body(...)) -> JSONResponse:
    result = get_service().remove(payload)
    return JSONResponse(result, status_code=200 if result.get("ok") else 400)


@router.post("/actions/mark_watched")
def api_playback_progress_mark_watched(payload: dict[str, Any] = Body(...)) -> JSONResponse:
    result = get_service().mark_watched(payload)
    return JSONResponse(result, status_code=200 if result.get("ok") else 400)


@router.post("/actions/update_progress")
def api_playback_progress_update_progress(payload: dict[str, Any] = Body(...)) -> JSONResponse:
    result = get_service().update_progress(payload)
    return JSONResponse(result, status_code=200 if result.get("ok") else 400)


@router.post("/actions/bulk")
def api_playback_progress_bulk(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    return get_service().bulk(payload)
