# /api/playbackProgressAPI.py
# CrossWatch - Local Playback Progress API
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from typing import Any, cast

from fastapi import APIRouter, Body, Query, Request
from fastapi.responses import JSONResponse

from services.playback_progress import get_service
from services.playback_progress.models import utc_now_iso
from cw_platform.config_base import load_config
from cw_platform.provider_instances import instances_for_user_profile

router = APIRouter(prefix="/api/playback_progress", tags=["playback_progress"])


def _playback_user_filter(request: Request | None, requested_profile: str = "") -> dict[str, list[str]]:
    try:
        from api.appAuthAPI import COOKIE_NAME, effective_user_profile_id
        cfg = load_config() or {}
        token = request.cookies.get(COOKIE_NAME) if request is not None else None
        profile = effective_user_profile_id(cfg, token, requested_profile)
        if not str(profile or "").strip():
            return {}
        return instances_for_user_profile(cfg, profile) or {"__NONE__": ["__NONE__"]}
    except Exception:
        return {"__NONE__": ["__NONE__"]}


@router.get("/providers")
def api_playback_progress_providers(request: Request = cast(Request, None), user_profile: str = Query("")) -> dict[str, Any]:
    service = get_service()
    return {
        "providers": [cap.to_dict() for cap in service.capabilities(user_filter=_playback_user_filter(request, user_profile))],
        "refreshed_at": utc_now_iso(),
    }


@router.get("/settings")
def api_playback_progress_settings(request: Request = cast(Request, None), user_profile: str = Query("")) -> dict[str, Any]:
    return get_service().settings(user_filter=_playback_user_filter(request, user_profile))


@router.post("/settings")
def api_playback_progress_save_settings(payload: dict[str, Any] = Body(...)) -> JSONResponse:
    result = get_service().save_settings(payload)
    return JSONResponse(result, status_code=200 if result.get("ok") else 400)


@router.get("/items")
def api_playback_progress_items(
    request: Request = cast(Request, None),
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
    user_profile: str = Query(""),
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
        user_filter=_playback_user_filter(request, user_profile),
    )


def _action_status(result: dict[str, Any]) -> int:
    if result.get("ok"):
        return 200
    return 403 if result.get("error_code") == "profile_scope_denied" else 400


@router.post("/actions/remove")
def api_playback_progress_remove(request: Request = cast(Request, None), payload: dict[str, Any] = Body(...)) -> JSONResponse:
    result = get_service().remove(payload, user_filter=_playback_user_filter(request))
    return JSONResponse(result, status_code=_action_status(result))


@router.post("/actions/mark_watched")
def api_playback_progress_mark_watched(request: Request = cast(Request, None), payload: dict[str, Any] = Body(...)) -> JSONResponse:
    result = get_service().mark_watched(payload, user_filter=_playback_user_filter(request))
    return JSONResponse(result, status_code=_action_status(result))


@router.post("/actions/update_progress")
def api_playback_progress_update_progress(request: Request = cast(Request, None), payload: dict[str, Any] = Body(...)) -> JSONResponse:
    result = get_service().update_progress(payload, user_filter=_playback_user_filter(request))
    return JSONResponse(result, status_code=_action_status(result))


@router.post("/actions/bulk")
def api_playback_progress_bulk(request: Request = cast(Request, None), payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    return get_service().bulk(payload, user_filter=_playback_user_filter(request))
