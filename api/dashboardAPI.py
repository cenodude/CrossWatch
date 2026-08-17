# /api/dashboardAPI.py
# CrossWatch - Main dashboard widget API
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from typing import cast

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from services.dashboard_widgets import dashboard_widgets_payload

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/widgets")
def dashboard_widgets(
    request: Request = cast(Request, None),
    history_limit: int = Query(8, ge=1, le=24),
    ratings_limit: int = Query(12, ge=1, le=24),
    scrobble_limit: int = Query(8, ge=1, le=24),
    progress_limit: int = Query(8, ge=1, le=24),
    playlists_limit: int = Query(8, ge=1, le=24),
    include: str = Query("history,ratings,scrobble,progress,playlists"),
    user_profile: str = Query("", alias="user_profile"),
) -> JSONResponse:
    try:
        from cw_platform.config_base import CONFIG, load_config
        from cw_platform.orchestrator._state_store import StateStore
        from api.appAuthAPI import COOKIE_NAME, effective_user_profile_id
        from cw_platform.provider_instances import instances_for_user_profile

        requested = {part.strip() for part in include.split(",") if part.strip()}
        state_features = requested & {"history", "ratings", "progress"}
        state = StateStore(CONFIG).load_state_features(state_features) if state_features else {}
        cfg = load_config() or {}
        token = request.cookies.get(COOKIE_NAME) if request is not None else None
        profile = effective_user_profile_id(cfg, token, user_profile)
        scoped = bool(str(profile or "").strip())
        user_filter = instances_for_user_profile(cfg, profile) if scoped else {}
        if scoped and not user_filter:
            user_filter = {"__NONE__": ["__NONE__"]}
        payload = dashboard_widgets_payload(
            state,
            history_limit=history_limit,
            ratings_limit=ratings_limit,
            scrobble_limit=scrobble_limit,
            progress_limit=progress_limit,
            playlists_limit=playlists_limit,
            include=requested,
            user_filter=user_filter,
        )
        if scoped:
            payload["user_profile"] = str(profile or "").strip()
        return JSONResponse(payload, headers={"Cache-Control": "no-store"})
    except Exception:
        return JSONResponse(
            {"ok": False, "error": "dashboard_widgets_failed"},
            status_code=200,
            headers={"Cache-Control": "no-store"},
        )
