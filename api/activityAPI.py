# /api/activityAPI.py
# CrossWatch - Local Recent Activity API
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from typing import Any, cast

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from cw_platform.access_policy import request_user
from cw_platform.config_base import load_config
from cw_platform.provider_instances import instances_for_user_profile, normalize_instance_id, provider_display_key
from services.activity import clear_events, list_events

router = APIRouter(prefix="/api/activity", tags=["activity"])


def _matches_user_profile(item: dict[str, Any], user_filter: dict[str, list[str]]) -> bool:
    if not user_filter:
        return True
    wanted = {
        provider_display_key(provider): {normalize_instance_id(inst) for inst in (instances or [])}
        for provider, instances in user_filter.items()
    }

    def hit(provider: Any, instance: Any) -> bool:
        prov = provider_display_key(provider)
        return bool(prov) and normalize_instance_id(instance) in wanted.get(prov, set())

    if hit(item.get("source"), item.get("source_instance")):
        return True
    if hit(item.get("provider"), item.get("provider_instance") or item.get("instance")):
        return True
    targets = item.get("targets")
    if isinstance(targets, list):
        for target in targets:
            if isinstance(target, dict) and hit(target.get("target") or target.get("provider"), target.get("target_instance") or target.get("instance")):
                return True
    return hit(item.get("target"), item.get("target_instance"))


def _apply_user_profile(payload: dict[str, Any], user_profile: str) -> dict[str, Any]:
    profile = str(user_profile or "").strip()
    if not profile:
        return payload
    try:
        user_filter = instances_for_user_profile(load_config() or {}, profile)
    except Exception:
        user_filter = {}
    if not user_filter:
        return {**payload, "items": [], "total": 0, "user_profile": profile}
    items = [item for item in (payload.get("items") or []) if isinstance(item, dict) and _matches_user_profile(item, user_filter)]
    return {**payload, "items": items, "total": len(items), "user_profile": profile}


def _effective_profile_from_request(request: Request | None, requested: str) -> str:
    try:
        from api.appAuthAPI import COOKIE_NAME, effective_user_profile_id

        cfg = load_config() or {}
        token = request.cookies.get(COOKIE_NAME) if request is not None else None
        return effective_user_profile_id(cfg, token, requested)
    except Exception:
        return "__none__"


@router.get("/recent")
def activity_recent(
    request: Request = cast(Request, None),
    limit: int = Query(10, ge=1, le=50),
    since: int | None = Query(None, ge=0),
    user_profile: str = Query(""),
) -> JSONResponse:
    profile = _effective_profile_from_request(request, user_profile)
    raw_limit = 500 if str(profile or "").strip() else limit
    payload = _apply_user_profile(list_events(limit=raw_limit, offset=0, since=since), profile)
    if str(profile or "").strip():
        payload["items"] = list(payload.get("items") or [])[:limit]
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@router.get("/history")
def activity_history(
    request: Request = cast(Request, None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    media_type: str = Query("all"),
    status: str = Query("all"),
    q: str = Query(""),
    since: int | None = Query(None, ge=0),
    user_profile: str = Query(""),
) -> JSONResponse:
    profile = _effective_profile_from_request(request, user_profile)
    scoped = bool(str(profile or "").strip())
    payload: dict[str, Any] = list_events(
        limit=500 if scoped else limit,
        offset=0 if scoped else offset,
        media_type=media_type,
        status=status,
        query=q,
        since=since,
    )
    payload = _apply_user_profile(payload, profile)
    if scoped:
        items = list(payload.get("items") or [])
        payload["items"] = items[offset:offset + limit]
        payload["has_more"] = offset + limit < len(items)
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@router.delete("/history")
def activity_clear(request: Request = cast(Request, None)) -> JSONResponse:
    user = request_user(request)
    if user and not bool(user.get("is_admin")):
        return JSONResponse({"ok": False, "error": "profile_scope_denied"}, status_code=403, headers={"Cache-Control": "no-store"})
    return JSONResponse(clear_events(), headers={"Cache-Control": "no-store"})
