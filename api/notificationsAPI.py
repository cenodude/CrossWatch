# /api/notificationsAPI.py
# CrossWatch - Notification list, read state, review waits and push subscriptions for each user
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from fastapi import APIRouter, Body, Query, Request
from fastapi.responses import JSONResponse

from api.appAuthAPI import auth_required
from cw_platform import notification_store as store
from cw_platform.access_policy import request_user
from cw_platform.config_base import load_config
from services import notification_center as center
from services import notification_push as push

router = APIRouter(prefix="/api/profile/notifications", tags=["notifications"])

_NO_STORE = {"Cache-Control": "no-store"}
_PERIOD = {"month": re.compile(r"^\d{4}-(0[1-9]|1[0-2])$"), "wrapped": re.compile(r"^\d{4}$")}


def _user_id(request: Request) -> str:
    user = getattr(getattr(request, "state", None), "cw_user", None)
    if isinstance(user, Mapping):
        if user.get("monitor_view"):
            return ""
        return str(user.get("id") or user.get("username") or "").strip()
    try:
        return "" if auth_required(load_config() or {}) else "local"
    except Exception:
        return ""


def _text(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _reply(body: dict[str, Any], status: int = 200) -> JSONResponse:
    return JSONResponse(body, status_code=status, headers=_NO_STORE)


def _denied() -> JSONResponse:
    return _reply({"ok": False, "error": "Unauthorized"}, 403)


def _unavailable() -> JSONResponse:
    return _reply({"ok": False, "error": "Notification storage unavailable"}, 503)


def _bad(message: str) -> JSONResponse:
    return _reply({"ok": False, "error": message}, 400)


def _state(uid: str, profile: str) -> dict[str, Any]:
    marks = store.marks(uid, ["account", profile])
    return {"account": marks.get("account", {"dismissed": [], "seen": []}), "profile": marks.get(profile, {"dismissed": [], "seen": []})}


@router.get("")
def api_notifications(request: Request, profile: str = Query("all", max_length=128), view_profile: str = Query("", max_length=64)) -> JSONResponse:
    uid = _user_id(request)
    scope = store.clean_scope(profile)
    if not uid:
        return _denied()
    if not scope or scope == "account":
        return _bad("Invalid profile")
    ctx = center.context(request, load_config() or {}, uid, scope, view_profile.strip())
    result = center.collect(ctx)
    try:
        state = _state(uid, scope)
    except Exception:
        state = None
    return _reply({"ok": True, **result, "state": state})


def _marks(request: Request, payload: Mapping[str, Any] | None, kind: str) -> JSONResponse:
    uid = _user_id(request)
    if not uid:
        return _denied()
    scope = store.clean_scope((payload or {}).get("scope"))
    if not scope:
        return _bad("Invalid scope")
    try:
        keys = store.add_marks(uid, scope, kind, (payload or {}).get("keys"))
    except Exception:
        return _unavailable()
    return _reply({"ok": True, "scope": scope, "keys": keys})


@router.post("/dismissed")
def api_notifications_dismiss(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    return _marks(request, payload, "dismissed")


@router.post("/seen")
def api_notifications_seen(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    return _marks(request, payload, "seen")


@router.post("/reviews")
def api_notifications_reviews(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    uid = _user_id(request)
    if not uid:
        return _denied()
    data = _mapping(payload)
    kind, action = _text(data.get("kind")), _text(data.get("action"))
    instance, period = _text(data.get("instance")).strip(), _text(data.get("period"))
    pattern = _PERIOD.get(kind)
    if pattern is None or action not in ("wait", "viewed") or not instance or len(instance) > 64 or not pattern.match(period):
        return _bad("Invalid review")
    try:
        if action == "viewed":
            store.review_viewed(uid, kind, instance, period)
        else:
            scope = store.clean_scope(data.get("profile"))
            if not scope or scope == "account":
                return _bad("Invalid profile")
            store.wait_for_review(uid, scope, kind, instance, period, _text(data.get("label")).strip() or instance)
    except Exception:
        return _unavailable()
    return _reply({"ok": True})


@router.get("/push")
def api_push_status(request: Request) -> JSONResponse:
    uid = _user_id(request)
    if not uid:
        return _denied()
    try:
        key = push.public_key()
        endpoints = [row["endpoint"] for row in store.subscriptions(uid)]
    except Exception:
        return _unavailable()
    return _reply({"ok": True, "public_key": key, "endpoints": endpoints})


@router.post("/push")
def api_push_subscribe(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    uid = _user_id(request)
    if not uid:
        return _denied()
    data = _mapping(payload)
    subscription = _mapping(data.get("subscription"))
    keys = _mapping(subscription.get("keys"))
    endpoint = subscription.get("endpoint")
    if not push.allowed_endpoint(endpoint):
        return _bad("This browser's push service is not supported")
    if not push.valid_keys(keys.get("p256dh"), keys.get("auth")):
        return _bad("Invalid push subscription")
    scope = store.clean_scope(data.get("profile"))
    view_profile = _text(data.get("view_profile"))
    label = _text(data.get("label"))
    if not scope or scope == "account" or len(view_profile) > 64:
        return _bad("Invalid profile")
    ctx = center.context(request, load_config() or {}, uid, scope, view_profile.strip())
    items = center.collect(ctx)["items"]
    try:
        viewer = request_user(request)
        view_as = str(viewer.get("profile_id") or "") if isinstance(viewer, Mapping) and viewer.get("view_as") else ""
        push.baseline(uid, str(endpoint), items)
        store.save_subscription(uid, scope, view_profile.strip(), str(endpoint), str(keys["p256dh"]), str(keys["auth"]), label.strip(), view_as=view_as)
    except Exception:
        return _unavailable()
    return _reply({"ok": True})


@router.delete("/push")
def api_push_unsubscribe(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    uid = _user_id(request)
    if not uid:
        return _denied()
    endpoint = (payload or {}).get("endpoint")
    if not isinstance(endpoint, str):
        return _bad("Invalid push subscription")
    try:
        removed = store.remove_subscription(endpoint, uid)
    except Exception:
        return _unavailable()
    return _reply({"ok": True, "removed": removed})


@router.post("/push/test")
def api_push_test(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    uid = _user_id(request)
    if not uid:
        return _denied()
    endpoint = (payload or {}).get("endpoint")
    row = next((row for row in store.subscriptions(uid) if row["endpoint"] == endpoint), None)
    if row is None:
        return _reply({"ok": False, "error": "Push is not turned on for this device"}, 404)
    message = {"title": "CrossWatch", "body": "Push notifications work on this device.", "url": "/", "tag": "cw-push-test"}
    if not push.deliver(row, message):
        return _reply({"ok": False, "error": "The push service did not accept the test notification"}, 502)
    return _reply({"ok": True})
