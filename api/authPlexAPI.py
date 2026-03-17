# api/authPlexAPI.py
# CrossWatch - Plex SSO authentication API endpoints
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any
import secrets

from fastapi import APIRouter, Body, Request
from fastapi.responses import HTMLResponse, JSONResponse

from cw_platform.config_base import load_config, save_config
from services import authPlex

from . import appAuthAPI as app_auth

try:
    from _logging import log as _real_log
except ImportError:
    _real_log = None

router = APIRouter(prefix="/api/app-auth/plex", tags=["app-auth"])
FLOW_COOKIE_NAME = "cw_plex_flow"


def _identity_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _log(msg: str, *, level: str = "INFO") -> None:
    try:
        if _real_log is not None:
            _real_log(msg, level=level, module="AUTH")
        else:
            print(f"[AUTH] {level}: {msg}")
    except Exception:
        pass


def _set_flow_cookie(resp: JSONResponse, nonce: str, request: Request) -> None:
    resp.set_cookie(
        FLOW_COOKIE_NAME,
        nonce,
        path="/api/app-auth/plex",
        httponly=True,
        samesite="lax",
        secure=app_auth._effective_scheme_is_https(request),
        max_age=10 * 60,
    )


def _del_flow_cookie(resp: JSONResponse, request: Request) -> None:
    resp.delete_cookie(
        FLOW_COOKIE_NAME,
        path="/api/app-auth/plex",
        httponly=True,
        samesite="lax",
        secure=app_auth._effective_scheme_is_https(request),
    )


def _flow_nonce_matches(request: Request, res: dict[str, Any]) -> bool:
    nonce = str(request.cookies.get(FLOW_COOKIE_NAME) or "").strip()
    want = str(res.get("flow_nonce_hash") or "").strip()
    if not nonce or not want:
        return False
    return app_auth._digest_eq(authPlex._sha256_hex(nonce), want)


def _callback_url(request: Request) -> str:
    origin = app_auth._expected_origin(request)
    if not origin:
        origin = f"{request.url.scheme}://{request.url.netloc}"
    return f"{origin.rstrip('/')}/api/app-auth/plex/callback"


def _unauthorized() -> JSONResponse:
    return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})


def _require_authenticated(request: Request, cfg: dict[str, Any]) -> str | None:
    token = request.cookies.get(app_auth.COOKIE_NAME)
    if app_auth.auth_required(cfg) and not app_auth.is_authenticated(cfg, token):
        return None
    return token


@router.get("/status")
def api_plex_status(request: Request) -> JSONResponse:
    cfg = load_config()
    token = request.cookies.get(app_auth.COOKIE_NAME)
    authed = (not app_auth.auth_required(cfg)) or app_auth.is_authenticated(cfg, token)
    st = authPlex.get_status(cfg)
    payload = {
        "enabled": bool(st["enabled"]),
        "linked": bool(st["linked"]),
        "login_available": authPlex.login_available(cfg),
        "linked_username": st["linked_username"] if authed else "",
        "linked_email": st["linked_email"] if authed else "",
        "linked_thumb": st["linked_thumb"] if authed else "",
        "linked_at": int(st["linked_at"] or 0) if authed else 0,
    }
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@router.post("/start")
def api_plex_start(request: Request, payload: dict[str, Any] | None = Body(None)) -> JSONResponse:
    cfg = load_config()
    if not authPlex.login_available(cfg):
        return JSONResponse({"ok": False, "error": "Plex sign-in is not linked yet"}, status_code=400, headers={"Cache-Control": "no-store"})

    remember_me = bool((payload or {}).get("remember_me"))
    try:
        flow_nonce = secrets.token_urlsafe(24)
        data = authPlex.start_flow(
            cfg,
            intent="login",
            callback_url=_callback_url(request),
            flow_nonce_hash=authPlex._sha256_hex(flow_nonce),
            remember_me=remember_me,
        )
        save_config(cfg)
        resp = JSONResponse(data, headers={"Cache-Control": "no-store"})
        _set_flow_cookie(resp, flow_nonce, request)
        return resp
    except Exception as exc:
        _log(f"Plex sign-in could not start: {exc}", level="ERROR")
        return JSONResponse({"ok": False, "error": "Plex sign-in could not start. Please try again."}, status_code=502, headers={"Cache-Control": "no-store"})


@router.post("/check")
def api_plex_check(request: Request, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    cfg = load_config()
    if not authPlex.login_available(cfg):
        return JSONResponse({"ok": False, "error": "Plex sign-in is not linked yet"}, status_code=400, headers={"Cache-Control": "no-store"})

    try:
        res = authPlex.check_flow(cfg, state=str(payload.get("state") or "").strip(), intent="login")
    except Exception as exc:
        _log(f"Plex sign-in failed: {exc}", level="ERROR")
        return JSONResponse({"ok": False, "error": "Plex sign-in failed. Please try again."}, status_code=502, headers={"Cache-Control": "no-store"})

    if not res.get("ok"):
        return JSONResponse({"ok": False, "error": str(res.get("error") or "Plex sign-in failed")}, status_code=int(res.get("status_code") or 400), headers={"Cache-Control": "no-store"})

    if res.get("pending"):
        return JSONResponse({"ok": True, "pending": True}, headers={"Cache-Control": "no-store"})

    if not _flow_nonce_matches(request, res):
        resp = JSONResponse({"ok": False, "error": "Plex sign-in expired. Start again."}, status_code=400, headers={"Cache-Control": "no-store"})
        _del_flow_cookie(resp, request)
        return resp

    identity = _identity_dict(res.get("identity"))
    if not authPlex.identity_matches(cfg, identity):
        resp = JSONResponse({"ok": False, "error": "This Plex account is not linked for CrossWatch sign-in"}, status_code=403, headers={"Cache-Control": "no-store"})
        _del_flow_cookie(resp, request)
        return resp

    token, exp = app_auth._issue_session(cfg, request)
    save_config(cfg)
    resp = JSONResponse(
        {
            "ok": True,
            "pending": False,
            "expires_at": exp,
            "username": str(identity.get("username") or ""),
        },
        headers={"Cache-Control": "no-store"},
    )
    _del_flow_cookie(resp, request)
    app_auth._set_cookie(resp, token, exp, request, persistent=bool(res.get("remember_me")))
    return resp


@router.post("/link/start")
def api_plex_link_start(request: Request) -> JSONResponse:
    cfg = load_config()
    token = _require_authenticated(request, cfg)
    if token is None:
        return _unauthorized()
    if token and not app_auth._origin_allowed(request):
        return app_auth._origin_blocked_response()

    try:
        flow_nonce = secrets.token_urlsafe(24)
        data = authPlex.start_flow(
            cfg,
            intent="link",
            callback_url=_callback_url(request),
            flow_nonce_hash=authPlex._sha256_hex(flow_nonce),
            remember_me=False,
        )
        save_config(cfg)
        resp = JSONResponse(data, headers={"Cache-Control": "no-store"})
        _set_flow_cookie(resp, flow_nonce, request)
        return resp
    except Exception as exc:
        _log(f"Plex link could not start: {exc}", level="ERROR")
        return JSONResponse({"ok": False, "error": "Plex link could not start. Please try again."}, status_code=502, headers={"Cache-Control": "no-store"})


@router.post("/link/check")
def api_plex_link_check(request: Request, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    cfg = load_config()
    token = _require_authenticated(request, cfg)
    if token is None:
        return _unauthorized()
    if token and not app_auth._origin_allowed(request):
        return app_auth._origin_blocked_response()

    try:
        res = authPlex.check_flow(cfg, state=str(payload.get("state") or "").strip(), intent="link")
    except Exception as exc:
        _log(f"Plex link failed: {exc}", level="ERROR")
        return JSONResponse({"ok": False, "error": "Plex link failed. Please try again."}, status_code=502, headers={"Cache-Control": "no-store"})

    if not res.get("ok"):
        return JSONResponse({"ok": False, "error": str(res.get("error") or "Plex link failed")}, status_code=int(res.get("status_code") or 400), headers={"Cache-Control": "no-store"})

    if res.get("pending"):
        return JSONResponse({"ok": True, "pending": True}, headers={"Cache-Control": "no-store"})

    if not _flow_nonce_matches(request, res):
        resp = JSONResponse({"ok": False, "error": "Plex sign-in expired. Start again."}, status_code=400, headers={"Cache-Control": "no-store"})
        _del_flow_cookie(resp, request)
        return resp

    identity = _identity_dict(res.get("identity"))
    st = authPlex.link_identity(cfg, identity)
    save_config(cfg)
    resp = JSONResponse({"ok": True, "pending": False, **st}, headers={"Cache-Control": "no-store"})
    _del_flow_cookie(resp, request)
    return resp


@router.post("/unlink")
def api_plex_unlink(request: Request) -> JSONResponse:
    cfg = load_config()
    token = _require_authenticated(request, cfg)
    if token is None:
        return _unauthorized()
    if token and not app_auth._origin_allowed(request):
        return app_auth._origin_blocked_response()

    st = authPlex.unlink_identity(cfg)
    save_config(cfg)
    return JSONResponse({"ok": True, **st}, headers={"Cache-Control": "no-store"})


@router.get("/callback")
def api_plex_callback() -> HTMLResponse:
    return HTMLResponse(
        """<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Plex sign-in complete</title></head><body style="font-family:Segoe UI,Arial,sans-serif;background:#08111d;color:#eef5ff;display:grid;place-items:center;min-height:100vh;margin:0"><div style="text-align:center"><div style="font-size:28px;margin-bottom:8px">Done</div><div style="opacity:.8">You can close this window.</div></div><script>try{window.close()}catch(e){}</script></body></html>""",
        headers={"Cache-Control": "no-store"},
    )


def register_auth_plex(app) -> None:
    app.include_router(router)
