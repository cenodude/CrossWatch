# api/authPlexAPI.py
# CrossWatch - Plex SSO authentication API endpoints
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any
import secrets

from fastapi import APIRouter, Body, Request
from fastapi.responses import HTMLResponse, JSONResponse

from cw_platform.config_base import load_config
from services import authPlex

from . import appAuthAPI as app_auth

try:
    from _logging import log as _real_log
except ImportError:
    _real_log = None

router = APIRouter(prefix="/api/app-auth/plex", tags=["app-auth"])
FLOW_COOKIE_NAME = "cw_plex_flow"
PENDING_2FA_TTL_SEC = 10 * 60
_PENDING_2FA: dict[str, dict[str, Any]] = {}


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


def _prune_pending_2fa() -> None:
    now = app_auth._now()
    for key in [k for k, v in _PENDING_2FA.items() if int(v.get("expires_at") or 0) <= now]:
        _PENDING_2FA.pop(key, None)


def _plex_totp_response(request: Request, cfg: dict[str, Any], state: str, rec: dict[str, Any], user: dict[str, Any]) -> JSONResponse | None:
    a = cfg.get("app_auth")
    if not isinstance(a, dict):
        return None
    totp = app_auth._totp_record_for_user(a, user)
    if not app_auth._totp_enabled(totp):
        return None
    user_id = str(user.get("id") or app_auth.ADMIN_USER_ID)
    code = app_auth._totp_clean_code(rec.get("totp_code"))
    if not code:
        return JSONResponse(
            {"ok": False, "error": "Verification code required", "requires_2fa": True, "state": state},
            status_code=401,
            headers={"Cache-Control": "no-store"},
        )
    ok_rl, retry = app_auth._rate_limit_ok(request)
    if not ok_rl:
        st = app_auth._rate_limit_state(request)
        body = app_auth._login_error_payload(error=f"Try again in {retry}s", attempts=st["n"], retry_after=retry)
        body["requires_2fa"] = True
        body["state"] = state
        app_auth._audit(request, "2fa_failed", actor=user, status="failed", message="Plex SSO two-factor verification was rate limited", target_type="user", target_id=user_id, fields={"attempts": st["n"], "retry_after": retry})
        return JSONResponse(body, status_code=429, headers={"Cache-Control": "no-store"})
    if not app_auth._totp_verify(totp.get("secret"), code):
        st = app_auth._rate_limit_fail(request)
        status = 429 if st["retry_after"] > 0 else 401
        msg = f"Too many failed attempts. Try again in {st['retry_after']}s" if st["retry_after"] > 0 else "Invalid verification code"
        body = app_auth._login_error_payload(error=msg, attempts=st["n"], retry_after=st["retry_after"])
        body["requires_2fa"] = True
        body["state"] = state
        app_auth._audit(request, "2fa_failed", actor=user, status="failed", message="Plex SSO two-factor verification failed", target_type="user", target_id=user_id, fields={"attempts": st["n"], "retry_after": st["retry_after"]})
        return JSONResponse(body, status_code=status, headers={"Cache-Control": "no-store"})
    return None


def _finish_login(request: Request, cfg: dict[str, Any], res: dict[str, Any], identity: dict[str, Any]) -> JSONResponse:
    def _mutate(latest: dict[str, Any]) -> tuple[dict[str, Any], str, int]:
        latest_user = _resolve_plex_user(latest, identity)
        if latest_user is None:
            raise KeyError("linked_user_not_found")
        token2, exp2 = app_auth._issue_session(latest, request, latest_user)
        return latest_user, token2, exp2

    try:
        cfg, result = app_auth._update_config(_mutate)
        user, token, exp = result
    except KeyError:
        resp = JSONResponse({"ok": False, "error": "This Plex account is not linked for CrossWatch sign-in"}, status_code=403, headers={"Cache-Control": "no-store"})
        _del_flow_cookie(resp, request)
        return resp
    app_auth._audit(request, "login", actor=user, message=f"{user.get('username') or 'User'} logged in with Plex SSO", target_type="user", target_id=user.get("id"), fields={"plex_username": identity.get("username"), "remember_me": bool(res.get("remember_me"))})
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
    app_auth._rate_limit_reset(request)
    app_auth._set_cookie(resp, token, exp, request, persistent=bool(res.get("remember_me")))
    return resp


def _callback_url(request: Request) -> str:
    origin = app_auth._expected_origin(request)
    if not origin:
        origin = f"{request.url.scheme}://{request.url.netloc}"
    return f"{origin.rstrip('/')}/api/app-auth/plex/callback"


def _unauthorized() -> JSONResponse:
    return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})


def _plex_link_conflict_error() -> str:
    return "This Plex account is already linked to another CrossWatch account"


def _require_authenticated(request: Request, cfg: dict[str, Any]) -> str | None:
    token = request.cookies.get(app_auth.COOKIE_NAME)
    if not app_auth.auth_required(cfg):
        return None
    if not app_auth.is_authenticated(cfg, token):
        return None
    return token


def _linked_raw_for_user(a: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    if user.get("is_admin"):
        return a
    uid = app_auth._normalize_app_user_id(user.get("id"))
    raw = app_auth._cfg_users(a).get(uid)
    return raw if isinstance(raw, dict) else {}


def _resolve_plex_user(cfg: dict[str, Any], identity: dict[str, Any]) -> dict[str, Any] | None:
    a = app_auth._cfg_auth(cfg)
    if authPlex.identity_matches_link(a.get("plex_sso") if isinstance(a.get("plex_sso"), dict) else {}, identity):
        return app_auth._admin_identity(a)
    for user_id, raw in app_auth._cfg_users(a).items():
        uid = app_auth._normalize_app_user_id(user_id)
        if not uid or not isinstance(raw, dict):
            continue
        public = app_auth._public_user(uid, raw)
        if public.get("enabled") and authPlex.identity_matches_link(raw.get("plex_sso") if isinstance(raw.get("plex_sso"), dict) else {}, identity):
            return public
    return None


def _plex_identity_in_use(cfg: dict[str, Any], identity: dict[str, Any], *, exclude_user_id: Any = "") -> bool:
    exclude = app_auth._normalize_app_user_id(exclude_user_id)
    a = app_auth._cfg_auth(cfg)
    if exclude != app_auth.ADMIN_USER_ID and authPlex.identity_matches_link(a.get("plex_sso") if isinstance(a.get("plex_sso"), dict) else {}, identity):
        return True
    for user_id, raw in app_auth._cfg_users(a).items():
        uid = app_auth._normalize_app_user_id(user_id)
        if not uid or uid == exclude or not isinstance(raw, dict):
            continue
        if authPlex.identity_matches_link(raw.get("plex_sso") if isinstance(raw.get("plex_sso"), dict) else {}, identity):
            return True
    return False


def _target_user_for_link(a: dict[str, Any], actor: dict[str, Any] | None, requested_user_id: Any = "") -> tuple[str, dict[str, Any], dict[str, Any]]:
    if not actor:
        return "", {}, {}
    requested = app_auth._normalize_app_user_id(requested_user_id)
    if actor.get("is_admin"):
        target_id = requested or app_auth.ADMIN_USER_ID
    else:
        actor_id = app_auth._normalize_app_user_id(actor.get("id"))
        if requested and requested != actor_id:
            return "", {}, {}
        target_id = actor_id
    if target_id == app_auth.ADMIN_USER_ID:
        return app_auth.ADMIN_USER_ID, app_auth._admin_identity(a), a
    raw = app_auth._cfg_users(a).get(target_id)
    if not isinstance(raw, dict):
        return "", {}, {}
    public = app_auth._public_user(target_id, raw)
    if not public.get("enabled"):
        return "", {}, {}
    return target_id, public, raw


@router.get("/status")
def api_plex_status(request: Request) -> JSONResponse:
    cfg = load_config()
    token = request.cookies.get(app_auth.COOKIE_NAME)
    authed = app_auth.auth_required(cfg) and app_auth.is_authenticated(cfg, token)
    user = app_auth.current_user(cfg, token) if authed else None
    a = app_auth._cfg_auth(cfg)
    raw_user = _linked_raw_for_user(a, user) if user else None
    st = authPlex.get_status(cfg, raw_user)
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
    if not app_auth.auth_required(cfg):
        return JSONResponse(
            {"ok": False, "error": "Authentication setup required"},
            status_code=403,
            headers={"Cache-Control": "no-store"},
        )
    if not authPlex.login_available(cfg):
        return JSONResponse({"ok": False, "error": "Plex sign-in is not linked yet"}, status_code=400, headers={"Cache-Control": "no-store"})

    remember_me = bool((payload or {}).get("remember_me"))
    try:
        flow_nonce = secrets.token_urlsafe(24)
        cfg, data = app_auth._update_config(lambda latest: authPlex.start_flow(
            latest,
            intent="login",
            callback_url=_callback_url(request),
            flow_nonce_hash=authPlex._sha256_hex(flow_nonce),
            remember_me=remember_me,
        ))
        resp = JSONResponse(data, headers={"Cache-Control": "no-store"})
        _set_flow_cookie(resp, flow_nonce, request)
        return resp
    except Exception as exc:
        _log(f"Plex sign-in could not start: {exc}", level="ERROR")
        return JSONResponse({"ok": False, "error": "Plex sign-in could not start. Please try again."}, status_code=502, headers={"Cache-Control": "no-store"})


@router.post("/check")
def api_plex_check(request: Request, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    cfg = load_config()
    if not app_auth.auth_required(cfg):
        return JSONResponse(
            {"ok": False, "error": "Authentication setup required"},
            status_code=403,
            headers={"Cache-Control": "no-store"},
        )
    if not authPlex.login_available(cfg):
        return JSONResponse({"ok": False, "error": "Plex sign-in is not linked yet"}, status_code=400, headers={"Cache-Control": "no-store"})

    _prune_pending_2fa()
    state = str(payload.get("state") or "").strip()
    rec = _PENDING_2FA.get(state)
    res: dict[str, Any]
    if isinstance(rec, dict):
        raw_result = rec.get("result")
        res = dict(raw_result) if isinstance(raw_result, dict) else {}
        res["flow_nonce_hash"] = str(rec.get("flow_nonce_hash") or "")
        res["remember_me"] = bool(rec.get("remember_me"))
        identity = _identity_dict(rec.get("identity"))
        res["identity"] = identity
        code = app_auth._totp_clean_code(payload.get("totp_code") or payload.get("code"))
        rec["totp_code"] = code
    else:
        try:
            res = dict(authPlex.check_flow(cfg, state=state, intent="login"))
        except Exception as exc:
            _log(f"Plex sign-in failed: {exc}", level="ERROR")
            return JSONResponse({"ok": False, "error": "Plex sign-in failed. Please try again."}, status_code=502, headers={"Cache-Control": "no-store"})

        if not res.get("ok"):
            return JSONResponse({"ok": False, "error": str(res.get("error") or "Plex sign-in failed")}, status_code=int(res.get("status_code") or 400), headers={"Cache-Control": "no-store"})

        if res.get("pending"):
            return JSONResponse({"ok": True, "pending": True}, headers={"Cache-Control": "no-store"})
        identity = _identity_dict(res.get("identity"))

    if not _flow_nonce_matches(request, res):
        resp = JSONResponse({"ok": False, "error": "Plex sign-in expired. Start again."}, status_code=400, headers={"Cache-Control": "no-store"})
        _del_flow_cookie(resp, request)
        _PENDING_2FA.pop(state, None)
        return resp

    user = _resolve_plex_user(cfg, identity)
    if user is None:
        resp = JSONResponse({"ok": False, "error": "This Plex account is not linked for CrossWatch sign-in"}, status_code=403, headers={"Cache-Control": "no-store"})
        _del_flow_cookie(resp, request)
        _PENDING_2FA.pop(state, None)
        return resp

    totp_rec: dict[str, Any] = dict(rec) if isinstance(rec, dict) else {}
    totp_rec["totp_code"] = app_auth._totp_clean_code(payload.get("totp_code") or payload.get("code"))
    pending_2fa = _plex_totp_response(request, cfg, state, totp_rec, user)
    if pending_2fa is not None:
        if not isinstance(rec, dict):
            _PENDING_2FA[state] = {
                "result": dict(res),
                "identity": identity,
                "remember_me": bool(res.get("remember_me")),
                "flow_nonce_hash": str(res.get("flow_nonce_hash") or ""),
                "expires_at": app_auth._now() + PENDING_2FA_TTL_SEC,
            }
        return pending_2fa

    _PENDING_2FA.pop(state, None)
    return _finish_login(request, cfg, res, identity)


@router.post("/link/start")
def api_plex_link_start(request: Request, payload: dict[str, Any] | None = Body(None)) -> JSONResponse:
    cfg = load_config()
    token = _require_authenticated(request, cfg)
    if token is None:
        return _unauthorized()
    actor = app_auth.current_user(cfg, token)
    if not actor:
        return _unauthorized()
    if token and not app_auth._origin_allowed(request):
        return app_auth._origin_blocked_response()
    target_id, _target_user, _target_raw = _target_user_for_link(app_auth._cfg_auth(cfg), actor, (payload or {}).get("user_id"))
    if not target_id:
        return _unauthorized()

    try:
        flow_nonce = secrets.token_urlsafe(24)
        cfg, data = app_auth._update_config(lambda latest: authPlex.start_flow(
            latest,
            intent="link",
            callback_url=_callback_url(request),
            flow_nonce_hash=authPlex._sha256_hex(flow_nonce),
            remember_me=False,
            target_user_id=target_id,
        ))
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
    actor = app_auth.current_user(cfg, token)
    if not actor:
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
    requested_target_id = str(res.get("target_user_id") or "").strip()

    def _mutate(latest: dict[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any]]:
        latest_a = app_auth._cfg_auth(latest)
        latest_actor = app_auth.current_user(latest, token)
        target_id, target_user, target_raw = _target_user_for_link(latest_a, latest_actor, requested_target_id)
        if not target_id:
            raise PermissionError("Unauthorized")
        if _plex_identity_in_use(latest, identity, exclude_user_id=target_id):
            raise ValueError("This Plex account is already linked to another CrossWatch account")
        status = authPlex.link_identity(latest, identity, target_raw if target_id != app_auth.ADMIN_USER_ID else None)
        return target_id, target_user, status

    try:
        cfg, link_result = app_auth._update_config(_mutate)
        target_id, target_user, st = link_result
    except PermissionError:
        return _unauthorized()
    except ValueError:
        return JSONResponse({"ok": False, "error": _plex_link_conflict_error()}, status_code=409, headers={"Cache-Control": "no-store"})
    app_auth._audit(request, "plex_sso_linked", actor=actor, target_type="plex_sso", target_id=identity.get("id"), message=f"Plex SSO was linked to {target_user.get('username') or target_id}", fields={"plex_username": identity.get("username"), "user_id": target_id})
    resp = JSONResponse({"ok": True, "pending": False, **st}, headers={"Cache-Control": "no-store"})
    _del_flow_cookie(resp, request)
    return resp


@router.post("/unlink")
def api_plex_unlink(request: Request, payload: dict[str, Any] | None = Body(None)) -> JSONResponse:
    cfg = load_config()
    token = _require_authenticated(request, cfg)
    if token is None:
        return _unauthorized()
    actor = app_auth.current_user(cfg, token)
    if not actor:
        return _unauthorized()
    if token and not app_auth._origin_allowed(request):
        return app_auth._origin_blocked_response()

    requested_target_id = (payload or {}).get("user_id")

    def _mutate(latest: dict[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any]]:
        latest_a = app_auth._cfg_auth(latest)
        latest_actor = app_auth.current_user(latest, token)
        target_id, target_user, target_raw = _target_user_for_link(latest_a, latest_actor, requested_target_id)
        if not target_id:
            raise PermissionError("Unauthorized")
        status = authPlex.unlink_identity(latest, target_raw if target_id != app_auth.ADMIN_USER_ID else None)
        return target_id, target_user, status

    try:
        cfg, unlink_result = app_auth._update_config(_mutate)
        target_id, target_user, st = unlink_result
    except PermissionError:
        return _unauthorized()
    app_auth._audit(request, "plex_sso_unlinked", actor=actor, target_type="plex_sso", target_id=target_id, message=f"Plex SSO was unlinked for {target_user.get('username') or target_id}")
    return JSONResponse({"ok": True, **st}, headers={"Cache-Control": "no-store"})


@router.get("/callback")
def api_plex_callback() -> HTMLResponse:
    return HTMLResponse(
        """<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Plex sign-in complete</title></head><body style="font-family:Segoe UI,Arial,sans-serif;background:#08111d;color:#eef5ff;display:grid;place-items:center;min-height:100vh;margin:0"><div style="text-align:center"><div style="font-size:28px;margin-bottom:8px">Done</div><div style="opacity:.8">You can close this window.</div></div><script>try{window.close()}catch(e){}</script></body></html>""",
        headers={"Cache-Control": "no-store"},
    )


def register_auth_plex(app) -> None:
    app.include_router(router)
