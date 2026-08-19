# api/authOidcAPI.py
# CrossWatch - OIDC authentication API endpoints
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import secrets

from fastapi import APIRouter, Body, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from cw_platform.config_base import load_config
from services import authOidc

from . import appAuthAPI as app_auth

try:
    from _logging import log as _real_log
except ImportError:
    _real_log = None

router = APIRouter(prefix="/api/app-auth/oidc", tags=["app-auth"])
FLOW_COOKIE_NAME = "cw_oidc_flow"
TOTP_COOKIE_NAME = "cw_oidc_2fa"
FLOW_COOKIE_PATH = "/api/app-auth/oidc"
PENDING_2FA_TTL_SEC = 10 * 60
_PENDING_2FA: dict[str, dict[str, Any]] = {}


def _set_flow_cookie(resp: Response, nonce: str, request: Request) -> None:
    resp.set_cookie(
        FLOW_COOKIE_NAME,
        nonce,
        path=FLOW_COOKIE_PATH,
        httponly=True,
        samesite="lax",
        secure=app_auth._effective_scheme_is_https(request),
        max_age=10 * 60,
    )


def _del_flow_cookie(resp: Response, request: Request) -> None:
    resp.delete_cookie(
        FLOW_COOKIE_NAME,
        path=FLOW_COOKIE_PATH,
        httponly=True,
        samesite="lax",
        secure=app_auth._effective_scheme_is_https(request),
    )


def _set_2fa_cookie(resp: Response, token: str, request: Request) -> None:
    resp.set_cookie(
        TOTP_COOKIE_NAME,
        token,
        path=FLOW_COOKIE_PATH,
        httponly=True,
        samesite="lax",
        secure=app_auth._effective_scheme_is_https(request),
        max_age=PENDING_2FA_TTL_SEC,
    )


def _del_2fa_cookie(resp: Response, request: Request) -> None:
    resp.delete_cookie(
        TOTP_COOKIE_NAME,
        path=FLOW_COOKIE_PATH,
        httponly=True,
        samesite="lax",
        secure=app_auth._effective_scheme_is_https(request),
    )


def _flow_nonce_matches(request: Request, want: str) -> bool:
    nonce = str(request.cookies.get(FLOW_COOKIE_NAME) or "").strip()
    wanted = str(want or "").strip()
    if not nonce or not wanted:
        return False
    return app_auth._digest_eq(authOidc._sha256_hex(nonce), wanted)


def _log(msg: str, *, level: str = "INFO") -> None:
    try:
        if _real_log is not None:
            _real_log(msg, level=level, module="AUTH")
        else:
            print(f"[AUTH] {level}: {msg}")
    except Exception:
        pass


def _callback_url(request: Request) -> str:
    origin = app_auth._expected_origin(request)
    if not origin:
        origin = f"{request.url.scheme}://{request.url.netloc}"
    return f"{origin.rstrip('/')}/api/app-auth/oidc/callback"


def _identity_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_next(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw.startswith("/"):
        return "/"
    if raw[1:2] in {"/", "\\"}:
        return "/"
    if any(ch in raw for ch in ("\\", "\r", "\n", "\t")):
        return "/"
    if raw.split("?", 1)[0].split("#", 1)[0] in {"/login", "/logout"}:
        return "/"
    return raw


def _unauthorized() -> JSONResponse:
    return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})


def _oidc_link_conflict_error() -> str:
    return "This OIDC account is already linked to another CrossWatch account"


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


def _resolve_oidc_user(cfg: dict[str, Any], identity: dict[str, Any]) -> dict[str, Any] | None:
    a = app_auth._cfg_auth(cfg)
    if authOidc.identity_matches_link(a.get("oidc_identity") if isinstance(a.get("oidc_identity"), dict) else {}, identity):
        return app_auth._admin_identity(a)
    for user_id, raw in app_auth._cfg_users(a).items():
        uid = app_auth._normalize_app_user_id(user_id)
        if not uid or not isinstance(raw, dict):
            continue
        public = app_auth._public_user(uid, raw)
        if public.get("enabled") and authOidc.identity_matches_link(raw.get("oidc") if isinstance(raw.get("oidc"), dict) else {}, identity):
            return public
    return None


def _oidc_identity_in_use(cfg: dict[str, Any], identity: dict[str, Any], *, exclude_user_id: Any = "") -> bool:
    exclude = app_auth._normalize_app_user_id(exclude_user_id)
    a = app_auth._cfg_auth(cfg)
    if exclude != app_auth.ADMIN_USER_ID and authOidc.identity_matches_link(a.get("oidc_identity") if isinstance(a.get("oidc_identity"), dict) else {}, identity):
        return True
    for user_id, raw in app_auth._cfg_users(a).items():
        uid = app_auth._normalize_app_user_id(user_id)
        if not uid or uid == exclude or not isinstance(raw, dict):
            continue
        if authOidc.identity_matches_link(raw.get("oidc") if isinstance(raw.get("oidc"), dict) else {}, identity):
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


def _prune_pending_2fa() -> None:
    now = app_auth._now()
    for key in [k for k, v in _PENDING_2FA.items() if int(v.get("expires_at") or 0) <= now]:
        _PENDING_2FA.pop(key, None)


def _totp_prompt_html(error: str = "") -> str:
    import html as html_lib

    note = f'<p class="err">{html_lib.escape(error)}</p>' if error else ""
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Two-factor verification | CrossWatch</title>
<style>body{{font-family:Segoe UI,Arial,sans-serif;background:#08111d;color:#eef5ff;display:grid;place-items:center;min-height:100vh;margin:0}}
.box{{width:min(360px,92vw);text-align:center}}h1{{font-size:20px;margin:0 0 6px}}p{{opacity:.78;margin:0 0 18px;font-size:14px}}
p.err{{color:#ff8c99;opacity:1}}input{{width:100%;padding:12px 14px;font-size:18px;letter-spacing:.24em;text-align:center;border-radius:8px;
border:1px solid rgba(165,126,255,.3);background:rgba(8,17,30,.94);color:#eef5ff;box-sizing:border-box}}
button{{width:100%;margin-top:12px;padding:12px 14px;font-size:15px;border:0;border-radius:8px;background:#8c6dff;color:#fff;cursor:pointer}}
button:disabled{{opacity:.6;cursor:default}}</style></head><body><div class="box">
<h1>Two-factor verification</h1><p>Enter the code from your authenticator app, or a recovery code.</p>{note}
<form id="f" autocomplete="off"><input id="c" name="code" inputmode="text" autocomplete="one-time-code" placeholder="123456" autofocus>
<button id="b" type="submit">Verify</button></form></div>
<script>
document.getElementById('f').addEventListener('submit',async(e)=>{{
  e.preventDefault();
  const b=document.getElementById('b');b.disabled=true;
  try{{
    const r=await fetch('/api/app-auth/oidc/2fa',{{method:'POST',headers:{{'Content-Type':'application/json'}},
      credentials:'same-origin',cache:'no-store',body:JSON.stringify({{code:document.getElementById('c').value}})}});
    const d=await r.json().catch(()=>({{}}));
    if(r.ok&&d.ok){{location.href=d.next||'/';return;}}
    location.href='/api/app-auth/oidc/2fa/retry';
  }}catch(err){{b.disabled=false;}}
}});
</script></body></html>"""


def _totp_pending_response(request: Request, cfg: dict[str, Any], res: dict[str, Any], identity: dict[str, Any], user: dict[str, Any]) -> Response | None:
    a = cfg.get("app_auth")
    if not isinstance(a, dict):
        return None
    totp = app_auth._totp_record_for_user(a, user)
    if not app_auth._totp_enabled(totp):
        return None
    _prune_pending_2fa()
    token = secrets.token_urlsafe(24)
    flow_nonce = secrets.token_urlsafe(24)
    _PENDING_2FA[token] = {
        "identity": dict(identity or {}),
        "remember_me": bool(res.get("remember_me")),
        "next_url": _safe_next(res.get("next_url")),
        "flow_nonce_hash": authOidc._sha256_hex(flow_nonce),
        "expires_at": app_auth._now() + PENDING_2FA_TTL_SEC,
    }
    resp = HTMLResponse(_totp_prompt_html(), headers={"Cache-Control": "no-store"})
    _set_flow_cookie(resp, flow_nonce, request)
    _set_2fa_cookie(resp, token, request)
    return resp


@router.get("/status")
def api_oidc_status(request: Request) -> JSONResponse:
    cfg = load_config()
    token = request.cookies.get(app_auth.COOKIE_NAME)
    authed = app_auth.auth_required(cfg) and app_auth.is_authenticated(cfg, token)
    user = app_auth.current_user(cfg, token) if authed else None
    a = app_auth._cfg_auth(cfg)
    raw_user = _linked_raw_for_user(a, user) if user and not user.get("is_admin") else None
    st = authOidc.get_status(cfg, raw_user)
    payload = {
        "enabled": bool(st["enabled"]),
        "configured": bool(st["configured"]),
        "linked": bool(st["linked"]),
        "login_available": authOidc.login_available(cfg),
        "issuer": st["issuer"] if authed and user and user.get("is_admin") else "",
        "linked_username": st["linked_username"] if authed else "",
        "linked_email": st["linked_email"] if authed else "",
        "linked_picture": st["linked_picture"] if authed else "",
        "linked_at": int(st["linked_at"] or 0) if authed else 0,
    }
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@router.get("/config")
def api_oidc_config(request: Request) -> JSONResponse:
    cfg = load_config()
    token = request.cookies.get(app_auth.COOKIE_NAME)
    user = app_auth.current_user(cfg, token)
    if not user or not user.get("is_admin"):
        return _unauthorized()
    return JSONResponse({"ok": True, **authOidc.public_config(cfg)}, headers={"Cache-Control": "no-store"})


@router.post("/config")
def api_oidc_save_config(request: Request, payload: dict[str, Any] | None = Body(None)) -> JSONResponse:
    cfg = load_config()
    token = request.cookies.get(app_auth.COOKIE_NAME)
    user = app_auth.current_user(cfg, token)
    if not user or not user.get("is_admin"):
        return _unauthorized()
    if token and not app_auth._origin_allowed(request):
        return app_auth._origin_blocked_response()

    def _mutate(latest: dict[str, Any]) -> dict[str, Any]:
        a = app_auth._cfg_auth(latest)
        prev = a.get("oidc") if isinstance(a.get("oidc"), dict) else {}
        a["oidc"] = authOidc.sanitize_config(payload, prev)
        return authOidc.public_config(latest)

    cfg, out = app_auth._update_config(_mutate)
    app_auth._audit(request, "oidc_config_saved", actor=user, target_type="oidc", target_id="config", message="OIDC configuration was updated")
    return JSONResponse({"ok": True, **out}, headers={"Cache-Control": "no-store"})


@router.get("/start")
def api_oidc_start(request: Request, remember_me: bool = Query(False), next: str = Query("")) -> Response:
    cfg = load_config()
    if not app_auth.auth_required(cfg):
        return JSONResponse({"ok": False, "error": "Authentication setup required"}, status_code=403, headers={"Cache-Control": "no-store"})
    if not authOidc.login_available(cfg):
        return JSONResponse({"ok": False, "error": "OIDC sign-in is not available"}, status_code=400, headers={"Cache-Control": "no-store"})
    try:
        flow_nonce = secrets.token_urlsafe(24)
        data = authOidc.start_flow(
            cfg,
            intent="login",
            callback_url=_callback_url(request),
            remember_me=remember_me,
            next_url=_safe_next(next),
            flow_nonce_hash=authOidc._sha256_hex(flow_nonce),
        )
        resp = RedirectResponse(url=str(data["auth_url"]), status_code=302)
        _set_flow_cookie(resp, flow_nonce, request)
        return resp
    except Exception as exc:
        _log(f"OIDC sign-in could not start: {exc}", level="ERROR")
        return JSONResponse({"ok": False, "error": "OIDC sign-in could not start. Check the OIDC configuration."}, status_code=502, headers={"Cache-Control": "no-store"})


@router.get("/callback")
def api_oidc_callback(request: Request, state: str = Query(""), code: str = Query(""), error: str = Query("")) -> Response:
    if error:
        resp = HTMLResponse("OIDC sign-in was cancelled or failed.", status_code=400, headers={"Cache-Control": "no-store"})
        _del_flow_cookie(resp, request)
        return resp
    cfg = load_config()
    if not _flow_nonce_matches(request, authOidc.flow_nonce_hash(state)):
        authOidc.drop_flow(state)
        resp = HTMLResponse("OIDC sign-in expired. Start again.", status_code=400, headers={"Cache-Control": "no-store"})
        _del_flow_cookie(resp, request)
        return resp
    try:
        res = authOidc.check_callback(cfg, state=state, code=code)
    except Exception as exc:
        _log(f"OIDC callback failed: {exc}", level="ERROR")
        resp = HTMLResponse("OIDC sign-in failed. Check the OIDC configuration.", status_code=502, headers={"Cache-Control": "no-store"})
        _del_flow_cookie(resp, request)
        return resp
    if not res.get("ok"):
        resp = HTMLResponse("OIDC sign-in failed. Start again.", status_code=int(res.get("status_code") or 400), headers={"Cache-Control": "no-store"})
        _del_flow_cookie(resp, request)
        return resp
    if res.get("intent") == "link":
        authOidc.complete_link(state, res)
        resp = HTMLResponse(
            """<!doctype html><html lang="en"><head><meta charset="utf-8"><title>OIDC link complete</title></head><body style="font-family:Segoe UI,Arial,sans-serif;background:#08111d;color:#eef5ff;display:grid;place-items:center;min-height:100vh;margin:0"><div style="text-align:center"><div style="font-size:28px;margin-bottom:8px">Done</div><div style="opacity:.8">You can close this window.</div></div><script>try{window.close()}catch(e){}</script></body></html>""",
            headers={"Cache-Control": "no-store"},
        )
        _del_flow_cookie(resp, request)
        return resp
    identity = _identity_dict(res.get("identity"))
    user = _resolve_oidc_user(cfg, identity)
    if user is None:
        resp = HTMLResponse("This OIDC account is not linked for CrossWatch sign-in.", status_code=403, headers={"Cache-Control": "no-store"})
        _del_flow_cookie(resp, request)
        return resp
    pending_2fa = _totp_pending_response(request, cfg, res, identity, user)
    if pending_2fa is not None:
        return pending_2fa

    def _mutate(latest: dict[str, Any]) -> tuple[dict[str, Any], str, int]:
        latest_user = _resolve_oidc_user(latest, identity)
        if latest_user is None:
            raise KeyError("linked_user_not_found")
        token2, exp2 = app_auth._issue_session(latest, request, latest_user)
        return latest_user, token2, exp2

    try:
        cfg, result = app_auth._update_config(_mutate)
        login_user, token, exp = result
    except KeyError:
        resp = HTMLResponse("This OIDC account is not linked for CrossWatch sign-in.", status_code=403, headers={"Cache-Control": "no-store"})
        _del_flow_cookie(resp, request)
        return resp
    app_auth._audit(request, "login", actor=login_user, message=f"{login_user.get('username') or 'User'} logged in with OIDC", target_type="user", target_id=login_user.get("id"), fields={"oidc_username": identity.get("username"), "remember_me": bool(res.get("remember_me"))})
    dest = _safe_next(res.get("next_url")) if login_user.get("is_admin") else "/profile"
    resp = RedirectResponse(url=dest, status_code=302)
    _del_flow_cookie(resp, request)
    app_auth._set_cookie(resp, token, exp, request, persistent=bool(res.get("remember_me")))
    return resp


@router.get("/2fa/retry")
def api_oidc_2fa_retry(request: Request) -> Response:
    _prune_pending_2fa()
    token = str(request.cookies.get(TOTP_COOKIE_NAME) or "").strip()
    rec = _PENDING_2FA.get(token)
    if not isinstance(rec, dict) or not _flow_nonce_matches(request, str(rec.get("flow_nonce_hash") or "")):
        _PENDING_2FA.pop(token, None)
        resp = RedirectResponse(url="/login", status_code=302)
        _del_flow_cookie(resp, request)
        _del_2fa_cookie(resp, request)
        return resp
    return HTMLResponse(_totp_prompt_html("Invalid verification code"), headers={"Cache-Control": "no-store"})


@router.post("/2fa")
def api_oidc_2fa(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    _prune_pending_2fa()
    cfg = load_config()
    token = str(request.cookies.get(TOTP_COOKIE_NAME) or (payload or {}).get("state") or "").strip()
    rec = _PENDING_2FA.get(token)
    if not isinstance(rec, dict) or not _flow_nonce_matches(request, str(rec.get("flow_nonce_hash") or "")):
        _PENDING_2FA.pop(token, None)
        resp = JSONResponse({"ok": False, "error": "Sign-in expired. Start again."}, status_code=400, headers={"Cache-Control": "no-store"})
        _del_2fa_cookie(resp, request)
        return resp

    identity = _identity_dict(rec.get("identity"))
    user = _resolve_oidc_user(cfg, identity)
    if user is None:
        _PENDING_2FA.pop(token, None)
        resp = JSONResponse({"ok": False, "error": "This OIDC account is not linked for CrossWatch sign-in"}, status_code=403, headers={"Cache-Control": "no-store"})
        _del_2fa_cookie(resp, request)
        return resp

    ok_rl, retry = app_auth._rate_limit_ok(request)
    if not ok_rl:
        return JSONResponse({"ok": False, "error": f"Try again in {retry}s"}, status_code=429, headers={"Cache-Control": "no-store"})

    raw_code = (payload or {}).get("code") or (payload or {}).get("totp_code")
    a = app_auth._cfg_auth(cfg)
    totp = app_auth._totp_record_for_user(a, user)
    code = app_auth._totp_clean_code(raw_code)
    totp_ok = app_auth._totp_verify(totp.get("secret"), code)
    recovery_used = False
    if not totp_ok:
        recovery_used = app_auth._consume_recovery_code(a, user, raw_code)
    if not (totp_ok or recovery_used):
        st = app_auth._rate_limit_fail(request)
        msg = f"Too many failed attempts. Try again in {st['retry_after']}s" if st["retry_after"] > 0 else "Invalid verification code"
        app_auth._audit(request, "2fa_failed", actor=user, status="failed", message="OIDC two-factor verification failed", target_type="user", target_id=user.get("id"), fields={"attempts": st["n"]})
        return JSONResponse({"ok": False, "error": msg}, status_code=429 if st["retry_after"] > 0 else 401, headers={"Cache-Control": "no-store"})

    def _mutate(latest: dict[str, Any]) -> tuple[dict[str, Any], str, int]:
        latest_user = _resolve_oidc_user(latest, identity)
        if latest_user is None:
            raise KeyError("linked_user_not_found")
        if recovery_used and not app_auth._consume_recovery_code(app_auth._cfg_auth(latest), latest_user, raw_code):
            raise ValueError("invalid_recovery_code")
        token2, exp2 = app_auth._issue_session(latest, request, latest_user)
        return latest_user, token2, exp2

    try:
        cfg, result = app_auth._update_config(_mutate)
        login_user, session_token, exp = result
    except (KeyError, ValueError):
        _PENDING_2FA.pop(token, None)
        resp = JSONResponse({"ok": False, "error": "This OIDC account is not linked for CrossWatch sign-in"}, status_code=403, headers={"Cache-Control": "no-store"})
        _del_2fa_cookie(resp, request)
        return resp

    _PENDING_2FA.pop(token, None)
    app_auth._rate_limit_reset(request)
    if recovery_used:
        app_auth._audit(request, "2fa_recovery_code_used", actor=login_user, message=f"{login_user.get('username') or 'User'} used a recovery code", target_type="user", target_id=login_user.get("id"))
    app_auth._audit(request, "login", actor=login_user, message=f"{login_user.get('username') or 'User'} logged in with OIDC", target_type="user", target_id=login_user.get("id"), fields={"oidc_username": identity.get("username"), "remember_me": bool(rec.get("remember_me"))})
    dest = _safe_next(rec.get("next_url")) if login_user.get("is_admin") else "/profile"
    resp = JSONResponse({"ok": True, "next": dest}, headers={"Cache-Control": "no-store"})
    _del_flow_cookie(resp, request)
    _del_2fa_cookie(resp, request)
    app_auth._set_cookie(resp, session_token, exp, request, persistent=bool(rec.get("remember_me")))
    return resp


@router.post("/link/start")
def api_oidc_link_start(request: Request, payload: dict[str, Any] | None = Body(None)) -> JSONResponse:
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
        data = authOidc.start_flow(
            cfg,
            intent="link",
            callback_url=_callback_url(request),
            target_user_id=target_id,
            flow_nonce_hash=authOidc._sha256_hex(flow_nonce),
        )
        resp = JSONResponse(data, headers={"Cache-Control": "no-store"})
        _set_flow_cookie(resp, flow_nonce, request)
        return resp
    except Exception as exc:
        _log(f"OIDC link could not start: {exc}", level="ERROR")
        return JSONResponse({"ok": False, "error": "OIDC link could not start. Check the OIDC configuration."}, status_code=502, headers={"Cache-Control": "no-store"})


@router.post("/link/check")
def api_oidc_link_check(request: Request, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    cfg = load_config()
    token = _require_authenticated(request, cfg)
    if token is None:
        return _unauthorized()
    actor = app_auth.current_user(cfg, token)
    if not actor:
        return _unauthorized()
    if token and not app_auth._origin_allowed(request):
        return app_auth._origin_blocked_response()
    res = authOidc.consume_link(str(payload.get("state") or ""))
    if res.get("pending"):
        return JSONResponse({"ok": True, "pending": True}, headers={"Cache-Control": "no-store"})
    if not res.get("ok"):
        return JSONResponse({"ok": False, "error": "OIDC link failed"}, status_code=int(res.get("status_code") or 400), headers={"Cache-Control": "no-store"})
    identity = _identity_dict(res.get("identity"))
    requested_target_id = str(res.get("target_user_id") or "").strip()

    def _mutate(latest: dict[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any]]:
        latest_a = app_auth._cfg_auth(latest)
        latest_actor = app_auth.current_user(latest, token)
        target_id, target_user, target_raw = _target_user_for_link(latest_a, latest_actor, requested_target_id)
        if not target_id:
            raise PermissionError("Unauthorized")
        if _oidc_identity_in_use(latest, identity, exclude_user_id=target_id):
            raise ValueError("This OIDC account is already linked to another CrossWatch account")
        status = authOidc.link_identity(latest, identity, target_raw if target_id != app_auth.ADMIN_USER_ID else None)
        return target_id, target_user, status

    try:
        cfg, link_result = app_auth._update_config(_mutate)
        target_id, target_user, st = link_result
    except PermissionError:
        return _unauthorized()
    except ValueError:
        return JSONResponse({"ok": False, "error": _oidc_link_conflict_error()}, status_code=409, headers={"Cache-Control": "no-store"})
    app_auth._audit(request, "oidc_linked", actor=actor, target_type="oidc", target_id=identity.get("sub"), message=f"OIDC was linked to {target_user.get('username') or target_id}", fields={"oidc_username": identity.get("username"), "user_id": target_id})
    return JSONResponse({"ok": True, "pending": False, **st}, headers={"Cache-Control": "no-store"})


@router.post("/unlink")
def api_oidc_unlink(request: Request, payload: dict[str, Any] | None = Body(None)) -> JSONResponse:
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
        status = authOidc.unlink_identity(latest, target_raw if target_id != app_auth.ADMIN_USER_ID else None)
        return target_id, target_user, status

    try:
        cfg, unlink_result = app_auth._update_config(_mutate)
        target_id, target_user, st = unlink_result
    except PermissionError:
        return _unauthorized()
    app_auth._audit(request, "oidc_unlinked", actor=actor, target_type="oidc", target_id=target_id, message=f"OIDC was unlinked for {target_user.get('username') or target_id}")
    return JSONResponse({"ok": True, **st}, headers={"Cache-Control": "no-store"})


def register_auth_oidc(app) -> None:
    app.include_router(router)
