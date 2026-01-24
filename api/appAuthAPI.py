# /api/appAuthAPI.py
# CrossWatch - UI authentication API
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from typing import Any

import base64
import hashlib
import hmac
import secrets
import time

from fastapi import APIRouter, Body, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from cw_platform.config_base import load_config, save_config

__all__ = [
    "router",
    "COOKIE_NAME",
    "AUTH_TTL_SEC",
    "auth_required",
    "is_authenticated",
    "register_app_auth",
]

COOKIE_NAME = "cw_auth"
AUTH_TTL_SEC = 30 * 24 * 60 * 60

_LOGIN_FAILS: dict[str, dict[str, Any]] = {}


def _now() -> int:
    return int(time.time())


def _b64e(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("ascii").rstrip("=")


def _b64d(s: str) -> bytes:
    pad = "=" * ((4 - (len(s) % 4)) % 4)
    return base64.urlsafe_b64decode((s or "") + pad)


def _sha256_hex(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _pbkdf2_hash(password: str, salt: bytes, *, iterations: int) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", (password or "").encode("utf-8"), salt, int(iterations))


def _cfg_auth(cfg: dict[str, Any]) -> dict[str, Any]:
    a = cfg.get("app_auth")
    return a if isinstance(a, dict) else {}


def _cfg_pwd(a: dict[str, Any]) -> dict[str, Any]:
    p = a.get("password")
    return p if isinstance(p, dict) else {}


def _cfg_session(a: dict[str, Any]) -> dict[str, Any]:
    s = a.get("session")
    return s if isinstance(s, dict) else {}


def auth_required(cfg: dict[str, Any]) -> bool:
    a = _cfg_auth(cfg)
    if not bool(a.get("enabled")):
        return False
    if not str(a.get("username") or "").strip():
        return False
    p = _cfg_pwd(a)
    if not str(p.get("hash") or "").strip():
        return False
    if not str(p.get("salt") or "").strip():
        return False
    return True


def is_authenticated(cfg: dict[str, Any], token: str | None) -> bool:
    if not auth_required(cfg):
        return True
    t = (token or "").strip()
    if not t:
        return False
    a = _cfg_auth(cfg)
    s = _cfg_session(a)
    exp = int(s.get("expires_at") or 0)
    if exp <= _now():
        return False
    want = str(s.get("token_hash") or "").strip()
    if not want:
        return False
    return hmac.compare_digest(_sha256_hex(t), want)


def _rate_limit_ok(request: Request) -> tuple[bool, int]:
    ip = getattr(getattr(request, "client", None), "host", "") or "local"
    rec = _LOGIN_FAILS.get(ip) or {"n": 0, "until": 0}
    until = int(rec.get("until") or 0)
    if until > _now():
        return False, max(1, until - _now())
    return True, 0


def _rate_limit_fail(request: Request) -> None:
    ip = getattr(getattr(request, "client", None), "host", "") or "local"
    rec = _LOGIN_FAILS.get(ip) or {"n": 0, "until": 0}
    n = int(rec.get("n") or 0) + 1
    backoff = min(60, 2 ** min(5, n))
    _LOGIN_FAILS[ip] = {"n": n, "until": _now() + backoff}


def _issue_session(cfg: dict[str, Any]) -> tuple[str, int]:
    token = secrets.token_urlsafe(32)
    exp = _now() + AUTH_TTL_SEC
    a = cfg.setdefault("app_auth", {})
    if not isinstance(a, dict):
        a = {}
        cfg["app_auth"] = a
    s = a.setdefault("session", {})
    if not isinstance(s, dict):
        s = {}
        a["session"] = s
    s["token_hash"] = _sha256_hex(token)
    s["expires_at"] = exp
    a["last_login_at"] = _now()
    return token, exp


def _clear_session(cfg: dict[str, Any]) -> None:
    a = cfg.get("app_auth")
    if not isinstance(a, dict):
        return
    s = a.get("session")
    if not isinstance(s, dict):
        s = {}
        a["session"] = s
    s["token_hash"] = ""
    s["expires_at"] = 0


def _set_cookie(resp: Response, token: str, exp: int, request: Request) -> None:
    secure = str(request.url.scheme).lower() == "https"
    resp.set_cookie(
        COOKIE_NAME,
        token,
        max_age=max(1, exp - _now()),
        expires=exp,
        path="/",
        httponly=True,
        samesite="strict",
        secure=secure,
    )


def _del_cookie(resp: Response, request: Request) -> None:
    secure = str(request.url.scheme).lower() == "https"
    resp.delete_cookie(COOKIE_NAME, path="/", samesite="strict", secure=secure)


def _login_html(username: str) -> str:
    u = (username or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f"""<!doctype html>
<html lang=\"en\"><head>
  <meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">
  <title>CrossWatch | Sign in</title>
  <link rel=\"icon\" type=\"image/svg+xml\" href=\"/favicon.svg\">
  <link rel=\"stylesheet\" href=\"/assets/crosswatch.css\">
  <style>
    body{{display:flex;align-items:center;justify-content:center;min-height:100vh}}
    .cw-login{{width:min(520px,92vw);padding:18px 18px 14px;border-radius:18px;background:rgba(13,15,22,.92);border:1px solid rgba(120,128,160,.14);box-shadow:0 10px 28px rgba(0,0,0,.45)}}
    .cw-login h1{{margin:0 0 10px;font-size:18px;letter-spacing:.2px}}
    .cw-login .sub{{opacity:.8;margin:0 0 14px;font-size:13px}}
    .cw-login .grid{{display:grid;grid-template-columns:1fr;gap:10px}}
    .cw-login input{{width:100%}}
    .cw-login .row{{display:flex;gap:10px;align-items:center;justify-content:space-between;margin-top:10px}}
    .cw-login .err{{margin-top:10px;display:none}}
  </style>
</head><body>
  <div class=\"cw-login\">
    <h1>CrossWatch Authentication</h1>
    <p class=\"sub\">Sign-in</p>
    <div class=\"grid\">
      <div><label>Username</label><input id=\"u\" autocomplete=\"username\" value=\"{u}\"></div>
      <div><label>Password</label><input id=\"p\" type=\"password\" autocomplete=\"current-password\"></div>
      <div class=\"row\">
        <button class=\"btn acc\" id=\"go\">Sign in</button>
        <span id=\"msg\" class=\"msg warn err\"></span>
      </div>
    </div>
  </div>
  <script>
    const $=(id)=>document.getElementById(id);
    const msg=$('msg');
    async function login(){{
      msg.style.display='none';
      const u=$('u').value.trim();
      const p=$('p').value;
      try{{
        const r=await fetch('/api/app-auth/login',{{method:'POST',headers:{{'Content-Type':'application/json'}},credentials:'same-origin',body:JSON.stringify({{username:u,password:p}})}});
        const data=await r.json().catch(()=>null);
        if(!r.ok || !data || !data.ok){{
          msg.textContent=(data && data.error) ? data.error : ('Login failed ('+r.status+')');
          msg.style.display='inline-flex';
          return;
        }}
        location.href='/';
      }}catch(e){{
        msg.textContent='Login failed';
        msg.style.display='inline-flex';
      }}
    }}
    $('go').addEventListener('click', login);
    $('p').addEventListener('keydown', (e)=>{{ if(e.key==='Enter') login(); }});
    $('u').addEventListener('keydown', (e)=>{{ if(e.key==='Enter') login(); }});
  </script>
</body></html>"""


router = APIRouter(prefix="/api/app-auth", tags=["app-auth"])


@router.get("/status")
def api_status(request: Request) -> JSONResponse:
    cfg = load_config()
    a = _cfg_auth(cfg)
    p = _cfg_pwd(a)
    configured = bool(str(a.get("username") or "").strip() and str(p.get("hash") or "").strip() and str(p.get("salt") or "").strip())
    enabled = bool(a.get("enabled"))
    token = request.cookies.get(COOKIE_NAME)
    return JSONResponse(
        {
            "enabled": enabled,
            "configured": configured,
            "username": str(a.get("username") or "") if enabled else "",
            "authenticated": is_authenticated(cfg, token),
            "session_expires_at": int((_cfg_session(a).get("expires_at") or 0)),
        },
        headers={"Cache-Control": "no-store"},
    )


@router.post("/login")
def api_login(request: Request, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    req = request
    cfg = load_config()
    a = _cfg_auth(cfg)
    if not auth_required(cfg):
        return JSONResponse({"ok": False, "error": "Authentication is not configured"}, status_code=400)

    ok_rl, retry = _rate_limit_ok(req)
    if not ok_rl:
        return JSONResponse({"ok": False, "error": f"Try again in {retry}s"}, status_code=429)

    u = str(payload.get("username") or "").strip()
    ptxt = str(payload.get("password") or "")
    if u != str(a.get("username") or ""):
        _rate_limit_fail(req)
        return JSONResponse({"ok": False, "error": "Invalid credentials"}, status_code=401)

    pwd = _cfg_pwd(a)
    try:
        salt = _b64d(str(pwd.get("salt") or ""))
        iters = int(pwd.get("iterations") or 260_000)
        want = str(pwd.get("hash") or "")
        got = _b64e(_pbkdf2_hash(ptxt, salt, iterations=iters))
        if not hmac.compare_digest(got, want):
            _rate_limit_fail(req)
            return JSONResponse({"ok": False, "error": "Invalid credentials"}, status_code=401)
    except Exception:
        return JSONResponse({"ok": False, "error": "Authentication is not configured"}, status_code=400)

    token, exp = _issue_session(cfg)
    save_config(cfg)
    resp = JSONResponse({"ok": True, "expires_at": exp}, headers={"Cache-Control": "no-store"})
    _set_cookie(resp, token, exp, req)
    return resp


@router.post("/logout")
def api_logout(request: Request) -> JSONResponse:
    cfg = load_config()
    _clear_session(cfg)
    save_config(cfg)
    resp = JSONResponse({"ok": True}, headers={"Cache-Control": "no-store"})
    _del_cookie(resp, request)
    return resp


@router.post("/credentials")
def api_set_credentials(request: Request, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    req = request
    cfg = load_config()
    a0 = _cfg_auth(cfg)
    configured0 = auth_required(cfg)
    token = req.cookies.get(COOKIE_NAME)

    if configured0 and not is_authenticated(cfg, token):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)

    enabled = bool(payload.get("enabled"))
    username = str(payload.get("username") or "").strip()
    password = str(payload.get("password") or "")

    a = cfg.setdefault("app_auth", {})
    if not isinstance(a, dict):
        a = {}
        cfg["app_auth"] = a

    if not enabled:
        a["enabled"] = False
        a["username"] = username or str(a.get("username") or "")
        _clear_session(cfg)
        save_config(cfg)
        resp = JSONResponse({"ok": True, "enabled": False}, headers={"Cache-Control": "no-store"})
        _del_cookie(resp, req)
        return resp

    if not username:
        return JSONResponse({"ok": False, "error": "Username is required"}, status_code=400)

    pwd = a.setdefault("password", {})
    if not isinstance(pwd, dict):
        pwd = {}
        a["password"] = pwd

    has_existing = bool(str(pwd.get("hash") or "").strip() and str(pwd.get("salt") or "").strip())
    if not password and not has_existing:
        return JSONResponse({"ok": False, "error": "Password is required"}, status_code=400)

    if password:
        salt = secrets.token_bytes(16)
        iters = 260_000
        pwd.update(
            {
                "scheme": "pbkdf2_sha256",
                "iterations": iters,
                "salt": _b64e(salt),
                "hash": _b64e(_pbkdf2_hash(password, salt, iterations=iters)),
            }
        )

    a["enabled"] = True
    a["username"] = username
    _clear_session(cfg)

    token2, exp2 = _issue_session(cfg)
    save_config(cfg)

    resp = JSONResponse({"ok": True, "enabled": True, "expires_at": exp2}, headers={"Cache-Control": "no-store"})
    _set_cookie(resp, token2, exp2, req)
    return resp


def register_app_auth(app) -> None:
    app.include_router(router)

    @app.get("/login", include_in_schema=False, tags=["ui"])
    def ui_login() -> Response:
        cfg = load_config()
        if not auth_required(cfg):
            return RedirectResponse(url="/", status_code=302)
        a = _cfg_auth(cfg)
        username = str(a.get("username") or "")
        return HTMLResponse(_login_html(username), headers={"Cache-Control": "no-store"})

    @app.get("/logout", include_in_schema=False, tags=["ui"])
    def ui_logout(request: Request) -> Response:
        cfg = load_config()
        _clear_session(cfg)
        save_config(cfg)
        resp = RedirectResponse(url="/login" if auth_required(cfg) else "/")
        _del_cookie(resp, request)
        return resp