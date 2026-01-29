# /api/appAuthAPI.py
# CrossWatch - UI authentication API
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from typing import Any

import base64
import hashlib
import hmac
import os
import secrets
import threading
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
MAX_SESSIONS = 10

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

def _is_sha256_hex(s: str) -> bool:
    v = (s or "").strip().lower()
    if len(v) != 64:
        return False
    for ch in v:
        if ch not in "0123456789abcdef":
            return False
    return True

def _digest_eq(a: str, b: str) -> bool:
    return hmac.compare_digest((a or "").encode("utf-8"), (b or "").encode("utf-8"))


def _pbkdf2_hash(password: str, salt: bytes, *, iterations: int) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", (password or "").encode("utf-8"), salt, int(iterations))


def _cfg_auth(cfg: dict[str, Any]) -> dict[str, Any]:
    a = cfg.get("app_auth")
    return a if isinstance(a, dict) else {}


def _cfg_pwd(a: dict[str, Any]) -> dict[str, Any]:
    p = a.get("password")
    return p if isinstance(p, dict) else {}


def _cfg_session(a: dict[str, Any]) -> dict[str, Any]:
    # Legacy: single-session storage backwards compatibility
    s = a.get("session")
    return s if isinstance(s, dict) else {}


def _cfg_sessions(a: dict[str, Any]) -> list[dict[str, Any]]:
    s = a.get("sessions")
    if not isinstance(s, list):
        return []
    return [x for x in s if isinstance(x, dict)]


def _legacy_session_as_entry(a: dict[str, Any]) -> dict[str, Any] | None:
    s = _cfg_session(a)
    token_hash = str(s.get("token_hash") or "").strip()
    exp = int(s.get("expires_at") or 0)
    if not token_hash or not _is_sha256_hex(token_hash) or exp <= 0:
        return None
    created_at = int(a.get("last_login_at") or 0) or max(1, exp - AUTH_TTL_SEC)
    return {
        "id": "legacy",
        "token_hash": token_hash,
        "created_at": created_at,
        "expires_at": exp,
    }


def _iter_sessions(a: dict[str, Any]) -> list[dict[str, Any]]:
    sessions = _cfg_sessions(a)
    legacy = _legacy_session_as_entry(a)
    if legacy is None:
        return sessions
    if any(str(x.get("token_hash") or "") == str(legacy.get("token_hash") or "") for x in sessions):
        return sessions
    return sessions + [legacy]


def _prune_sessions(sessions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    now = _now()
    keep: list[dict[str, Any]] = []
    for s in sessions:
        exp = int(s.get("expires_at") or 0)
        th = str(s.get("token_hash") or "").strip()
        if th and _is_sha256_hex(th) and exp > now:
            keep.append(s)
    keep.sort(key=lambda x: int(x.get("created_at") or 0) or int(x.get("expires_at") or 0))
    if len(keep) > MAX_SESSIONS:
        keep = keep[-MAX_SESSIONS:]
    return keep


def _sync_legacy_session(a: dict[str, Any], sessions: list[dict[str, Any]]) -> None:
    s = a.setdefault("session", {})
    if not isinstance(s, dict):
        s = {}
        a["session"] = s
    if not sessions:
        s["token_hash"] = ""
        s["expires_at"] = 0
        return
    last = sessions[-1]
    s["token_hash"] = str(last.get("token_hash") or "")
    s["expires_at"] = int(last.get("expires_at") or 0)


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


def _find_session(a: dict[str, Any], token: str | None) -> dict[str, Any] | None:
    t = (token or "").strip()
    if not t:
        return None
    th = _sha256_hex(t)
    now = _now()
    for s in _iter_sessions(a):
        exp = int(s.get("expires_at") or 0)
        want = str(s.get("token_hash") or "").strip()
        if not want or exp <= now:
            continue
        if not _is_sha256_hex(want):
            continue
        if _digest_eq(th, want):
            return s
    return None


def is_authenticated(cfg: dict[str, Any], token: str | None) -> bool:
    if not auth_required(cfg):
        return True
    a = _cfg_auth(cfg)
    return _find_session(a, token) is not None


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


def _issue_session(cfg: dict[str, Any], request: Request) -> tuple[str, int]:
    token = secrets.token_urlsafe(32)
    exp = _now() + AUTH_TTL_SEC
    a = cfg.setdefault("app_auth", {})
    if not isinstance(a, dict):
        a = {}
        cfg["app_auth"] = a

    sessions = _prune_sessions(_iter_sessions(a))
    ip = getattr(getattr(request, "client", None), "host", "") or ""
    ua = str(request.headers.get("user-agent") or "")[:240]
    now = _now()
    sessions.append(
        {
            "id": secrets.token_hex(8),
            "token_hash": _sha256_hex(token),
            "created_at": now,
            "expires_at": exp,
            "ip": ip,
            "ua": ua,
        }
    )
    sessions = _prune_sessions(sessions)
    a["sessions"] = sessions
    _sync_legacy_session(a, sessions)
    a["last_login_at"] = now
    return token, exp


def _drop_session(cfg: dict[str, Any], token: str | None) -> None:
    a = cfg.get("app_auth")
    if not isinstance(a, dict):
        return
    t = (token or "").strip()
    if not t:
        return
    th = _sha256_hex(t)
    sessions = _prune_sessions(_iter_sessions(a))
    kept = [s for s in sessions if not _digest_eq(str(s.get("token_hash") or ""), th)]
    a["sessions"] = kept
    _sync_legacy_session(a, kept)


def _clear_sessions(cfg: dict[str, Any]) -> None:
    a = cfg.get("app_auth")
    if not isinstance(a, dict):
        return
    a["sessions"] = []
    _sync_legacy_session(a, [])


def _set_cookie(resp: Response, token: str, exp: int, request: Request) -> None:
    xfp = str(request.headers.get("x-forwarded-proto") or request.headers.get("x-forwarded-protocol") or "").lower()
    secure = (str(request.url.scheme).lower() == "https") or (xfp == "https")
    resp.set_cookie(
        COOKIE_NAME,
        token,
        max_age=max(1, exp - _now()),
        expires=exp,
        path="/",
        httponly=True,
        samesite="lax",
        secure=secure,
    )


def _del_cookie(resp: Response, request: Request) -> None:
    xfp = str(request.headers.get("x-forwarded-proto") or request.headers.get("x-forwarded-protocol") or "").lower()
    secure = (str(request.url.scheme).lower() == "https") or (xfp == "https")
    resp.delete_cookie(COOKIE_NAME, path="/", samesite="lax", secure=secure)


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
        const next = (new URLSearchParams(location.search)).get('next') || '/';
          const safe = (next.startsWith('/') && !next.startsWith('//')) ? next : '/';
          location.href = safe;
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
    s = _find_session(a, token)
    return JSONResponse(
        {
            "enabled": enabled,
            "configured": configured,
            "username": str(a.get("username") or "") if enabled else "",
            "authenticated": (s is not None) if auth_required(cfg) else True,
            "session_expires_at": int((s or {}).get("expires_at") or 0),
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

    token, exp = _issue_session(cfg, req)
    save_config(cfg)
    resp = JSONResponse({"ok": True, "expires_at": exp}, headers={"Cache-Control": "no-store"})
    _set_cookie(resp, token, exp, req)
    return resp


@router.post("/logout")
def api_logout(request: Request) -> JSONResponse:
    cfg = load_config()
    token = request.cookies.get(COOKIE_NAME)
    _drop_session(cfg, token)
    save_config(cfg)
    resp = JSONResponse({"ok": True}, headers={"Cache-Control": "no-store"})
    _del_cookie(resp, request)
    return resp


@router.post("/logout-all")
def api_logout_all(request: Request) -> JSONResponse:
    cfg = load_config()
    _clear_sessions(cfg)
    save_config(cfg)
    resp = JSONResponse({"ok": True}, headers={"Cache-Control": "no-store"})
    _del_cookie(resp, request)
    return resp


@router.post("/apply-now")
def api_apply_now(request: Request, payload: dict[str, Any] | None = Body(None)) -> JSONResponse:
    cfg = load_config()
    token = request.cookies.get(COOKIE_NAME)

    if auth_required(cfg) and not is_authenticated(cfg, token):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})

    _clear_sessions(cfg)
    save_config(cfg)

    def _kill() -> None:
        os._exit(0)

    threading.Timer(0.75, _kill).start()

    resp = JSONResponse({"ok": True}, headers={"Cache-Control": "no-store"})
    _del_cookie(resp, request)
    return resp


@router.post("/credentials")
def api_set_credentials(request: Request, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    req = request
    cfg = load_config()
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
        _clear_sessions(cfg)
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
    _clear_sessions(cfg)

    token2, exp2 = _issue_session(cfg, req)
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
        token = request.cookies.get(COOKIE_NAME)
        _drop_session(cfg, token)
        save_config(cfg)
        resp = RedirectResponse(url="/login" if auth_required(cfg) else "/")
        _del_cookie(resp, request)
        return resp
