# /api/appAuthAPI.py
# CrossWatch - UI authentication API
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from typing import Any

import base64
import hashlib
import ipaddress
import hmac
import os
import secrets
import threading
import time
from urllib.parse import urlsplit

from fastapi import APIRouter, Body, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from cw_platform.config_base import load_config, save_config

__all__ = [
    "router",
    "COOKIE_NAME",
    "AUTH_TTL_SEC",
    "MIN_PASSWORD_LENGTH",
    "auth_required",
    "is_authenticated",
    "reset_pending",
    "setup_lock_required",
    "register_app_auth",
]

COOKIE_NAME = "cw_auth"
AUTH_TTL_SEC = 30 * 24 * 60 * 60
MAX_SESSIONS = 10
MAX_REMEMBER_SESSION_DAYS = 365
DEFAULT_REMEMBER_ME_DAYS = 60
MIN_PASSWORD_LENGTH = 8
FORGOT_HELP_URL = "https://wiki.crosswatch.app/"

_LOGIN_FAILS: dict[str, dict[str, Any]] = {}
_TRUSTED_PROXY_CACHE: dict[str, Any] = {"at": 0.0, "nets": []}

def _trusted_proxy_nets() -> list[ipaddress._BaseNetwork]:
    now = time.time()
    try:
        if (now - float(_TRUSTED_PROXY_CACHE.get("at") or 0.0)) < 5.0:
            nets = _TRUSTED_PROXY_CACHE.get("nets") or []
            if isinstance(nets, list):
                return nets
    except Exception:
        pass

    raw: Any = []
    try:
        cfg = load_config()
        sec = cfg.get("security") if isinstance(cfg, dict) else {}
        if not isinstance(sec, dict):
            sec = {}
        raw = sec.get("trusted_proxies") or []
    except Exception:
        raw = []

    items = raw if isinstance(raw, (list, tuple, set)) else ([raw] if raw else [])
    nets: list[ipaddress._BaseNetwork] = []
    for it in items:
        s = str(it or "").strip()
        if not s:
            continue
        try:
            if "/" in s:
                nets.append(ipaddress.ip_network(s, strict=False))
            else:
                ip = ipaddress.ip_address(s)
                bits = 32 if ip.version == 4 else 128
                nets.append(ipaddress.ip_network(f"{ip}/{bits}", strict=False))
        except Exception:
            continue

    try:
        _TRUSTED_PROXY_CACHE["at"] = now
        _TRUSTED_PROXY_CACHE["nets"] = nets
    except Exception:
        pass

    return nets


def _is_trusted_proxy_request(request: Request) -> bool:
    host = getattr(getattr(request, "client", None), "host", "") or ""
    if not host:
        return False
    try:
        ip = ipaddress.ip_address(host)
    except Exception:
        return False
    for net in _trusted_proxy_nets():
        try:
            if ip in net:
                return True
        except Exception:
            continue
    return False


def _effective_client_ip(request: Request) -> str:
    peer = getattr(getattr(request, "client", None), "host", "") or "local"
    if not _is_trusted_proxy_request(request):
        return peer

    xff = str(request.headers.get("x-forwarded-for") or "").strip()
    if not xff:
        return peer
    cand = xff.split(",")[0].strip()
    try:
        ipaddress.ip_address(cand)
        return cand
    except Exception:
        return peer


def _effective_scheme_is_https(request: Request) -> bool:
    scheme = str(request.url.scheme).lower()
    if scheme == "https":
        return True

    if _is_trusted_proxy_request(request):
        xf = str(request.headers.get("x-forwarded-proto") or "").split(",", 1)[0].strip().lower()
        if xf == "https":
            return True

    return False

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


def _cfg_remember_session_enabled(a: dict[str, Any]) -> bool:
    return bool(a.get("remember_session_enabled"))


def _cfg_remember_session_days(a: dict[str, Any]) -> int:
    try:
        days = int(a.get("remember_session_days") or 30)
    except Exception:
        days = 30
    if days < 1:
        days = 1
    if days > MAX_REMEMBER_SESSION_DAYS:
        days = MAX_REMEMBER_SESSION_DAYS
    return days


def _session_ttl_sec(a: dict[str, Any]) -> int:
    return _cfg_remember_session_days(a) * 24 * 60 * 60


def _legacy_session_as_entry(a: dict[str, Any]) -> dict[str, Any] | None:
    s = _cfg_session(a)
    token_hash = str(s.get("token_hash") or "").strip()
    exp = int(s.get("expires_at") or 0)
    if not token_hash or not _is_sha256_hex(token_hash) or exp <= 0:
        return None
    created_at = int(a.get("last_login_at") or 0) or max(1, exp - _session_ttl_sec(a))
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


def reset_pending(cfg: dict[str, Any]) -> bool:
    a = _cfg_auth(cfg)
    return bool(a.get("reset_required"))


def _norm_version_text(v: Any) -> str:
    raw = str(v or "").strip()
    if raw.lower().startswith("v"):
        raw = raw[1:]
    return raw


def _version_key(v: Any) -> tuple[int, ...]:
    raw = _norm_version_text(v)
    if not raw:
        return (0,)
    out: list[int] = []
    for part in raw.split("."):
        try:
            out.append(int(part))
        except Exception:
            out.append(0)
    return tuple(out or [0])


def _current_version_text() -> str:
    try:
        from api.versionAPI import CURRENT_VERSION as _CURRENT_VERSION

        return _norm_version_text(_CURRENT_VERSION)
    except Exception:
        return _norm_version_text(os.getenv("APP_VERSION") or "0.0.0")


def _config_needs_upgrade(cfg: dict[str, Any]) -> bool:
    return _version_key(_current_version_text()) > _version_key(cfg.get("version"))


def setup_lock_required(cfg: dict[str, Any]) -> bool:
    if reset_pending(cfg):
        return True
    return (not auth_required(cfg)) and _config_needs_upgrade(cfg)


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
    ip = _effective_client_ip(request)
    rec = _LOGIN_FAILS.get(ip) or {"n": 0, "until": 0}
    until = int(rec.get("until") or 0)
    if until > _now():
        return False, max(1, until - _now())
    return True, 0


def _login_lockout_seconds(n: int) -> int:
    if n >= 10:
        return 10 * 60
    if n >= 6:
        return 5 * 60
    if n >= 3:
        return 60
    return 0


def _rate_limit_fail(request: Request) -> dict[str, int]:
    ip = _effective_client_ip(request)
    rec = _LOGIN_FAILS.get(ip) or {"n": 0, "until": 0}
    n = int(rec.get("n") or 0) + 1
    backoff = _login_lockout_seconds(n)
    until = (_now() + backoff) if backoff > 0 else 0
    _LOGIN_FAILS[ip] = {"n": n, "until": until}
    return {"n": n, "retry_after": backoff}


def _rate_limit_state(request: Request) -> dict[str, int]:
    ip = _effective_client_ip(request)
    rec = _LOGIN_FAILS.get(ip) or {"n": 0, "until": 0}
    until = int(rec.get("until") or 0)
    retry_after = max(0, until - _now()) if until > 0 else 0
    return {"n": int(rec.get("n") or 0), "retry_after": retry_after}


def _rate_limit_reset(request: Request) -> None:
    ip = _effective_client_ip(request)
    _LOGIN_FAILS.pop(ip, None)


def _login_error_payload(*, error: str, attempts: int, retry_after: int = 0) -> dict[str, Any]:
    return {
        "ok": False,
        "error": error,
        "attempts": attempts,
        "retry_after": max(0, int(retry_after or 0)),
        "show_help_banner": attempts >= 3,
        "forgot_help_url": FORGOT_HELP_URL if attempts >= 3 else "",
    }


def _issue_session(cfg: dict[str, Any], request: Request) -> tuple[str, int]:
    token = secrets.token_urlsafe(32)
    a = cfg.setdefault("app_auth", {})
    if not isinstance(a, dict):
        a = {}
        cfg["app_auth"] = a
    exp = _now() + _session_ttl_sec(a)

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


def _clear_setup_autogen_flag(cfg: dict[str, Any]) -> None:
    try:
        ui = cfg.get("ui")
        if isinstance(ui, dict):
            ui.pop("_autogen", None)
    except Exception:
        pass


def _mark_upgrade_pending_if_needed(cfg: dict[str, Any]) -> None:
    try:
        if not _config_needs_upgrade(cfg):
            return
        ui = cfg.get("ui")
        if not isinstance(ui, dict):
            ui = {}
            cfg["ui"] = ui
        ui["_pending_upgrade_from_version"] = str(cfg.get("version") or "").strip()
    except Exception:
        pass


def _effective_host(request: Request) -> str:
    if _is_trusted_proxy_request(request):
        xfh = str(request.headers.get("x-forwarded-host") or "").split(",", 1)[0].strip()
        if xfh:
            return xfh
    return str(request.headers.get("host") or request.url.netloc or "").strip()


def _forwarded_header_param(request: Request, name: str) -> str:
    raw = str(request.headers.get("forwarded") or "").strip()
    if not raw:
        return ""
    first = raw.split(",", 1)[0].strip()
    if not first:
        return ""
    want = str(name or "").strip().lower()
    if not want:
        return ""
    for part in first.split(";"):
        key, sep, value = part.partition("=")
        if not sep or key.strip().lower() != want:
            continue
        return value.strip().strip('"').strip()
    return ""


def _forwarded_proto_hint(request: Request) -> str:
    xf = str(request.headers.get("x-forwarded-proto") or "").split(",", 1)[0].strip().lower()
    if xf in {"http", "https"}:
        return xf
    forwarded = _forwarded_header_param(request, "proto").lower()
    return forwarded if forwarded in {"http", "https"} else ""


def _origin_host_candidates(request: Request) -> list[str]:
    raw_hosts = [
        request.headers.get("host"),
        request.url.netloc,
        str(request.headers.get("x-forwarded-host") or "").split(",", 1)[0].strip(),
        _forwarded_header_param(request, "host"),
    ]
    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_hosts:
        host = str(raw or "").strip().lower()
        if not host or host in seen:
            continue
        seen.add(host)
        out.append(host)
    return out


def _normalize_origin(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    try:
        parsed = urlsplit(text)
    except Exception:
        return ""
    if not parsed.scheme or not parsed.netloc:
        return ""
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"


def _expected_origin(request: Request) -> str:
    host = _effective_host(request)
    if not host:
        return ""
    scheme = "https" if _effective_scheme_is_https(request) else "http"
    return _normalize_origin(f"{scheme}://{host}")


def _origin_allowed(request: Request) -> bool:
    got = _normalize_origin(request.headers.get("origin"))
    if not got:
        return True
    want = _expected_origin(request)
    if not want:
        return False
    if hmac.compare_digest(got.encode("utf-8"), want.encode("utf-8")):
        return True

    try:
        parsed = urlsplit(got)
        origin_host = str(parsed.netloc or "").strip().lower()
        origin_scheme = str(parsed.scheme or "").strip().lower()
        if (
            origin_host
            and origin_host in _origin_host_candidates(request)
            and origin_scheme
            and origin_scheme == _forwarded_proto_hint(request)
        ):
            return True
    except Exception:
        pass

    return False


def _origin_blocked_response() -> JSONResponse:
    return JSONResponse(
        {"ok": False, "error": "Origin mismatch"},
        status_code=403,
        headers={"Cache-Control": "no-store"},
    )


def _set_cookie(resp: Response, token: str, exp: int, request: Request, *, persistent: bool) -> None:
    # Secure cookie only when CW itself is running on HTTPS.
    secure = _effective_scheme_is_https(request)
    kwargs: dict[str, Any] = {
        "path": "/",
        "httponly": True,
        "samesite": "lax",
        "secure": secure,
    }
    if persistent:
        kwargs["max_age"] = max(1, exp - _now())
        kwargs["expires"] = exp
    resp.set_cookie(COOKIE_NAME, token, **kwargs)

def _del_cookie(resp: Response, request: Request) -> None:
    secure = _effective_scheme_is_https(request)
    resp.delete_cookie(COOKIE_NAME, path="/", samesite="lax", secure=secure)


_LOGIN_PAGE_CSS = """
:root{
  --cw-bg:#06111d;--cw-panel:rgba(7,16,28,.82);--cw-panel-strong:rgba(8,17,30,.94);
  --cw-border:rgba(177,146,255,.16);--cw-border-strong:rgba(165,126,255,.30);
  --cw-text:#edf6ff;--cw-soft:rgba(214,231,249,.72);--cw-accent:#8c6dff;--cw-accent-2:#c08cff;
  --cw-warn-bg:rgba(255,178,102,.10);--cw-warn-border:rgba(255,192,120,.24);
  --cw-danger-bg:rgba(255,98,114,.12);--cw-danger-border:rgba(255,133,146,.22);
  --cw-shadow:0 32px 80px rgba(0,0,0,.42);
}
*{box-sizing:border-box}
html,body{min-height:100%}
body{
  margin:0;display:grid;place-items:center;min-height:100vh;color:var(--cw-text);
  font-family:"Segoe UI Variable","Avenir Next","Trebuchet MS",sans-serif;
  background:
    radial-gradient(900px circle at 8% 10%, rgba(140,109,255,.18), transparent 42%),
    radial-gradient(780px circle at 92% 18%, rgba(192,140,255,.18), transparent 40%),
    radial-gradient(760px circle at 50% 110%, rgba(112,92,214,.16), transparent 42%),
    linear-gradient(180deg,#07111b 0%,#03070c 100%);
  overflow:hidden;
}
body::before{
  content:"";position:fixed;inset:0;pointer-events:none;opacity:.28;background-size:44px 44px;
  background-image:
    linear-gradient(rgba(255,255,255,.03) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255,255,255,.03) 1px, transparent 1px);
  mask-image:radial-gradient(circle at center, rgba(0,0,0,.85), transparent 82%);
}
.cw-login-shell{
  width:min(1040px,calc(100vw - 32px));display:grid;
  grid-template-columns:minmax(0,1.05fr) minmax(360px,.95fr);
  border:1px solid var(--cw-border);border-radius:28px;overflow:hidden;
  background:linear-gradient(135deg, rgba(7,15,27,.90), rgba(4,10,20,.78));
  box-shadow:var(--cw-shadow);backdrop-filter:blur(16px) saturate(135%);
  -webkit-backdrop-filter:blur(16px) saturate(135%);
}
.cw-hero{
  position:relative;display:flex;flex-direction:column;padding:34px 34px 30px;
  background:
    radial-gradient(420px circle at 14% 10%, rgba(140,109,255,.22), transparent 42%),
    radial-gradient(460px circle at 80% 28%, rgba(192,140,255,.16), transparent 38%),
    linear-gradient(180deg, rgba(255,255,255,.04), rgba(255,255,255,.01));
  border-right:1px solid rgba(255,255,255,.06);
}
.cw-hero::after{
  content:"";position:absolute;right:26px;bottom:26px;width:188px;height:188px;border-radius:36px;
  background:url("/assets/img/CROSSWATCH.svg") center/62% no-repeat, linear-gradient(135deg, rgba(140,109,255,.18), rgba(192,140,255,.05));
  border:1px solid rgba(255,255,255,.06);opacity:.96;transform:rotate(14deg);pointer-events:none;
  box-shadow:inset 0 1px 0 rgba(255,255,255,.04);filter:drop-shadow(0 20px 40px rgba(0,0,0,.22));
}
.cw-mark{display:flex;align-items:center;margin-top:6px}
.cw-mark img{width:min(360px,100%);height:auto;display:block;filter:drop-shadow(0 18px 30px rgba(0,0,0,.34))}
.cw-hero h1{margin:28px 0 12px;max-width:12ch;font-size:clamp(34px,4.6vw,56px);line-height:.98;letter-spacing:-.04em;font-weight:900}
.cw-hero p{margin:0;max-width:44ch;color:var(--cw-soft);font-size:15px;line-height:1.65}
.cw-metrics{display:grid;grid-template-columns:1fr;gap:12px;max-width:320px;margin-top:auto;padding-top:32px}
.cw-login{
  display:grid;align-content:center;gap:18px;padding:34px;
  background:linear-gradient(180deg, rgba(6,12,22,.94), rgba(5,10,18,.98));
}
.cw-login-head,.cw-form,.cw-field,.cw-help-copy{display:grid}
.cw-login-head{gap:8px}.cw-form{gap:14px}.cw-field{gap:7px}.cw-help-copy{gap:3px;min-width:0}
.cw-login-kicker,.cw-field label,.cw-help-kicker{
  font-weight:800;text-transform:uppercase;letter-spacing:.08em
}
.cw-login-kicker{font-size:12px;letter-spacing:.12em;color:rgba(210,231,251,.64)}
.cw-login h2{margin:0;font-size:30px;line-height:1.05;letter-spacing:-.03em;font-weight:900}
.cw-login .sub{margin:0;color:var(--cw-soft);font-size:14px;line-height:1.55}
.cw-banner,.cw-msg{
  display:none;padding:13px 14px;border:1px solid transparent;border-radius:18px;
  font-size:13px;line-height:1.55;
}
.cw-banner.show,.cw-msg.show{display:block}
.cw-banner{background:linear-gradient(180deg, var(--cw-warn-bg), rgba(255,255,255,.02));border-color:var(--cw-warn-border);color:#ffe9cf}
.cw-banner a{color:#fff3de;font-weight:800}
.cw-msg{background:linear-gradient(180deg, var(--cw-danger-bg), rgba(255,255,255,.02));border-color:var(--cw-danger-border);color:#ffd9dd}
.cw-field label{font-size:12px;color:rgba(231,242,255,.86)}
.cw-field input{
  width:100%;min-height:52px;padding:0 16px;border:1px solid rgba(255,255,255,.10);border-radius:18px;
  background:rgba(2,8,19,.76);color:var(--cw-text);font:inherit;
  box-shadow:inset 0 1px 0 rgba(255,255,255,.03);
  transition:border-color .18s ease, box-shadow .18s ease, background .18s ease, transform .18s ease;
}
.cw-field input:focus{
  outline:none;transform:translateY(-1px);border-color:var(--cw-border-strong);background:rgba(4,10,22,.94);
  box-shadow:0 0 0 4px rgba(140,109,255,.14), inset 0 1px 0 rgba(255,255,255,.04);
}
.cw-actions{display:grid;grid-template-columns:auto minmax(0,1fr);align-items:start;gap:14px}
.cw-action-primary{min-width:0}
.cw-action-plex{display:grid;gap:12px;min-width:0}
.cw-remember{
  display:flex;align-items:flex-start;gap:10px;padding:10px 12px;border:1px solid rgba(255,255,255,.08);border-radius:16px;
  background:rgba(255,255,255,.03);color:var(--cw-soft);
}
.cw-remember input{width:18px;height:18px;margin-top:2px;flex:0 0 auto;accent-color:var(--cw-accent)}
.cw-remember b{display:block;color:var(--cw-text);font-size:13px}
.cw-remember span{display:block;margin-top:3px;font-size:12px;line-height:1.45}
.cw-login .btn{
  min-width:144px;min-height:52px;border:1px solid rgba(166,126,255,.30);border-radius:18px;
  background:linear-gradient(135deg, rgba(123,95,255,.96), rgba(186,96,255,.88));color:#f5f9ff;
  font-weight:900;font-size:16px;letter-spacing:.01em;
  box-shadow:0 18px 36px rgba(106,66,255,.30), inset 0 1px 0 rgba(255,255,255,.14);
  transition:transform .14s ease, box-shadow .18s ease, filter .18s ease;
}
.cw-login .btn:hover{transform:translateY(-1px);filter:brightness(1.04)}
.cw-login .btn:disabled{opacity:.72;cursor:progress;transform:none;box-shadow:none}
.cw-plex-btn{
  position:relative;overflow:hidden;width:100%;min-height:58px;
  border:1px solid rgba(255,203,103,.32)!important;border-radius:28px!important;
  background:
    linear-gradient(135deg, rgba(255,187,24,.98), rgba(206,132,0,.94))!important;
  color:#fff7eb!important;
  box-shadow:0 16px 34px rgba(156,97,0,.28), inset 0 1px 0 rgba(255,255,255,.18)!important;
}
.cw-plex-btn::before{
  content:"";position:absolute;inset:-16% -2% -16% auto;width:170px;pointer-events:none;opacity:.22;
  background:
    linear-gradient(135deg, transparent 0 38%, rgba(111,68,0,.46) 38% 53%, transparent 53% 60%, rgba(111,68,0,.36) 60% 75%, transparent 75%);
  transform:skewX(-10deg);
}
.cw-plex-btn::after{
  content:"";position:absolute;inset:0;border-radius:inherit;pointer-events:none;
  background:linear-gradient(180deg, rgba(255,255,255,.12), transparent 34%, transparent 72%, rgba(93,57,0,.10));
}
.cw-plex-btn span{position:relative;z-index:1}
.cw-plex-btn:hover{filter:brightness(1.05)!important}
.cw-plex-copy{
  margin:12px 2px 0;color:rgba(245,225,191,.82);font-size:13px;line-height:1.5;
}
.cw-help-link{
  display:flex;align-items:center;justify-content:space-between;gap:12px;width:100%;
  padding:14px 16px;border:1px solid rgba(255,255,255,.08);border-radius:18px;
  background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.02));
  color:rgba(236,244,255,.92);text-decoration:none;
  box-shadow:inset 0 1px 0 rgba(255,255,255,.03);
  transition:border-color .16s ease, transform .16s ease, filter .16s ease;
}
.cw-help-link:hover{transform:translateY(-1px);filter:brightness(1.04);border-color:rgba(146,118,255,.24)}
.cw-help-kicker{font-size:11px;letter-spacing:.14em;color:rgba(228,234,255,.54)}
.cw-help-sub{font-size:12.5px;line-height:1.5;font-weight:600;color:var(--cw-soft)}
.cw-help-icon{
  width:34px;height:34px;display:grid;place-items:center;flex:0 0 auto;border:1px solid rgba(166,126,255,.18);border-radius:12px;
  background:linear-gradient(135deg,rgba(123,95,255,.18),rgba(186,96,255,.12));color:#eef3ff;
}
.cw-help-icon svg{width:18px;height:18px;display:block}
@media (max-width:860px){
  .cw-login-shell{grid-template-columns:1fr}
  .cw-hero{padding:28px 24px 22px;border-right:0;border-bottom:1px solid rgba(255,255,255,.06)}
  .cw-hero h1{max-width:none}
  .cw-metrics{margin-top:28px;padding-top:0}
  .cw-login{padding:24px}
}
@media (max-width:560px){
  .cw-login-shell{width:min(100vw - 18px,1040px);border-radius:24px}
  .cw-hero,.cw-login{padding:20px}
  .cw-actions{grid-template-columns:1fr;align-items:stretch}
  .cw-action-primary,.cw-action-plex{min-width:100%}
  .cw-login .btn{width:100%}
}
"""


def _login_html(username: str, *, plex_sso_available: bool = False) -> str:
    u = (username or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    plex_html = ""
    if plex_sso_available:
        plex_html = """
        <div class="cw-action-plex">
          <button class="btn cw-plex-btn" id="go-plex" type="button"><span>Sign in with Plex</span></button>
          <p class="cw-plex-copy">Use your linked Plex account, then return here to finish sign-in.</p>
        </div>
        """
    return f"""<!doctype html>
<html lang=\"en\"><head>
  <meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">
  <title>Sign in | CrossWatch</title>
  <link rel=\"icon\" type=\"image/svg+xml\" href=\"/favicon.svg\">
  <link rel=\"stylesheet\" href=\"/assets/crosswatch.css\">
  <style>{_LOGIN_PAGE_CSS}</style>
</head><body>
  <div class=\"cw-login-shell\">
    <section class=\"cw-hero\" aria-hidden=\"true\">
      <div class=\"cw-mark\">
        <img src=\"/assets/img/CrossWatch.png\" alt=\"CrossWatch\">
      </div>
      <h1>Sign in to your sync hub</h1>
      <p>CrossWatch keeps your media world synced, simple and self hosted.</p>
      <div class=\"cw-metrics\">
        <a class=\"cw-help-link\" href=\"https://wiki.crosswatch.app/\" target=\"_blank\" rel=\"noopener noreferrer\">
          <span class=\"cw-help-copy\">
            <span class=\"cw-help-kicker\">Documentation</span>
            <span class=\"cw-help-sub\">Setup guides and troubleshooting help.</span>
          </span>
          <span class=\"cw-help-icon\" aria-hidden=\"true\">
            <svg viewBox=\"0 0 24 24\" fill=\"none\" xmlns=\"http://www.w3.org/2000/svg\">
              <path d=\"M6 5.75C6 4.78 6.78 4 7.75 4H18a1 1 0 0 1 1 1v12.25A2.75 2.75 0 0 1 16.25 20H8.75A2.75 2.75 0 0 1 6 17.25V5.75Z\" stroke=\"currentColor\" stroke-width=\"1.8\" stroke-linejoin=\"round\"/>
              <path d=\"M9 8h6M9 11h6M9 14h4\" stroke=\"currentColor\" stroke-width=\"1.8\" stroke-linecap=\"round\"/>
              <path d=\"M6.25 17.5H16\" stroke=\"currentColor\" stroke-width=\"1.8\" stroke-linecap=\"round\"/>
            </svg>
          </span>
        </a>
      </div>
    </section>
    <section class=\"cw-login\">
      <div class=\"cw-login-head\">
        <div class=\"cw-login-kicker\">Authentication</div>
        <h2>Welcome back</h2>
        <p class=\"sub\">Use your local CrossWatch admin credentials to continue.</p>
      </div>
      <div id=\"help\" class=\"cw-banner\" role=\"status\" aria-live=\"polite\"></div>
      <div id=\"msg\" class=\"cw-msg\" role=\"alert\" aria-live=\"assertive\"></div>
      <div class=\"cw-form\">
        <div class=\"cw-field\">
          <label for=\"u\">Username</label>
          <input id=\"u\" name=\"username\" autocomplete=\"username\" value=\"{u}\">
        </div>
        <div class=\"cw-field\">
          <label for=\"p\">Password</label>
          <input id=\"p\" name=\"password\" type=\"password\" autocomplete=\"current-password\">
        </div>
        <label class=\"cw-remember\" for=\"remember\">
          <input id=\"remember\" name=\"remember\" type=\"checkbox\">
          <span><b>Remember me</b><span>Keep this browser signed in for up to {DEFAULT_REMEMBER_ME_DAYS} days.</span></span>
        </label>
        <div class=\"cw-actions\">
          <div class=\"cw-action-primary\">
            <button class=\"btn acc\" id=\"go\">Sign in</button>
          </div>
          {plex_html}
        </div>
      </div>
    </section>
  </div>
  <script>
    const $=(id)=>document.getElementById(id);
    const msg=$('msg');
    const help=$('help');
    const btn=$('go');
    const plexBtn=$('go-plex');
    let plexPolling=false;
    function setMsg(text){{
      msg.textContent=text||'';
      msg.classList.toggle('show',!!text);
    }}
    function nextUrl(){{
      const next = (new URLSearchParams(location.search)).get('next') || '/';
      return (next.startsWith('/') && !next.startsWith('//')) ? next : '/';
    }}
    function setHelp(data){{
      const url=(data&&data.forgot_help_url)||'https://wiki.crosswatch.app/';
      const on=!!(data&&data.show_help_banner);
      help.innerHTML=on?`Forget username/password? Visit <a href="${{url}}" target="_blank" rel="noopener noreferrer">${{url}}</a>`:'';
      help.classList.toggle('show',on);
    }}
    async function login(){{
      setMsg('');
      const u=$('u').value.trim();
      const p=$('p').value;
      const remember=$('remember')?.checked===true;
      btn.disabled=true;
      btn.textContent='Signing in...';
      try{{
        const r=await fetch('/api/app-auth/login',{{method:'POST',headers:{{'Content-Type':'application/json'}},credentials:'same-origin',body:JSON.stringify({{username:u,password:p,remember_me:remember}})}});
        const data=await r.json().catch(()=>null);
        if(!r.ok || !data || !data.ok){{
          setHelp(data);
          const base=(data && data.error) ? data.error : ('Login failed ('+r.status+')');
          const retry=(data && Number.isFinite(data.retry_after) && data.retry_after>0) ? ` Login paused for ${{data.retry_after}}s.` : '';
          setMsg(base + retry);
          return;
        }}
        setHelp(null);
        location.href = nextUrl();
      }}catch(e){{
        setMsg('Login failed');
      }}finally{{
        btn.disabled=false;
        btn.textContent='Sign in';
      }}
    }}
    async function startPlex(){{
      if(plexPolling) return;
      setMsg('');
      setHelp(null);
      plexPolling=true;
      const remember=$('remember')?.checked===true;
      const popup=window.open('about:blank','cw_plex_auth','width=620,height=760,popup=yes');
      if(plexBtn){{
        plexBtn.disabled=true;
        plexBtn.textContent='Waiting for Plex...';
      }}
      try{{
        const r=await fetch('/api/app-auth/plex/start',{{method:'POST',headers:{{'Content-Type':'application/json'}},credentials:'same-origin',body:JSON.stringify({{remember_me:remember}})}});
        const data=await r.json().catch(()=>null);
        if(!r.ok || !data || !data.ok || !data.state || !data.auth_url){{
          if(popup && !popup.closed) popup.close();
          setMsg((data&&data.error)||('Plex sign-in failed ('+r.status+')'));
          return;
        }}
        if(popup && !popup.closed) popup.location.href=data.auth_url;
        else window.open(data.auth_url,'_blank','noopener,noreferrer');
        for(;;){{
          await new Promise(resolve=>setTimeout(resolve, 2000));
          const pr=await fetch('/api/app-auth/plex/check',{{method:'POST',headers:{{'Content-Type':'application/json'}},credentials:'same-origin',body:JSON.stringify({{state:data.state}})}});
          const pd=await pr.json().catch(()=>null);
          if(pr.ok && pd && pd.ok && pd.pending===true) continue;
          if(!pr.ok || !pd || !pd.ok){{
            if(popup && !popup.closed) popup.close();
            setMsg((pd&&pd.error)||('Plex sign-in failed ('+pr.status+')'));
            return;
          }}
          if(popup && !popup.closed) popup.close();
          location.href = nextUrl();
          return;
        }}
      }}catch(e){{
        if(popup && !popup.closed) popup.close();
        setMsg('Plex sign-in failed');
      }}finally{{
        plexPolling=false;
        if(plexBtn){{
          plexBtn.disabled=false;
          plexBtn.textContent='Sign in with Plex';
        }}
      }}
    }}
    $('go').addEventListener('click', login);
    $('p').addEventListener('keydown', (e)=>{{ if(e.key==='Enter') login(); }});
    $('u').addEventListener('keydown', (e)=>{{ if(e.key==='Enter') login(); }});
    plexBtn?.addEventListener('click', startPlex);
  </script>
</body></html>"""


router = APIRouter(prefix="/api/app-auth", tags=["app-auth"])


@router.get("/status")
def api_status(request: Request) -> JSONResponse:
    cfg = load_config()
    a = _cfg_auth(cfg)
    p = _cfg_pwd(a)
    try:
        from services import authPlex

        plex_st = authPlex.get_status(cfg)
    except Exception:
        plex_st = {"enabled": False, "linked": False}
    configured = bool(str(a.get("username") or "").strip() and str(p.get("hash") or "").strip() and str(p.get("salt") or "").strip())
    enabled = bool(a.get("enabled"))
    pending_setup = setup_lock_required(cfg)
    token = request.cookies.get(COOKIE_NAME)
    s = _find_session(a, token)
    return JSONResponse(
        {
            "enabled": enabled,
            "configured": configured,
            "username": str(a.get("username") or "") if (enabled and s is not None) else "",
            "authenticated": (s is not None) if auth_required(cfg) else (not pending_setup),
            "session_expires_at": int((s or {}).get("expires_at") or 0),
            "reset_required": reset_pending(cfg),
            "remember_session_enabled": _cfg_remember_session_enabled(a),
            "remember_session_days": _cfg_remember_session_days(a),
            "plex_sso_enabled": bool(plex_st.get("enabled")),
            "plex_sso_linked": bool(plex_st.get("linked")),
        },
        headers={"Cache-Control": "no-store"},
    )


@router.post("/login")
def api_login(request: Request, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    req = request
    cfg = load_config()
    a = _cfg_auth(cfg)
    remember_me = bool(payload.get("remember_me"))
    if not auth_required(cfg):
        return JSONResponse({"ok": False, "error": "Authentication is not configured"}, status_code=400)

    ok_rl, retry = _rate_limit_ok(req)
    if not ok_rl:
        st = _rate_limit_state(req)
        return JSONResponse(
            _login_error_payload(error=f"Try again in {retry}s", attempts=st["n"], retry_after=retry),
            status_code=429,
            headers={"Cache-Control": "no-store"},
        )

    u = str(payload.get("username") or "").strip()
    ptxt = str(payload.get("password") or "")
    if u != str(a.get("username") or ""):
        st = _rate_limit_fail(req)
        status = 429 if st["retry_after"] > 0 else 401
        msg = f"Too many failed attempts. Try again in {st['retry_after']}s" if st["retry_after"] > 0 else "Invalid credentials"
        return JSONResponse(
            _login_error_payload(error=msg, attempts=st["n"], retry_after=st["retry_after"]),
            status_code=status,
            headers={"Cache-Control": "no-store"},
        )

    pwd = _cfg_pwd(a)
    try:
        salt = _b64d(str(pwd.get("salt") or ""))
        iters = int(pwd.get("iterations") or 260_000)
        want = str(pwd.get("hash") or "")
        got = _b64e(_pbkdf2_hash(ptxt, salt, iterations=iters))
        if not hmac.compare_digest(got, want):
            st = _rate_limit_fail(req)
            status = 429 if st["retry_after"] > 0 else 401
            msg = f"Too many failed attempts. Try again in {st['retry_after']}s" if st["retry_after"] > 0 else "Invalid credentials"
            return JSONResponse(
                _login_error_payload(error=msg, attempts=st["n"], retry_after=st["retry_after"]),
                status_code=status,
                headers={"Cache-Control": "no-store"},
            )
    except Exception:
        return JSONResponse({"ok": False, "error": "Authentication is not configured"}, status_code=400)

    if remember_me:
        a["remember_session_enabled"] = True
        if _cfg_remember_session_days(a) == 30:
            a["remember_session_days"] = DEFAULT_REMEMBER_ME_DAYS

    _rate_limit_reset(req)
    token, exp = _issue_session(cfg, req)
    save_config(cfg)
    resp = JSONResponse({"ok": True, "expires_at": exp}, headers={"Cache-Control": "no-store"})
    _set_cookie(resp, token, exp, req, persistent=remember_me)
    return resp


@router.post("/logout")
def api_logout(request: Request) -> JSONResponse:
    cfg = load_config()
    token = request.cookies.get(COOKIE_NAME)
    if auth_required(cfg) and token and not _origin_allowed(request):
        return _origin_blocked_response()
    _drop_session(cfg, token)
    save_config(cfg)
    resp = JSONResponse({"ok": True}, headers={"Cache-Control": "no-store"})
    _del_cookie(resp, request)
    return resp


@router.post("/logout-all")
def api_logout_all(request: Request) -> JSONResponse:
    cfg = load_config()
    token = request.cookies.get(COOKIE_NAME)
    if auth_required(cfg):
        if not is_authenticated(cfg, token):
            return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})
        if not _origin_allowed(request):
            return _origin_blocked_response()
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
    if auth_required(cfg) and token and not _origin_allowed(request):
        return _origin_blocked_response()

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
    recovery_mode = reset_pending(cfg)
    token = req.cookies.get(COOKIE_NAME)

    if configured0 and not recovery_mode and not is_authenticated(cfg, token):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    if configured0 and token and not _origin_allowed(req):
        return _origin_blocked_response()

    enabled = bool(payload.get("enabled"))
    username = str(payload.get("username") or "").strip()
    password = str(payload.get("password") or "")

    a = cfg.setdefault("app_auth", {})
    if not isinstance(a, dict):
        a = {}
        cfg["app_auth"] = a

    a["remember_session_enabled"] = bool(payload.get("remember_session_enabled", a.get("remember_session_enabled", False)))
    try:
        remember_days_raw = payload.get("remember_session_days", a.get("remember_session_days", 30))
        remember_days = int(remember_days_raw or 30)
    except Exception:
        remember_days = 30
    if remember_days < 1:
        remember_days = 1
    if remember_days > MAX_REMEMBER_SESSION_DAYS:
        remember_days = MAX_REMEMBER_SESSION_DAYS
    a["remember_session_days"] = remember_days

    if not enabled:
        a["enabled"] = False
        a["reset_required"] = False
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
    if password and len(password) < MIN_PASSWORD_LENGTH:
        return JSONResponse(
            {"ok": False, "error": f"Password must be at least {MIN_PASSWORD_LENGTH} characters"},
            status_code=400,
        )

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
    a["reset_required"] = False
    _clear_sessions(cfg)
    _clear_setup_autogen_flag(cfg)
    _mark_upgrade_pending_if_needed(cfg)

    token2, exp2 = _issue_session(cfg, req)
    save_config(cfg)

    resp = JSONResponse({"ok": True, "enabled": True, "expires_at": exp2}, headers={"Cache-Control": "no-store"})
    _set_cookie(resp, token2, exp2, req, persistent=_cfg_remember_session_enabled(a))
    return resp


def register_app_auth(app) -> None:
    app.include_router(router)
    try:
        from .authPlexAPI import register_auth_plex

        register_auth_plex(app)
    except Exception:
        pass

    @app.get("/login", include_in_schema=False, tags=["ui"])
    def ui_login() -> Response:
        cfg = load_config()
        if not auth_required(cfg):
            return RedirectResponse(url="/", status_code=302)
        a = _cfg_auth(cfg)
        username = str(a.get("username") or "")
        try:
            from services import authPlex

            plex_sso_available = authPlex.login_available(cfg)
        except Exception:
            plex_sso_available = False
        return HTMLResponse(_login_html(username, plex_sso_available=plex_sso_available), headers={"Cache-Control": "no-store"})

    @app.get("/logout", include_in_schema=False, tags=["ui"])
    def ui_logout(request: Request) -> Response:
        cfg = load_config()
        token = request.cookies.get(COOKIE_NAME)
        _drop_session(cfg, token)
        save_config(cfg)
        resp = RedirectResponse(url="/login" if auth_required(cfg) else "/")
        _del_cookie(resp, request)
        return resp
