# /api/appAuthAPI.py
# CrossWatch - UI authentication API
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from typing import Any

import base64
import hashlib
import ipaddress
import hmac
import logging
import os
import re
import secrets
import threading
import time
from pathlib import Path
from urllib.parse import quote, urlsplit

from fastapi import APIRouter, Body, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from cw_platform.config_base import CONFIG as CONFIG_DIR, load_config, save_config, update_config as _atomic_update_config
from cw_platform.access_policy import clean_managed_permissions
from cw_platform.event_archive.audit import record_audit
from cw_platform.provider_instances import list_user_profiles, normalize_user_profile_id

__all__ = [
    "router",
    "COOKIE_NAME",
    "AUTH_TTL_SEC",
    "MIN_PASSWORD_LENGTH",
    "auth_required",
    "credentials_configured",
    "clean_user_preferences",
    "current_user",
    "effective_user_profile_id",
    "is_authenticated",
    "is_admin_authenticated",
    "non_admin_api_allowed",
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
ADMIN_USER_ID = "administrator"
TOTP_ISSUER = "CrossWatch"
TOTP_STEP_SECONDS = 30
TOTP_DIGITS = 6
TOTP_WINDOW = 1
LOGIN_FAIL_TTL_SEC = 60 * 60

_LOG = logging.getLogger("crosswatch.api.app_auth")

_ERR_CREATE_USER_FAILED = "Unable to create user"
_ERR_UPDATE_USER_FAILED = "Unable to update user"
_ERR_UPDATE_CREDENTIALS_FAILED = "Unable to update credentials"
_ERR_VERIFY_TOTP_FAILED = "Unable to verify two-factor setup"

_LOGIN_FAILS: dict[str, dict[str, Any]] = {}
_TRUSTED_PROXY_CACHE: dict[str, Any] = {"at": 0.0, "nets": []}


def _update_config(mutator: Any) -> tuple[dict[str, Any], Any]:
    if (
        getattr(load_config, "__module__", "") != "cw_platform.config_base"
        or getattr(save_config, "__module__", "") != "cw_platform.config_base"
    ):
        cfg = load_config()
        result = mutator(cfg)
        save_config(cfg)
        return cfg, result
    return _atomic_update_config(mutator)


def _audit(
    request: Request,
    action: str,
    *,
    actor: dict[str, Any] | None = None,
    status: str = "success",
    target_type: str = "",
    target_id: Any = "",
    message: str = "",
    fields: dict[str, Any] | None = None,
) -> None:
    try:
        record_audit(
            action,
            actor=actor,
            request=request,
            status=status,
            target_type=target_type,
            target_id=target_id,
            message=message,
            fields=fields,
            source_kind="app_auth",
        )
    except Exception:
        pass

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


def _ip_is_trusted_proxy(value: Any) -> bool:
    try:
        ip = ipaddress.ip_address(str(value or "").strip())
    except Exception:
        return False
    for net in _trusted_proxy_nets():
        try:
            if ip in net:
                return True
        except Exception:
            continue
    return False


def _is_trusted_proxy_request(request: Request) -> bool:
    host = getattr(getattr(request, "client", None), "host", "") or ""
    return bool(host) and _ip_is_trusted_proxy(host)


def _effective_client_ip(request: Request) -> str:
    peer = getattr(getattr(request, "client", None), "host", "") or "local"
    if not _is_trusted_proxy_request(request):
        return peer

    xff = str(request.headers.get("x-forwarded-for") or "").strip()
    if not xff:
        return peer

    for raw in reversed(xff.split(",")):
        cand = raw.strip()
        if not cand:
            continue
        try:
            ipaddress.ip_address(cand)
        except Exception:
            return peer
        if not _ip_is_trusted_proxy(cand):
            return cand
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


def _normalize_app_user_id(v: Any) -> str:
    raw = str(v or "").strip().lower()
    if not raw:
        return ""
    compact = raw.replace("-", "")
    if re.fullmatch(r"[a-f0-9]{32}", compact):
        return compact
    if re.fullmatch(r"[a-z0-9][a-z0-9-]{1,63}", raw):
        return raw
    return ""


def _cfg_users(a: dict[str, Any], *, create: bool = False) -> dict[str, Any]:
    users = a.get("users")
    if isinstance(users, dict):
        return users
    if create:
        a["users"] = {}
        return a["users"]
    return {}


def _generate_app_user_id(a: dict[str, Any]) -> str:
    users = _cfg_users(a)
    for _ in range(32):
        uid = secrets.token_hex(16)
        if uid not in users:
            return uid
    raise RuntimeError("app_user_id_generation_failed")


def _clean_username(v: Any) -> str:
    return " ".join(str(v or "").strip().split())[:64]


def _clean_permissions(v: Any) -> dict[str, bool]:
    return clean_managed_permissions(v)


def _clean_create_permissions(v: Any) -> dict[str, bool]:
    defaults = {"dashboard": True, "watchlist": True, "playback": True, "write": True}
    if isinstance(v, dict):
        return clean_managed_permissions({**defaults, **v})
    return clean_managed_permissions(defaults)


def clean_user_preferences(raw: Any) -> dict[str, bool]:
    src = raw if isinstance(raw, dict) else {}
    return {
        "playing_card": src.get("playing_card") is not False,
        "quick_add": src.get("quick_add") is not False,
    }


def _password_hash(password: str) -> dict[str, Any]:
    salt = secrets.token_bytes(16)
    iters = 260_000
    return {
        "scheme": "pbkdf2_sha256",
        "iterations": iters,
        "salt": _b64e(salt),
        "hash": _b64e(_pbkdf2_hash(password, salt, iterations=iters)),
    }


def _password_matches(pwd: dict[str, Any], password: str) -> bool:
    try:
        salt = _b64d(str(pwd.get("salt") or ""))
        iters = int(pwd.get("iterations") or 260_000)
        want = str(pwd.get("hash") or "")
        got = _b64e(_pbkdf2_hash(password, salt, iterations=iters))
        return bool(want) and hmac.compare_digest(got, want)
    except Exception:
        return False


def _totp_cfg(raw: Any) -> dict[str, Any]:
    return raw if isinstance(raw, dict) else {}


def _totp_enabled(raw: Any) -> bool:
    t = _totp_cfg(raw)
    return bool(t.get("enabled")) and bool(str(t.get("secret") or "").strip())


def _totp_public(raw: Any) -> dict[str, Any]:
    t = _totp_cfg(raw)
    return {"enabled": _totp_enabled(t), "pending": bool(str(t.get("pending_secret") or "").strip())}


def _totp_clean_code(value: Any) -> str:
    return re.sub(r"\D+", "", str(value or ""))[:TOTP_DIGITS]


def _totp_new_secret() -> str:
    return base64.b32encode(secrets.token_bytes(20)).decode("ascii").rstrip("=")


def _totp_key(secret: Any) -> bytes:
    raw = re.sub(r"\s+", "", str(secret or "")).upper()
    if not raw:
        return b""
    raw += "=" * ((8 - (len(raw) % 8)) % 8)
    try:
        return base64.b32decode(raw, casefold=True)
    except Exception:
        return b""


def _hotp(secret: Any, counter: int) -> str:
    key = _totp_key(secret)
    if not key:
        return ""
    msg = int(counter).to_bytes(8, "big")
    digest = hmac.new(key, msg, hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    binary = ((digest[offset] & 0x7F) << 24) | ((digest[offset + 1] & 0xFF) << 16) | ((digest[offset + 2] & 0xFF) << 8) | (digest[offset + 3] & 0xFF)
    return str(binary % (10 ** TOTP_DIGITS)).zfill(TOTP_DIGITS)


def _totp_verify(secret: Any, code: Any, *, at: int | None = None) -> bool:
    got = _totp_clean_code(code)
    if len(got) != TOTP_DIGITS:
        return False
    now = _now() if at is None else int(at)
    counter = now // TOTP_STEP_SECONDS
    for drift in range(-TOTP_WINDOW, TOTP_WINDOW + 1):
        want = _hotp(secret, counter + drift)
        if want and hmac.compare_digest(want.encode("ascii"), got.encode("ascii")):
            return True
    return False


def _totp_uri(username: Any, secret: str) -> str:
    account = str(username or "CrossWatch").strip() or "CrossWatch"
    label = quote(f"{TOTP_ISSUER}:{account}", safe="")
    issuer = quote(TOTP_ISSUER, safe="")
    return f"otpauth://totp/{label}?secret={quote(secret, safe='')}&issuer={issuer}&algorithm=SHA1&digits={TOTP_DIGITS}&period={TOTP_STEP_SECONDS}"


def _admin_identity(a: dict[str, Any]) -> dict[str, Any]:
    username = str(a.get("username") or "")
    display_name = _clean_username(a.get("display_name")) or username or "Administrator"
    return {
        "id": ADMIN_USER_ID,
        "username": username,
        "label": display_name,
        "display_name": display_name,
        "avatar_url": _managed_avatar_url(a, ADMIN_USER_ID),
        "role": "admin",
        "is_admin": True,
        "profile_id": "",
        "permissions": _clean_permissions({"dashboard": True, "watchlist": True, "playback": True, "write": True}),
        "totp_enabled": _totp_enabled(a.get("totp")),
        "created_at": int(a.get("created_at") or 0),
        "preferences": clean_user_preferences(a.get("preferences")),
    }


def _linked_avatar_source(raw: dict[str, Any]) -> tuple[str, int]:
    candidates: list[tuple[str, Any]] = []
    plex_sso = raw.get("plex_sso")
    if isinstance(plex_sso, dict):
        candidates.append((str(plex_sso.get("linked_thumb") or plex_sso.get("thumb") or "").strip(), plex_sso.get("linked_at")))
    oidc = raw.get("oidc_identity")
    if isinstance(oidc, dict):
        candidates.append((str(oidc.get("picture") or "").strip(), oidc.get("linked_at")))
    oidc_user = raw.get("oidc")
    if isinstance(oidc_user, dict):
        candidates.append((str(oidc_user.get("picture") or "").strip(), oidc_user.get("linked_at")))
    for url, updated_at in candidates:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
            continue
        try:
            version = int(updated_at or 0)
        except Exception:
            version = 0
        return url, version
    return "", 0


def _managed_avatar_url(raw: dict[str, Any], user_id: Any = "") -> str:
    avatar = raw.get("avatar")
    version = 0
    if isinstance(avatar, dict):
        name = os.path.basename(str(avatar.get("file") or ""))
        if re.fullmatch(r"[a-f0-9]{32}\.(png|jpg|webp)", name):
            path = Path(CONFIG_DIR) / "profile_avatars" / name
            if path.exists():
                try:
                    version = max(int(avatar.get("updated_at") or 0), int(path.stat().st_mtime_ns // 1_000_000))
                except Exception:
                    version = int(avatar.get("updated_at") or 0)
            else:
                path = None
        else:
            path = None
    else:
        path = None
    if path is None:
        linked_url, linked_version = _linked_avatar_source(raw)
        if not linked_url:
            return ""
        version = linked_version
    uid = _normalize_app_user_id(user_id)
    if uid:
        return f"/api/profile/avatar/{uid}?ts={version}"
    return f"/api/profile/avatar?ts={version}"


def _public_user(user_id: str, raw: dict[str, Any]) -> dict[str, Any]:
    username = _clean_username(raw.get("username"))
    role = "admin" if str(raw.get("role") or "").strip().lower() == "admin" else "user"
    avatar_url = _managed_avatar_url(raw, user_id)
    return {
        "id": user_id,
        "username": username,
        "label": username,
        "display_name": _clean_username(raw.get("display_name")) or username,
        "avatar_url": avatar_url,
        "enabled": bool(raw.get("enabled", True)),
        "role": role,
        "is_admin": role == "admin",
        "profile_id": normalize_user_profile_id(raw.get("profile_id")),
        "permissions": _clean_permissions(raw.get("permissions")),
        "totp_enabled": _totp_enabled(raw.get("totp")),
        "created_at": int(raw.get("created_at") or 0),
        "preferences": clean_user_preferences(raw.get("preferences")),
    }


def _list_managed_users(a: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for user_id, raw in _cfg_users(a).items():
        uid = _normalize_app_user_id(user_id)
        if not uid or not isinstance(raw, dict):
            continue
        public = _public_user(uid, raw)
        if public["username"]:
            rows.append(public)
    rows.sort(key=lambda row: (str(row.get("username") or "").lower(), str(row.get("id") or "")))
    return rows


def _username_in_use(a: dict[str, Any], username: Any, *, exclude_id: Any = None) -> bool:
    clean = _clean_username(username).casefold()
    if not clean:
        return False
    excluded = _normalize_app_user_id(exclude_id)
    if _clean_username(a.get("username")).casefold() == clean and excluded != ADMIN_USER_ID:
        return True
    for row in _list_managed_users(a):
        if excluded and row["id"] == excluded:
            continue
        if _clean_username(row.get("username")).casefold() == clean:
            return True
    return False


def _find_user_for_login(a: dict[str, Any], username: Any, password: Any) -> dict[str, Any] | None:
    clean = _clean_username(username)
    ptxt = str(password or "")
    if clean and clean == str(a.get("username") or "") and _password_matches(_cfg_pwd(a), ptxt):
        return _admin_identity(a)
    for user_id, raw in _cfg_users(a).items():
        uid = _normalize_app_user_id(user_id)
        if not uid or not isinstance(raw, dict):
            continue
        public = _public_user(uid, raw)
        if not public["enabled"] or public["username"] != clean:
            continue
        pwd = raw.get("password")
        if isinstance(pwd, dict) and _password_matches(pwd, ptxt):
            return public
    return None


def _consume_recovery_code(a: dict[str, Any], user: dict[str, Any], code: Any) -> bool:
    uid = _normalize_app_user_id(user.get("id"))
    if not uid:
        return False
    raw = a if uid == ADMIN_USER_ID else _cfg_users(a, create=True).get(uid)
    if not isinstance(raw, dict):
        return False
    clean = re.sub(r"[^A-Z0-9]+", "", str(code or "").upper())
    if len(clean) < 10:
        return False
    rows = raw.get("recovery_codes")
    if not isinstance(rows, list):
        return False
    kept: list[Any] = []
    matched = False
    variants = {clean, f"{clean[:10]}-{clean[10:]}"}
    for row in rows:
        if isinstance(row, dict) and not matched and any(_password_matches(row, value) for value in variants):
            matched = True
            continue
        kept.append(row)
    if matched:
        raw["recovery_codes"] = kept
    return matched


def _totp_record_for_user(a: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    if user.get("is_admin") or str(user.get("id") or "") == ADMIN_USER_ID:
        t = a.setdefault("totp", {})
        if not isinstance(t, dict):
            t = {}
            a["totp"] = t
        return t
    uid = _normalize_app_user_id(user.get("id"))
    raw = _cfg_users(a, create=True).get(uid)
    if not isinstance(raw, dict):
        return {}
    t = raw.setdefault("totp", {})
    if not isinstance(t, dict):
        t = {}
        raw["totp"] = t
    return t


def _target_user_for_totp(a: dict[str, Any], actor: dict[str, Any], requested_user_id: Any = None) -> tuple[str, dict[str, Any], dict[str, Any] | None]:
    actor_id = _normalize_app_user_id(actor.get("id")) or ADMIN_USER_ID
    wanted = _normalize_app_user_id(requested_user_id) or actor_id
    if not actor.get("is_admin") and wanted != actor_id:
        return "", {}, None
    if wanted == ADMIN_USER_ID:
        admin = _admin_identity(a)
        return ADMIN_USER_ID, admin, a
    raw = _cfg_users(a, create=True).get(wanted)
    if not isinstance(raw, dict):
        return "", {}, None
    public = _public_user(wanted, raw)
    if not public.get("enabled"):
        return "", {}, None
    return wanted, public, raw


def _session_user_id(session: dict[str, Any] | None) -> str:
    if not isinstance(session, dict):
        return ""
    raw = session.get("user_id")
    if raw is None or str(raw).strip() == "":
        return ADMIN_USER_ID
    return _normalize_app_user_id(raw)


def _session_identity(a: dict[str, Any], session: dict[str, Any] | None) -> dict[str, Any] | None:
    if session is None:
        return None
    user_id = _session_user_id(session)
    if not user_id:
        return None
    if user_id == ADMIN_USER_ID:
        return _admin_identity(a)
    raw = _cfg_users(a).get(user_id)
    if not isinstance(raw, dict):
        return None
    public = _public_user(user_id, raw)
    return public if public["enabled"] else None


def current_user(cfg: dict[str, Any], token: str | None) -> dict[str, Any] | None:
    if not auth_required(cfg):
        return None
    a = _cfg_auth(cfg)
    return _session_identity(a, _find_session(a, token))


def is_admin_authenticated(cfg: dict[str, Any], token: str | None) -> bool:
    user = current_user(cfg, token)
    return bool(user and user.get("is_admin"))


def effective_user_profile_id(cfg: dict[str, Any], token: str | None, requested_profile: Any = "") -> str:
    req = normalize_user_profile_id(requested_profile)
    if not auth_required(cfg):
        return req
    user = current_user(cfg, token)
    if not user:
        return req
    if user.get("is_admin"):
        return req
    return normalize_user_profile_id(user.get("profile_id")) or "__none__"


def non_admin_api_allowed(path: Any, method: Any) -> bool:
    p = str(path or "").split("?", 1)[0]
    m = str(method or "GET").upper()
    if not p.startswith("/api/"):
        return True
    if m in {"OPTIONS", "HEAD"}:
        return True
    if p in {"/api/app-auth/status", "/api/app-auth/logout", "/api/app-auth/logout-others"}:
        return True
    if p in {"/api/app-auth/plex/status", "/api/app-auth/plex/link/start", "/api/app-auth/plex/link/check", "/api/app-auth/plex/unlink"}:
        return m in {"GET", "POST"}
    if p in {"/api/app-auth/oidc/status", "/api/app-auth/oidc/link/start", "/api/app-auth/oidc/link/check", "/api/app-auth/oidc/unlink"}:
        return m in {"GET", "POST"}
    if p.startswith("/api/app-auth/"):
        return False
    if p == "/api/profile" or p.startswith("/api/profile/"):
        return m in {"GET", "POST", "PUT", "PATCH", "DELETE"}
    if p == "/api/config/meta":
        return m == "GET"
    if p == "/api/watch/currently_watching":
        return m == "GET"
    if p in {"/api/playback_progress/providers", "/api/playback_progress/settings", "/api/playback_progress/items"}:
        return m == "GET"
    if p == "/api/insights":
        return m == "GET"
    if p == "/api/metadata/bulk":
        return m == "POST"
    if p == "/api/metadata/search":
        return m == "GET"
    if p == "/api/metadata/resolve":
        return m == "POST"
    if p == "/api/provider-instances" or p.startswith("/api/provider-instances/"):
        return m == "GET"
    if p == "/api/status":
        return m == "GET"
    if p == "/api/logs/stream":
        return m == "GET"
    if p == "/api/logs/watcher":
        return False
    if p in {"/api/run", "/api/run/cancel", "/api/run/summary", "/api/run/summary/file", "/api/run/summary/stream", "/api/run/unresolved"}:
        return m in {"GET", "POST"}
    if p == "/api/pairs" or p.startswith("/api/pairs/"):
        return m in {"GET", "POST", "PUT", "DELETE"}
    if p == "/api/sync/providers" or p == "/api/sync/providers/counts":
        return m == "GET"
    if p.startswith("/api/playback_progress/actions/"):
        return m == "POST"
    for prefix in (
        "/api/analyzer",
        "/api/events",
        "/api/export",
        "/api/import",
        "/api/editor",
        "/api/playlists",
        "/api/snapshots",
        "/api/manual",
    ):
        base = prefix.rstrip("/")
        if p == base or p.startswith(base + "/"):
            return m in {"GET", "POST", "PUT", "PATCH", "DELETE"}
    for prefix in (
        "/api/dashboard/",
        "/api/state/wall",
        "/api/watchlist",
        "/api/activity/recent",
        "/api/activity/history",
        "/api/user-profiles",
        "/api/version",
    ):
        base = prefix.rstrip("/")
        if p == base or p.startswith(base + "/"):
            if prefix == "/api/watchlist":
                return m in {"GET", "POST", "DELETE"}
            return m == "GET"
    return False


def _admin_required(request: Request, cfg: dict[str, Any]) -> JSONResponse | None:
    token = request.cookies.get(COOKIE_NAME)
    if auth_required(cfg) and not is_admin_authenticated(cfg, token):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})
    if auth_required(cfg) and token and not _origin_allowed(request):
        return _origin_blocked_response()
    return None


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
        if th and _is_sha256_hex(th) and exp > now and _session_user_id(s):
            keep.append(s)
    keep.sort(key=lambda x: int(x.get("created_at") or 0) or int(x.get("expires_at") or 0))
    buckets: dict[str, list[dict[str, Any]]] = {}
    for session in keep:
        uid = _session_user_id(session)
        buckets.setdefault(uid, []).append(session)
    out: list[dict[str, Any]] = []
    for rows in buckets.values():
        out.extend(rows[-MAX_SESSIONS:])
    out.sort(key=lambda x: int(x.get("created_at") or 0) or int(x.get("expires_at") or 0))
    return out


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


def credentials_configured(cfg: dict[str, Any]) -> bool:
    a = _cfg_auth(cfg)
    if not str(a.get("username") or "").strip():
        return False
    p = _cfg_pwd(a)
    if not str(p.get("hash") or "").strip():
        return False
    if not str(p.get("salt") or "").strip():
        return False
    return True


def auth_required(cfg: dict[str, Any]) -> bool:
    a = _cfg_auth(cfg)
    return bool(a.get("enabled")) and credentials_configured(cfg)


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


def _public_session_entry(s: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(s.get("id") or "").strip(),
        "created_at": int(s.get("created_at") or 0),
        "expires_at": int(s.get("expires_at") or 0),
        "ip": str(s.get("ip") or "").strip(),
        "ua": str(s.get("ua") or "").strip()[:240],
        "user_id": str(s.get("user_id") or ADMIN_USER_ID).strip(),
        "username": str(s.get("username") or "").strip(),
        "role": str(s.get("role") or "admin").strip(),
        "profile_id": normalize_user_profile_id(s.get("profile_id")),
    }


def _public_session_state(a: dict[str, Any], token: str | None) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    sessions = _prune_sessions(_iter_sessions(a))
    current = _find_session(a, token)
    if current is None:
        return None, []
    current_token_hash = str((current or {}).get("token_hash") or "").strip()

    current_public = _public_session_entry(current)
    other_public: list[dict[str, Any]] = []
    for session in sessions:
        token_hash = str(session.get("token_hash") or "").strip()
        if current_token_hash and _digest_eq(token_hash, current_token_hash):
            continue
        other_public.append(_public_session_entry(session))
    return current_public, other_public


def is_authenticated(cfg: dict[str, Any], token: str | None) -> bool:
    if not auth_required(cfg):
        return False
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


def _prune_login_fails() -> None:
    cutoff = _now() - LOGIN_FAIL_TTL_SEC
    for key in [k for k, v in _LOGIN_FAILS.items() if int((v or {}).get("at") or 0) < cutoff]:
        _LOGIN_FAILS.pop(key, None)


def _rate_limit_fail(request: Request) -> dict[str, int]:
    _prune_login_fails()
    ip = _effective_client_ip(request)
    rec = _LOGIN_FAILS.get(ip) or {"n": 0, "until": 0}
    n = int(rec.get("n") or 0) + 1
    backoff = _login_lockout_seconds(n)
    until = (_now() + backoff) if backoff > 0 else 0
    _LOGIN_FAILS[ip] = {"n": n, "until": until, "at": _now()}
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


def _issue_session(cfg: dict[str, Any], request: Request, user: dict[str, Any] | None = None) -> tuple[str, int]:
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
    ident = user or _admin_identity(a)
    sessions.append(
        {
            "id": secrets.token_hex(8),
            "token_hash": _sha256_hex(token),
            "created_at": now,
            "expires_at": exp,
            "ip": ip,
            "ua": ua,
            "user_id": str(ident.get("id") or ADMIN_USER_ID),
            "username": str(ident.get("username") or ""),
            "role": "admin" if ident.get("is_admin") or ident.get("role") == "admin" else "user",
            "profile_id": normalize_user_profile_id(ident.get("profile_id")),
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


def _clear_other_sessions(cfg: dict[str, Any], token: str | None) -> None:
    a = cfg.get("app_auth")
    if not isinstance(a, dict):
        return
    keep = _find_session(a, token)
    kept = [keep] if keep is not None else []
    a["sessions"] = kept
    _sync_legacy_session(a, kept)


def _clear_user_sessions(cfg: dict[str, Any], user_id: Any) -> None:
    a = cfg.get("app_auth")
    if not isinstance(a, dict):
        return
    uid = _normalize_app_user_id(user_id)
    if not uid:
        return
    kept = [s for s in _prune_sessions(_iter_sessions(a)) if _normalize_app_user_id(s.get("user_id")) != uid]
    a["sessions"] = kept
    _sync_legacy_session(a, kept)


def _clear_user_sessions_except(cfg: dict[str, Any], user_id: Any, token: str | None) -> None:
    a = cfg.get("app_auth")
    if not isinstance(a, dict):
        return
    uid = _normalize_app_user_id(user_id)
    if not uid:
        return
    th = _sha256_hex(token or "") if token else ""
    kept: list[dict[str, Any]] = []
    for s in _prune_sessions(_iter_sessions(a)):
        sid = _normalize_app_user_id(s.get("user_id")) or ADMIN_USER_ID
        sth = str(s.get("token_hash") or "").strip()
        if sid != uid or (th and _digest_eq(sth, th)):
            kept.append(s)
    a["sessions"] = kept
    _sync_legacy_session(a, kept)


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
        referer = _normalize_origin(request.headers.get("referer"))
        if referer:
            got = referer
    if not got:
        fetch_site = str(request.headers.get("sec-fetch-site") or "").strip().lower()
        return fetch_site in {"none", "same-origin", "same-site"}
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
  margin:0;display:grid;place-items:center;min-height:100vh;min-height:100svh;color:var(--cw-text);
  font-family:"Segoe UI Variable","Avenir Next","Trebuchet MS",sans-serif;
  background:
    radial-gradient(900px circle at 8% 10%, rgba(140,109,255,.18), transparent 42%),
    radial-gradient(780px circle at 92% 18%, rgba(192,140,255,.18), transparent 40%),
    radial-gradient(760px circle at 50% 110%, rgba(112,92,214,.16), transparent 42%),
    linear-gradient(180deg,#07111b 0%,#03070c 100%);
  overflow-x:hidden;overflow-y:auto;padding:16px;
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
.cw-totp-hidden{display:none!important}
.cw-actions{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));align-items:start;gap:14px}
.cw-action-primary{min-width:0}
.cw-action-plex,.cw-action-oidc{display:grid;gap:12px;min-width:0}
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
.cw-oidc-btn{
  width:100%;min-height:58px;border-radius:28px!important;
  border:1px solid rgba(147,197,253,.34)!important;
  background:linear-gradient(135deg,rgba(59,130,246,.92),rgba(99,102,241,.88))!important;
  color:#eef6ff!important;
  box-shadow:0 16px 34px rgba(59,130,246,.22), inset 0 1px 0 rgba(255,255,255,.16)!important;
}
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
  body{display:block;min-height:100svh;padding:10px}
  .cw-login-shell{width:min(100vw - 20px,1040px);border-radius:22px;margin:0 auto}
  .cw-hero,.cw-login{padding:18px}
  .cw-hero::after{display:none}
  .cw-mark img{width:min(220px,72vw)}
  .cw-hero h1{margin:16px 0 8px;font-size:clamp(26px,8vw,36px)}
  .cw-hero p{font-size:13px;line-height:1.5}
  .cw-metrics{gap:10px;margin-top:18px;max-width:none}
  .cw-login{gap:14px}
  .cw-login h2{font-size:24px}
  .cw-field input,.cw-login .btn{min-height:48px}
  .cw-remember{padding:9px 10px}
  .cw-remember span{font-size:11.5px;line-height:1.4}
  .cw-actions{grid-template-columns:1fr;align-items:stretch}
  .cw-action-primary,.cw-action-plex,.cw-action-oidc{min-width:100%}
  .cw-login .btn{width:100%}
}
@media (max-width:400px){
  body{padding:8px}
  .cw-login-shell{width:calc(100vw - 16px);border-radius:20px}
  .cw-hero,.cw-login{padding:16px}
  .cw-metrics{display:none}
}
"""


def _login_html(username: str, *, plex_sso_available: bool = False, oidc_available: bool = False) -> str:
    plex_html = ""
    if plex_sso_available:
        plex_html = """
        <div class="cw-action-plex">
          <button class="btn cw-plex-btn" id="go-plex" type="button"><span>Sign in with Plex</span></button>
          <p class="cw-plex-copy">Use your linked Plex account, then return here to finish sign-in.</p>
        </div>
        """
    oidc_html = ""
    if oidc_available:
        oidc_html = """
        <div class="cw-action-oidc">
          <button class="btn cw-oidc-btn" id="go-oidc" type="button"><span>Sign in with OIDC</span></button>
          <p class="cw-plex-copy">Use your linked external account, then return to CrossWatch.</p>
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
        <p class=\"sub\">Use your local CrossWatch credentials to continue.</p>
      </div>
      <div id=\"help\" class=\"cw-banner\" role=\"status\" aria-live=\"polite\"></div>
      <div id=\"msg\" class=\"cw-msg\" role=\"alert\" aria-live=\"assertive\"></div>
      <form class=\"cw-form\" id=\"login-form\" autocomplete=\"on\">
        <div class=\"cw-field\">
          <label for=\"u\">Username</label>
          <input id=\"u\" name=\"username\" autocomplete=\"username\">
        </div>
        <div class=\"cw-field\">
          <label for=\"p\">Password</label>
          <input id=\"p\" name=\"password\" type=\"password\" autocomplete=\"current-password\">
        </div>
        <div class=\"cw-field cw-totp-hidden\" id=\"totp-wrap\">
          <label for=\"totp\">Verification code</label>
          <input id=\"totp\" name=\"totp\" type=\"text\" inputmode=\"numeric\" pattern=\"[0-9]*\" maxlength=\"6\" autocomplete=\"one-time-code\">
        </div>
        <label class=\"cw-remember\" for=\"remember\">
          <input id=\"remember\" name=\"remember\" type=\"checkbox\">
          <span><b>Remember me</b><span>Keep this browser signed in for up to {DEFAULT_REMEMBER_ME_DAYS} days.</span></span>
        </label>
        <div class=\"cw-actions\">
          <div class=\"cw-action-primary\">
            <button class=\"btn acc\" id=\"go\" type=\"submit\">Sign in</button>
          </div>
          {plex_html}
          {oidc_html}
        </div>
      </form>
    </section>
  </div>
  <script>
    const $=(id)=>document.getElementById(id);
    const msg=$('msg');
    const help=$('help');
    const btn=$('go');
    const plexBtn=$('go-plex');
    const oidcBtn=$('go-oidc');
    let needs2fa=false;
    let plexPolling=false;
    let plex2faState='';
    function setMsg(text){{
      msg.textContent=text||'';
      msg.classList.toggle('show',!!text);
    }}
    function nextUrl(data){{
      const next = (new URLSearchParams(location.search)).get('next') || '/';
      let safe = '/';
      try{{
        if(next.startsWith('/') && next[1] !== '/' && next[1] !== '\\\\' && !next.includes('\\\\')){{
          const u = new URL(next, location.origin);
          if(u.origin === location.origin) safe = u.pathname + u.search + u.hash;
        }}
      }}catch(e){{ safe = '/'; }}
      if(safe && safe !== '/' && safe !== '/login' && safe !== '/logout') return safe;
      return data && data.user && data.user.is_admin === false ? '/profile' : '/';
    }}
    function setHelp(data){{
      const url=(data&&data.forgot_help_url)||'https://wiki.crosswatch.app/';
      const on=!!(data&&data.show_help_banner);
      help.innerHTML=on?`Forget username/password? Visit <a href="${{url}}" target="_blank" rel="noopener noreferrer">${{url}}</a>`:'';
      help.classList.toggle('show',on);
    }}
    function set2fa(on){{
      needs2fa=!!on;
      const wrap=$('totp-wrap');
      if(wrap) wrap.classList.toggle('cw-totp-hidden',!needs2fa);
      btn.textContent=needs2fa?'Verify':'Sign in';
      if(needs2fa) setTimeout(()=>{{ try{{$('totp')?.focus();}}catch(e){{}} }},0);
    }}
    async function finishPlex2fa(){{
      if(!plex2faState) return false;
      setMsg('');
      const code=$('totp')?.value.trim()||'';
      btn.disabled=true;
      if(plexBtn) plexBtn.disabled=true;
      btn.textContent='Verifying...';
      try{{
        const r=await fetch('/api/app-auth/plex/check',{{method:'POST',headers:{{'Content-Type':'application/json'}},credentials:'same-origin',body:JSON.stringify({{state:plex2faState,totp_code:code}})}});
        const data=await r.json().catch(()=>null);
        if(!r.ok || !data || !data.ok){{
          if(data&&data.requires_2fa){{
            set2fa(true);
            if(data.state) plex2faState=data.state;
          }}
          setHelp(data);
          const base=(data && data.error) ? data.error : ('Plex sign-in failed ('+r.status+')');
          const retry=(data && Number.isFinite(data.retry_after) && data.retry_after>0) ? ` Login paused for ${{data.retry_after}}s.` : '';
          setMsg(base + retry);
          return true;
        }}
        plex2faState='';
        setHelp(null);
        location.href = nextUrl(data);
        return true;
      }}catch(e){{
        setMsg('Plex sign-in failed');
        return true;
      }}finally{{
        btn.disabled=false;
        if(plexBtn) plexBtn.disabled=false;
        btn.textContent=needs2fa?'Verify':'Sign in';
        if(plexBtn) plexBtn.textContent=plex2faState?'Verify Plex':'Sign in with Plex';
      }}
    }}
    async function login(){{
      if(plex2faState){{
        await finishPlex2fa();
        return;
      }}
      setMsg('');
      const u=$('u').value.trim();
      const p=$('p').value;
      const code=$('totp')?.value.trim()||'';
      const remember=$('remember')?.checked===true;
      btn.disabled=true;
      btn.textContent=needs2fa?'Verifying...':'Signing in...';
      try{{
        const r=await fetch('/api/app-auth/login',{{method:'POST',headers:{{'Content-Type':'application/json'}},credentials:'same-origin',body:JSON.stringify({{username:u,password:p,totp_code:code,remember_me:remember}})}});
        const data=await r.json().catch(()=>null);
        if(!r.ok || !data || !data.ok){{
          if(data&&data.requires_2fa){{
            set2fa(true);
          }}
          setHelp(data);
          const base=(data && data.error) ? data.error : ('Login failed ('+r.status+')');
          const retry=(data && Number.isFinite(data.retry_after) && data.retry_after>0) ? ` Login paused for ${{data.retry_after}}s.` : '';
          setMsg(base + retry);
          return;
        }}
        setHelp(null);
        location.href = nextUrl(data);
      }}catch(e){{
        setMsg('Login failed');
      }}finally{{
        btn.disabled=false;
        btn.textContent=needs2fa?'Verify':'Sign in';
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
            if(pd&&pd.requires_2fa){{
              plex2faState=pd.state||data.state;
              set2fa(true);
              setHelp(pd);
              setMsg(pd.error||'Verification code required');
              return;
            }}
            setMsg((pd&&pd.error)||('Plex sign-in failed ('+pr.status+')'));
            return;
          }}
          if(popup && !popup.closed) popup.close();
          location.href = nextUrl(pd);
          return;
        }}
      }}catch(e){{
        if(popup && !popup.closed) popup.close();
        setMsg('Plex sign-in failed');
      }}finally{{
        plexPolling=false;
        if(plexBtn){{
          plexBtn.disabled=false;
          plexBtn.textContent=plex2faState?'Verify Plex':'Sign in with Plex';
        }}
      }}
    }}
    $('login-form')?.addEventListener('submit', (e)=>{{ e.preventDefault(); login(); }});
    plexBtn?.addEventListener('click', async()=>{{ if(plex2faState) await finishPlex2fa(); else await startPlex(); }});
    oidcBtn?.addEventListener('click', ()=>{{
      const remember=$('remember')?.checked===true ? '1' : '0';
      const next=(new URLSearchParams(location.search)).get('next')||'/';
      location.href='/api/app-auth/oidc/start?remember_me='+encodeURIComponent(remember)+'&next='+encodeURIComponent(next);
    }});
    $('totp')?.addEventListener('input',()=>{{ const el=$('totp'); if(el) el.value=el.value.replace(/\\D+/g,'').slice(0,6); }});
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
    try:
        from services import authOidc

        oidc_st = authOidc.get_status(cfg)
    except Exception:
        oidc_st = {"enabled": False, "linked": False, "configured": False}
    configured = credentials_configured(cfg)
    enabled = bool(a.get("enabled"))
    pending_setup = setup_lock_required(cfg)
    active = auth_required(cfg)
    token = request.cookies.get(COOKIE_NAME)
    s = _find_session(a, token)
    user = _session_identity(a, s)
    current_session, other_sessions = _public_session_state(a, token)
    return JSONResponse(
        {
            "enabled": enabled,
            "configured": configured,
            "setup_required": pending_setup,
            "username": str(user.get("username") or "") if (enabled and user is not None) else "",
            "authenticated": (user is not None) if active else False,
            "user": user if (enabled and user is not None) else None,
            "user_id": str(user.get("id") or "") if user else "",
            "profile_id": str(user.get("profile_id") or "") if user else "",
            "role": str(user.get("role") or "") if user else "",
            "is_admin": bool(user.get("is_admin")) if user else False,
            "totp_enabled": bool(user.get("totp_enabled")) if user else False,
            "session_expires_at": int((s or {}).get("expires_at") or 0),
            "current_session": current_session,
            "other_sessions": other_sessions,
            "other_session_count": len(other_sessions),
            "reset_required": reset_pending(cfg),
            "remember_session_enabled": _cfg_remember_session_enabled(a),
            "remember_session_days": _cfg_remember_session_days(a),
            "plex_sso_enabled": bool(plex_st.get("enabled")),
            "plex_sso_linked": bool(plex_st.get("linked")),
            "oidc_enabled": bool(oidc_st.get("enabled")),
            "oidc_configured": bool(oidc_st.get("configured")),
            "oidc_linked": bool(oidc_st.get("linked")),
        },
        headers={"Cache-Control": "no-store"},
    )


@router.post("/login")
def api_login(request: Request, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    req = request
    if not _origin_allowed(req):
        return _origin_blocked_response()
    cfg = load_config()
    a = _cfg_auth(cfg)
    remember_me = bool(payload.get("remember_me"))
    if not auth_required(cfg):
        return JSONResponse({"ok": False, "error": "Authentication is not configured"}, status_code=400)

    ok_rl, retry = _rate_limit_ok(req)
    if not ok_rl:
        st = _rate_limit_state(req)
        u = str(payload.get("username") or "").strip()
        _audit(req, "login_blocked", status="blocked", message=f"Login blocked for {u or 'unknown'} by rate limit", fields={"username": u, "retry_after": retry, "attempts": st["n"]})
        return JSONResponse(
            _login_error_payload(error=f"Try again in {retry}s", attempts=st["n"], retry_after=retry),
            status_code=429,
            headers={"Cache-Control": "no-store"},
        )

    u = str(payload.get("username") or "").strip()
    ptxt = str(payload.get("password") or "")
    user = _find_user_for_login(a, u, ptxt)
    if user is None:
        st = _rate_limit_fail(req)
        status = 429 if st["retry_after"] > 0 else 401
        msg = f"Too many failed attempts. Try again in {st['retry_after']}s" if st["retry_after"] > 0 else "Invalid credentials"
        _audit(req, "login_failed", status="failed", message=f"Login failed for {u or 'unknown'}", fields={"username": u, "attempts": st["n"], "retry_after": st["retry_after"]})
        return JSONResponse(
            _login_error_payload(error=msg, attempts=st["n"], retry_after=st["retry_after"]),
            status_code=status,
            headers={"Cache-Control": "no-store"},
        )

    raw_code = ""
    code = ""
    recovery_used = False
    totp = _totp_record_for_user(a, user)
    if _totp_enabled(totp):
        raw_code = payload.get("totp_code") or payload.get("code") or payload.get("recovery_code")
        code = _totp_clean_code(raw_code)
        if not str(raw_code or "").strip():
            _audit(req, "2fa_required", actor=user, status="blocked", message=f"Two-factor verification required for {user.get('username') or 'user'}", target_type="user", target_id=user.get("id"))
            return JSONResponse(
                {"ok": False, "error": "Verification code required", "requires_2fa": True},
                status_code=401,
                headers={"Cache-Control": "no-store"},
            )
        totp_ok = _totp_verify(totp.get("secret"), code)
        if not totp_ok:
            recovery_used = _consume_recovery_code(a, user, raw_code)
        if not (totp_ok or recovery_used):
            st = _rate_limit_fail(req)
            status = 429 if st["retry_after"] > 0 else 401
            msg = f"Too many failed attempts. Try again in {st['retry_after']}s" if st["retry_after"] > 0 else "Invalid verification code"
            body = _login_error_payload(error=msg, attempts=st["n"], retry_after=st["retry_after"])
            body["requires_2fa"] = True
            _audit(req, "2fa_failed", actor=user, status="failed", message=f"Two-factor verification failed for {user.get('username') or 'user'}", target_type="user", target_id=user.get("id"), fields={"attempts": st["n"], "retry_after": st["retry_after"]})
            return JSONResponse(body, status_code=status, headers={"Cache-Control": "no-store"})
        if recovery_used:
            _audit(req, "2fa_recovery_code_used", actor=user, message=f"{user.get('username') or 'User'} used a recovery code", target_type="user", target_id=user.get("id"))

    _rate_limit_reset(req)
    recovery_raw_code = raw_code if _totp_enabled(totp) else ""

    def _mutate_login(latest: dict[str, Any]) -> tuple[dict[str, Any], str, int]:
        latest_a = _cfg_auth(latest)
        latest_user = _find_user_for_login(latest_a, u, ptxt)
        if latest_user is None:
            raise ValueError("invalid_credentials")
        latest_totp = _totp_record_for_user(latest_a, latest_user)
        if _totp_enabled(latest_totp):
            if recovery_used and not _consume_recovery_code(latest_a, latest_user, recovery_raw_code):
                raise ValueError("invalid_recovery_code")
            if not recovery_used and not _totp_verify(latest_totp.get("secret"), code):
                raise ValueError("invalid_verification_code")
        if remember_me:
            latest_a["remember_session_enabled"] = True
            if _cfg_remember_session_days(latest_a) == 30:
                latest_a["remember_session_days"] = DEFAULT_REMEMBER_ME_DAYS
        token2, exp2 = _issue_session(latest, req, latest_user)
        return latest_user, token2, exp2

    try:
        cfg, login_result = _update_config(_mutate_login)
        user, token, exp = login_result
    except ValueError:
        st = _rate_limit_fail(req)
        return JSONResponse(
            _login_error_payload(error="Invalid credentials", attempts=st["n"], retry_after=st["retry_after"]),
            status_code=401,
            headers={"Cache-Control": "no-store"},
        )
    _audit(req, "login", actor=user, message=f"{user.get('username') or 'User'} logged in", target_type="user", target_id=user.get("id"), fields={"remember_me": remember_me})
    resp = JSONResponse({"ok": True, "expires_at": exp, "user": user}, headers={"Cache-Control": "no-store"})
    _set_cookie(resp, token, exp, req, persistent=remember_me)
    return resp


@router.post("/logout")
def api_logout(request: Request) -> JSONResponse:
    cfg = load_config()
    token = request.cookies.get(COOKIE_NAME)
    if auth_required(cfg) and token and not _origin_allowed(request):
        return _origin_blocked_response()
    actor = _session_identity(_cfg_auth(cfg), _find_session(_cfg_auth(cfg), token))
    _update_config(lambda latest: _drop_session(latest, token))
    _audit(request, "logout", actor=actor, message=f"{(actor or {}).get('username') or 'User'} logged out", target_type="user", target_id=(actor or {}).get("id"))
    resp = JSONResponse({"ok": True}, headers={"Cache-Control": "no-store"})
    _del_cookie(resp, request)
    return resp


@router.post("/logout-all")
def api_logout_all(request: Request) -> JSONResponse:
    cfg = load_config()
    token = request.cookies.get(COOKIE_NAME)
    actor: dict[str, Any] | None = None
    if auth_required(cfg):
        if not is_authenticated(cfg, token):
            return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})
        if not _origin_allowed(request):
            return _origin_blocked_response()
        actor = _session_identity(_cfg_auth(cfg), _find_session(_cfg_auth(cfg), token))
    _update_config(lambda latest: _clear_sessions(latest))
    _audit(request, "logout_all", actor=actor, message=f"{(actor or {}).get('username') or 'User'} cleared all sessions", target_type="user", target_id=(actor or {}).get("id"))
    resp = JSONResponse({"ok": True}, headers={"Cache-Control": "no-store"})
    _del_cookie(resp, request)
    return resp


@router.post("/logout-others")
def api_logout_others(request: Request) -> JSONResponse:
    cfg = load_config()
    token = request.cookies.get(COOKIE_NAME)
    actor: dict[str, Any] | None = None
    if auth_required(cfg):
        if not is_authenticated(cfg, token):
            return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})
        if not _origin_allowed(request):
            return _origin_blocked_response()
        actor = _session_identity(_cfg_auth(cfg), _find_session(_cfg_auth(cfg), token))
    _update_config(lambda latest: _clear_other_sessions(latest, token))
    _audit(request, "logout_others", actor=actor, message=f"{(actor or {}).get('username') or 'User'} cleared other sessions", target_type="user", target_id=(actor or {}).get("id"))
    return JSONResponse({"ok": True}, headers={"Cache-Control": "no-store"})


@router.post("/apply-now")
def api_apply_now(request: Request, payload: dict[str, Any] | None = Body(None)) -> JSONResponse:
    cfg = load_config()
    token = request.cookies.get(COOKIE_NAME)

    if auth_required(cfg) and not is_admin_authenticated(cfg, token):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})
    if auth_required(cfg) and token and not _origin_allowed(request):
        return _origin_blocked_response()

    _update_config(lambda latest: _clear_sessions(latest))

    def _kill() -> None:
        os._exit(0)

    threading.Timer(0.75, _kill).start()

    resp = JSONResponse({"ok": True}, headers={"Cache-Control": "no-store"})
    _del_cookie(resp, request)
    return resp


def _profile_id_valid(cfg: dict[str, Any], profile_id: Any) -> bool:
    pid = normalize_user_profile_id(profile_id)
    if not pid:
        return False
    return any(str(row.get("id") or "") == pid for row in list_user_profiles(cfg))


def _managed_user_response(user_id: str, raw: dict[str, Any]) -> dict[str, Any]:
    public = _public_user(user_id, raw)
    public["protected"] = False
    return public


@router.get("/users")
def api_users_all(request: Request) -> JSONResponse:
    cfg = load_config()
    if not auth_required(cfg):
        return JSONResponse({"ok": False, "error": "Authentication is not configured"}, status_code=403, headers={"Cache-Control": "no-store"})
    blocked = _admin_required(request, cfg)
    if blocked is not None:
        return blocked
    a = _cfg_auth(cfg)
    admin = _admin_identity(a)
    admin.update({"enabled": credentials_configured(cfg), "protected": True})
    return JSONResponse({"ok": True, "items": [admin, *_list_managed_users(a)]}, headers={"Cache-Control": "no-store"})


@router.post("/users")
def api_users_create(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    cfg = load_config()
    if not auth_required(cfg):
        return JSONResponse({"ok": False, "error": "Authentication is not configured"}, status_code=403, headers={"Cache-Control": "no-store"})
    blocked = _admin_required(request, cfg)
    if blocked is not None:
        return blocked
    a = _cfg_auth(cfg)
    username = _clean_username((payload or {}).get("username") or (payload or {}).get("label"))
    password = str((payload or {}).get("password") or "")
    profile_id = normalize_user_profile_id((payload or {}).get("profile_id"))
    if not username:
        return JSONResponse({"ok": False, "error": "Username is required"}, status_code=400, headers={"Cache-Control": "no-store"})
    if _username_in_use(a, username):
        return JSONResponse({"ok": False, "error": "Username already exists"}, status_code=409, headers={"Cache-Control": "no-store"})
    if len(password) < MIN_PASSWORD_LENGTH:
        return JSONResponse({"ok": False, "error": f"Password must be at least {MIN_PASSWORD_LENGTH} characters"}, status_code=400, headers={"Cache-Control": "no-store"})
    if not _profile_id_valid(cfg, profile_id):
        return JSONResponse({"ok": False, "error": "User profile is required"}, status_code=400, headers={"Cache-Control": "no-store"})

    def _mutate(latest: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        latest_a = _cfg_auth(latest)
        if _username_in_use(latest_a, username):
            raise ValueError("Username already exists")
        if not _profile_id_valid(latest, profile_id):
            raise KeyError("User profile is required")
        users = _cfg_users(latest_a, create=True)
        user_id = _generate_app_user_id(latest_a)
        users[user_id] = {
            "username": username,
            "enabled": bool((payload or {}).get("enabled", True)),
            "role": "user",
            "profile_id": profile_id,
            "permissions": _clean_create_permissions((payload or {}).get("permissions")),
            "password": _password_hash(password),
            "created_at": _now(),
        }
        return user_id, users[user_id]

    try:
        cfg, created = _update_config(_mutate)
        user_id, raw = created
    except ValueError:
        return JSONResponse({"ok": False, "error": _ERR_CREATE_USER_FAILED}, status_code=409, headers={"Cache-Control": "no-store"})
    except KeyError:
        return JSONResponse({"ok": False, "error": _ERR_CREATE_USER_FAILED}, status_code=400, headers={"Cache-Control": "no-store"})
    _audit(
        request,
        "user_created",
        actor=current_user(cfg, request.cookies.get(COOKIE_NAME)),
        target_type="managed_user",
        target_id=user_id,
        message=f"Managed user {username} was created",
        fields={"username": username, "profile_id": profile_id, "enabled": raw["enabled"], "write": bool(raw["permissions"].get("write"))},
    )
    return JSONResponse({"ok": True, "user": _managed_user_response(user_id, raw)}, headers={"Cache-Control": "no-store"})


@router.put("/users/{user_id}")
def api_users_update(request: Request, user_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    cfg = load_config()
    if not auth_required(cfg):
        return JSONResponse({"ok": False, "error": "Authentication is not configured"}, status_code=403, headers={"Cache-Control": "no-store"})
    blocked = _admin_required(request, cfg)
    if blocked is not None:
        return blocked
    a = _cfg_auth(cfg)
    uid = _normalize_app_user_id(user_id)
    users = _cfg_users(a)
    raw = users.get(uid)
    if not uid or not isinstance(raw, dict):
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404, headers={"Cache-Control": "no-store"})
    password = str(payload.get("password") or "")
    if password:
        if len(password) < MIN_PASSWORD_LENGTH:
            return JSONResponse({"ok": False, "error": f"Password must be at least {MIN_PASSWORD_LENGTH} characters"}, status_code=400, headers={"Cache-Control": "no-store"})

    def _mutate(latest: dict[str, Any]) -> dict[str, Any]:
        latest_a = _cfg_auth(latest)
        users = _cfg_users(latest_a)
        raw = users.get(uid)
        if not uid or not isinstance(raw, dict):
            raise KeyError("Not found")
        if "username" in payload or "label" in payload:
            username = _clean_username(payload.get("username") if "username" in payload else payload.get("label"))
            if not username:
                raise ValueError("Username is required")
            if _username_in_use(latest_a, username, exclude_id=uid):
                raise RuntimeError("Username already exists")
            raw["username"] = username
        if "enabled" in payload:
            raw["enabled"] = bool(payload.get("enabled"))
            if not raw["enabled"]:
                _clear_user_sessions(latest, uid)
        if "profile_id" in payload:
            profile_id = normalize_user_profile_id(payload.get("profile_id"))
            if not _profile_id_valid(latest, profile_id):
                raise ValueError("User profile is required")
            raw["profile_id"] = profile_id
        if "permissions" in payload:
            raw["permissions"] = _clean_permissions(payload.get("permissions"))
        if password:
            raw["password"] = _password_hash(password)
            _clear_user_sessions(latest, uid)
        raw["role"] = "user"
        return raw

    try:
        cfg, raw = _update_config(_mutate)
    except KeyError:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404, headers={"Cache-Control": "no-store"})
    except RuntimeError:
        return JSONResponse({"ok": False, "error": _ERR_UPDATE_USER_FAILED}, status_code=409, headers={"Cache-Control": "no-store"})
    except ValueError:
        return JSONResponse({"ok": False, "error": _ERR_UPDATE_USER_FAILED}, status_code=400, headers={"Cache-Control": "no-store"})
    _audit(
        request,
        "user_updated",
        actor=current_user(cfg, request.cookies.get(COOKIE_NAME)),
        target_type="managed_user",
        target_id=uid,
        message=f"Managed user {raw.get('username') or uid} was updated",
        fields={"username": raw.get("username"), "profile_id": raw.get("profile_id"), "enabled": raw.get("enabled"), "permissions_changed": "permissions" in payload, "password_changed": bool(password)},
    )
    return JSONResponse({"ok": True, "user": _managed_user_response(uid, raw)}, headers={"Cache-Control": "no-store"})


@router.delete("/users/{user_id}")
def api_users_delete(request: Request, user_id: str) -> JSONResponse:
    cfg = load_config()
    if not auth_required(cfg):
        return JSONResponse({"ok": False, "error": "Authentication is not configured"}, status_code=403, headers={"Cache-Control": "no-store"})
    blocked = _admin_required(request, cfg)
    if blocked is not None:
        return blocked
    uid = _normalize_app_user_id(user_id)
    if uid == ADMIN_USER_ID:
        return JSONResponse({"ok": False, "error": "Administrator cannot be deleted"}, status_code=400, headers={"Cache-Control": "no-store"})

    def _mutate(latest: dict[str, Any]) -> tuple[bool, str]:
        a = _cfg_auth(latest)
        users = _cfg_users(a)
        if not uid or uid not in users:
            return False, uid
        raw = users.get(uid)
        username = str(raw.get("username") or uid) if isinstance(raw, dict) else uid
        users.pop(uid, None)
        _clear_user_sessions(latest, uid)
        return True, username

    cfg, result = _update_config(_mutate)
    deleted, username = result
    _audit(request, "user_deleted", actor=current_user(cfg, request.cookies.get(COOKIE_NAME)), target_type="managed_user", target_id=uid, message=f"Managed user {username} was deleted", fields={"username": username})
    return JSONResponse({"ok": True, "deleted": deleted}, headers={"Cache-Control": "no-store"})


@router.post("/credentials")
def api_set_credentials(request: Request, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    req = request
    cfg = load_config()
    configured0 = auth_required(cfg)
    recovery_mode = reset_pending(cfg)
    token = req.cookies.get(COOKIE_NAME)

    if configured0 and not recovery_mode and not is_admin_authenticated(cfg, token):
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
        actor = current_user(cfg, token)

        def _mutate_disable(latest: dict[str, Any]) -> dict[str, Any]:
            latest_a = latest.setdefault("app_auth", {})
            if not isinstance(latest_a, dict):
                latest_a = {}
                latest["app_auth"] = latest_a
            latest_a["remember_session_enabled"] = bool(payload.get("remember_session_enabled", latest_a.get("remember_session_enabled", False)))
            latest_a["remember_session_days"] = remember_days
            latest_a["enabled"] = False
            latest_a["reset_required"] = False
            latest_a["username"] = username or str(latest_a.get("username") or "")
            _clear_sessions(latest)
            return latest_a

        cfg, a = _update_config(_mutate_disable)
        _audit(req, "credentials_updated", actor=actor, message="Application authentication was disabled", fields={"enabled": False, "username": a["username"]})
        resp = JSONResponse({"ok": True, "enabled": False}, headers={"Cache-Control": "no-store"})
        _del_cookie(resp, req)
        return resp

    if not username:
        return JSONResponse({"ok": False, "error": "Username is required"}, status_code=400)
    if _username_in_use(a, username, exclude_id=ADMIN_USER_ID):
        return JSONResponse({"ok": False, "error": "Username already exists"}, status_code=409)

    if not int(a.get("created_at") or 0):
        a["created_at"] = _now()

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

    def _mutate_enable(latest: dict[str, Any]) -> tuple[dict[str, Any], str, int]:
        latest_a = latest.setdefault("app_auth", {})
        if not isinstance(latest_a, dict):
            latest_a = {}
            latest["app_auth"] = latest_a
        latest_a["remember_session_enabled"] = bool(payload.get("remember_session_enabled", latest_a.get("remember_session_enabled", False)))
        latest_a["remember_session_days"] = remember_days
        if _username_in_use(latest_a, username, exclude_id=ADMIN_USER_ID):
            raise RuntimeError("Username already exists")
        latest_pwd = latest_a.setdefault("password", {})
        if not isinstance(latest_pwd, dict):
            latest_pwd = {}
            latest_a["password"] = latest_pwd
        latest_has_existing = bool(str(latest_pwd.get("hash") or "").strip() and str(latest_pwd.get("salt") or "").strip())
        if not password and not latest_has_existing:
            raise ValueError("Password is required")
        if password:
            latest_pwd.update(_password_hash(password))
        latest_a["enabled"] = True
        latest_a["username"] = username
        latest_a["reset_required"] = False
        _clear_sessions(latest)
        _clear_setup_autogen_flag(latest)
        _mark_upgrade_pending_if_needed(latest)
        token2, exp2 = _issue_session(latest, req)
        return latest_a, token2, exp2

    try:
        cfg, enabled_result = _update_config(_mutate_enable)
    except RuntimeError:
        return JSONResponse({"ok": False, "error": _ERR_UPDATE_CREDENTIALS_FAILED}, status_code=409)
    except ValueError:
        return JSONResponse({"ok": False, "error": _ERR_UPDATE_CREDENTIALS_FAILED}, status_code=400)
    a, token2, exp2 = enabled_result
    _audit(req, "credentials_updated", actor=current_user(cfg, token2), message="Application authentication credentials were updated", fields={"enabled": True, "username": username, "password_changed": bool(password), "remember_session_enabled": a.get("remember_session_enabled"), "remember_session_days": a.get("remember_session_days")})

    resp = JSONResponse({"ok": True, "enabled": True, "expires_at": exp2}, headers={"Cache-Control": "no-store"})
    _set_cookie(resp, token2, exp2, req, persistent=_cfg_remember_session_enabled(a))
    return resp


@router.post("/totp/setup")
def api_totp_setup(request: Request, payload: dict[str, Any] | None = Body(default_factory=dict)) -> JSONResponse:
    cfg = load_config()
    if not auth_required(cfg):
        return JSONResponse({"ok": False, "error": "Authentication is not configured"}, status_code=403, headers={"Cache-Control": "no-store"})
    token = request.cookies.get(COOKIE_NAME)
    actor = current_user(cfg, token)
    if not actor:
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})
    if not actor.get("is_admin"):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})
    if token and not _origin_allowed(request):
        return _origin_blocked_response()
    secret = _totp_new_secret()

    def _mutate(latest: dict[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any]]:
        a = _cfg_auth(latest)
        target_id, target_user, _raw = _target_user_for_totp(a, actor, (payload or {}).get("user_id"))
        if not target_id:
            raise KeyError("not_found")
        t = _totp_record_for_user(a, target_user)
        t["pending_secret"] = secret
        t["pending_created_at"] = _now()
        return target_id, target_user, t

    try:
        cfg, result = _update_config(_mutate)
        target_id, target_user, t = result
    except KeyError:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404, headers={"Cache-Control": "no-store"})
    _audit(request, "totp_setup", actor=actor, target_type="user", target_id=target_id, message=f"Two-factor setup was started for {target_user.get('username') or target_id}")
    return JSONResponse(
        {
            "ok": True,
            "user_id": target_id,
            "username": str(target_user.get("username") or ""),
            "secret": secret,
            "otpauth_url": _totp_uri(target_user.get("username"), secret),
            "enabled": _totp_enabled(t),
        },
        headers={"Cache-Control": "no-store"},
    )


@router.post("/totp/verify")
def api_totp_verify(request: Request, payload: dict[str, Any] | None = Body(default_factory=dict)) -> JSONResponse:
    cfg = load_config()
    if not auth_required(cfg):
        return JSONResponse({"ok": False, "error": "Authentication is not configured"}, status_code=403, headers={"Cache-Control": "no-store"})
    token = request.cookies.get(COOKIE_NAME)
    actor = current_user(cfg, token)
    if not actor:
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})
    if not actor.get("is_admin"):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})
    if token and not _origin_allowed(request):
        return _origin_blocked_response()

    def _mutate(latest: dict[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any]]:
        a = _cfg_auth(latest)
        target_id, target_user, _raw = _target_user_for_totp(a, actor, (payload or {}).get("user_id"))
        if not target_id:
            raise KeyError("not_found")
        t = _totp_record_for_user(a, target_user)
        pending = str(t.get("pending_secret") or "").strip()
        if not pending:
            raise ValueError("No setup pending")
        if not _totp_verify(pending, (payload or {}).get("code") or (payload or {}).get("totp_code")):
            raise RuntimeError("Invalid verification code")
        t["secret"] = pending
        t["enabled"] = True
        t.pop("pending_secret", None)
        t.pop("pending_created_at", None)
        _clear_user_sessions_except(latest, target_id, token)
        return target_id, target_user, t

    try:
        cfg, result = _update_config(_mutate)
        target_id, target_user, t = result
    except KeyError:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404, headers={"Cache-Control": "no-store"})
    except ValueError:
        return JSONResponse({"ok": False, "error": _ERR_VERIFY_TOTP_FAILED}, status_code=400, headers={"Cache-Control": "no-store"})
    except RuntimeError:
        return JSONResponse({"ok": False, "error": _ERR_VERIFY_TOTP_FAILED}, status_code=400, headers={"Cache-Control": "no-store"})
    _audit(request, "totp_enabled", actor=actor, target_type="user", target_id=target_id, message=f"Two-factor authentication was enabled for {target_user.get('username') or target_id}")
    return JSONResponse({"ok": True, "user_id": target_id, "totp": _totp_public(t)}, headers={"Cache-Control": "no-store"})


@router.post("/totp/disable")
def api_totp_disable(request: Request, payload: dict[str, Any] | None = Body(default_factory=dict)) -> JSONResponse:
    cfg = load_config()
    if not auth_required(cfg):
        return JSONResponse({"ok": False, "error": "Authentication is not configured"}, status_code=403, headers={"Cache-Control": "no-store"})
    token = request.cookies.get(COOKIE_NAME)
    actor = current_user(cfg, token)
    if not actor:
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})
    if not actor.get("is_admin"):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})
    if token and not _origin_allowed(request):
        return _origin_blocked_response()

    def _mutate(latest: dict[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any]]:
        a = _cfg_auth(latest)
        target_id, target_user, _raw = _target_user_for_totp(a, actor, (payload or {}).get("user_id"))
        if not target_id:
            raise KeyError("not_found")
        t = _totp_record_for_user(a, target_user)
        t["enabled"] = False
        t["secret"] = ""
        t.pop("pending_secret", None)
        t.pop("pending_created_at", None)
        _clear_user_sessions_except(latest, target_id, token)
        return target_id, target_user, t

    try:
        cfg, result = _update_config(_mutate)
        target_id, target_user, t = result
    except KeyError:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404, headers={"Cache-Control": "no-store"})
    _audit(request, "totp_disabled", actor=actor, target_type="user", target_id=target_id, message=f"Two-factor authentication was disabled for {target_user.get('username') or target_id}")
    return JSONResponse({"ok": True, "user_id": target_id, "totp": _totp_public(t)}, headers={"Cache-Control": "no-store"})


def register_app_auth(app) -> None:
    app.include_router(router)
    try:
        from .authPlexAPI import register_auth_plex

        register_auth_plex(app)
    except Exception:
        _LOG.exception("Plex SSO routes failed to register - Plex sign-in will be unavailable")
    try:
        from .authOidcAPI import register_auth_oidc

        register_auth_oidc(app)
    except Exception:
        _LOG.exception("OIDC routes failed to register - OIDC sign-in will be unavailable")

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
        try:
            from services import authOidc

            oidc_available = authOidc.login_available(cfg)
        except Exception:
            oidc_available = False
        return HTMLResponse(_login_html(username, plex_sso_available=plex_sso_available, oidc_available=oidc_available), headers={"Cache-Control": "no-store"})

    @app.get("/logout", include_in_schema=False, tags=["ui"])
    def ui_logout(request: Request) -> Response:
        cfg = load_config()
        token = request.cookies.get(COOKIE_NAME)
        actor = _session_identity(_cfg_auth(cfg), _find_session(_cfg_auth(cfg), token))
        _update_config(lambda latest: _drop_session(latest, token))
        _audit(request, "logout", actor=actor, message=f"{(actor or {}).get('username') or 'User'} logged out", target_type="user", target_id=(actor or {}).get("id"))
        resp = RedirectResponse(url="/login" if auth_required(cfg) else "/")
        _del_cookie(resp, request)
        return resp
