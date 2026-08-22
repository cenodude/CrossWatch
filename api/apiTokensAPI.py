# /api/apiTokensAPI.py
# CrossWatch - API tokens for headless and CLI access
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hmac
import re
import secrets
import time
from typing import Any

from fastapi import APIRouter, Body, Request
from fastapi.responses import JSONResponse

from cw_platform.config_base import _load_config_key, load_config

from .appAuthAPI import (
    ADMIN_USER_ID,
    COOKIE_NAME,
    _admin_identity,
    _audit,
    _cfg_auth,
    _cfg_users,
    _normalize_app_user_id,
    _now,
    _origin_allowed,
    _origin_blocked_response,
    _password_matches,
    _public_user,
    _update_config,
    auth_required,
    current_user,
)

TOKEN_PREFIX = "cwt_"
TOKEN_HEADER = "x-cw-token"
MAX_TOKENS_PER_USER = 20
TOUCH_INTERVAL_SEC = 300
TOKEN_MAX_LENGTH = 256
TOKEN_V2_SECRET_BYTES = 32

_TOKEN_V2_RE = re.compile(r"^cwt_([a-f0-9]{16})\.([A-Za-z0-9_-]{32,})$")
_SHA256_HEX_RE = re.compile(r"^[a-f0-9]{64}$")
_TRUSTED_LOCAL_TOKEN_ORIGINS = {"local_cli", "local_bootstrap"}

_TOUCH_CACHE: dict[str, float] = {}


def _nostore(payload: dict[str, Any], status: int = 200) -> JSONResponse:
    return JSONResponse(payload, status_code=status, headers={"Cache-Control": "no-store"})


def _cfg_api_tokens(a: dict[str, Any]) -> list[dict[str, Any]]:
    raw = a.get("api_tokens")
    if not isinstance(raw, list):
        return []
    return [x for x in raw if isinstance(x, dict)]


def _valid_token_hash(raw: Any) -> bool:
    if not isinstance(raw, dict):
        return False
    if str(raw.get("scheme") or "") != "pbkdf2_sha256":
        return False
    if not str(raw.get("salt") or "").strip():
        return False
    if not str(raw.get("hash") or "").strip():
        return False
    try:
        return int(raw.get("iterations") or 0) > 0
    except Exception:
        return False


def _token_digest(raw: str, *, create_key: bool = False) -> str:
    key = _load_config_key(create=create_key)
    if not key:
        return ""
    return hmac.new(key, str(raw or "").encode("utf-8"), "sha256").hexdigest()


def _valid_v1_entry(entry: dict[str, Any]) -> bool:
    if not _valid_token_hash(entry.get("token_hash")):
        return False
    if not str(entry.get("id") or "").strip():
        return False
    exp = int(entry.get("expires_at") or 0)
    return exp <= 0 or exp > _now()


def _valid_v2_entry(entry: dict[str, Any]) -> bool:
    try:
        version = int(entry.get("version") or 0)
    except Exception:
        return False
    if version != 2:
        return False
    if not re.fullmatch(r"[a-f0-9]{16}", str(entry.get("id") or "")):
        return False
    digest = str(entry.get("token_digest") or "")
    if str(entry.get("digest_scheme") or "") != "hmac_sha256" or not _SHA256_HEX_RE.fullmatch(digest):
        return False
    exp = int(entry.get("expires_at") or 0)
    return exp <= 0 or exp > _now()


def _valid_entry(entry: dict[str, Any]) -> bool:
    return _valid_v2_entry(entry) or _valid_v1_entry(entry)


def _prune_api_tokens(tokens: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keep = [t for t in tokens if _valid_entry(t)]
    buckets: dict[str, list[dict[str, Any]]] = {}
    for entry in keep:
        buckets.setdefault(_entry_user_id(entry), []).append(entry)
    out: list[dict[str, Any]] = []
    for entries in buckets.values():
        entries.sort(key=lambda x: int(x.get("created_at") or 0))
        out.extend(entries[-MAX_TOKENS_PER_USER:])
    out.sort(key=lambda x: int(x.get("created_at") or 0))
    return out


def _entry_user_id(entry: dict[str, Any]) -> str:
    raw = entry.get("user_id")
    if raw is None or str(raw).strip() == "":
        return ADMIN_USER_ID
    return _normalize_app_user_id(raw) or ADMIN_USER_ID


def _entry_identity(a: dict[str, Any], entry: dict[str, Any]) -> dict[str, Any] | None:
    user_id = _entry_user_id(entry)
    if user_id == ADMIN_USER_ID:
        return _admin_identity(a)
    raw = _cfg_users(a).get(user_id)
    if not isinstance(raw, dict):
        return None
    public = _public_user(user_id, raw)
    return public if public["enabled"] else None


def _public_entry(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(entry.get("id") or ""),
        "version": 2 if _valid_v2_entry(entry) else 1,
        "name": str(entry.get("name") or ""),
        "prefix": str(entry.get("prefix") or ""),
        "user_id": _entry_user_id(entry),
        "username": str(entry.get("username") or ""),
        "created_at": int(entry.get("created_at") or 0),
        "expires_at": int(entry.get("expires_at") or 0),
        "last_used_at": int(entry.get("last_used_at") or 0),
    }


def extract_api_token(request: Request) -> str:
    try:
        headers = request.headers
    except Exception:
        return ""
    raw = str(headers.get(TOKEN_HEADER) or "").strip()
    if raw:
        return raw
    auth = str(headers.get("authorization") or "").strip()
    if len(auth) > 7 and auth[:7].lower() == "bearer ":
        return auth[7:].strip()
    return ""


def resolve_api_token(cfg: dict[str, Any], raw: str) -> dict[str, Any] | None:
    token = str(raw or "").strip()
    if not token or len(token) > TOKEN_MAX_LENGTH or not token.startswith(TOKEN_PREFIX):
        return None
    a = _cfg_auth(cfg)
    tokens = _cfg_api_tokens(a)
    if not tokens:
        return None

    v2_match = _TOKEN_V2_RE.fullmatch(token)
    if v2_match:
        token_id = v2_match.group(1)
        digest = _token_digest(token)
        if not digest:
            return None
        for entry in tokens:
            if str(entry.get("id") or "") != token_id or not _valid_v2_entry(entry):
                continue
            if not hmac.compare_digest(digest, str(entry.get("token_digest") or "")):
                return None
            identity = _entry_identity(a, entry)
            if identity is None:
                return None
            out = dict(identity)
            out["auth_kind"] = "api_token"
            out["api_token_id"] = str(entry.get("id") or "")
            out["api_token_name"] = str(entry.get("name") or "")
            return out
        return None

    if "." in token:
        return None

    for entry in tokens:
        if not _valid_v1_entry(entry):
            continue
        token_hash = entry.get("token_hash")
        if not isinstance(token_hash, dict) or not _password_matches(token_hash, token):
            continue
        identity = _entry_identity(a, entry)
        if identity is None:
            return None
        out = dict(identity)
        out["auth_kind"] = "api_token"
        out["api_token_id"] = str(entry.get("id") or "")
        out["api_token_name"] = str(entry.get("name") or "")
        return out
    return None


def touch_api_token(token_id: str) -> None:
    tid = str(token_id or "").strip()
    if not tid:
        return
    now = time.time()
    last = _TOUCH_CACHE.get(tid) or 0.0
    if (now - last) < TOUCH_INTERVAL_SEC:
        return
    _TOUCH_CACHE[tid] = now

    def _mutate(latest: dict[str, Any]) -> None:
        a = latest.get("app_auth")
        if not isinstance(a, dict):
            return
        for entry in _cfg_api_tokens(a):
            if str(entry.get("id") or "") == tid:
                entry["last_used_at"] = _now()
                return

    try:
        _update_config(_mutate)
    except Exception:
        pass


def issue_api_token(
    cfg: dict[str, Any],
    *,
    name: str,
    user_id: str = "",
    expires_days: int = 0,
    created_via: str = "api",
) -> tuple[str, dict[str, Any]]:
    label = str(name or "").strip()[:64] or "CLI"
    days = max(0, int(expires_days or 0))
    entry_id = secrets.token_hex(8)
    secret = secrets.token_urlsafe(TOKEN_V2_SECRET_BYTES)
    raw = f"{TOKEN_PREFIX}{entry_id}.{secret}"
    created = _now()
    expires = created + days * 86400 if days else 0
    target = _normalize_app_user_id(user_id) or ADMIN_USER_ID
    origin = str(created_via or "api").strip()
    if origin not in {*_TRUSTED_LOCAL_TOKEN_ORIGINS, "api"}:
        origin = "api"

    def _mutate(latest: dict[str, Any]) -> dict[str, Any]:
        a = latest.setdefault("app_auth", {})
        if not isinstance(a, dict):
            a = {}
            latest["app_auth"] = a
        identity = None
        if target == ADMIN_USER_ID:
            identity = _admin_identity(a)
        else:
            found = _cfg_users(a).get(target)
            if isinstance(found, dict):
                identity = _public_user(target, found)
        if identity is None:
            raise KeyError("not_found")
        entry = {
            "id": entry_id,
            "version": 2,
            "name": label,
            "token_digest": _token_digest(raw, create_key=True),
            "digest_scheme": "hmac_sha256",
            "prefix": f"{TOKEN_PREFIX}{entry_id}.{secret[:6]}",
            "user_id": target,
            "username": str(identity.get("username") or ""),
            "created_via": origin,
            "created_at": created,
            "expires_at": expires,
            "last_used_at": 0,
        }
        a["api_tokens"] = _prune_api_tokens([*_cfg_api_tokens(a), entry])
        return entry

    _cfg, entry = _update_config(_mutate)
    return raw, _public_entry(entry)


def list_api_tokens(cfg: dict[str, Any], *, user_id: str = "") -> list[dict[str, Any]]:
    a = _cfg_auth(cfg)
    entries = _prune_api_tokens(_cfg_api_tokens(a))
    scope = _normalize_app_user_id(user_id)
    if scope:
        entries = [e for e in entries if _entry_user_id(e) == scope]
    return [_public_entry(e) for e in entries]


def revoke_api_token(token_id: str, *, user_id: str = "") -> bool:
    tid = str(token_id or "").strip()
    if not tid:
        return False
    scope = _normalize_app_user_id(user_id)

    def _mutate(latest: dict[str, Any]) -> bool:
        a = latest.get("app_auth")
        if not isinstance(a, dict):
            return False
        tokens = _cfg_api_tokens(a)
        keep = []
        removed = False
        for entry in tokens:
            match = str(entry.get("id") or "") == tid
            if match and scope and _entry_user_id(entry) != scope:
                match = False
            if match:
                removed = True
                continue
            keep.append(entry)
        if removed:
            a["api_tokens"] = keep
        return removed

    _cfg, removed = _update_config(_mutate)
    _TOUCH_CACHE.pop(tid, None)
    return bool(removed)


def revoke_all_api_tokens(*, user_id: str = "") -> int:
    scope = _normalize_app_user_id(user_id)

    def _mutate(latest: dict[str, Any]) -> int:
        a = latest.get("app_auth")
        if not isinstance(a, dict):
            return 0
        tokens = _cfg_api_tokens(a)
        keep = [e for e in tokens if scope and _entry_user_id(e) != scope]
        a["api_tokens"] = keep
        return len(tokens) - len(keep)

    _cfg, count = _update_config(_mutate)
    _TOUCH_CACHE.clear()
    return int(count or 0)


router = APIRouter(prefix="/api/app-auth/tokens", tags=["app-auth"])


def _actor(request: Request) -> tuple[dict[str, Any] | None, JSONResponse | None]:
    cfg = load_config()
    if not auth_required(cfg):
        return None, _nostore({"ok": False, "error": "Authentication is not configured"}, 403)
    token = request.cookies.get(COOKIE_NAME)
    user = current_user(cfg, token)
    if user is None:
        api_user = resolve_api_token(cfg, extract_api_token(request))
        if api_user is not None:
            return api_user, None
        return None, _nostore({"ok": False, "error": "Unauthorized"}, 401)
    if not user.get("is_admin"):
        return None, _nostore({"ok": False, "error": "Administrator access required"}, 403)
    if token and not _origin_allowed(request):
        return None, _origin_blocked_response()
    return user, None


@router.get("/whoami")
def api_tokens_whoami(request: Request) -> JSONResponse:
    cfg = load_config()
    if not auth_required(cfg):
        return _nostore({"ok": True, "auth_required": False, "user": None})
    actor = current_user(cfg, request.cookies.get(COOKIE_NAME))
    kind = "session"
    if actor is None:
        actor = resolve_api_token(cfg, extract_api_token(request))
        kind = "api_token"
    if actor is None:
        return _nostore({"ok": False, "error": "Unauthorized"}, 401)
    return _nostore(
        {
            "ok": True,
            "auth_required": True,
            "auth_kind": kind,
            "token_id": str(actor.get("api_token_id") or ""),
            "token_name": str(actor.get("api_token_name") or ""),
            "user": {
                "id": actor.get("id"),
                "username": actor.get("username"),
                "display_name": actor.get("display_name"),
                "is_admin": bool(actor.get("is_admin")),
                "profile_id": actor.get("profile_id"),
            },
        }
    )


@router.get("")
def api_tokens_list(request: Request) -> JSONResponse:
    actor, err = _actor(request)
    if err is not None:
        return err
    cfg = load_config()
    scope = "" if (actor or {}).get("is_admin") else str((actor or {}).get("id") or "")
    return _nostore({"ok": True, "tokens": list_api_tokens(cfg, user_id=scope)})


@router.post("")
def api_tokens_create(request: Request, payload: dict[str, Any] | None = Body(default_factory=dict)) -> JSONResponse:
    actor, err = _actor(request)
    if err is not None:
        return err
    body = payload or {}
    name = str(body.get("name") or "").strip()
    try:
        expires_days = int(body.get("expires_days") or 0)
    except Exception:
        expires_days = 0
    target = str(body.get("user_id") or "").strip()
    if not (actor or {}).get("is_admin"):
        target = str((actor or {}).get("id") or "")
    cfg = load_config()
    try:
        raw, entry = issue_api_token(cfg, name=name, user_id=target, expires_days=expires_days)
    except KeyError:
        return _nostore({"ok": False, "error": "Not found"}, 404)
    _audit(
        request,
        "api_token_created",
        actor=actor,
        target_type="api_token",
        target_id=entry.get("id"),
        message=f"API token {entry.get('name') or entry.get('id')} was created",
        fields={"expires_at": entry.get("expires_at")},
    )
    return _nostore({"ok": True, "token": raw, "entry": entry})


@router.delete("/{token_id}")
def api_tokens_revoke(request: Request, token_id: str) -> JSONResponse:
    actor, err = _actor(request)
    if err is not None:
        return err
    scope = "" if (actor or {}).get("is_admin") else str((actor or {}).get("id") or "")
    removed = revoke_api_token(token_id, user_id=scope)
    if not removed:
        return _nostore({"ok": False, "error": "Not found"}, 404)
    _audit(
        request,
        "api_token_revoked",
        actor=actor,
        target_type="api_token",
        target_id=token_id,
        message=f"API token {token_id} was revoked",
    )
    return _nostore({"ok": True, "revoked": token_id})


def register_api_tokens(app) -> None:
    app.include_router(router)
