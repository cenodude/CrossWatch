# /api/profileAPI.py
# CrossWatch - User Profile API
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from typing import Any

import base64
import re
import secrets
import time
import urllib.parse
import urllib.request
from pathlib import Path

from fastapi import APIRouter, Body, Request
from fastapi.responses import FileResponse, JSONResponse, Response

from api.appAuthAPI import (
    ADMIN_USER_ID,
    COOKIE_NAME,
    MIN_PASSWORD_LENGTH,
    _admin_identity,
    _audit,
    _cfg_auth,
    _cfg_pwd,
    _cfg_users,
    _clear_user_sessions_except,
    _iter_sessions,
    _normalize_app_user_id,
    _origin_allowed,
    _origin_blocked_response,
    _password_hash,
    _password_matches,
    _prune_sessions,
    _public_session_state,
    _public_user,
    _sync_legacy_session,
    _totp_clean_code,
    _totp_enabled,
    _totp_new_secret,
    _totp_public,
    _totp_record_for_user,
    _totp_uri,
    _totp_verify,
    clean_user_preferences,
    current_user,
)
from cw_platform.config_base import CONFIG as CONFIG_DIR, load_config, update_config
from cw_platform.provider_instances import list_user_profiles

router = APIRouter(prefix="/api/profile", tags=["profile"])

MAX_AVATAR_BYTES = 5 * 1024 * 1024
AVATAR_TYPES = {
    "image/png": (".png", b"\x89PNG\r\n\x1a\n"),
    "image/jpeg": (".jpg", b"\xff\xd8\xff"),
    "image/webp": (".webp", b"RIFF"),
}
DEFAULT_AVATAR_SVG = b'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 128 128"><rect width="128" height="128" rx="64" fill="#202638"/><circle cx="64" cy="49" r="22" fill="#9fb3ff"/><path d="M28 110c5-25 21-38 36-38s31 13 36 38" fill="#9fb3ff"/></svg>'


def _json_error(error: str, status_code: int) -> JSONResponse:
    return JSONResponse({"ok": False, "error": error}, status_code=status_code, headers={"Cache-Control": "no-store"})


ProfileContext = tuple[dict[str, Any], dict[str, Any], str, dict[str, Any], dict[str, Any], str]


def _profile_context_from_config(cfg: dict[str, Any], request: Request) -> ProfileContext | JSONResponse:
    token = request.cookies.get(COOKIE_NAME)
    user = current_user(cfg, token)
    if not user:
        return _json_error("Unauthorized", 401)
    a = _cfg_auth(cfg)
    if user.get("is_admin"):
        return cfg, a, ADMIN_USER_ID, a, _admin_identity(a), str(token or "")
    uid = _normalize_app_user_id(user.get("id"))
    raw = _cfg_users(a, create=True).get(uid)
    if not uid or not isinstance(raw, dict):
        return _json_error("User not found", 404)
    return cfg, a, uid, raw, user, str(token or "")


def _profile_context(request: Request) -> ProfileContext | JSONResponse:
    return _profile_context_from_config(load_config() or {}, request)


def _profile_write(request: Request, mutator: Any) -> tuple[dict[str, Any], Any] | JSONResponse:
    if not _origin_allowed(request):
        return _origin_blocked_response()

    def _mutate(cfg: dict[str, Any]) -> Any:
        ctx = _profile_context_from_config(cfg, request)
        if isinstance(ctx, JSONResponse):
            return ctx
        return mutator(*ctx)

    cfg, result = update_config(_mutate)
    if isinstance(result, JSONResponse):
        return result
    return cfg, result


def _target_type(uid: str) -> str:
    return "admin_user" if uid == ADMIN_USER_ID else "managed_user"


def _account_password(a: dict[str, Any], uid: str, raw: dict[str, Any]) -> dict[str, Any] | None:
    pwd = _cfg_pwd(a) if uid == ADMIN_USER_ID else raw.get("password")
    return pwd if isinstance(pwd, dict) else None


def _avatar_dir() -> Path:
    path = Path(CONFIG_DIR) / "profile_avatars"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _avatar_path(filename: Any) -> Path | None:
    name = Path(str(filename or "")).name
    if not name or not re.fullmatch(r"[a-f0-9]{32}\.(png|jpg|webp)", name):
        return None
    root = _avatar_dir().resolve()
    path = (root / name).resolve()
    if root not in path.parents and path != root:
        return None
    return path


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
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
            continue
        try:
            version = int(updated_at or 0)
        except Exception:
            version = 0
        return url, version
    return "", 0


def _avatar_url(raw: dict[str, Any], user_id: Any = "") -> str:
    avatar = raw.get("avatar")
    version = 0
    if isinstance(avatar, dict):
        path = _avatar_path(avatar.get("file"))
        if path is not None and path.exists():
            try:
                version = max(int(avatar.get("updated_at") or 0), int(path.stat().st_mtime_ns // 1_000_000))
            except Exception:
                version = int(avatar.get("updated_at") or 0)
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


def _avatar_response(raw: dict[str, Any]) -> Response:
    avatar = raw.get("avatar")
    path = _avatar_path(avatar.get("file") if isinstance(avatar, dict) else "")
    if path is None or not path.exists():
        linked_url, _version = _linked_avatar_source(raw)
        if linked_url:
            try:
                req = urllib.request.Request(linked_url, headers={"User-Agent": "CrossWatch/0.11"})
                with urllib.request.urlopen(req, timeout=8) as res:
                    content_type = str(res.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
                    if content_type in AVATAR_TYPES:
                        data = res.read(MAX_AVATAR_BYTES + 1)
                        if len(data) <= MAX_AVATAR_BYTES:
                            return Response(content=data, media_type=content_type, headers={"Cache-Control": "private, max-age=300"})
            except Exception:
                pass
        return Response(content=DEFAULT_AVATAR_SVG, media_type="image/svg+xml", headers={"Cache-Control": "no-store"})
    media_type = str((avatar or {}).get("content_type") or "image/png") if isinstance(avatar, dict) else "image/png"
    return FileResponse(path, media_type=media_type, headers={"Cache-Control": "no-store"})


def _profile_label(cfg: dict[str, Any], profile_id: Any) -> str:
    pid = str(profile_id or "").strip()
    if not pid:
        return ""
    for row in list_user_profiles(cfg):
        if str(row.get("id") or "") == pid:
            return str(row.get("label") or pid)
    return pid


def _public_profile(cfg: dict[str, Any], a: dict[str, Any], uid: str, raw: dict[str, Any], token: str | None) -> dict[str, Any]:
    user = _admin_identity(a) if uid == ADMIN_USER_ID else _public_user(uid, raw)
    user["display_name"] = str(raw.get("display_name") or user.get("display_name") or user.get("username") or "").strip()
    user["label"] = user["display_name"] or str(user.get("username") or "")
    user["avatar_url"] = _avatar_url(raw, uid)
    user["profile_label"] = "Administrator" if uid == ADMIN_USER_ID else _profile_label(cfg, user.get("profile_id"))
    current_session, other_sessions = _public_session_state(a, token)
    other_sessions = [
        row for row in other_sessions
        if _normalize_app_user_id(row.get("user_id")) == uid
    ]
    user["current_session"] = current_session
    user["other_sessions"] = other_sessions
    user["other_session_count"] = len(other_sessions)
    user["recovery_codes_count"] = len(raw.get("recovery_codes") or []) if isinstance(raw.get("recovery_codes"), list) else 0
    user["created_at"] = int(raw.get("created_at") or 0)
    user["preferences"] = clean_user_preferences(raw.get("preferences"))
    return user


def _clean_display_name(value: Any, fallback: str) -> str:
    text = " ".join(str(value or "").strip().split())[:64]
    return text or fallback


def _decode_avatar(payload: dict[str, Any]) -> tuple[bytes, str] | JSONResponse:
    content_type = str(payload.get("content_type") or payload.get("type") or "").split(";", 1)[0].strip().lower()
    data = str(payload.get("data") or "")
    if data.startswith("data:"):
        head, _, body = data.partition(",")
        match = re.match(r"^data:([^;,]+);base64$", head, re.I)
        if match:
            content_type = match.group(1).lower()
        data = body
    if content_type not in AVATAR_TYPES:
        return _json_error("Unsupported image type", 400)
    try:
        raw = base64.b64decode(data, validate=True)
    except Exception:
        return _json_error("Invalid image data", 400)
    if not raw:
        return _json_error("Image is required", 400)
    if len(raw) > MAX_AVATAR_BYTES:
        return _json_error("Profile picture must be 5 MB or smaller", 413)
    _ext, magic = AVATAR_TYPES[content_type]
    if content_type == "image/webp":
        if not (raw.startswith(b"RIFF") and raw[8:12] == b"WEBP"):
            return _json_error("Invalid image file", 400)
    elif not raw.startswith(magic):
        return _json_error("Invalid image file", 400)
    return raw, content_type


def _generate_recovery_codes(raw: dict[str, Any]) -> list[str]:
    codes = [f"{secrets.token_hex(5).upper()}-{secrets.token_hex(5).upper()}" for _ in range(10)]
    raw["recovery_codes"] = [_password_hash(code) for code in codes]
    return codes


@router.get("")
def api_profile_get(request: Request) -> JSONResponse:
    ctx = _profile_context(request)
    if isinstance(ctx, JSONResponse):
        return ctx
    cfg, a, uid, raw, _user, token = ctx
    return JSONResponse({"ok": True, "user": _public_profile(cfg, a, uid, raw, token)}, headers={"Cache-Control": "no-store"})


@router.post("")
def api_profile_update(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    def _mutate(cfg: dict[str, Any], a: dict[str, Any], uid: str, raw: dict[str, Any], user: dict[str, Any], token: str) -> tuple[dict[str, Any], str, dict[str, Any], dict[str, Any], str]:
        if "display_name" in (payload or {}):
            raw["display_name"] = _clean_display_name(payload.get("display_name"), str(raw.get("username") or "User"))
        if "preferences" in (payload or {}):
            current = clean_user_preferences(raw.get("preferences"))
            incoming = (payload or {}).get("preferences")
            merged = dict(current)
            if isinstance(incoming, dict):
                for key in ("playing_card", "quick_add"):
                    if key in incoming:
                        merged[key] = bool(incoming.get(key))
            raw["preferences"] = clean_user_preferences(merged)
        return a, uid, raw, user, token

    updated = _profile_write(request, _mutate)
    if isinstance(updated, JSONResponse):
        return updated
    cfg, (a, uid, raw, user, token) = updated
    _audit(request, "profile_updated", actor=user, target_type=_target_type(uid), target_id=uid, message=f"{user.get('username') or 'User'} updated their profile")
    return JSONResponse({"ok": True, "user": _public_profile(cfg, a, uid, raw, token)}, headers={"Cache-Control": "no-store"})


@router.get("/avatar")
def api_profile_avatar(request: Request) -> Response:
    ctx = _profile_context(request)
    if isinstance(ctx, JSONResponse):
        return ctx
    _cfg, _a, _uid, raw, _user, _token = ctx
    return _avatar_response(raw)


@router.get("/avatar/{user_id}")
def api_profile_avatar_for_user(request: Request, user_id: str) -> Response:
    cfg = load_config() or {}
    token = request.cookies.get(COOKIE_NAME)
    user = current_user(cfg, token)
    if not user:
        return _json_error("Unauthorized", 401)
    wanted = _normalize_app_user_id(user_id)
    current = _normalize_app_user_id(user.get("id"))
    if not wanted:
        return _json_error("User not found", 404)
    if not user.get("is_admin") and current != wanted:
        return _json_error("Unauthorized", 403)
    a = _cfg_auth(cfg)
    if wanted == ADMIN_USER_ID:
        if not user.get("is_admin"):
            return _json_error("Unauthorized", 403)
        return _avatar_response(a)
    raw = _cfg_users(a).get(wanted)
    if not isinstance(raw, dict):
        return _json_error("User not found", 404)
    return _avatar_response(raw)


@router.post("/avatar")
def api_profile_avatar_save(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    preflight = _profile_context(request)
    if isinstance(preflight, JSONResponse):
        return preflight
    if not _origin_allowed(request):
        return _origin_blocked_response()
    decoded = _decode_avatar(payload or {})
    if isinstance(decoded, JSONResponse):
        return decoded
    data, content_type = decoded
    ext = AVATAR_TYPES[content_type][0]
    filename = f"{secrets.token_hex(16)}{ext}"
    path = _avatar_dir() / filename
    path.write_bytes(data)

    def _mutate(cfg: dict[str, Any], a: dict[str, Any], uid: str, raw: dict[str, Any], user: dict[str, Any], token: str) -> tuple[dict[str, Any], str, dict[str, Any], dict[str, Any], str, Any]:
        old = raw.get("avatar")
        raw["avatar"] = {"file": filename, "content_type": content_type, "updated_at": int(time.time() * 1000)}
        return a, uid, raw, user, token, old

    updated = _profile_write(request, _mutate)
    if isinstance(updated, JSONResponse):
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
        return updated
    cfg, (a, uid, raw, user, token, old) = updated
    if isinstance(old, dict):
        old_path = _avatar_path(old.get("file"))
        if old_path and old_path != path:
            try:
                old_path.unlink(missing_ok=True)
            except Exception:
                pass
    _audit(request, "profile_avatar_updated", actor=user, target_type=_target_type(uid), target_id=uid, message=f"{user.get('username') or 'User'} updated their profile picture")
    return JSONResponse({"ok": True, "user": _public_profile(cfg, a, uid, raw, token)}, headers={"Cache-Control": "no-store"})


@router.delete("/avatar")
def api_profile_avatar_delete(request: Request) -> JSONResponse:
    def _mutate(cfg: dict[str, Any], a: dict[str, Any], uid: str, raw: dict[str, Any], user: dict[str, Any], token: str) -> tuple[dict[str, Any], str, dict[str, Any], dict[str, Any], str, Any]:
        avatar = raw.get("avatar")
        raw.pop("avatar", None)
        return a, uid, raw, user, token, avatar

    updated = _profile_write(request, _mutate)
    if isinstance(updated, JSONResponse):
        return updated
    cfg, (a, uid, raw, user, token, avatar) = updated
    if isinstance(avatar, dict):
        path = _avatar_path(avatar.get("file"))
        if path:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass
    _audit(request, "profile_avatar_removed", actor=user, target_type=_target_type(uid), target_id=uid, message=f"{user.get('username') or 'User'} removed their profile picture")
    return JSONResponse({"ok": True, "user": _public_profile(cfg, a, uid, raw, token)}, headers={"Cache-Control": "no-store"})


@router.post("/password")
def api_profile_password(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    def _mutate(cfg: dict[str, Any], a: dict[str, Any], uid: str, raw: dict[str, Any], user: dict[str, Any], token: str) -> tuple[dict[str, Any], str, dict[str, Any], dict[str, Any], str] | JSONResponse:
        current_password = str((payload or {}).get("current_password") or "")
        new_password = str((payload or {}).get("new_password") or "")
        pwd = _account_password(a, uid, raw)
        if not isinstance(pwd, dict) or not _password_matches(pwd, current_password):
            return _json_error("Current password is incorrect", 400)
        if len(new_password) < MIN_PASSWORD_LENGTH:
            return _json_error(f"Password must be at least {MIN_PASSWORD_LENGTH} characters", 400)
        if uid == ADMIN_USER_ID:
            a["password"] = _password_hash(new_password)
        else:
            raw["password"] = _password_hash(new_password)
        _clear_user_sessions_except(cfg, uid, token)
        return a, uid, raw, user, token

    updated = _profile_write(request, _mutate)
    if isinstance(updated, JSONResponse):
        return updated
    cfg, (a, uid, raw, user, token) = updated
    _audit(request, "profile_password_changed", actor=user, target_type=_target_type(uid), target_id=uid, message=f"{user.get('username') or 'User'} changed their password")
    return JSONResponse({"ok": True, "user": _public_profile(cfg, a, uid, raw, token)}, headers={"Cache-Control": "no-store"})


@router.post("/totp/setup")
def api_profile_totp_setup(request: Request) -> JSONResponse:
    secret = _totp_new_secret()

    def _mutate(cfg: dict[str, Any], a: dict[str, Any], uid: str, raw: dict[str, Any], user: dict[str, Any], token: str) -> tuple[str, dict[str, Any], str, dict[str, Any], dict[str, Any]]:
        t = _totp_record_for_user(a, user)
        t["pending_secret"] = secret
        t["pending_created_at"] = int(time.time())
        return uid, user, str(user.get("username") or ""), t, raw

    updated = _profile_write(request, _mutate)
    if isinstance(updated, JSONResponse):
        return updated
    _cfg, (uid, user, username, t, _raw) = updated
    uri = _totp_uri(username, secret)
    _audit(request, "profile_totp_setup", actor=user, target_type=_target_type(uid), target_id=uid, message=f"{user.get('username') or 'User'} started two-factor setup")
    return JSONResponse({"ok": True, "secret": secret, "otpauth_url": uri, "qr_svg": _qr_svg(uri), "totp": _totp_public(t)}, headers={"Cache-Control": "no-store"})


@router.post("/totp/verify")
def api_profile_totp_verify(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    def _mutate(cfg: dict[str, Any], a: dict[str, Any], uid: str, raw: dict[str, Any], user: dict[str, Any], token: str) -> tuple[dict[str, Any], str, dict[str, Any], dict[str, Any], str, list[str]] | JSONResponse:
        t = _totp_record_for_user(a, user)
        pending = str(t.get("pending_secret") or "").strip()
        if not pending:
            return _json_error("No setup pending", 400)
        if not _totp_verify(pending, (payload or {}).get("code") or (payload or {}).get("totp_code")):
            return _json_error("Invalid verification code", 400)
        t["secret"] = pending
        t["enabled"] = True
        t.pop("pending_secret", None)
        t.pop("pending_created_at", None)
        codes = _generate_recovery_codes(raw)
        _clear_user_sessions_except(cfg, uid, token)
        return a, uid, raw, user, token, codes

    updated = _profile_write(request, _mutate)
    if isinstance(updated, JSONResponse):
        return updated
    cfg, (a, uid, raw, user, token, codes) = updated
    _audit(request, "profile_totp_enabled", actor=user, target_type=_target_type(uid), target_id=uid, message=f"{user.get('username') or 'User'} enabled two-factor authentication")
    return JSONResponse({"ok": True, "user": _public_profile(cfg, a, uid, raw, token), "recovery_codes": codes}, headers={"Cache-Control": "no-store"})


@router.post("/totp/disable")
def api_profile_totp_disable(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    def _mutate(cfg: dict[str, Any], a: dict[str, Any], uid: str, raw: dict[str, Any], user: dict[str, Any], token: str) -> tuple[dict[str, Any], str, dict[str, Any], dict[str, Any], str] | JSONResponse:
        current_password = str((payload or {}).get("current_password") or "")
        pwd = _account_password(a, uid, raw)
        if not isinstance(pwd, dict) or not _password_matches(pwd, current_password):
            return _json_error("Current password is incorrect", 400)
        t = _totp_record_for_user(a, user)
        t["enabled"] = False
        t["secret"] = ""
        t.pop("pending_secret", None)
        t.pop("pending_created_at", None)
        raw.pop("recovery_codes", None)
        _clear_user_sessions_except(cfg, uid, token)
        return a, uid, raw, user, token

    updated = _profile_write(request, _mutate)
    if isinstance(updated, JSONResponse):
        return updated
    cfg, (a, uid, raw, user, token) = updated
    _audit(request, "profile_totp_disabled", actor=user, target_type=_target_type(uid), target_id=uid, message=f"{user.get('username') or 'User'} disabled two-factor authentication")
    return JSONResponse({"ok": True, "user": _public_profile(cfg, a, uid, raw, token)}, headers={"Cache-Control": "no-store"})


@router.post("/recovery-codes")
def api_profile_recovery_codes(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    def _mutate(cfg: dict[str, Any], a: dict[str, Any], uid: str, raw: dict[str, Any], user: dict[str, Any], token: str) -> tuple[dict[str, Any], str, dict[str, Any], dict[str, Any], str, list[str]] | JSONResponse:
        current_password = str((payload or {}).get("current_password") or "")
        pwd = _account_password(a, uid, raw)
        if not isinstance(pwd, dict) or not _password_matches(pwd, current_password):
            return _json_error("Current password is incorrect", 400)
        if not _totp_enabled(raw.get("totp")):
            return _json_error("Two-factor authentication is not enabled", 400)
        codes = _generate_recovery_codes(raw)
        return a, uid, raw, user, token, codes

    updated = _profile_write(request, _mutate)
    if isinstance(updated, JSONResponse):
        return updated
    cfg, (a, uid, raw, user, token, codes) = updated
    _audit(request, "profile_recovery_codes_generated", actor=user, target_type=_target_type(uid), target_id=uid, message=f"{user.get('username') or 'User'} generated recovery codes")
    return JSONResponse({"ok": True, "user": _public_profile(cfg, a, uid, raw, token), "recovery_codes": codes}, headers={"Cache-Control": "no-store"})


@router.post("/sessions/revoke-others")
def api_profile_revoke_other_sessions(request: Request) -> JSONResponse:
    def _mutate(cfg: dict[str, Any], a: dict[str, Any], uid: str, raw: dict[str, Any], user: dict[str, Any], token: str) -> tuple[dict[str, Any], str, dict[str, Any], dict[str, Any], str]:
        _clear_user_sessions_except(cfg, uid, token)
        return a, uid, raw, user, token

    updated = _profile_write(request, _mutate)
    if isinstance(updated, JSONResponse):
        return updated
    cfg, (a, uid, raw, user, token) = updated
    _audit(request, "profile_sessions_revoked", actor=user, target_type=_target_type(uid), target_id=uid, message=f"{user.get('username') or 'User'} revoked other sessions")
    return JSONResponse({"ok": True, "user": _public_profile(cfg, a, uid, raw, token)}, headers={"Cache-Control": "no-store"})


@router.delete("/sessions/{session_id}")
def api_profile_revoke_session(request: Request, session_id: str) -> JSONResponse:
    def _mutate(cfg: dict[str, Any], a: dict[str, Any], uid: str, raw: dict[str, Any], user: dict[str, Any], token: str) -> tuple[dict[str, Any], str, dict[str, Any], dict[str, Any], str] | JSONResponse:
        wanted = str(session_id or "").strip()
        current, _others = _public_session_state(a, token)
        if current and str(current.get("id") or "") == wanted:
            return _json_error("Use logout to end the current session", 400)
        sessions = _prune_sessions(_iter_sessions(a))
        kept: list[dict[str, Any]] = []
        deleted = False
        for session in sessions:
            same_session = str(session.get("id") or "").strip() == wanted
            same_user = _normalize_app_user_id(session.get("user_id")) == uid
            if same_session and same_user:
                deleted = True
                continue
            kept.append(session)
        if not deleted:
            return _json_error("Session not found", 404)
        a["sessions"] = kept
        _sync_legacy_session(a, kept)
        return a, uid, raw, user, token

    updated = _profile_write(request, _mutate)
    if isinstance(updated, JSONResponse):
        return updated
    cfg, (a, uid, raw, user, token) = updated
    _audit(request, "profile_session_revoked", actor=user, target_type=_target_type(uid), target_id=uid, message=f"{user.get('username') or 'User'} revoked a session")
    return JSONResponse({"ok": True, "user": _public_profile(cfg, a, uid, raw, token)}, headers={"Cache-Control": "no-store"})


def _qr_svg(value: str) -> str:
    try:
        import qrcode
        from qrcode.constants import ERROR_CORRECT_M
        import qrcode.image.svg

        qr = qrcode.QRCode(error_correction=ERROR_CORRECT_M, border=4, box_size=10)
        qr.add_data(str(value or ""))
        qr.make(fit=True)
        img = qr.make_image(image_factory=qrcode.image.svg.SvgPathImage)
        return img.to_string(encoding="unicode")
    except Exception:
        return ""
