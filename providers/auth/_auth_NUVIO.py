# providers/auth/_auth_NUVIO.py
# CrossWatch - Nuvio Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import secrets
import threading
import time
from collections.abc import Mapping, MutableMapping
from datetime import datetime, timezone
from typing import Any

import requests

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, get_provider_block, normalize_instance_id

try:
    from _logging import log as _real_log
except ImportError:
    _real_log = None

__VERSION__ = "0.1"
API_BASE = "https://api.nuvio.tv"
TV_LOGIN_WEB_BASE_URL = "https://nuvio.tv/tv-login"
SHARED_PUBLIC_CLIENT_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJyb2xlIjoiYW5vbiIsImlzcyI6InN1cGFiYXNlIiwiaWF0IjoxNzgxNTIxMzQ2LCJleHAiOjE5MzkyMDEzNDZ9."
    "tmQaj682pwzehpqlgCDMnySOqiUvpgRbrE43T4VJpDI"
)
REFRESH_SKEW_SEC = 300
UA = "CrossWatch/NuvioAuth"
DEFAULT_PROFILE: dict[str, Any] = {
    "profile_id": 1,
    "profile_index": 1,
    "name": "Profile 1",
    "uses_primary_addons": False,
    "uses_primary_plugins": False,
}

_REFRESH_LOCKS: dict[str, threading.Lock] = {}
_REFRESH_LOCKS_LOCK = threading.Lock()


class NuvioError(RuntimeError):
    pass


class NuvioAuthError(NuvioError):
    pass


class NuvioTokenRefreshError(NuvioAuthError):
    pass


class NuvioInvalidResponse(NuvioError):
    pass


class NuvioServiceUnavailable(NuvioError):
    pass


class NuvioProfileUnavailable(NuvioError):
    pass


def _public_log_message(msg: Any) -> str:
    text = str(msg or "")
    allowed = {
        "NUVIO: anonymous session request",
        "NUVIO: anonymous session ok",
        "NUVIO: start TV login request",
        "NUVIO: start TV login ok",
        "NUVIO: poll TV login request",
        "NUVIO: poll TV login approved",
        "NUVIO: exchange TV login request",
        "NUVIO: exchange TV login ok",
        "NUVIO: refresh token request",
        "NUVIO: refresh token ok",
        "NUVIO: profile selected",
        "NUVIO: disconnected",
    }
    if text in allowed:
        return text
    if text.startswith("NUVIO: HTTP error"):
        return text
    if text.startswith("NUVIO: invalid response"):
        return text
    if text.startswith("NUVIO: auth error"):
        return text
    return "NUVIO: auth event"


def log(msg: str, level: str = "INFO", module: str = "AUTH", **fields: Any) -> None:
    public_msg = _public_log_message(msg)
    try:
        if _real_log is not None:
            _real_log(public_msg, level=level, module=module, extra=fields or None)
        else:
            print(f"[{module}] {level}: {public_msg}")
    except Exception:
        pass


def _safe_keys(value: Any) -> str:
    if isinstance(value, Mapping):
        keys = sorted(str(k) for k in value.keys())
        return ",".join(keys[:16]) + ("..." if len(keys) > 16 else "")
    if isinstance(value, list):
        first = value[0] if value else None
        return f"list[{len(value)}] first={_safe_keys(first)}"
    return type(value).__name__


def _log_invalid_response(reason: str, value: Any = None) -> None:
    detail = f" reason={reason}"
    if value is not None:
        detail += f" shape={_safe_keys(value)}"
    log(f"NUVIO: invalid response{detail}", level="WARN", module="AUTH")


def now() -> int:
    return int(time.time())


def normalize_base_url(value: Any) -> str:
    raw = str(value or API_BASE).strip() or API_BASE
    return raw.rstrip("/")


def app_public_client_key(block: Mapping[str, Any] | None = None) -> str:
    return SHARED_PUBLIC_CLIENT_KEY.strip()


def provider_block(cfg: Mapping[str, Any] | None, instance_id: Any = None) -> dict[str, Any]:
    b = get_provider_block(cfg or {}, "nuvio", instance_id)
    if b:
        return b
    base = (cfg or {}).get("nuvio") if isinstance(cfg, Mapping) else None
    return dict(base or {}) if isinstance(base, Mapping) else {}


def writable_block(cfg: dict[str, Any], instance_id: Any = None) -> dict[str, Any]:
    probe = cfg.get("_cw_probe") if isinstance(cfg.get("_cw_probe"), Mapping) else {}
    if str((probe or {}).get("provider") or "").upper() == "NUVIO" and normalize_instance_id((probe or {}).get("instance")) == normalize_instance_id(instance_id):
        view = cfg.get("nuvio")
        if isinstance(view, dict):
            view["base_url"] = normalize_base_url(view.get("base_url"))
            return view
    b = ensure_instance_block(cfg, "nuvio", instance_id)
    b["base_url"] = normalize_base_url(b.get("base_url"))
    return b


def profile_id_value(block: Mapping[str, Any] | None) -> int | None:
    raw = (block or {}).get("profile_id")
    if raw is None or isinstance(raw, bool):
        return None
    try:
        n = int(str(raw).strip())
    except Exception:
        return None
    return n if n > 0 else None


def has_auth(block: Mapping[str, Any] | None) -> bool:
    b = block or {}
    return bool(str(b.get("access_token") or "").strip() or str(b.get("refresh_token") or "").strip())


def is_configured(block: Mapping[str, Any] | None) -> bool:
    b = block or {}
    return bool(normalize_base_url(b.get("base_url")) and has_auth(b) and profile_id_value(b) is not None)


def status_for_block(block: Mapping[str, Any] | None) -> dict[str, Any]:
    b = block or {}
    pid = profile_id_value(b)
    return {
        "connected": is_configured(b),
        "authenticated": has_auth(b),
        "base_url_configured": bool(normalize_base_url(b.get("base_url"))),
        "client_key_configured": bool(app_public_client_key(b)),
        "profile_id": pid,
        "profile_name": str(b.get("profile_name") or ""),
        "expires_at": int(b.get("expires_at") or 0) if str(b.get("expires_at") or "").strip() else 0,
    }


def normalize_auth_method(value: Any, block: Mapping[str, Any] | None = None) -> str:
    return "tv_login"


def active_method(block: Mapping[str, Any] | None) -> str:
    return "tv_login"


def set_active_method(block: MutableMapping[str, Any], method: str) -> str:
    block["auth_method"] = "tv_login"
    return "tv_login"


def clear_oauth(block: MutableMapping[str, Any]) -> None:
    for key in ("access_token", "refresh_token", "expires_at", "profile_id", "profile_name", "_pending_tv_login", "_pending_tv_caller"):
        if key == "expires_at":
            block[key] = 0
        elif key in ("_pending_tv_login", "_pending_tv_caller"):
            block.pop(key, None)
        else:
            block[key] = ""


def about_to_expire(block: Mapping[str, Any], skew_sec: int = REFRESH_SKEW_SEC) -> bool:
    try:
        exp = int(block.get("expires_at") or 0)
    except Exception:
        exp = 0
    return bool(exp and exp - now() <= max(0, int(skew_sec)))


def normalize_profile(row: Mapping[str, Any]) -> dict[str, Any]:
    raw_id = row.get("profile_index")
    if raw_id is None:
        raw_id = row.get("id")
    if raw_id is None:
        raise NuvioInvalidResponse("invalid_profile_id")
    try:
        idx = int(raw_id)
    except Exception as exc:
        raise NuvioInvalidResponse("invalid_profile_id") from exc
    if idx <= 0:
        raise NuvioInvalidResponse("invalid_profile_id")
    return {
        "profile_id": idx,
        "profile_index": idx,
        "name": str(row.get("name") or f"Profile {idx}").strip() or f"Profile {idx}",
        "uses_primary_addons": bool(row.get("uses_primary_addons") if "uses_primary_addons" in row else row.get("usesPrimaryAddons")),
        "uses_primary_plugins": bool(row.get("uses_primary_plugins") if "uses_primary_plugins" in row else row.get("usesPrimaryPlugins")),
    }


def normalize_profiles(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        raise NuvioInvalidResponse("profiles_not_list")
    if not payload:
        return [dict(DEFAULT_PROFILE)]
    out: list[dict[str, Any]] = []
    for row in payload:
        if not isinstance(row, Mapping):
            raise NuvioInvalidResponse("profile_not_object")
        out.append(normalize_profile(row))
    return out


def _refresh_lock_key(instance_id: Any) -> str:
    return normalize_instance_id(instance_id)


def _refresh_lock(instance_id: Any) -> threading.Lock:
    key = _refresh_lock_key(instance_id)
    with _REFRESH_LOCKS_LOCK:
        lock = _REFRESH_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _REFRESH_LOCKS[key] = lock
        return lock


def _json_response(resp: requests.Response) -> Any:
    try:
        if not str(getattr(resp, "text", "") or "").strip():
            return {}
        return resp.json()
    except Exception as exc:
        _log_invalid_response("invalid_json")
        raise NuvioInvalidResponse("invalid_json") from exc


def _token_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    session = payload.get("session")
    nested = session if isinstance(session, Mapping) else {}
    access = str(payload.get("access_token") or payload.get("accessToken") or nested.get("access_token") or "").strip()
    refresh = str(payload.get("refresh_token") or payload.get("refreshToken") or nested.get("refresh_token") or "").strip()
    if not access:
        _log_invalid_response("missing_access_token", payload)
        raise NuvioInvalidResponse("missing_access_token")
    if not refresh:
        _log_invalid_response("missing_refresh_token", payload)
        raise NuvioInvalidResponse("missing_refresh_token")
    expires_in_raw = payload.get("expires_in") or 0
    try:
        expires_in = int(expires_in_raw or 0)
    except Exception:
        expires_in = 0
    return {
        "access_token": access,
        "refresh_token": refresh,
        "expires_at": now() + expires_in if expires_in > 0 else 0,
    }


def _tv_login_expiry_epoch(session: Mapping[str, Any]) -> int:
    raw_ms = session.get("expires_at_millis")
    if raw_ms is not None:
        try:
            value = int(raw_ms)
        except Exception as exc:
            _log_invalid_response("invalid_expiry", session)
            raise NuvioInvalidResponse("invalid_expiry") from exc
        if value <= 100_000_000_000:
            _log_invalid_response("invalid_expiry", session)
            raise NuvioInvalidResponse("invalid_expiry")
        return int(value / 1000)

    raw_at = str(session.get("expires_at") or "").strip()
    if raw_at:
        try:
            parsed = datetime.fromisoformat(raw_at.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            value = int(parsed.timestamp())
        except Exception as exc:
            _log_invalid_response("invalid_expiry", session)
            raise NuvioInvalidResponse("invalid_expiry") from exc
        if value <= now():
            _log_invalid_response("invalid_expiry", session)
            raise NuvioInvalidResponse("invalid_expiry")
        return value

    _log_invalid_response("invalid_expiry", session)
    raise NuvioInvalidResponse("invalid_expiry")


def _pending_tv_login_expiry_epoch(pending: Mapping[str, Any]) -> int:
    try:
        return int(pending.get("expires_at") or 0)
    except Exception:
        return 0


class NuvioClient:
    def __init__(self, cfg: Mapping[str, Any] | None, *, instance_id: Any = None, session: requests.Session | None = None):
        self.cfg = cfg or {}
        self.instance_id = normalize_instance_id(instance_id)
        self.block = provider_block(self.cfg, self.instance_id)
        self.base_url = normalize_base_url(self.block.get("base_url"))
        self.public_client_key = app_public_client_key(self.block)
        self.session = session or requests.Session()

    def url(self, path: str) -> str:
        return f"{self.base_url}/{str(path or '').lstrip('/')}"

    def public_headers(self, bearer: str | None = None) -> dict[str, str]:
        if not self.public_client_key:
            raise NuvioAuthError("missing_public_client_key")
        token = str(bearer or self.public_client_key).strip()
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": UA,
            "apikey": self.public_client_key,
            "Authorization": f"Bearer {token}",
        }

    def _post_json(self, path: str, payload: Mapping[str, Any], *, bearer: str | None = None, timeout: float = 20.0) -> tuple[int, Any]:
        try:
            resp = self.session.post(self.url(path), json=dict(payload or {}), headers=self.public_headers(bearer), timeout=timeout)
        except requests.RequestException as exc:
            log(f"NUVIO: HTTP error path={path} reason=network", level="ERROR", module="AUTH")
            raise NuvioServiceUnavailable("service_unavailable") from exc
        if resp.status_code >= 500 or resp.status_code == 0:
            log(f"NUVIO: HTTP error path={path} status={resp.status_code}", level="ERROR", module="AUTH")
            raise NuvioServiceUnavailable("service_unavailable")
        if resp.status_code in (401, 403):
            log(f"NUVIO: auth error path={path} status={resp.status_code}", level="ERROR", module="AUTH")
            raise NuvioAuthError("authentication_failed")
        if resp.status_code >= 400:
            log(f"NUVIO: HTTP error path={path} status={resp.status_code}", level="WARN", module="AUTH")
            raise NuvioServiceUnavailable("service_unavailable")
        return int(resp.status_code), _json_response(resp)

    def anonymous_session(self, cfg: dict[str, Any], block: MutableMapping[str, Any]) -> dict[str, str]:
        log("NUVIO: anonymous session request", level="INFO", module="AUTH")
        headers = self.public_headers(self.public_client_key)
        payload: Mapping[str, Any] = {"data": {"tv_client": "crosswatch"}}
        try:
            resp = self.session.post(self.url("/auth/v1/signup"), json=payload, headers=headers, timeout=20)
        except requests.RequestException as exc:
            raise NuvioServiceUnavailable("service_unavailable") from exc
        if resp.status_code in (401, 403):
            raise NuvioAuthError("authentication_failed")
        if resp.status_code >= 400:
            raise NuvioServiceUnavailable("service_unavailable")
        data = _json_response(resp)
        if not isinstance(data, Mapping):
            _log_invalid_response("anonymous_session_not_object", data)
            raise NuvioInvalidResponse("anonymous_session_not_object")
        tok = _token_payload(data)
        block["_pending_tv_caller"] = {
            "access_token": tok["access_token"],
            "refresh_token": tok["refresh_token"],
            "expires_at": tok["expires_at"],
        }
        save_config(cfg)
        log("NUVIO: anonymous session ok", level="INFO", module="AUTH")
        return {"access_token": tok["access_token"], "refresh_token": tok["refresh_token"]}

    def start_tv_login_session(self, cfg: dict[str, Any], *, redirect_base_url: str = TV_LOGIN_WEB_BASE_URL, device_name: str = "CrossWatch") -> dict[str, Any]:
        log("NUVIO: start TV login request", level="INFO", module="AUTH")
        block = writable_block(cfg, self.instance_id)
        self.block = dict(block)
        self.base_url = normalize_base_url(block.get("base_url"))
        self.public_client_key = app_public_client_key(block)
        caller = self.anonymous_session(cfg, block)
        device_nonce = secrets.token_urlsafe(24)
        payload: dict[str, Any] = {
            "p_device_nonce": device_nonce,
            "p_redirect_base_url": str(redirect_base_url or TV_LOGIN_WEB_BASE_URL).strip() or TV_LOGIN_WEB_BASE_URL,
            "p_device_name": str(device_name or "CrossWatch").strip() or "CrossWatch",
        }

        _, data = self._post_json("/rest/v1/rpc/start_tv_login_session", payload, bearer=caller["access_token"])
        if not isinstance(data, list) or not data or not isinstance(data[0], Mapping):
            _log_invalid_response("start_session_not_list_object", data)
            raise NuvioInvalidResponse("start_session_not_list_object")
        session = dict(data[0])
        code = str(session.get("code") or "").strip()
        login_url = str(session.get("qr_content") or session.get("web_url") or "").strip()
        if not code or not login_url:
            _log_invalid_response("missing_code_or_qr_content", session)
            raise NuvioInvalidResponse("missing_code_or_qr_content")
        expires_at = _tv_login_expiry_epoch(session)
        try:
            interval = int(session.get("poll_interval_seconds") or 3)
        except Exception as exc:
            _log_invalid_response("invalid_poll_interval", session)
            raise NuvioInvalidResponse("invalid_poll_interval") from exc
        block["_pending_tv_login"] = {
            "code": code,
            "device_nonce": device_nonce,
            "expires_at": int(expires_at),
            "created_at": now(),
            "poll_interval_seconds": max(1, interval),
            "caller_access_token": caller["access_token"],
            "caller_refresh_token": caller["refresh_token"],
        }
        save_config(cfg)
        log("NUVIO: start TV login ok", level="INFO", module="AUTH")
        return {
            "ok": True,
            "instance": self.instance_id,
            "code": code,
            "login_url": login_url,
            "expires_at": int(expires_at),
            "interval": max(1, interval),
        }

    def _pending(self, cfg: dict[str, Any]) -> Mapping[str, Any]:
        b = writable_block(cfg, self.instance_id)
        p = b.get("_pending_tv_login")
        return p if isinstance(p, Mapping) else {}

    def poll_tv_login_session(self, cfg: dict[str, Any]) -> dict[str, Any]:
        pending = self._pending(cfg)
        code = str(pending.get("code") or "").strip()
        nonce = str(pending.get("device_nonce") or "").strip()
        caller = str(pending.get("caller_access_token") or "").strip()
        if not code or not nonce or not caller:
            return {"ok": False, "status": "no_pending_login", "instance": self.instance_id}
        expires_at = _pending_tv_login_expiry_epoch(pending)
        if expires_at and now() >= expires_at:
            return {"ok": False, "status": "expired", "instance": self.instance_id}
        log("NUVIO: poll TV login request", level="DEBUG", module="AUTH")
        _, data = self._post_json("/rest/v1/rpc/poll_tv_login_session", {"p_code": code, "p_device_nonce": nonce}, bearer=caller)
        if not isinstance(data, list) or not data or not isinstance(data[0], Mapping):
            _log_invalid_response("poll_session_not_list_object", data)
            raise NuvioInvalidResponse("poll_session_not_list_object")
        row = data[0]
        status = str(row.get("status") or "").strip().lower() or "pending"
        if status == "approved":
            log("NUVIO: poll TV login approved", level="INFO", module="AUTH")
        interval_raw = row.get("poll_interval_seconds") or pending.get("poll_interval_seconds") or 3
        return {
            "ok": status == "approved",
            "status": status,
            "instance": self.instance_id,
            "expires_at": int(expires_at),
            "interval": int(interval_raw),
        }

    def exchange_tv_login_session(self, cfg: dict[str, Any]) -> dict[str, Any]:
        log("NUVIO: exchange TV login request", level="INFO", module="AUTH")
        block = writable_block(cfg, self.instance_id)
        pending = self._pending(cfg)
        code = str(pending.get("code") or "").strip()
        nonce = str(pending.get("device_nonce") or "").strip()
        caller = str(pending.get("caller_access_token") or "").strip()
        if not code or not nonce or not caller:
            return {"ok": False, "status": "no_pending_login", "instance": self.instance_id}
        _, data = self._post_json("/functions/v1/tv-logins-exchange", {"code": code, "device_nonce": nonce}, bearer=caller)
        if not isinstance(data, Mapping):
            _log_invalid_response("exchange_not_object", data)
            raise NuvioInvalidResponse("exchange_not_object")
        tokens = _token_payload(data)
        block["access_token"] = tokens["access_token"]
        block["refresh_token"] = tokens["refresh_token"]
        block["expires_at"] = tokens["expires_at"]
        block.pop("_pending_tv_login", None)
        block.pop("_pending_tv_caller", None)
        save_config(cfg)
        profiles = self.pull_profiles(cfg, refresh=True)
        if len(profiles) == 1:
            selected = profiles[0]
            block["profile_id"] = int(selected["profile_id"])
            block["profile_name"] = str(selected["name"])
            save_config(cfg)
        log("NUVIO: exchange TV login ok", level="INFO", module="AUTH")
        return {"ok": True, "status": "ok", "instance": self.instance_id, "expires_at": int(block.get("expires_at") or 0), "profiles": profiles}

    def refresh_token(self, cfg: dict[str, Any] | None = None) -> dict[str, Any]:
        log("NUVIO: refresh token request", level="INFO", module="AUTH")
        full = load_config()
        inst = self.instance_id
        with _refresh_lock(inst):
            block = writable_block(full, inst)
            src = provider_block(cfg, inst) if isinstance(cfg, Mapping) else {}
            if src:
                if not str(block.get("base_url") or "").strip():
                    block["base_url"] = src.get("base_url") or API_BASE
            rt = str(block.get("refresh_token") or "").strip()
            if not rt and isinstance(cfg, Mapping):
                rt = str(src.get("refresh_token") or "").strip()
                if rt:
                    block["refresh_token"] = rt
            if not rt:
                return {"ok": False, "status": "missing_refresh", "instance": inst}
            key = app_public_client_key(block)
            if not key:
                return {"ok": False, "status": "missing_public_client_key", "instance": inst}
            self.block = dict(block)
            self.base_url = normalize_base_url(block.get("base_url"))
            self.public_client_key = key
            try:
                resp = self.session.post(
                    self.url("/auth/v1/token?grant_type=refresh_token"),
                    json={"refresh_token": rt},
                    headers=self.public_headers(key),
                    timeout=20,
                )
            except requests.RequestException:
                return {"ok": False, "status": "network_error", "instance": inst}
            if resp.status_code in (401, 403):
                return {"ok": False, "status": "invalid_refresh", "instance": inst}
            if resp.status_code >= 400:
                return {"ok": False, "status": "refresh_failed", "instance": inst}
            data = _json_response(resp)
            if not isinstance(data, Mapping):
                return {"ok": False, "status": "invalid_response", "instance": inst}
            try:
                tokens = _token_payload(data)
            except NuvioInvalidResponse:
                return {"ok": False, "status": "invalid_response", "instance": inst}
            block["access_token"] = tokens["access_token"]
            block["refresh_token"] = tokens["refresh_token"]
            block["expires_at"] = tokens["expires_at"]
            save_config(full)
            if isinstance(cfg, dict):
                try:
                    dst = writable_block(cfg, inst)
                    dst["access_token"] = tokens["access_token"]
                    dst["refresh_token"] = tokens["refresh_token"]
                    dst["expires_at"] = tokens["expires_at"]
                except Exception:
                    pass
            log("NUVIO: refresh token ok", level="INFO", module="AUTH")
            return {"ok": True, "status": "ok", "instance": inst, "expires_at": int(block.get("expires_at") or 0)}

    def access_token(self, cfg: Mapping[str, Any] | None = None, *, refresh: bool = True) -> str:
        block = provider_block(cfg or self.cfg, self.instance_id)
        if refresh and (not str(block.get("access_token") or "").strip() or about_to_expire(block)):
            res = self.refresh_token(dict(cfg or self.cfg or {}))
            if not res.get("ok"):
                raise NuvioTokenRefreshError(str(res.get("status") or "refresh_failed"))
            block = provider_block(load_config(), self.instance_id)
        token = str(block.get("access_token") or "").strip()
        if not token:
            raise NuvioAuthError("missing_authentication")
        return token

    def request_json(self, method: str, path: str, *, payload: Mapping[str, Any] | None = None, refresh: bool = True, retry: bool = True, timeout: float = 20.0) -> Any:
        token = self.access_token(self.cfg, refresh=refresh)
        try:
            resp = self.session.request(method.upper(), self.url(path), json=dict(payload or {}) if method.upper() != "GET" else None, headers=self.public_headers(token), timeout=timeout)
        except requests.RequestException as exc:
            raise NuvioServiceUnavailable("service_unavailable") from exc
        if resp.status_code in (401, 403) and retry:
            res = self.refresh_token(dict(self.cfg or {}))
            if not res.get("ok"):
                raise NuvioTokenRefreshError(str(res.get("status") or "refresh_failed"))
            self.cfg = load_config()
            return self.request_json(method, path, payload=payload, refresh=False, retry=False, timeout=timeout)
        if resp.status_code in (401, 403):
            raise NuvioAuthError("authentication_failed")
        if resp.status_code >= 500 or resp.status_code == 0:
            raise NuvioServiceUnavailable("service_unavailable")
        if resp.status_code >= 400:
            raise NuvioServiceUnavailable("service_unavailable")
        return _json_response(resp)

    def pull_profiles(self, cfg: Mapping[str, Any] | None = None, *, refresh: bool = True) -> list[dict[str, Any]]:
        if cfg is not None:
            self.cfg = cfg
        data = self.request_json("POST", "/rest/v1/rpc/sync_pull_profiles", payload={}, refresh=refresh, retry=True)
        return normalize_profiles(data)

    def select_profile(self, cfg: dict[str, Any], profile_id: Any) -> dict[str, Any]:
        profiles = self.pull_profiles(cfg, refresh=True)
        try:
            pid = int(profile_id)
        except Exception:
            raise NuvioInvalidResponse("invalid_profile_id")
        selected = next((p for p in profiles if int(p["profile_id"]) == pid), None)
        if not selected:
            raise NuvioProfileUnavailable("profile_unavailable")
        block = writable_block(cfg, self.instance_id)
        block["profile_id"] = int(selected["profile_id"])
        block["profile_name"] = str(selected["name"])
        save_config(cfg)
        log("NUVIO: profile selected", level="INFO", module="AUTH")
        return selected

    def disconnect(self, cfg: dict[str, Any]) -> None:
        block = writable_block(cfg, self.instance_id)
        clear_oauth(block)
        save_config(cfg)
        log("NUVIO: disconnected", level="INFO", module="AUTH")


def start_device_code(
    cfg: dict[str, Any] | None,
    *,
    instance_id: Any = None,
    redirect_uri: str | None = None,
    redirect_base_url: str | None = None,
    device_name: str = "CrossWatch",
    **_: Any,
) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_config()
    return NuvioClient(cfgd, instance_id=instance_id).start_tv_login_session(
        cfgd,
        redirect_base_url=redirect_uri or redirect_base_url or TV_LOGIN_WEB_BASE_URL,
        device_name=device_name,
    )


def poll_device_code(
    cfg: dict[str, Any] | None,
    *,
    instance_id: Any = None,
    **_: Any,
) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_config()
    return NuvioClient(cfgd, instance_id=instance_id).poll_tv_login_session(cfgd)


def refresh_token(
    cfg: dict[str, Any] | None = None,
    *,
    instance_id: Any = None,
    **_: Any,
) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_config()
    return NuvioClient(cfgd, instance_id=instance_id).refresh_token(cfgd)


def _request_headers(
    client: NuvioClient,
    cfg: Mapping[str, Any] | None,
    headers: Mapping[str, Any] | None,
    *,
    refresh: bool,
) -> dict[str, str]:
    token = client.access_token(cfg, refresh=refresh)
    auth_headers = client.public_headers(token)
    out = {str(k): str(v) for k, v in dict(headers or {}).items()}
    for key, value in auth_headers.items():
        out.setdefault(key, value)
    out["apikey"] = auth_headers["apikey"]
    out["Authorization"] = auth_headers["Authorization"]
    return out


def request_with_auth(
    session: requests.Session,
    method: str,
    url: str,
    *,
    cfg: Mapping[str, Any] | None,
    instance_id: Any = None,
    timeout: float = 10.0,
    max_retries: int = 3,
    request_func: Any = None,
    **kwargs: Any,
) -> requests.Response:
    client = NuvioClient(cfg, instance_id=instance_id, session=session)
    req_kwargs = dict(kwargs)
    req_kwargs["headers"] = _request_headers(client, cfg, req_kwargs.get("headers"), refresh=True)

    def call() -> requests.Response:
        if request_func is not None:
            return request_func(session, method, url, timeout=timeout, max_retries=max_retries, **req_kwargs)
        return session.request(method, url, timeout=timeout, **req_kwargs)

    resp = call()
    if getattr(resp, "status_code", None) not in (401, 403):
        return resp

    res = refresh_token(dict(cfg or {}), instance_id=instance_id)
    if not res.get("ok"):
        return resp
    fresh_cfg = _load_config()
    fresh_client = NuvioClient(fresh_cfg, instance_id=instance_id, session=session)
    req_kwargs["headers"] = _request_headers(fresh_client, fresh_cfg, kwargs.get("headers"), refresh=False)
    return call()


def _load_config() -> dict[str, Any]:
    try:
        return dict(load_config() or {})
    except Exception:
        return {}


class NuvioAuth(AuthProvider):
    name = "NUVIO"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="NUVIO",
            label="Nuvio",
            flow="tv_login",
            fields=[],
            actions={"start": True, "finish": True, "refresh": True, "disconnect": True},
            verify_url="https://nuvio.tv",
            notes="Experimental TV login flow. Nuvio API contracts may change.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"watchlist": False, "ratings": False, "history": False, "progress": False, "playlists": False}

    def get_status(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        block = provider_block(cfg, inst)
        raw = status_for_block(block)
        connected = bool(raw.get("connected"))
        label = "Nuvio" if inst == "default" else f"Nuvio ({inst})"
        extra = {
            "authenticated": bool(raw.get("authenticated")),
            "profile_id": raw.get("profile_id"),
            "profile_name": raw.get("profile_name") or "",
            "client_key_configured": bool(app_public_client_key(block)),
        }
        return AuthStatus(
            connected=connected,
            label=label,
            user=str(raw.get("profile_name") or "") or None,
            expires_at=int(raw.get("expires_at") or 0) or None,
            extra=extra,
        )

    def start(self, cfg: MutableMapping[str, Any] | None = None, *, redirect_uri: str | None = None, instance_id: Any = None) -> dict[str, Any]:
        cfgd = cfg if isinstance(cfg, dict) else _load_config()
        return NuvioClient(cfgd, instance_id=instance_id).start_tv_login_session(cfgd, redirect_base_url=redirect_uri or "https://nuvio.tv/tv-login")

    def finish(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None, **payload: Any) -> AuthStatus | Mapping[str, Any]:
        cfgd = cfg if isinstance(cfg, dict) else _load_config()
        client = NuvioClient(cfgd, instance_id=instance_id)
        action = str(payload.get("action") or "").strip().lower()
        if action in {"select_profile", "profile"} or payload.get("profile_id") is not None:
            try:
                selected = client.select_profile(cfgd, payload.get("profile_id"))
            except NuvioProfileUnavailable:
                return {"ok": False, "status": "profile_unavailable", "instance": normalize_instance_id(instance_id)}
            except NuvioInvalidResponse:
                return {"ok": False, "status": "invalid_profile", "instance": normalize_instance_id(instance_id)}
            return {"ok": True, "status": "ok", "instance": normalize_instance_id(instance_id), "profile": selected}
        client.exchange_tv_login_session(cfgd)
        return self.get_status(cfgd, instance_id=instance_id)

    def refresh(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        client = NuvioClient(cfg if isinstance(cfg, Mapping) else _load_config(), instance_id=instance_id)
        client.refresh_token(dict(cfg or {}))
        return self.get_status(_load_config(), instance_id=instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfgd = cfg if isinstance(cfg, dict) else _load_config()
        NuvioClient(cfgd, instance_id=instance_id).disconnect(cfgd)
        return self.get_status(cfgd, instance_id=instance_id)


def html() -> str:
    return r"""<div class="section" id="sec-nuvio">
  <style>
    #sec-nuvio .hidden{display:none!important}
    #sec-nuvio .muted{opacity:.7;font-size:.92em}
    #sec-nuvio .nuvio-actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:12px}
    #sec-nuvio .nuvio-qc{margin-top:12px;padding:14px;border-radius:12px;border:1px solid rgba(0,224,132,.35);background:rgba(0,224,132,.06)}
    #sec-nuvio .nuvio-qc-codewrap{display:flex;align-items:center;justify-content:center;gap:12px}
    #sec-nuvio .nuvio-qc-code{
      font-size:2em;font-weight:700;letter-spacing:.18em;padding:6px 0 6px .18em;color:#8ff0c2;
      text-align:center;text-transform:uppercase;font-variant-numeric:tabular-nums;word-break:break-all;
    }
    #sec-nuvio .nuvio-qc-copy{
      appearance:none;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;
      width:34px;height:34px;border-radius:9px;flex:0 0 auto;
      border:1px solid rgba(0,224,132,.35);background:rgba(0,224,132,.08);color:#8ff0c2;
      transition:background .15s ease,border-color .15s ease,color .15s ease,transform .12s ease;
    }
    #sec-nuvio .nuvio-qc-copy:hover{background:rgba(0,224,132,.16);border-color:rgba(0,224,132,.6)}
    #sec-nuvio .nuvio-qc-copy:active{transform:scale(.94)}
    #sec-nuvio .nuvio-qc-copy.copied{background:rgba(0,224,132,.24);border-color:rgba(0,224,132,.75)}
    #sec-nuvio .nuvio-qc-copy svg{width:16px;height:16px;display:block}
    #sec-nuvio .nuvio-qc-line{display:flex;align-items:center;gap:8px 12px;flex-wrap:wrap;margin-top:6px}
    #sec-nuvio .nuvio-qc-meta{display:flex;justify-content:space-between;gap:12px;margin-top:6px}
    #sec-nuvio .nuvio-profile-row{display:flex;gap:10px;align-items:end;flex-wrap:wrap;margin-top:12px}
    #sec-nuvio .nuvio-profile-row select{min-width:240px}
    #sec-nuvio .msg{padding:8px 12px;border-radius:8px;border:1px solid rgba(0,255,170,.18);background:rgba(0,255,170,.08);color:#b9ffd7;font-weight:600}
    #sec-nuvio .msg.warn{border-color:rgba(255,210,0,.18);background:rgba(255,210,0,.08);color:#ffe9a6}
    #sec-nuvio #nuvio_connect{background:linear-gradient(135deg,#00e084,#2ea859);border-color:rgba(0,224,132,.45);box-shadow:0 0 14px rgba(0,224,132,.35);color:#fff}
  </style>
  <div class="head" data-toggle-section="sec-nuvio">
    <span class="chev"></span><strong>Nuvio</strong>
  </div>
  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="nuvio">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">Nuvio <span class="badge feature-disabled">Experimental</span></div>
            <div class="muted">Connect with Nuvio TV login and select one Nuvio profile for this CrossWatch instance.</div>
          </div>
        </div>
        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
        </div>
        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="cw-auth-journey" style="--cw-auth-c1:255,255,255;--cw-auth-c2:255,255,255;--cw-auth-logo:url('/assets/img/NUVIO.png')">
              <div class="cw-auth-journey-text">
                <div class="cw-auth-journey-title">Connect to Nuvio</div>
                <div class="cw-auth-journey-copy">Use Nuvio TV login, approve the temporary code, then choose a profile. Nuvio API contracts may change.</div>
              </div>
            </div>
            <div class="nuvio-actions">
              <button id="nuvio_connect" class="btn" type="button">Connect Nuvio</button>
              <button id="nuvio_disconnect" class="hidden" type="button">Disconnect Nuvio</button>
              <div id="nuvio_msg" class="msg warn hidden" aria-live="polite"></div>
            </div>
            <div id="nuvio_profile_state" class="nuvio-profile-row hidden">
              <div>
                <label for="nuvio_profile_select">Nuvio profile</label>
                <select id="nuvio_profile_select"></select>
              </div>
            </div>
            <div id="nuvio_login_state" class="nuvio-qc hidden">
              <input id="nuvio_code_input" type="hidden">
              <div class="nuvio-qc-codewrap">
                <div class="nuvio-qc-code" id="nuvio_code">------</div>
                <button type="button" id="nuvio_code_copy" class="nuvio-qc-copy" title="Copy code" aria-label="Copy code">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                </button>
              </div>
              <div class="muted nuvio-qc-line">
                <span>Opening the Nuvio approval page - approve CrossWatch in the browser.</span>
                <a id="nuvio_login_url" href="#" target="_blank" rel="noopener">Open Nuvio approval page</a>
                <span id="nuvio_polling">Waiting for approval...</span>
              </div>
              <div class="nuvio-qc-meta">
                <span></span>
                <span class="muted" id="nuvio_expiry"></span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
"""


PROVIDER = NuvioAuth()
__all__ = [
    "PROVIDER",
    "NuvioAuth",
    "html",
    "__VERSION__",
    "active_method",
    "clear_oauth",
    "is_configured",
    "normalize_auth_method",
    "poll_device_code",
    "refresh_token",
    "request_with_auth",
    "set_active_method",
    "start_device_code",
    "status_for_block",
]
