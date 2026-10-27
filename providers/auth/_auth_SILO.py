# providers/auth/_auth_SILO.py
# CrossWatch - Silo Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import secrets
import threading
import time
from collections.abc import Mapping, MutableMapping
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import requests

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.app_version import user_agent as http_user_agent
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import (
    ensure_instance_block,
    is_sync_user_instance,
    list_instance_ids,
    normalize_instance_id,
    resolve_provider_block,
    resolve_sync_user_block,
    split_sync_user_id,
)

try:
    from _logging import log as _real_log
except ImportError:
    _real_log = None

__VERSION__ = "0.1"
API_PREFIX = "/api/v2"
DEVICE_NAME = "CrossWatch"
DEVICE_PLATFORM = "CrossWatch"
POLL_INTERVAL_SEC = 3
REFRESH_SKEW_SEC = 300
KEEPALIVE_SEC = 3 * 86400
HTTP_TIMEOUT = 20
PROBLEM_PREFIX = "https://siloserver.org/docs/api/v2/problems/"
UA = http_user_agent("SiloAuth", override_env="CW_SILO_UA")

_SESSION_KEYS = (
    "access_token",
    "refresh_token",
    "expires_at",
    "user_id",
    "username",
    "role",
    "profile_id",
    "profile_name",
    "profile_has_pin",
    "profile_token",
    "profile_token_expires_at",
    "session_refreshed_at",
)

_REFRESH_LOCKS: dict[str, threading.Lock] = {}
_REFRESH_LOCKS_GUARD = threading.Lock()


class SiloError(RuntimeError):
    pass


class SiloAuthError(SiloError):
    pass


class SiloReconnectRequired(SiloAuthError):
    pass


class SiloProfileVerificationRequired(SiloAuthError):
    pass


class SiloServiceUnavailable(SiloError):
    pass


class SiloInvalidResponse(SiloError):
    pass


class SiloServerError(SiloError):
    pass


class SiloProfileError(SiloError):
    pass


class SiloRateLimited(SiloError):
    def __init__(self, code: str, retry_after: int = 0):
        super().__init__(code)
        self.retry_after = int(retry_after or 0)


def log(msg: str, level: str = "INFO", module: str = "AUTH", **fields: Any) -> None:
    try:
        if _real_log is not None:
            _real_log(msg, level=level, module=module, extra=fields or None)
        else:
            print(f"[{module}] {level}: {msg}")
    except Exception:
        pass


def now() -> int:
    return int(time.time())


def normalize_server_url(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = f"http://{raw}"
    parts = urlsplit(raw)
    if parts.scheme not in ("http", "https") or not parts.hostname or parts.username or parts.password:
        return ""
    path = parts.path.rstrip("/")
    if path.endswith(API_PREFIX):
        path = path[: -len(API_PREFIX)]
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def verify_ssl_of(block: Mapping[str, Any] | None) -> bool:
    raw = (block or {}).get("verify_ssl")
    return False if raw is None else bool(raw)


def provider_block(cfg: Mapping[str, Any] | None, instance_id: Any = None) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    if is_sync_user_instance(inst):
        base = (cfg or {}).get("silo") if isinstance(cfg, Mapping) else None
        return resolve_sync_user_block(base, inst) if isinstance(base, Mapping) else {}
    return resolve_provider_block(cfg or {}, "silo", inst)


def token_instance(instance_id: Any) -> str:
    ref = split_sync_user_id(normalize_instance_id(instance_id))
    return ref[0] if ref else normalize_instance_id(instance_id)


def sync_user_entry(cfg: dict[str, Any], instance_id: Any) -> dict[str, Any] | None:
    ref = split_sync_user_id(normalize_instance_id(instance_id))
    if ref is None:
        return None
    parent = ensure_instance_block(cfg, "silo", ref[0])
    users = parent.get("sync_users")
    entry = users.get(ref[1]) if isinstance(users, dict) else None
    return entry if isinstance(entry, dict) else None


def writable_block(cfg: dict[str, Any], instance_id: Any = None) -> dict[str, Any]:
    probe = cfg.get("_cw_probe") if isinstance(cfg.get("_cw_probe"), Mapping) else {}
    if str((probe or {}).get("provider") or "").upper() == "SILO" and normalize_instance_id((probe or {}).get("instance")) == normalize_instance_id(instance_id):
        view = cfg.get("silo")
        if isinstance(view, dict):
            return view
    return ensure_instance_block(cfg, "silo", instance_id)


def ensure_device_id(block: MutableMapping[str, Any]) -> str:
    did = str(block.get("device_id") or "").strip()
    if not did:
        did = f"crosswatch-{secrets.token_hex(8)}"
        block["device_id"] = did
    return did


def has_auth(block: Mapping[str, Any] | None) -> bool:
    b = block or {}
    return bool(str(b.get("access_token") or "").strip() or str(b.get("refresh_token") or "").strip())


def needs_reconnect(block: Mapping[str, Any] | None) -> bool:
    return str((block or {}).get("auth_error") or "") == "reconnect_required"


def needs_profile_pin(block: Mapping[str, Any] | None) -> bool:
    return str((block or {}).get("auth_error") or "") == "profile_pin_required"


def is_configured(block: Mapping[str, Any] | None) -> bool:
    b = block or {}
    return bool(
        normalize_server_url(b.get("server_url"))
        and has_auth(b)
        and str(b.get("profile_id") or "").strip()
        and not needs_reconnect(b)
    )


def normalize_auth_method(value: Any = None, block: Mapping[str, Any] | None = None) -> str:
    return "device_code"


def active_method(block: Mapping[str, Any] | None = None) -> str:
    return "device_code"


def set_active_method(block: MutableMapping[str, Any], method: str = "device_code") -> str:
    block["auth_method"] = "device_code"
    return "device_code"


def clear_oauth(block: MutableMapping[str, Any]) -> None:
    for key in _SESSION_KEYS:
        if key in block:
            block[key] = 0 if key in {"expires_at", "profile_token_expires_at", "session_refreshed_at"} else (False if key == "profile_has_pin" else "")
    block.pop("_pending_device", None)
    block.pop("auth_error", None)


def about_to_expire(block: Mapping[str, Any], skew_sec: int = REFRESH_SKEW_SEC) -> bool:
    try:
        exp = int(block.get("expires_at") or 0)
    except Exception:
        exp = 0
    return bool(exp and exp - now() <= max(0, int(skew_sec)))


def _pending_view(block: Mapping[str, Any]) -> dict[str, Any] | None:
    pend = block.get("_pending_device")
    if not isinstance(pend, Mapping) or not str(pend.get("user_code") or "").strip():
        return None
    return {
        "user_code": str(pend.get("user_code") or ""),
        "verification_uri": str(pend.get("verification_uri") or ""),
        "verification_uri_complete": str(pend.get("verification_uri_complete") or ""),
        "expires_at": int(pend.get("expires_at") or 0),
        "interval": int(pend.get("interval") or POLL_INTERVAL_SEC),
    }


def status_for_block(block: Mapping[str, Any] | None) -> dict[str, Any]:
    b = block or {}
    out: dict[str, Any] = {
        "auth_method": "device_code",
        "connected": is_configured(b),
        "authenticated": has_auth(b) and not needs_reconnect(b),
        "reconnect_required": needs_reconnect(b),
        "profile_pin_required": needs_profile_pin(b),
        "server_url": normalize_server_url(b.get("server_url")),
        "verify_ssl": verify_ssl_of(b),
        "username": str(b.get("username") or ""),
        "user_id": str(b.get("user_id") or ""),
        "role": str(b.get("role") or ""),
        "profile_id": str(b.get("profile_id") or ""),
        "profile_name": str(b.get("profile_name") or ""),
        "profile_has_pin": bool(b.get("profile_has_pin")),
        "expires_at": int(b.get("expires_at") or 0) if str(b.get("expires_at") or "").strip() else 0,
    }
    pending = _pending_view(b)
    if pending:
        out["pending"] = pending
    return out


def _parse_epoch(value: Any) -> int:
    raw = str(value or "").strip()
    if not raw:
        return 0
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return 0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def _retry_after(resp: requests.Response) -> int:
    try:
        return max(0, int(float(resp.headers.get("Retry-After") or 0)))
    except Exception:
        return 0


def problem_code(resp: requests.Response) -> str:
    try:
        body = resp.json()
    except Exception:
        return ""
    if not isinstance(body, Mapping):
        return ""
    raw = str(body.get("type") or "").strip()
    if raw.startswith(PROBLEM_PREFIX):
        return raw[len(PROBLEM_PREFIX):].strip("/")
    return str(body.get("error") or body.get("code") or "").strip()


def _json(resp: requests.Response) -> Any:
    if not str(getattr(resp, "text", "") or "").strip():
        return {}
    try:
        return resp.json()
    except Exception as exc:
        raise SiloInvalidResponse("invalid_json") from exc


def _apply_tokens(block: MutableMapping[str, Any], tok: Mapping[str, Any]) -> None:
    access = str(tok.get("access_token") or "").strip()
    if not access:
        raise SiloInvalidResponse("missing_access_token")
    refresh = str(tok.get("refresh_token") or block.get("refresh_token") or "").strip()
    try:
        expires_in = int(tok.get("expires_in") or 0)
    except Exception:
        expires_in = 0
    block["access_token"] = access
    block["refresh_token"] = refresh
    block["expires_at"] = now() + expires_in if expires_in > 0 else 0
    block["session_refreshed_at"] = now()
    block["auth_method"] = "device_code"
    if needs_reconnect(block):
        block.pop("auth_error", None)
    user = tok.get("user")
    if isinstance(user, Mapping):
        _apply_user(block, user)


def _apply_user(block: MutableMapping[str, Any], user: Mapping[str, Any]) -> None:
    if str(user.get("id") or "").strip():
        block["user_id"] = str(user.get("id")).strip()
    if str(user.get("username") or "").strip():
        block["username"] = str(user.get("username")).strip()
    if str(user.get("role") or "").strip():
        block["role"] = str(user.get("role")).strip()


def normalize_profile(row: Mapping[str, Any]) -> dict[str, Any]:
    pid = str(row.get("id") or "").strip()
    if not pid:
        raise SiloInvalidResponse("invalid_profile_id")
    return {
        "id": pid,
        "profile_id": pid,
        "name": str(row.get("name") or "").strip() or "Profile",
        "is_primary": bool(row.get("is_primary")),
        "has_pin": bool(row.get("has_pin")),
        "is_child": bool(row.get("is_child")),
    }


def _refresh_lock(instance_id: Any) -> threading.Lock:
    inst = normalize_instance_id(instance_id)
    with _REFRESH_LOCKS_GUARD:
        lock = _REFRESH_LOCKS.get(inst)
        if lock is None:
            lock = threading.Lock()
            _REFRESH_LOCKS[inst] = lock
        return lock


def _load_config() -> dict[str, Any]:
    try:
        cfg = load_config() or {}
        return cfg if isinstance(cfg, dict) else dict(cfg)
    except Exception:
        return {}


def base_headers(block: Mapping[str, Any] | None, *, token: str | None = None, profile: bool = False) -> dict[str, str]:
    b = block or {}
    h = {
        "Accept": "application/json",
        "User-Agent": UA,
        "X-Silo-Device-Name": DEVICE_NAME,
        "X-Silo-Device-Platform": DEVICE_PLATFORM,
    }
    did = str(b.get("device_id") or "").strip()
    if did:
        h["X-Silo-Device-Id"] = did
    if token:
        h["Authorization"] = f"Bearer {token}"
    if profile:
        pid = str(b.get("profile_id") or "").strip()
        if pid:
            h["X-Profile-Id"] = pid
            ptok = str(b.get("profile_token") or "").strip()
            if ptok:
                h["X-Profile-Token"] = ptok
    return h


def api_url(server_url: str, path: str) -> str:
    base = normalize_server_url(server_url)
    if not base:
        raise SiloServerError("invalid_url")
    p = "/" + str(path or "").lstrip("/")
    if not p.startswith(API_PREFIX + "/"):
        p = API_PREFIX + p
    return f"{base}{p}"


def check_server(server_url: Any, *, verify_ssl: bool = False, session: requests.Session | None = None, timeout: float = HTTP_TIMEOUT) -> dict[str, Any]:
    base = normalize_server_url(server_url)
    if not base:
        raise SiloServerError("invalid_url")
    sess = session or requests.Session()
    try:
        resp = sess.get(api_url(base, "/auth/device/capability"), headers=base_headers({}), timeout=timeout, verify=verify_ssl)
    except requests.exceptions.SSLError as exc:
        raise SiloServerError("ssl_error") from exc
    except requests.RequestException as exc:
        raise SiloServerError("connection_failed") from exc
    if resp.status_code == 404:
        raise SiloServerError("not_silo")
    if resp.status_code >= 400:
        raise SiloServerError("http_error")
    try:
        data = resp.json()
    except Exception as exc:
        raise SiloServerError("not_silo") from exc
    if not isinstance(data, Mapping) or "state" not in data or "protocol_versions" not in data:
        raise SiloServerError("not_silo")
    if str(data.get("state") or "") != "available":
        raise SiloServerError("device_login_unavailable")
    return {"server_url": base, "cancel": bool(data.get("cancel"))}


class SiloClient:
    def __init__(self, cfg: Mapping[str, Any] | None, *, instance_id: Any = None, session: requests.Session | None = None):
        self.cfg = cfg if cfg is not None else {}
        self.instance_id = normalize_instance_id(instance_id)
        self.session = session or requests.Session()

    @property
    def block(self) -> dict[str, Any]:
        return provider_block(self.cfg, self.instance_id)

    def _url(self, path: str, block: Mapping[str, Any] | None = None) -> str:
        return api_url(str((block or self.block).get("server_url") or ""), path)

    def _post_public(self, block: Mapping[str, Any], path: str, payload: Mapping[str, Any]) -> requests.Response:
        try:
            return self.session.post(
                self._url(path, block),
                json=dict(payload),
                headers={**base_headers(block), "Content-Type": "application/json"},
                timeout=HTTP_TIMEOUT,
                verify=verify_ssl_of(block),
            )
        except requests.RequestException as exc:
            raise SiloServiceUnavailable("service_unavailable") from exc

    def configure_server(self, cfg: dict[str, Any], server_url: Any, verify_ssl: Any = None) -> dict[str, Any]:
        block = writable_block(cfg, self.instance_id)
        verify = verify_ssl_of(block) if verify_ssl is None else bool(verify_ssl)
        found = check_server(server_url, verify_ssl=verify, session=self.session)
        new_url = found["server_url"]
        old_url = normalize_server_url(block.get("server_url"))
        changed = old_url != new_url
        if changed and has_auth(block):
            raise SiloServerError("disconnect_first")
        if changed:
            clear_oauth(block)
        block["server_url"] = new_url
        block["verify_ssl"] = verify
        ensure_device_id(block)
        save_config(cfg)
        if changed:
            log(f"SILO: server set (instance={self.instance_id})", level="INFO")
        self.cfg = cfg
        return {"ok": True, "instance": self.instance_id, "server_url": new_url, "verify_ssl": verify, "changed": changed}

    def start_device_login(self, cfg: dict[str, Any], *, server_url: Any = None, verify_ssl: Any = None) -> dict[str, Any]:
        if server_url is not None and str(server_url).strip():
            self.configure_server(cfg, server_url, verify_ssl)
        block = writable_block(cfg, self.instance_id)
        if not normalize_server_url(block.get("server_url")):
            raise SiloServerError("invalid_url")
        ensure_device_id(block)
        set_active_method(block)
        resp = self._post_public(block, "/auth/device/start", {"client_purpose": "device_login", "device_name": DEVICE_NAME, "device_platform": DEVICE_PLATFORM})
        if resp.status_code == 429:
            raise SiloRateLimited("rate_limited", _retry_after(resp))
        if resp.status_code >= 500:
            raise SiloServiceUnavailable("service_unavailable")
        if resp.status_code >= 400:
            raise SiloServerError(problem_code(resp) or "http_error")
        data = _json(resp)
        if not isinstance(data, Mapping):
            raise SiloInvalidResponse("invalid_response")
        device_code = str(data.get("device_code") or "").strip()
        user_code = str(data.get("user_code") or "").strip()
        if not device_code or not user_code:
            raise SiloInvalidResponse("missing_device_code")
        try:
            interval = max(1, int(data.get("interval") or POLL_INTERVAL_SEC))
        except Exception:
            interval = POLL_INTERVAL_SEC
        expires_at = _parse_epoch(data.get("expires_at"))
        if not expires_at:
            try:
                expires_at = now() + int(data.get("expires_in") or 900)
            except Exception:
                expires_at = now() + 900
        block["_pending_device"] = {
            "device_code": device_code,
            "user_code": user_code,
            "verification_uri": str(data.get("verification_uri") or ""),
            "verification_uri_complete": str(data.get("verification_uri_complete") or ""),
            "interval": interval,
            "expires_at": expires_at,
            "created_at": now(),
        }
        save_config(cfg)
        self.cfg = cfg
        log(f"SILO: device login started (instance={self.instance_id})", level="INFO")
        return {"ok": True, "instance": self.instance_id, **(_pending_view(block) or {})}

    def poll_device_login(self, cfg: dict[str, Any]) -> dict[str, Any]:
        block = writable_block(cfg, self.instance_id)
        pend = block.get("_pending_device") if isinstance(block.get("_pending_device"), Mapping) else {}
        device_code = str((pend or {}).get("device_code") or "").strip()
        if not device_code:
            return {"ok": False, "status": "no_pending_login", "instance": self.instance_id}
        expires_at = int((pend or {}).get("expires_at") or 0)
        if expires_at and now() >= expires_at:
            block.pop("_pending_device", None)
            save_config(cfg)
            return {"ok": False, "status": "expired", "instance": self.instance_id}
        resp = self._post_public(block, "/auth/device/poll", {"device_code": device_code})
        if resp.status_code == 429:
            return {"ok": False, "status": "slow_down", "retry_after": _retry_after(resp), "instance": self.instance_id}
        if resp.status_code == 404:
            block.pop("_pending_device", None)
            save_config(cfg)
            return {"ok": False, "status": "expired", "instance": self.instance_id}
        if resp.status_code >= 500:
            return {"ok": False, "status": "service_unavailable", "instance": self.instance_id}
        if resp.status_code >= 400:
            return {"ok": False, "status": problem_code(resp) or "poll_failed", "instance": self.instance_id}
        data = _json(resp)
        if not isinstance(data, Mapping):
            raise SiloInvalidResponse("invalid_response")
        status = str(data.get("status") or "").strip().lower() or "pending"
        try:
            interval = max(1, int(data.get("poll_after") or (pend or {}).get("interval") or POLL_INTERVAL_SEC))
        except Exception:
            interval = POLL_INTERVAL_SEC
        new_exp = _parse_epoch(data.get("expires_at"))
        if status == "pending":
            if new_exp and new_exp != expires_at and isinstance(block.get("_pending_device"), dict):
                block["_pending_device"]["expires_at"] = new_exp
                save_config(cfg)
            return {"ok": False, "status": "pending", "opened": bool(data.get("opened")), "interval": interval, "expires_at": new_exp or expires_at, "instance": self.instance_id}
        tokens = data.get("tokens")
        if status == "approved" and isinstance(tokens, Mapping):
            for key in ("profile_id", "profile_name", "profile_token", "profile_has_pin", "profile_token_expires_at"):
                block.pop(key, None)
            _apply_tokens(block, tokens)
            block.pop("_pending_device", None)
            block.pop("auth_error", None)
            save_config(cfg)
            self.cfg = cfg
            log(f"SILO: device login approved (instance={self.instance_id})", level="SUCCESS")
            profiles = self.list_profiles()
            auto = [p for p in profiles if not p["has_pin"]]
            if len(profiles) == 1 and auto:
                self._store_profile(cfg, auto[0], None, None)
            return {"ok": True, "status": "authorized", "instance": self.instance_id, "username": str(block.get("username") or ""), "profiles": profiles}
        block.pop("_pending_device", None)
        save_config(cfg)
        return {"ok": False, "status": status, "instance": self.instance_id}

    def cancel_device_login(self, cfg: dict[str, Any]) -> dict[str, Any]:
        block = writable_block(cfg, self.instance_id)
        pend = block.pop("_pending_device", None)
        device_code = str((pend or {}).get("device_code") or "").strip() if isinstance(pend, Mapping) else ""
        if device_code:
            try:
                self._post_public(block, "/auth/device/cancel", {"device_code": device_code})
            except SiloError:
                pass
            save_config(cfg)
        return {"ok": True, "cancelled": bool(device_code), "instance": self.instance_id}

    def refresh_token(self, cfg: Mapping[str, Any] | None = None, *, force: bool = False, rejected_token: str | None = None) -> dict[str, Any]:
        inst = token_instance(self.instance_id)
        with _refresh_lock(inst):
            full = _load_config()
            block = writable_block(full, inst)
            if not normalize_server_url(block.get("server_url")) and isinstance(cfg, Mapping):
                src = provider_block(cfg, inst)
                for key in ("server_url", "verify_ssl", "refresh_token", "device_id"):
                    if src.get(key) is not None and not block.get(key):
                        block[key] = src.get(key)
            if needs_reconnect(block):
                return {"ok": False, "status": "reconnect_required", "reconnect_required": True, "instance": inst}
            current = str(block.get("access_token") or "").strip()
            if not force and current and not about_to_expire(block):
                return {"ok": True, "status": "fresh", "instance": inst, "expires_at": int(block.get("expires_at") or 0)}
            rejected = str(rejected_token or "").strip()
            if rejected and current and current != rejected and not about_to_expire(block):
                return {"ok": True, "status": "fresh", "instance": inst, "expires_at": int(block.get("expires_at") or 0)}
            rt = str(block.get("refresh_token") or "").strip()
            if not rt or not normalize_server_url(block.get("server_url")):
                return {"ok": False, "status": "missing_refresh", "instance": inst}
            try:
                resp = self._post_public(block, "/auth/refresh", {"refresh_token": rt})
            except SiloServiceUnavailable:
                return {"ok": False, "status": "network_error", "instance": inst}
            if resp.status_code == 429 or resp.status_code >= 500:
                return {"ok": False, "status": "service_unavailable", "retry_after": _retry_after(resp), "instance": inst}
            if resp.status_code in (400, 401, 403, 404):
                block["auth_error"] = "reconnect_required"
                save_config(full)
                log(f"SILO: session ended, reconnect required (instance={inst} status={resp.status_code} code={problem_code(resp) or '-'})", level="ERROR")
                return {"ok": False, "status": "reconnect_required", "reconnect_required": True, "instance": inst}
            if resp.status_code >= 400:
                return {"ok": False, "status": f"refresh_failed:{resp.status_code}", "instance": inst}
            try:
                data = _json(resp)
                if not isinstance(data, Mapping):
                    raise SiloInvalidResponse("invalid_response")
                _apply_tokens(block, data)
            except SiloInvalidResponse:
                return {"ok": False, "status": "invalid_response", "instance": inst}
            save_config(full)
            if isinstance(cfg, dict):
                try:
                    dst = writable_block(cfg, inst)
                    for key in ("access_token", "refresh_token", "expires_at", "session_refreshed_at"):
                        dst[key] = block.get(key)
                except Exception:
                    pass
            self.cfg = full
            log(f"SILO: token refreshed (instance={inst})", level="INFO")
            return {"ok": True, "status": "ok", "instance": inst, "expires_at": int(block.get("expires_at") or 0)}

    def access_token(self, *, refresh: bool = True) -> str:
        block = self.block
        if needs_reconnect(block):
            raise SiloReconnectRequired("reconnect_required")
        if refresh and (not str(block.get("access_token") or "").strip() or about_to_expire(block)):
            res = self.refresh_token(self.cfg)
            if not res.get("ok"):
                if res.get("reconnect_required"):
                    raise SiloReconnectRequired("reconnect_required")
                raise SiloServiceUnavailable(str(res.get("status") or "refresh_failed"))
            self.cfg = _load_config()
            block = self.block
        token = str(block.get("access_token") or "").strip()
        if not token:
            raise SiloAuthError("missing_authentication")
        return token

    def request(
        self,
        method: str,
        path: str,
        *,
        profile: bool = True,
        json: Any = None,
        params: Mapping[str, Any] | None = None,
        timeout: float = HTTP_TIMEOUT,
        retry: bool = True,
    ) -> requests.Response:
        token = self.access_token()
        block = self.block
        headers = base_headers(block, token=token, profile=profile)
        if json is not None:
            headers["Content-Type"] = "application/json"
        try:
            resp = self.session.request(
                method.upper(),
                self._url(path, block),
                headers=headers,
                json=json,
                params=dict(params or {}) or None,
                timeout=timeout,
                verify=verify_ssl_of(block),
            )
        except requests.RequestException as exc:
            raise SiloServiceUnavailable("service_unavailable") from exc
        if resp.status_code == 401 and retry:
            res = self.refresh_token(self.cfg, force=True, rejected_token=token)
            if not res.get("ok"):
                if res.get("reconnect_required"):
                    raise SiloReconnectRequired("reconnect_required")
                return resp
            self.cfg = _load_config()
            return self.request(method, path, profile=profile, json=json, params=params, timeout=timeout, retry=False)
        if resp.status_code == 403 and problem_code(resp) == "profile_verification_required":
            self._flag_profile_pin()
            raise SiloProfileVerificationRequired("profile_verification_required")
        return resp

    def request_json(self, method: str, path: str, **kwargs: Any) -> Any:
        resp = self.request(method, path, **kwargs)
        if resp.status_code == 401:
            raise SiloAuthError("authentication_failed")
        if resp.status_code == 429:
            raise SiloRateLimited("rate_limited", _retry_after(resp))
        if resp.status_code >= 500:
            raise SiloServiceUnavailable("service_unavailable")
        if resp.status_code >= 400:
            raise SiloInvalidResponse(problem_code(resp) or f"http_{resp.status_code}")
        return _json(resp)

    def _flag_profile_pin(self) -> None:
        try:
            full = _load_config()
            block = sync_user_entry(full, self.instance_id) if is_sync_user_instance(self.instance_id) else writable_block(full, self.instance_id)
            if block is None:
                return
            if str(block.get("auth_error") or "") != "profile_pin_required":
                block["auth_error"] = "profile_pin_required"
                block["profile_token"] = ""
                save_config(full)
                log(f"SILO: profile PIN required (instance={self.instance_id})", level="WARN")
        except Exception:
            pass

    def account(self) -> dict[str, Any]:
        data = self.request_json("GET", "/account/me", profile=False)
        if not isinstance(data, Mapping) or not str(data.get("id") or "").strip():
            raise SiloInvalidResponse("invalid_account")
        return dict(data)

    def list_profiles(self) -> list[dict[str, Any]]:
        data = self.request_json("GET", "/profiles", profile=False)
        if not isinstance(data, Mapping) or not isinstance(data.get("items"), list):
            raise SiloInvalidResponse("profiles_not_list")
        return [normalize_profile(row) for row in data["items"] if isinstance(row, Mapping)]

    def _store_profile(self, cfg: dict[str, Any], profile: Mapping[str, Any], token: str | None, token_expires_at: int | None) -> None:
        block = writable_block(cfg, self.instance_id)
        block["profile_id"] = str(profile["id"])
        block["profile_name"] = str(profile.get("name") or "")
        block["profile_has_pin"] = bool(profile.get("has_pin"))
        block["profile_token"] = str(token or "")
        block["profile_token_expires_at"] = int(token_expires_at or 0)
        if needs_profile_pin(block):
            block.pop("auth_error", None)
        save_config(cfg)
        self.cfg = cfg
        log(f"SILO: profile selected (instance={self.instance_id})", level="INFO")

    def select_profile(self, cfg: dict[str, Any], profile_id: Any, pin: Any = None) -> dict[str, Any]:
        self.cfg = cfg
        pid = str(profile_id or "").strip()
        if not pid:
            raise SiloProfileError("invalid_profile")
        profiles = self.list_profiles()
        selected = next((p for p in profiles if p["id"] == pid), None)
        if not selected:
            raise SiloProfileError("profile_unavailable")
        if not selected["has_pin"]:
            self._store_profile(cfg, selected, None, None)
            return selected
        token, expires = self.verify_pin(pid, pin)
        self._store_profile(cfg, selected, token, expires)
        return selected

    def verify_pin(self, profile_id: str, pin: Any) -> tuple[str, int]:
        pin_value = str(pin or "").strip()
        if not pin_value:
            raise SiloProfileError("pin_required")
        resp = self.request("POST", f"/profiles/{quote(str(profile_id), safe='')}/verify-pin", profile=False, json={"pin": pin_value})
        if resp.status_code == 429:
            raise SiloRateLimited("pin_locked", _retry_after(resp))
        if resp.status_code >= 500:
            raise SiloServiceUnavailable("service_unavailable")
        if resp.status_code >= 400:
            raise SiloProfileError(problem_code(resp) or "pin_failed")
        data = _json(resp)
        if not isinstance(data, Mapping):
            raise SiloInvalidResponse("invalid_response")
        if not data.get("valid") or not str(data.get("profile_token") or "").strip():
            raise SiloProfileError("invalid_pin")
        return str(data.get("profile_token")), _parse_epoch(data.get("expires_at"))

    def disconnect(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg
        block = writable_block(cfg, self.instance_id)
        token = str(block.get("access_token") or "").strip()
        if token and normalize_server_url(block.get("server_url")):
            try:
                self.session.post(self._url("/auth/logout", block), headers=base_headers(block, token=token), timeout=10, verify=verify_ssl_of(block))
            except requests.RequestException:
                pass
        pend = block.get("_pending_device")
        if isinstance(pend, Mapping) and str(pend.get("device_code") or "").strip():
            try:
                self._post_public(block, "/auth/device/cancel", {"device_code": str(pend.get("device_code"))})
            except SiloError:
                pass
        clear_oauth(block)
        save_config(cfg)
        log(f"SILO: disconnected (instance={self.instance_id})", level="INFO")


def start_device_code(cfg: dict[str, Any] | None, *, instance_id: Any = None, server_url: Any = None, verify_ssl: Any = None, **_: Any) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_config()
    return SiloClient(cfgd, instance_id=instance_id).start_device_login(cfgd, server_url=server_url, verify_ssl=verify_ssl)


def poll_device_code(cfg: dict[str, Any] | None, *, instance_id: Any = None, **_: Any) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_config()
    return SiloClient(cfgd, instance_id=instance_id).poll_device_login(cfgd)


def cancel_device_code(cfg: dict[str, Any] | None = None, *, instance_id: Any = None, **_: Any) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_config()
    return SiloClient(cfgd, instance_id=instance_id).cancel_device_login(cfgd)


def refresh_token(cfg: dict[str, Any] | None = None, *, instance_id: Any = None, force: bool = False, **_: Any) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_config()
    return SiloClient(cfgd, instance_id=instance_id).refresh_token(cfgd, force=force)


def refresh_all(keepalive_s: int = KEEPALIVE_SEC) -> dict[str, str]:
    out: dict[str, str] = {}
    cfg = _load_config()
    for inst in list_instance_ids(cfg, "silo"):
        block = provider_block(cfg, inst)
        if not is_configured(block) or needs_reconnect(block):
            continue
        try:
            last = int(block.get("session_refreshed_at") or 0)
        except Exception:
            last = 0
        if now() - last < max(60, int(keepalive_s)):
            continue
        res = SiloClient(cfg, instance_id=inst).refresh_token(cfg, force=True)
        out[inst] = str(res.get("status") or "")
        cfg = _load_config()
    return out


def request_with_auth(
    session: requests.Session,
    method: str,
    url: str,
    *,
    cfg: Mapping[str, Any] | None,
    instance_id: Any = None,
    timeout: float = HTTP_TIMEOUT,
    max_retries: int = 3,
    request_func: Any = None,
    profile: bool = True,
    **kwargs: Any,
) -> requests.Response:
    from providers.sync._mod_common import request_with_retries

    call = request_func or request_with_retries
    client = SiloClient(cfg, instance_id=instance_id, session=session)

    def merged(token: str) -> dict[str, Any]:
        out = dict(kwargs)
        block = client.block
        headers = base_headers(block, token=token, profile=profile)
        headers.update({str(k): str(v) for k, v in dict(out.get("headers") or {}).items()})
        out["headers"] = headers
        out.setdefault("verify", verify_ssl_of(block))
        return out

    token = client.access_token()
    resp = call(session, method, url, timeout=timeout, max_retries=max_retries, **merged(token))
    if getattr(resp, "status_code", None) == 401:
        res = client.refresh_token(cfg, force=True, rejected_token=token)
        if not res.get("ok"):
            return resp
        client.cfg = _load_config()
        resp = call(session, method, url, timeout=timeout, max_retries=max_retries, **merged(client.access_token(refresh=False)))
    if getattr(resp, "status_code", None) == 403 and problem_code(resp) == "profile_verification_required":
        client._flag_profile_pin()
    return resp


class SiloAuth(AuthProvider):
    name = "SILO"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="SILO",
            label="Silo",
            flow="device_code",
            fields=[
                {"key": "silo.server_url", "label": "Server URL", "type": "text", "required": True},
                {"key": "silo.verify_ssl", "label": "Verify SSL", "type": "bool"},
            ],
            actions={"start": True, "finish": True, "refresh": True, "disconnect": True},
            verify_url=None,
            notes="Device sign-in on your own Silo server, then choose a household profile.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": False}

    def get_status(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        raw = status_for_block(provider_block(cfg, inst))
        label = "Silo" if inst == "default" else f"Silo ({inst})"
        return AuthStatus(
            connected=bool(raw.get("connected")),
            label=label,
            user=str(raw.get("username") or "") or None,
            expires_at=int(raw.get("expires_at") or 0) or None,
            extra={
                "authenticated": bool(raw.get("authenticated")),
                "profile_id": raw.get("profile_id") or "",
                "profile_name": raw.get("profile_name") or "",
                "server_url": raw.get("server_url") or "",
            },
        )

    def start(self, cfg: MutableMapping[str, Any] | None = None, *, redirect_uri: str | None = None, instance_id: Any = None) -> dict[str, Any]:
        cfgd = cfg if isinstance(cfg, dict) else _load_config()
        return start_device_code(cfgd, instance_id=instance_id)

    def finish(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None, **payload: Any) -> AuthStatus | Mapping[str, Any]:
        cfgd = cfg if isinstance(cfg, dict) else _load_config()
        client = SiloClient(cfgd, instance_id=instance_id)
        if payload.get("profile_id") is not None:
            selected = client.select_profile(cfgd, payload.get("profile_id"), payload.get("pin"))
            return {"ok": True, "status": "ok", "instance": normalize_instance_id(instance_id), "profile": selected}
        client.poll_device_login(cfgd)
        return self.get_status(cfgd, instance_id=instance_id)

    def refresh(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        refresh_token(dict(cfg or {}), instance_id=instance_id)
        return self.get_status(_load_config(), instance_id=instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfgd = cfg if isinstance(cfg, dict) else _load_config()
        SiloClient(cfgd, instance_id=instance_id).disconnect(cfgd)
        return self.get_status(cfgd, instance_id=instance_id)


def html() -> str:
    return r"""<div class="section" id="sec-silo">
  <style>
    #sec-silo .hidden{display:none!important}
    #sec-silo .muted{opacity:.7;font-size:.92em}
    #sec-silo .silo-server-row{display:flex;gap:10px;align-items:end;flex-wrap:wrap;margin-top:12px}
    #sec-silo .silo-server-url{width:min(380px,100%)}
    #sec-silo .silo-server-url input{width:100%}
    #sec-silo .silo-ssl{display:flex;align-items:center;gap:8px;padding-bottom:8px}
    #sec-silo .silo-actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:12px}
    #sec-silo .silo-profile-row{display:flex;gap:10px;align-items:end;flex-wrap:wrap;margin-top:12px}
    #sec-silo .silo-profile-row>div{width:min(320px,100%)}
    #sec-silo .silo-profile-row select{width:320px;min-width:280px}
    #sec-silo .silo-profile-row .cw-icon-select{width:320px;min-width:280px;max-width:100%}
    #sec-silo .silo-pin{width:min(160px,100%)}
    #sec-silo .silo-pin input{width:100%}
    #sec-silo .silo-qc{margin-top:12px;padding:14px;border-radius:12px;border:1px solid rgba(20,135,254,.35);background:rgba(0,52,251,.08)}
    #sec-silo .silo-qc-codewrap{display:flex;align-items:center;justify-content:center;gap:12px}
    #sec-silo .silo-qc-code{font-size:2em;font-weight:700;letter-spacing:.18em;padding:6px 0 6px .18em;color:#8fc2ff;text-align:center;font-variant-numeric:tabular-nums}
    #sec-silo .silo-qc-copy{appearance:none;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;width:34px;height:34px;border-radius:9px;flex:0 0 auto;border:1px solid rgba(20,135,254,.35);background:rgba(0,52,251,.1);color:#8fc2ff;transition:background .15s ease,border-color .15s ease,transform .12s ease}
    #sec-silo .silo-qc-copy:hover{background:rgba(0,52,251,.18);border-color:rgba(20,135,254,.6)}
    #sec-silo .silo-qc-copy:active{transform:scale(.94)}
    #sec-silo .silo-qc-copy.copied{background:rgba(0,52,251,.26);border-color:rgba(20,135,254,.75)}
    #sec-silo .silo-qc-copy svg{width:16px;height:16px;display:block}
    #sec-silo .silo-qc-line{display:flex;align-items:center;gap:8px 12px;flex-wrap:wrap;margin-top:6px}
    #sec-silo .silo-qc-meta{display:flex;justify-content:space-between;gap:12px;margin-top:6px}
    #sec-silo .msg{padding:8px 12px;border-radius:8px;border:1px solid rgba(0,255,170,.18);background:rgba(0,255,170,.08);color:#b9ffd7;font-weight:600}
    #sec-silo .msg.warn{border-color:rgba(255,210,0,.18);background:rgba(255,210,0,.08);color:#ffe9a6}
    #sec-silo #silo_connect{background:linear-gradient(135deg,#1487fe,#0034fb);border-color:rgba(20,135,254,.45);box-shadow:0 0 14px rgba(0,52,251,.35);color:#fff}
  </style>
  <div class="head" data-toggle-section="sec-silo">
    <span class="chev"></span><strong>Silo</strong>
  </div>
  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="silo">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">Silo <span class="badge feature-disabled">Experimental</span></div>
            <div class="muted">Sign in to your Silo server with a device code and choose the household profile for this CrossWatch instance.</div>
          </div>
        </div>
        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
          <button type="button" class="cw-subtile" data-sub="whitelist">Whitelisting</button>
          <button type="button" class="cw-subtile" data-sub="users">Users</button>
        </div>
        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="cw-auth-journey" style="--cw-auth-c1:20,135,254;--cw-auth-c2:245,11,79;--cw-auth-logo:url('/assets/img/SILO.svg')">
              <div class="cw-auth-journey-text">
                <div class="cw-auth-journey-title">Connect to Silo</div>
                <div class="cw-auth-journey-copy">Enter your Silo server URL, approve the code on its activate page, then choose a household profile.</div>
              </div>
            </div>
            <div class="silo-server-row">
              <div class="silo-server-url">
                <label for="silo_server_url">Server URL</label>
                <input id="silo_server_url" type="text" placeholder="http://192.168.1.10:8090" autocomplete="off" spellcheck="false" autocapitalize="off">
              </div>
              <label class="silo-ssl" for="silo_verify_ssl"><input id="silo_verify_ssl" type="checkbox"> Verify SSL</label>
            </div>
            <div class="silo-actions">
              <button id="silo_connect" class="btn" type="button">Connect Silo</button>
              <button id="silo_cancel" class="btn danger hidden" type="button">Cancel</button>
              <button id="silo_disconnect" class="hidden" type="button">Disconnect Silo</button>
              <div id="silo_msg" class="msg warn hidden" aria-live="polite"></div>
            </div>
            <div id="silo_login_state" class="silo-qc hidden">
              <input id="silo_code_input" type="hidden">
              <div class="silo-qc-codewrap">
                <div class="silo-qc-code" id="silo_code">---- ----</div>
                <button type="button" id="silo_code_copy" class="silo-qc-copy" title="Copy code" aria-label="Copy code">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                </button>
              </div>
              <div class="muted silo-qc-line">
                <span>Approve CrossWatch on your Silo server while signed in.</span>
                <a id="silo_login_url" href="#" target="_blank" rel="noopener">Open Silo activate page</a>
              </div>
              <div class="silo-qc-meta">
                <span class="muted" id="silo_polling">Waiting for approval...</span>
                <span class="muted" id="silo_expiry"></span>
              </div>
            </div>
            <div id="silo_profile_state" class="silo-profile-row hidden">
              <div>
                <label for="silo_profile_select">Silo profile</label>
                <select id="silo_profile_select" autocomplete="off"></select>
              </div>
              <div id="silo_pin_wrap" class="silo-pin hidden">
                <label for="silo_profile_pin">Profile PIN</label>
                <input id="silo_profile_pin" type="password" inputmode="numeric" autocomplete="off">
              </div>
            </div>
          </div>
          <div class="cw-subpanel" data-sub="whitelist">
            <div style="width:100%;max-width:none">
              <div id="silo_lib_matrix"></div>
            </div>
          </div>
          <div class="cw-subpanel" data-sub="users">
            <div id="silo_sync_users"></div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
"""


PROVIDER = SiloAuth()
__all__ = [
    "PROVIDER",
    "SiloAuth",
    "SiloClient",
    "html",
    "__VERSION__",
    "active_method",
    "cancel_device_code",
    "check_server",
    "clear_oauth",
    "is_configured",
    "normalize_auth_method",
    "normalize_server_url",
    "poll_device_code",
    "refresh_all",
    "refresh_token",
    "request_with_auth",
    "set_active_method",
    "start_device_code",
    "status_for_block",
]
