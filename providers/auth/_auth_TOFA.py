# providers/auth/_auth_TOFA.py
# CrossWatch - Tofa Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

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
API_PREFIX = "/api/v1"
CLIENT_NAME = "CrossWatch"
CLIENT_TYPE = "script"
DEFAULT_CONNECT_URL = "https://api.tofa.tv"
DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"
POLL_INTERVAL_SEC = 5
REFRESH_SKEW_SEC = 3 * 86400
KEEPALIVE_SEC = 7 * 86400
HTTP_TIMEOUT = 20
CLOUD_POST_PER_SEC = 5.0
PROFILE_ERROR_CODES = frozenset({"invalid_pin", "profile_locked", "profile_unlock_expired"})
UA = http_user_agent("TofaAuth", override_env="CW_TOFA_UA")

_SESSION_KEYS = (
    "access_token",
    "refresh_token",
    "expires_at",
    "user_id",
    "username",
    "is_admin",
    "profile_id",
    "profile_name",
    "profile_locked",
    "profile_primary",
    "profile_token",
    "profile_token_expires_at",
    "session_refreshed_at",
)

_REFRESH_LOCKS: dict[str, threading.Lock] = {}
_REFRESH_LOCKS_GUARD = threading.Lock()
_CLOUD_LIMITER: Any = None


class TofaError(RuntimeError):
    pass


class TofaAuthError(TofaError):
    pass


class TofaReconnectRequired(TofaAuthError):
    pass


class TofaProfileLocked(TofaAuthError):
    pass


class TofaServiceUnavailable(TofaError):
    pass


class TofaInvalidResponse(TofaError):
    pass


class TofaServerError(TofaError):
    pass


class TofaProfileError(TofaError):
    pass


class TofaRateLimited(TofaError):
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


def normalize_connect_url(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parts = urlsplit(raw)
    host = str(parts.hostname or "").lower()
    if parts.scheme != "https" or parts.username or parts.password or not (host == "tofa.tv" or host.endswith(".tofa.tv")):
        return ""
    return urlunsplit(("https", parts.netloc, parts.path.rstrip("/"), "", ""))


def connect_url_of(block: Mapping[str, Any] | None) -> str:
    return normalize_connect_url((block or {}).get("connect_url")) or DEFAULT_CONNECT_URL


def verify_ssl_of(block: Mapping[str, Any] | None) -> bool:
    raw = (block or {}).get("verify_ssl")
    return False if raw is None else bool(raw)


def provider_block(cfg: Mapping[str, Any] | None, instance_id: Any = None) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    base = (cfg or {}).get("tofa") if isinstance(cfg, Mapping) else None
    if is_sync_user_instance(inst):
        return resolve_sync_user_block(base, inst) if isinstance(base, Mapping) else {}
    insts = base.get("instances") if isinstance(base, Mapping) else None
    if inst != "default" and isinstance(insts, Mapping):
        own = insts.get(inst)
        return dict(own) if isinstance(own, Mapping) else {}
    return resolve_provider_block(cfg or {}, "tofa", inst)


def token_instance(instance_id: Any) -> str:
    ref = split_sync_user_id(normalize_instance_id(instance_id))
    return ref[0] if ref else normalize_instance_id(instance_id)


def sync_user_entry(cfg: dict[str, Any], instance_id: Any) -> dict[str, Any] | None:
    ref = split_sync_user_id(normalize_instance_id(instance_id))
    if ref is None:
        return None
    parent = ensure_instance_block(cfg, "tofa", ref[0])
    users = parent.get("sync_users")
    entry = users.get(ref[1]) if isinstance(users, dict) else None
    return entry if isinstance(entry, dict) else None


def profile_block(cfg: dict[str, Any], instance_id: Any) -> dict[str, Any] | None:
    if is_sync_user_instance(instance_id):
        return sync_user_entry(cfg, instance_id)
    return writable_block(cfg, instance_id)


def writable_block(cfg: dict[str, Any], instance_id: Any = None) -> dict[str, Any]:
    probe = cfg.get("_cw_probe") if isinstance(cfg.get("_cw_probe"), Mapping) else {}
    if str((probe or {}).get("provider") or "").upper() == "TOFA" and normalize_instance_id((probe or {}).get("instance")) == normalize_instance_id(instance_id):
        view = cfg.get("tofa")
        if isinstance(view, dict):
            return view
    return ensure_instance_block(cfg, "tofa", instance_id)


def has_auth(block: Mapping[str, Any] | None) -> bool:
    b = block or {}
    return bool(str(b.get("access_token") or "").strip() or str(b.get("refresh_token") or "").strip())


def needs_reconnect(block: Mapping[str, Any] | None) -> bool:
    return str((block or {}).get("auth_error") or "") == "reconnect_required"


def profile_token_expired(block: Mapping[str, Any] | None) -> bool:
    b = block or {}
    if not b.get("profile_locked"):
        return False
    try:
        exp = int(b.get("profile_token_expires_at") or 0)
    except Exception:
        exp = 0
    return not str(b.get("profile_token") or "").strip() or bool(exp and exp <= now())


def needs_profile_pin(block: Mapping[str, Any] | None) -> bool:
    b = block or {}
    if not str(b.get("profile_id") or "").strip():
        return False
    return str(b.get("auth_error") or "") == "profile_pin_required" or profile_token_expired(b)


def is_configured(block: Mapping[str, Any] | None) -> bool:
    b = block or {}
    return bool(
        normalize_server_url(b.get("server_url"))
        and str(b.get("server_id") or "").strip()
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
        if key in {"expires_at", "profile_token_expires_at", "session_refreshed_at"}:
            block[key] = 0
        elif key in {"is_admin", "profile_locked", "profile_primary"}:
            block[key] = False
        else:
            block[key] = ""
    block.pop("_pending_device", None)
    block["auth_error"] = ""


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
        "is_admin": bool(b.get("is_admin")),
        "profile_id": str(b.get("profile_id") or ""),
        "profile_name": str(b.get("profile_name") or ""),
        "profile_locked": bool(b.get("profile_locked")),
        "profile_token_expires_at": int(b.get("profile_token_expires_at") or 0) if str(b.get("profile_token_expires_at") or "").strip() else 0,
        "expires_at": int(b.get("expires_at") or 0) if str(b.get("expires_at") or "").strip() else 0,
    }
    pending = _pending_view(b)
    if pending:
        out["pending"] = pending
    return out


def parse_epoch(value: Any) -> int:
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


def error_code(resp: requests.Response) -> str:
    try:
        body = resp.json()
    except Exception:
        return ""
    if not isinstance(body, Mapping):
        return ""
    return str(body.get("code") or body.get("error") or "").strip()


def _json(resp: requests.Response) -> Any:
    if not str(getattr(resp, "text", "") or "").strip():
        return {}
    try:
        return resp.json()
    except Exception as exc:
        raise TofaInvalidResponse("invalid_json") from exc


def _apply_tokens(block: MutableMapping[str, Any], tok: Mapping[str, Any]) -> None:
    access = str(tok.get("access_token") or "").strip()
    if not access:
        raise TofaInvalidResponse("missing_access_token")
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
        block["auth_error"] = ""


def normalize_profile(row: Mapping[str, Any]) -> dict[str, Any]:
    pid = str(row.get("id") or "").strip()
    if not pid:
        raise TofaInvalidResponse("invalid_profile_id")
    return {
        "id": pid,
        "profile_id": pid,
        "name": str(row.get("name") or "").strip() or "Profile",
        "is_primary": bool(row.get("is_primary")),
        "has_pin": bool(row.get("is_locked")),
        "is_child": bool(row.get("is_kids")),
    }


def _refresh_lock(instance_id: Any) -> threading.Lock:
    inst = normalize_instance_id(instance_id)
    with _REFRESH_LOCKS_GUARD:
        lock = _REFRESH_LOCKS.get(inst)
        if lock is None:
            lock = threading.Lock()
            _REFRESH_LOCKS[inst] = lock
        return lock


def _cloud_limiter() -> Any:
    global _CLOUD_LIMITER
    if _CLOUD_LIMITER is None:
        from providers.sync._mod_common import SimpleRateLimiter

        with _REFRESH_LOCKS_GUARD:
            if _CLOUD_LIMITER is None:
                _CLOUD_LIMITER = SimpleRateLimiter(rates_per_sec={"POST": CLOUD_POST_PER_SEC})
    return _CLOUD_LIMITER


def _load_config() -> dict[str, Any]:
    try:
        cfg = load_config() or {}
        return cfg if isinstance(cfg, dict) else dict(cfg)
    except Exception:
        return {}


def base_headers(block: Mapping[str, Any] | None, *, token: str | None = None, profile: bool = False) -> dict[str, str]:
    b = block or {}
    h = {"Accept": "application/json", "User-Agent": UA}
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
        raise TofaServerError("invalid_url")
    p = "/" + str(path or "").lstrip("/")
    if not p.startswith(API_PREFIX + "/"):
        p = API_PREFIX + p
    return f"{base}{p}"


def check_server(server_url: Any, *, verify_ssl: bool = False, session: requests.Session | None = None, timeout: float = HTTP_TIMEOUT) -> dict[str, Any]:
    base = normalize_server_url(server_url)
    if not base:
        raise TofaServerError("invalid_url")
    sess = session or requests.Session()
    try:
        resp = sess.get(api_url(base, "/auth/status"), headers=base_headers({}), timeout=timeout, verify=verify_ssl)
    except requests.exceptions.SSLError as exc:
        raise TofaServerError("ssl_error") from exc
    except requests.RequestException as exc:
        raise TofaServerError("connection_failed") from exc
    if resp.status_code == 404:
        raise TofaServerError("not_tofa")
    if resp.status_code >= 400:
        raise TofaServerError("http_error")
    try:
        data = resp.json()
    except Exception as exc:
        raise TofaServerError("not_tofa") from exc
    if not isinstance(data, Mapping) or "claimed" not in data or not str(data.get("server_id") or "").strip():
        raise TofaServerError("not_tofa")
    if not data.get("claimed"):
        raise TofaServerError("server_unclaimed")
    connect = normalize_connect_url(data.get("connect_url")) if data.get("connect_url") else DEFAULT_CONNECT_URL
    if not connect:
        raise TofaServerError("invalid_connect_url")
    return {"server_url": base, "server_id": str(data.get("server_id")).strip(), "connect_url": connect}


class TofaClient:
    def __init__(self, cfg: Mapping[str, Any] | None, *, instance_id: Any = None, session: requests.Session | None = None):
        self.cfg = cfg if cfg is not None else {}
        self.instance_id = normalize_instance_id(instance_id)
        self.session = session or requests.Session()

    @property
    def block(self) -> dict[str, Any]:
        return provider_block(self.cfg, self.instance_id)

    def _url(self, path: str, block: Mapping[str, Any] | None = None) -> str:
        return api_url(str((block or self.block).get("server_url") or ""), path)

    def _cloud_post(self, block: Mapping[str, Any], path: str, *, json: Mapping[str, Any] | None = None,
                    data: Mapping[str, Any] | None = None) -> requests.Response:
        url = connect_url_of(block) + "/" + str(path or "").lstrip("/")
        headers = base_headers({})
        if json is not None:
            headers["Content-Type"] = "application/json"
        try:
            _cloud_limiter().wait("POST")
            return self.session.post(url, json=dict(json) if json is not None else None, data=dict(data) if data is not None else None,
                                     headers=headers, timeout=HTTP_TIMEOUT)
        except requests.RequestException as exc:
            raise TofaServiceUnavailable("service_unavailable") from exc

    def configure_server(self, cfg: dict[str, Any], server_url: Any, verify_ssl: Any = None) -> dict[str, Any]:
        block = writable_block(cfg, self.instance_id)
        verify = verify_ssl_of(block) if verify_ssl is None else bool(verify_ssl)
        found = check_server(server_url, verify_ssl=verify, session=self.session)
        new_url = found["server_url"]
        old_url = normalize_server_url(block.get("server_url"))
        changed = old_url != new_url or str(block.get("server_id") or "") != found["server_id"]
        if changed and has_auth(block):
            raise TofaServerError("disconnect_first")
        if changed:
            clear_oauth(block)
        block["server_url"] = new_url
        block["server_id"] = found["server_id"]
        block["connect_url"] = found["connect_url"]
        block["verify_ssl"] = verify
        save_config(cfg)
        if changed:
            log(f"TOFA: server set (instance={self.instance_id})", level="INFO")
        self.cfg = cfg
        return {"ok": True, "instance": self.instance_id, "server_url": new_url, "verify_ssl": verify, "changed": changed}

    def start_device_login(self, cfg: dict[str, Any], *, server_url: Any = None, verify_ssl: Any = None) -> dict[str, Any]:
        if server_url is not None and str(server_url).strip():
            self.configure_server(cfg, server_url, verify_ssl)
        block = writable_block(cfg, self.instance_id)
        if not normalize_server_url(block.get("server_url")) or not str(block.get("server_id") or "").strip():
            raise TofaServerError("invalid_url")
        set_active_method(block)
        resp = self._cloud_post(block, "/device/code", json={"server_id": str(block["server_id"]), "client_name": CLIENT_NAME, "client_type": CLIENT_TYPE})
        if resp.status_code == 429:
            raise TofaRateLimited("rate_limited", _retry_after(resp))
        if resp.status_code >= 500:
            raise TofaServiceUnavailable("service_unavailable")
        if resp.status_code >= 400:
            raise TofaServerError(error_code(resp) or "http_error")
        data = _json(resp)
        if not isinstance(data, Mapping):
            raise TofaInvalidResponse("invalid_response")
        device_code = str(data.get("device_code") or "").strip()
        user_code = str(data.get("user_code") or "").strip()
        if not device_code or not user_code:
            raise TofaInvalidResponse("missing_device_code")
        try:
            interval = max(1, int(data.get("interval") or POLL_INTERVAL_SEC))
        except Exception:
            interval = POLL_INTERVAL_SEC
        try:
            expires_at = now() + int(data.get("expires_in") or 600)
        except Exception:
            expires_at = now() + 600
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
        log(f"TOFA: device login started (instance={self.instance_id})", level="INFO")
        out = {"ok": True, "instance": self.instance_id, **(_pending_view(block) or {})}
        qr = str(data.get("qr_code_svg") or "").strip()
        if qr.startswith("<svg"):
            out["qr_code_svg"] = qr
        return out

    def poll_device_login(self, cfg: dict[str, Any]) -> dict[str, Any]:
        block = writable_block(cfg, self.instance_id)
        pend = block.get("_pending_device") if isinstance(block.get("_pending_device"), Mapping) else {}
        device_code = str((pend or {}).get("device_code") or "").strip()
        if not device_code:
            return {"ok": False, "status": "no_pending_login", "instance": self.instance_id}
        expires_at = int((pend or {}).get("expires_at") or 0)
        interval = int((pend or {}).get("interval") or POLL_INTERVAL_SEC)
        if expires_at and now() >= expires_at:
            block.pop("_pending_device", None)
            save_config(cfg)
            return {"ok": False, "status": "expired", "instance": self.instance_id}
        resp = self._cloud_post(block, "/device/token", data={"grant_type": DEVICE_GRANT, "device_code": device_code})
        if resp.status_code == 429 or resp.status_code >= 500:
            return {"ok": False, "status": "service_unavailable", "retry_after": _retry_after(resp), "instance": self.instance_id}
        if resp.status_code >= 400:
            code = error_code(resp)
            if code == "authorization_pending":
                return {"ok": False, "status": "pending", "interval": interval, "expires_at": expires_at, "instance": self.instance_id}
            if code == "slow_down":
                interval += 5
                if isinstance(block.get("_pending_device"), dict):
                    block["_pending_device"]["interval"] = interval
                    save_config(cfg)
                return {"ok": False, "status": "slow_down", "interval": interval, "instance": self.instance_id}
            block.pop("_pending_device", None)
            save_config(cfg)
            status = {"expired_token": "expired", "access_denied": "denied"}.get(code, code or "poll_failed")
            return {"ok": False, "status": status, "instance": self.instance_id}
        data = _json(resp)
        if not isinstance(data, Mapping):
            raise TofaInvalidResponse("invalid_response")
        block.update({"profile_id": "", "profile_name": "", "profile_token": "", "profile_locked": False, "profile_token_expires_at": 0,
                      "user_id": "", "username": "", "is_admin": False})
        _apply_tokens(block, data)
        block.pop("_pending_device", None)
        block["auth_error"] = ""
        save_config(cfg)
        self.cfg = cfg
        log(f"TOFA: device login approved (instance={self.instance_id})", level="SUCCESS")
        profiles = self.list_profiles()
        auto = [p for p in profiles if not p["has_pin"]]
        if len(profiles) == 1 and auto:
            self._store_profile(cfg, auto[0], None, None)
        return {"ok": True, "status": "authorized", "instance": self.instance_id, "profiles": profiles}

    def cancel_device_login(self, cfg: dict[str, Any]) -> dict[str, Any]:
        block = writable_block(cfg, self.instance_id)
        pend = block.pop("_pending_device", None)
        cancelled = isinstance(pend, Mapping) and bool(str(pend.get("device_code") or "").strip())
        if cancelled:
            save_config(cfg)
        return {"ok": True, "cancelled": cancelled, "instance": self.instance_id}

    def refresh_token(self, cfg: Mapping[str, Any] | None = None, *, force: bool = False, rejected_token: str | None = None) -> dict[str, Any]:
        inst = token_instance(self.instance_id)
        with _refresh_lock(inst):
            full = _load_config()
            block = writable_block(full, inst)
            if not normalize_server_url(block.get("server_url")) and isinstance(cfg, Mapping):
                src = provider_block(cfg, inst)
                for key in ("server_url", "server_id", "connect_url", "verify_ssl", "refresh_token"):
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
            server_id = str(block.get("server_id") or "").strip()
            if not rt or not server_id:
                return {"ok": False, "status": "missing_refresh", "instance": inst}
            try:
                resp = self._cloud_post(block, f"/servers/{quote(server_id, safe='')}/device-token/refresh", json={"refresh_token": rt})
            except TofaServiceUnavailable:
                return {"ok": False, "status": "network_error", "instance": inst}
            if resp.status_code == 429 or resp.status_code >= 500:
                return {"ok": False, "status": "service_unavailable", "retry_after": _retry_after(resp), "instance": inst}
            if resp.status_code in (400, 401, 403, 404):
                block["auth_error"] = "reconnect_required"
                save_config(full)
                log(f"TOFA: session ended, reconnect required (instance={inst} status={resp.status_code} code={error_code(resp) or '-'})", level="ERROR")
                return {"ok": False, "status": "reconnect_required", "reconnect_required": True, "instance": inst}
            if resp.status_code >= 400:
                return {"ok": False, "status": f"refresh_failed:{resp.status_code}", "instance": inst}
            try:
                data = _json(resp)
                if not isinstance(data, Mapping):
                    raise TofaInvalidResponse("invalid_response")
                rotated = bool(str(data.get("refresh_token") or "").strip()) and str(data.get("refresh_token")).strip() != rt
                _apply_tokens(block, data)
            except TofaInvalidResponse:
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
            log(f"TOFA: token refreshed (instance={inst} rotated={rotated})", level="INFO")
            return {"ok": True, "status": "ok", "instance": inst, "expires_at": int(block.get("expires_at") or 0)}

    def access_token(self, *, refresh: bool = True) -> str:
        block = self.block
        if needs_reconnect(block):
            raise TofaReconnectRequired("reconnect_required")
        if refresh and (not str(block.get("access_token") or "").strip() or about_to_expire(block)):
            res = self.refresh_token(self.cfg)
            if not res.get("ok"):
                if res.get("reconnect_required"):
                    raise TofaReconnectRequired("reconnect_required")
                raise TofaServiceUnavailable(str(res.get("status") or "refresh_failed"))
            self.cfg = _load_config()
            block = self.block
        token = str(block.get("access_token") or "").strip()
        if not token:
            raise TofaAuthError("missing_authentication")
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
            raise TofaServiceUnavailable("service_unavailable") from exc
        if resp.status_code == 401 and error_code(resp) == "profile_unlock_expired":
            if profile and retry and self._profile_unlocked_again():
                self.cfg = _load_config()
                return self.request(method, path, profile=profile, json=json, params=params, timeout=timeout, retry=False)
            raise TofaProfileLocked("profile_locked")
        if resp.status_code == 401 and retry and error_code(resp) not in PROFILE_ERROR_CODES:
            res = self.refresh_token(self.cfg, force=True, rejected_token=token)
            if not res.get("ok"):
                if res.get("reconnect_required"):
                    raise TofaReconnectRequired("reconnect_required")
                return resp
            self.cfg = _load_config()
            return self.request(method, path, profile=profile, json=json, params=params, timeout=timeout, retry=False)
        if resp.status_code == 403 and error_code(resp) == "profile_locked":
            self._flag_profile_pin()
            raise TofaProfileLocked("profile_locked")
        return resp

    def request_json(self, method: str, path: str, **kwargs: Any) -> Any:
        resp = self.request(method, path, **kwargs)
        if resp.status_code == 401:
            raise TofaAuthError("authentication_failed")
        if resp.status_code == 429:
            raise TofaRateLimited("rate_limited", _retry_after(resp))
        if resp.status_code >= 500:
            raise TofaServiceUnavailable("service_unavailable")
        if resp.status_code >= 400:
            raise TofaInvalidResponse(error_code(resp) or f"http_{resp.status_code}")
        return _json(resp)

    def _profile_unlocked_again(self) -> bool:
        pid = str(self.block.get("profile_id") or "").strip()
        try:
            row = next((p for p in self.list_profiles() if p["id"] == pid), None)
        except TofaError:
            row = None
        if row is None or row["has_pin"]:
            self._flag_profile_pin()
            return False
        try:
            full = _load_config()
            block = profile_block(full, self.instance_id)
            if block is None:
                return False
            block["profile_locked"] = False
            block["profile_token"] = ""
            block["profile_token_expires_at"] = 0
            if str(block.get("auth_error") or "") == "profile_pin_required":
                block["auth_error"] = ""
            save_config(full)
            log(f"TOFA: profile PIN removed, unlock token dropped (instance={self.instance_id})", level="INFO")
        except Exception:
            return False
        return True

    def _flag_profile_pin(self) -> None:
        try:
            full = _load_config()
            block = profile_block(full, self.instance_id)
            if block is None:
                return
            if str(block.get("auth_error") or "") != "profile_pin_required":
                block["auth_error"] = "profile_pin_required"
                block["profile_token"] = ""
                save_config(full)
                log(f"TOFA: profile PIN required (instance={self.instance_id})", level="WARN")
        except Exception:
            pass

    def account(self) -> dict[str, Any]:
        data = self.request_json("GET", "/users/me", profile=True)
        if not isinstance(data, Mapping) or not str(data.get("id") or "").strip():
            raise TofaInvalidResponse("invalid_account")
        return dict(data)

    def list_profiles(self) -> list[dict[str, Any]]:
        data = self.request_json("GET", "/profiles", profile=False)
        if not isinstance(data, list):
            raise TofaInvalidResponse("profiles_not_list")
        return [normalize_profile(row) for row in data if isinstance(row, Mapping)]

    def _store_profile(self, cfg: dict[str, Any], profile: Mapping[str, Any], token: str | None, token_expires_at: int | None) -> None:
        block = writable_block(cfg, self.instance_id)
        block["profile_id"] = str(profile["id"])
        block["profile_name"] = str(profile.get("name") or "")
        block["profile_locked"] = bool(profile.get("has_pin"))
        block["profile_primary"] = bool(profile.get("is_primary"))
        block["profile_token"] = str(token or "")
        block["profile_token_expires_at"] = int(token_expires_at or 0)
        if str(block.get("auth_error") or "") == "profile_pin_required":
            block["auth_error"] = ""
        self.cfg = cfg
        try:
            me = self.account()
            block["user_id"] = str(me.get("id") or "")
            block["username"] = str(me.get("username") or "")
            block["is_admin"] = bool(me.get("is_admin"))
        except TofaError as exc:
            log(f"TOFA: account lookup failed (instance={self.instance_id} reason={exc})", level="WARN")
        save_config(cfg)
        log(f"TOFA: profile selected (instance={self.instance_id})", level="INFO")

    def select_profile(self, cfg: dict[str, Any], profile_id: Any, pin: Any = None) -> dict[str, Any]:
        self.cfg = cfg
        pid = str(profile_id or "").strip()
        if not pid:
            raise TofaProfileError("invalid_profile")
        profiles = self.list_profiles()
        selected = next((p for p in profiles if p["id"] == pid), None)
        if not selected:
            raise TofaProfileError("profile_unavailable")
        if not selected["has_pin"]:
            self._store_profile(cfg, selected, None, None)
            return selected
        token, expires = self.verify_pin(pid, pin)
        self._store_profile(cfg, selected, token, expires)
        return selected

    def verify_pin(self, profile_id: str, pin: Any) -> tuple[str, int]:
        pin_value = str(pin or "").strip()
        if not pin_value:
            raise TofaProfileError("pin_required")
        resp = self.request("POST", f"/profiles/{quote(str(profile_id), safe='')}/verify-pin", profile=False,
                            json={"pin": pin_value, "remember_device": True})
        if resp.status_code in (423, 429):
            raise TofaRateLimited("pin_locked", _retry_after(resp))
        if resp.status_code >= 500:
            raise TofaServiceUnavailable("service_unavailable")
        if resp.status_code == 401 or error_code(resp) == "invalid_pin":
            raise TofaProfileError("invalid_pin")
        if resp.status_code >= 400:
            raise TofaProfileError(error_code(resp) or "pin_failed")
        data = _json(resp)
        if not isinstance(data, Mapping) or not str(data.get("profile_token") or "").strip():
            raise TofaInvalidResponse("invalid_response")
        return str(data.get("profile_token")), parse_epoch(data.get("expires_at"))

    def disconnect(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg
        block = writable_block(cfg, self.instance_id)
        clear_oauth(block)
        save_config(cfg)
        log(f"TOFA: disconnected (instance={self.instance_id})", level="INFO")


def start_device_code(cfg: dict[str, Any] | None, *, instance_id: Any = None, server_url: Any = None, verify_ssl: Any = None, **_: Any) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_config()
    return TofaClient(cfgd, instance_id=instance_id).start_device_login(cfgd, server_url=server_url, verify_ssl=verify_ssl)


def poll_device_code(cfg: dict[str, Any] | None, *, instance_id: Any = None, **_: Any) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_config()
    return TofaClient(cfgd, instance_id=instance_id).poll_device_login(cfgd)


def cancel_device_code(cfg: dict[str, Any] | None = None, *, instance_id: Any = None, **_: Any) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_config()
    return TofaClient(cfgd, instance_id=instance_id).cancel_device_login(cfgd)


def refresh_token(cfg: dict[str, Any] | None = None, *, instance_id: Any = None, force: bool = False, **_: Any) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_config()
    return TofaClient(cfgd, instance_id=instance_id).refresh_token(cfgd, force=force)


def refresh_all(keepalive_s: int = KEEPALIVE_SEC) -> dict[str, str]:
    out: dict[str, str] = {}
    cfg = _load_config()
    for inst in list_instance_ids(cfg, "tofa"):
        block = provider_block(cfg, inst)
        if not is_configured(block) or needs_reconnect(block):
            continue
        try:
            last = int(block.get("session_refreshed_at") or 0)
        except Exception:
            last = 0
        if now() - last < max(60, int(keepalive_s)) and not about_to_expire(block):
            continue
        res = TofaClient(cfg, instance_id=inst).refresh_token(cfg, force=True)
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
    client = TofaClient(cfg, instance_id=instance_id, session=session)

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
    if getattr(resp, "status_code", None) == 401 and error_code(resp) == "profile_unlock_expired":
        if profile and client._profile_unlocked_again():
            client.cfg = _load_config()
            return call(session, method, url, timeout=timeout, max_retries=max_retries, **merged(client.access_token(refresh=False)))
        return resp
    if getattr(resp, "status_code", None) == 401 and error_code(resp) not in PROFILE_ERROR_CODES:
        res = client.refresh_token(cfg, force=True, rejected_token=token)
        if not res.get("ok"):
            return resp
        client.cfg = _load_config()
        resp = call(session, method, url, timeout=timeout, max_retries=max_retries, **merged(client.access_token(refresh=False)))
    if getattr(resp, "status_code", None) == 403 and error_code(resp) == "profile_locked":
        client._flag_profile_pin()
    return resp


class TofaAuth(AuthProvider):
    name = "TOFA"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="TOFA",
            label="Tofa",
            flow="device_code",
            fields=[
                {"key": "tofa.server_url", "label": "Server URL", "type": "text", "required": True},
                {"key": "tofa.verify_ssl", "label": "Verify SSL", "type": "bool"},
            ],
            actions={"start": True, "finish": True, "refresh": True, "disconnect": True},
            verify_url=None,
            notes="Device sign-in through your Tofa account, then choose a household profile.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"watchlist": True, "ratings": False, "history": True, "progress": True, "playlists": False}

    def get_status(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        raw = status_for_block(provider_block(cfg, inst))
        label = "Tofa" if inst == "default" else f"Tofa ({inst})"
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

    def start(self, cfg: MutableMapping[str, Any] | None = None, redirect_uri: str | None = None, *, instance_id: Any = None) -> dict[str, Any]:
        cfgd = cfg if isinstance(cfg, dict) else _load_config()
        return start_device_code(cfgd, instance_id=instance_id)

    def finish(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None, **payload: Any) -> AuthStatus | Mapping[str, Any]:
        cfgd = cfg if isinstance(cfg, dict) else _load_config()
        client = TofaClient(cfgd, instance_id=instance_id)
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
        TofaClient(cfgd, instance_id=instance_id).disconnect(cfgd)
        return self.get_status(cfgd, instance_id=instance_id)


def html() -> str:
    return r"""<div class="section" id="sec-tofa">
  <style>
    #sec-tofa .hidden{display:none!important}
    #sec-tofa .muted{opacity:.7;font-size:.92em}
    #sec-tofa .tofa-server-row{display:flex;gap:10px;align-items:end;flex-wrap:wrap;margin-top:12px}
    #sec-tofa .tofa-server-url{width:min(380px,100%)}
    #sec-tofa .tofa-server-url input{width:100%}
    #sec-tofa .tofa-ssl{display:flex;align-items:center;gap:8px;padding-bottom:8px}
    #sec-tofa .tofa-actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:12px}
    #sec-tofa .tofa-profile-row{display:flex;gap:10px;align-items:end;flex-wrap:wrap;margin-top:12px}
    #sec-tofa .tofa-profile-row>div{width:min(320px,100%)}
    #sec-tofa .tofa-profile-row select{width:320px;min-width:280px}
    #sec-tofa .tofa-profile-row .cw-icon-select{width:320px;min-width:280px;max-width:100%}
    #sec-tofa .tofa-pin{width:min(160px,100%)}
    #sec-tofa .tofa-pin input{width:100%}
    #sec-tofa .tofa-qc{margin-top:12px;padding:14px;border-radius:12px;border:1px solid rgba(8,214,184,.35);background:rgba(8,214,184,.08)}
    #sec-tofa .tofa-qc-body{display:flex;align-items:center;justify-content:center;gap:18px;flex-wrap:wrap}
    #sec-tofa .tofa-qc-qr{width:132px;height:132px;padding:6px;border-radius:10px;background:#fff}
    #sec-tofa .tofa-qc-qr img{width:100%;height:100%;display:block}
    #sec-tofa .tofa-qc-codewrap{display:flex;align-items:center;justify-content:center;gap:12px}
    #sec-tofa .tofa-qc-code{font-size:2em;font-weight:700;letter-spacing:.18em;padding:6px 0 6px .18em;color:#7cf0dc;text-align:center;font-variant-numeric:tabular-nums}
    #sec-tofa .tofa-qc-copy{appearance:none;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;width:34px;height:34px;border-radius:9px;flex:0 0 auto;border:1px solid rgba(8,214,184,.35);background:rgba(8,214,184,.1);color:#7cf0dc;transition:background .15s ease,border-color .15s ease,transform .12s ease}
    #sec-tofa .tofa-qc-copy:hover{background:rgba(8,214,184,.18);border-color:rgba(8,214,184,.6)}
    #sec-tofa .tofa-qc-copy:active{transform:scale(.94)}
    #sec-tofa .tofa-qc-copy.copied{background:rgba(8,214,184,.26);border-color:rgba(8,214,184,.75)}
    #sec-tofa .tofa-qc-copy svg{width:16px;height:16px;display:block}
    #sec-tofa .tofa-qc-line{display:flex;align-items:center;gap:8px 12px;flex-wrap:wrap;margin-top:6px}
    #sec-tofa .tofa-qc-meta{display:flex;justify-content:space-between;gap:12px;margin-top:6px}
    #sec-tofa .msg{padding:8px 12px;border-radius:8px;border:1px solid rgba(0,255,170,.18);background:rgba(0,255,170,.08);color:#b9ffd7;font-weight:600}
    #sec-tofa .msg.warn{border-color:rgba(255,210,0,.18);background:rgba(255,210,0,.08);color:#ffe9a6}
    #sec-tofa #tofa_connect{background:linear-gradient(135deg,#08d6b8,#009894);border-color:rgba(8,214,184,.45);box-shadow:0 0 14px rgba(8,214,184,.35);color:#fff}
  </style>
  <div class="head" data-toggle-section="sec-tofa">
    <span class="chev"></span><strong>Tofa</strong>
  </div>
  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="tofa">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">Tofa <span class="badge feature-disabled">Experimental</span></div>
            <div class="muted">Sign in to your Tofa server with a device code and choose the household profile for this CrossWatch instance.</div>
          </div>
        </div>
        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
          <button type="button" class="cw-subtile" data-sub="whitelist">Whitelisting</button>
          <button type="button" class="cw-subtile" data-sub="users">Identities</button>
        </div>
        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="cw-auth-journey" style="--cw-auth-c1:8,214,184;--cw-auth-c2:0,152,148;--cw-auth-logo:url('/assets/img/TOFA.svg')">
              <div class="cw-auth-journey-text">
                <div class="cw-auth-journey-title">Connect to Tofa</div>
                <div class="cw-auth-journey-copy">Enter your Tofa server URL, approve the code with your Tofa account, then choose a household profile.</div>
              </div>
            </div>
            <div class="tofa-server-row">
              <div class="tofa-server-url">
                <label for="tofa_server_url">Server URL</label>
                <input id="tofa_server_url" type="text" placeholder="http://192.168.1.10:33333" autocomplete="off" spellcheck="false" autocapitalize="off">
              </div>
              <label class="tofa-ssl" for="tofa_verify_ssl"><input id="tofa_verify_ssl" type="checkbox"> Verify SSL</label>
            </div>
            <div class="tofa-actions">
              <button id="tofa_connect" class="btn" type="button">Connect Tofa</button>
              <button id="tofa_cancel" class="btn danger hidden" type="button">Cancel</button>
              <button id="tofa_disconnect" class="hidden" type="button">Disconnect Tofa</button>
              <div id="tofa_msg" class="msg warn hidden" aria-live="polite"></div>
            </div>
            <div id="tofa_login_state" class="tofa-qc hidden">
              <input id="tofa_code_input" type="hidden">
              <div class="tofa-qc-body">
                <div id="tofa_qr" class="tofa-qc-qr hidden" aria-hidden="true"></div>
                <div class="tofa-qc-codewrap">
                  <div class="tofa-qc-code" id="tofa_code">----</div>
                  <button type="button" id="tofa_code_copy" class="tofa-qc-copy" title="Copy code" aria-label="Copy code">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                  </button>
                </div>
              </div>
              <div class="muted tofa-qc-line">
                <span>Approve CrossWatch while signed in to your Tofa account.</span>
                <a id="tofa_login_url" href="#" target="_blank" rel="noopener">Open Tofa link page</a>
              </div>
              <div class="tofa-qc-meta">
                <span class="muted" id="tofa_polling">Waiting for approval...</span>
                <span class="muted" id="tofa_expiry"></span>
              </div>
            </div>
            <div id="tofa_profile_state" class="tofa-profile-row hidden">
              <div>
                <label for="tofa_profile_select">Tofa profile</label>
                <select id="tofa_profile_select" autocomplete="off"></select>
              </div>
              <div id="tofa_pin_wrap" class="tofa-pin hidden">
                <label for="tofa_profile_pin">Profile PIN</label>
                <input id="tofa_profile_pin" type="password" inputmode="numeric" autocomplete="off">
              </div>
            </div>
          </div>
          <div class="cw-subpanel" data-sub="whitelist">
            <div style="width:100%;max-width:none">
              <div id="tofa_lib_matrix"></div>
            </div>
          </div>
          <div class="cw-subpanel" data-sub="users">
            <div id="tofa_sync_users"></div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
"""


PROVIDER = TofaAuth()
__all__ = [
    "PROVIDER",
    "TofaAuth",
    "TofaClient",
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
    "sync_user_entry",
    "token_instance",
]
