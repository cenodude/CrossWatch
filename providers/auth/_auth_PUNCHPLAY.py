# providers/auth/_auth_PUNCHPLAY.py
# CrossWatch - PunchPlay Authentication Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import secrets
import threading
import time
from collections.abc import Mapping, MutableMapping
from typing import Any

import requests

from ._auth_base import AuthManifest, AuthStatus
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, get_provider_block, normalize_instance_id

try:
    from _logging import log as _real_log
except ImportError:
    _real_log = None


def log(msg: str, level: str = "INFO", module: str = "AUTH", **_: Any) -> None:
    try:
        if _real_log is not None:
            _real_log(msg, level=level, module=module, **_)
        else:
            print(f"[{module}] {level}: {msg}")
    except Exception:
        pass


API_BASE = "https://punchplay.tv"
PLATFORM_BASE = f"{API_BASE}/api/platform/v1"
DEVICE_CODE_URL = f"{PLATFORM_BASE}/auth/device/code"
DEVICE_TOKEN_URL = f"{PLATFORM_BASE}/auth/device/token"
REFRESH_URL = f"{PLATFORM_BASE}/auth/refresh"
REVOKE_URL = f"{PLATFORM_BASE}/oauth/revoke"
ME_URL = f"{PLATFORM_BASE}/me"
VERIFY_URL = f"{API_BASE}/link"

DEFAULT_CLIENT_ID = "ppc_5234760ad6e8ee951a753073"
CLIENT_ID_ENV = "CROSSWATCH_PUNCHPLAY_CLIENT_ID"

DEFAULT_SCOPES: tuple[str, ...] = (
    "profile:read",
    "history:read",
    "history:write",
    "lists:read",
    "lists:write",
    "ratings:read",
    "ratings:write",
    "collection:read",
    "collection:write",
    "playback:read",
    "playback:write",
    "events:read",
)
SCOPE = " ".join(DEFAULT_SCOPES)

DEVICE_NAME = "CrossWatch"
POLL_INTERVAL_SEC = 5
REFRESH_SKEW_SEC = 300
HTTP_TIMEOUT = 20
UA = "CrossWatch/PunchPlayAuth"
__VERSION__ = "0.1"

_TOKEN_KEYS = (
    "access_token",
    "refresh_token",
    "token_type",
    "scope",
    "expires_at",
    "refresh_expires_at",
    "username",
    "user_id",
)

_REFRESH_LOCKS: dict[str, threading.Lock] = {}
_REFRESH_LOCKS_GUARD = threading.Lock()

DEVICE_CODE_BUDGET = (10, 3600.0)
DEVICE_TOKEN_BUDGET = (200, 600.0)
REFRESH_BUDGET = (20, 3600.0)
FORCED_REFRESH_MIN_INTERVAL = 60.0

_AUTH_BUDGETS: dict[str, tuple[int, float]] = {
    "device_code": DEVICE_CODE_BUDGET,
    "device_token": DEVICE_TOKEN_BUDGET,
    "refresh": REFRESH_BUDGET,
}


class _IPRateGuard:
    def __init__(self, budgets: dict[str, tuple[int, float]] | None = None) -> None:
        self._lock = threading.Lock()
        self._budgets = dict(budgets or _AUTH_BUDGETS)
        self._hits: dict[str, list[float]] = {}
        self._blocked: dict[str, float] = {}

    def _prune(self, name: str, now: float) -> list[float]:
        limit_window = self._budgets.get(name)
        hits = self._hits.setdefault(name, [])
        if limit_window:
            cutoff = now - limit_window[1]
            while hits and hits[0] <= cutoff:
                hits.pop(0)
        return hits

    def retry_after(self, name: str) -> int:
        with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._blocked.get(name, 0.0) - now)
            budget = self._budgets.get(name)
            if budget:
                hits = self._prune(name, now)
                if len(hits) >= budget[0]:
                    wait = max(wait, hits[0] + budget[1] - now)
            return int(wait) + 1 if wait > 0 else 0

    def reserve(self, name: str) -> int:
        with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._blocked.get(name, 0.0) - now)
            budget = self._budgets.get(name)
            hits = self._prune(name, now) if budget else []
            if budget and len(hits) >= budget[0]:
                wait = max(wait, hits[0] + budget[1] - now)
            if wait > 0:
                return int(wait) + 1
            if budget:
                hits.append(now)
            return 0

    def note_429(self, name: str, retry_after: float) -> None:
        delay = max(1.0, float(retry_after or 0.0))
        with self._lock:
            self._blocked[name] = max(self._blocked.get(name, 0.0), time.monotonic() + delay)

    def reset(self) -> None:
        with self._lock:
            self._hits.clear()
            self._blocked.clear()


_AUTH_GUARD = _IPRateGuard()
_LAST_FORCED_REFRESH: dict[str, float] = {}
_FORCED_REFRESH_GUARD = threading.Lock()


class PunchPlayAuthError(RuntimeError):
    pass


def now() -> int:
    return int(time.time())


def app_client_id() -> str:
    return str(os.environ.get(CLIENT_ID_ENV) or DEFAULT_CLIENT_ID).strip()


def _refresh_lock(instance_id: Any) -> threading.Lock:
    inst = normalize_instance_id(instance_id)
    with _REFRESH_LOCKS_GUARD:
        lock = _REFRESH_LOCKS.get(inst)
        if lock is None:
            lock = threading.Lock()
            _REFRESH_LOCKS[inst] = lock
        return lock


def _load_full_cfg() -> dict[str, Any]:
    try:
        cfg = load_config() or {}
        return cfg if isinstance(cfg, dict) else dict(cfg)
    except Exception:
        return {}


def _save_full_cfg(cfg: dict[str, Any]) -> None:
    save_config(cfg)


def _headers(token: str | None = None) -> dict[str, str]:
    h = {"Accept": "application/json", "Content-Type": "application/json", "User-Agent": UA}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _error_of(r: requests.Response) -> str:
    try:
        body = r.json() or {}
    except Exception:
        return ""
    if not isinstance(body, Mapping):
        return ""
    return str(body.get("error") or "").strip()


def _retry_after(r: requests.Response) -> int:
    try:
        return max(0, int(float(r.headers.get("Retry-After") or 0)))
    except Exception:
        return 0


def provider_block(cfg: Mapping[str, Any] | None, instance_id: Any = None) -> dict[str, Any]:
    out = get_provider_block(cfg or {}, "punchplay", instance_id)
    if out:
        return out
    base = (cfg or {}).get("punchplay") if isinstance(cfg, Mapping) else None
    return dict(base or {}) if isinstance(base, Mapping) else {}


def writable_block(cfg: dict[str, Any], instance_id: Any = None) -> dict[str, Any]:
    return ensure_instance_block(cfg, "punchplay", instance_id)


def ensure_device_id(block: MutableMapping[str, Any]) -> str:
    did = str(block.get("device_id") or "").strip()
    if not did:
        did = f"crosswatch-{secrets.token_hex(6)}"
        block["device_id"] = did
    return did


def normalize_auth_method(value: Any = None, block: Mapping[str, Any] | None = None) -> str:
    return "device_code"


def active_method(block: Mapping[str, Any] | None = None) -> str:
    return "device_code"


def set_active_method(block: MutableMapping[str, Any], method: str = "device_code") -> str:
    block["auth_method"] = "device_code"
    return "device_code"


def clear_oauth(block: MutableMapping[str, Any]) -> None:
    for key in _TOKEN_KEYS:
        if key in block:
            block[key] = 0 if key in {"expires_at", "refresh_expires_at"} else ""
    block.pop("_pending_device", None)


def is_configured(block: Mapping[str, Any] | None) -> bool:
    return bool(str((block or {}).get("access_token") or "").strip())


def about_to_expire(block: Mapping[str, Any], skew_sec: int = REFRESH_SKEW_SEC) -> bool:
    try:
        exp = int(block.get("expires_at") or 0)
    except Exception:
        exp = 0
    return bool(exp and exp - now() <= max(0, int(skew_sec)))


def scopes_of(block: Mapping[str, Any] | None) -> list[str]:
    raw = str((block or {}).get("scope") or "").strip()
    return [s for s in raw.split(" ") if s]


def status_for_block(block: Mapping[str, Any] | None) -> dict[str, Any]:
    b = block or {}
    out: dict[str, Any] = {
        "auth_method": "device_code",
        "connected": is_configured(b),
        "client_id_configured": bool(app_client_id()),
        "expires_at": int(b.get("expires_at") or 0),
        "refresh_expires_at": int(b.get("refresh_expires_at") or 0),
        "username": str(b.get("username") or ""),
        "user_id": str(b.get("user_id") or ""),
        "scopes": scopes_of(b),
    }
    pend = b.get("_pending_device")
    if isinstance(pend, Mapping) and str(pend.get("user_code") or "").strip():
        out["pending"] = {
            "user_code": str(pend.get("user_code") or ""),
            "verification_uri": str(pend.get("verification_uri") or VERIFY_URL),
            "verification_uri_complete": str(pend.get("verification_uri_complete") or ""),
            "expires_at": int(pend.get("expires_at") or 0),
            "interval": int(pend.get("interval") or POLL_INTERVAL_SEC),
        }
    return out


def _apply_token_response(block: MutableMapping[str, Any], tok: Mapping[str, Any], *, fallback_refresh: str = "") -> None:
    try:
        expires_in = int(tok.get("expires_in") or 0)
    except Exception:
        expires_in = 0
    try:
        refresh_expires_in = int(tok.get("refresh_expires_in") or 0)
    except Exception:
        refresh_expires_in = 0

    block["access_token"] = str(tok.get("access_token") or "").strip()
    block["refresh_token"] = str(tok.get("refresh_token") or fallback_refresh or "").strip()
    block["token_type"] = str(tok.get("token_type") or "bearer").strip() or "bearer"
    block["scope"] = str(tok.get("scope") or block.get("scope") or SCOPE).strip() or SCOPE
    block["expires_at"] = now() + expires_in if expires_in > 0 else 0
    block["refresh_expires_at"] = now() + refresh_expires_in if refresh_expires_in > 0 else 0
    block["auth_method"] = "device_code"


def fetch_identity(access_token: str, *, timeout: float = HTTP_TIMEOUT) -> dict[str, Any]:
    if not str(access_token or "").strip():
        return {}
    try:
        r = requests.get(ME_URL, headers=_headers(access_token), timeout=timeout)
    except requests.RequestException:
        return {}
    if r.status_code >= 400:
        return {}
    try:
        data = r.json() or {}
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def _store_identity(block: MutableMapping[str, Any], access_token: str) -> None:
    me = fetch_identity(access_token)
    if not me:
        return
    username = str(me.get("username") or "").strip()
    if not username:
        profile = me.get("profile")
        if isinstance(profile, Mapping):
            username = str(profile.get("displayName") or "").strip()
    if not username:
        username = str(me.get("name") or "").strip()
    if username:
        block["username"] = username
    user_id = str(me.get("id") or "").strip()
    if user_id:
        block["user_id"] = user_id
    granted = me.get("scopes")
    if isinstance(granted, (list, tuple)) and granted:
        block["scope"] = " ".join(str(s) for s in granted if str(s or "").strip())


def start_device_code(
    cfg: dict[str, Any] | None = None,
    *,
    instance_id: Any = None,
    scope: str | None = None,
    timeout: float = HTTP_TIMEOUT,
) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_full_cfg()
    inst = normalize_instance_id(instance_id)
    block = writable_block(cfgd, inst)
    cid = app_client_id()
    if not cid:
        return {"ok": False, "error": "missing_client_id", "instance": inst}

    ensure_device_id(block)
    set_active_method(block)

    wait = _AUTH_GUARD.reserve("device_code")
    if wait:
        log(f"PUNCHPLAY: device code throttled locally (instance={inst})", level="WARN", module="AUTH")
        return {"ok": False, "error": "rate_limited", "retry_after": wait, "local": True, "instance": inst}

    try:
        r = requests.post(
            DEVICE_CODE_URL,
            json={"client_id": cid, "scope": str(scope or SCOPE).strip() or SCOPE},
            headers=_headers(),
            timeout=timeout,
        )
    except requests.RequestException as e:
        return {"ok": False, "error": "network_error", "detail": str(e), "instance": inst}

    if r.status_code == 429:
        retry = _retry_after(r)
        _AUTH_GUARD.note_429("device_code", retry or 60)
        return {"ok": False, "error": "rate_limited", "retry_after": retry, "instance": inst}
    if r.status_code >= 400:
        return {"ok": False, "error": _error_of(r) or "http_error", "status": int(r.status_code), "instance": inst}

    try:
        data: dict[str, Any] = r.json() or {}
    except ValueError:
        return {"ok": False, "error": "invalid_json", "instance": inst}

    device_code = str(data.get("device_code") or "").strip()
    user_code = str(data.get("user_code") or "").strip()
    if not device_code or not user_code:
        return {"ok": False, "error": "invalid_response", "instance": inst}

    verification_uri = str(data.get("verification_uri") or VERIFY_URL).strip() or VERIFY_URL
    verification_uri_complete = str(data.get("verification_uri_complete") or "").strip()
    try:
        expires_in = int(data.get("expires_in") or 600)
    except Exception:
        expires_in = 600
    expires_at = now() + expires_in
    granted_scope = str(data.get("scope") or scope or SCOPE).strip() or SCOPE

    block["_pending_device"] = {
        "device_code": device_code,
        "user_code": user_code,
        "verification_uri": verification_uri,
        "verification_uri_complete": verification_uri_complete,
        "interval": POLL_INTERVAL_SEC,
        "expires_at": expires_at,
        "created_at": now(),
        "scope": granted_scope,
    }
    _save_full_cfg(cfgd)

    log(f"PUNCHPLAY: device code issued (instance={inst})", level="INFO", module="AUTH")
    return {
        "ok": True,
        "instance": inst,
        "device_code": device_code,
        "user_code": user_code,
        "verification_uri": verification_uri,
        "verification_uri_complete": verification_uri_complete,
        "verification_uri_qr": str(data.get("verification_uri_qr") or ""),
        "interval": POLL_INTERVAL_SEC,
        "expires_in": expires_in,
        "expires_at": expires_at,
        "scope": granted_scope,
    }


def poll_device_code(
    cfg: dict[str, Any] | None = None,
    *,
    instance_id: Any = None,
    device_code: str | None = None,
    timeout: float = HTTP_TIMEOUT,
) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_full_cfg()
    inst = normalize_instance_id(instance_id)
    block = writable_block(cfgd, inst)
    cid = app_client_id()
    pend = block.get("_pending_device") if isinstance(block.get("_pending_device"), Mapping) else {}
    dc = str(device_code or (pend or {}).get("device_code") or "").strip()

    if not cid:
        return {"ok": False, "status": "missing_client_id", "instance": inst}
    if not dc:
        return {"ok": False, "status": "no_device_code", "instance": inst}
    pend_expires = int((pend or {}).get("expires_at") or 0)
    if pend_expires and now() >= pend_expires:
        block.pop("_pending_device", None)
        _save_full_cfg(cfgd)
        return {"ok": False, "status": "expired", "instance": inst}

    payload = {
        "client_id": cid,
        "device_code": dc,
        "device_name": DEVICE_NAME,
        "device_id": ensure_device_id(block),
    }

    wait = _AUTH_GUARD.reserve("device_token")
    if wait:
        return {"ok": False, "status": "slow_down", "retry_after": wait, "local": True, "instance": inst}

    try:
        r = requests.post(DEVICE_TOKEN_URL, json=payload, headers=_headers(), timeout=timeout)
    except requests.RequestException as e:
        return {"ok": False, "status": "network_error", "error": str(e), "instance": inst}

    if r.status_code == 429:
        retry = _retry_after(r)
        _AUTH_GUARD.note_429("device_token", retry or 10)
        return {"ok": False, "status": "slow_down", "retry_after": retry, "instance": inst}
    if r.status_code >= 500:
        return {"ok": False, "status": "server_error", "instance": inst}
    if r.status_code >= 400:
        err = _error_of(r) or "authorization_pending"
        if err == "expired":
            block.pop("_pending_device", None)
            _save_full_cfg(cfgd)
        return {"ok": False, "status": err, "instance": inst}

    try:
        tok: dict[str, Any] = r.json() or {}
    except ValueError:
        return {"ok": False, "status": "bad_json", "instance": inst}

    access_token = str(tok.get("access_token") or "").strip()
    if not access_token:
        return {"ok": False, "status": "no_access_token", "instance": inst}

    _apply_token_response(block, tok)
    username = str(tok.get("username") or "").strip()
    if username:
        block["username"] = username
    block.pop("_pending_device", None)
    _store_identity(block, access_token)
    _save_full_cfg(cfgd)

    log(f"PUNCHPLAY: tokens stored (instance={inst})", level="SUCCESS", module="AUTH")
    return {
        "ok": True,
        "status": "authorized",
        "instance": inst,
        "expires_at": int(block.get("expires_at") or 0),
        "username": str(block.get("username") or ""),
    }


def cancel_device_code(cfg: dict[str, Any] | None = None, *, instance_id: Any = None) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_full_cfg()
    inst = normalize_instance_id(instance_id)
    block = writable_block(cfgd, inst)
    existed = block.pop("_pending_device", None) is not None
    if existed:
        _save_full_cfg(cfgd)
    return {"ok": True, "cancelled": existed, "instance": inst}


def _copy_token_fields(dst: MutableMapping[str, Any], src: Mapping[str, Any]) -> None:
    for key in _TOKEN_KEYS:
        if key in src:
            dst[key] = src.get(key)


def refresh_token(
    cfg: dict[str, Any] | None = None,
    *,
    instance_id: Any = None,
    update_cfg: dict[str, Any] | None = None,
    force: bool = False,
    timeout: float = HTTP_TIMEOUT,
) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    with _refresh_lock(inst):
        full = _load_full_cfg()
        block = writable_block(full, inst)

        if not force and str(block.get("access_token") or "").strip() and not about_to_expire(block):
            return {"ok": True, "status": "fresh", "instance": inst, "expires_at": int(block.get("expires_at") or 0)}

        cid = app_client_id()
        rt = str(block.get("refresh_token") or "").strip()
        if not rt and isinstance(cfg, Mapping):
            rt = str(provider_block(cfg, inst).get("refresh_token") or "").strip()
        if not cid or not rt:
            return {"ok": False, "status": "missing_refresh", "instance": inst}

        wait = _AUTH_GUARD.reserve("refresh")
        if wait:
            log(f"PUNCHPLAY: refresh throttled locally (instance={inst})", level="WARN", module="AUTH")
            return {"ok": False, "status": "rate_limited", "retry_after": wait, "local": True, "instance": inst}

        try:
            r = requests.post(
                REFRESH_URL,
                json={"client_id": cid, "refresh_token": rt},
                headers=_headers(),
                timeout=timeout,
            )
        except requests.RequestException as e:
            return {"ok": False, "status": "network_error", "error": str(e), "instance": inst}

        if r.status_code == 429:
            retry = _retry_after(r)
            _AUTH_GUARD.note_429("refresh", retry or 300)
            log(f"PUNCHPLAY: refresh rate limited (instance={inst})", level="WARN", module="AUTH")
            return {"ok": False, "status": "rate_limited", "retry_after": retry, "instance": inst}

        if r.status_code >= 400:
            err = _error_of(r)
            if err in {"invalid_grant", "invalid_client", "unauthorized_client"}:
                clear_oauth(block)
                _save_full_cfg(full)
                log(f"PUNCHPLAY: refresh rejected ({err}); reconnect required (instance={inst})", level="ERROR", module="AUTH")
                return {"ok": False, "status": err, "instance": inst, "reconnect_required": True}
            return {"ok": False, "status": f"refresh_failed:{r.status_code}", "error": err, "instance": inst}

        try:
            tok: dict[str, Any] = r.json() or {}
        except ValueError:
            return {"ok": False, "status": "bad_json", "instance": inst}

        if not str(tok.get("access_token") or "").strip():
            return {"ok": False, "status": "no_access_token", "instance": inst}

        _apply_token_response(block, tok, fallback_refresh=rt)
        _save_full_cfg(full)

        for target in (cfg, update_cfg):
            if isinstance(target, dict):
                try:
                    _copy_token_fields(writable_block(target, inst), block)
                except Exception:
                    pass

        log(f"PUNCHPLAY: token refreshed (instance={inst})", level="INFO", module="AUTH")
        return {"ok": True, "status": "ok", "instance": inst, "expires_at": int(block.get("expires_at") or 0)}


def revoke_token(
    cfg: Mapping[str, Any] | None = None,
    *,
    instance_id: Any = None,
    token: str | None = None,
    token_type_hint: str = "refresh_token",
    timeout: float = HTTP_TIMEOUT,
) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    block = provider_block(cfg if cfg is not None else _load_full_cfg(), inst)
    cid = app_client_id()
    tok = str(token or block.get("refresh_token") or block.get("access_token") or "").strip()
    if not cid or not tok:
        return {"ok": False, "status": "missing_token", "instance": inst}

    try:
        r = requests.post(
            REVOKE_URL,
            json={"client_id": cid, "token": tok, "token_type_hint": token_type_hint},
            headers=_headers(),
            timeout=timeout,
        )
    except requests.RequestException as e:
        return {"ok": False, "status": "network_error", "error": str(e), "instance": inst}

    if r.status_code >= 400:
        return {"ok": False, "status": f"revoke_failed:{r.status_code}", "error": _error_of(r), "instance": inst}
    return {"ok": True, "status": "ok", "instance": inst}


def _allow_forced_refresh(instance_id: Any) -> bool:
    inst = normalize_instance_id(instance_id)
    now = time.monotonic()
    with _FORCED_REFRESH_GUARD:
        last = _LAST_FORCED_REFRESH.get(inst, 0.0)
        if now - last < FORCED_REFRESH_MIN_INTERVAL:
            return False
        _LAST_FORCED_REFRESH[inst] = now
        return True


def prepare_auth(
    cfg: Mapping[str, Any] | None,
    *,
    instance_id: Any = None,
    refresh: bool = True,
) -> dict[str, str]:
    inst = normalize_instance_id(instance_id)
    block = provider_block(cfg, inst)

    if refresh and (not str(block.get("access_token") or "").strip() or about_to_expire(block)):
        res = refresh_token(dict(cfg or {}), instance_id=inst)
        if not res.get("ok"):
            raise PunchPlayAuthError(str(res.get("status") or "refresh_failed"))
        block = provider_block(_load_full_cfg(), inst)

    token = str(block.get("access_token") or "").strip()
    if not token:
        raise PunchPlayAuthError("missing_access_token")
    return {"Authorization": f"Bearer {token}"}


def merge_auth_kwargs(
    cfg: Mapping[str, Any] | None,
    *,
    instance_id: Any = None,
    kwargs: dict[str, Any] | None = None,
    refresh: bool = True,
) -> dict[str, Any]:
    out = dict(kwargs or {})
    headers = dict(out.get("headers") or {})
    headers.setdefault("Accept", "application/json")
    headers.setdefault("User-Agent", UA)
    headers.update(prepare_auth(cfg, instance_id=instance_id, refresh=refresh))
    out["headers"] = headers
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
    **kwargs: Any,
) -> requests.Response:
    from providers.sync._mod_common import request_with_retries

    call = request_func or request_with_retries
    req_kwargs = merge_auth_kwargs(cfg, instance_id=instance_id, kwargs=kwargs)
    resp = call(session, method, url, timeout=timeout, max_retries=max_retries, **req_kwargs)
    if getattr(resp, "status_code", None) != 401:
        return resp

    if not _allow_forced_refresh(instance_id):
        return resp

    res = refresh_token(dict(cfg or {}), instance_id=instance_id, force=True)
    if not res.get("ok"):
        return resp
    req_kwargs = merge_auth_kwargs(_load_full_cfg(), instance_id=instance_id, kwargs=kwargs, refresh=False)
    return call(session, method, url, timeout=timeout, max_retries=max_retries, **req_kwargs)


class PunchPlayAuth:
    name = "PUNCHPLAY"
    label = "PunchPlay"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name=self.name,
            label=self.label,
            flow="device_code",
            fields=[],
            actions={"start": True, "finish": True, "refresh": True, "disconnect": True},
            verify_url=VERIFY_URL,
            notes="Device code only. CrossWatch ships a public PunchPlay app id, so no keys are required.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {
            "watchlist": True,
            "ratings": True,
            "history": True,
            "playlists": True,
            "playback": True,
            "device_code": True,
            "refresh": True,
            "revoke": True,
        }

    def get_status(self, cfg: Mapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfgd = cfg if cfg is not None else _load_full_cfg()
        inst = normalize_instance_id(instance_id)
        block = provider_block(cfgd, inst)
        label = "PunchPlay" if inst == "default" else f"PunchPlay ({inst})"
        return AuthStatus(
            connected=is_configured(block),
            label=label,
            user=str(block.get("username") or "") or None,
            expires_at=int(block.get("expires_at") or 0) or None,
            scopes=scopes_of(block) or None,
        )

    def start(
        self,
        cfg: MutableMapping[str, Any] | None = None,
        redirect_uri: str | None = None,
        *,
        instance_id: Any = None,
    ) -> dict[str, Any]:
        cfgd = dict(cfg or _load_full_cfg())
        return start_device_code(cfgd, instance_id=instance_id)

    def finish(
        self,
        cfg: MutableMapping[str, Any] | None = None,
        *,
        instance_id: Any = None,
        **payload: Any,
    ) -> AuthStatus:
        cfgd = dict(cfg or _load_full_cfg())
        poll_device_code(cfgd, instance_id=instance_id, device_code=str(payload.get("device_code") or "").strip() or None)
        return self.get_status(_load_full_cfg(), instance_id=instance_id)

    def refresh(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        try:
            refresh_token(dict(cfg or _load_full_cfg()), instance_id=instance_id)
        except Exception:
            pass
        return self.get_status(_load_full_cfg(), instance_id=instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfgd = dict(cfg or _load_full_cfg())
        inst = normalize_instance_id(instance_id)
        try:
            revoke_token(cfgd, instance_id=inst)
        except Exception:
            pass
        block = writable_block(cfgd, inst)
        clear_oauth(block)
        _save_full_cfg(cfgd)
        log(f"PUNCHPLAY: disconnected (instance={inst})", level="INFO", module="AUTH")
        return self.get_status(cfgd, instance_id=inst)

    def html(self, cfg: Mapping[str, Any] | None = None) -> str:
        return html()


PROVIDER = PunchPlayAuth()
__all__ = ["PROVIDER", "PunchPlayAuth", "PunchPlayAuthError", "html", "__VERSION__"]


def html() -> str:
    return r'''<div class="section" id="sec-punchplay">
  <style>
    #sec-punchplay .inline{display:flex;gap:8px;align-items:center}
    #sec-punchplay .sub{opacity:.7;font-size:.92em}
    #sec-punchplay .hidden{display:none !important}
    #sec-punchplay .pp-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:14px}

    #sec-punchplay #punchplay_device_start{
      background: linear-gradient(135deg,#f4400f,#0084d8);
      border-color: rgba(244,64,15,.45);
      box-shadow: 0 0 14px rgba(244,64,15,.35);
      color: #fff;
    }
    #sec-punchplay #punchplay_device_start:hover{
      filter: brightness(1.06);
      box-shadow: 0 0 18px rgba(244,64,15,.5);
    }

    #sec-punchplay .pp-qc{margin-top:12px;padding:14px;border-radius:12px;border:1px solid rgba(244,64,15,.35);background:rgba(244,64,15,.06)}
    #sec-punchplay .pp-qc-codewrap{display:flex;align-items:center;justify-content:center;gap:12px}
    #sec-punchplay .pp-qc-code{
      font-size:2em;font-weight:700;letter-spacing:.18em;padding:6px 0 6px .18em;color:#ff9d6a;
      text-align:center;text-transform:uppercase;font-variant-numeric:tabular-nums;
    }
    #sec-punchplay .pp-qc-copy{
      appearance:none;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;
      width:34px;height:34px;border-radius:9px;flex:0 0 auto;
      border:1px solid rgba(244,64,15,.35);background:rgba(244,64,15,.08);color:#ff9d6a;
      transition:background .15s ease, border-color .15s ease, color .15s ease, transform .12s ease;
    }
    #sec-punchplay .pp-qc-copy:hover{background:rgba(244,64,15,.16);border-color:rgba(244,64,15,.6)}
    #sec-punchplay .pp-qc-copy:active{transform:scale(.94)}
    #sec-punchplay .pp-qc-copy.copied{background:rgba(244,64,15,.24);border-color:rgba(244,64,15,.75)}
    #sec-punchplay .pp-qc-copy svg{width:16px;height:16px;display:block}
    #sec-punchplay .pp-qc-meta{display:flex;justify-content:space-between;gap:12px;margin-top:6px}
    #sec-punchplay .pp-qc-qr{display:flex;justify-content:center;margin-top:12px}
    #sec-punchplay .pp-qc-qr img{width:148px;height:148px;border-radius:10px;background:#fff;padding:6px}
  </style>

  <div class="head" data-toggle-section="sec-punchplay">
    <span class="chev"></span><strong>PunchPlay</strong>
  </div>

  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="punchplay">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">PunchPlay</div>
            <div class="muted">Connect your PunchPlay account with a device code.</div>
          </div>
        </div>

        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
        </div>

        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="cw-auth-journey" style="--cw-auth-c1:244,64,15;--cw-auth-c2:0,132,216;--cw-auth-logo:url('/assets/img/PUNCHPLAY.png')">
              <div class="cw-auth-journey-text">
                <div class="cw-auth-journey-title">Connect to PunchPlay</div>
                <div class="cw-auth-journey-copy">Click Connect PunchPlay and approve the code at punchplay.tv/link. No API keys are needed &mdash; CrossWatch ships its own PunchPlay app id.</div>
              </div>
            </div>

            <div id="punchplay_device_panel">
              <input id="punchplay_device_code" type="hidden">
              <div id="punchplay_qc_state" class="pp-qc hidden">
                <div class="pp-qc-codewrap">
                  <div class="pp-qc-code" id="punchplay_qc_code">----&ndash;----</div>
                  <button type="button" id="punchplay_qc_copy" class="pp-qc-copy" title="Copy code" aria-label="Copy code">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                  </button>
                </div>
                <div class="sub" id="punchplay_qc_help">Opening punchplay.tv/link &mdash; enter this code there and approve CrossWatch.</div>
                <div class="pp-qc-qr hidden" id="punchplay_qc_qrwrap"><img id="punchplay_qc_qr" alt="Approval QR code"></div>
                <div class="pp-qc-meta">
                  <span class="sub" id="punchplay_qc_status">Waiting for approval&hellip;</span>
                  <span class="sub" id="punchplay_qc_timer"></span>
                </div>
              </div>
            </div>

            <div class="pp-actions">
              <button id="punchplay_device_start" class="btn" type="button">Connect PunchPlay</button>
              <button id="punchplay_device_cancel" class="btn danger hidden" type="button">Cancel</button>
              <button id="punchplay_device_restart" class="btn hidden" type="button">Restart</button>
              <button id="punchplay_disconnect" class="btn danger" type="button">Delete</button>
              <div id="punchplay_msg" class="msg ok hidden" role="status" aria-live="polite"></div>
            </div>
          </div>
        </div>

      </div>
    </div>
  </div>
</div>
'''
