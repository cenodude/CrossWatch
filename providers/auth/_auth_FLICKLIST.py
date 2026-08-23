# providers/auth/_auth_FLICKLIST.py
# CrossWatch - FlickList Authentication Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import threading
import time
from collections.abc import Mapping, MutableMapping
from datetime import datetime, timezone
from typing import Any

import requests

from ._auth_base import AuthManifest, AuthStatus
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, normalize_instance_id, resolve_provider_block

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


API_BASE = "https://flicklist.tv"
API_AUTH_BASE = f"{API_BASE}/api/auth"
API_V3_BASE = f"{API_BASE}/api/v3"
DEVICE_CODE_URL = f"{API_AUTH_BASE}/device/code"
DEVICE_TOKEN_URL = f"{API_AUTH_BASE}/device/token"
REFRESH_URL = f"{API_AUTH_BASE}/refresh"
ME_URL = f"{API_V3_BASE}/me"
VERIFY_URL = f"{API_BASE}/link"

CLIENT_ID_ENV = "CROSSWATCH_FLICKLIST_CLIENT_ID"
DEFAULT_CLIENT_ID = "crosswatch_923f4294"
POLL_INTERVAL_SEC = 5
REFRESH_AFTER_SEC = 7 * 86400
REFRESH_SKEW_SEC = 86400
HTTP_TIMEOUT = 20.0
UA = "CrossWatch/FlickListAuth"
__VERSION__ = "0.1"

_TOKEN_KEYS = (
    "api_key",
    "access_token",
    "token",
    "credential",
    "auth_method",
    "expires_at",
    "stored_at",
    "username",
    "user_id",
    "display_name",
    "avatar_url",
)

_REFRESH_LOCKS: dict[str, threading.Lock] = {}
_REFRESH_LOCKS_GUARD = threading.Lock()


class FlickListAuthError(RuntimeError):
    pass


def now() -> int:
    return int(time.time())


def app_client_id(block: Mapping[str, Any] | None = None) -> str:
    configured = str((block or {}).get("client_id") or "").strip()
    return str(os.getenv(CLIENT_ID_ENV) or configured or DEFAULT_CLIENT_ID).strip()


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


def provider_block(cfg: Mapping[str, Any] | None, instance_id: Any = None) -> dict[str, Any]:
    try:
        return dict(resolve_provider_block(cfg or {}, "flicklist", instance_id) or {})
    except Exception:
        base = (cfg or {}).get("flicklist") if isinstance(cfg, Mapping) else None
        return dict(base or {}) if isinstance(base, Mapping) else {}


def writable_block(cfg: dict[str, Any], instance_id: Any = None) -> dict[str, Any]:
    return ensure_instance_block(cfg, "flicklist", instance_id)


def normalize_auth_method(value: Any = None, block: Mapping[str, Any] | None = None) -> str:
    text = str(value or (block or {}).get("auth_method") or "").strip().lower()
    if text in {"device", "device_code", "session"}:
        return "device_code"
    if text in {"api_key", "key"}:
        return "api_key"
    if str((block or {}).get("api_key") or "").strip():
        return "api_key"
    return "device_code"


def active_method(block: Mapping[str, Any] | None = None) -> str:
    return normalize_auth_method(None, block)


def set_active_method(block: MutableMapping[str, Any], method: str = "device_code") -> str:
    value = normalize_auth_method(method, block)
    block["auth_method"] = value
    return value


def clear_oauth(block: MutableMapping[str, Any]) -> None:
    for key in _TOKEN_KEYS:
        if key in block:
            block[key] = 0 if key in {"expires_at", "stored_at"} else ""
    block.pop("_pending_device", None)


def access_token_of(block: Mapping[str, Any] | None) -> str:
    b = block or {}
    return str(b.get("api_key") or b.get("access_token") or b.get("token") or "").strip()


def is_configured(block: Mapping[str, Any] | None) -> bool:
    return bool(access_token_of(block))


def is_api_key(block: Mapping[str, Any] | None) -> bool:
    token = access_token_of(block)
    return active_method(block) == "api_key" or token.startswith("fs_live_") or str((block or {}).get("credential") or "") == "key"


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
    return str(body.get("error") or body.get("message") or body.get("detail") or "").strip()


def _epoch(value: Any) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    try:
        dt = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return 0


def _store_identity(block: MutableMapping[str, Any], data: Mapping[str, Any] | None) -> None:
    user = data if isinstance(data, Mapping) else {}
    nested = user.get("user") if isinstance(user.get("user"), Mapping) else None
    if nested:
        user = nested
    username = str(user.get("username") or "").strip()
    if username:
        block["username"] = username
    user_id = str(user.get("id") or "").strip()
    if user_id:
        block["user_id"] = user_id
    display = str(user.get("display_name") or "").strip()
    if display:
        block["display_name"] = display
    avatar = str(user.get("avatar_url") or "").strip()
    if avatar:
        block["avatar_url"] = avatar


def fetch_identity(token: str, *, timeout: float = HTTP_TIMEOUT) -> dict[str, Any]:
    if not str(token or "").strip():
        return {}
    try:
        resp = requests.get(ME_URL, headers=_headers(token), timeout=timeout)
    except requests.RequestException:
        return {}
    if resp.status_code >= 400:
        return {}
    try:
        data = resp.json() or {}
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def about_to_expire(block: Mapping[str, Any] | None) -> bool:
    if not block or is_api_key(block):
        return False
    exp = int(block.get("expires_at") or 0)
    stored = int(block.get("stored_at") or 0)
    if exp and exp - now() <= REFRESH_SKEW_SEC:
        return True
    return bool(stored and now() - stored >= REFRESH_AFTER_SEC)


def status_for_block(block: Mapping[str, Any] | None) -> dict[str, Any]:
    b = block or {}
    out: dict[str, Any] = {
        "auth_method": active_method(b),
        "connected": is_configured(b),
        "client_id_configured": bool(app_client_id(b)),
        "api_key_configured": bool(str(b.get("api_key") or "").strip()),
        "expires_at": int(b.get("expires_at") or 0),
        "username": str(b.get("username") or ""),
        "user_id": str(b.get("user_id") or ""),
        "display_name": str(b.get("display_name") or ""),
    }
    pend = b.get("_pending_device")
    if isinstance(pend, Mapping) and str(pend.get("user_code") or "").strip():
        out["pending"] = {
            "user_code": str(pend.get("user_code") or ""),
            "verification_uri": str(pend.get("verification_uri") or VERIFY_URL),
            "verification_uri_complete": str(pend.get("verification_uri_complete") or ""),
            "expires_at": int(pend.get("expires_at") or 0),
            "interval": int(pend.get("interval") or POLL_INTERVAL_SEC),
            "credential": str(pend.get("credential") or "session"),
        }
    return out


def save_api_key(cfg: dict[str, Any] | None = None, *, instance_id: Any = None, api_key: Any = None) -> dict[str, Any]:
    key = str(api_key or "").strip()
    if not key:
        return {"ok": False, "error": "api_key_required"}
    cfgd = cfg if isinstance(cfg, dict) else _load_full_cfg()
    inst = normalize_instance_id(instance_id)
    block = writable_block(cfgd, inst)
    block["api_key"] = key
    block["access_token"] = ""
    block["token"] = ""
    block["credential"] = "key"
    block["auth_method"] = "api_key"
    block["expires_at"] = 0
    block["stored_at"] = now()
    me = fetch_identity(key)
    if not me:
        return {"ok": False, "error": "invalid_api_key", "instance": inst}
    _store_identity(block, me)
    _save_full_cfg(cfgd)
    log(f"FLICKLIST: API key saved (instance={inst})", level="SUCCESS", module="AUTH")
    return {"ok": True, "instance": inst, "username": str(block.get("username") or "")}


def start_device_code(
    cfg: dict[str, Any] | None = None,
    *,
    instance_id: Any = None,
    credential: str = "session",
    timeout: float = HTTP_TIMEOUT,
) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_full_cfg()
    inst = normalize_instance_id(instance_id)
    block = writable_block(cfgd, inst)
    cid = app_client_id(block)
    if not cid:
        return {"ok": False, "error": "missing_client_id", "instance": inst}
    cred = str(credential or "session").strip().lower()
    if cred not in {"key", "session"}:
        cred = "session"
    set_active_method(block, "device_code")
    try:
        resp = requests.post(DEVICE_CODE_URL, json={"client_id": cid, "credential": cred}, headers=_headers(), timeout=timeout)
    except requests.RequestException as exc:
        return {"ok": False, "error": "network_error", "detail": str(exc), "instance": inst}
    if resp.status_code >= 400:
        return {"ok": False, "error": _error_of(resp) or "http_error", "status": int(resp.status_code), "instance": inst}
    try:
        data: dict[str, Any] = resp.json() or {}
    except ValueError:
        return {"ok": False, "error": "invalid_json", "instance": inst}
    device_code = str(data.get("device_code") or "").strip()
    user_code = str(data.get("user_code") or "").strip()
    if not device_code or not user_code:
        return {"ok": False, "error": "invalid_response", "instance": inst}
    expires_in = int(data.get("expires_in") or 900)
    interval = int(data.get("interval") or POLL_INTERVAL_SEC)
    pending = {
        "device_code": device_code,
        "user_code": user_code,
        "verification_uri": str(data.get("verification_uri") or VERIFY_URL),
        "verification_uri_complete": str(data.get("verification_uri_complete") or ""),
        "interval": max(1, interval),
        "expires_at": now() + max(1, expires_in),
        "created_at": now(),
        "credential": cred,
    }
    block["_pending_device"] = pending
    block["client_id"] = cid
    _save_full_cfg(cfgd)
    return {"ok": True, "instance": inst, "expires_in": expires_in, **pending}


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
    pend = block.get("_pending_device") if isinstance(block.get("_pending_device"), Mapping) else {}
    dc = str(device_code or (pend or {}).get("device_code") or "").strip()
    if not dc:
        return {"ok": False, "status": "no_device_code", "instance": inst}
    if int((pend or {}).get("expires_at") or 0) and now() >= int((pend or {}).get("expires_at") or 0):
        block.pop("_pending_device", None)
        _save_full_cfg(cfgd)
        return {"ok": False, "status": "expired_token", "instance": inst}
    try:
        resp = requests.post(DEVICE_TOKEN_URL, json={"device_code": dc}, headers=_headers(), timeout=timeout)
    except requests.RequestException as exc:
        return {"ok": False, "status": "network_error", "error": str(exc), "instance": inst}
    if resp.status_code >= 500:
        return {"ok": False, "status": "server_error", "instance": inst}
    if resp.status_code >= 400:
        return {"ok": False, "status": _error_of(resp) or f"http:{resp.status_code}", "instance": inst}
    try:
        tok: dict[str, Any] = resp.json() or {}
    except ValueError:
        return {"ok": False, "status": "bad_json", "instance": inst}
    if tok.get("error"):
        status = str(tok.get("error") or "")
        if status in {"expired_token", "access_denied"}:
            block.pop("_pending_device", None)
            _save_full_cfg(cfgd)
        return {"ok": False, "status": status, "instance": inst}
    token = str(tok.get("access_token") or tok.get("token") or "").strip()
    if not token:
        return {"ok": False, "status": "no_access_token", "instance": inst}
    credential = str(tok.get("credential") or (pend or {}).get("credential") or "session").strip().lower()
    if credential == "key" or token.startswith("fs_live_"):
        block["api_key"] = token
        block["access_token"] = ""
        block["token"] = ""
        block["auth_method"] = "api_key"
    else:
        block["api_key"] = ""
        block["access_token"] = token
        block["token"] = token
        block["auth_method"] = "device_code"
    block["credential"] = credential
    block["expires_at"] = _epoch(tok.get("expires_at"))
    block["stored_at"] = now()
    block.pop("_pending_device", None)
    _store_identity(block, tok)
    _save_full_cfg(cfgd)
    return {"ok": True, "status": "authorized", "instance": inst, "credential": credential, "username": str(block.get("username") or "")}


def cancel_device_code(cfg: dict[str, Any] | None = None, *, instance_id: Any = None) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_full_cfg()
    inst = normalize_instance_id(instance_id)
    block = writable_block(cfgd, inst)
    existed = block.pop("_pending_device", None) is not None
    if existed:
        _save_full_cfg(cfgd)
    return {"ok": True, "cancelled": existed, "instance": inst}


def refresh_token(
    cfg: dict[str, Any] | None = None,
    *,
    instance_id: Any = None,
    force: bool = False,
    timeout: float = HTTP_TIMEOUT,
    **_: Any,
) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    with _refresh_lock(inst):
        full = _load_full_cfg()
        block = writable_block(full, inst)
        if is_api_key(block):
            return {"ok": True, "status": "api_key", "instance": inst}
        if not force and access_token_of(block) and not about_to_expire(block):
            return {"ok": True, "status": "fresh", "instance": inst, "expires_at": int(block.get("expires_at") or 0)}
        old = access_token_of(block)
        if not old:
            return {"ok": False, "status": "missing_token", "instance": inst}
        try:
            resp = requests.post(REFRESH_URL, headers=_headers(old), timeout=timeout)
        except requests.RequestException as exc:
            return {"ok": False, "status": "network_error", "error": str(exc), "instance": inst}
        if resp.status_code >= 400:
            if resp.status_code in {400, 401, 403}:
                clear_oauth(block)
                _save_full_cfg(full)
                return {"ok": False, "status": "invalid_grant", "instance": inst, "reconnect_required": True}
            return {"ok": False, "status": f"http:{resp.status_code}", "error": _error_of(resp), "instance": inst}
        try:
            tok: dict[str, Any] = resp.json() or {}
        except ValueError:
            return {"ok": False, "status": "bad_json", "instance": inst}
        token = str(tok.get("token") or tok.get("access_token") or "").strip()
        if not token:
            return {"ok": False, "status": "no_token", "instance": inst}
        block["access_token"] = token
        block["token"] = token
        block["credential"] = "session"
        block["auth_method"] = "device_code"
        block["expires_at"] = _epoch(tok.get("expires_at"))
        block["stored_at"] = now()
        _store_identity(block, tok)
        _save_full_cfg(full)
        return {"ok": True, "status": "ok", "instance": inst, "expires_at": int(block.get("expires_at") or 0)}


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

    inst = normalize_instance_id(instance_id)
    block = provider_block(cfg, inst)
    if about_to_expire(block):
        refresh_token(dict(cfg or {}), instance_id=inst)
        block = provider_block(_load_full_cfg(), inst)
    token = access_token_of(block)
    if not token:
        raise FlickListAuthError("missing_access_token")
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update(_headers(token))
    if not any(key in kwargs for key in ("json", "data", "files")):
        headers.pop("Content-Type", None)
    call = request_func or request_with_retries
    resp = call(session, method, url, headers=headers, timeout=timeout, max_retries=max_retries, **kwargs)
    if getattr(resp, "status_code", None) != 401 or is_api_key(block):
        return resp
    res = refresh_token(dict(cfg or {}), instance_id=inst, force=True)
    if not res.get("ok"):
        return resp
    token = access_token_of(provider_block(_load_full_cfg(), inst))
    if not token:
        return resp
    headers.update(_headers(token))
    return call(session, method, url, headers=headers, timeout=timeout, max_retries=max_retries, **kwargs)


class FlickListAuth:
    name = "FLICKLIST"
    label = "FlickList"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name=self.name,
            label=self.label,
            flow="device_code",
            fields=[
                {"key": "flicklist.client_id", "label": "Client ID override", "type": "text", "required": False, "secret": False},
                {"key": "flicklist.api_key", "label": "API Key", "type": "password", "required": False},
            ],
            actions={"start": True, "finish": True, "refresh": True, "disconnect": True},
            verify_url=VERIFY_URL,
            notes="Device Code uses CrossWatch's built-in public client_id. Never store the FlickList client_secret in CrossWatch.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"api_key": True, "device_code": True, "refresh": True, "watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": True, "scrobble": True}

    def get_status(self, cfg: Mapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfgd = cfg if cfg is not None else _load_full_cfg()
        inst = normalize_instance_id(instance_id)
        block = provider_block(cfgd, inst)
        return AuthStatus(
            connected=is_configured(block),
            label="FlickList" if inst == "default" else f"FlickList ({inst})",
            user=str(block.get("username") or "") or None,
            expires_at=int(block.get("expires_at") or 0) or None,
        )

    def start(self, cfg: MutableMapping[str, Any] | None = None, redirect_uri: str | None = None, *, instance_id: Any = None) -> dict[str, Any]:
        return start_device_code(dict(cfg or _load_full_cfg()), instance_id=instance_id)

    def finish(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None, **payload: Any) -> AuthStatus:
        poll_device_code(dict(cfg or _load_full_cfg()), instance_id=instance_id, device_code=str(payload.get("device_code") or "").strip() or None)
        return self.get_status(_load_full_cfg(), instance_id=instance_id)

    def refresh(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        refresh_token(dict(cfg or _load_full_cfg()), instance_id=instance_id)
        return self.get_status(_load_full_cfg(), instance_id=instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfgd = dict(cfg or _load_full_cfg())
        inst = normalize_instance_id(instance_id)
        block = writable_block(cfgd, inst)
        clear_oauth(block)
        _save_full_cfg(cfgd)
        log(f"FLICKLIST: disconnected (instance={inst})", level="INFO", module="AUTH")
        return self.get_status(cfgd, instance_id=inst)

    def html(self, cfg: Mapping[str, Any] | None = None) -> str:
        return html()


PROVIDER = FlickListAuth()
__all__ = ["PROVIDER", "FlickListAuth", "FlickListAuthError", "html", "__VERSION__"]


def html() -> str:
    return r'''<div class="section" id="sec-flicklist">
  <style>
    #sec-flicklist .sub{opacity:.7;font-size:.92em}
    #sec-flicklist .hidden{display:none !important}
    #sec-flicklist .fl-method-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:12px;align-items:center;margin-top:14px}
    #sec-flicklist .fl-methods{display:flex;gap:8px;min-width:0}
    #sec-flicklist .fl-method{appearance:none;cursor:pointer;flex:1 1 0;padding:10px 12px;border-radius:10px;border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.03);color:inherit;font:inherit;display:flex;align-items:center;justify-content:center;gap:8px;transition:border-color .15s ease,background .15s ease}
    #sec-flicklist .fl-method:hover{border-color:rgba(88,208,248,.52)}
    #sec-flicklist .fl-method.active{border-color:rgba(88,208,248,.72);background:linear-gradient(135deg,rgba(8,80,184,.18),rgba(88,208,248,.10));box-shadow:0 0 12px rgba(8,80,184,.24)}
    #sec-flicklist .fl-method .badge{display:inline-flex;align-items:center;line-height:1;font-size:.72em;text-transform:uppercase;letter-spacing:.05em;padding:3px 7px;border-radius:999px;background:rgba(88,208,248,.18);border:1px solid rgba(88,208,248,.42);color:#aeeeff}
    #sec-flicklist .fl-actions{display:flex;align-items:center;gap:8px;justify-content:flex-end;flex-wrap:wrap}
    #sec-flicklist .fl-pane{margin-top:12px}
    #sec-flicklist .fl-grid{display:block}
    #sec-flicklist .fl-field{display:grid;gap:8px;min-width:0}
    #sec-flicklist .fl-field input{width:100%;min-width:0}
    #sec-flicklist .fl-api-field-row{display:flex;gap:12px;align-items:center;max-width:760px}
    #sec-flicklist .fl-api-field-row input{flex:1 1 300px;min-width:0}
    #sec-flicklist .fl-api-field-row .msg{flex:0 1 auto;margin-top:0}
    #sec-flicklist #flicklist_save,#sec-flicklist #flicklist_device_start{background:linear-gradient(135deg,#0850b8,#58d0f8);border-color:rgba(8,80,184,.45);box-shadow:0 0 14px rgba(8,80,184,.35);color:#fff}
    #sec-flicklist .fl-qc{margin-top:12px;padding:14px;border-radius:12px;border:1px solid rgba(8,80,184,.35);background:rgba(8,80,184,.06)}
    #sec-flicklist .fl-qc-code{font-size:2em;font-weight:700;letter-spacing:.18em;padding:6px 0 6px .18em;color:#58d0f8;text-align:center;text-transform:uppercase;font-variant-numeric:tabular-nums}
    #sec-flicklist .fl-qc-meta{display:flex;justify-content:space-between;gap:12px;margin-top:6px}
    #sec-flicklist .inline{display:flex;gap:8px;align-items:center}
    #sec-flicklist .inline .msg{margin-left:auto;padding:8px 12px;border-radius:12px;border:1px solid rgba(88,208,248,.18);background:rgba(8,80,184,.10);color:#c7f4ff;font-weight:600}
    #sec-flicklist .inline .msg.warn{border-color:rgba(255,210,0,.18);background:rgba(255,210,0,.08);color:#ffe9a6}
    #sec-flicklist .inline .msg.hidden{display:none}
    @media(max-width:900px){#sec-flicklist .fl-method-row{grid-template-columns:1fr}#sec-flicklist .fl-actions{justify-content:flex-start}#sec-flicklist .fl-api-field-row{display:block}#sec-flicklist .fl-api-field-row .msg{margin-top:8px}}
  </style>
  <div class="head" data-toggle-section="sec-flicklist"><span class="chev"></span><strong>FlickList</strong></div>
  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="flicklist">
        <div class="cw-panel-head"><div><div class="cw-panel-title">FlickList</div><div class="muted">Connect FlickList with Device Code. API key remains available as a fallback.</div></div></div>
        <div class="cw-subtiles" style="margin-top:2px"><button type="button" class="cw-subtile active" data-sub="auth">Authentication</button></div>
        <div class="cw-subpanels"><div class="cw-subpanel active" data-sub="auth">
          <div class="cw-auth-journey" style="--cw-auth-c1:8,80,184;--cw-auth-c2:88,208,248;--cw-auth-logo:url('/assets/img/FLICKLIST.png')">
            <div class="cw-auth-journey-text"><div class="cw-auth-journey-title">Connect to FlickList</div><div class="cw-auth-journey-copy">Approve CrossWatch at flicklist.tv/link using the built-in public app id. Do not enter or store the client secret here.</div></div>
          </div>

          <div class="fl-method-row">
            <div class="fl-methods" role="tablist" aria-label="Authentication method">
              <button type="button" class="fl-method active" data-method="device_code" role="tab" aria-selected="true">Device Code <span class="badge">Recommended</span></button>
              <button type="button" class="fl-method" data-method="api_key" role="tab" aria-selected="false">API Key</button>
            </div>
            <div class="fl-actions fl-actions-device" data-method-actions="device_code">
              <button id="flicklist_device_start" class="btn" type="button">Connect FlickList</button>
              <button id="flicklist_device_cancel" class="btn danger hidden" type="button">Cancel</button>
              <button id="flicklist_disconnect_device" class="btn danger" type="button">Delete</button>
            </div>
            <div class="fl-actions fl-actions-api hidden" data-method-actions="api_key">
              <button id="flicklist_save" class="btn" type="button">Connect FlickList</button>
              <button id="flicklist_disconnect_api" class="btn danger" type="button">Delete</button>
            </div>
          </div>
          <input id="flicklist_auth_method" name="flicklist_auth_method" type="hidden" value="device_code">

          <div id="flicklist_device_panel" class="fl-pane" data-method="device_code">
            <input id="flicklist_device_code" type="hidden">
            <div id="flicklist_qc_state" class="fl-qc hidden">
              <div class="fl-qc-code" id="flicklist_qc_code">----&ndash;----</div>
              <div class="sub" id="flicklist_qc_help">Open flicklist.tv/link and enter this code to approve CrossWatch.</div>
              <div class="fl-qc-meta"><span class="sub" id="flicklist_qc_status">Waiting for approval&hellip;</span><span class="sub" id="flicklist_qc_timer"></span></div>
            </div>
          </div>

          <div id="flicklist_api_panel" class="fl-pane" data-method="api_key" style="display:none">
            <div class="fl-grid">
              <div class="fl-field">
                <label for="flicklist_api_key">API Key</label>
                <div class="fl-api-field-row">
                  <input id="flicklist_api_key" type="password" autocomplete="off" spellcheck="false" placeholder="fs_live_..." data-lpignore="true" data-1p-ignore="true" data-bwignore="true">
                  <div id="flicklist_hint" class="msg warn">Create an API key in FlickList Developer settings.</div>
                </div>
              </div>
            </div>
          </div>

          <div class="inline" style="margin-top:10px;justify-content:flex-end">
            <div id="flicklist_msg" class="msg ok hidden" role="status" aria-live="polite"></div>
          </div>
        </div></div>
      </div>
    </div>
  </div>
</div>
'''
