# /providers/sync/mdblist/_auth.py
# Shared MDBList authentication helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import os
import time
from collections.abc import Mapping, MutableMapping
from typing import Any

import requests

from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, get_provider_block, normalize_instance_id

API_BASE = "https://api.mdblist.com"
OAUTH_TOKEN_URL = f"{API_BASE}/oauth/token/"
OAUTH_DEVICE_URL = f"{API_BASE}/oauth/device-authorization/"
VERIFY_URL = "https://mdblist.com/oauth/device/"
DEFAULT_CLIENT_ID = "4A5MNaWPLOLSHws7JCQXOtATBOs2AmYJsqcwT7Uj"
CLIENT_ID_ENV = "CROSSWATCH_MDBLIST_CLIENT_ID"
SCOPE = "write"
REFRESH_SKEW_SEC = 300
UA = "CrossWatch/MDBListAuth"


class MDBListAuthError(RuntimeError):
    pass


def now() -> int:
    return int(time.time())


def app_client_id() -> str:
    return str(os.environ.get(CLIENT_ID_ENV) or DEFAULT_CLIENT_ID).strip()


def normalize_auth_method(value: Any, block: Mapping[str, Any] | None = None) -> str:
    b = block or {}
    has_api_key = bool(str(b.get("api_key") or b.get("key") or "").strip())
    has_oauth = bool(str(b.get("access_token") or "").strip() or str(b.get("refresh_token") or "").strip())
    has_pending_device = isinstance(b.get("_pending_device"), Mapping)
    if has_api_key and not has_oauth:
        if has_pending_device:
            return "device_code"
        return "api_key"
    if has_oauth or has_pending_device:
        return "device_code"

    raw = str(value or "").strip().lower().replace("-", "_")
    if raw in {"api", "apikey", "api_key", "key"}:
        return "api_key"
    if raw in {"device", "device_code", "oauth", "oauth_device", "bearer"}:
        return "device_code"
    return "device_code"


def normalize_instance_id_value(instance_id: Any = None) -> str:
    return normalize_instance_id(instance_id)


def provider_block(cfg: Mapping[str, Any] | None, instance_id: Any = None) -> dict[str, Any]:
    out = get_provider_block(cfg or {}, "mdblist", instance_id)
    if out:
        return out

    base = (cfg or {}).get("mdblist") if isinstance(cfg, Mapping) else None
    return dict(base or {}) if isinstance(base, Mapping) else {}


def writable_block(cfg: dict[str, Any], instance_id: Any = None) -> dict[str, Any]:
    return ensure_instance_block(cfg, "mdblist", instance_id)


def active_method(block: Mapping[str, Any] | None) -> str:
    b = block or {}
    return normalize_auth_method(b.get("auth_method"), b)


def set_active_method(block: MutableMapping[str, Any], method: str) -> str:
    m = normalize_auth_method(method, block)
    block["auth_method"] = m
    if m == "api_key":
        clear_oauth(block)
    elif str(block.get("access_token") or "").strip() or str(block.get("refresh_token") or "").strip():
        block["api_key"] = ""
    return m


def clear_oauth(block: MutableMapping[str, Any]) -> None:
    for key in (
        "access_token",
        "refresh_token",
        "token_type",
        "scope",
        "expires_at",
        "username",
        "user_id",
        "_pending_device",
    ):
        if key in block:
            if key == "expires_at":
                block[key] = 0
            elif key == "_pending_device":
                block.pop(key, None)
            else:
                block[key] = ""


def clear_api_key(block: MutableMapping[str, Any]) -> None:
    block["api_key"] = ""


def is_configured(block: Mapping[str, Any] | None) -> bool:
    b = block or {}
    if active_method(b) == "api_key":
        return bool(str(b.get("api_key") or b.get("key") or "").strip())
    return bool(str(b.get("access_token") or "").strip())


def status_for_block(block: Mapping[str, Any] | None) -> dict[str, Any]:
    b = block or {}
    method = active_method(b)
    out: dict[str, Any] = {
        "auth_method": method,
        "connected": is_configured(b),
        "api_key_configured": bool(str(b.get("api_key") or b.get("key") or "").strip()),
        "device_configured": bool(str(b.get("access_token") or "").strip()),
        "client_id_configured": bool(app_client_id()),
        "expires_at": int(b.get("expires_at") or 0) if method == "device_code" else 0,
        "username": str(b.get("username") or ""),
    }
    pend = b.get("_pending_device")
    if isinstance(pend, Mapping):
        out["pending"] = {
            "user_code": str(pend.get("user_code") or ""),
            "verification_uri": str(pend.get("verification_uri") or pend.get("verification_url") or VERIFY_URL),
            "expires_at": int(pend.get("expires_at") or 0),
            "interval": int(pend.get("interval") or 5),
        }
    return out


def about_to_expire(block: Mapping[str, Any], skew_sec: int = REFRESH_SKEW_SEC) -> bool:
    exp = int(block.get("expires_at") or 0)
    return bool(exp and exp - now() <= max(0, int(skew_sec)))


def _load_full_cfg() -> dict[str, Any]:
    try:
        cfg = load_config() or {}
        return cfg if isinstance(cfg, dict) else dict(cfg)
    except Exception:
        return {}


def _save_full_cfg(cfg: dict[str, Any]) -> None:
    save_config(cfg)


def _copy_token_fields(dst: MutableMapping[str, Any], src: Mapping[str, Any]) -> None:
    for key in ("access_token", "refresh_token", "token_type", "scope", "expires_at", "username", "user_id"):
        if key in src:
            dst[key] = src.get(key)


def start_device_code(
    cfg: dict[str, Any] | None,
    *,
    instance_id: Any = None,
    client_id: str | None = None,
    scope: str = SCOPE,
    timeout: float = 20.0,
) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_full_cfg()
    inst = normalize_instance_id(instance_id)
    block = writable_block(cfgd, inst)
    cid = app_client_id()
    if not cid:
        return {"ok": False, "error": "missing_client_id", "instance": inst}

    block.pop("client_id", None)
    set_active_method(block, "device_code")

    try:
        r = requests.post(
            OAUTH_DEVICE_URL,
            data={"client_id": cid, "scope": scope or SCOPE},
            headers={"Accept": "application/json", "User-Agent": UA},
            timeout=timeout,
        )
    except requests.RequestException as e:
        return {"ok": False, "error": "network_error", "detail": str(e), "instance": inst}

    if r.status_code >= 400:
        return {"ok": False, "error": "http_error", "status": int(r.status_code), "body": (r.text or "")[:400], "instance": inst}

    try:
        data: dict[str, Any] = r.json() or {}
    except ValueError:
        return {"ok": False, "error": "invalid_json", "body": (r.text or "")[:400], "instance": inst}

    device_code = str(data.get("device_code") or "").strip()
    user_code = str(data.get("user_code") or "").strip()
    verification_uri = str(data.get("verification_uri") or data.get("verification_url") or VERIFY_URL).strip() or VERIFY_URL
    interval = int(data.get("interval") or 5)
    expires_at = now() + int(data.get("expires_in") or 300)
    if not device_code or not user_code:
        return {"ok": False, "error": "invalid_response", "body": (r.text or "")[:400], "instance": inst}

    block["_pending_device"] = {
        "device_code": device_code,
        "user_code": user_code,
        "verification_uri": verification_uri,
        "interval": interval,
        "expires_at": expires_at,
        "created_at": now(),
    }
    block["auth_method"] = "device_code"
    _save_full_cfg(cfgd)
    return {
        "ok": True,
        "instance": inst,
        "device_code": device_code,
        "user_code": user_code,
        "verification_uri": verification_uri,
        "interval": interval,
        "expires_at": expires_at,
    }


def poll_device_code(
    cfg: dict[str, Any] | None,
    *,
    instance_id: Any = None,
    device_code: str | None = None,
    timeout: float = 20.0,
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
    if int((pend or {}).get("expires_at") or 0) and now() >= int((pend or {}).get("expires_at") or 0):
        return {"ok": False, "status": "expired_token", "instance": inst}

    try:
        r = requests.post(
            OAUTH_TOKEN_URL,
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": dc,
                "client_id": cid,
            },
            headers={"Accept": "application/json", "User-Agent": UA},
            timeout=timeout,
        )
    except requests.RequestException as e:
        return {"ok": False, "status": "network_error", "error": str(e), "instance": inst}

    if r.status_code in (400, 401, 403):
        try:
            err = str((r.json() or {}).get("error") or "authorization_pending")
        except Exception:
            err = "authorization_pending"
        return {"ok": False, "status": err, "instance": inst}
    if r.status_code >= 400:
        return {"ok": False, "status": f"http:{r.status_code}", "body": (r.text or "")[:400], "instance": inst}

    try:
        tok: dict[str, Any] = r.json() or {}
    except ValueError:
        return {"ok": False, "status": "bad_json", "instance": inst}

    access_token = str(tok.get("access_token") or "").strip()
    if not access_token:
        return {"ok": False, "status": "no_access_token", "instance": inst}

    block.update(
        {
            "auth_method": "device_code",
            "api_key": "",
            "access_token": access_token,
            "refresh_token": str(tok.get("refresh_token") or block.get("refresh_token") or "").strip(),
            "token_type": str(tok.get("token_type") or "Bearer").strip() or "Bearer",
            "scope": str(tok.get("scope") or SCOPE).strip() or SCOPE,
            "expires_at": now() + int(tok.get("expires_in") or 0),
        }
    )
    block.pop("_pending_device", None)
    _save_full_cfg(cfgd)
    return {"ok": True, "status": "ok", "instance": inst, "expires_at": int(block.get("expires_at") or 0)}


def refresh_token(
    cfg: dict[str, Any] | None = None,
    *,
    instance_id: Any = None,
    update_cfg: dict[str, Any] | None = None,
    timeout: float = 20.0,
) -> dict[str, Any]:
    full = _load_full_cfg()
    inst = normalize_instance_id(instance_id)
    block = writable_block(full, inst)
    cid = app_client_id()
    rt = str(block.get("refresh_token") or "").strip()
    if not rt and isinstance(cfg, Mapping):
        rt = str(provider_block(cfg, inst).get("refresh_token") or "").strip()
    if not cid or not rt:
        return {"ok": False, "status": "missing_refresh", "instance": inst}

    try:
        r = requests.post(
            OAUTH_TOKEN_URL,
            data={"grant_type": "refresh_token", "refresh_token": rt, "client_id": cid},
            headers={"Accept": "application/json", "User-Agent": UA},
            timeout=timeout,
        )
    except requests.RequestException as e:
        return {"ok": False, "status": "network_error", "error": str(e), "instance": inst}

    if r.status_code >= 400:
        err = ""
        try:
            body = r.json() or {}
            err = str(body.get("error") or body.get("error_description") or "")
        except Exception:
            err = (r.text or "")[:400]
        return {"ok": False, "status": f"refresh_failed:{r.status_code}", "error": err, "instance": inst}

    try:
        tok: dict[str, Any] = r.json() or {}
    except ValueError:
        return {"ok": False, "status": "bad_json", "instance": inst}

    access_token = str(tok.get("access_token") or "").strip()
    if not access_token:
        return {"ok": False, "status": "no_access_token", "instance": inst}

    block.update(
        {
            "auth_method": "device_code",
            "api_key": "",
            "access_token": access_token,
            "refresh_token": str(tok.get("refresh_token") or rt).strip(),
            "token_type": str(tok.get("token_type") or block.get("token_type") or "Bearer").strip() or "Bearer",
            "scope": str(tok.get("scope") or block.get("scope") or SCOPE).strip() or SCOPE,
            "expires_at": now() + int(tok.get("expires_in") or 0),
        }
    )
    _save_full_cfg(full)

    for target in (cfg, update_cfg):
        if isinstance(target, dict):
            try:
                _copy_token_fields(writable_block(target, inst), block)
            except Exception:
                try:
                    md = target.get("mdblist")
                    if isinstance(md, dict):
                        _copy_token_fields(md, block)
                except Exception:
                    pass

    return {"ok": True, "status": "ok", "instance": inst, "expires_at": int(block.get("expires_at") or 0)}


def prepare_auth(
    cfg: Mapping[str, Any] | None,
    *,
    instance_id: Any = None,
    refresh: bool = True,
) -> tuple[dict[str, str], dict[str, Any]]:
    inst = normalize_instance_id(instance_id)
    block = provider_block(cfg, inst)
    method = active_method(block)
    if method == "api_key":
        api_key = str(block.get("api_key") or block.get("key") or "").strip()
        if not api_key:
            raise MDBListAuthError("missing_api_key")
        return {}, {"apikey": api_key}

    if refresh and (not str(block.get("access_token") or "").strip() or about_to_expire(block)):
        res = refresh_token(dict(cfg or {}), instance_id=inst)
        if not res.get("ok"):
            raise MDBListAuthError(str(res.get("status") or "refresh_failed"))
        block = provider_block(_load_full_cfg(), inst)

    token = str(block.get("access_token") or "").strip()
    if not token:
        raise MDBListAuthError("missing_access_token")
    return {"Authorization": f"Bearer {token}"}, {}


def merge_auth_kwargs(
    cfg: Mapping[str, Any] | None,
    *,
    instance_id: Any = None,
    kwargs: dict[str, Any] | None = None,
    refresh: bool = True,
) -> dict[str, Any]:
    out = dict(kwargs or {})
    params = dict(out.get("params") or {})
    params.pop("apikey", None)
    headers = dict(out.get("headers") or {})
    auth_headers, auth_params = prepare_auth(cfg, instance_id=instance_id, refresh=refresh)
    params.update(auth_params)
    headers.update(auth_headers)
    if params:
        out["params"] = params
    elif "params" in out:
        out.pop("params", None)
    if headers:
        out["headers"] = headers
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
    from providers.sync._mod_common import request_with_retries

    call = request_func or request_with_retries
    req_kwargs = merge_auth_kwargs(cfg, instance_id=instance_id, kwargs=kwargs)
    resp = call(session, method, url, timeout=timeout, max_retries=max_retries, **req_kwargs)
    if getattr(resp, "status_code", None) != 401 or active_method(provider_block(cfg, instance_id)) != "device_code":
        return resp

    res = refresh_token(dict(cfg or {}), instance_id=instance_id)
    if not res.get("ok"):
        return resp
    fresh_cfg = _load_full_cfg()
    req_kwargs = merge_auth_kwargs(fresh_cfg, instance_id=instance_id, kwargs=kwargs, refresh=False)
    return call(session, method, url, timeout=timeout, max_retries=max_retries, **req_kwargs)
