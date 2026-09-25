# providers/auth/_auth_WETRAKR.py
# CrossWatch - WeTrakr authentication and account status
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import base64
import hashlib
import os
import secrets
import threading
import time
from collections.abc import Mapping, MutableMapping
from typing import Any
from urllib.parse import urlencode, urlsplit

import requests

from cw_platform.app_version import user_agent
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, normalize_instance_id, resolve_provider_block
from ._auth_base import AuthManifest, AuthStatus

__VERSION__ = "0.1"
API_BASE = "https://api.wetrakr.com"
AUTHORIZE_URL = f"{API_BASE}/oauth/authorize"
TOKEN_URL = f"{API_BASE}/oauth/token"
REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob"
LOGIN_TTL = 600
REFRESH_URL = f"{API_BASE}/oauth/token/refresh"
REVOKE_URL = f"{API_BASE}/oauth/logout"
ME_URL = f"{API_BASE}/account/settings"
PLAN_URL = f"{API_BASE}/account/plan-usage"
DEFAULT_CLIENT_ID = "bb1d7ef30d706d17c150bd12132071a7"
CLIENT_ID_ENV = "CROSSWATCH_WETRAKR_CLIENT_ID"
HTTP_TIMEOUT = 20
_LOCKS: dict[str, Any] = {}
_LOCKS_GUARD = threading.Lock()
_CONFIG_LOCK = threading.RLock()
_REFRESH_RETRY_AT: dict[str, float] = {}
_PENDING: dict[str, dict[str, Any]] = {}
_TOKEN_FIELDS = ("access_token", "refresh_token", "token_type", "expires_at", "username", "user_id", "plan", "reauth_required")


class WeTrakrAuthError(RuntimeError):
    pass


def app_client_id() -> str:
    return str(os.getenv(CLIENT_ID_ENV) or DEFAULT_CLIENT_ID).strip()


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _lock(instance_id: Any) -> Any:
    with _LOCKS_GUARD:
        return _LOCKS.setdefault(normalize_instance_id(instance_id), threading.RLock())


def _load_full_cfg() -> dict[str, Any]:
    return load_config()


def _save_full_cfg(cfg: dict[str, Any]) -> None:
    save_config(cfg)


def provider_block(cfg: Mapping[str, Any] | None, instance_id: Any = None) -> dict[str, Any]:
    return resolve_provider_block(cfg or {}, "wetrakr", instance_id)


def _update(instance_id: Any, values: Mapping[str, Any]) -> None:
    with _CONFIG_LOCK:
        cfg = _load_full_cfg()
        block = ensure_instance_block(cfg, "wetrakr", instance_id)
        block.update(values)
        _save_full_cfg(cfg)


def _headers(token: str = "") -> dict[str, str]:
    headers = {
        "Accept": "application/json", "Content-Type": "application/json",
        "wetrakr-api-key": app_client_id(), "wetrakr-api-version": "1",
        "User-Agent": user_agent("WeTrakrAuth", override_env="CW_WETRAKR_UA"),
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _body(response: Any) -> dict[str, Any]:
    try:
        data = response.json()
    except (ValueError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def _failure(response: Any) -> dict[str, Any]:
    data = _body(response)
    status = int(response.status_code)
    if data.get("cloudflare_error") or data.get("error_code") == 1010:
        error = "access_blocked"
    elif status == 429:
        error = "rate_limited"
    elif status >= 500:
        error = "server_error"
    else:
        value = data.get("error")
        allowed = {"access_denied", "invalid_client", "invalid_grant"}
        error = value if isinstance(value, str) and value in allowed else "http_error"
    return {"ok": False, "status": error, "error": error, "http_status": status,
            "retry_after": max(0, _int(response.headers.get("Retry-After"))),
            "ray_id": str(data.get("ray_id") or "")}


def _post(url: str, payload: Mapping[str, Any], *, token: str = "", timeout: float = HTTP_TIMEOUT) -> tuple[dict[str, Any], bool]:
    try:
        response = requests.post(url, json=dict(payload), headers=_headers(token), timeout=timeout, allow_redirects=False)
    except requests.RequestException:
        return {"ok": False, "status": "network_error", "error": "network_error"}, False
    if not 200 <= response.status_code < 300:
        return _failure(response), False
    return _body(response), True


def is_configured(block: Mapping[str, Any] | None) -> bool:
    return bool((block or {}).get("access_token")) and not bool((block or {}).get("reauth_required"))


def normalize_auth_method(value: Any = None, block: Mapping[str, Any] | None = None) -> str:
    return "pkce"


def active_method(block: Mapping[str, Any] | None = None) -> str:
    return "pkce"


def set_active_method(block: MutableMapping[str, Any], method: str = "pkce") -> str:
    block["auth_method"] = "pkce"
    return "pkce"


def clear_oauth(block: MutableMapping[str, Any]) -> None:
    for key in _TOKEN_FIELDS:
        block[key] = 0 if key == "expires_at" else False if key == "reauth_required" else ""


def _require_reauth(instance_id: str, rejected_token: str) -> None:
    with _lock(instance_id):
        if provider_block(_load_full_cfg(), instance_id).get("access_token") != rejected_token:
            return
        cleared: dict[str, Any] = {}
        clear_oauth(cleared)
        cleared["reauth_required"] = True
        _update(instance_id, cleared)


def status_for_block(block: Mapping[str, Any] | None) -> dict[str, Any]:
    b = block or {}
    plan = str(b.get("plan") or "")
    result: dict[str, Any] = {
        "auth_method": "pkce", "connected": is_configured(b), "experimental": True,
        "username": str(b.get("username") or ""), "user_id": str(b.get("user_id") or ""),
        "expires_at": _int(b.get("expires_at")), "reauth_required": bool(b.get("reauth_required")),
        "plan": plan, "vip": plan == "vip", "client_id_configured": bool(app_client_id()),
    }
    return result


def start_oauth(cfg: Mapping[str, Any] | None = None, *, instance_id: Any = None) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    with _lock(inst):
        client_id = app_client_id()
        if not client_id:
            return {"ok": False, "error": "missing_client_id", "instance": inst}
        verifier = secrets.token_urlsafe(64)
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest()).rstrip(b"=").decode("ascii")
        state = secrets.token_urlsafe(32)
        expires_at = int(time.time()) + LOGIN_TTL
        _PENDING[inst] = {"state": state, "verifier": verifier, "client_id": client_id, "expires_at": expires_at}
        query = urlencode({"client_id": client_id, "redirect_uri": REDIRECT_URI, "code_challenge": challenge,
                           "code_challenge_method": "S256", "state": state})
        return {"ok": True, "instance": inst, "flow_id": state, "authorization_url": f"{AUTHORIZE_URL}?{query}", "expires_at": expires_at}


def _identity(token: str, *, timeout: float = HTTP_TIMEOUT) -> tuple[dict[str, Any], bool]:
    try:
        response = requests.get(ME_URL, headers=_headers(token), timeout=timeout, allow_redirects=False)
    except requests.RequestException:
        return {"ok": False, "status": "network_error", "error": "network_error"}, False
    if response.status_code != 200:
        return _failure(response), False
    account = _body(response)
    if not account.get("id"):
        return {"ok": False, "status": "invalid_account", "error": "invalid_account"}, False
    return account, True


def account_info(account: Mapping[str, Any]) -> dict[str, Any]:
    info = account.get("info")
    if not isinstance(info, Mapping):
        info = {}
    return {"user_id": str(account.get("id") or ""), "username": str(info.get("username") or ""),
            "display_name": str(info.get("display_name") or ""), "plan": str(account.get("plan") or ""),
            "vip": account.get("plan") == "vip"}


def _token_values(data: Mapping[str, Any], *, fallback_refresh: str = "") -> dict[str, Any]:
    expires = _int(data.get("expires_in"), 604800)
    return {"access_token": str(data.get("access_token") or ""),
            "refresh_token": str(data.get("new_refresh_token") or data.get("refresh_token") or fallback_refresh),
            "expires_at": int(time.time()) + max(1, expires), "token_type": "bearer", "reauth_required": False}


def finish_oauth(cfg: Mapping[str, Any] | None = None, *, code: str = "", flow_id: str = "",
                 instance_id: Any = None, timeout: float = HTTP_TIMEOUT) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    with _lock(inst):
        pending = _PENDING.get(inst)
        if not pending or not secrets.compare_digest(str(pending["state"]), str(flow_id)):
            return {"ok": False, "error": "invalid_flow", "instance": inst}
        if time.time() >= pending["expires_at"]:
            _PENDING.pop(inst, None)
            return {"ok": False, "error": "expired_flow", "instance": inst}
        code = str(code).strip()
        if not code or len(code) > 2048 or any(char.isspace() for char in code):
            return {"ok": False, "error": "invalid_code", "instance": inst, "retryable": True}
        retry_at = pending.get("retry_at", 0)
        if retry_at > time.time():
            return {"ok": False, "error": "rate_limited", "instance": inst, "retryable": True,
                    "retry_after": int(retry_at - time.time()) + 1}
        token = pending.get("token_response")
        if not isinstance(token, Mapping):
            token, ok = _post(TOKEN_URL, {"client_id": pending["client_id"], "code": code,
                                        "code_verifier": pending["verifier"]}, timeout=timeout)
            if not ok:
                if token.get("http_status") == 429:
                    pending["retry_at"] = time.time() + max(5, _int(token.get("retry_after")))
                    return {**token, "instance": inst, "retryable": True}
                _PENDING.pop(inst, None)
                error = "invalid_code" if token.get("http_status") == 400 else token.get("error", "http_error")
                return {**token, "error": error, "instance": inst, "retryable": False}
            if not token.get("access_token") or not token.get("refresh_token"):
                _PENDING.pop(inst, None)
                return {"ok": False, "error": "invalid_response", "instance": inst}
            pending["token_response"] = dict(token)
        account, ok = _identity(str(token["access_token"]), timeout=timeout)
        if not ok:
            retryable = account.get("status") in {"network_error", "server_error", "rate_limited"}
            if retryable:
                pending["retry_at"] = time.time() + max(5, _int(account.get("retry_after")))
            else:
                _PENDING.pop(inst, None)
            error = "account_access_denied" if account.get("http_status") == 401 else account.get("error", "invalid_account")
            return {**account, "error": error, "instance": inst, "retryable": retryable}
        values = {**_token_values(token), **account_info(account), "auth_method": "pkce"}
        values.pop("vip", None)
        values.pop("display_name", None)
        _update(inst, values)
        _PENDING.pop(inst, None)
        _REFRESH_RETRY_AT.pop(inst, None)
        return {"ok": True, "status": "authorized", "instance": inst, **account_info(account)}


def cancel_oauth(cfg: Mapping[str, Any] | None = None, *, flow_id: str = "", instance_id: Any = None) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    with _lock(inst):
        pending = _PENDING.get(inst)
        if pending and secrets.compare_digest(str(pending["state"]), str(flow_id)):
            _PENDING.pop(inst, None)
    return {"ok": True, "instance": inst}


def refresh_token(cfg: Mapping[str, Any] | None = None, *, instance_id: Any = None, force: bool = False,
                  rejected_token: str = "", timeout: float = HTTP_TIMEOUT) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    with _lock(inst):
        block = provider_block(_load_full_cfg(), inst)
        access = str(block.get("access_token") or "")
        if access and ((rejected_token and access != rejected_token) or (not force and _int(block.get("expires_at")) > time.time() + 300)):
            return {"ok": True, "status": "fresh", "instance": inst}
        refresh = str(block.get("refresh_token") or "")
        if not refresh:
            if rejected_token or (access and _int(block.get("expires_at")) and _int(block.get("expires_at")) <= time.time()):
                _require_reauth(inst, access)
            return {"ok": False, "status": "missing_refresh", "instance": inst}
        retry_at = _REFRESH_RETRY_AT.get(inst, 0)
        if retry_at > time.time():
            return {"ok": False, "status": "rate_limited", "retry_after": int(retry_at - time.time()) + 1, "instance": inst}
        _REFRESH_RETRY_AT[inst] = time.time() + 30
        data, ok = _post(REFRESH_URL, {"refresh_token": refresh}, timeout=timeout)
        if not ok:
            _REFRESH_RETRY_AT[inst] = time.time() + max(30, _int(data.get("retry_after")))
            if data.get("error") == "invalid_grant" or (data.get("http_status") == 401 and data.get("error") != "access_blocked"):
                _require_reauth(inst, access)
            return {**data, "instance": inst}
        if not data.get("access_token") or not (data.get("new_refresh_token") or data.get("refresh_token")):
            return {"ok": False, "status": "invalid_response", "instance": inst}
        _update(inst, _token_values(data))
        return {"ok": True, "status": "refreshed", "instance": inst}


def request_with_auth(session: requests.Session, method: str, url: str, *, cfg: Mapping[str, Any] | None,
                      instance_id: Any = None, timeout: float = HTTP_TIMEOUT, max_retries: int = 1,
                      request_func: Any = None, **kwargs: Any) -> requests.Response:
    from providers.sync._mod_common import request_with_retries

    if urlsplit(url).netloc != "api.wetrakr.com" or urlsplit(url).scheme != "https":
        raise WeTrakrAuthError("invalid_api_url")
    inst = normalize_instance_id(instance_id)
    block = provider_block(_load_full_cfg(), inst)
    if not is_configured(block):
        raise WeTrakrAuthError("reconnect_required")
    proactive = bool(_int(block.get("expires_at")) and _int(block.get("expires_at")) <= time.time() + 300)
    refreshed = False
    if proactive and block.get("refresh_token"):
        if refresh_token(cfg, instance_id=inst, timeout=timeout).get("ok"):
            block = provider_block(_load_full_cfg(), inst)
            refreshed = True
        elif not is_configured(provider_block(_load_full_cfg(), inst)):
            raise WeTrakrAuthError("reconnect_required")
    token = str(block.get("access_token") or "")
    call = request_func or request_with_retries
    headers = {**dict(kwargs.pop("headers", {}) or {}), **_headers(token)}
    kwargs["allow_redirects"] = False
    response = call(session, method, url, headers=headers, timeout=timeout, max_retries=max_retries, **kwargs)
    if response.status_code == 401 and not proactive and block.get("refresh_token"):
        result = refresh_token(cfg, instance_id=inst, force=True, rejected_token=token, timeout=timeout)
        if result.get("ok"):
            headers.update(_headers(str(provider_block(_load_full_cfg(), inst).get("access_token") or "")))
            response = call(session, method, url, headers=headers, timeout=timeout, max_retries=max_retries, **kwargs)
            if response.status_code == 401:
                _require_reauth(inst, headers["Authorization"].removeprefix("Bearer "))
    elif response.status_code == 401 and (not block.get("refresh_token") or proactive):
        if not block.get("refresh_token") or refreshed:
            _require_reauth(inst, token)
    return response


def account_status(cfg: Mapping[str, Any], *, instance_id: Any = None) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    result = status_for_block(provider_block(cfg, inst))
    result["instance"] = inst
    if not result["connected"]:
        return result
    with requests.Session() as session:
        try:
            response = request_with_auth(session, "GET", ME_URL, cfg=cfg, instance_id=inst)
            account = _body(response)
            if response.status_code != 200 or not account.get("id"):
                return {**result, "connected": False, "error": "reconnect_required" if response.status_code == 401 else "account_unavailable"}
            result.update(account_info(account))
        except (requests.RequestException, WeTrakrAuthError):
            return {**result, "connected": False, "error": "account_unavailable"}
        try:
            response = request_with_auth(session, "GET", PLAN_URL, cfg=_load_full_cfg(), instance_id=inst)
            usage = _body(response)
            if response.status_code == 200 and usage.get("tier"):
                result["plan_usage"] = {key: usage.get(key) for key in ("plan", "tier", "features")}
        except (requests.RequestException, WeTrakrAuthError):
            pass
        return result


class WeTrakrAuth:
    name = "WETRAKR"
    label = "WeTrakr"

    def manifest(self) -> AuthManifest:
        return AuthManifest(name=self.name, label=self.label, flow="pkce", fields=[],
                            actions={"start": True, "finish": True, "refresh": True, "disconnect": True},
                            verify_url=AUTHORIZE_URL, notes="Approve in your browser and paste the authorization code. PKCE requires no client secret.")

    def capabilities(self) -> dict[str, Any]:
        return {"pkce": True, "out_of_band": True, "refresh": True, "revoke": True, "experimental": True}

    def get_status(self, cfg: Mapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        state = status_for_block(provider_block(cfg if cfg is not None else _load_full_cfg(), instance_id))
        return AuthStatus(connected=state["connected"], label=self.label, user=state["username"] or None,
                          expires_at=state["expires_at"] or None, extra={"plan": state["plan"], "vip": state["vip"]})

    def start(self, cfg: Any = None, redirect_uri: str | None = None, *, instance_id: Any = None) -> dict[str, Any]:
        return start_oauth(cfg, instance_id=instance_id)

    def finish(self, cfg: Any = None, *, instance_id: Any = None, **payload: Any) -> AuthStatus:
        finish_oauth(cfg, code=payload.get("code", ""), flow_id=payload.get("flow_id", ""), instance_id=instance_id)
        return self.get_status(instance_id=instance_id)

    def refresh(self, cfg: Any = None, *, instance_id: Any = None) -> AuthStatus:
        refresh_token(cfg, instance_id=instance_id)
        return self.get_status(instance_id=instance_id)

    def disconnect(self, cfg: Any = None, *, instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        with _lock(inst):
            block = provider_block(_load_full_cfg(), inst)
            if block.get("refresh_token") and block.get("access_token"):
                _post(REVOKE_URL, {"refresh_token": block["refresh_token"]}, token=str(block["access_token"]))
            _PENDING.pop(inst, None)
            _REFRESH_RETRY_AT.pop(inst, None)
            cleared: dict[str, Any] = {}
            clear_oauth(cleared)
            _update(inst, cleared)
        return self.get_status(instance_id=inst)

    def html(self, cfg: Any = None) -> str:
        return html()


PROVIDER = WeTrakrAuth()


def html() -> str:
    return '''<div class="section" id="sec-wetrakr">
  <div class="head" data-toggle-section="sec-wetrakr"><span class="chev"></span><strong>WeTrakr</strong></div>
  <div class="body"><div class="cw-panel"><div class="cw-meta-provider-panel active" data-provider="wetrakr">
    <div class="cw-panel-head"><div><div class="cw-panel-title">WeTrakr</div><div class="muted">Approve CrossWatch in your browser, then paste the authorization code here.</div></div></div>
    <div class="cw-subtiles"><button type="button" class="cw-subtile active" data-sub="auth">Authentication</button></div>
    <div class="cw-subpanels"><div class="cw-subpanel active" data-sub="auth">
      <div id="wetrakr_oauth_panel" class="hidden">
        <p>Sign in to WeTrakr and approve CrossWatch. Copy the code WeTrakr displays and paste it below.</p>
        <a id="wetrakr_approval_link" target="_blank" rel="noopener noreferrer">Open WeTrakr approval page</a>
        <label for="wetrakr_code">Authorization code</label>
        <input id="wetrakr_code" type="text" autocomplete="off" autocapitalize="none" spellcheck="false" maxlength="2048" placeholder="Paste the code from WeTrakr">
        <div id="wetrakr_login_timer" class="muted" role="status"></div>
      </div>
      <div class="wt-actions">
        <button id="wetrakr_oauth_start" class="btn" type="button">Connect WeTrakr</button>
        <button id="wetrakr_oauth_finish" class="btn hidden" type="button">Complete connection</button>
        <button id="wetrakr_oauth_cancel" class="btn hidden" type="button">Cancel</button>
        <button id="wetrakr_disconnect" class="btn danger" type="button">Delete</button>
        <div id="wetrakr_msg" class="msg hidden" role="status" aria-live="polite"></div>
      </div>
      <div id="wetrakr_account" class="hidden"><div id="wetrakr_plan"></div><div id="wetrakr_limits" class="muted"></div></div>
    </div></div>
  </div></div></div>
</div>'''
