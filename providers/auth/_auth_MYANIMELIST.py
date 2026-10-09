# providers/auth/_auth_MYANIMELIST.py
# CrossWatch - MyAnimeList hosted authentication and account status
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import secrets
import socket
import threading
import time
from collections.abc import Mapping, MutableMapping
from typing import Any
from urllib.parse import urlsplit

import requests

from _logging import log as _app_log
from cw_platform.app_version import user_agent
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, normalize_instance_id, resolve_provider_block
from ._auth_base import AuthManifest, AuthStatus

__VERSION__ = "0.1"
ME_URL = "https://api.myanimelist.net/v2/users/@me"
BROKER_URL = "https://auth.crosswatch.app"
HTTP_TIMEOUT = 20
LOGIN_TTL = 600
_LOCKS: dict[str, Any] = {}
_LOCKS_GUARD = threading.Lock()
_CONFIG_LOCK = threading.RLock()
_PENDING: dict[str, dict[str, Any]] = {}
_REFRESH_RETRY_AT: dict[str, float] = {}
_TOKEN_FIELDS = ("access_token", "refresh_token", "token_type", "expires_at", "username", "user_id", "auth_broker", "reauth_required")


class MyAnimeListAuthError(RuntimeError):
    pass


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _lock(instance_id: Any) -> Any:
    with _LOCKS_GUARD:
        return _LOCKS.setdefault(normalize_instance_id(instance_id), threading.RLock())


def provider_block(cfg: Mapping[str, Any] | None, instance_id: Any = None) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    config = cfg if cfg is not None else load_config()
    if inst != "default":
        base = config.get("myanimelist")
        if not isinstance(base, Mapping) or "instances" not in base:
            base = load_config().get("myanimelist")
        instances = base.get("instances") if isinstance(base, Mapping) else {}
        config = {"myanimelist": {"instances": instances or {}}}
    return dict(resolve_provider_block(config, "myanimelist", inst))


def _update(instance_id: Any, values: Mapping[str, Any]) -> None:
    with _CONFIG_LOCK:
        cfg = load_config()
        ensure_instance_block(cfg, "myanimelist", instance_id).update(values)
        save_config(cfg)


def _headers(token: str = "") -> dict[str, str]:
    result = {"Accept": "application/json", "User-Agent": user_agent("MyAnimeListAuth")}
    if token:
        result["Authorization"] = f"Bearer {token}"
    return result


def _body(response: Any) -> dict[str, Any]:
    try:
        body = response.json()
        return body if isinstance(body, dict) else {}
    except (ValueError, TypeError):
        return {}


def _log(message: str, level: str = "WARN") -> None:
    try:
        _app_log(f"[MYANIMELIST] {message}", level=level, module="AUTH")
    except Exception:
        pass


def _network_reason(error: BaseException) -> str:
    if isinstance(error, requests.exceptions.SSLError):
        return "network_tls"
    if isinstance(error, requests.exceptions.ProxyError):
        return "network_proxy"
    pending = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, socket.gaierror) or type(current).__name__ == "NameResolutionError":
            return "network_dns"
        if isinstance(current, (requests.exceptions.Timeout, TimeoutError)):
            return "network_timeout"
        pending.extend(value for value in (current.__cause__, current.__context__, getattr(current, "reason", None), *current.args) if isinstance(value, BaseException))
    return "network_connection"


def _post(origin: str, action: str, payload: Mapping[str, Any], *, credential: str = "", timeout: float = HTTP_TIMEOUT) -> dict[str, Any]:
    if not origin or origin != BROKER_URL:
        return {"ok": False, "error": "broker_unavailable"}
    try:
        response = requests.post(f"{origin}/mal/{action}", json=dict(payload), headers={**_headers(credential), "X-CrossWatch-Client": "CrossWatch"}, timeout=timeout, allow_redirects=False)
    except requests.RequestException as exc:
        reason = _network_reason(exc)
        _log(f"Broker {action} failed: {reason}; host=auth.crosswatch.app; exception={type(exc).__name__}")
        return {"ok": False, "error": "network_error", "reason": reason}
    body = _body(response)
    if response.status_code == 200:
        if not body:
            _log(f"Broker {action} returned an invalid JSON response; HTTP 200")
        return body
    allowed = {"invalid_grant", "invalid_client", "access_denied", "expired_flow", "invalid_flow"}
    error = body.get("error")
    error = error if isinstance(error, str) and error in allowed else "broker_error"
    if response.status_code == 429:
        error = "rate_limited"
    _log(f"Broker {action} failed: HTTP {response.status_code}; error={error}")
    return {"ok": False, "error": error, "retry_after": max(5, _int(response.headers.get("Retry-After"), 30))}


def is_configured(block: Mapping[str, Any] | None) -> bool:
    return bool((block or {}).get("access_token")) and not bool((block or {}).get("reauth_required"))


def normalize_auth_method(value: Any = None, block: Mapping[str, Any] | None = None) -> str:
    return "hosted_oauth"


def active_method(block: Mapping[str, Any] | None = None) -> str:
    return "hosted_oauth"


def set_active_method(block: MutableMapping[str, Any], method: str = "hosted_oauth") -> str:
    block["auth_method"] = "hosted_oauth"
    return "hosted_oauth"


def clear_oauth(block: MutableMapping[str, Any]) -> None:
    for key in _TOKEN_FIELDS:
        block[key] = 0 if key == "expires_at" else False if key == "reauth_required" else ""


def _require_reauth(instance_id: str, rejected_token: str) -> None:
    with _lock(instance_id):
        if provider_block(load_config(), instance_id).get("access_token") != rejected_token:
            return
        cleared: dict[str, Any] = {}
        clear_oauth(cleared)
        cleared["reauth_required"] = True
        _update(instance_id, cleared)


def status_for_block(block: Mapping[str, Any] | None) -> dict[str, Any]:
    b = block or {}
    return {"connected": is_configured(b), "auth_method": "hosted_oauth", "experimental": True,
            "username": str(b.get("username") or ""), "user_id": str(b.get("user_id") or ""),
            "expires_at": _int(b.get("expires_at")), "reauth_required": bool(b.get("reauth_required")),
            "login_available": True}


def _token_values(data: Mapping[str, Any]) -> dict[str, Any] | None:
    access, refresh = data.get("access_token"), data.get("refresh_token")
    expires = _int(data.get("expires_in"))
    if not isinstance(access, str) or not access.strip() or not isinstance(refresh, str) or not refresh.strip() or expires <= 0:
        return None
    if str(data.get("token_type") or "").lower() != "bearer":
        return None
    return {"access_token": access, "refresh_token": refresh, "token_type": "bearer",
            "expires_at": int(time.time()) + expires, "reauth_required": False}


def account_info(account: Mapping[str, Any]) -> dict[str, Any]:
    return {"user_id": str(account.get("id") or ""), "username": str(account.get("name") or "")}


def valid_account(account: Mapping[str, Any]) -> bool:
    return _int(account.get("id")) > 0 and isinstance(account.get("name"), str) and bool(account["name"].strip())


def start_oauth(cfg: Mapping[str, Any] | None = None, *, instance_id: Any = None) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    with _lock(inst):
        if inst in _PENDING:
            cancel_oauth(flow_id=_PENDING[inst]["flow_id"], instance_id=inst)
        origin = BROKER_URL
        result = _post(origin, "start", {})
        if not result.get("ok"):
            return {"ok": False, "error": result.get("error", "invalid_response"), "reason": result.get("reason", ""), "instance": inst}
        url = str(result.get("authorization_url") or "")
        parsed = urlsplit(url)
        session_id, poll_token = result.get("session_id"), result.get("poll_token")
        expires = _int(result.get("expires_in"))
        if parsed.scheme != "https" or parsed.netloc != "myanimelist.net" or parsed.path != "/v1/oauth2/authorize" or parsed.fragment or not isinstance(session_id, str) or not session_id or not isinstance(poll_token, str) or len(poll_token) < 32 or expires <= 0:
            return {"ok": False, "error": "invalid_response", "instance": inst}
        flow_id = secrets.token_urlsafe(32)
        interval = max(3, min(30, _int(result.get("interval"), 5)))
        expires_at = int(time.time()) + min(LOGIN_TTL, expires)
        _PENDING[inst] = {"flow_id": flow_id, "session_id": session_id, "poll_token": poll_token,
                          "origin": origin, "expires_at": expires_at, "interval": interval, "next_poll": 0}
        return {"ok": True, "flow_id": flow_id, "authorization_url": url, "expires_at": expires_at, "interval": interval, "instance": inst}


def poll_oauth(cfg: Mapping[str, Any] | None = None, *, flow_id: str = "", instance_id: Any = None) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    with _lock(inst):
        pending = _PENDING.get(inst)
        if not pending or not secrets.compare_digest(pending["flow_id"].encode(), flow_id.encode()):
            return {"ok": False, "error": "invalid_flow"}
        if time.time() >= pending["expires_at"]:
            _PENDING.pop(inst, None)
            return {"ok": False, "error": "expired_flow"}
        if time.time() < pending["next_poll"]:
            return {"ok": True, "status": "pending", "interval": pending["interval"]}
        pending["next_poll"] = time.time() + pending["interval"]
        values = pending.get("tokens")
        if values is None:
            data = _post(pending["origin"], "poll", {"session_id": pending["session_id"]}, credential=pending["poll_token"])
            if not data.get("ok"):
                retryable = data.get("error") in {"network_error", "broker_error", "rate_limited"}
                if retryable:
                    pending["next_poll"] = time.time() + max(pending["interval"], _int(data.get("retry_after"), 5))
                else:
                    _PENDING.pop(inst, None)
                return {"ok": False, "error": data.get("error", "invalid_response"), "reason": data.get("reason", ""), "retryable": retryable}
            if data.get("status") == "pending":
                return {"ok": True, "status": "pending", "interval": pending["interval"]}
            values = _token_values(data)
            if data.get("status") != "authorized" or values is None:
                _PENDING.pop(inst, None)
                return {"ok": False, "error": "invalid_response"}
            pending["tokens"] = values
        try:
            response = requests.get(ME_URL, headers=_headers(values["access_token"]), timeout=HTTP_TIMEOUT, allow_redirects=False)
        except requests.RequestException as exc:
            reason = _network_reason(exc)
            _log(f"Account verification failed: {reason}; host=api.myanimelist.net; exception={type(exc).__name__}")
            return {"ok": False, "error": "network_error", "reason": reason, "retryable": True}
        if response.status_code == 429 or response.status_code >= 500:
            _log(f"Account verification failed: HTTP {response.status_code}")
            pending["next_poll"] = time.time() + max(5, _int(response.headers.get("Retry-After"), 30))
            return {"ok": False, "error": "account_unavailable", "retryable": True}
        account = _body(response)
        if response.status_code != 200 or not valid_account(account):
            _log(f"Account verification rejected: HTTP {response.status_code}; invalid account response")
            _PENDING.pop(inst, None)
            return {"ok": False, "error": "invalid_account"}
        _update(inst, {**values, **account_info(account), "auth_broker": pending["origin"], "auth_method": "hosted_oauth"})
        _PENDING.pop(inst, None)
        _REFRESH_RETRY_AT.pop(inst, None)
        return {"ok": True, "status": "authorized", "instance": inst, **account_info(account)}


def cancel_oauth(cfg: Mapping[str, Any] | None = None, *, flow_id: str = "", instance_id: Any = None) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    with _lock(inst):
        pending = _PENDING.get(inst)
        if pending and secrets.compare_digest(pending["flow_id"].encode(), flow_id.encode()):
            _PENDING.pop(inst, None)
            _post(pending["origin"], "cancel", {"session_id": pending["session_id"]}, credential=pending["poll_token"])
    return {"ok": True, "instance": inst}


def refresh_token(cfg: Mapping[str, Any] | None = None, *, instance_id: Any = None, force: bool = False,
                  rejected_token: str = "", timeout: float = HTTP_TIMEOUT) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    with _lock(inst):
        block = provider_block(load_config(), inst)
        access = str(block.get("access_token") or "")
        if access and ((rejected_token and access != rejected_token) or (not force and _int(block.get("expires_at")) > time.time() + 300)):
            return {"ok": True, "status": "fresh"}
        if not block.get("refresh_token"):
            if access:
                _require_reauth(inst, access)
            return {"ok": False, "error": "reconnect_required"}
        if time.time() < _REFRESH_RETRY_AT.get(inst, 0):
            return {"ok": False, "error": "rate_limited"}
        _REFRESH_RETRY_AT[inst] = time.time() + 30
        data = _post(str(block.get("auth_broker") or ""), "refresh", {"refresh_token": block["refresh_token"]}, timeout=timeout)
        if not data.get("ok"):
            _REFRESH_RETRY_AT[inst] = time.time() + max(30, _int(data.get("retry_after")))
            if data.get("error") == "invalid_grant":
                _require_reauth(inst, access)
            return {"ok": False, "error": data.get("error", "invalid_response")}
        values = _token_values(data)
        if values is None:
            return {"ok": False, "error": "invalid_response"}
        _update(inst, values)
        return {"ok": True, "status": "refreshed"}


def request_with_auth(session: requests.Session, method: str, url: str, *, cfg: Mapping[str, Any] | None,
                      instance_id: Any = None, timeout: float = HTTP_TIMEOUT, max_retries: int = 1,
                      request_func: Any = None, **kwargs: Any) -> requests.Response:
    from providers.sync._mod_common import request_with_retries

    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.netloc != "api.myanimelist.net" or not parsed.path.startswith("/v2/"):
        raise MyAnimeListAuthError("invalid_api_url")
    inst = normalize_instance_id(instance_id)
    block = provider_block(load_config(), inst)
    if not is_configured(block):
        raise MyAnimeListAuthError("reconnect_required")
    proactive = _int(block.get("expires_at")) <= time.time() + 300
    if proactive:
        result = refresh_token(cfg, instance_id=inst, timeout=timeout)
        if not result.get("ok"):
            raise MyAnimeListAuthError(str(result.get("error", "refresh_failed")))
        block = provider_block(load_config(), inst)
    token = str(block.get("access_token") or "")
    headers = {k: v for k, v in dict(kwargs.pop("headers", {}) or {}).items() if k.lower() != "authorization"}
    headers.update(_headers(token))
    kwargs["allow_redirects"] = False
    call = request_func or request_with_retries
    response = call(session, method, url, headers=headers, timeout=timeout, max_retries=max_retries, **kwargs)
    if response.status_code == 401 and not proactive:
        result = refresh_token(cfg, instance_id=inst, force=True, rejected_token=token, timeout=timeout)
        if result.get("ok"):
            token = str(provider_block(load_config(), inst).get("access_token") or "")
            headers.update(_headers(token))
            response = call(session, method, url, headers=headers, timeout=timeout, max_retries=max_retries, **kwargs)
            if response.status_code == 401:
                _require_reauth(inst, token)
    elif response.status_code == 401:
        _require_reauth(inst, token)
    return response


def account_status(cfg: Mapping[str, Any], *, instance_id: Any = None) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    result = {**status_for_block(provider_block(cfg, inst)), "instance": inst}
    if not result["connected"]:
        return result
    try:
        with requests.Session() as session:
            response = request_with_auth(session, "GET", ME_URL, cfg=cfg, instance_id=inst)
        account = _body(response)
        if response.status_code == 200 and valid_account(account):
            return {**result, **account_info(account)}
        return {**result, "connected": False, "error": "reconnect_required" if response.status_code == 401 else "account_unavailable"}
    except (requests.RequestException, MyAnimeListAuthError):
        return {**result, "connected": False, "error": "account_unavailable"}


class MyAnimeListAuth:
    name = "MYANIMELIST"
    label = "MyAnimeList"

    def manifest(self) -> AuthManifest:
        return AuthManifest(name=self.name, label=self.label, flow="oauth2", fields=[],
                            actions={"start": True, "finish": False, "refresh": True, "disconnect": True},
                            notes="Approve CrossWatch in MyAnimeList using the shared authentication service.")

    def capabilities(self) -> dict[str, Any]:
        return {"refresh": True, "experimental": True, "features": {}}

    def get_status(self, cfg: Mapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        state = status_for_block(provider_block(cfg, instance_id))
        return AuthStatus(connected=state["connected"], label=self.label, user=state["username"] or None,
                          expires_at=state["expires_at"] or None)

    def start(self, cfg: Any = None, redirect_uri: str | None = None, *, instance_id: Any = None) -> dict[str, Any]:
        return start_oauth(cfg, instance_id=instance_id)

    def finish(self, cfg: Any = None, *, instance_id: Any = None, **payload: Any) -> dict[str, Any]:
        return poll_oauth(cfg, flow_id=str(payload.get("flow_id") or ""), instance_id=instance_id)

    def refresh(self, cfg: Any = None, *, instance_id: Any = None) -> AuthStatus:
        refresh_token(cfg, instance_id=instance_id)
        return self.get_status(instance_id=instance_id)

    def disconnect(self, cfg: Any = None, *, instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        with _lock(inst):
            if inst in _PENDING:
                cancel_oauth(flow_id=_PENDING[inst]["flow_id"], instance_id=inst)
            cleared: dict[str, Any] = {}
            clear_oauth(cleared)
            _update(inst, cleared)
            _REFRESH_RETRY_AT.pop(inst, None)
        return self.get_status(instance_id=inst)

    def html(self, cfg: Any = None) -> str:
        return html()


PROVIDER = MyAnimeListAuth()


def html() -> str:
    return '''<div class="section" id="sec-myanimelist">
  <div class="head" data-toggle-section="sec-myanimelist"><span class="chev"></span><strong>MyAnimeList</strong></div>
  <div class="body"><div class="cw-panel"><div class="cw-meta-provider-panel active" data-provider="myanimelist">
    <div class="cw-panel-head"><div><div class="cw-panel-title">MyAnimeList</div><div class="muted">Connect your MyAnimeList account.</div></div></div>
    <div class="cw-subtiles"><button type="button" class="cw-subtile active" data-sub="auth">Authentication</button></div>
    <div class="cw-subpanels"><div class="cw-subpanel active" data-sub="auth">
      <p class="muted">Account connection preview. Sync and scrobbling are not available yet.</p>
      <div id="myanimelist_oauth_panel" class="hidden">
        <p>Sign in to MyAnimeList and approve CrossWatch. This window will update automatically.</p>
        <a id="myanimelist_approval_link" target="_blank" rel="noopener noreferrer">Open MyAnimeList approval page</a>
        <div id="myanimelist_login_timer" class="muted" role="status"></div>
      </div>
      <div class="mal-actions">
        <button id="myanimelist_oauth_start" class="btn" type="button">Connect MyAnimeList</button>
        <button id="myanimelist_oauth_cancel" class="btn hidden" type="button">Cancel</button>
        <button id="myanimelist_disconnect" class="btn danger" type="button">Delete</button>
        <div id="myanimelist_msg" class="msg hidden" role="status" aria-live="polite"></div>
      </div>
    </div></div>
  </div></div></div>
</div>'''
