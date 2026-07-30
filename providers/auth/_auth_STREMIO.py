# providers/auth/_auth_STREMIO.py
# CrossWatch - Stremio Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any, cast

import requests

from cw_platform.config_base import _decrypt_secret
from cw_platform.provider_instances import ensure_instance_block, get_provider_block, normalize_instance_id

from ._auth_base import AuthManifest, AuthProvider, AuthStatus

__VERSION__ = "0.1"

API_BASE = "https://api.strem.io/api"
UA = "CrossWatch/1.0 (Stremio)"


class StremioAuthError(RuntimeError):
    def __init__(self, message: str, *, reason: str = "error", detail: Any = None, status_code: int | None = None, endpoint: str | None = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.detail = detail
        self.status_code = status_code
        self.endpoint = endpoint


def _block(cfg: Mapping[str, Any], instance_id: Any = None) -> dict[str, Any]:
    return get_provider_block(cfg or {}, "stremio", instance_id)


def auth_key_value(block: Mapping[str, Any] | None) -> str:
    raw = (block or {}).get("auth_key") or (block or {}).get("authKey") or ""
    try:
        return str(_decrypt_secret(raw) or "").strip()
    except Exception:
        return ""


def is_configured(block: Mapping[str, Any] | None) -> bool:
    return bool(auth_key_value(block))


def status_for_block(block: Mapping[str, Any] | None) -> dict[str, Any]:
    return {
        "connected": is_configured(block),
        "authenticated": is_configured(block),
        "auth_key_configured": is_configured(block),
        "experimental": True,
    }


def clear_auth(block: MutableMapping[str, Any]) -> None:
    block["auth_key"] = ""
    block.pop("authKey", None)


def normalize_auth_method(value: Any, block: Mapping[str, Any] | None = None) -> str:
    return "credentials"


def active_method(block: Mapping[str, Any] | None) -> str:
    return "credentials"


def set_active_method(block: MutableMapping[str, Any], method: str) -> str:
    return "credentials"


def clear_oauth(block: MutableMapping[str, Any]) -> None:
    clear_auth(block)


def start_device_code(cfg: dict[str, Any] | None, **_: Any) -> dict[str, Any]:
    return {"ok": False, "status": "unsupported"}


def poll_device_code(cfg: dict[str, Any] | None, **_: Any) -> dict[str, Any]:
    return {"ok": False, "status": "unsupported"}


def refresh_token(cfg: dict[str, Any] | None = None, **_: Any) -> dict[str, Any]:
    return {"ok": True, "status": "unsupported"}


def _json_response(resp: requests.Response) -> Any:
    try:
        if hasattr(resp, "text") and not str(getattr(resp, "text", "") or "").strip():
            return {}
        return resp.json()
    except Exception as exc:
        raise StremioAuthError("Stremio returned invalid JSON", reason="invalid_response") from exc


def _error_reason(value: Any) -> str:
    text = str(value or "").strip().lower()
    if any(word in text for word in ("auth", "login", "token", "session", "unauthor", "credential")):
        return "invalid_credentials"
    return "request_failed"


def _post_api(path: str, payload: Mapping[str, Any], *, session: requests.Session | None = None, timeout: float = 20.0) -> Any:
    client = session or requests.Session()
    endpoint = str(path or "").lstrip("/")
    url = f"{API_BASE}/{str(path or '').lstrip('/')}"
    try:
        resp = client.post(
            url,
            json=dict(payload or {}),
            headers={"Accept": "application/json", "Content-Type": "application/json", "User-Agent": UA},
            timeout=timeout,
        )
    except requests.Timeout as exc:
        raise StremioAuthError("Stremio API timed out", reason="unreachable", endpoint=endpoint) from exc
    except requests.RequestException as exc:
        raise StremioAuthError("Stremio API is unreachable", reason="unreachable", endpoint=endpoint) from exc

    if resp.status_code in (401, 403):
        raise StremioAuthError("Stremio rejected the credentials", reason="invalid_credentials", status_code=resp.status_code, endpoint=endpoint)
    if resp.status_code >= 500 or resp.status_code == 0:
        raise StremioAuthError("Stremio API is unavailable", reason="service_unavailable", status_code=resp.status_code, endpoint=endpoint)
    if resp.status_code >= 400:
        raise StremioAuthError("Stremio request failed", reason="request_failed", status_code=resp.status_code, endpoint=endpoint)

    data = _json_response(resp)
    if isinstance(data, Mapping) and data.get("error"):
        detail = data.get("error")
        raise StremioAuthError("Stremio request failed", reason=_error_reason(detail), detail=detail, status_code=resp.status_code, endpoint=endpoint)
    return data


def request_with_auth(
    session: requests.Session,
    method: str,
    url: str,
    *,
    cfg: Mapping[str, Any] | None,
    instance_id: Any = None,
    timeout: float = 10.0,
    **kwargs: Any,
) -> requests.Response:
    key = auth_key_value(_block(cfg or {}, instance_id))
    if not key:
        raise StremioAuthError("Missing Stremio auth key", reason="missing_auth_key")
    req_kwargs = dict(kwargs)
    if str(method or "").upper() == "POST":
        payload = dict(req_kwargs.get("json") or {})
        payload.setdefault("authKey", key)
        req_kwargs["json"] = payload
    return session.request(method.upper(), url, timeout=timeout, **req_kwargs)


def login(email: str, password: str, *, session: requests.Session | None = None, timeout: float = 20.0) -> dict[str, Any]:
    mail = str(email or "").strip()
    secret = str(password or "")
    if not mail or not secret:
        raise StremioAuthError("Missing Stremio email or password", reason="missing_credentials")
    data = _post_api(
        "login",
        {"type": "Login", "email": mail, "password": secret, "facebook": False},
        session=session,
        timeout=timeout,
    )
    result = data.get("result") if isinstance(data, Mapping) else None
    if not isinstance(result, Mapping):
        raise StremioAuthError("Stremio login returned an invalid response", reason="invalid_response")
    auth_key = str(result.get("authKey") or "").strip()
    if not auth_key:
        raise StremioAuthError("Stremio login did not return an auth key", reason="invalid_response")
    return {"auth_key": auth_key, "user": dict(result.get("user") or {}) if isinstance(result.get("user"), Mapping) else {}}


class StremioClient:
    def __init__(self, cfg: Mapping[str, Any] | None, *, instance_id: Any = None, session: requests.Session | None = None):
        self.cfg = cfg or {}
        self.instance_id = normalize_instance_id(instance_id)
        self.block = _block(self.cfg, self.instance_id)
        self.session = session or requests.Session()

    def auth_key(self) -> str:
        key = auth_key_value(self.block)
        if not key:
            raise StremioAuthError("Missing Stremio auth key", reason="missing_auth_key")
        return key

    def request_json(self, path: str, payload: Mapping[str, Any] | None = None, *, timeout: float = 20.0) -> Any:
        body = dict(payload or {})
        body["authKey"] = self.auth_key()
        return _post_api(path, body, session=self.session, timeout=timeout)

    def validate(self) -> bool:
        data = self.request_json("datastoreMeta", {"collection": "libraryItem"}, timeout=12.0)
        return isinstance(data, Mapping)


class StremioAuth(AuthProvider):
    name = "STREMIO"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="STREMIO",
            label="Stremio",
            flow="credentials",
            fields=[
                {"key": "stremio.email", "label": "Email", "type": "text", "required": True},
                {"key": "stremio.password", "label": "Password", "type": "password", "required": True},
            ],
            actions={"start": True, "finish": False, "refresh": False, "disconnect": True},
            verify_url="https://www.stremio.com/login",
            notes="Experimental internal Stremio account API. CrossWatch stores only the returned auth key.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"features": {}}

    def get_status(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        connected = is_configured(_block(cfg, inst))
        label = "Stremio" if inst == "default" else f"Stremio ({inst})"
        return AuthStatus(connected, label, None, None, None, {"experimental": True, "auth_key_configured": connected})

    def connect(
        self,
        cfg: MutableMapping[str, Any],
        *,
        email: str,
        password: str,
        instance_id: Any = None,
    ) -> dict[str, Any]:
        inst = normalize_instance_id(instance_id)
        block = ensure_instance_block(cast(dict[str, Any], cfg), "stremio", inst)
        result = login(email, password)
        insts = block.get("instances") if inst == "default" and isinstance(block.get("instances"), dict) else None
        block.clear()
        block["auth_key"] = str(result["auth_key"])
        if isinstance(insts, dict):
            block["instances"] = insts
        return {"ok": True, "instance": inst}

    def start(
        self,
        cfg: MutableMapping[str, Any],
        redirect_uri: str | None = None,
        *,
        instance_id: Any = None,
    ) -> dict[str, Any]:
        block = _block(cfg, instance_id)
        return self.connect(cfg, email=str(block.get("email") or ""), password=str(block.get("password") or ""), instance_id=instance_id)

    def finish(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None, **payload: Any) -> AuthStatus:
        return self.get_status(cfg, instance_id=instance_id)

    def refresh(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        return self.get_status(cfg, instance_id=instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        block = ensure_instance_block(cast(dict[str, Any], cfg), "stremio", normalize_instance_id(instance_id))
        clear_auth(block)
        return self.get_status(cfg, instance_id=instance_id)


def html() -> str:
    return r"""<div class="section" id="sec-stremio">
  <style>
    #sec-stremio .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    #sec-stremio .inline{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:12px}
    #sec-stremio .msg{margin-left:auto;padding:8px 12px;border-radius:12px;border:1px solid rgba(0,255,170,.18);background:rgba(0,255,170,.08);color:#b9ffd7;font-weight:600}
    #sec-stremio .msg.warn{border-color:rgba(255,210,0,.18);background:rgba(255,210,0,.08);color:#ffe9a6}
    #sec-stremio .msg.hidden{display:none}
    #sec-stremio #stremio_connect{background:linear-gradient(135deg,#722cfe,#16b6ff);border-color:rgba(114,44,254,.45);box-shadow:0 0 14px rgba(114,44,254,.32);color:#fff}
  </style>
  <div class="head" data-toggle-section="sec-stremio">
    <span class="chev"></span><strong>Stremio</strong>
  </div>
  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="stremio">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">Stremio <span class="badge feature-disabled">Experimental</span></div>
            <div class="muted">Connect with a Stremio account. CrossWatch stores only the returned auth key.</div>
          </div>
        </div>
        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
        </div>
        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="grid2">
              <div>
                <label for="stremio_email">Email</label>
                <input id="stremio_email" name="stremio_email" autocomplete="username" placeholder="user@example.com">
              </div>
              <div>
                <label for="stremio_password">Password</label>
                <input id="stremio_password" name="stremio_password" type="password" autocomplete="current-password">
              </div>
            </div>
            <div class="inline">
              <button id="stremio_connect" class="btn" type="button">Connect Stremio</button>
              <button id="stremio_disconnect" type="button" hidden aria-hidden="true" tabindex="-1"></button>
              <div id="stremio_msg" class="msg ok hidden" role="status" aria-live="polite"></div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
"""


PROVIDER = StremioAuth()
__all__ = [
    "PROVIDER",
    "StremioAuth",
    "StremioAuthError",
    "StremioClient",
    "active_method",
    "auth_key_value",
    "clear_auth",
    "clear_oauth",
    "html",
    "is_configured",
    "login",
    "normalize_auth_method",
    "poll_device_code",
    "refresh_token",
    "request_with_auth",
    "set_active_method",
    "start_device_code",
    "status_for_block",
    "__VERSION__",
]
