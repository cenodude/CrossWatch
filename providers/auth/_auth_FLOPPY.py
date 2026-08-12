# providers/auth/_auth_FLOPPY.py
# CrossWatch - Floppy Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

import requests

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, get_provider_block, normalize_instance_id

__VERSION__ = "0.1"
UA = "CrossWatch/1.0"


class FloppyAuthError(RuntimeError):
    def __init__(self, message: str, *, reason: str = "request_failed", status_code: int | None = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.status_code = status_code


def _load_config() -> dict[str, Any]:
    try:
        return dict(load_config() or {})
    except Exception:
        return {}


def normalize_server_url(value: Any) -> str:
    server = str(value or "").strip().rstrip("/")
    if server and not server.startswith(("http://", "https://")):
        server = "http://" + server
    return server.rstrip("/")


def provider_block(cfg: Mapping[str, Any] | None, instance_id: Any = None) -> dict[str, Any]:
    block = get_provider_block(cfg or {}, "floppy", instance_id)
    if block:
        return block
    base = (cfg or {}).get("floppy") if isinstance(cfg, Mapping) else None
    if not isinstance(base, Mapping):
        return {}
    inst = normalize_instance_id(instance_id)
    if inst == "default" or "instances" not in base:
        return dict(base)
    return {}


def _block(cfg: Mapping[str, Any], instance_id: Any = None) -> dict[str, Any]:
    return provider_block(cfg, instance_id)


def is_configured(block: Mapping[str, Any] | None) -> bool:
    return bool(str((block or {}).get("server_url") or "").strip() and str((block or {}).get("api_token") or "").strip())


def status_for_block(block: Mapping[str, Any] | None, *, instance_id: Any = None) -> AuthStatus:
    inst = normalize_instance_id(instance_id)
    label = "Floppy" if inst == "default" else f"Floppy ({inst})"
    return AuthStatus(connected=is_configured(block), label=label)


class FloppyClient:
    def __init__(self, server_url: str, api_token: str, *, verify_ssl: bool = False, timeout: float = 12.0, session: requests.Session | None = None) -> None:
        self.server_url = normalize_server_url(server_url)
        self.api_token = str(api_token or "").strip()
        self.verify_ssl = bool(verify_ssl)
        self.timeout = float(timeout or 12.0)
        self.session = session or requests.Session()

    @property
    def api_base(self) -> str:
        return f"{self.server_url}/api/v1"

    def request_json(self, path: str, *, method: str = "GET", **kwargs: Any) -> Any:
        if not self.server_url:
            raise FloppyAuthError("Missing Floppy server URL", reason="server_url_required")
        if not self.api_token:
            raise FloppyAuthError("Missing Floppy API token", reason="api_token_required")
        url = f"{self.api_base}/{str(path or '').strip('/')}"
        headers = dict(kwargs.pop("headers", {}) or {})
        headers.update({"Accept": "application/json", "Authorization": f"Bearer {self.api_token}", "User-Agent": UA})
        try:
            response = self.session.request(method, url, headers=headers, timeout=self.timeout, verify=self.verify_ssl, **kwargs)
        except requests.Timeout as exc:
            raise FloppyAuthError("Floppy validation timed out", reason="validation_timeout") from exc
        except requests.exceptions.SSLError as exc:
            raise FloppyAuthError("Floppy SSL validation failed", reason="invalid_ssl") from exc
        except requests.ConnectionError as exc:
            raise FloppyAuthError("Floppy server is unreachable", reason="unreachable") from exc
        except requests.RequestException as exc:
            raise FloppyAuthError("Floppy request failed", reason="validation_failed") from exc
        if response.status_code in (401, 403):
            raise FloppyAuthError("Floppy rejected the API token", reason="invalid_api_token", status_code=response.status_code)
        if response.status_code >= 500:
            raise FloppyAuthError("Floppy server error", reason="server_error", status_code=response.status_code)
        if response.status_code >= 400:
            raise FloppyAuthError("Floppy validation failed", reason=f"validation_http_{int(response.status_code)}", status_code=response.status_code)
        try:
            return response.json()
        except ValueError as exc:
            raise FloppyAuthError("Floppy returned a non JSON response", reason="validation_bad_response", status_code=response.status_code) from exc


def validate_credentials(server_url: str, api_token: str, *, timeout: float = 12.0, verify_ssl: bool = False, session: requests.Session | None = None) -> tuple[bool, str]:
    try:
        FloppyClient(server_url, api_token, verify_ssl=verify_ssl, timeout=timeout, session=session).request_json("lists/", params={"limit": 1})
        return True, ""
    except FloppyAuthError as exc:
        return False, str(exc.reason or "validation_failed")


class FloppyAuth(AuthProvider):
    name = "FLOPPY"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="FLOPPY",
            label="Floppy",
            flow="api_key",
            fields=[
                {"key": "floppy.server_url", "label": "Server URL", "type": "text", "required": True, "placeholder": "http://localhost:8000"},
                {"key": "floppy.api_token", "label": "API token", "type": "password", "required": True, "placeholder": "********"},
                {"key": "floppy.verify_ssl", "label": "Verify SSL", "type": "bool", "required": False, "default": False},
            ],
            actions={"start": False, "finish": True, "refresh": False, "disconnect": True},
    notes="Create the API token in Floppy > Settings > Integrations.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"watchlist": True, "ratings": True, "history": True, "progress": False, "playlists": False}

    def get_status(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        return status_for_block(_block(cfg, normalize_instance_id(instance_id)), instance_id=instance_id)

    def start(self, cfg: MutableMapping[str, Any] | None = None, *, redirect_uri: str | None = None, instance_id: Any = None) -> dict[str, Any]:
        return {}

    def finish(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None, **payload: Any) -> AuthStatus:
        cfgd = dict(cfg or _load_config() or {})
        inst = normalize_instance_id(instance_id)
        server_url = normalize_server_url(payload.get("server_url") or payload.get("floppy.server_url"))
        api_token = str(payload.get("api_token") or payload.get("floppy.api_token") or "").strip()
        verify_ssl = bool(payload.get("verify_ssl", payload.get("floppy.verify_ssl", False)))
        ok, reason = validate_credentials(server_url, api_token, verify_ssl=verify_ssl)
        if not ok:
            raise ValueError(reason)
        f = ensure_instance_block(cfgd, "floppy", inst)
        f["server_url"] = server_url
        f["api_token"] = api_token
        f["verify_ssl"] = verify_ssl
        save_config(cfgd)
        return self.get_status(cfgd, instance_id=inst)

    def refresh(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        return self.get_status(cfg, instance_id=instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfgd = dict(cfg or _load_config() or {})
        inst = normalize_instance_id(instance_id)
        f = ensure_instance_block(cfgd, "floppy", inst)
        f["server_url"] = ""
        f["api_token"] = ""
        save_config(cfgd)
        return self.get_status(cfgd, instance_id=inst)


def html() -> str:
    return r"""<div class="section" id="sec-floppy">
  <style>
    #sec-floppy .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    #sec-floppy .inline{display:flex;gap:8px;align-items:center}
    #sec-floppy .verify{display:flex;gap:8px;align-items:center;margin-top:10px}
    #sec-floppy .msg{margin-left:auto;padding:8px 12px;border-radius:12px;border:1px solid rgba(0,255,170,.18);background:rgba(0,255,170,.08);color:#b9ffd7;font-weight:600}
    #sec-floppy .msg.warn{border-color:rgba(255,210,0,.18);background:rgba(255,210,0,.08);color:#ffe9a6}
    #sec-floppy .msg.hidden{display:none}
    #sec-floppy .btn.danger{background:#a8182e;border-color:rgba(255,107,107,.4)}
    #sec-floppy #floppy_connect{background:linear-gradient(135deg,#f5651e,#ffcd16,#04b5dc);border-color:rgba(245,101,30,.45);box-shadow:0 0 14px rgba(4,181,220,.24);color:#fff}
  </style>
  <div class="head" data-toggle-section="sec-floppy">
    <span class="chev"></span><strong>Floppy</strong>
  </div>
  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="floppy">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">Floppy <span class="cw-exp-badge">Experimental</span></div>
            <div class="muted">Connect your Floppy server and API token.</div>
          </div>
        </div>
        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
        </div>
        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="cw-auth-journey" style="--cw-auth-c1:245,101,30;--cw-auth-c2:4,181,220;--cw-auth-logo:url('/assets/img/FLOPPY.png')">
              <div class="cw-auth-journey-text">
                <div class="cw-auth-journey-title">Connect to Floppy</div>
              <div class="cw-auth-journey-copy">Create an API token in Floppy &rsaquo; Settings &rsaquo; Integrations, then enter your server URL and token here.</div>
              </div>
            </div>
            <div class="grid2">
              <div>
                <label for="floppy_server">Server URL</label>
                <input id="floppy_server" name="floppy_server" type="text" placeholder="http://localhost:8000" autocomplete="off" spellcheck="false" autocapitalize="off">
              </div>
              <div>
                <label for="floppy_token">API token</label>
                <input id="floppy_token" name="floppy_token" type="password" placeholder="********" autocomplete="off" spellcheck="false" autocapitalize="off" data-lpignore="true" data-1p-ignore="true" data-bwignore="true">
              </div>
            </div>
            <label class="verify"><input id="floppy_verify_ssl" type="checkbox"> Verify SSL</label>
            <div id="floppy_actions_row" class="inline" style="margin-top:12px">
              <button id="floppy_connect" class="btn" type="button">Connect Floppy</button>
              <button id="floppy_disconnect" type="button" hidden aria-hidden="true" tabindex="-1"></button>
              <div id="floppy_msg" class="msg ok hidden" role="status" aria-live="polite"></div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
"""


PROVIDER = FloppyAuth()
