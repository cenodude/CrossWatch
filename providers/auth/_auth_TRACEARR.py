# providers/auth/_auth_TRACEARR.py
# CrossWatch - Tracearr Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

import requests

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, normalize_instance_id, resolve_provider_block

__VERSION__ = "0.1"
UA = f"CrossWatch/{__VERSION__} (TracearrAuth)"
API_PREFIX = "/api/v2/public"


def _load_config() -> dict[str, Any]:
    try:
        return dict(load_config() or {})
    except Exception:
        return {}


def _block(cfg: Mapping[str, Any], instance_id: Any = None) -> dict[str, Any]:
    return resolve_provider_block(cfg or {}, "tracearr", instance_id)


def normalize_server_url(value: Any) -> str:
    server = str(value or "").strip().rstrip("/")
    if server and not server.startswith(("http://", "https://")):
        server = "http://" + server
    return server


def is_configured(block: Mapping[str, Any] | None) -> bool:
    b = block or {}
    return bool(normalize_server_url(b.get("server_url") or b.get("server")) and str(b.get("api_key") or "").strip())


def _get(server_url: str, api_key: str, path: str, params: Mapping[str, Any], *, timeout: float, verify_ssl: bool) -> requests.Response:
    return requests.get(
        f"{normalize_server_url(server_url)}{API_PREFIX}{path}",
        params=dict(params),
        headers={"Accept": "application/json", "User-Agent": UA, "Authorization": f"Bearer {str(api_key or '').strip()}"},
        timeout=timeout,
        verify=bool(verify_ssl),
    )


def _error_for_status(code: int) -> str:
    if code in (401, 403):
        return "invalid_api_key"
    if code == 404:
        return "api_v2_unavailable"
    return f"validation_http_{int(code)}"


def validate_credentials(server_url: str, api_key: str, *, timeout: float = 12.0, verify_ssl: bool = True) -> tuple[bool, str]:
    if not normalize_server_url(server_url):
        return False, "server_url_required"
    if not str(api_key or "").strip():
        return False, "api_key_required"
    try:
        r = _get(server_url, api_key, "/users", {"pageSize": 1}, timeout=timeout, verify_ssl=verify_ssl)
    except requests.Timeout:
        return False, "validation_timeout"
    except requests.RequestException:
        return False, "validation_failed"
    if r.status_code >= 400:
        return False, _error_for_status(r.status_code)
    try:
        data = r.json() or {}
    except ValueError:
        return False, "validation_bad_response"
    if isinstance(data, dict) and isinstance(data.get("data"), list):
        return True, ""
    return False, "validation_bad_response"


def list_users(server_url: str, api_key: str, *, timeout: float = 12.0, verify_ssl: bool = True, max_pages: int = 20) -> tuple[list[dict[str, Any]], str]:
    users: list[dict[str, Any]] = []
    cursor: str | None = None
    for _ in range(max(1, int(max_pages))):
        params: dict[str, Any] = {"pageSize": 100}
        if cursor:
            params["cursor"] = cursor
        try:
            r = _get(server_url, api_key, "/users", params, timeout=timeout, verify_ssl=verify_ssl)
        except requests.Timeout:
            return users, "validation_timeout"
        except requests.RequestException:
            return users, "validation_failed"
        if r.status_code >= 400:
            return users, _error_for_status(r.status_code)
        try:
            data = r.json() or {}
        except ValueError:
            return users, "validation_bad_response"
        rows = data.get("data") if isinstance(data, dict) else None
        if not isinstance(rows, list):
            return users, "validation_bad_response"
        for row in rows:
            if not isinstance(row, dict) or not str(row.get("id") or "").strip():
                continue
            servers = sorted({
                str(a.get("server_type") or "").strip().lower()
                for a in (row.get("accounts") or [])
                if isinstance(a, dict) and str(a.get("server_type") or "").strip()
            })
            users.append({"id": str(row["id"]), "username": str(row.get("username") or row["id"]), "servers": servers})
        meta = data.get("meta") if isinstance(data, dict) else None
        cursor = str((meta or {}).get("nextCursor") or "").strip() if isinstance(meta, dict) else ""
        if not cursor:
            break
    users.sort(key=lambda u: str(u.get("username") or "").lower())
    return users, ""


class TracearrAuth(AuthProvider):
    name = "TRACEARR"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="TRACEARR",
            label="Tracearr",
            flow="api_keys",
            fields=[
                {
                    "key": "tracearr.server_url",
                    "label": "Server URL",
                    "type": "text",
                    "required": True,
                    "placeholder": "http://localhost:3000",
                },
                {
                    "key": "tracearr.api_key",
                    "label": "API Key",
                    "type": "password",
                    "required": True,
                    "placeholder": "trr_pub_...",
                },
                {
                    "key": "tracearr.verify_ssl",
                    "label": "Verify SSL",
                    "type": "bool",
                    "required": False,
                    "placeholder": "",
                },
                {
                    "key": "tracearr.history.user_id",
                    "label": "History User ID (optional)",
                    "type": "text",
                    "required": False,
                    "placeholder": "",
                },
            ],
            actions={"start": False, "finish": True, "refresh": False, "disconnect": True},
            notes="API key is in Tracearr > Settings > API. Requires Tracearr 2.0 or later.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"watchlist": False, "ratings": False, "history": True}

    def get_status(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        ok = is_configured(_block(cfg, inst))
        label = "Tracearr" if inst == "default" else f"Tracearr ({inst})"
        return AuthStatus(connected=ok, label=label)

    def start(self, cfg: MutableMapping[str, Any] | None = None, *, redirect_uri: str | None = None, instance_id: Any = None) -> dict[str, Any]:
        return {}

    def finish(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None, **payload: Any) -> AuthStatus:
        cfgd = dict(cfg or _load_config() or {})
        inst = normalize_instance_id(instance_id)
        t = ensure_instance_block(cfgd, "tracearr", inst)
        server_url = normalize_server_url(payload.get("server_url") or payload.get("tracearr.server_url"))
        api_key = str(payload.get("api_key") or payload.get("tracearr.api_key") or "").strip()
        verify_ssl = bool(payload.get("verify_ssl", payload.get("tracearr.verify_ssl", True)))
        ok, reason = validate_credentials(server_url, api_key, verify_ssl=verify_ssl)
        if not ok:
            raise ValueError(reason)
        t["server_url"] = server_url
        t["api_key"] = api_key
        if "verify_ssl" in payload or "tracearr.verify_ssl" in payload:
            t["verify_ssl"] = verify_ssl

        user_id = str(payload.get("user_id") or payload.get("tracearr.history.user_id") or "").strip()
        if user_id:
            t.setdefault("history", {})["user_id"] = user_id

        save_config(cfgd)
        return self.get_status(cfgd, instance_id=inst)

    def refresh(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        return self.get_status(cfg, instance_id=instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfgd = dict(cfg or _load_config() or {})
        inst = normalize_instance_id(instance_id)
        t = ensure_instance_block(cfgd, "tracearr", inst)
        t["server_url"] = ""
        t["api_key"] = ""
        save_config(cfgd)
        return self.get_status(cfgd, instance_id=inst)


def html() -> str:
    return r"""<div class="section" id="sec-tracearr">
  <style>
    #sec-tracearr .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    #sec-tracearr .inline{display:flex;gap:8px;align-items:center}
    #sec-tracearr .msg{margin-left:auto;padding:8px 12px;border-radius:12px;border:1px solid rgba(0,255,170,.18);background:rgba(0,255,170,.08);color:#b9ffd7;font-weight:600}
    #sec-tracearr .msg.warn{border-color:rgba(255,210,0,.18);background:rgba(255,210,0,.08);color:#ffe9a6}
    #sec-tracearr .msg.hidden{display:none}
    #sec-tracearr #tracearr_hint{margin-left:0}
    #sec-tracearr #tracearr_user_id{max-width:240px;width:240px}
    #sec-tracearr .btn.danger{ background:#a8182e; border-color:rgba(255,107,107,.4) }
    #sec-tracearr #tracearr_save{
      background: linear-gradient(135deg,#18d1e7,#0e8fa6);
      border-color: rgba(24,209,231,.45);
      box-shadow: 0 0 14px rgba(24,209,231,.35);
      color: #fff;
    }
    #sec-tracearr #tracearr_save:hover{filter:brightness(1.06);box-shadow:0 0 18px rgba(24,209,231,.5)}
    #sec-tracearr .cw-help{
      display:inline-flex;
      align-items:center;
      justify-content:center;
      width:18px;
      height:18px;
      margin-left:6px;
      border-radius:999px;
      border:1px solid rgba(255,255,255,.22);
      color:rgba(255,255,255,.78);
      font-size:12px;
      line-height:1;
      cursor:help;
    }
  </style>

  <div class="head" data-toggle-section="sec-tracearr">
    <span class="chev"></span><strong>Tracearr</strong>
  </div>

  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="tracearr">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">Tracearr</div>
            <div class="muted">Connect your Tracearr server and API key.</div>
          </div>
        </div>

        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
        </div>

        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="cw-auth-journey" style="--cw-auth-c1:24,209,231;--cw-auth-c2:14,143,166;--cw-auth-logo:url('/assets/img/TRACEARR.png')">
              <div class="cw-auth-journey-text">
                <div class="cw-auth-journey-title">Connect to Tracearr</div>
                <div class="cw-auth-journey-copy">Enter your Tracearr server URL and public API key (Tracearr &rsaquo; Settings &rsaquo; API). CrossWatch reads your Tracearr watch history. Requires Tracearr 2.0 or later.</div>
              </div>
            </div>

            <div class="grid2">
              <div>
                <label for="tracearr_server">Server URL</label>
                <input id="tracearr_server" name="tracearr_server" type="text" placeholder="http://localhost:3000" autocomplete="off" spellcheck="false" autocapitalize="off" />
              </div>
              <div>
                <label for="tracearr_key">API Key</label>
                <input id="tracearr_key" name="tracearr_key" type="password" placeholder="trr_pub_..." autocomplete="off" spellcheck="false" autocapitalize="off" data-lpignore="true" data-1p-ignore="true" data-bwignore="true" />
              </div>
            </div>

            <div style="margin-top:10px;max-width:240px">
              <label for="tracearr_user_id">User (optional)<span class="cw-help" title="If this is set to All users, CrossWatch imports history for every Tracearr user. Normally you only want one user.">?</span></label>
              <select id="tracearr_user_id" name="tracearr_user_id">
                <option value="">All users</option>
              </select>
            </div>

            <div id="tracearr_hint" class="msg warn" style="margin-top:10px">
              API key: Tracearr > Settings > API.
            </div>

            <div id="tracearr_actions_row" class="inline" style="margin-top:12px">
              <button id="tracearr_save" class="btn">Connect Tracearr</button>
              <button id="tracearr_disconnect" class="btn danger">Delete</button>
              <div id="tracearr_msg" class="msg ok hidden" aria-live="polite"></div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
"""
