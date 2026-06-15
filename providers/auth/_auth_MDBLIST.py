# providers/auth/_auth_MDBLIST.py
# CrossWatch - MDBList Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

import requests

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, get_provider_block, normalize_instance_id
from providers.sync.mdblist import _auth as mdblist_auth

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


API_BASE = "https://api.mdblist.com"
UA = "CrossWatch/1.0"
HTTP_TIMEOUT = 10
__VERSION__ = "2.1.0"


def _load_config() -> dict[str, Any]:
    try:
        return dict(load_config() or {})
    except Exception:
        return {}


def _block(cfg: Mapping[str, Any], instance_id: Any = None) -> dict[str, Any]:
    return get_provider_block(cfg or {}, "mdblist", instance_id)


def _get(cfg: Mapping[str, Any], path: str, *, instance_id: Any = None, timeout: int = HTTP_TIMEOUT) -> tuple[int, dict[str, Any]]:
    if not mdblist_auth.is_configured(_block(cfg, instance_id)):
        return 0, {}
    try:
        session = requests.Session()
        r = mdblist_auth.request_with_auth(
            session,
            "GET",
            f"{API_BASE}{path}",
            cfg=cfg,
            instance_id=instance_id,
            timeout=timeout,
            max_retries=1,
            headers={"Accept": "application/json", "User-Agent": UA},
        )
    except Exception:
        return 0, {}
    try:
        j: dict[str, Any] = r.json()
    except Exception:
        j = {}
    return r.status_code, j


class MDBListAuth(AuthProvider):
    name = "MDBLIST"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="MDBLIST",
            label="MDBList",
            flow="device_code",
            fields=[
                {
                    "key": "mdblist.api_key",
                    "label": "API Key",
                    "type": "password",
                    "required": False,
                    "placeholder": "********",
                },
            ],
            actions={"start": True, "finish": True, "refresh": True, "disconnect": True},
            notes="Device Code is preferred. API key remains available as a legacy option.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"watchlist": True, "ratings": True, "history": True}

    def get_status(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        status = mdblist_auth.status_for_block(_block(cfg, inst))
        label = "MDBList" if inst == "default" else f"MDBList ({inst})"
        return AuthStatus(
            connected=bool(status.get("connected")),
            label=label,
            user=str(status.get("username") or "") or None,
            expires_at=int(status.get("expires_at") or 0) or None,
        )

    def start(self, cfg: MutableMapping[str, Any] | None = None, *, redirect_uri: str | None = None, instance_id: Any = None) -> dict[str, Any]:
        cfgd = dict(cfg or _load_config() or {})
        return mdblist_auth.start_device_code(cfgd, instance_id=instance_id)

    def finish(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None, **payload: Any) -> AuthStatus:
        cfgd = dict(cfg or _load_config() or {})
        b = ensure_instance_block(cfgd, "mdblist", instance_id)
        method = mdblist_auth.normalize_auth_method(payload.get("auth_method") or b.get("auth_method"), b)
        if method == "api_key":
            b["api_key"] = str(payload.get("api_key") or payload.get("mdblist.api_key") or "").strip()
            mdblist_auth.set_active_method(b, "api_key")
        else:
            b.pop("client_id", None)
            mdblist_auth.set_active_method(b, "device_code")
        save_config(cfgd)
        log(f"MDBList auth saved (method={method}, instance={normalize_instance_id(instance_id)}).", module="AUTH")
        return self.get_status(cfgd, instance_id=instance_id)

    def refresh(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        try:
            mdblist_auth.refresh_token(dict(cfg or _load_config() or {}), instance_id=instance_id)
        except Exception:
            pass
        return self.get_status(cfg, instance_id=instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfgd = dict(cfg or _load_config() or {})
        b = ensure_instance_block(cfgd, "mdblist", instance_id)
        b["api_key"] = ""
        mdblist_auth.clear_oauth(b)
        save_config(cfgd)
        log(f"MDBList disconnected (instance={normalize_instance_id(instance_id)}).", module="AUTH")
        return self.get_status(cfgd, instance_id=instance_id)


PROVIDER = MDBListAuth()


def html() -> str:
    return r"""<div class="section" id="sec-mdblist">
  <style>
    #sec-mdblist .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    #sec-mdblist .inline{display:flex;gap:8px;align-items:center}
    #sec-mdblist .muted{opacity:.7;font-size:.92em}
    #sec-mdblist .inline .msg{margin-left:auto;padding:8px 12px;border-radius:12px;border:1px solid rgba(0,255,170,.18);background:rgba(0,255,170,.08);color:#b9ffd7;font-weight:600}
    #sec-mdblist .inline .msg.warn{border-color:rgba(255,210,0,.18);background:rgba(255,210,0,.08);color:#ffe9a6}
    #sec-mdblist .inline .msg.hidden{display:none}
    #sec-mdblist .btn.danger{ background:#a8182e; border-color:rgba(255,107,107,.4) }
    #sec-mdblist .btn.danger:hover{ filter:brightness(1.08) }
    #sec-mdblist .mdblist-action-row{display:flex;gap:12px;align-items:stretch}
    #sec-mdblist .mdblist-action-row input{flex:1 1 auto;min-width:0}
    #sec-mdblist .mdblist-action-row .btn{flex:0 0 auto;white-space:nowrap;min-height:44px}
    #sec-mdblist #mdblist_save,#sec-mdblist #mdblist_device_start{
      background: linear-gradient(135deg,#00e084,#2ea859);
      border-color: rgba(0,224,132,.45);
      box-shadow: 0 0 14px rgba(0,224,132,.35);
      color: #fff;
      min-width: 146px;
    }
    #sec-mdblist #mdblist_save:hover,#sec-mdblist #mdblist_device_start:hover{
      filter: brightness(1.06);
      box-shadow: 0 0 18px rgba(0,224,132,.5);
    }
  </style>

  <div class="head" data-toggle-section="sec-mdblist">
    <span class="chev">&#9654;</span><strong>MDBList</strong>
  </div>

  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="mdblist">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">MDBList</div>
            <div class="muted">Connect with Device Code or use a legacy API key.</div>
          </div>
        </div>

        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
        </div>

        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="grid2">
              <div>
                <label for="mdblist_auth_method">Authentication Method</label>
                <select id="mdblist_auth_method" name="mdblist_auth_method">
                  <option value="device_code">Device Code</option>
                  <option value="api_key">API Key</option>
                </select>
              </div>
            </div>

            <div id="mdblist_device_panel" style="margin-top:10px">
              <div class="sep"></div>
              <div>
                <div class="field-label">Link code (PIN)</div>
                <div class="mdblist-action-row">
                  <input id="mdblist_device_code" readonly placeholder="" />
                  <button id="mdblist_copy_code" class="btn copy">Copy</button>
                </div>
              </div>
              <div class="inline" style="margin-top:10px">
                <button id="mdblist_device_start" class="btn">Connect MDBList</button>
                <button id="mdblist_disconnect_device" class="btn danger">Delete</button>
                <div class="sub">A browser window opens so you can enter your code.</div>
              </div>
            </div>

            <div id="mdblist_api_panel" style="margin-top:10px">
              <div class="grid2">
                <div>
                  <label for="mdblist_key">API Key</label>
                  <div class="mdblist-action-row">
                    <input id="mdblist_key" name="mdblist_key" type="password" placeholder="********" />
                    <button id="mdblist_save" class="btn">Connect MDBList</button>
                    <button id="mdblist_disconnect_api" class="btn danger">Delete</button>
                  </div>
                  <div id="mdblist_hint" class="msg warn" style="margin-top:8px">
                    You need an MDBList API key. Create one at
                    <a href="https://mdblist.com/preferences/#api" target="_blank" rel="noopener">MDBList Preferences</a>.
                  </div>
                </div>
              </div>
            </div>

            <div class="inline" style="margin-top:10px;justify-content:flex-end">
              <div id="mdblist_msg" class="msg ok hidden" aria-live="polite"></div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
"""
