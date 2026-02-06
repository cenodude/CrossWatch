# providers/auth/_auth_MDBLIST.py
# CrossWatch - MDBList Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

import requests

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import get_provider_block, ensure_instance_block, normalize_instance_id

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
__VERSION__ = "2.0.0"

def _load_config() -> dict[str, Any]:
    try:
        return dict(load_config() or {})
    except Exception:
        return {}

def _block(cfg: Mapping[str, Any], instance_id: Any = None) -> dict[str, Any]:
    return get_provider_block(cfg or {}, "mdblist", instance_id)

def _get(cfg: Mapping[str, Any], path: str, *, instance_id: Any = None, timeout: int = HTTP_TIMEOUT) -> tuple[int, dict[str, Any]]:
    b = _block(cfg, instance_id)
    key = str(b.get("api_key") or "").strip()
    if not key:
        return 0, {}
    url = f"{API_BASE}{path}?apikey={key}"
    try:
        r = requests.get(url, headers={"Accept": "application/json", "User-Agent": UA}, timeout=timeout)
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
            flow="api_keys",
            fields=[
                {
                    "key": "mdblist.api_key",
                    "label": "API Key",
                    "type": "password",
                    "required": True,
                    "placeholder": "••••••••",
                }
            ],
            actions={"start": False, "finish": True, "refresh": False, "disconnect": True},
            notes="Generate your API key in mdblist.com > Preferences.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"watchlist": True, "ratings": True, "history": True}

    def get_status(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        b = _block(cfg, inst)
        has = bool(str(b.get("api_key") or "").strip())
        label = "MDBList" if inst == "default" else f"MDBList ({inst})"
        return AuthStatus(connected=has, label=label, user=None)

    def start(self, cfg: MutableMapping[str, Any] | None = None, *, redirect_uri: str | None = None, instance_id: Any = None) -> dict[str, Any]:
        return {}

    def finish(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None, **payload: Any) -> AuthStatus:
        cfgd = dict(cfg or _load_config() or {})
        b = ensure_instance_block(cfgd, "mdblist", instance_id)
        key = str(payload.get("api_key") or payload.get("mdblist.api_key") or "").strip()
        b["api_key"] = key
        save_config(cfgd)
        log(f"MDBList API key saved (instance={normalize_instance_id(instance_id)}).", module="AUTH")
        return self.get_status(cfgd, instance_id=instance_id)

    def refresh(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        return self.get_status(cfg, instance_id=instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfgd = dict(cfg or _load_config() or {})
        b = ensure_instance_block(cfgd, "mdblist", instance_id)
        b["api_key"] = ""
        save_config(cfgd)
        log(f"MDBList disconnected (instance={normalize_instance_id(instance_id)}).", module="AUTH")
        return self.get_status(cfgd, instance_id=instance_id)



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
    
    /* MDBList Connect  */
    #sec-mdblist #mdblist_save{
      background: linear-gradient(135deg,#00e084,#2ea859);
      border-color: rgba(0,224,132,.45);
      box-shadow: 0 0 14px rgba(0,224,132,.35);
      color: #fff;
    }
    #sec-mdblist #mdblist_save:hover{
      filter: brightness(1.06);
      box-shadow: 0 0 18px rgba(0,224,132,.5);
    }
  </style>

  <div class="head" onclick="toggleSection && toggleSection('sec-mdblist')">
    <span class="chev">▶</span><strong>MDBList</strong>
  </div>

  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="mdblist">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">MDBList</div>
            <div class="muted">Connect and verify your MDBList API key.</div>
          </div>
        </div>

        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
        </div>

        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="grid2">
                  <div>
                    <label>API Key</label>
                    <div style="display:flex;gap:8px">
                      <input id="mdblist_key" type="password" placeholder="••••••••" />
                      <button id="mdblist_save" class="btn">Connect</button>
                    </div>
                    <div id="mdblist_hint" class="msg warn" style="margin-top:8px">
                      You need an MDBList API key. Create one at
                      <a href="https://mdblist.com/preferences/#api" target="_blank" rel="noopener">MDBList Preferences</a>.
                    </div>
                  </div>
            
                  <div>
                    <label>Status</label>
                    <div class="inline">
                      <button id="mdblist_verify" class="btn">Verify</button>
                      <button id="mdblist_disconnect" class="btn danger">Disconnect</button>
                      <div id="mdblist_msg" class="msg ok hidden" aria-live="polite"></div>
                    </div>
                  </div>
                </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
"""