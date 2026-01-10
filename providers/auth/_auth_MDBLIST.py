# providers/auth/_auth_MDBLIST.py
# CrossWatch - MDBList Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

import requests

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.config_base import save_config

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
__VERSION__ = "1.0.0"


def _get(cfg: Mapping[str, Any], path: str, timeout: int = HTTP_TIMEOUT) -> tuple[int, dict[str, Any]]:
    key = ((cfg.get("mdblist") or {}).get("api_key") or "").strip()
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

    def get_status(self, cfg: Mapping[str, Any]) -> AuthStatus:
        has = bool(((cfg.get("mdblist") or {}).get("api_key") or "").strip())
        return AuthStatus(connected=has, label="MDBList", user=None)

    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str) -> dict[str, Any]:
        return {}

    def finish(self, cfg: MutableMapping[str, Any], **payload: Any) -> AuthStatus:
        key = (payload.get("api_key") or payload.get("mdblist.api_key") or "").strip()
        cfg.setdefault("mdblist", {})["api_key"] = key
        save_config(dict(cfg))
        log("MDBList API key saved.", module="AUTH")
        return self.get_status(cfg)

    def refresh(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        return self.get_status(cfg)

    def disconnect(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        cfg.setdefault("mdblist", {})["api_key"] = ""
        save_config(dict(cfg))
        log("MDBList disconnected.", module="AUTH")
        return self.get_status(cfg)


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

  <div class="head" onclick="toggleSection('sec-mdblist')">
    <span class="chev">▶</span><strong>MDBList</strong>
  </div>

  <div class="body">
    <div class="grid2">
      <div>
        <label>API Key</label>
        <div style="display:flex;gap:8px">
          <input id="mdblist_key" type="password" placeholder="••••••••" />
          <button id="mdblist_save" class="btn">Connect</button>
        </div>
        <div id="mdblist_hint" class="msg warn" style="margin-top:8px">
          You need an MDBList API key. Create one at
          <a href="https://mdblist.com/preferences/" target="_blank" rel="noopener">MDBList Preferences</a>.
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
</div>"""