# providers/auth/_auth_TAUTULLI.py
# CrossWatch - Tautulli Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import get_provider_block, ensure_instance_block, normalize_instance_id

__VERSION__ = "1.1.0"



def _load_config() -> dict[str, Any]:
    try:
        return dict(load_config() or {})
    except Exception:
        return {}


def _block(cfg: Mapping[str, Any], instance_id: Any = None) -> dict[str, Any]:
    return get_provider_block(cfg or {}, "tautulli", instance_id)



class TautulliAuth(AuthProvider):
    name = "TAUTULLI"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="TAUTULLI",
            label="Tautulli",
            flow="api_keys",
            fields=[
                {
                    "key": "tautulli.server_url",
                    "label": "Server URL",
                    "type": "text",
                    "required": True,
                    "placeholder": "http://localhost:8181",
                },
                {
                    "key": "tautulli.api_key",
                    "label": "API Key",
                    "type": "password",
                    "required": True,
                    "placeholder": "••••••••",
                },
                {
                    "key": "tautulli.verify_ssl",
                    "label": "Verify SSL",
                    "type": "bool",
                    "required": False,
                    "placeholder": "",
                },
                {
                    "key": "tautulli.history.user_id",
                    "label": "History User ID (optional)",
                    "type": "text",
                    "required": False,
                    "placeholder": "1",
                },
            ],
            actions={"start": False, "finish": True, "refresh": False, "disconnect": True},
            notes="API key is in Tautulli > Settings > Web Interface > API.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"watchlist": False, "ratings": False, "history": True}

    def get_status(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        t = _block(cfg, inst)
        ok = bool(str(t.get("server_url") or "").strip() and str(t.get("api_key") or "").strip())
        label = "Tautulli" if inst == "default" else f"Tautulli ({inst})"
        return AuthStatus(connected=ok, label=label)

    def start(self, cfg: MutableMapping[str, Any] | None = None, *, redirect_uri: str | None = None, instance_id: Any = None) -> dict[str, Any]:
        return {}

    def finish(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None, **payload: Any) -> AuthStatus:
        cfgd = dict(cfg or _load_config() or {})
        inst = normalize_instance_id(instance_id)
        t = ensure_instance_block(cfgd, "tautulli", inst)
        t["server_url"] = str(payload.get("server_url") or payload.get("tautulli.server_url") or "").strip()
        t["api_key"] = str(payload.get("api_key") or payload.get("tautulli.api_key") or "").strip()
        if "verify_ssl" in payload or "tautulli.verify_ssl" in payload:
            t["verify_ssl"] = bool(payload.get("verify_ssl", payload.get("tautulli.verify_ssl", True)))

        user_id = str(payload.get("user_id") or payload.get("tautulli.history.user_id") or "").strip()
        if user_id:
            t.setdefault("history", {})["user_id"] = user_id

        save_config(cfgd)
        return self.get_status(cfgd, instance_id=inst)

    def refresh(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        return self.get_status(cfg, instance_id=instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfgd = dict(cfg or _load_config() or {})
        inst = normalize_instance_id(instance_id)
        t = ensure_instance_block(cfgd, "tautulli", inst)
        t["server_url"] = ""
        t["api_key"] = ""
        save_config(cfgd)
        return self.get_status(cfgd, instance_id=inst)


def html() -> str:
    return r"""<div class="section" id="sec-tautulli">
  <style>
    #sec-tautulli .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    #sec-tautulli .inline{display:flex;gap:8px;align-items:center}
    #sec-tautulli .msg{margin-left:auto;padding:8px 12px;border-radius:12px;border:1px solid rgba(0,255,170,.18);background:rgba(0,255,170,.08);color:#b9ffd7;font-weight:600}
    #sec-tautulli .msg.warn{border-color:rgba(255,210,0,.18);background:rgba(255,210,0,.08);color:#ffe9a6}
    #sec-tautulli .msg.hidden{display:none}
    #sec-tautulli .btn.danger{ background:#a8182e; border-color:rgba(255,107,107,.4) }
    #sec-tautulli #tautulli_save{
      background: linear-gradient(135deg,#ff8a00,#ff5a1f);
      border-color: rgba(255,138,0,.55);
      box-shadow: 0 0 14px rgba(255,138,0,.35);
      color: #fff;
    }
    #sec-tautulli #tautulli_save:hover{filter:brightness(1.06);box-shadow:0 0 18px rgba(255,138,0,.5)}
  </style>

  <div class="head" onclick="toggleSection && toggleSection('sec-tautulli')">
    <span class="chev">▶</span><strong>Tautulli</strong>
  </div>

  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="tautulli">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">Tautulli</div>
            <div class="muted">Connect your Tautulli server and API key.</div>
          </div>
        </div>

        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
        </div>

        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="grid2">
                  <div>
                    <label>Server URL</label>
                    <input id="tautulli_server" type="text" placeholder="http://localhost:8181" />
                    <label style="margin-top:10px">API Key</label>
                    <div style="display:flex;gap:8px">
                      <input id="tautulli_key" type="password" placeholder="••••••••" />
                      <button id="tautulli_save" class="btn">Connect</button>
                    </div>
                    <label style="margin-top:10px">User ID (optional)</label>
                    <input id="tautulli_user_id" type="text" placeholder="1" />
            
                    <div id="tautulli_hint" class="msg warn" style="margin-top:8px">
                      API key: Tautulli → Settings → Web Interface → API.
                    </div>
                  </div>
            
                  <div>
                    <label>Status</label>
                    <div class="inline">
                      <button id="tautulli_verify" class="btn">Verify</button>
                      <button id="tautulli_disconnect" class="btn danger">Disconnect</button>
                      <div id="tautulli_msg" class="msg ok hidden" aria-live="polite"></div>
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
