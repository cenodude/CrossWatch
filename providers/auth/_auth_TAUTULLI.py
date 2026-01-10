# providers/auth/_auth_TAUTULLI.py
# CrossWatch - Tautulli Auth Provider (API key)
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.config_base import save_config

__VERSION__ = "1.0.0"


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

    def get_status(self, cfg: Mapping[str, Any]) -> AuthStatus:
        t = cfg.get("tautulli") or {}
        ok = bool(str(t.get("server_url") or "").strip() and str(t.get("api_key") or "").strip())
        return AuthStatus(connected=ok, label="Tautulli")

    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str) -> dict[str, Any]:
        return {}

    def finish(self, cfg: MutableMapping[str, Any], **payload: Any) -> AuthStatus:
        t = cfg.setdefault("tautulli", {})
        t["server_url"] = str(payload.get("server_url") or payload.get("tautulli.server_url") or "").strip()
        t["api_key"] = str(payload.get("api_key") or payload.get("tautulli.api_key") or "").strip()
        if "verify_ssl" in payload or "tautulli.verify_ssl" in payload:
            t["verify_ssl"] = bool(payload.get("verify_ssl", payload.get("tautulli.verify_ssl", True)))

        user_id = str(payload.get("user_id") or payload.get("tautulli.history.user_id") or "").strip()
        if user_id:
            t.setdefault("history", {})["user_id"] = user_id

        save_config(dict(cfg))
        return self.get_status(cfg)

    def refresh(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        return self.get_status(cfg)

    def disconnect(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        t = cfg.setdefault("tautulli", {})
        t["server_url"] = ""
        t["api_key"] = ""
        save_config(dict(cfg))
        return self.get_status(cfg)


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

  <div class="head" onclick="toggleSection('sec-tautulli')">
    <span class="chev">▶</span><strong>Tautulli</strong>
  </div>

  <div class="body">
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
          <div id="tautulli_msg" class="msg hidden" aria-live="polite"></div>
        </div>
      </div>
    </div>
  </div>
</div>"""
