# providers/auth/_auth_SIMKL.py
# CrossWatch - SIMKL Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from collections.abc import Mapping, MutableMapping
from typing import Any
from urllib.parse import urlencode

import requests

from ._auth_base import AuthManifest, AuthProvider, AuthStatus

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

SIMKL_AUTH = "https://simkl.com/oauth/authorize"
SIMKL_TOKEN = "https://api.simkl.com/oauth/token"
UA = "CrossWatch/1.0"
__VERSION__ = "1.0.0"

class SimklAuth(AuthProvider):
    name = "SIMKL"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="SIMKL",
            label="SIMKL",
            flow="oauth",
            fields=[
                {
                    "key": "simkl.client_id",
                    "label": "Client ID",
                    "type": "text",
                    "required": True,
                },
                {
                    "key": "simkl.client_secret",
                    "label": "Client Secret",
                    "type": "password",
                    "required": True,
                },
            ],
            actions={"start": True, "finish": False, "refresh": True, "disconnect": True},
            notes="Authorize with SIMKL; you'll be redirected back to the app.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {
            "features": {
                "watchlist": {"read": True, "write": True},
                "collections": {"read": False, "write": False},
                "ratings": {"read": True, "write": True, "scale": "1-10"},
                "watched": {"read": True, "write": True},
                "liked_lists": {"read": True, "write": False},
            },
            "entity_types": ["movie", "show"],
        }

    def get_status(self, cfg: Mapping[str, Any]) -> AuthStatus:
        s = cfg.get("simkl") or {}
        ok = bool(s.get("access_token"))
        return AuthStatus(
            connected=ok,
            label="SIMKL",
            user=s.get("account") or None,
            expires_at=int(s.get("token_expires_at") or 0) or None,
            scopes=s.get("scopes") or None,
        )

    def _apply_token_response(self, cfg: MutableMapping[str, Any], j: dict[str, Any]) -> None:
        s = cfg.setdefault("simkl", {})
        if j.get("access_token"):
            s["access_token"] = j["access_token"]
        if "refresh_token" in j and j.get("refresh_token") is not None:
            s["refresh_token"] = j["refresh_token"]
        exp_in = j.get("expires_in")
        if isinstance(exp_in, (int, float)) and exp_in > 0:
            s["token_expires_at"] = int(time.time()) + int(exp_in)
        else:
            if "token_expires_at" in j:
                try:
                    s["token_expires_at"] = int(j["token_expires_at"])
                except Exception:
                    pass
            if "expires_at" in j:
                try:
                    s["token_expires_at"] = int(j["expires_at"])
                except Exception:
                    pass
        if j.get("scope"):
            s["scopes"] = j["scope"]

    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str) -> dict[str, Any]:
        s = cfg.get("simkl") or {}
        client_id = s.get("client_id") or ""
        params = {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": "public write offline_access",
        }
        url = f"{SIMKL_AUTH}?{urlencode(params)}"
        log("SIMKL: start OAuth", level="INFO", module="AUTH", extra={"redirect_uri": redirect_uri})
        return {"url": url}

    def finish(self, cfg: MutableMapping[str, Any], **payload: Any) -> AuthStatus:
        s = cfg.setdefault("simkl", {})
        data = {
            "grant_type": "authorization_code",
            "client_id": s.get("client_id", ""),
            "client_secret": s.get("client_secret", ""),
            "redirect_uri": payload.get("redirect_uri", ""),
            "code": payload.get("code", ""),
        }
        headers = {
            "User-Agent": UA,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "simkl-api-key": s.get("client_id", ""),
        }
        log("SIMKL: exchange code", level="INFO", module="AUTH")
        r = requests.post(SIMKL_TOKEN, json=data, headers=headers, timeout=12)
        r.raise_for_status()
        j = r.json() or {}
        self._apply_token_response(cfg, j)
        log("SIMKL: tokens stored", level="SUCCESS", module="AUTH")
        return self.get_status(cfg)

    def refresh(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        s = cfg.setdefault("simkl", {})
        if not s.get("refresh_token"):
            log("SIMKL: no refresh token", level="WARNING", module="AUTH")
            return self.get_status(cfg)
        data = {
            "grant_type": "refresh_token",
            "client_id": s.get("client_id", ""),
            "client_secret": s.get("client_secret", ""),
            "refresh_token": s.get("refresh_token", ""),
        }
        headers = {
            "User-Agent": UA,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "simkl-api-key": s.get("client_id", ""),
        }
        log("SIMKL: refresh token", level="INFO", module="AUTH")
        r = requests.post(SIMKL_TOKEN, json=data, headers=headers, timeout=12)
        r.raise_for_status()
        j = r.json() or {}
        self._apply_token_response(cfg, j)
        log("SIMKL: refresh ok", level="SUCCESS", module="AUTH")
        return self.get_status(cfg)

    def disconnect(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        s = cfg.setdefault("simkl", {})
        for k in ("access_token", "refresh_token", "token_expires_at", "scopes", "account"):
            s.pop(k, None)
        log("SIMKL: disconnected", level="INFO", module="AUTH")
        return self.get_status(cfg)


PROVIDER = SimklAuth()
__all__ = ["PROVIDER", "SimklAuth", "html", "__VERSION__"]


def html() -> str:
    return r'''<div class="section" id="sec-simkl">
  <style>
    #sec-simkl .inline{display:flex;gap:8px;align-items:center}
    #sec-simkl .inline .msg{margin-left:auto;padding:8px 12px;border-radius:12px;border:1px solid rgba(0,255,170,.18);background:rgba(0,255,170,.08);color:#b9ffd7;font-weight:600}
    #sec-simkl .inline .msg.warn{border-color:rgba(255,210,0,.18);background:rgba(255,210,0,.08);color:#ffe9a6}
    #sec-simkl .inline .msg.hidden{display:none}
    #sec-simkl .btn.danger{background:#a8182e;border-color:rgba(255,107,107,.4)}
    #sec-simkl .btn.danger:hover{filter:brightness(1.08)}

    /* Connect SIMKL */
    #sec-simkl #btn-connect-simkl{
      background: linear-gradient(135deg,#00e084,#2ea859);
      border-color: rgba(0,224,132,.45);
      box-shadow: 0 0 14px rgba(0,224,132,.35);
      color: #fff;
    }
    #sec-simkl #btn-connect-simkl:hover{
      filter: brightness(1.06);
      box-shadow: 0 0 18px rgba(0,224,132,.5);
    }
  </style>

  <div class="head" onclick="toggleSection('sec-simkl')">
    <span class="chev"></span><strong>SIMKL</strong>
  </div>
  <div class="body">
    <div class="grid2">
      <div>
        <label>Client ID</label>
        <input id="simkl_client_id" placeholder="Your SIMKL client id" oninput="updateSimklButtonState()">
      </div>
      <div>
        <label>Client Secret</label>
        <input id="simkl_client_secret" placeholder="Your SIMKL client secret" oninput="updateSimklButtonState()" type="password">
      </div>
    </div>

    <div id="simkl_hint" class="msg warn hidden">
      You need a SIMKL API key. Create one at
      <a href="https://simkl.com/settings/developer/" target="_blank" rel="noopener">SIMKL Developer</a>.
      Set the Redirect URL to <code id="redirect_uri_preview"></code>.
      <button class="btn" style="margin-left:8px" onclick="copyRedirect()">Copy Redirect URL</button>
    </div>

    <div class="inline" style="margin-top:8px">
      <button id="btn-connect-simkl" class="btn" onclick="startSimkl()">Connect SIMKL</button>
      <button class="btn danger" onclick="try{ simklDeleteToken && simklDeleteToken(); }catch(_){;}">Delete</button>
      <span id="simkl-countdown" style="min-width:60px;"></span>
      <div id="simkl-status" class="text-sm" style="color:var(--muted)">Opens SIMKL authorize; callback returns here</div>
      <div id="simkl_msg" class="msg ok hidden">Successfully retrieved token</div>
    </div>

    <div class="grid2" style="margin-top:8px">
      <div>
        <label>Access token</label>
        <input id="simkl_access_token" readonly placeholder="empty = not set">
      </div>
    </div>

    <div class="sep"></div>
  </div>
</div>
'''