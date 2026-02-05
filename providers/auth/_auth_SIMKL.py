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
from cw_platform.config_base import save_config
from cw_platform.provider_instances import ensure_instance_block, ensure_provider_block, normalize_instance_id

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
__VERSION__ = "1.1.0"

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

    def get_status(self, cfg: Mapping[str, Any], instance_id: str | None = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        base: Mapping[str, Any] = {}
        blk: Mapping[str, Any] = {}

        s0 = cfg.get("simkl") if isinstance(cfg, Mapping) else None
        if isinstance(s0, Mapping):
            base = s0
            blk = s0
            if inst != "default":
                insts = s0.get("instances")
                sub = insts.get(inst) if isinstance(insts, Mapping) else None
                if isinstance(sub, Mapping):
                    blk = sub

        ok = bool((blk.get("access_token") or "").strip())
        try:
            exp = int(blk.get("token_expires_at") or 0) or None
        except Exception:
            exp = None

        return AuthStatus(
            connected=ok,
            label="SIMKL",
            user=blk.get("account") or None,
            expires_at=exp,
            scopes=blk.get("scopes") or None,
        )

    def _resolve_creds(self, cfg: MutableMapping[str, Any], instance_id: str | None) -> tuple[str, str, dict[str, Any]]:
        inst = normalize_instance_id(instance_id)
        if isinstance(cfg, dict):
            base = ensure_provider_block(cfg, "simkl")
            view_like = inst != "default" and "instances" not in base and any(k in base for k in ("access_token", "refresh_token", "token_expires_at"))
            if view_like:
                blk = base
            else:
                blk = ensure_instance_block(cfg, "simkl", inst)
                if base.get("client_id") and not blk.get("client_id"):
                    blk["client_id"] = base.get("client_id")
                if base.get("client_secret") and not blk.get("client_secret"):
                    blk["client_secret"] = base.get("client_secret")
                if base.get("client_id") and not blk.get("api_key"):
                    blk["api_key"] = base.get("client_id")

            client_id = str((blk.get("client_id") or "")).strip() or str((base.get("client_id") or "")).strip()
            client_secret = str((blk.get("client_secret") or "")).strip() or str((base.get("client_secret") or "")).strip()
            return client_id, client_secret, blk

        s0 = cfg.get("simkl") if isinstance(cfg, dict) else (cfg.get("simkl") if hasattr(cfg, "get") else None)
        base = dict(s0 or {}) if isinstance(s0, Mapping) else {}
        client_id = str(base.get("client_id") or "").strip()
        client_secret = str(base.get("client_secret") or "").strip()
        return client_id, client_secret, base

    def _apply_token_response(self, target: MutableMapping[str, Any], j: dict[str, Any]) -> None:
        if j.get("access_token"):
            target["access_token"] = j["access_token"]
        if "refresh_token" in j and j.get("refresh_token") is not None:
            target["refresh_token"] = j["refresh_token"]

        exp_in = j.get("expires_in")
        if isinstance(exp_in, (int, float)) and exp_in > 0:
            target["token_expires_at"] = int(time.time()) + int(exp_in)
        else:
            for k in ("token_expires_at", "expires_at"):
                if k in j:
                    try:
                        target["token_expires_at"] = int(j[k])
                        break
                    except Exception:
                        pass

        if j.get("scope"):
            target["scopes"] = j["scope"]

    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str, instance_id: str | None = None) -> dict[str, Any]:
        client_id, _, _ = self._resolve_creds(cfg, instance_id)
        params = {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": "public write offline_access",
        }
        url = f"{SIMKL_AUTH}?{urlencode(params)}"
        inst = normalize_instance_id(instance_id)
        log("SIMKL: start OAuth", level="INFO", module="AUTH", extra={"instance": inst, "redirect_uri": redirect_uri})
        return {"url": url}

    def finish(self, cfg: MutableMapping[str, Any], instance_id: str | None = None, **payload: Any) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        client_id, client_secret, target = self._resolve_creds(cfg, inst)

        data = {
            "grant_type": "authorization_code",
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": payload.get("redirect_uri", ""),
            "code": payload.get("code", ""),
        }
        headers = {
            "User-Agent": UA,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "simkl-api-key": client_id,
        }
        log("SIMKL: exchange code", level="INFO", module="AUTH", extra={"instance": inst})
        r = requests.post(SIMKL_TOKEN, json=data, headers=headers, timeout=12)
        r.raise_for_status()
        j = r.json() or {}

        self._apply_token_response(target, j)
        try:
            if isinstance(cfg, dict):
                save_config(dict(cfg))
        except Exception:
            pass

        log("SIMKL: tokens stored", level="SUCCESS", module="AUTH", extra={"instance": inst})
        return self.get_status(cfg, inst)

    def refresh(self, cfg: MutableMapping[str, Any], instance_id: str | None = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        client_id, client_secret, target = self._resolve_creds(cfg, inst)

        if not (target.get("refresh_token") or "").strip():
            log("SIMKL: no refresh token", level="WARNING", module="AUTH", extra={"instance": inst})
            return self.get_status(cfg, inst)

        data = {
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": target.get("refresh_token", ""),
        }
        headers = {
            "User-Agent": UA,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "simkl-api-key": client_id,
        }
        log("SIMKL: refresh token", level="INFO", module="AUTH", extra={"instance": inst})
        r = requests.post(SIMKL_TOKEN, json=data, headers=headers, timeout=12)
        r.raise_for_status()
        j = r.json() or {}

        self._apply_token_response(target, j)
        try:
            if isinstance(cfg, dict):
                save_config(dict(cfg))
        except Exception:
            pass

        log("SIMKL: refresh ok", level="SUCCESS", module="AUTH", extra={"instance": inst})
        return self.get_status(cfg, inst)

    def disconnect(self, cfg: MutableMapping[str, Any], instance_id: str | None = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        if isinstance(cfg, dict):
            base = ensure_provider_block(cfg, "simkl")
            view_like = inst != "default" and "instances" not in base and any(k in base for k in ("access_token", "refresh_token", "token_expires_at"))
            target = base if view_like else ensure_instance_block(cfg, "simkl", inst)
        else:
            target = cfg.setdefault("simkl", {})  # type: ignore[assignment]

        for k in ("access_token", "refresh_token", "token_expires_at", "scopes", "account"):
            try:
                target.pop(k, None)
            except Exception:
                pass

        try:
            if isinstance(cfg, dict):
                save_config(dict(cfg))
        except Exception:
            pass

        log("SIMKL: disconnected", level="INFO", module="AUTH", extra={"instance": inst})
        return self.get_status(cfg, inst)


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
  
    #sec-simkl .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
  </style>

  <div class="head" onclick="toggleSection && toggleSection('sec-simkl')">
    <span class="chev">▶</span><strong>SIMKL</strong>
  </div>

  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="simkl">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">SIMKL</div>
            <div class="muted">Connect your SIMKL account for watchlist/ratings sync.</div>
          </div>
        </div>

        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
        </div>

        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
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
      </div>
    </div>
  </div>
</div>
'''