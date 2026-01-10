# providers/auth/_auth_ANILIST.py
# CrossWatch - AniList Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any
from urllib.parse import urlencode

import requests

from ._auth_base import AuthManifest, AuthProvider, AuthStatus

try:
    from _logging import log as _real_log
except ImportError:
    _real_log = None

__VERSION__ = "0.1.0"

UA = "CrossWatch/1.0"
AUTH_URL = "https://anilist.co/api/v2/oauth/authorize"
TOKEN_URL = "https://anilist.co/api/v2/oauth/token"
GQL_URL = "https://graphql.anilist.co"


def log(msg: str, *, level: str = "INFO", module: str = "AUTH", extra: dict[str, Any] | None = None) -> None:
    try:
        if callable(_real_log):
            _real_log(msg, level=level, module=module, extra=extra or {})
    except Exception:
        pass


def _gql_viewer(access_token: str) -> dict[str, Any] | None:
    q = "query { Viewer { id name } }"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": UA,
    }
    r = requests.post(GQL_URL, json={"query": q}, headers=headers, timeout=15)
    if not r.ok:
        return None
    return (r.json() or {}).get("data", {}).get("Viewer")


def _token_exchange(code: str, *, client_id: str, client_secret: str, redirect_uri: str) -> str:
    payload = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "code": code,
    }
    headers = {"Accept": "application/json", "User-Agent": UA}

    r = requests.post(TOKEN_URL, json=payload, headers=headers, timeout=15)
    if r.status_code >= 400:
        r = requests.post(TOKEN_URL, data=payload, headers=headers, timeout=15)
    r.raise_for_status()

    j = r.json() or {}
    tok = (j.get("access_token") or "").strip()
    if not tok:
        raise RuntimeError("AniList token exchange returned no access_token")
    return tok


class AniListAuth(AuthProvider):
    name = "ANILIST"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="ANILIST",
            label="AniList",
            flow="oauth2",
            fields=[
                {"key": "anilist.client_id", "label": "Client ID", "type": "text", "required": True},
                {"key": "anilist.client_secret", "label": "Client Secret", "type": "password", "required": True},
            ],
            actions={"start": True, "finish": False, "refresh": False, "disconnect": True},
            notes="Authorize with AniList; you'll be redirected back to the app.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"features": {"watchlist": {"read": True, "write": True}}}

    def get_status(self, cfg: Mapping[str, Any]) -> AuthStatus:
        s = (cfg.get("anilist") or {}) if isinstance(cfg, Mapping) else {}
        tok = str(s.get("access_token") or "").strip()
        user = s.get("user") or {}
        uname = None
        if isinstance(user, Mapping):
            uname = user.get("name")
        return AuthStatus(connected=bool(tok), label="AniList", user=str(uname) if uname else None)

    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str) -> dict[str, Any]:
        s = cfg.get("anilist") or {}
        client_id = str(s.get("client_id") or "").strip()
        params = {"client_id": client_id, "response_type": "code", "redirect_uri": redirect_uri}
        url = f"{AUTH_URL}?{urlencode(params)}"
        log("ANILIST: start OAuth", extra={"redirect_uri": redirect_uri})
        return {"url": url}

    def finish(self, cfg: MutableMapping[str, Any], **payload: Any) -> AuthStatus:
        s = cfg.setdefault("anilist", {})
        code = str(payload.get("code") or "").strip()
        redirect_uri = str(payload.get("redirect_uri") or "").strip()
        client_id = str(s.get("client_id") or "").strip()
        client_secret = str(s.get("client_secret") or "").strip()

        tok = _token_exchange(code, client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri)
        s["access_token"] = tok

        viewer = None
        try:
            viewer = _gql_viewer(tok)
        except Exception:
            viewer = None
        if viewer:
            s["user"] = dict(viewer)

        return self.get_status(cfg)

    def refresh(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        return self.get_status(cfg)

    def disconnect(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        s = cfg.setdefault("anilist", {})
        s["access_token"] = ""
        s.pop("user", None)
        return self.get_status(cfg)


PROVIDER = AniListAuth()


def html() -> str:
    return r'''<div class="section" id="sec-anilist">
  <style>
    #sec-anilist .inline{display:flex;gap:8px;align-items:center}
    #sec-anilist .inline .msg{margin-left:auto;padding:8px 12px;border-radius:999px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.04);color:#ddd;font-weight:600}
    #sec-anilist .inline .msg.ok{border-color:rgba(0,255,170,.18);background:rgba(0,255,170,.08);color:#b9ffd7}
    #sec-anilist .inline .msg.warn{border-color:rgba(255,210,0,.18);background:rgba(255,210,0,.08);color:#ffe9a6}
    #sec-anilist .inline .msg.hidden{display:none}
    #sec-anilist .btn.danger{background:#a8182e;border-color:rgba(255,107,107,.4)}
    #sec-anilist .btn.danger:hover{filter:brightness(1.08)}
    #sec-anilist #btn-connect-anilist{
      background: linear-gradient(135deg,#6d5dfc,#a855f7);
      border-color: rgba(168,85,247,.45);
      box-shadow: 0 0 12px rgba(168,85,247,.35);
    }
    #sec-anilist #btn-connect-anilist:hover{filter:brightness(1.06);box-shadow: 0 0 18px rgba(168,85,247,.5)}
  </style>

  <div class="head" onclick="toggleSection('sec-anilist')">
    <span class="chev">▶</span><strong>AniList</strong>
  </div>
  <div class="body">
    <div class="grid2">
      <div>
        <label>Client ID</label>
        <input id="anilist_client_id" placeholder="Your AniList client_id" autocomplete="off" oninput="updateAniListButtonState()" />
      </div>
      <div>
        <label>Client Secret</label>
        <input id="anilist_client_secret" placeholder="Your AniList client_secret" type="password" autocomplete="off" oninput="updateAniListButtonState()" />
      </div>
    </div>

    <div id="anilist_hint" class="msg warn hidden">
    You need an AniList API key. Create one at
    <a href="https://anilist.co/settings/developer" target="_blank" rel="noopener">AniList Developer</a>.
    Set the Redirect URL to <code id="redirect_uri_preview_anilist"></code>.
    <button class="btn" style="margin-left:8px" onclick="copyAniListRedirect()">Copy Redirect URL</button>
    </div>


    <div class="inline" style="margin-top:10px">
      <button class="btn" id="btn-connect-anilist" onclick="startAniList()">Connect AniList</button>
      <button class="btn danger" onclick="anilistDeleteToken()">Disconnect</button>
      <span class="msg hidden" id="anilist_msg"></span>
    </div>

    <div style="margin-top:10px">
      <label>Access Token</label>
      <input id="anilist_access_token" placeholder="(auto-filled after auth)" autocomplete="off" />
    </div>
  </div>
</div>'''
