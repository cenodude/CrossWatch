# providers/auth/_auth_JELLYFIN.py
from __future__ import annotations

import secrets
from typing import Any, Mapping, MutableMapping, Optional, Dict
from urllib.parse import urljoin

try:
    from _logging import log
except Exception:
    def log(msg: str, level: str = "INFO", module: str = "AUTH", **_):
        try: print(f"[{module}] {level}: {msg}")
        except Exception: pass

from ._auth_base import AuthProvider, AuthStatus, AuthManifest

UA = "CrossWatch/1.0"
__VERSION__ = "0.1.0"
HTTP_TIMEOUT_POST = 15
HTTP_TIMEOUT_GET = 10

def _clean_base(url: str) -> str:
    u = (url or "").strip()
    if not u: return ""
    if not (u.startswith("http://") or u.startswith("https://")): u = "http://" + u
    return u if u.endswith("/") else u + "/"

def _mb_auth_value(token: Optional[str], device_id: str) -> str:
    base = f'MediaBrowser Client="CrossWatch", Device="Web", DeviceId="{device_id}", Version="1.0"'
    return f'{base}, Token="{token}"' if token else base

def _headers(token: Optional[str], device_id: str) -> Dict[str, str]:
    auth_val = _mb_auth_value(token, device_id)
    h: Dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": UA,
        "Authorization": auth_val,
        "X-Emby-Authorization": auth_val,
    }
    if token: h["X-MediaBrowser-Token"] = token
    return h

def _raise_with_details(resp, default: str) -> None:
    msg = default
    try:
        j = resp.json() or {}
        msg = (j.get("ErrorMessage") or j.get("Message") or msg)
    except Exception:
        t = (getattr(resp, "text", "") or "").strip()
        if t: msg = f"{default}: {t[:200]}"
    try:
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError(msg) from e
    raise RuntimeError(msg)

class JellyfinAuth(AuthProvider):
    name = "JELLYFIN"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="JELLYFIN",
            label="Jellyfin",
            flow="token",
            fields=[
                {"key": "jellyfin.server",   "label": "Server URL", "type": "text",     "required": True},
                {"key": "jellyfin.username", "label": "Username",   "type": "text",     "required": True},
                {"key": "jellyfin.password", "label": "Password",   "type": "password", "required": True},
            ],
            actions={"start": True, "finish": False, "refresh": False, "disconnect": True},
            notes="Sign in with your Jellyfin account to obtain a user access token.",
        )

    def capabilities(self) -> dict:
        return {
            "features": {
                "watchlist": {"read": True, "write": True},
                "ratings":   {"read": True, "write": True},
                "watched":   {"read": True, "write": True},
                "playlists": {"read": True, "write": True},
            },
            "entity_types": ["movie", "show", "episode"],
        }

    def get_status(self, cfg: Mapping[str, Any]) -> AuthStatus:
        jf = cfg.get("jellyfin") or {}
        server = (jf.get("server") or "").strip()
        token  = (jf.get("access_token") or "").strip()
        user   = (jf.get("user") or jf.get("username") or "").strip() or None
        return AuthStatus(connected=bool(server and token), label="Jellyfin", user=user)

    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str) -> Dict[str, Any]:
        import requests  # lazy
        from requests import exceptions as rx

        jf = cfg.setdefault("jellyfin", {})
        base = _clean_base(jf.get("server", ""))
        user = (jf.get("username") or "").strip()
        pw   = (jf.get("password") or "").strip()
        if not base: raise RuntimeError("Malformed request: missing server")
        if not user or not pw: raise RuntimeError("Malformed request: missing username/password")

        dev_id = (jf.get("device_id") or "").strip() or secrets.token_hex(16)
        jf["device_id"] = dev_id
        jf["server"] = base  # persist normalized

        url = urljoin(base, "Users/AuthenticateByName")
        headers = _headers(token=None, device_id=dev_id)
        headers["Content-Type"] = "application/json"
        payload = {"Username": user, "Pw": pw}

        log("Jellyfin: authenticating...", level="INFO", module="AUTH")
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=HTTP_TIMEOUT_POST)
        except rx.ConnectTimeout:
            raise RuntimeError("Server not reachable: timeout")
        except rx.ReadTimeout:
            raise RuntimeError("Server not reachable: timeout")
        except rx.SSLError:
            raise RuntimeError("Server not reachable: ssl")
        except rx.ConnectionError:
            raise RuntimeError("Server not reachable: connection")
        except rx.InvalidURL:
            raise RuntimeError("Malformed request: server url")
        except rx.RequestException as e:
            raise RuntimeError(f"Server not reachable: {e.__class__.__name__}")

        if r.status_code in (401, 403): raise RuntimeError("Invalid credentials")
        if r.status_code >= 500:        raise RuntimeError(f"Server error ({r.status_code})")
        if not r.ok:                    _raise_with_details(r, "Login failed")

        data = r.json() or {}
        token = (data.get("AccessToken") or "").strip()
        if not token: raise RuntimeError("Login failed: no access token returned")

        user_obj = data.get("User") or {}
        user_id  = (user_obj.get("Id") or "").strip()
        display  = (user_obj.get("Name") or user).strip()

        # Optional token check
        try:
            me = requests.get(urljoin(base, "Users/Me"), headers=_headers(token, dev_id), timeout=HTTP_TIMEOUT_GET)
            if me.ok:
                info = me.json() or {}
                display = (info.get("Name") or display).strip()
        except Exception:
            pass

        jf["access_token"] = token
        jf["user_id"] = user_id or jf.get("user_id") or ""
        jf["user"] = display or user
        jf.pop("password", None)

        log("Jellyfin: access token stored", level="SUCCESS", module="AUTH")
        return {"ok": True, "mode": "user_token", "user_id": jf.get("user_id") or ""}

    def finish(self, cfg: MutableMapping[str, Any], **payload) -> AuthStatus:
        return self.get_status(cfg)

    def refresh(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        return self.get_status(cfg)

    def disconnect(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        jf = cfg.setdefault("jellyfin", {})
        for k in ("access_token", "user_id"):
            jf.pop(k, None)
        log("Jellyfin: disconnected", level="INFO", module="AUTH")
        return self.get_status(cfg)

PROVIDER = JellyfinAuth()
__all__ = ["PROVIDER", "JellyfinAuth", "html", "__VERSION__"]

def html() -> str:
    return r'''<div class="section" id="sec-jellyfin">
  <div class="head" onclick="toggleSection && toggleSection('sec-jellyfin')">
    <span class="chev"></span><strong>Jellyfin</strong>
  </div>
  <div class="body">
    <div class="grid2">
      <div>
        <label>Server URL</label>
        <input id="jfy_server" placeholder="http://host:8096/" onchange="try{saveSetting('jellyfin.server', this.value);}catch(_){;}">
      </div>
      <div>
        <label>Username</label>
        <input id="jfy_user" placeholder="username" onchange="try{saveSetting('jellyfin.username', this.value);}catch(_){;}">
      </div>
    </div>
    <div class="grid2" style="margin-top:8px">
      <div>
        <label>Password</label>
        <input id="jfy_pass" type="password" placeholder="********" onchange="try{saveSetting('jellyfin.password', this.value);}catch(_){;}">
      </div>
      <div>
        <label>Access Token</label>
        <input id="jfy_tok" readonly placeholder="empty = not set">
      </div>
    </div>
    <div style="display:flex;gap:8px;margin-top:10px;align-items:center">
      <button class="btn jellyfin" onclick="try{ jfyLogin && jfyLogin(); }catch(_){;}">Sign in</button>
      <div class="muted">Username/password -> user access token.</div>
    </div>
    <div class="sep"></div>
  </div>
</div>'''
