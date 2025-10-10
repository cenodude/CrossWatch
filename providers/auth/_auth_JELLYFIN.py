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
  <style>
    /* TEMP: hide the Jellyfin Detail Settings */
    #sec-jellyfin details.settings { display: none !important; }
    #sec-jellyfin .footspace { display: none !important; }
    /* (------------------------------------------------------------------- */
    #sec-jellyfin details.settings{border:1px dashed var(--border);border-radius:12px;background:#0b0d12;padding:10px 12px 14px;margin-top:8px}
    #sec-jellyfin details.settings summary{cursor:pointer;font-weight:700;opacity:.9;list-style:none}
    #sec-jellyfin details.settings .wrap{margin-top:10px;display:grid;grid-template-columns:1fr 1fr;gap:12px;align-items:start}
    #sec-jellyfin .inline{display:flex;gap:8px;align-items:center}
    #sec-jellyfin .btnrow{display:flex;gap:8px;margin-top:12px}
    #sec-jellyfin .sub{opacity:.7;font-size:.92em}
    #sec-jellyfin select[multiple]{min-height:140px}
  </style>

  <div class="head" onclick="toggleSection && toggleSection('sec-jellyfin')">
    <span class="chev">▶</span><strong>Jellyfin</strong>
  </div>

  <div class="body">
    <div class="grid2">
      <div>
        <label>Server URL</label>
        <input id="jfy_server" placeholder="http://host:8096/">
      </div>
      <div>
        <label>Username</label>
        <input id="jfy_user" placeholder="username">
      </div>
    </div>
    <div class="grid2" style="margin-top:8px">
      <div>
        <label>Password</label>
        <input id="jfy_pass" type="password" placeholder="********">
      </div>
      <div>
        <label>Access Token</label>
        <input id="jfy_tok" readonly placeholder="empty = not set">
      </div>
    </div>
    <div class="inline" style="margin-top:10px">
      <button class="btn jellyfin" onclick="try{ jfyLogin && jfyLogin(); }catch(_){;}">Sign in</button>
      <div class="muted">Username/password → user access token.</div>
    </div>
    <div class="sep"></div>

    <details class="settings">
      <summary>Settings</summary>
      <div class="wrap">
        <!-- Left column -->
        <div>
          <label>Server URL</label>
          <input id="jfy_server_url" placeholder="http://host:8096/">
          <div class="sub">Leave blank to discover.</div>

          <label style="margin-top:10px">Username</label>
          <input id="jfy_username" placeholder="Display name">

          <div class="btnrow">
            <button class="btn" title="Fetch server/username from Jellyfin" onclick="jfyAuto()">Auto-Fetch</button>
            <button class="btn" title="Load Jellyfin libraries" onclick="jfyLoadLibraries()">Load libraries</button>
            <span class="sub" style="align-self:center">Edit values before Save if needed.</span>
          </div>
        </div>

        <!-- Right column -->
        <div>
          <label>Whitelist libraries – History</label>
          <select id="jfy_lib_history" multiple></select>
          <div class="sub">Empty = all libraries.</div>

          <label style="margin-top:10px">Whitelist libraries – Ratings</label>
          <select id="jfy_lib_ratings" multiple></select>
          <div class="sub">Empty = all libraries.</div>
        </div>
      </div>
    </details>
  </div>
</div>

<script>
(function(){
  const Q = s => document.querySelector(s);

  async function hydrate(){
    try{
      const r = await fetch('/api/config',{cache:'no-store'});
      const cfg = r.ok? await r.json() : null;
      const jf = (cfg&&cfg.jellyfin)||{};
      const s = jf.server||''; const u = jf.user||jf.username||'';
      const hasTok = !!(jf.access_token||'').trim();
      if(Q('#jfy_server'))     Q('#jfy_server').value = s;
      if(Q('#jfy_user'))       Q('#jfy_user').value   = u;
      if(Q('#jfy_server_url')) Q('#jfy_server_url').value = s;
      if(Q('#jfy_username'))   Q('#jfy_username').value   = u;
      if(Q('#jfy_tok')){ Q('#jfy_tok').value = hasTok?'••••••••':''; Q('#jfy_tok').dataset.masked = hasTok?'1':'0'; }
    }catch{}
  }
  document.readyState==='loading' ? document.addEventListener('DOMContentLoaded',hydrate,{once:true}) : hydrate();

  // ensure Save merges our fields (crosswatch.js picks these up)
  window.mergeJellyfinIntoCfg = function(cfg){
    const v = sel => { const el=Q(sel); return el?el.value.trim():''; };
    const has = Q('#jfy_server_url') || Q('#jfy_username') || Q('#jfy_lib_history') || Q('#jfy_lib_ratings');
    if(!has) return cfg;
    const jf = (cfg.jellyfin = cfg.jellyfin || {});
    const server = v('#jfy_server_url') || v('#jfy_server');
    const user   = v('#jfy_username')   || v('#jfy_user');
    jf.server = server;
    jf.user   = user;
    jf.username = user || jf.username || '';
    const valsInt = sel => {
      const el = Q(sel);
      return el ? Array.from(el.selectedOptions||[]).map(o=>parseInt(o.value,10)).filter(Number.isFinite) : [];
    };
    jf.history = Object.assign({}, jf.history||{}, { libraries: valsInt('#jfy_lib_history') });
    jf.ratings = Object.assign({}, jf.ratings||{}, { libraries: valsInt('#jfy_lib_ratings') });
    return cfg;
  };

  // expose for buttons
  window.jfyAuto = async function(){
    try{
      const r = await fetch('/api/jellyfin/inspect?ts='+Date.now(), {cache:'no-store'});
      if(!r.ok) throw 0;
      const d = await r.json();
      if(d.server_url && Q('#jfy_server_url')) Q('#jfy_server_url').value = d.server_url;
      if(d.username   && Q('#jfy_username'))   Q('#jfy_username').value   = d.username;
      if(d.server_url && Q('#jfy_server'))     Q('#jfy_server').value     = d.server_url;
      if(d.username   && Q('#jfy_user'))       Q('#jfy_user').value       = d.username;
    }catch{}
  };

  window.jfyLoadLibraries = async function(){
    try{
      const r = await fetch('/api/jellyfin/libraries?ts='+Date.now(), {cache:'no-store'});
      if(!r.ok) throw 0;
      const d = await r.json();
      const libs = Array.isArray(d?.libraries) ? d.libraries : [];
      const fill = id => {
        const el = Q(id); if(!el) return;
        const keep = new Set(Array.from(el.selectedOptions||[]).map(o=>o.value));
        el.innerHTML='';
        libs.forEach(it=>{
          const o = document.createElement('option');
          o.value = String(it.key);
          o.textContent = `${it.title} (${it.type||'lib'}) — #${it.key}`;
          if(keep.has(o.value)) o.selected = true;
          el.appendChild(o);
        });
      };
      fill('#jfy_lib_history'); fill('#jfy_lib_ratings');
    }catch{}
  };

  // hook into global save
  document.addEventListener('DOMContentLoaded', ()=>{
    const btn = document.getElementById('save-fab-btn');
    if(btn) btn.addEventListener('click', ()=>{ try{ window.mergeJellyfinIntoCfg?.(window.__cfg||{}); }catch{} }, true);
  });
})();
</script>
'''
