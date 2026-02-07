# providers/auth/_auth_JELLYFIN.py
# CrossWatch - Orchestrator
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import secrets
from collections.abc import Mapping, MutableMapping
from typing import Any, cast
from urllib.parse import urljoin

from cw_platform.provider_instances import ensure_instance_block, normalize_instance_id

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

from ._auth_base import AuthManifest, AuthProvider, AuthStatus

UA = "CrossWatch/1.0"
__VERSION__ = "2.0.0"
HTTP_TIMEOUT_POST = 15
HTTP_TIMEOUT_GET = 10


def _clean_base(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if not (u.startswith("http://") or u.startswith("https://")):
        u = "http://" + u
    return u if u.endswith("/") else u + "/"


def _mb_auth_value(token: str | None, device_id: str) -> str:
    base = (
        f'MediaBrowser Client="CrossWatch", Device="Web", '
        f'DeviceId="{device_id}", Version="1.0"'
    )
    return f'{base}, Token="{token}"' if token else base


def _headers(token: str | None, device_id: str) -> dict[str, str]:
    auth_val = _mb_auth_value(token, device_id)
    h: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": UA,
        "Authorization": auth_val,
        "X-Emby-Authorization": auth_val,
    }
    if token:
        h["X-MediaBrowser-Token"] = token
    return h


def _raise_with_details(resp: Any, default: str) -> None:
    msg = default
    try:
        j = resp.json() or {}
        msg = j.get("ErrorMessage") or j.get("Message") or msg
    except Exception:
        t = (getattr(resp, "text", "") or "").strip()
        if t:
            msg = f"{default}: {t[:200]}"
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
                {
                    "key": "jellyfin.server",
                    "label": "Server URL",
                    "type": "text",
                    "required": True,
                },
                {
                    "key": "jellyfin.username",
                    "label": "Username",
                    "type": "text",
                    "required": True,
                },
                {
                    "key": "jellyfin.password",
                    "label": "Password",
                    "type": "password",
                    "required": False,
                },
            ],
            actions={"start": True, "finish": False, "refresh": False, "disconnect": True},
            notes="Sign in with your Jellyfin account to obtain a user access token.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {
            "features": {
                "watchlist": {"read": True, "write": True},
                "ratings": {"read": True, "write": True},
                "watched": {"read": True, "write": True},
                "playlists": {"read": True, "write": True},
            },
            "entity_types": ["movie", "show", "episode"],
        }

    def get_status(self, cfg: Mapping[str, Any], instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        if inst == "default" or not isinstance(cfg, dict):
            jf = (cfg.get("jellyfin") or {}) if isinstance(cfg, Mapping) else {}
        else:
            cfg_dict = cast(dict[str, Any], cfg)
            jf = ensure_instance_block(cfg_dict, "jellyfin", inst)
        server = (jf.get("server") or "").strip()
        token = (jf.get("access_token") or "").strip()
        user = (jf.get("user") or jf.get("username") or "").strip() or None
        return AuthStatus(connected=bool(server and token), label="Jellyfin", user=user)

    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str, instance_id: Any = None) -> dict[str, Any]:
        import requests
        from requests import exceptions as rx

        inst = normalize_instance_id(instance_id)

        cfg_dict = cast(dict[str, Any], cfg)
        jf = ensure_instance_block(cfg_dict, "jellyfin", inst)
        base = _clean_base(jf.get("server", ""))
        user = (jf.get("username") or "").strip()
        pw = str(jf.get("password") or "").strip()
        if not base:
            raise RuntimeError("Malformed request: missing server")
        if not user:
            raise RuntimeError("Malformed request: missing username")

        dev_id = (jf.get("device_id") or "").strip() or secrets.token_hex(16)
        jf["device_id"] = dev_id
        jf["server"] = base

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

        if r.status_code in (401, 403):
            raise RuntimeError("Invalid credentials")
        if r.status_code >= 500:
            raise RuntimeError(f"Server error ({r.status_code})")
        if not r.ok:
            _raise_with_details(r, "Login failed")

        data = r.json() or {}
        token = (data.get("AccessToken") or "").strip()
        if not token:
            raise RuntimeError("Login failed: no access token returned")

        user_obj = data.get("User") or {}
        user_id = (user_obj.get("Id") or "").strip()
        display = (user_obj.get("Name") or user).strip()

        try:
            me = requests.get(
                urljoin(base, "Users/Me"),
                headers=_headers(token, dev_id),
                timeout=HTTP_TIMEOUT_GET,
            )
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

    def finish(self, cfg: MutableMapping[str, Any], **payload: Any) -> AuthStatus:
        return self.get_status(cfg)

    def refresh(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        return self.get_status(cfg)

    def disconnect(self, cfg: MutableMapping[str, Any], instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)

        cfg_dict = cast(dict[str, Any], cfg)
        jf = ensure_instance_block(cfg_dict, "jellyfin", inst)
        for k in ("access_token", "user_id"):
            jf.pop(k, None)
        log("Jellyfin: disconnected", level="INFO", module="AUTH")
        return self.get_status(cfg, inst)


PROVIDER = JellyfinAuth()
__all__ = ["PROVIDER", "JellyfinAuth", "html", "__VERSION__"]


def html() -> str:
    return r'''<div class="section" id="sec-jellyfin">
  <style>
    #sec-jellyfin .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    #sec-jellyfin .inline{display:flex;gap:8px;align-items:center}
    #sec-jellyfin .sub{opacity:.7;font-size:.92em}
    #sec-jellyfin input[type="checkbox"]{transform:translateY(1px)}
    #sec-jellyfin .inp-row{display:flex;gap:12px;align-items:center}
    #sec-jellyfin .inp-row .grow{flex:1 1 auto}
    #sec-jellyfin .verify{display:flex;gap:8px;align-items:center;white-space:nowrap}
    #sec-jellyfin .btn.danger{ background:#a8182e; border-color:rgba(255,107,107,.4) }
    #sec-jellyfin .btn.danger:hover{ filter:brightness(1.08) }

    /* matrix */
    #sec-jellyfin .lm-head{display:grid;grid-template-columns:1fr auto auto auto auto auto;gap:10px;align-items:center;margin-bottom:8px}
    #sec-jellyfin .lm-head .title{font-weight:700}
    #sec-jellyfin .lm-rows{
      display:grid;gap:6px;max-height:280px;min-height:200px;
      overflow:auto;border:1px solid var(--border);border-radius:10px;padding:8px;background:#090b10
    }
    #sec-jellyfin .lm-row{display:grid;grid-template-columns:1fr 40px 40px 40px;gap:6px;align-items:center;background:#0b0d12;border-radius:8px;padding:6px 8px}
    #sec-jellyfin .lm-row.hide{display:none}
    #sec-jellyfin .lm-name{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    #sec-jellyfin .lm-dot{width:16px;height:16px;border-radius:50%;border:2px solid currentColor;background:transparent;cursor:pointer;display:inline-block;vertical-align:middle}
    #sec-jellyfin .lm-dot.hist{color:#b066ff;box-shadow:0 0 6px rgba(176,102,255,.55)}
    #sec-jellyfin .lm-dot.hist.on{background:#b066ff;box-shadow:0 0 10px rgba(176,102,255,.95)}
    #sec-jellyfin .lm-dot.rate{color:#00d1ff;box-shadow:0 0 6px rgba(0,209,255,.55)}
    #sec-jellyfin .lm-dot.rate.on{background:#00d1ff;box-shadow:0 0 10px rgba(0,209,255,.95)}
    #sec-jellyfin .lm-dot.scr{color:#35ff8f;box-shadow:0 0 6px rgba(53,255,143,.55)}
    #sec-jellyfin .lm-dot.scr.on{background:#35ff8f;box-shadow:0 0 10px rgba(53,255,143,.95)}
    #sec-jellyfin .lm-col{display:flex;align-items:center;gap:6px}
    #sec-jellyfin .lm-filter{min-width:160px}
    #sec-jellyfin select.lm-hidden{display:none}

    #sec-jellyfin .inline .msg{margin-left:auto}
    #sec-jellyfin .inline .msg.hidden{display:none}

    /* Jellyfin Sign in */
    #sec-jellyfin .btn.jellyfin{
      background: linear-gradient(135deg,#00e084,#2ea859);
      border-color: rgba(0,224,132,.45);
      box-shadow: 0 0 14px rgba(0,224,132,.35);
      color: #fff;
    }
    #sec-jellyfin .btn.jellyfin:hover{
      filter: brightness(1.06);
      box-shadow: 0 0 18px rgba(0,224,132,.5);
    }
  </style>

  <div class="head" onclick="toggleSection && toggleSection('sec-jellyfin')">
    <span class="chev">▶</span><strong>Jellyfin</strong>
  </div>

  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="jellyfin">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">Jellyfin</div>
            <div class="muted">Sign in, tune settings, and whitelist libraries.</div>
          </div>
        </div>

        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
          <button type="button" class="cw-subtile" data-sub="settings">Settings</button>
          <button type="button" class="cw-subtile" data-sub="whitelist">Whitelisting</button>
        </div>

        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="grid2">
              <div>
                <label>Server URL</label>
                <div class="inp-row">
                  <input id="jfy_server" class="grow" placeholder="http://host:8096/">
                  <label class="verify"><input id="jfy_verify_ssl" type="checkbox"> Verify SSL</label>
                </div>
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
              <button class="btn danger" onclick="try{ jfyDeleteToken && jfyDeleteToken(); }catch(_){;}">Delete</button>
              <div id="jfy_msg" class="msg ok hidden" role="status" aria-live="polite"></div>
            </div>
          </div>

          <div class="cw-subpanel" data-sub="settings">
            <div style="max-width:820px">
              <label>Server URL</label>
              <div class="inp-row">
                <input id="jfy_server_url" class="grow" placeholder="http://host:8096/">
                <label class="verify"><input id="jfy_verify_ssl_dup" type="checkbox" onclick="(function(){var a=document.getElementById('jfy_verify_ssl'); if(a) a.checked = document.getElementById('jfy_verify_ssl_dup').checked;})();"> Verify SSL</label>
              </div>
              <div class="sub">Leave blank to discover.</div>

              <label style="margin-top:10px">Username</label>
              <input id="jfy_username" placeholder="Display name">

              <label style="margin-top:10px">User_ID</label>
              <div class="inp-row">
                <input id="jfy_user_id" class="grow" placeholder="e.g. 6f7a0b3b-... (GUID)">
                <button id="jfy_pick_user" class="btn" type="button">Pick user</button>
              </div>
              <div class="sub">Uses your current token. Admin tokens show all users; otherwise you'll only see yourself.</div>

              <div class="inline" style="gap:12px;margin-top:12px">
                <button class="btn" onclick="(window.jfyAuto||function(){})();">Auto-Fetch</button>
                <span class="sub" style="margin-left:auto">Edit values before Save if needed.</span>
              </div>
            </div>
          </div>

          <div class="cw-subpanel" data-sub="whitelist">
            <div style="max-width:980px">
              <div class="inline" style="gap:12px;margin-top:0;margin-bottom:12px">
                <button class="btn" title="Load Jellyfin libraries" onclick="(window.jfyLoadLibraries||function(){})();">Load libraries</button>
                <span class="sub" style="margin-left:auto">Refresh after changing Server URL / token.</span>
              </div>

              <div class="lm-head">
                <div class="title">Whitelist Libraries</div>
                <input id="jfy_lib_filter" class="lm-filter" placeholder="Filter…">
                <div class="lm-col"><span class="sub">Select all:</span></div>
                <div class="lm-col"><button id="jfy_hist_all" type="button" class="lm-dot hist" title="Toggle all History" aria-pressed="false"></button><span class="sub">History</span></div>
                <div class="lm-col"><button id="jfy_rate_all" type="button" class="lm-dot rate" title="Toggle all Ratings" aria-pressed="false"></button><span class="sub">Ratings</span></div>
                <div class="lm-col"><button id="jfy_scr_all" type="button" class="lm-dot scr" title="Toggle all Scrobble" aria-pressed="false"></button><span class="sub">Scrobble</span></div>
              </div>
              <div id="jfy_lib_matrix" class="lm-rows"></div>
              <div class="sub" style="margin-top:6px">Empty = all libraries.</div>
              <select id="jfy_lib_history" class="lm-hidden" multiple></select>
              <select id="jfy_lib_ratings" class="lm-hidden" multiple></select>
              <select id="jfy_lib_scrobble" class="lm-hidden" multiple></select>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>

'''