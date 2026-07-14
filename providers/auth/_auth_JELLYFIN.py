# providers/auth/_auth_JELLYFIN.py
# CrossWatch - Orchestrator
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import threading
import time
from collections.abc import Mapping, MutableMapping
from typing import Any, cast

from cw_platform.provider_instances import ensure_instance_block, normalize_instance_id
from providers.sync.jellyfin._auth_http import (
    JellyfinAuthError,
    JellyfinAuthSession,
    MINIMUM_SERVER_VERSION_TEXT,
    clean_base,
    new_device_id,
    validate_server_version,
)

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

__VERSION__ = "2.1.0"

_QC_TTL = 600
_qc_lock = threading.Lock()
_qc_sessions: dict[str, dict[str, Any]] = {}


def _qc_prune(now: float | None = None) -> None:
    ts = now if now is not None else time.time()
    for key in [k for k, v in _qc_sessions.items() if ts - float(v.get("created_at", 0)) > _QC_TTL]:
        _qc_sessions.pop(key, None)


def _finalize_token(
    jf: MutableMapping[str, Any],
    *,
    base: str,
    device_id: str,
    token: str,
    user_id: str,
    display: str,
    server_version: str,
    method: str,
) -> None:
    jf["access_token"] = token
    jf["user_id"] = user_id or jf.get("user_id") or ""
    jf["user"] = display or jf.get("user") or jf.get("username") or ""
    jf["server_version"] = server_version
    jf["device_id"] = device_id
    jf["server"] = base
    jf["auth_method"] = method
    jf.pop("password", None)


class JellyfinAuth(AuthProvider):
    name = "JELLYFIN"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            "JELLYFIN",
            "Jellyfin",
            "token",
            [
                {"key": "jellyfin.server", "label": "Server URL", "type": "text", "required": True},
                {"key": "jellyfin.username", "label": "Username", "type": "text", "required": False},
                {"key": "jellyfin.password", "label": "Password", "type": "password", "required": False},
            ],
            {"start": True, "finish": False, "refresh": False, "disconnect": True},
            None,
            "Connect with Quick Connect or your Jellyfin username and password to obtain a user access token.",
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

    def _block(self, cfg: Mapping[str, Any], inst: str) -> dict[str, Any]:
        if inst == "default" or not isinstance(cfg, dict):
            return (cfg.get("jellyfin") or {}) if isinstance(cfg, Mapping) else {}
        return ensure_instance_block(cast(dict[str, Any], cfg), "jellyfin", inst)

    def get_status(self, cfg: Mapping[str, Any], instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        jf = self._block(cfg, inst)
        server = (jf.get("server") or "").strip()
        token = (jf.get("access_token") or "").strip()
        user = (jf.get("user") or jf.get("username") or "").strip() or None
        method = (jf.get("auth_method") or "").strip() or None
        return AuthStatus(
            connected=bool(server and token),
            label="Jellyfin",
            user=user,
            extra={"auth_method": method} if method else {},
        )

    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str | None = None, *, instance_id: Any = None) -> dict[str, Any]:
        inst = normalize_instance_id(instance_id)
        cfg_dict = cast(dict[str, Any], cfg)
        jf = ensure_instance_block(cfg_dict, "jellyfin", inst)

        base = clean_base(jf.get("server", ""))
        user = (jf.get("username") or "").strip()
        pw = str(jf.get("password") or "")
        if not base:
            raise JellyfinAuthError("Malformed request: missing server", reason="missing_server")
        if not user:
            raise JellyfinAuthError("Malformed request: missing username", reason="missing_username")

        dev_id = (jf.get("device_id") or "").strip() or new_device_id()
        jf["device_id"] = dev_id
        jf["server"] = base

        log("Jellyfin: authenticating...", level="INFO", module="AUTH")
        with JellyfinAuthSession(base, device_id=dev_id, verify_ssl=bool(jf.get("verify_ssl", False))) as session:
            data = session.authenticate_by_name(user, pw)
            token = str(data.get("AccessToken") or "").strip()
            if not token:
                raise JellyfinAuthError("Login failed: no access token returned", reason="login_failed")
            session.token = token

            user_obj = data.get("User") or {}
            user_id = str(user_obj.get("Id") or "").strip()
            display = str(user_obj.get("Name") or user).strip()

            server_version = session.server_version()
            me_id, me_name = session.resolve_user(token)
            user_id = me_id or user_id
            display = me_name or display

        _finalize_token(
            jf,
            base=base,
            device_id=dev_id,
            token=token,
            user_id=user_id,
            display=display,
            server_version=server_version,
            method="password",
        )
        with _qc_lock:
            _qc_sessions.pop(inst, None)
        log("Jellyfin: access token stored", level="SUCCESS", module="AUTH")
        return {"ok": True, "mode": "user_token", "user_id": jf.get("user_id") or "", "server_version": server_version, "auth_method": "password"}

    def quick_connect_available(self, server: str, *, verify_ssl: bool = False) -> dict[str, Any]:
        base = clean_base(server)
        if not base:
            return {"ok": False, "supported": False, "enabled": False, "reason": "missing_server", "error": "Missing server URL"}
        try:
            with JellyfinAuthSession(base, verify_ssl=verify_ssl) as session:
                version = session.server_version(require_token=False)
                enabled = session.quick_connect_enabled()
            return {"ok": True, "supported": True, "enabled": bool(enabled), "server_version": version}
        except JellyfinAuthError as exc:
            return {"ok": False, "supported": False, "enabled": False, "reason": exc.reason, "error": str(exc)}

    def quick_connect_start(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> dict[str, Any]:
        inst = normalize_instance_id(instance_id)
        cfg_dict = cast(dict[str, Any], cfg)
        jf = ensure_instance_block(cfg_dict, "jellyfin", inst)

        base = clean_base(jf.get("server", ""))
        if not base:
            raise JellyfinAuthError("Malformed request: missing server", reason="missing_server")
        verify_ssl = bool(jf.get("verify_ssl", False))
        dev_id = (jf.get("device_id") or "").strip() or new_device_id()

        with JellyfinAuthSession(base, device_id=dev_id, verify_ssl=verify_ssl) as session:
            validate_server_version(session.system_info(require_token=False).get("Version"))
            if not session.quick_connect_enabled():
                raise JellyfinAuthError("Quick Connect is disabled on this server", reason="disabled")
            result = session.quick_connect_initiate()

        jf["device_id"] = dev_id
        jf["server"] = base

        now = time.time()
        with _qc_lock:
            _qc_prune(now)
            _qc_sessions[inst] = {
                "secret": str(result.get("Secret") or ""),
                "device_id": dev_id,
                "server": base,
                "verify_ssl": verify_ssl,
                "created_at": now,
            }
        log(f"Jellyfin: Quick Connect initiated (instance={inst})", level="INFO", module="AUTH")
        return {"ok": True, "code": str(result.get("Code") or ""), "method": "quick_connect", "expires_in": _QC_TTL}

    def quick_connect_poll(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> dict[str, Any]:
        inst = normalize_instance_id(instance_id)
        now = time.time()
        with _qc_lock:
            _qc_prune(now)
            state = _qc_sessions.get(inst)
            state = dict(state) if state else None
        if not state:
            return {"ok": False, "state": "expired", "error": "No active Quick Connect request"}

        secret = str(state.get("secret") or "")
        base = str(state.get("server") or "")
        dev_id = str(state.get("device_id") or "")
        verify_ssl = bool(state.get("verify_ssl", False))

        try:
            with JellyfinAuthSession(base, device_id=dev_id, verify_ssl=verify_ssl) as session:
                status = session.quick_connect_state(secret)
                if not status.get("Authenticated"):
                    return {"ok": True, "state": "waiting"}
                data = session.authenticate_with_quick_connect(secret)
                token = str(data.get("AccessToken") or "").strip()
                if not token:
                    raise JellyfinAuthError("Quick Connect authentication failed", reason="connect_failed")
                user_obj = data.get("User") or {}
                user_id = str(user_obj.get("Id") or "").strip()
                display = str(user_obj.get("Name") or "").strip()
                server_version = session.server_version(require_token=False)
                me_id, me_name = session.resolve_user(token)
                user_id = me_id or user_id
                display = me_name or display
        except JellyfinAuthError as exc:
            if exc.reason == "expired":
                with _qc_lock:
                    _qc_sessions.pop(inst, None)
                return {"ok": False, "state": "expired", "error": str(exc)}
            raise

        cfg_dict = cast(dict[str, Any], cfg)
        jf = ensure_instance_block(cfg_dict, "jellyfin", inst)
        _finalize_token(
            jf,
            base=base,
            device_id=dev_id,
            token=token,
            user_id=user_id,
            display=display,
            server_version=server_version,
            method="quick_connect",
        )
        with _qc_lock:
            _qc_sessions.pop(inst, None)
        log(f"Jellyfin: Quick Connect authorized (instance={inst})", level="SUCCESS", module="AUTH")
        return {
            "ok": True,
            "state": "authorized",
            "user_id": jf.get("user_id") or "",
            "username": jf.get("user") or jf.get("username") or None,
            "server": jf.get("server") or None,
            "server_version": server_version,
            "auth_method": "quick_connect",
        }

    def quick_connect_cancel(self, *, instance_id: Any = None) -> dict[str, Any]:
        inst = normalize_instance_id(instance_id)
        with _qc_lock:
            existed = _qc_sessions.pop(inst, None) is not None
        if existed:
            log(f"Jellyfin: Quick Connect cancelled (instance={inst})", level="INFO", module="AUTH")
        return {"ok": True, "cancelled": existed}

    def finish(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None, **payload: Any) -> AuthStatus:
        return self.get_status(cfg, instance_id)

    def refresh(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        return self.get_status(cfg, instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any], instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        cfg_dict = cast(dict[str, Any], cfg)
        jf = ensure_instance_block(cfg_dict, "jellyfin", inst)
        for k in ("access_token", "user_id", "auth_method"):
            jf.pop(k, None)
        with _qc_lock:
            _qc_sessions.pop(inst, None)
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

    #sec-jellyfin .inline .msg{margin-left:auto}
    #sec-jellyfin .inline .msg.hidden{display:none}
    #sec-jellyfin .hidden{display:none !important}
    #sec-jellyfin .jfy-actions-row{display:flex;gap:12px;align-items:center;justify-content:flex-end;margin-top:12px;flex-wrap:wrap}
    #sec-jellyfin .jfy-actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
    #sec-jellyfin .jfy-actions-row .msg{margin-left:0}

    #sec-jellyfin .btn.jellyfin{
      background: linear-gradient(135deg,#00e084,#2ea859) !important;
      border-color: rgba(0,224,132,.45) !important;
      box-shadow: 0 0 14px rgba(0,224,132,.35);
      color: #fff !important;
    }
    #sec-jellyfin .btn.jellyfin:hover{
      filter: brightness(1.06);
      box-shadow: 0 0 18px rgba(0,224,132,.5);
    }

    #sec-jellyfin .jfy-methods{display:flex;gap:8px;margin-top:14px}
    #sec-jellyfin .jfy-method{
      appearance:none;cursor:pointer;flex:1 1 0;padding:10px 12px;border-radius:10px;
      border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.03);
      color:inherit;font:inherit;display:flex;align-items:center;justify-content:center;gap:8px;
      transition:border-color .15s ease, background .15s ease;
    }
    #sec-jellyfin .jfy-method:hover{border-color:rgba(0,224,132,.5)}
    #sec-jellyfin .jfy-method.active{
      border-color:rgba(0,224,132,.7);
      background:linear-gradient(135deg,rgba(0,224,132,.16),rgba(46,168,89,.10));
      box-shadow:0 0 12px rgba(0,224,132,.20);
    }
    #sec-jellyfin .jfy-method .badge{
      display:inline-flex;align-items:center;line-height:1;
      font-size:.72em;text-transform:uppercase;letter-spacing:.05em;padding:3px 7px;border-radius:999px;
      background:rgba(0,224,132,.22);border:1px solid rgba(0,224,132,.45);color:#8ff0c2;
    }
    #sec-jellyfin .jfy-pane{margin-top:12px}
    #sec-jellyfin .jfy-qc{margin-top:12px;padding:14px;border-radius:12px;border:1px solid rgba(0,224,132,.35);background:rgba(0,224,132,.06)}
    #sec-jellyfin .jfy-qc-codewrap{display:flex;align-items:center;justify-content:center;gap:12px}
    #sec-jellyfin .jfy-qc-code{
      font-size:2em;font-weight:700;letter-spacing:.24em;text-align:center;
      font-variant-numeric:tabular-nums;padding:6px 0;color:#8ff0c2;
    }
    #sec-jellyfin .jfy-qc-code{letter-spacing:.24em;padding-left:.24em}
    #sec-jellyfin .jfy-qc-copy{
      appearance:none;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;
      width:34px;height:34px;border-radius:9px;flex:0 0 auto;
      border:1px solid rgba(0,224,132,.35);background:rgba(0,224,132,.08);color:#8ff0c2;
      transition:background .15s ease, border-color .15s ease, color .15s ease, transform .12s ease;
    }
    #sec-jellyfin .jfy-qc-copy:hover{background:rgba(0,224,132,.16);border-color:rgba(0,224,132,.6)}
    #sec-jellyfin .jfy-qc-copy:active{transform:scale(.94)}
    #sec-jellyfin .jfy-qc-copy.copied{background:rgba(0,224,132,.24);border-color:rgba(0,224,132,.75)}
    #sec-jellyfin .jfy-qc-copy svg{width:16px;height:16px;display:block}
    #sec-jellyfin .jfy-qc-meta{display:flex;justify-content:space-between;gap:12px;margin-top:6px}
    #sec-jellyfin .jfy-conn-note{margin-top:8px}
    #sec-jellyfin #jfy_method_note.warn{color:#ffb4b4;opacity:.95}
    @media(max-width:900px){#sec-jellyfin .jfy-actions-row{justify-content:flex-start}}
  </style>

  <div class="head" data-toggle-section="sec-jellyfin">
    <span class="chev"></span><strong>Jellyfin</strong>
  </div>

  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="jellyfin">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">Jellyfin</div>
            <div class="muted">Connect, tune settings, and whitelist libraries.</div>
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
              <div style="grid-column:1 / -1">
                <label for="jfy_server">Server URL</label>
                <div class="inp-row">
                  <input id="jfy_server" name="jfy_server" class="grow" placeholder="http://host:8096/">
                  <label class="verify"><input id="jfy_verify_ssl" type="checkbox"> Verify SSL</label>
                </div>
              </div>
            </div>

            <div class="jfy-methods" role="tablist" aria-label="Authentication method">
              <button type="button" class="jfy-method active" data-method="quick" role="tab" aria-selected="true">
                Quick Connect <span class="badge">Recommended</span>
              </button>
              <button type="button" class="jfy-method" data-method="password" role="tab" aria-selected="false">
                Username and Password
              </button>
            </div>
            <div id="jfy_method_note" class="sub jfy-conn-note" role="status" aria-live="polite"></div>

            <div class="jfy-pane" data-method="quick">
              <div id="jfy_qc_state" class="jfy-qc hidden">
                <div class="jfy-qc-codewrap">
                  <div class="jfy-qc-code" id="jfy_qc_code">------</div>
                  <button type="button" id="jfy_qc_copy" class="jfy-qc-copy" title="Copy code" aria-label="Copy code">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                  </button>
                </div>
                <div class="sub" id="jfy_qc_help">In Jellyfin open your user menu &rarr; Quick Connect, then enter this code and authorize.</div>
                <div class="jfy-qc-meta">
                  <span class="sub" id="jfy_qc_status">Waiting for authorization&hellip;</span>
                  <span class="sub" id="jfy_qc_timer"></span>
                </div>
              </div>
            </div>

            <div class="jfy-pane hidden" data-method="password">
              <div class="grid2" style="margin-top:4px">
                <div>
                  <label for="jfy_user">Username</label>
                  <input id="jfy_user" name="jfy_user" placeholder="username" autocomplete="username">
                </div>
                <div>
                  <label for="jfy_pass">Password</label>
                  <input id="jfy_pass" name="jfy_pass" type="password" placeholder="********" autocomplete="current-password">
                </div>
              </div>
            </div>

            <div class="jfy-actions-row">
              <div class="jfy-actions" data-method-actions="quick">
                <button id="btn-jfy-qc-start" class="btn jellyfin" type="button">Start Quick Connect</button>
                <button id="btn-jfy-qc-cancel" class="btn danger hidden" type="button">Cancel</button>
                <button id="btn-jfy-qc-restart" class="btn jellyfin hidden" type="button">Restart</button>
                <button class="btn danger cw-jfy-delete" type="button">Delete</button>
              </div>
              <div class="jfy-actions hidden" data-method-actions="password">
                <button id="btn-jfy-login" class="btn jellyfin" type="button">Sign in with password</button>
                <button class="btn danger cw-jfy-delete" type="button">Delete</button>
              </div>
              <div id="jfy_msg" class="msg ok hidden" role="status" aria-live="polite"></div>
            </div>
          </div>

          <div class="cw-subpanel" data-sub="settings">
            <div style="max-width:820px">
              <label for="jfy_server_url">Server URL</label>
              <div class="inp-row">
                <input id="jfy_server_url" name="jfy_server_url" class="grow" placeholder="http://host:8096/">
                <label class="verify"><input id="jfy_verify_ssl_dup" type="checkbox"> Verify SSL</label>
              </div>
              <div class="sub">Leave blank to discover.</div>

              <label for="jfy_username" style="margin-top:10px">Username</label>
              <input id="jfy_username" name="jfy_username" placeholder="Display name">

              <label for="jfy_user_id" style="margin-top:10px">User_ID</label>
              <div class="inp-row">
                <input id="jfy_user_id" name="jfy_user_id" class="grow" placeholder="e.g. 6f7a0b3b-... (GUID)">
                <button id="jfy_pick_user" class="btn" type="button">Pick user</button>
              </div>
              <div class="sub">Uses your signed-in account. Admin accounts show all users; otherwise you'll only see yourself.</div>

              <div class="inline" style="gap:12px;margin-top:12px">
                <button id="btn-jfy-auto" class="btn" type="button">Auto-Fetch</button>
                <span class="sub" style="margin-left:auto">Edit values before Save if needed.</span>
              </div>
            </div>
          </div>

          <div class="cw-subpanel" data-sub="whitelist">
            <div style="max-width:980px">
              <div class="lm-head">
                <div class="title">Whitelist Libraries</div>
                <div class="lm-col"><span class="sub">Select all:</span></div>
                <div class="lm-col"><button id="jfy_hist_all" type="button" class="lm-dot hist" title="Toggle all History" aria-pressed="false"></button><span class="sub">History</span></div>
                <div class="lm-col"><button id="jfy_rate_all" type="button" class="lm-dot rate" title="Toggle all Ratings" aria-pressed="false"></button><span class="sub">Ratings</span></div>
                <div class="lm-col"><button id="jfy_prog_all" type="button" class="lm-dot prog" title="Toggle all Progress" aria-pressed="false"></button><span class="sub">Progress</span></div>
                <div class="lm-col"><button id="jfy_scr_all" type="button" class="lm-dot scr" title="Toggle all Scrobble" aria-pressed="false"></button><span class="sub">Scrobble</span></div>
              </div>
              <div id="jfy_lib_matrix" class="lm-rows"></div>
              <div class="inline" style="gap:12px;margin-top:12px;margin-bottom:0">
                <button id="btn-jfy-load-libraries" class="btn" type="button" title="Load Jellyfin libraries">Load libraries</button>
              </div>
              <div class="sub" style="margin-top:6px">Empty = all libraries.</div>
              <select id="jfy_lib_history" name="jfy_lib_history" class="lm-hidden" multiple></select>
              <select id="jfy_lib_ratings" name="jfy_lib_ratings" class="lm-hidden" multiple></select>
              <select id="jfy_lib_progress" name="jfy_lib_progress" class="lm-hidden" multiple></select>
              <select id="jfy_lib_scrobble" name="jfy_lib_scrobble" class="lm-hidden" multiple></select>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>

'''
