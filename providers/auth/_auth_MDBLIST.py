# providers/auth/_auth_MDBLIST.py
# CrossWatch - MDBList Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

import requests

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, get_provider_block, normalize_instance_id
from providers.sync.mdblist import _auth as mdblist_auth

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
__VERSION__ = "2.1.0"


def _load_config() -> dict[str, Any]:
    try:
        return dict(load_config() or {})
    except Exception:
        return {}


def _block(cfg: Mapping[str, Any], instance_id: Any = None) -> dict[str, Any]:
    return get_provider_block(cfg or {}, "mdblist", instance_id)


def _get(cfg: Mapping[str, Any], path: str, *, instance_id: Any = None, timeout: int = HTTP_TIMEOUT) -> tuple[int, dict[str, Any]]:
    if not mdblist_auth.is_configured(_block(cfg, instance_id)):
        return 0, {}
    try:
        session = requests.Session()
        r = mdblist_auth.request_with_auth(
            session,
            "GET",
            f"{API_BASE}{path}",
            cfg=cfg,
            instance_id=instance_id,
            timeout=timeout,
            max_retries=1,
            headers={"Accept": "application/json", "User-Agent": UA},
        )
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
            flow="device_code",
            fields=[
                {
                    "key": "mdblist.api_key",
                    "label": "API Key",
                    "type": "password",
                    "required": False,
                    "placeholder": "********",
                },
            ],
            actions={"start": True, "finish": True, "refresh": True, "disconnect": True},
            notes="Device Code is preferred. API key remains available as a legacy option.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"watchlist": True, "ratings": True, "history": True}

    def get_status(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        status = mdblist_auth.status_for_block(_block(cfg, inst))
        label = "MDBList" if inst == "default" else f"MDBList ({inst})"
        return AuthStatus(
            connected=bool(status.get("connected")),
            label=label,
            user=str(status.get("username") or "") or None,
            expires_at=int(status.get("expires_at") or 0) or None,
        )

    def start(self, cfg: MutableMapping[str, Any] | None = None, *, redirect_uri: str | None = None, instance_id: Any = None) -> dict[str, Any]:
        cfgd = dict(cfg or _load_config() or {})
        return mdblist_auth.start_device_code(cfgd, instance_id=instance_id)

    def finish(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None, **payload: Any) -> AuthStatus:
        cfgd = dict(cfg or _load_config() or {})
        b = ensure_instance_block(cfgd, "mdblist", instance_id)
        method = mdblist_auth.normalize_auth_method(payload.get("auth_method") or b.get("auth_method"), b)
        if method == "api_key":
            b["api_key"] = str(payload.get("api_key") or payload.get("mdblist.api_key") or "").strip()
            mdblist_auth.set_active_method(b, "api_key")
        else:
            b.pop("client_id", None)
            mdblist_auth.set_active_method(b, "device_code")
        save_config(cfgd)
        log(f"MDBList auth saved (method={method}, instance={normalize_instance_id(instance_id)}).", module="AUTH")
        return self.get_status(cfgd, instance_id=instance_id)

    def refresh(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        try:
            mdblist_auth.refresh_token(dict(cfg or _load_config() or {}), instance_id=instance_id)
        except Exception:
            pass
        return self.get_status(cfg, instance_id=instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfgd = dict(cfg or _load_config() or {})
        b = ensure_instance_block(cfgd, "mdblist", instance_id)
        b["api_key"] = ""
        mdblist_auth.clear_oauth(b)
        save_config(cfgd)
        log(f"MDBList disconnected (instance={normalize_instance_id(instance_id)}).", module="AUTH")
        return self.get_status(cfgd, instance_id=instance_id)


PROVIDER = MDBListAuth()


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
    #sec-mdblist .mdbl-method-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:12px;align-items:center;margin-top:14px}
    #sec-mdblist .mdbl-actions{display:flex;align-items:center;gap:8px;justify-content:flex-end;flex-wrap:wrap}
    #sec-mdblist .mdbl-api-field-row{display:flex;gap:12px;align-items:center;max-width:760px}
    #sec-mdblist .mdbl-api-field-row input{flex:1 1 300px;min-width:0}
    #sec-mdblist .mdbl-api-field-row .msg{flex:0 1 auto;margin-top:0}
    #sec-mdblist #mdblist_save,#sec-mdblist #mdblist_device_start{
      background: linear-gradient(135deg,#00e084,#2ea859);
      border-color: rgba(0,224,132,.45);
      box-shadow: 0 0 14px rgba(0,224,132,.35);
      color: #fff;
      min-width: 146px;
    }
    #sec-mdblist #mdblist_save:hover,#sec-mdblist #mdblist_device_start:hover{
      filter: brightness(1.06);
      box-shadow: 0 0 18px rgba(0,224,132,.5);
    }
    #sec-mdblist .mdbl-methods{display:flex;gap:8px;min-width:0}
    #sec-mdblist .mdbl-method{
      appearance:none;cursor:pointer;flex:1 1 0;padding:10px 12px;border-radius:10px;
      border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.03);
      color:inherit;font:inherit;display:flex;align-items:center;justify-content:center;gap:8px;
      transition:border-color .15s ease, background .15s ease;
    }
    #sec-mdblist .mdbl-method:hover{border-color:rgba(0,224,132,.5)}
    #sec-mdblist .mdbl-method.active{
      border-color:rgba(0,224,132,.7);
      background:linear-gradient(135deg,rgba(0,224,132,.16),rgba(46,168,89,.10));
      box-shadow:0 0 12px rgba(0,224,132,.20);
    }
    #sec-mdblist .mdbl-method .badge{
      display:inline-flex;align-items:center;line-height:1;
      font-size:.72em;text-transform:uppercase;letter-spacing:.05em;padding:3px 7px;border-radius:999px;
      background:rgba(0,224,132,.22);border:1px solid rgba(0,224,132,.45);color:#8ff0c2;
    }
    #sec-mdblist .mdbl-pane{margin-top:12px}
    #sec-mdblist #mdblist_api_panel .grid2{display:block}

    /* Quick Connect-style link-code card */
    #sec-mdblist .hidden{display:none !important}
    #sec-mdblist .mdbl-qc{margin-top:12px;padding:14px;border-radius:12px;border:1px solid rgba(0,224,132,.35);background:rgba(0,224,132,.06)}
    #sec-mdblist .mdbl-qc-codewrap{display:flex;align-items:center;justify-content:center;gap:12px}
    #sec-mdblist .mdbl-qc-code{
      font-size:2em;font-weight:700;letter-spacing:.24em;padding:6px 0 6px .24em;color:#8ff0c2;
      text-align:center;text-transform:uppercase;font-variant-numeric:tabular-nums;
    }
    #sec-mdblist .mdbl-qc-copy{
      appearance:none;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;
      width:34px;height:34px;border-radius:9px;flex:0 0 auto;
      border:1px solid rgba(0,224,132,.35);background:rgba(0,224,132,.08);color:#8ff0c2;
      transition:background .15s ease, border-color .15s ease, color .15s ease, transform .12s ease;
    }
    #sec-mdblist .mdbl-qc-copy:hover{background:rgba(0,224,132,.16);border-color:rgba(0,224,132,.6)}
    #sec-mdblist .mdbl-qc-copy:active{transform:scale(.94)}
    #sec-mdblist .mdbl-qc-copy.copied{background:rgba(0,224,132,.24);border-color:rgba(0,224,132,.75)}
    #sec-mdblist .mdbl-qc-copy svg{width:16px;height:16px;display:block}
    #sec-mdblist .mdbl-qc-meta{display:flex;justify-content:space-between;gap:12px;margin-top:6px}
    @media(max-width:900px){
      #sec-mdblist .mdbl-method-row{grid-template-columns:1fr}
      #sec-mdblist .mdbl-actions{justify-content:flex-start}
      #sec-mdblist .mdbl-api-field-row{display:block}
      #sec-mdblist .mdbl-api-field-row .msg{margin-top:8px}
    }
  </style>

  <div class="head" data-toggle-section="sec-mdblist">
    <span class="chev">&#9654;</span><strong>MDBList</strong>
  </div>

  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="mdblist">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">MDBList</div>
            <div class="muted">Connect with Device Code or use a legacy API key.</div>
          </div>
        </div>

        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
        </div>

        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="cw-auth-journey" style="--cw-auth-c1:64,132,200;--cw-auth-c2:64,132,200;--cw-auth-logo:url('/assets/img/MDBLIST.svg')">
              <div class="cw-auth-journey-text">
                <div class="cw-auth-journey-title">Connect to MDBList</div>
                <div class="cw-auth-journey-copy">Connect with a Device Code (recommended) or paste a legacy API key. Device Code opens a browser window so you can approve CrossWatch without sharing a key.</div>
              </div>
            </div>

            <div class="mdbl-method-row">
              <div class="mdbl-methods" role="tablist" aria-label="Authentication method">
                <button type="button" class="mdbl-method active" data-method="device_code" role="tab" aria-selected="true">
                  Device Code <span class="badge">Recommended</span>
                </button>
                <button type="button" class="mdbl-method" data-method="api_key" role="tab" aria-selected="false">
                  API Key
                </button>
              </div>
              <div class="mdbl-actions mdbl-actions-device" data-method-actions="device_code">
                <button id="mdblist_device_start" class="btn">Connect MDBList</button>
                <button id="mdblist_device_cancel" class="btn danger hidden" type="button">Cancel</button>
                <button id="mdblist_device_restart" class="btn hidden" type="button">Restart</button>
                <button id="mdblist_disconnect_device" class="btn danger">Delete</button>
              </div>
              <div class="mdbl-actions mdbl-actions-api hidden" data-method-actions="api_key">
                <button id="mdblist_save" class="btn">Connect MDBList</button>
                <button id="mdblist_disconnect_api" class="btn danger">Delete</button>
              </div>
            </div>
            <input id="mdblist_auth_method" name="mdblist_auth_method" type="hidden" value="device_code">

            <div id="mdblist_device_panel" class="mdbl-pane" data-method="device_code">
              <input id="mdblist_device_code" type="hidden">
              <div id="mdblist_qc_state" class="mdbl-qc hidden">
                <div class="mdbl-qc-codewrap">
                  <div class="mdbl-qc-code" id="mdblist_qc_code">------</div>
                  <button type="button" id="mdblist_qc_copy" class="mdbl-qc-copy" title="Copy code" aria-label="Copy code">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                  </button>
                </div>
                <div class="sub" id="mdblist_qc_help">Opening the MDBList approval page &mdash; enter this code there and approve CrossWatch.</div>
                <div class="mdbl-qc-meta">
                  <span class="sub" id="mdblist_qc_status">Waiting for approval&hellip;</span>
                  <span class="sub" id="mdblist_qc_timer"></span>
                </div>
              </div>
            </div>

            <div id="mdblist_api_panel" class="mdbl-pane" data-method="api_key" style="display:none">
              <div class="grid2">
                <div>
                  <label for="mdblist_key">API Key</label>
                  <div class="mdbl-api-field-row">
                    <input id="mdblist_key" name="mdblist_key" type="password" placeholder="********" />
                    <div id="mdblist_hint" class="msg warn">
                      Create API key at
                      <a href="https://mdblist.com/preferences/#api" target="_blank" rel="noopener">MDBList Preferences</a>.
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div class="inline" style="margin-top:10px;justify-content:flex-end">
              <div id="mdblist_msg" class="msg ok hidden" aria-live="polite"></div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
"""
