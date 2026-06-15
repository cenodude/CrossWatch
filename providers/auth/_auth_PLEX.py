# providers/auth/_auth_PLEX.py
# CrossWatch - PLEX Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import secrets
import time
from collections.abc import Mapping, MutableMapping
from typing import Any

import requests

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.config_base import DEFAULT_CFG, save_config
from cw_platform.provider_instances import ensure_provider_block, ensure_instance_block, normalize_instance_id

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


PLEX_PIN_URL = "https://plex.tv/api/v2/pins"
UA = "CrossWatch/1.0"
HTTP_TIMEOUT = 10
__VERSION__ = "2.1.0"


class PlexAuth(AuthProvider):
    name = "PLEX"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="PLEX",
            label="Plex",
            flow="device_pin",
            fields=[],
            actions={"start": True, "finish": True, "refresh": False, "disconnect": True},
            verify_url="https://plex.tv/pin",
            notes="Open Plex, enter the PIN, then click 'Check PIN'.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {
            "features": {
                "watchlist": {"read": True, "write": True},
                "collections": {"read": True, "write": False},
                "ratings": {"read": True, "write": True, "scale": "1-10"},
                "watched": {"read": True, "write": True},
                "liked_lists": {"read": False, "write": False},
            },
            "entity_types": ["movie", "show"],
        }

    def get_status(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        base = (cfg.get("plex") or {}) if isinstance(cfg, Mapping) else {}
        block: Mapping[str, Any] = base if isinstance(base, Mapping) else {}
        if inst != "default":
            insts = block.get("instances")
            inst_block = insts.get(inst) if isinstance(insts, Mapping) else None
            block = inst_block if isinstance(inst_block, Mapping) else {}
        token = str(block.get("account_token") or "").strip()
        label = "Plex" if inst == "default" else f"Plex ({inst})"
        return AuthStatus(connected=bool(token), label=label)

    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str, instance_id: Any = None) -> dict[str, Any]:
        inst = normalize_instance_id(instance_id)
        log(f"Plex[{inst}]: request PIN", level="INFO", module="AUTH")

        cfgd: dict[str, Any] = cfg if isinstance(cfg, dict) else dict(cfg)
        base = ensure_provider_block(cfgd, "plex")
        plex = ensure_instance_block(cfgd, "plex", inst)

        cid = str((base.get("client_id") or plex.get("client_id") or "")).strip()
        if not cid:
            cid = secrets.token_hex(12)
            base["client_id"] = cid
        plex["client_id"] = cid

        headers = {
            "Accept": "application/json",
            "User-Agent": UA,
            "X-Plex-Product": "CrossWatch",
            "X-Plex-Version": "1.0",
            "X-Plex-Client-Identifier": cid,
            "X-Plex-Platform": "Web",
        }

        r = requests.post(PLEX_PIN_URL, headers=headers, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        j = r.json()

        plex["_pending_pin"] = {"id": j["id"], "code": j["code"], "created": int(time.time())}
        save_config(cfgd)

        log(f"Plex[{inst}]: PIN issued", level="SUCCESS", module="AUTH", extra={"pin_id": j.get("id")})
        return {"pin": j.get("code"), "verify_url": "https://plex.tv/pin"}

    def finish(self, cfg: MutableMapping[str, Any], instance_id: Any = None, **payload: Any) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        cfgd: dict[str, Any] = cfg if isinstance(cfg, dict) else dict(cfg)
        plex = ensure_instance_block(cfgd, "plex", inst)
        pend = plex.get("_pending_pin") or {}
        if not pend:
            return AuthStatus(connected=bool(str(plex.get("account_token") or "").strip()), label="Plex")

        base = ensure_provider_block(cfgd, "plex")
        cid = str((base.get("client_id") or plex.get("client_id") or "")).strip()
        url = f"{PLEX_PIN_URL}/{pend['id']}"
        r = requests.get(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": UA,
                "X-Plex-Product": "CrossWatch",
                "X-Plex-Version": "1.0",
                "X-Plex-Client-Identifier": cid,
                "X-Plex-Platform": "Web",
            },
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        j = r.json()

        if j.get("authToken"):
            plex["account_token"] = j["authToken"]
            plex.pop("_pending_pin", None)
            save_config(cfgd)
            log(f"Plex[{inst}]: token stored", level="SUCCESS", module="AUTH")
        return AuthStatus(connected=bool(str(plex.get("account_token") or "").strip()), label="Plex")

    def refresh(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        return self.get_status(cfg, instance_id=instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any], instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        defaults = DEFAULT_CFG.get("plex")
        if not isinstance(defaults, dict) or not defaults:
            return AuthStatus(connected=False, label="Plex")

        cfgd: dict[str, Any] = cfg if isinstance(cfg, dict) else dict(cfg)
        base = cfgd.get("plex")
        if not isinstance(base, dict):
            base = {}
            cfgd["plex"] = base

        def reset_block(dst: dict[str, Any], *, keep_instances: bool) -> None:
            keep = dst.get("instances") if keep_instances else None
            dst.clear()
            dst.update(copy.deepcopy(defaults))
            dst["account_id"] = ""
            if keep_instances and isinstance(keep, dict) and keep:
                dst["instances"] = keep

        if inst == "default":
            reset_block(base, keep_instances=True)
            save_config(cfgd)
            log("Plex[default]: disconnected (reset)", level="INFO", module="AUTH")
            return AuthStatus(connected=False, label="Plex")

        insts = base.get("instances")
        blk = insts.get(inst) if isinstance(insts, dict) else None
        if isinstance(blk, dict):
            reset_block(blk, keep_instances=False)
            save_config(cfgd)
            log(f"Plex[{inst}]: disconnected (reset)", level="INFO", module="AUTH")
            return AuthStatus(connected=False, label="Plex")

        has_base_profile = any(
            str(base.get(k) or "").strip()
            for k in ("account_token", "pms_token", "server_url", "client_id", "machine_id", "username", "account_id")
        )
        if (not isinstance(insts, dict) or not insts) and has_base_profile:
            reset_block(base, keep_instances=True)
            save_config(cfgd)
            log(f"Plex[{inst}]: missing profile, reset default instead", level="INFO", module="AUTH")

        return AuthStatus(connected=False, label="Plex")

PROVIDER = PlexAuth()
__all__ = ["PROVIDER", "PlexAuth", "html", "__VERSION__"]


def html() -> str:
    return r'''<div class="section" id="sec-plex">
  <style>
    #sec-plex details.settings{border:1px dashed var(--border);border-radius:12px;background:#0b0d12;padding:10px 12px 12px;margin-top:8px}
    #sec-plex details.settings .wrap{margin-top:10px;display:grid;grid-template-columns:1fr 1fr;gap:12px;align-items:start}
    #sec-plex .inline{display:flex;gap:8px;align-items:center}
    #sec-plex .btnrow{display:flex;gap:8px;margin-top:12px;margin-bottom:24px}
    #sec-plex .footspace{grid-column:1 / -1;height:48px}
    #sec-plex .sub{opacity:.7;font-size:.92em}
    #sec-plex .btn.danger{ background:#a8182e; border-color:rgba(255,107,107,.4) }
    #sec-plex .btn.danger:hover{ filter:brightness(1.08) }

    /* summary CTA */
    #sec-plex details.settings summary{
      position:relative;display:flex;align-items:center;gap:10px;
      padding:10px 12px;margin:-2px;border-radius:12px;cursor:pointer;list-style:none;
      background:linear-gradient(#0b0d12,#0b0d12) padding-box,
                 linear-gradient(90deg, rgba(176,102,255,.7), rgba(0,209,255,.7)) border-box;
      border:1px solid transparent;box-shadow:inset 0 0 0 1px rgba(255,255,255,.03),0 8px 20px rgba(0,0,0,.35);
      transition:box-shadow .18s ease,transform .18s ease,opacity .18s ease
    }
    #sec-plex details.settings summary .plex-ico{
      width:26px;height:26px;border-radius:999px;display:grid;place-items:center;font-weight:700;color:#fff;
      background:linear-gradient(180deg,#b066ff 0%,#00d1ff 100%);text-shadow:0 1px 2px rgba(0,0,0,.35);
      box-shadow:0 0 10px rgba(176,102,255,.6),0 0 10px rgba(0,209,255,.6)
    }
    #sec-plex details.settings summary .title{font-weight:700;letter-spacing:.2px;position:relative;top:3.5px}
    #sec-plex details.settings summary .hint{opacity:.7;font-size:.92em;margin-left:auto;text-align:right;padding-right:26px}
    #sec-plex details.settings summary::after{
      content:'>';color:#b066ff;text-shadow:0 0 8px rgba(176,102,255,.65);
      position:absolute;right:10px;top:50%;transform:translateY(-50%);transition:transform .18s ease,color .18s ease,text-shadow .18s ease
    }
    #sec-plex details.settings[open] > summary::after{transform:translateY(-50%) rotate(90deg);color:#00d1ff;text-shadow:0 0 10px rgba(0,209,255,.85)}
    #sec-plex details.settings summary:hover,
    #sec-plex details.settings summary:focus-visible{
      box-shadow:inset 0 0 0 1px rgba(255,255,255,.05),0 0 18px rgba(176,102,255,.25),0 0 18px rgba(0,209,255,.25);
      transform:translateY(-1px)
    }

    /* user picker */
    #sec-plex .userpick{position:relative;display:inline-block}
    #sec-plex .userpop{color:#e9eefb}
    #sec-plex .userrow strong{color:#fff}
    #sec-plex .userpop{
      position:fixed;left:0;top:0;width:320px !important;max-width:92vw;max-height:340px;overflow:hidden;
      background:#0b0d12;border:1px solid var(--border);border-radius:12px;padding:8px;z-index:9999;box-shadow:0 12px 30px rgba(0,0,0,.55)
    }
    #sec-plex .userpop.hidden{display:none}
    #sec-plex .userpop .pophead{display:flex;gap:8px;align-items:center;margin-bottom:8px}
    #sec-plex .userpop .pophead input{flex:1;min-width:0}
    #sec-plex .userpop .userlist{display:flex;flex-direction:column;gap:6px;max-height:280px;overflow:auto}
    #sec-plex .userrow{display:block;text-align:left;padding:10px 12px;border-radius:10px;background:#0a0d14;border:1px solid rgba(255,255,255,.06);cursor:pointer}
    #sec-plex .userrow:hover{background:#0f1320;border-color:#b066ff;box-shadow:0 0 0 2px rgba(176,102,255,.15)}
    #sec-plex .userrow .row1{display:flex;align-items:center;gap:10px;margin-bottom:2px}
    #sec-plex .tag{font-size:.75em;opacity:.9;padding:2px 8px;border-radius:999px;border:1px solid var(--border)}
    #sec-plex .tag.owner{color:#b066ff;border-color:#b066ff;box-shadow:0 0 6px rgba(176,102,255,.55)}
    #sec-plex .tag.friend{color:#00d1ff;border-color:#00d1ff;box-shadow:0 0 6px rgba(0,209,255,.55)}
    #sec-plex .tag.managed{color:#35ff8f;border-color:#35ff8f;box-shadow:0 0 6px rgba(53,255,143,.55)}
    #sec-plex .fieldline{display:grid;grid-template-columns:1fr auto;gap:8px;align-items:center}

    /* SSL toggle */
    #sec-plex .sslopt{display:flex;align-items:center;gap:8px;white-space:nowrap}
    #sec-plex .sslopt input[type="checkbox"]{width:18px;height:18px;accent-color:#00d1ff;cursor:pointer}
    #sec-plex .sslopt .lbl{opacity:.9}

    /* neon scrollbar (user list) */
    #sec-plex .userpop .userlist{scrollbar-width:thin;scrollbar-color:#00d1ff #0b0d12}
    #sec-plex .userpop .userlist::-webkit-scrollbar{width:10px}
    #sec-plex .userpop .userlist::-webkit-scrollbar-track{background:#0b0d12;border-radius:10px}
    #sec-plex .userpop .userlist::-webkit-scrollbar-thumb{border-radius:10px;border:2px solid #0b0d12;background:linear-gradient(180deg,#00d1ff 0%,#b066ff 100%);box-shadow:0 0 8px rgba(0,209,255,.55),0 0 8px rgba(176,102,255,.55)}

    /* pin row */
    #sec-plex .pinrow{ margin-top:8px; display:flex; gap:12px; align-items:center; }
    #sec-plex .pinrow .hint{ color:var(--muted); }
    #sec-plex .plexmsg{ margin-left:auto; display:flex; flex-direction:column; align-items:flex-end; gap:4px; }
    #sec-plex #plex_msg{
      display:inline-flex; align-items:center;
      white-space:nowrap; width:auto; min-width:0;
      padding:6px 12px; border-radius:10px;
    }
    #sec-plex #plex_msg.hidden{ display:none; }

    #sec-plex #plex_msg_detail{
      font-size:12px; line-height:1.2;
      color:var(--muted);
      max-width:420px;
      text-align:right;
    }
    #sec-plex #plex_msg_detail.warn{ color:#f7b955; }
    #sec-plex #plex_msg_detail.hidden{ display:none; }
    
    /* Connect PLEX  */
    #sec-plex .pinrow .btn:first-child{
      background: linear-gradient(135deg,#00e084,#2ea859);
      border-color: rgba(0,224,132,.45);
      box-shadow: 0 0 14px rgba(0,224,132,.35);
      color: #fff;
    }
    #sec-plex .pinrow .btn:first-child:hover{
      filter: brightness(1.06);
      box-shadow: 0 0 18px rgba(0,224,132,.5);
    }

    /* Link code (PIN)  */
    #sec-plex #plex_pin { max-width: none; }
    #sec-plex .inline input#plex_pin{
      flex: 1 1 0; min-width: 0; width: auto; font: inherit;
      font-variant-numeric: normal; letter-spacing: normal; text-transform: none;
      line-height: normal; height: auto; padding: 10px 12px;
      background: #0b0d12; border: 1px solid var(--border); border-radius: 12px; box-shadow: none;
    }

  </style>

  <div class="head" data-toggle-section="sec-plex">
    <span class="chev"></span><strong>Plex</strong>
  </div>

  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="plex">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">Plex</div>
            <div class="muted">Connect your account, tune settings, and whitelist libraries.</div>
          </div>
        </div>

        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
          <button type="button" class="cw-subtile" data-sub="settings">Settings</button>
          <button type="button" class="cw-subtile" data-sub="whitelist">Whitelisting</button>
        </div>

        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div>
              <label for="plex_pin">Link code (PIN)</label>
              <div class="inline">
                <input id="plex_pin" name="plex_pin" placeholder="" readonly>
                <button id="btn-copy-plex-pin" type="button" class="btn copy">Copy</button>
              </div>
            </div>

            <div class="inline pinrow">
              <button id="btn-connect-plex" class="btn" type="button">Connect PLEX</button>
              <button id="btn-delete-plex" class="btn danger" type="button">Delete</button>
              <div class="hint">Opens plex.tv/link to complete sign-in.</div>
              <div class="plexmsg">
                <div id="plex_msg" class="msg ok hidden">PIN</div>
                <div id="plex_msg_detail" class="hidden"></div>
              </div>
            </div>
          </div>

          <div class="cw-subpanel" data-sub="settings">
            <div style="max-width:820px">
              <label for="plex_server_url">Server URL</label>
              <div class="fieldline">
                <input id="plex_server_url" name="plex_server_url" placeholder="http://host:32400" list="plex_server_suggestions">
                <datalist id="plex_server_suggestions"></datalist>
                <label class="sslopt" title="Verify server TLS certificate">
                  <input type="checkbox" id="plex_verify_ssl">
                  <span class="lbl">Verify SSL</span>
                </label>
              </div>
              <div class="fieldline" style="margin-top:6px">
                <select id="plex_server_url_select" class="hidden" style="width:100%" title="Discovered server URLs"></select>
              </div>
              <div id="plex_server_url_select_hint" class="sub hidden" style="margin-top:4px">Pick a discovered URL to fill Server URL.</div>

              <div class="sub">Leave blank to discover.</div>

              <label for="plex_username" style="margin-top:10px">Username</label>
              <div class="fieldline userpick">
                <input id="plex_username" name="plex_username" placeholder="Plex Home profile">
                <button class="btn" id="plex_user_pick_btn" title="Choose from server users">Pick</button>
                <div id="plex_user_pop" class="userpop hidden">
                  <div class="pophead">
                    <input id="plex_user_filter" placeholder="Filter users...">
                    <button type="button" class="btn" id="plex_user_close">Close</button>
                  </div>
                  <div id="plex_user_list" class="userlist"></div>
                  <div class="sub" style="margin-top:6px">Click a user to fill Username and Account_ID.</div>
                </div>
              </div>

              <label for="plex_account_id" style="margin-top:10px">Account_ID</label>
              <input id="plex_account_id" name="plex_account_id" type="number" min="1" placeholder="e.g. 1">

              <label for="plex_home_pin" style="margin-top:10px">Home PIN (optional)</label>
              <input id="plex_home_pin" name="plex_home_pin" type="password" inputmode="numeric" autocomplete="new-password" placeholder="4 digits">
              <div class="sub">Only needed if the selected Plex Home user is PIN-protected.</div>

              <div class="plex-btnrow">
                <button id="btn-plex-auto" class="btn" type="button" title="Fetch Server URL, Username, Account ID">Auto-Fetch</button>
                <span class="sub" style="align-self:center">Edit values before Save if needed.</span>
              </div>
            </div>
          </div>

          <div class="cw-subpanel" data-sub="whitelist">
            <div style="max-width:980px">
              <div class="plex-btnrow" style="margin-top:0;margin-bottom:12px">
                <button id="btn-plex-load-libraries" class="btn" type="button" title="Load Plex libraries">Load libraries</button>
                <span class="sub" style="align-self:center">Refresh after changing Server URL / token.</span>
              </div>

              <div class="lm-head">
                <div class="title">Whitelist Libraries</div>
                <input id="plex_lib_filter" class="lm-filter" placeholder="Filter...">
                <div class="lm-col"><button id="plex_hist_all" type="button" class="lm-dot hist" title="Toggle all History"></button><span class="sub">History</span></div>
                <div class="lm-col"><button id="plex_rate_all" type="button" class="lm-dot rate" title="Toggle all Ratings"></button><span class="sub">Ratings</span></div>
                <div class="lm-col"><button id="plex_scr_all" type="button" class="lm-dot scr" title="Toggle all Scrobble"></button><span class="sub">Scrobble</span></div>
              </div>
              <div id="plex_lib_matrix" class="lm-rows"></div>
              <div class="sub" style="margin-top:6px">Empty = all libraries.</div>
              <select id="plex_lib_history" class="lm-hidden" multiple></select>
              <select id="plex_lib_ratings" class="lm-hidden" multiple></select>
              <select id="plex_lib_scrobble" class="lm-hidden" multiple></select>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>

</div>'''
