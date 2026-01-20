# providers/auth/_auth_PLEX.py
# CrossWatch - PLEX Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import secrets
import time
from collections.abc import Mapping, MutableMapping
from typing import Any

import requests

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.config_base import save_config

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
__VERSION__ = "1.1.0"

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

    def get_status(self, cfg: Mapping[str, Any]) -> AuthStatus:
        token = (cfg.get("plex") or {}).get("account_token") or ""
        return AuthStatus(connected=bool(token), label="Plex")

    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str) -> dict[str, Any]:
        log("Plex: request PIN", level="INFO", module="AUTH")
        plex = cfg.setdefault("plex", {})

        cid = plex.get("client_id")
        if not cid:
            cid = secrets.token_hex(12)
            plex["client_id"] = cid
            save_config(dict(cfg))

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

        plex["_pending_pin"] = {
            "id": j["id"],
            "code": j["code"],
            "created": int(time.time()),
        }
        save_config(dict(cfg))

        log("Plex: PIN issued", level="SUCCESS", module="AUTH", extra={"pin_id": j["id"]})
        return {"pin": j["code"], "verify_url": "https://plex.tv/pin"}

    def finish(self, cfg: MutableMapping[str, Any], **payload: Any) -> AuthStatus:
        plex = cfg.setdefault("plex", {})
        pend = plex.get("_pending_pin") or {}
        if not pend:
            log("Plex: no pending PIN", level="WARNING", module="AUTH")
            return self.get_status(cfg)

        url = f"{PLEX_PIN_URL}/{pend['id']}"
        r = requests.get(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": UA,
                "X-Plex-Product": "CrossWatch",
                "X-Plex-Version": "1.0",
                "X-Plex-Client-Identifier": cfg.get("plex", {}).get("client_id", ""),
                "X-Plex-Platform": "Web",
            },
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        j = r.json()
        if j.get("authToken"):
            plex["account_token"] = j["authToken"]
            plex.pop("_pending_pin", None)
            save_config(dict(cfg))
            log("Plex: token stored", level="SUCCESS", module="AUTH")
        else:
            log("Plex: token not ready", level="INFO", module="AUTH")
        return self.get_status(cfg)

    def refresh(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        log("Plex: refresh noop", level="DEBUG", module="AUTH")
        return self.get_status(cfg)

    def disconnect(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        plex = cfg.setdefault("plex", {})
        plex.pop("account_token", None)
        plex.pop("_pending_pin", None)
        save_config(dict(cfg))
        log("Plex: disconnected", level="INFO", module="AUTH")
        return self.get_status(cfg)


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
      content:'▸';color:#b066ff;text-shadow:0 0 8px rgba(176,102,255,.65);
      position:absolute;right:10px;top:50%;transform:translateY(-50%);transition:transform .18s ease,color .18s ease,text-shadow .18s ease
    }
    #sec-plex details.settings[open] > summary::after{transform:translateY(-50%) rotate(90deg);color:#00d1ff;text-shadow:0 0 10px rgba(0,209,255,.85)}
    #sec-plex details.settings summary:hover,
    #sec-plex details.settings summary:focus-visible{
      box-shadow:inset 0 0 0 1px rgba(255,255,255,.05),0 0 18px rgba(176,102,255,.25),0 0 18px rgba(0,209,255,.25);
      transform:translateY(-1px)
    }

    /* matrix */
    #sec-plex .lm-head{display:grid;grid-template-columns:1fr auto auto auto auto auto;gap:10px;align-items:center;margin-bottom:8px}
    #sec-plex .lm-head .title{font-weight:700}
    #sec-plex .lm-rows{display:grid;gap:6px;max-height:260px;overflow:auto;border:1px solid var(--border);border-radius:10px;padding:6px;background:#090b10}
    #sec-plex .lm-row{display:grid;grid-template-columns:1fr 40px 40px 40px;gap:6px;align-items:center;background:#0b0d12;border-radius:8px;padding:6px 8px}
    #sec-plex .lm-row.hide{display:none}
    #sec-plex .lm-name{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    #sec-plex .lm-id{opacity:.5;margin-left:6px}
    #sec-plex .lm-dot{width:16px;height:16px;border-radius:50%;border:2px solid currentColor;background:transparent;cursor:pointer;display:inline-block;vertical-align:middle}
    #sec-plex .lm-dot.hist{color:#b066ff;box-shadow:0 0 6px rgba(176,102,255,.55)}
    #sec-plex .lm-dot.hist.on{background:#b066ff;box-shadow:0 0 10px rgba(176,102,255,.95)}
    #sec-plex .lm-dot.rate{color:#00d1ff;box-shadow:0 0 6px rgba(0,209,255,.55)}
    #sec-plex .lm-dot.rate.on{background:#00d1ff;box-shadow:0 0 10px rgba(0,209,255,.95)}
    #sec-plex .lm-dot.scr{color:#35ff8f;box-shadow:0 0 6px rgba(53,255,143,.55)}
    #sec-plex .lm-dot.scr.on{background:#35ff8f;box-shadow:0 0 10px rgba(53,255,143,.95)}
    #sec-plex .lm-col{display:flex;align-items:center;gap:6px}
    #sec-plex .lm-filter{min-width:160px}
    #sec-plex select.lm-hidden{display:none}

    /* neon scrollbar (matrix) */
    #sec-plex .lm-rows{scrollbar-width:thin;scrollbar-color:#b066ff #0b0d12}
    #sec-plex .lm-rows::-webkit-scrollbar{width:10px}
    #sec-plex .lm-rows::-webkit-scrollbar-track{background:#0b0d12;border-radius:10px}
    #sec-plex .lm-rows::-webkit-scrollbar-thumb{border-radius:10px;border:2px solid #0b0d12;background:linear-gradient(180deg,#b066ff 0%,#00d1ff 100%);box-shadow:0 0 8px rgba(176,102,255,.55),0 0 8px rgba(0,209,255,.55)}
    #sec-plex .lm-rows::-webkit-scrollbar-thumb:hover{filter:brightness(1.08);box-shadow:0 0 10px rgba(176,102,255,.85),0 0 10px rgba(0,209,255,.85)}
    #sec-plex .lm-rows::-webkit-scrollbar-thumb:active{filter:brightness(1.15)}

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

  <div class="head" onclick="toggleSection('sec-plex')">
    <span class="chev">▶</span><strong>Plex</strong>
  </div>

  <div class="body">
    <div class="grid2">
      <div>
        <label>Current token</label>
        <div class="inline">
          <input id="plex_token" placeholder="empty = not set">
          <button class="btn copy" onclick="copyInputValue('plex_token', this)">Copy</button>
        </div>
      </div>
      <div>
        <label>Link code (PIN)</label>
        <div class="inline">
          <input id="plex_pin" placeholder="" readonly>
          <button class="btn copy" onclick="copyInputValue('plex_pin', this)">Copy</button>
        </div>
      </div>
    </div>

    <div class="inline pinrow">
      <button class="btn" onclick="requestPlexPin()">Connect PLEX</button>
      <button class="btn danger" onclick="try{ plexDeleteToken && plexDeleteToken(); }catch(_){;}">Delete</button>
      <div class="hint">Opens plex.tv/link to complete sign-in.</div>
      <div class="plexmsg">
        <div id="plex_msg" class="msg ok hidden">PIN</div>
        <div id="plex_msg_detail" class="hidden"></div>
      </div>
    </div>

    <details class="settings">
      <summary><span class="plex-ico">⚙︎</span><span class="title">Settings</span><span class="hint">Server · User · Whitelist</span></summary>
      <div class="wrap">
        <div>
        <label>Server URL</label>
        <div class="fieldline">
          <input id="plex_server_url" placeholder="http://host:32400" list="plex_server_suggestions">
          <datalist id="plex_server_suggestions"></datalist>
          <label class="sslopt" title="Verify server TLS certificate">
            <input type="checkbox" id="plex_verify_ssl">
            <span class="lbl">Verify SSL</span>
          </label>
        </div>
          <div class="sub">Leave blank to discover.</div>

          <label style="margin-top:10px">Username</label>
          <div class="fieldline userpick">
            <input id="plex_username" placeholder="Plex Home profile">
            <button class="btn" id="plex_user_pick_btn" title="Choose from server users">Pick</button>
            <div id="plex_user_pop" class="userpop hidden">
              <div class="pophead">
                <input id="plex_user_filter" placeholder="Filter users…">
                <button type="button" class="btn" id="plex_user_close">Close</button>
              </div>
              <div id="plex_user_list" class="userlist"></div>
              <div class="sub" style="margin-top:6px">Click a user to fill Username and Account_ID.</div>
            </div>
          </div>

          <label style="margin-top:10px">Account_ID</label>
          <input id="plex_account_id" type="number" min="1" placeholder="e.g. 1">

          <label style="margin-top:10px">Home PIN (optional)</label>
          <input id="plex_home_pin" type="password" inputmode="numeric" autocomplete="new-password" placeholder="4 digits">
          <div class="sub">Only needed if the selected Plex Home user is PIN-protected.</div>

          <div class="btnrow">
            <button class="btn" title="Fetch Server URL, Username, Account ID" onclick="plexAuto()">Auto-Fetch</button>
            <button class="btn" title="Load Plex libraries" onclick="refreshPlexLibraries()">Load libraries</button>
            <span class="sub" style="align-self:center">Edit values before Save if needed.</span>
          </div>
        </div>

        <div>
          <div class="lm-head">
            <div class="title">Whitelist Libraries</div>
            <input id="plex_lib_filter" class="lm-filter" placeholder="Filter…">
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

        <div class="footspace" aria-hidden="true"></div>
      </div>
    </details>
  </div>

  <script>
  (function(){
    const $ = (id)=>document.getElementById(id);
    const esc = (s)=>String(s||"").replace(/[&<>"']/g,c=>({ "&":"&amp;","<":"&lt;","&gt;":"&gt;","\"":"&quot;","'":"&#39;" }[c]));
    let __users = null;

    (async ()=>{
      try{
        const r = await fetch("/api/config",{cache:"no-store"});
        const cfg = await r.json();
        const on = !!(cfg?.plex?.verify_ssl);
        const cb = $("plex_verify_ssl"); if (cb) cb.checked = on;
      }catch{}
    })();

    document.addEventListener("settings-collect",(ev)=>{
      try{
        const cfg = ev?.detail?.cfg || (window.__cfg ||= {});
        const plex = (cfg.plex ||= {});
        plex.verify_ssl = !!$("plex_verify_ssl")?.checked;
      }catch{}
    }, true);

    async function fetchUsers(){
      if (Array.isArray(__users) && __users.length) return __users;
      try{
        const r = await fetch("/api/plex/users?ts="+Date.now(), { cache:"no-store" });
        const j = await r.json();
        __users = Array.isArray(j?.users) ? j.users : [];
      }catch{ __users = []; }
      return __users;
    }

    function renderUsers(){
      const box = $("plex_user_list"); if (!box) return;
      const q = ($("plex_user_filter")?.value || "").trim().toLowerCase();
      const list = (__users||[]).filter(u=>{
        const hay = [u.title,u.username,u.email,u.type].filter(Boolean).join(" ").toLowerCase();
        return !q || hay.includes(q);
      });
      box.innerHTML = list.length ? list.map(u => `
        <button type="button" class="userrow" data-uid="${esc(u.id)}" data-username="${esc(u.username||"")}">
          <div class="row1">
            <strong>${esc(u.title||u.username||("User #"+u.id))}</strong>
            <span class="sub">@${esc(u.username||"")}</span>
            <span class="tag ${u.type==='owner'?'owner':'friend'}">${esc(u.type||"")}</span>
          </div>
          ${u.email ? `<div class="sub">${esc(u.email)}</div>` : ""}
        </button>
      `).join("") : '<div class="sub">No users found.</div>';
    }

    function openPicker(){
      const pop = $("plex_user_pop"); if (!pop) return;
      pop.classList.remove("hidden");
      fetchUsers().then(renderUsers);
      $("plex_user_filter")?.focus();
    }
    function closePicker(){ $("plex_user_pop")?.classList.add("hidden"); }

    function attachOnce(){
      const pick = $("plex_user_pick_btn");
      if (pick && !pick.__wired){ pick.__wired = true; pick.addEventListener("click", openPicker); }
      const close = $("plex_user_close");
      if (close && !close.__wired){ close.__wired = true; close.addEventListener("click", closePicker); }
      const filter = $("plex_user_filter");
      if (filter && !filter.__wired){ filter.__wired = true; filter.addEventListener("input", renderUsers); }
      const list = $("plex_user_list");
      if (list && !list.__wired){
        list.__wired = true;
        list.addEventListener("click", (e)=>{
          const row = e.target.closest(".userrow"); if (!row) return;
          const uname = row.dataset.username || "";
          const uid   = row.dataset.uid || "";
          const uEl = $("plex_username"); if (uEl) uEl.value = uname;
          const aEl = $("plex_account_id"); if (aEl) aEl.value = uid;
          closePicker();
          try{ document.dispatchEvent(new CustomEvent("settings-collect",{detail:{section:"plex-users"}})); }catch{}
        });
      }
      if (!document.__plexUserAway){
        document.__plexUserAway = true;
        document.addEventListener("click",(e)=>{
          const pop = $("plex_user_pop");
          if (!pop || pop.classList.contains("hidden")) return;
          if (pop.contains(e.target) || e.target.id==="plex_user_pick_btn") return;
          pop.classList.add("hidden");
        });
      }
    }

    attachOnce();
    document.addEventListener("tab-changed", ()=>attachOnce());

    async function refreshPlexLibraries(){
      try{
        const host = document.getElementById("plex_lib_matrix");
        if (host) host.innerHTML = '<div class="sub">Loading libraries…</div>';
      }catch{}

      let usedHelpers = false;
      try{
        if (typeof window.hydratePlexFromConfigRaw === "function") { await window.hydratePlexFromConfigRaw(); usedHelpers = true; }
      }catch{}
      try{
        if (typeof window.plexLoadLibraries === "function") { await window.plexLoadLibraries(); usedHelpers = true; }
      }catch{}
      try{
        if (typeof window.mountPlexLibraryMatrix === "function") { window.mountPlexLibraryMatrix(); usedHelpers = true; }
      }catch{}

      if (usedHelpers) return;

      try{
        const r = await fetch("/api/plex/libraries?ts="+Date.now(), { cache:"no-store" });
        const j = await r.json();
        const libs = Array.isArray(j?.libraries) ? j.libraries : [];
        const fill = (id)=>{
          const el = document.getElementById(id); if (!el) return;
          const keep = new Set(Array.from(el.selectedOptions||[]).map(o=>o.value));
          el.innerHTML = "";
          libs.forEach(it=>{
            const o = document.createElement("option");
            o.value = String(it.key);
            o.textContent = `${it.title} (${it.type||"lib"}) — #${it.key}`;
            if (keep.has(o.value)) o.selected = true;
            el.appendChild(o);
          });
        };
        fill("plex_lib_history"); fill("plex_lib_ratings");
        const host = document.getElementById("plex_lib_matrix");
        if (host) host.innerHTML = '<div class="sub">Libraries loaded.</div>';
      }catch{}
    }

    window.refreshPlexLibraries = refreshPlexLibraries;

  })();
  </script>
</div>'''