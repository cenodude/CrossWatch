// Scrobbler UI
(function (w, d) {
  const $ = (s, r) => (r || d).querySelector(s);
  const $all = (s, r) => [...(r || d).querySelectorAll(s)];
  const el = (t, a) => Object.assign(d.createElement(t), a || {});
  const on = (n, e, f) => n && n.addEventListener(e, f);

  const j = async (u, o) => {
    const r = await fetch(u, { cache: "no-store", ...(o || {}) });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  };
  const STICKY_NOTES = {};


  function setNote(id, msg, kind) {
    const n = d.getElementById(id);
    if (!n) return;
    const sid = String(id || "");
    if (!msg && STICKY_NOTES[sid]) {
      const s = STICKY_NOTES[sid];
      n.textContent = s.msg || "";
      n.style.display = s.msg ? "" : "none";
      n.classList.remove("err", "warn", "ok");
      if (s.kind) n.classList.add(s.kind);
      return;
    }
    n.textContent = msg || "";
    const color = kind === "err" ? "#ff6b6b" : kind === "warn" ? "#f59e0b" : "var(--muted,#a7a7a7)";
    n.style.cssText =
      "margin:6px 0 2px;font-size:12px;opacity:.92;color:" +
      color +
      (kind === "warn" ? ";font-weight:700" : "");
  }


function setStickyNote(id, msg, kind) {
  const sid = String(id || "");
  STICKY_NOTES[sid] = { msg: String(msg || ""), kind: kind || "" };
  setNote(sid, msg, kind);
}

function clearStickyNote(id) {
  const sid = String(id || "");
  delete STICKY_NOTES[sid];
  setNote(sid, "");
}

  const HELP_TEXT = {
    "sc-help-auto-remove":
      "When you finish a movie, CW will automatically remove that title from your configured Watchlists. It’s currently movies-only. It honors your filters (username/server). If the movie isn’t on your Watchlist, nothing happens, your libraries and other services remain untouched.",
    "sc-help-webhook-plex-ratings":
      "When enabled, we’ll send ratings to Trakt. Movies, shows, seasons, and episodes are supported.",
    "sc-help-watch-plex-ratings":
      "When enabled, we’ll send ratings to Trakt and/or SIMKL.\nTrakt: Movies, shows, seasons, and episodes are supported.\nMDBList: Movies, shows, seasons, and episodes are supported.\nSIMKL: Movies and shows are supported.\nAdd the below webhook to your Plex instance to enable ratings.",

    "sc-help-adv-pause":
      "Pause debounce (sec) (default 5) - Ignore rapid, duplicate pause events.",
    "sc-help-adv-suppress":
      "Suppress start @ (%) (default 99) - If play/resume is at or above this %, don’t send /scrobble/start.",
    "sc-help-adv-regress":
      "Regress tol % (default 5) - Block progress rollbacks bigger than this %.",
    "sc-help-adv-stop-pause":
      "Stop pause ≥ (%) (default 80) - If STOP arrives below this %, treat it as PAUSE.",
    "sc-help-adv-force-stop":
      "Force stop @ (%) (default 80) - If STOP is at or above this %, send /scrobble/stop.",
    "sc-help-adv-progress-step":
      "Progress updates in percentages, which can significantly reduce or increase the number of API calls required. When in doubt, default to 25% increments.",
    "sc-help-watch-filters":
      "Don't skip the filtering step! While optional for solo media server users, it becomes essential the moment you share your server with other users. Without filters, the system will scrobble everything",
    "sc-help-watch-advanced":
      "Do not alter the Advanced settings unless you fully understand their impact. When in doubt, leave them untouched.",
  };

  const helpBtn = (tipId) =>
    `<button type="button" class="cx-help material-symbols-rounded" data-tip-id="${tipId}" aria-label="Help">help</button>`;

  function bindHelpTips(root) {
    const scope = root || d;
    $all(".cx-help[data-tip-id]", scope).forEach((btn) => {
      const id = btn.getAttribute("data-tip-id") || "";
      const text = HELP_TEXT[id];
      if (text && !btn.title) btn.title = text;

      if (btn.dataset.cxBound === "1") return;
      btn.dataset.cxBound = "1";
      btn.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
      });
    });
  }

  function injectStyles() {
    if (d.getElementById("sc-styles")) return;
    const s = d.createElement("style");
    s.id = "sc-styles";
    s.textContent = `
    .row{display:flex;gap:14px;align-items:center;flex-wrap:wrap}
    .codepair{display:flex;gap:8px;align-items:center}
    .codepair code{padding:6px 8px;border-radius:8px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08)}
    .badge{padding:4px 10px;border-radius:999px;font-weight:600;opacity:.9}.badge.is-on{background:#0a3;color:#fff}.badge.is-off{background:#333;color:#bbb;border:1px solid #444}
    .status-dot{width:10px;height:10px;border-radius:50%}.status-dot.on{background:#22c55e}.status-dot.off{background:#ef4444}
    .watcher-row{display:grid;grid-template-columns:1fr 1fr;gap:16px}
    @media (max-width:960px){.watcher-row{grid-template-columns:1fr}}

    .chips{display:flex;flex-wrap:wrap;gap:6px}
    .chip{display:inline-flex;align-items:center;gap:6px;padding:4px 8px;border-radius:10px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.08)}
    .chip .rm{cursor:pointer;opacity:.7}

    .sc-filter-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}

    .sc-adv-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:10px}
    .sc-adv-grid .field{display:flex;align-items:center;gap:8px}
    .sc-adv-grid .field label{flex:0 0 auto;white-space:nowrap;font-size:12px;opacity:.8}
    .sc-adv-grid .field .cx-help{flex:0 0 auto}
    .sc-adv-grid .field input{flex:0 0 auto;width:64px;max-width:100%;margin-left:0}
    .sc-adv-grid .field input{width:64px;max-width:100%;justify-self:end}
    @media (max-width:1100px){.sc-adv-grid{grid-template-columns:repeat(2,minmax(0,1fr));}}
    @media (max-width:640px){.sc-adv-grid{grid-template-columns:1fr;}}

    details.sc-filters,details.sc-advanced,details.sc-box{display:block;margin-top:12px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset}
    details.sc-filters>summary,details.sc-advanced>summary,details.sc-box>summary{cursor:pointer;list-style:none;padding:14px;border-radius:12px;font-weight:600}
    details.sc-filters[open]>summary,details.sc-advanced[open]>summary,details.sc-box[open]>summary{border-bottom:1px solid rgba(255,255,255,.06)}
    details.sc-filters .body,details.sc-advanced .body,details.sc-box .body{padding:12px 14px}

    .sc-subbox{margin-top:12px;border-radius:12px;background:rgba(255,255,255,.04);box-shadow:0 0 0 1px rgba(255,255,255,.06) inset}
    .sc-subbox .head{padding:12px 14px;font-weight:700;opacity:.92}
    .sc-subbox .body{padding:12px 14px;border-top:1px solid rgba(255,255,255,.06)}

    .sc-toggle{display:inline-flex;align-items:center;gap:8px;font-size:12px;opacity:.9;white-space:nowrap}
	    .cx-toggle{display:inline-flex;align-items:center;gap:10px;cursor:pointer;user-select:none}
	    .cx-toggle input{position:absolute;opacity:0;width:1px;height:1px;pointer-events:none}
	    .cx-toggle-ui{width:46px;height:26px;border-radius:999px;background:rgba(255,255,255,.10);border:1px solid rgba(255,255,255,.14);position:relative;box-shadow:inset 0 0 0 1px rgba(0,0,0,.18);transition:background .15s ease,border-color .15s ease,box-shadow .15s ease}
	    .cx-toggle-ui:after{content:"";position:absolute;top:3px;left:3px;width:20px;height:20px;border-radius:999px;background:rgba(255,255,255,.92);box-shadow:0 8px 18px rgba(0,0,0,.35);transition:transform .15s ease,background .15s ease}
	    .cx-toggle-text{font-size:12px;opacity:.9;white-space:nowrap}
	    .cx-toggle-state{font-size:11px;padding:2px 8px;border-radius:999px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.08);opacity:.85}
	    .cx-toggle-state:before{content:"Off"}
	    .cx-toggle:hover .cx-toggle-ui{border-color:rgba(255,255,255,.22)}
	    .cx-toggle input:checked + .cx-toggle-ui{background:rgba(34,197,94,.28);border-color:rgba(34,197,94,.45)}
	    .cx-toggle input:checked + .cx-toggle-ui:after{transform:translateX(20px)}
	    .cx-toggle input:checked ~ .cx-toggle-state:before{content:"On"}
	    .cx-toggle input:focus-visible + .cx-toggle-ui{box-shadow:0 0 0 2px rgba(255,255,255,.14),0 0 0 6px rgba(34,197,94,.15),inset 0 0 0 1px rgba(0,0,0,.18)}

    .wh-top{display:grid;grid-template-columns:auto 1fr;align-items:start;gap:12px;margin-bottom:8px;position:relative}
    .wh-toggle{display:inline-flex;gap:8px;align-items:center}
    .wh-endpoints{display:flex;flex-direction:column;gap:8px;align-items:flex-end}
    .codepair.right{justify-content:flex-end}
    @media(max-width:960px){.wh-top{grid-template-columns:1fr}.wh-endpoints{align-items:flex-start}}
    .wh-logo{width:var(--wh-logo,24px);height:var(--wh-logo,24px);aspect-ratio:1/1;object-fit:contain;display:block;transform-origin:center}
    .wh-logo[alt="Plex"]{transform:scale(1.15)}
    .wh-logo[alt="Jellyfin"]{transform:scale(1.0)}
    .wh-logo[alt="Emby"]{transform:scale(1.15)}

    .sc-opt-col{display:flex;flex-direction:column;gap:10px}
    .sc-opt-row{display:flex;align-items:center;gap:10px;flex-wrap:wrap}

    .sc-pillbar{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
    .sc-pill{display:inline-flex;align-items:center;justify-content:center;padding:7px 10px;border-radius:999px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.05);color:rgba(255,255,255,.92);font-size:12px;line-height:1;cursor:pointer;user-select:none;transition:background .15s ease,border-color .15s ease,opacity .15s ease}
    .sc-pill.off{opacity:.78}
    .sc-pill.on{background:rgba(34,197,94,.18);border-color:rgba(34,197,94,.45);opacity:1}
    .sc-pill:hover{border-color:rgba(255,255,255,.22)}
    .sc-pill:focus-visible{outline:0;box-shadow:0 0 0 2px rgba(255,255,255,.14),0 0 0 6px rgba(34,197,94,.15)}
    .sc-pill:disabled{cursor:default;opacity:.45}


    .sc-user-pop{position:fixed;z-index:9999;width:min(360px,calc(100vw - 24px));max-height:min(420px,calc(100vh - 24px));border-radius:14px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.08) inset,0 18px 50px rgba(0,0,0,.55);border:1px solid rgba(255,255,255,.10);overflow:hidden}
    .sc-user-pop.hidden{display:none}
    .sc-user-pop .head{display:flex;justify-content:space-between;align-items:center;gap:10px;padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.06)}
    .sc-user-pop .title{font-weight:800}
    .sc-user-pop .body{padding:10px 12px;display:grid;gap:10px}
    .sc-user-pop .list{overflow:auto;border:1px solid rgba(255,255,255,.08);border-radius:12px;max-height:280px}
    .sc-user-pop .userrow{width:100%;text-align:left;background:transparent;border:0;color:inherit;padding:10px 10px;cursor:pointer}
    .sc-user-pop .userrow:hover{background:rgba(255,255,255,.05)}
    .sc-user-pop .row1{display:flex;justify-content:space-between;align-items:center;gap:8px}
    .sc-user-pop .sub{font-size:12px;opacity:.7;padding:10px}
    .sc-user-pop .tag{font-size:11px;padding:2px 8px;border-radius:999px;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.10);opacity:.85}
    .sc-prov-wrap{position:relative;display:inline-block}
    .sc-prov-btn{width:140px;display:flex;align-items:center;justify-content:space-between;gap:10px;padding:8px 10px;cursor:pointer}
    .sc-prov-left{display:inline-flex;align-items:center;gap:8px;min-width:0}
    .sc-prov-ico{width:18px;height:18px;object-fit:contain}
    .sc-prov-caret{opacity:.7}
    .sc-prov-menu{position:absolute;right:0;top:calc(100% + 6px);min-width:140px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.08) inset,0 18px 50px rgba(0,0,0,.55);border:1px solid rgba(255,255,255,.10);overflow:hidden;z-index:1000}
    .sc-prov-menu.hidden{display:none}
    .sc-prov-item{width:100%;display:flex;align-items:center;gap:8px;padding:10px 10px;background:transparent;border:0;color:inherit;cursor:pointer;text-align:left}
    .sc-prov-item:hover{background:rgba(255,255,255,.05)}
    .sc-prov-item[aria-selected="true"]{background:rgba(34,197,94,.18)}
    .sc-prov-btn,.sc-prov-btn *{color:rgba(255,255,255,.92)!important;-webkit-text-fill-color:rgba(255,255,255,.92)!important}
    .sc-prov-btn:disabled,.sc-prov-btn:disabled *{color:rgba(255,255,255,.55)!important;-webkit-text-fill-color:rgba(255,255,255,.55)!important}

    #sc-provider,#sc-sink{color:rgba(255,255,255,.92)!important;-webkit-text-fill-color:rgba(255,255,255,.92)!important}
    #sc-provider:disabled,#sc-sink:disabled{color:rgba(255,255,255,.55)!important;-webkit-text-fill-color:rgba(255,255,255,.55)!important}
    #sc-provider option,#sc-sink option{color:#fff;background:#111}

    #sc-filters>summary,#sc-advanced>summary{display:flex;align-items:center;gap:8px;}
    
    /* Routes UI */
    .sc-route-table table{width:100%;border-collapse:separate;border-spacing:0 8px}
    .sc-route-table th{font-size:12px;opacity:.8;text-align:left;padding:0 6px}
    .sc-route-table td{padding:0 6px;vertical-align:middle}
    .sc-route-row{background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.06);border-radius:12px}
    .sc-route-row td{padding:8px 6px}
    .sc-route-actions{display:flex;gap:8px;justify-content:flex-end;flex-wrap:wrap}
    .sc-route-table select.input{height:34px}
    .sc-route-table .sc-prov-wrap{display:block;width:100%}
    .sc-route-table .sc-prov-btn{width:100%;height:34px;padding:6px 10px}
    .sc-route-table .sc-prov-menu{left:0;right:0;min-width:0}

`;
    d.head.appendChild(s);
  }

  const DEFAULTS = {
    watch: { pause_debounce_seconds: 5, suppress_start_at: 99 },
    trakt: { stop_pause_threshold: 80, force_stop_at: 80, regress_tolerance_percent: 5, progress_step: 25 },
  };

  const STATE = { mount: null, webhookHost: null, watcherHost: null, cfg: {}, users: [], pms: [], ui: { watchProvider: null, watchSink: null, scrobbleEnabled: null, scrobbleMode: null, watchAutostart: null }, pf: { key: "cx_sc_watch_filters_by_provider_v1", store: {}, loaded: false }, _pfMute: false, _noSinkAutostartFixApplied: false };

  const deepSet = (o, p, v) =>
    p.split(".").reduce(
      (a, k, i, arr) =>
        i === arr.length - 1
          ? (a[k] = v)
          : (((a[k] && typeof a[k] === "object") || (a[k] = {})), a[k]),
      o
    );

  const read = (p, dflt) => p.split(".").reduce((v, k) => (v && typeof v === "object" ? v[k] : undefined), STATE.cfg) ?? dflt;

  function write(p, v) {
    deepSet(STATE.cfg, p, v);
    try {
      w._cfgCache ||= {};
      deepSet(w._cfgCache, p, v);
    } catch {}
    try {
      syncHiddenServerInputs();
    } catch {}
    try {
      if (!STATE._pfMute && !isRoutesMode() && String(p || "").startsWith("scrobble.watch.filters.")) {
        saveCurrentProviderFilters();
      }
    } catch {}
  }

  const asArray = (v) => (Array.isArray(v) ? v.slice() : v == null || v === "" ? [] : [String(v)]);


  function pfLoadStore() {
    if (STATE.pf?.loaded) return;
    STATE.pf.loaded = true;
    try {
      const raw = localStorage.getItem(STATE.pf.key);
      const obj = raw ? JSON.parse(raw) : {};
      if (obj && typeof obj === "object") STATE.pf.store = obj;
    } catch {
      STATE.pf.store = {};
    }
  }

  function pfSaveStore() {
    try {
      localStorage.setItem(STATE.pf.key, JSON.stringify(STATE.pf.store || {}));
    } catch {}
  }

  function snapshotWatchFilters() {
    return {
      username_whitelist: asArray(read("scrobble.watch.filters.username_whitelist", [])),
      server_uuid: String(read("scrobble.watch.filters.server_uuid", "") || "").trim(),
      user_id: String(read("scrobble.watch.filters.user_id", "") || "").trim(),
    };
  }

  function saveCurrentProviderFilters(provOverride) {
    pfLoadStore();
    const prov = String(provOverride || provider() || "plex").toLowerCase().trim();
    if (!["plex", "emby", "jellyfin"].includes(prov)) return;
    STATE.pf.store ||= {};
    STATE.pf.store[prov] = snapshotWatchFilters();
    pfSaveStore();
  }

  function applyProviderFilters(provOverride) {
    pfLoadStore();
    const prov = String(provOverride || provider() || "plex").toLowerCase().trim();
    if (!["plex", "emby", "jellyfin"].includes(prov)) return;

    const snap = STATE.pf.store?.[prov] || null;
    STATE._pfMute = true;
    try {
      if (snap) {
        write("scrobble.watch.filters.username_whitelist", asArray(snap.username_whitelist));
        if (prov === "plex") {
          write("scrobble.watch.filters.server_uuid", String(snap.server_uuid || "").trim());
          write("scrobble.watch.filters.user_id", "");
        } else {
          const uid = String(snap.user_id || snap.server_uuid || "").trim();
          write("scrobble.watch.filters.user_id", uid);
          write("scrobble.watch.filters.server_uuid", uid);
        }
      } else {
        write("scrobble.watch.filters.username_whitelist", []);
        write("scrobble.watch.filters.server_uuid", "");
        write("scrobble.watch.filters.user_id", "");
      }
    } finally {
      STATE._pfMute = false;
    }
    saveCurrentProviderFilters(prov);
  }
  const clamp100 = (n) => Math.min(100, Math.max(1, Math.round(Number(n))));
  const norm100 = (n, dflt) => clamp100(Number.isFinite(+n) ? +n : dflt);
  const clampRange = (n, min, max) => Math.min(max, Math.max(min, Math.round(Number(n))));
  const normRange = (n, dflt, min, max) => clampRange(Number.isFinite(+n) ? +n : dflt, min, max);
  const provider = () => String(read("scrobble.watch.provider", "plex") || "plex").toLowerCase();

  const PROVIDER_META = {
    plex: { label: "Plex", icon: "/assets/img/PLEX-log.svg", alt: "Plex" },
    emby: { label: "Emby", icon: "/assets/img/EMBY-log.svg", alt: "Emby" },
    jellyfin: { label: "Jellyfin", icon: "/assets/img/JELLYFIN-log.svg", alt: "Jellyfin" },
  };

  const SINK_META = {
    trakt: { label: "Trakt", icon: "/assets/img/TRAKT-log.svg", alt: "Trakt" },
    simkl: { label: "SIMKL", icon: "/assets/img/SIMKL-log.svg", alt: "SIMKL" },
    mdblist: { label: "MDBList", icon: "/assets/img/MDBLIST-log.svg", alt: "MDBList" },
  };

  let ROUTE_DD_OPEN = null;

  function closeRouteDd() {
    const cur = ROUTE_DD_OPEN;
    if (!cur) return;
    cur.menu.classList.add("hidden");
    cur.btn.setAttribute("aria-expanded", "false");
    ROUTE_DD_OPEN = null;
  }

  function bindRouteDdAway() {
    if (STATE.__scRouteDdAwayBound) return;
    STATE.__scRouteDdAwayBound = true;
    d.addEventListener("click", (e) => {
      const cur = ROUTE_DD_OPEN;
      if (!cur) return;
      if (cur.menu.contains(e.target)) return;
      if (cur.btn === e.target || cur.btn.contains(e.target)) return;
      closeRouteDd();
    });
    d.addEventListener("keydown", (e) => {
      if (e.key === "Escape") closeRouteDd();
    });
  }

  function makeRouteIconDropdown(sel, metaMap, labelFallback) {
    const wrap = el("div", { className: "sc-prov-wrap sc-route-dd" });
    const btn = el("button", { type: "button", className: "input sc-prov-btn" });
    btn.style.width = "100%";
    btn.setAttribute("aria-haspopup", "listbox");
    btn.setAttribute("aria-expanded", "false");

    const left = el("span", { className: "sc-prov-left" });
    const ico = el("img", { className: "wh-logo sc-prov-ico", alt: "" });
    const label = el("span", { className: "truncate" });
    left.append(ico, label);

    const caret = el("span", { className: "sc-prov-caret", textContent: "▾" });
    caret.setAttribute("aria-hidden", "true");
    btn.append(left, caret);

    const menu = el("div", { className: "sc-prov-menu hidden", role: "listbox" });

    const items = [];
    [...sel.options].forEach((opt) => {
      const v = String(opt.value || "").toLowerCase().trim();
      if (!v) return;
      const key = v === "embv" ? "emby" : v;
      const meta = metaMap[key] || { label: opt.textContent || labelFallback || v, icon: "", alt: opt.textContent || v };
      const it = el("button", { type: "button", className: "sc-prov-item", role: "option" });
      it.dataset.value = v;
      if (meta.icon) it.appendChild(el("img", { className: "wh-logo sc-prov-ico", src: meta.icon, alt: meta.alt || meta.label || v }));
      it.appendChild(el("span", { textContent: meta.label || opt.textContent || v }));
      menu.appendChild(it);
      items.push(it);
    });

    sel.style.display = "none";

    const sync = () => {
      const v0 = String(sel.value || "").toLowerCase().trim();
      const v = v0 === "embv" ? "emby" : v0;
      const meta = metaMap[v] || { label: labelFallback || v0 || "", icon: "", alt: labelFallback || v0 || "" };
      if (meta.icon) {
        ico.src = meta.icon;
        ico.alt = meta.alt || meta.label || v0;
        ico.style.display = "";
      } else {
        ico.removeAttribute("src");
        ico.alt = "";
        ico.style.display = "none";
      }
      label.textContent = meta.label || v0 || "";
      items.forEach((it) => it.setAttribute("aria-selected", String(it.dataset.value || "") === v0 ? "true" : "false"));
    };

    const open = () => {
      bindRouteDdAway();
      if (ROUTE_DD_OPEN && ROUTE_DD_OPEN.menu !== menu) closeRouteDd();
      menu.classList.remove("hidden");
      btn.setAttribute("aria-expanded", "true");
      ROUTE_DD_OPEN = { menu, btn };
      try {
        const sc = d.getElementById("sc-routes");
        if (sc) {
          menu.style.top = "calc(100% + 6px)";
          menu.style.bottom = "";
          menu.style.visibility = "hidden";
          const h = menu.offsetHeight || 0;
          const rBtn = btn.getBoundingClientRect();
          const rSc = sc.getBoundingClientRect();
          const spaceBelow = rSc.bottom - rBtn.bottom;
          const spaceAbove = rBtn.top - rSc.top;
          if (h && spaceBelow < h && spaceAbove > h) {
            menu.style.top = "auto";
            menu.style.bottom = "calc(100% + 6px)";
          }
          menu.style.visibility = "";
        }
      } catch {}
      sync();
    };

    const toggle = () => (menu.classList.contains("hidden") ? open() : closeRouteDd());

    on(btn, "click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      toggle();
    });

    on(menu, "click", (e) => {
      const it = e.target && e.target.closest ? e.target.closest("button[data-value]") : null;
      if (!it) return;
      e.preventDefault();
      const v = String(it.dataset.value || "").toLowerCase().trim();
      if (v && sel.value !== v) {
        sel.value = v;
        sel.dispatchEvent(new Event("change", { bubbles: true }));
      } else {
        sync();
      }
      closeRouteDd();
    });

    on(sel, "change", sync);

    wrap.append(btn, menu, sel);
    sync();
    return wrap;
  }


  function syncProviderPickerUi() {
    const sel = $("#sc-provider", STATE.mount);
    const btn = $("#sc-provider-btn", STATE.mount);
    const icon = $("#sc-provider-icon", STATE.mount);
    const label = $("#sc-provider-label", STATE.mount);
    const menu = $("#sc-provider-menu", STATE.mount);
    const v = String(sel?.value || provider() || "plex").toLowerCase().trim();
    const meta = PROVIDER_META[v] || PROVIDER_META.plex;

    if (icon) {
      icon.src = meta.icon;
      icon.alt = meta.alt;
    }
    if (label) label.textContent = meta.label;
    if (btn) btn.title = `Pick ${meta.label} provider`;

    if (menu) {
      $all(".sc-prov-item[data-value]", menu).forEach((it) => {
        const iv = String(it.getAttribute("data-value") || "").toLowerCase().trim();
        it.setAttribute("aria-selected", iv === v ? "true" : "false");
      });
    }
  }

  function closeProviderMenu() {
    const menu = $("#sc-provider-menu", STATE.mount);
    const btn = $("#sc-provider-btn", STATE.mount);
    if (menu) menu.classList.add("hidden");
    if (btn) btn.setAttribute("aria-expanded", "false");
  }

  function toggleProviderMenu() {
    const menu = $("#sc-provider-menu", STATE.mount);
    const btn = $("#sc-provider-btn", STATE.mount);
    if (!menu || !btn) return;
    const open = menu.classList.contains("hidden");
    if (open) {
      menu.classList.remove("hidden");
      btn.setAttribute("aria-expanded", "true");
      syncProviderPickerUi();
    } else {
      closeProviderMenu();
    }
  }
  const SINK_ORDER = ["trakt", "simkl", "mdblist"];
  function normSinkCsv(raw) {
    const parts = String(raw || "")
      .split(",")
      .map((s) => s.trim().toLowerCase())
      .filter(Boolean);
    const uniq = [...new Set(parts)];
    uniq.sort((a, b) => {
      const ia = SINK_ORDER.indexOf(a);
      const ib = SINK_ORDER.indexOf(b);
      if (ia === -1 && ib === -1) return a.localeCompare(b);
      if (ia === -1) return 1;
      if (ib === -1) return -1;
      return ia - ib;
    });
    return uniq.join(",");
  }
  function normSinkCsvOrDefault(raw, dflt = "trakt") {
    const v = normSinkCsv(raw);
    return v || String(dflt || "trakt").toLowerCase();
  }

  const SINK_LABELS = { trakt: "Trakt", simkl: "SIMKL", mdblist: "MDBList" };

  function ensureSinkPillBar(bar) {
    if (!bar || bar.dataset.scBuilt === "1") return;
    bar.dataset.scBuilt = "1";
    bar.innerHTML = SINK_ORDER.map((k) => `<button type="button" class="sc-pill off" data-sink="${k}" aria-pressed="false">${SINK_LABELS[k] || k}</button>`).join("");
  }

  function csvFromSelect(sel, allowNone = false) {
    const raw = String(sel?.value || "").toLowerCase().trim();
    if (allowNone && raw === "none") return "";
    return normSinkCsv(raw);
  }

  function syncPillBar(bar, csv) {
    if (!bar) return;
    ensureSinkPillBar(bar);
    const on = new Set(String(csv || "").split(",").filter(Boolean));
    $all("button[data-sink]", bar).forEach((btn) => {
      const k = String(btn.getAttribute("data-sink") || "");
      const active = on.has(k);
      btn.classList.toggle("on", active);
      btn.classList.toggle("off", !active);
      btn.setAttribute("aria-pressed", active ? "true" : "false");
    });
  }

  function syncSinkPillsFromSelect() {
    const sel = $("#sc-sink", STATE.mount);
    const bar = $("#sc-sink-pills", STATE.mount);
    if (!sel || !bar) return;
    syncPillBar(bar, normSinkCsv(sel.value));
  }

  function syncPlexRatingsPillsFromSelect() {
    const sel = $("#sc-plex-ratings", STATE.mount);
    const bar = $("#sc-plex-ratings-pills", STATE.mount);
    if (!sel || !bar) return;
    syncPillBar(bar, csvFromSelect(sel, true));
  }


  const API = {
    cfgGet: () => j("/api/config"),
    providerInstances: (p) => j(`/api/provider-instances/${encodeURIComponent(String(p || ""))}?ts=${Date.now()}`),
    users: async (instanceId) => {
  const prov = provider();
  const routesMode = isRoutesMode();
  let routesOut = undefined;
  const inst = String(instanceId || "default");
  if (prov === "emby") {
    const x = await j(`/api/emby/users?instance=${encodeURIComponent(inst)}`);
    const a = Array.isArray(x) ? x : Array.isArray(x?.users) ? x.users : [];
    return Array.isArray(a) ? a : [];
  }
  if (prov === "jellyfin") {
    const x = await j(`/api/jellyfin/users?instance=${encodeURIComponent(inst)}`);
    const a = Array.isArray(x) ? x : Array.isArray(x?.users) ? x.users : [];
    return Array.isArray(a) ? a : [];
  }
  const x = await j(`/api/plex/users?instance=${encodeURIComponent(inst)}`);
  const a = Array.isArray(x) ? x : Array.isArray(x?.users) ? x.users : [];
  return Array.isArray(a) ? a : [];
},
serverUUID: async (instanceId) => {
  const prov = provider();
  const inst = String(instanceId || "default");
  if (prov === "emby") {
    const x = await j(`/api/emby/inspect?instance=${encodeURIComponent(inst)}`);
    const uid = x?.user_id || x?.user?.Id || x?.id || "";
    return { id: uid };
  }
  if (prov === "jellyfin") {
    const x = await j(`/api/jellyfin/inspect?instance=${encodeURIComponent(inst)}`);
    const uid = x?.user_id || x?.user?.Id || x?.id || "";
    return { id: uid };
  }
  // Plex: inspect no longer returns server UUID; use the dedicated endpoint.
  const x = await j(`/api/plex/server_uuid?instance=${encodeURIComponent(inst)}`);
  return { server_uuid: x?.server_uuid || x?.uuid || x?.serverUUID || "" };
},
    watch: {
      status: () => j("/api/watch/status"),
      start: (prov, sink) => (prov && sink) ? j(`/api/watch/start?provider=${encodeURIComponent(prov)}&sink=${encodeURIComponent(sink)}`, { method: "POST" }) : j("/api/watch/start", { method: "POST" }),
      stop: () => j("/api/watch/stop", { method: "POST" }),
    },
  };

  async function persistConfigPaths(pairs, noteId) {
    try {
      const serverCfg = await API.cfgGet();
      const cfg = typeof structuredClone === "function" ? structuredClone(serverCfg || {}) : JSON.parse(JSON.stringify(serverCfg || {}));
      for (const [path, value] of pairs || []) deepSet(cfg, String(path || ""), value);
      const r = await fetch("/api/config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        cache: "no-store",
        body: JSON.stringify(cfg),
      });
      if (!r.ok) throw new Error(`POST /api/config ${r.status}`);
    } catch (e) {
      console.warn("[scrobbler] save failed:", e);
      if (noteId) setNote(noteId, "Couldn’t save settings. Hit Save or check logs.", "err");
    }
  }

  
  // Routes mode support
  const ROUTES_NOTICE_KEY = "cw.scrobble.migrated_notice.v1";
  const ROUTES_TAB_KEY = "cw.ui.scrobbler.routes.active.v1";
  const ROUTE_PROVIDERS = ["plex", "emby", "jellyfin"];
  const ROUTE_SINKS = ["trakt", "simkl", "mdblist"];

  const hasOwn = (o, k) => !!o && Object.prototype.hasOwnProperty.call(o, k);

  function isRoutesMode() {
    const wcfg = STATE.cfg?.scrobble?.watch;
    return !!wcfg && hasOwn(wcfg, "routes") && Array.isArray(wcfg.routes);
  }

  function getRoutes() {
    const wcfg = STATE.cfg?.scrobble?.watch;
    return Array.isArray(wcfg?.routes) ? wcfg.routes : [];
  }

  function setRoutes(routes) {
    STATE.cfg.scrobble ||= {};
    STATE.cfg.scrobble.watch ||= {};
    STATE.cfg.scrobble.watch.routes = Array.isArray(routes) ? routes : [];
  }

  function deepClone(v) {
    try { return typeof structuredClone === "function" ? structuredClone(v) : JSON.parse(JSON.stringify(v)); } catch { return v; }
  }
  function canonicalInstanceId(id){
    const fn = (window.cwMediaUserPicker && typeof window.cwMediaUserPicker.canonicalInstanceId === "function") ? window.cwMediaUserPicker.canonicalInstanceId : null;
    if (fn) return fn(id);
    const s = String(id || "").trim();
    const out = s.replace(/:\d+$/, "").trim();
    return out || "default";
  }


  function normalizeRoute(r, idFallback) {
    const x = (r && typeof r === "object") ? r : {};
    const p0 = x.provider;
    const s0 = x.sink;
    const pi0 = x.provider_instance;
    const si0 = x.sink_instance;
    const p = (p0 === undefined || p0 === null) ? "plex" : String(p0);
    const s = (s0 === undefined || s0 === null) ? "trakt" : String(s0);
    const pi = (pi0 === undefined || pi0 === null) ? "default" : String(pi0);
    const si = (si0 === undefined || si0 === null) ? "default" : String(si0);
    return {
      id: String(x.id || idFallback || "").trim() || "R1",
      enabled: x.enabled !== false,
      provider: p.trim().toLowerCase(),
      provider_instance: canonicalInstanceId(pi),
      sink: s.trim().toLowerCase(),
      sink_instance: canonicalInstanceId(si),
      filters: deepClone(x.filters || {}),
    };
  }


  function nextRouteId() {
    const used = new Set(getRoutes().map(r => String(r?.id || "").trim()).filter(Boolean));
    let i = 1;
    while (used.has(`R${i}`)) i++;
    return `R${i}`;
  }

  function legacyToRoutesIfMissing() {
    const wcfg = STATE.cfg?.scrobble?.watch || {};
    const hasRoutesKey = hasOwn(wcfg, "routes");
    const routesArr = Array.isArray(wcfg.routes) ? wcfg.routes : null;
    if (routesArr && routesArr.length) return { migrated: false };
    if (hasRoutesKey && !routesArr) return { migrated: false };

    const prov = String(wcfg.provider || "").trim();
    const sink = String(wcfg.sink || "").trim();
    if (!prov || !sink) return { migrated: false };
    const sinks = sink.split(",").map(s => s.trim()).filter(Boolean);
    const filters = deepClone(wcfg.filters || {});
    const routes = sinks.map((s, i) => normalizeRoute({ id: `R${i + 1}`, enabled: true, provider: prov, provider_instance: "default", sink: s, sink_instance: "default", filters }, `R${i + 1}`));
    STATE.cfg.scrobble ||= {};
    STATE.cfg.scrobble.watch ||= {};
    STATE.cfg.scrobble.watch.routes = routes;
    STATE.cfg.scrobble.watch.routes_migrated_from_legacy = true;
    return { migrated: true };
  }

  function activeRouteId() {
    const routes = getRoutes();
    if (!routes.length) return null;
    const saved = String(localStorage.getItem(ROUTES_TAB_KEY) || "").trim();
    if (saved && routes.some(r => r.id === saved)) return saved;
    return routes[0].id;
  }

  function setActiveRouteId(id) {
    const rid = String(id || "").trim();
    if (!rid) return;
    localStorage.setItem(ROUTES_TAB_KEY, rid);
  }

  function getActiveRoute() {
    const rid = activeRouteId();
    if (!rid) return null;
    return getRoutes().find(r => r.id === rid) || null;
  }

  function routeLabel(r) {
    const pi = String(r.provider_instance || "default");
    const si = String(r.sink_instance || "default");
    const p0 = String(r.provider || "").trim();
    const s0 = String(r.sink || "").trim();
    const p = p0 ? p0 : "—";
    const s = s0 ? s0 : "—";
    return `${r.id} ${p}(${pi}) → ${s}(${si})`;
  }


function routeKey(r) {
  const p = String(r?.provider || "").trim().toLowerCase();
  const s = String(r?.sink || "").trim().toLowerCase();
  if (!p || !s) return "";
  const pi = String(r?.provider_instance || "default").trim().toLowerCase() || "default";
  const si = String(r?.sink_instance || "default").trim().toLowerCase() || "default";
  return `${p}|${pi}|${s}|${si}`;
}

function isDuplicateRoute(candidate, routes, selfId) {
  const key = routeKey(candidate);
  const sid = String(selfId || candidate?.id || "").trim();
  if (!key || !sid) return false;
  return (routes || []).some(r => String(r?.id || "").trim() !== sid && routeKey(r) === key);
}

function findDuplicateRouteKeys(routes) {
  const map = new Map();
  for (const r of (routes || [])) {
    const k = routeKey(r);
    if (!k) continue;
    const arr = map.get(k) || [];
    arr.push(String(r?.id || ""));
    map.set(k, arr);
  }
  const dups = [];
  for (const [k, ids] of map.entries()) {
    if (ids.length > 1) dups.push({ key: k, ids });
  }
  return dups;
}


function pickNonDuplicateTemplate(routes, baseProv, baseSink) {
  const provs = [String(baseProv || "").trim(), "plex", "emby", "jellyfin"].filter(Boolean);
  const sinks = [String(baseSink || "").trim(), "trakt", "simkl", "mdblist"].filter(Boolean);
  const uniq = (arr) => {
    const out = [];
    const seen = new Set();
    for (const v of arr) {
      const k = String(v || "").trim().toLowerCase();
      if (!k || seen.has(k)) continue;
      seen.add(k);
      out.push(k);
    }
    return out;
  };

  const P = uniq(provs);
  const S = uniq(sinks);

  for (const p of P) {
    for (const s of S) {
      const cand = { id: "__tmp__", provider: p, provider_instance: "default", sink: s, sink_instance: "default" };
      if (!isDuplicateRoute(cand, routes, "__tmp__")) return { provider: p, sink: s };
    }
  }
  return null;
}

  function applyRouteView(route) {
    if (!route) return;
    // Clear transient route-specific picker notes when switching routes
    try {
      setNote("sc-users-note", "");
      setNote("sc-uuid-note", "");
    } catch {}
    const prov = String(route.provider || "").trim().toLowerCase();
    const sink = String(route.sink || "").trim().toLowerCase();
    if (prov) deepSet(STATE.cfg, "scrobble.watch.provider", prov);
    if (sink) deepSet(STATE.cfg, "scrobble.watch.sink", sink);
    deepSet(STATE.cfg, "scrobble.watch.filters", deepClone(route.filters || {}));
    try {
      const pvSel = $("#sc-provider", STATE.mount);
      if (pvSel && prov) pvSel.value = prov;
      const skSel = $("#sc-sink", STATE.mount);
      if (skSel && sink) skSel.value = sink;
      try { syncProviderPickerUi(); } catch {}
      try { syncSinkPillsFromSelect(); } catch {}
    } catch {}
  }

  function syncActiveRouteFromView(filtersObj) {
    const r = getActiveRoute();
    if (!r) return;
    const f = filtersObj || read("scrobble.watch.filters", {}) || {};
    r.filters = deepClone(f);
  }

  function activeProviderInstance() {
    const r = getActiveRoute();
    return r ? canonicalInstanceId(r.provider_instance || "default") : "default";
  }

  async function getInstanceOptions(providerName) {
    const p = String(providerName || "").toLowerCase();
    if (!p) return [{ id: "default", name: "Default" }];
    STATE._routesCache ||= {};
    if (STATE._routesCache[p]) return STATE._routesCache[p];
    try {
      const x = await API.providerInstances(p);
      const items0 = Array.isArray(x) ? x : (x?.instances || []);
      const items = (items0 || []).map(i => ({
        id: String(i?.id || "").trim(),
        name: String(i?.label || i?.name || i?.id || "").trim() || String(i?.id || "").trim(),
      })).filter(i => i.id);

      // Deduplicate by id
      const seen = new Set();
      const uniq = [];
      for (const it of items) {
        const k = it.id.toLowerCase();
        if (seen.has(k)) continue;
        seen.add(k);
        uniq.push(it);
      }

      const hasDefault = uniq.some(i => i.id.toLowerCase() === "default");
      const def = { id: "default", name: "Default" };
      const list = hasDefault
        ? uniq.map(i => (i.id.toLowerCase() === "default" ? { id: "default", name: i.name || "Default" } : i))
        : [def].concat(uniq);

      STATE._routesCache[p] = list;
      return list;
    } catch {
      const fallback = [{ id: "default", name: "Default" }];
      STATE._routesCache[p] = fallback;
      return fallback;
    }
  }

  function overlayCfgFor(name, inst) {
    const p = String(name || "").toLowerCase();
    const iid = String(inst || "default");
    const base = (STATE.cfg && STATE.cfg[p]) ? STATE.cfg[p] : {};
    if (iid && iid !== "default" && base?.instances && base.instances[iid]) return Object.assign({}, base, base.instances[iid]);
    return base || {};
  }

  function activeRouteContext() {
    if (!isRoutesMode()) return { provider_instance: "default", sink_instance: "default" };
    const r = getActiveRoute() || null;
    return { provider_instance: String(r?.provider_instance || "default"), sink_instance: String(r?.sink_instance || "default") };
  }

  function activeProviderServerUrl() {
    const prov = provider();
    const ctx = activeRouteContext();
    if (prov === "plex") return String(overlayCfgFor("plex", ctx.provider_instance)?.server_url || "");
    if (prov === "emby") return String(overlayCfgFor("emby", ctx.provider_instance)?.server || "");
    return String(overlayCfgFor("jellyfin", ctx.provider_instance)?.server || "");
  }

  function syncServerPreviewUi() {
    const inp = $("#sc-pms-input", STATE.mount);
    if (!inp) return;
    inp.value = activeProviderServerUrl();
    inp.disabled = true;
  }

  function syncRouteActiveRowUi(rid) {
    const host = $("#sc-routes", STATE.mount);
    if (!host) return;
    $all("tr.sc-route-row", host).forEach((tr) => {
      const id = String(tr?.dataset?.rid || "").trim();
      tr.classList.toggle("sc-route-active", id && id === String(rid || "").trim());
    });
  }

  function setActiveRouteFromUi(rid) {
    const id = String(rid || "").trim();
    if (!id || id === activeRouteId()) return;
    try { syncActiveRouteFromView(); } catch {}
    setActiveRouteId(id);
    const r = getActiveRoute();
    if (r) applyRouteView(r);
    try {
      const sel = $("#sc-route-select", STATE.mount);
      if (sel) sel.value = id;
    } catch {}
    try { syncRouteActiveRowUi(id); } catch {}
    try { syncServerPreviewUi(); } catch {}
    try { applyModeDisable(); } catch {}
  }

  function providerAuthOkForRoute(r) {
    const p = String(r?.provider || "").toLowerCase();
    const ov = overlayCfgFor(p, r?.provider_instance);
    if (p === "plex") return !!String(ov.account_token || "").trim() && isValidServerUrl(String(ov.server_url || ""));
    if (p === "emby") return !!String(ov.access_token || "").trim();
    if (p === "jellyfin") return !!String(ov.access_token || "").trim();
    return false;
  }

  function sinkAuthOkForRoute(r) {
    const s = String(r?.sink || "").toLowerCase();
    const ov = overlayCfgFor(s, r?.sink_instance);
    if (s === "trakt") return !!String(ov.access_token || "").trim();
    if (s === "simkl") return !!String(ov.access_token || "").trim();
    if (s === "mdblist") return !!String(ov.api_key || "").trim();
    return false;
  }

  function anyStartableRoute() {
    return getRoutes().some(r => r?.enabled && providerAuthOkForRoute(r) && sinkAuthOkForRoute(r));
  }

  async function renderRouteSelector() {
    const wrap = $("#sc-route-filter-wrap", STATE.mount);
    const sel = $("#sc-route-select", STATE.mount);
    if (!wrap || !sel) return;
    const on = isRoutesMode();
    wrap.style.display = on ? "" : "none";
    if (!on) return;
    sel.innerHTML = "";
    const routes = getRoutes();
    routes.forEach((r) => sel.appendChild(el("option", { value: r.id, textContent: routeLabel(r) })));
    const rid = activeRouteId();
    if (rid) sel.value = rid;
  }

  function renderMigrateBanner(migrated) {
    const host = $("#sc-migrate-banner", STATE.mount);
    if (!host) return;
    if (!migrated) { host.style.display = "none"; host.innerHTML = ""; return; }
    if (localStorage.getItem(ROUTES_NOTICE_KEY) === "1") { host.style.display = "none"; host.innerHTML = ""; return; }

    host.style.display = "";
    host.innerHTML = `
      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
        <span>Routes were migrated from legacy watcher config.</span>
        <button type="button" id="sc-migrate-save" class="btn small">Upgrade watcher config</button>
        <button type="button" id="sc-migrate-dismiss" class="btn small">Dismiss</button>
      </div>
    `;
    const saveBtn = $("#sc-migrate-save", host);
    const disBtn = $("#sc-migrate-dismiss", host);
    on(saveBtn, "click", async () => {
      localStorage.setItem(ROUTES_NOTICE_KEY, "1");
      try {
        const sc = getScrobbleConfig();
        const serverCfg = await API.cfgGet();
        const cfg = deepClone(serverCfg || {});
        cfg.scrobble = sc;
        const rp = getRootPatch();
        cfg.plex = Object.assign({}, cfg.plex || {}, rp.plex || {});
        cfg.emby = Object.assign({}, cfg.emby || {}, rp.emby || {});
        cfg.jellyfin = Object.assign({}, cfg.jellyfin || {}, rp.jellyfin || {});
        const r = await fetch("/api/config", { method: "POST", headers: { "Content-Type": "application/json" }, cache: "no-store", body: JSON.stringify(cfg) });
        if (!r.ok) throw new Error(`POST /api/config ${r.status}`);
        w._cfgCache = cfg;
        STATE.cfg = cfg;
        setNote("sc-note", "Watcher config upgraded.");
      } catch {
        setNote("sc-note", "Couldn’t upgrade watcher config. Hit Save or check logs.", "err");
      }
      host.style.display = "none";
      host.innerHTML = "";
    });
    on(disBtn, "click", () => {
      localStorage.setItem(ROUTES_NOTICE_KEY, "1");
      host.style.display = "none";
      host.innerHTML = "";
    });
  }

  async function renderRoutesUi() {
    const wrap = $("#sc-routes-wrap", STATE.mount);
    const host = $("#sc-routes", STATE.mount);
    const legacy = $("#sc-legacy-picks", STATE.mount);
    const onMode = isRoutesMode();
    if (wrap) wrap.style.display = onMode ? "" : "none";
    if (legacy) legacy.style.display = onMode ? "none" : "";
    if (!host) return;
    if (!onMode) { host.innerHTML = ""; return; }

try {
  if (!d.getElementById("sc-routes-style")) {
    const st = d.createElement("style");
    st.id = "sc-routes-style";
    st.textContent = ".sc-route-row{cursor:pointer}.sc-route-active{box-shadow:0 0 0 2px rgba(34,197,94,.35) inset}.sc-route-dup{outline:2px solid rgba(220,53,69,.6);border-radius:6px}.sc-dup-badge{display:inline-block;font-size:11px;padding:2px 6px;border-radius:10px;background:rgba(220,53,69,.15);color:#dc3545;margin-right:8px}";
    d.head.appendChild(st);
  }
} catch {}

    const routes = getRoutes().map((r, i) => normalizeRoute(r, `R${i + 1}`));
    setRoutes(routes);

    const dups = findDuplicateRouteKeys(routes);
    STATE._dupRouteIds = new Set(dups.flatMap(d => d.ids || []));

    const rid0 = activeRouteId();
    if (!rid0 && routes.length) setActiveRouteId(routes[0].id);

    // Build table
    const table = el("table");
    const thead = el("thead");
    thead.innerHTML = "<tr><th>On</th><th>Provider</th><th>Profile</th><th>Sink</th><th>Profile</th><th></th></tr>";
    table.appendChild(thead);
    const tbody = el("tbody");
    table.appendChild(tbody);

    const activeRid = activeRouteId();

    for (const r of routes) {
            const isActive = String(r.id || "") === String(activeRid || "");
      const isDup = STATE._dupRouteIds && STATE._dupRouteIds.has(String(r.id || ""));
      const tr = el("tr", { className: "sc-route-row" + (isActive ? " sc-route-active" : "") + (isDup ? " sc-route-dup" : "") });
      tr.dataset.rid = r.id;

      const cOn = el("td");
      const chk = el("input", { type: "checkbox", checked: !!r.enabled });
      chk.dataset.rid = r.id;
      chk.dataset.f = "enabled";
      cOn.appendChild(chk);
      tr.appendChild(cOn);

      const cP = el("td");
      const pSel = el("select", { className: "input" });
      pSel.appendChild(el("option", { value: "", textContent: "Select…" }));
      ROUTE_PROVIDERS.forEach((p) => {
        const meta = PROVIDER_META[p] || { label: p, icon: "", alt: p };
        pSel.appendChild(el("option", { value: p, textContent: meta.label || p }));
      });
      pSel.value = r.provider;
      pSel.dataset.rid = r.id;
      pSel.dataset.f = "provider";
      cP.appendChild(makeRouteIconDropdown(pSel, PROVIDER_META, "Provider"));
      tr.appendChild(cP);

      const cPI = el("td");
      const piSel = el("select", { className: "input" });
      const hasP = !!String(r.provider || "").trim();
      const pOpts = hasP ? await getInstanceOptions(r.provider) : [{ id: "default", name: "Default" }];
      pOpts.forEach(i => piSel.appendChild(el("option", { value: i.id, textContent: i.name })));
      piSel.value = r.provider_instance || "default";
      piSel.disabled = !hasP;
      piSel.dataset.rid = r.id;
      piSel.dataset.f = "provider_instance";
      cPI.appendChild(piSel);
      tr.appendChild(cPI);

      const cS = el("td");
      const sSel = el("select", { className: "input" });
      sSel.appendChild(el("option", { value: "", textContent: "Select…" }));
      ROUTE_SINKS.forEach((s) => {
        const meta = SINK_META[s] || { label: s, icon: "", alt: s };
        sSel.appendChild(el("option", { value: s, textContent: meta.label || s }));
      });
      sSel.value = r.sink;
      sSel.dataset.rid = r.id;
      sSel.dataset.f = "sink";
      cS.appendChild(makeRouteIconDropdown(sSel, SINK_META, "Sink"));
      tr.appendChild(cS);

      const cSI = el("td");
      const siSel = el("select", { className: "input" });
      const hasS = !!String(r.sink || "").trim();
      const sOpts = hasS ? await getInstanceOptions(r.sink) : [{ id: "default", name: "Default" }];
      sOpts.forEach(i => siSel.appendChild(el("option", { value: i.id, textContent: i.name })));
      siSel.value = r.sink_instance || "default";
      siSel.disabled = !hasS;
      siSel.dataset.rid = r.id;
      siSel.dataset.f = "sink_instance";
      cSI.appendChild(siSel);
      tr.appendChild(cSI);

      const cA = el("td", { className: "sc-route-actions" });
      const filt = el("button", { type: "button", className: "btn small", textContent: "Filters" });
      filt.dataset.act = "filters";
      filt.dataset.rid = r.id;
      const rm = el("button", { type: "button", className: "btn small", textContent: "Remove" });
      rm.dataset.act = "remove";
      rm.dataset.rid = r.id;
      cA.append(filt, rm);
      tr.appendChild(cA);

      tbody.appendChild(tr);
    }

    host.innerHTML = "";
    host.appendChild(table);
    try { host.classList.toggle("sc-routes-auto", routes.length <= 3); } catch {}

    // Keep active view valid
    const rid = activeRouteId();
    if (!rid && routes.length) setActiveRouteId(routes[0].id);
    const ar = getActiveRoute() || routes[0] || null;
    if (ar) applyRouteView(ar);

    await renderRouteSelector();
    applyModeDisable();
  }
function chip(text, onRemove, onClick) {
    const c = el("span", { className: "chip" });
    const t = el("span", { textContent: text });
    if (onClick) {
      t.style.cursor = "pointer";
      t.title = "Click to select";
      on(t, "click", () => onClick(text));
    }
    const rm = el("span", { className: "rm", textContent: "×" });
    on(rm, "click", () => onRemove(text));
    c.append(t, rm);
    return c;
  }

  function isValidServerUrl(s) {
    try {
      const u = new URL(String(s || "").trim());
      return u.protocol === "http:" || u.protocol === "https:";
    } catch {
      return false;
    }
  }

  function setWatcherStatus(st) {
    const dot = $("#sc-status-dot", STATE.mount);
    const badge = $("#sc-status-badge", STATE.mount);
    const text = $("#sc-status-text", STATE.mount);
    const last = $("#sc-status-last", STATE.mount);
    const up = $("#sc-status-up", STATE.mount);
    const alive = !!st?.alive;
    if (dot) dot.className = "status-dot " + (alive ? "on" : "off");
    if (badge) {
      badge.className = "badge " + (alive ? "is-on" : "is-off");
      badge.textContent = alive ? "Running" : "Stopped";
    }
    if (text) text.textContent = alive ? "Active" : "Inactive";
    if (last) last.textContent = st?.last_run ? `Last: ${st.last_run}` : "";
    if (up) up.textContent = st?.uptime ? `Up: ${st.uptime}` : "";
  }

  function applyModeDisable() {
  const enabled = !!read("scrobble.enabled", false);
  const mode = String(read("scrobble.mode", "webhook")).toLowerCase();
  const useWebhook = enabled && mode === "webhook";
  const useWatch = enabled && mode === "watch";

  const webRoot = STATE.webhookHost;
  const watchRoot = STATE.watcherHost;
  if (!webRoot || !watchRoot) return;

  const wh = $("#sc-enable-webhook", STATE.mount);
  const wa = $("#sc-enable-watcher", STATE.mount);

  const webhookOn = !!wh?.checked && useWebhook;
  const watcherOn = !!wa?.checked && useWatch;

  // Routes mode: show routes editor and per-route filters
  try {
    const routesMode = isRoutesMode();
    const legacy = $("#sc-legacy-picks", STATE.mount);
    const routesWrap = $("#sc-routes-wrap", STATE.mount);
    const routeFilterWrap = $("#sc-route-filter-wrap", STATE.mount);
    if (legacy) legacy.style.display = routesMode ? "none" : "";
    if (routesWrap) routesWrap.style.display = routesMode ? "" : "none";
    if (routeFilterWrap) routeFilterWrap.style.display = routesMode ? "" : "none";
  } catch {}

  $all(".input, input, button, select, textarea", webRoot).forEach((n) => {
    if (!String(n.id || "").startsWith("sc-enable-webhook")) n.disabled = !webhookOn;
  });
  $all(".input, input, button, select, textarea", watchRoot).forEach((n) => {
    if (n.id !== "sc-enable-watcher") n.disabled = !watcherOn;
  });

  const prov = provider();
  const ctx = activeRouteContext();
  const provInst = isRoutesMode() ? ctx.provider_instance : "default";
  const sinkInst = isRoutesMode() ? ctx.sink_instance : "default";

  const plexCfg = overlayCfgFor("plex", provInst);
  const embyCfg = overlayCfgFor("emby", provInst);
  const jellyCfg = overlayCfgFor("jellyfin", provInst);

  const srv =
    prov === "plex"
      ? String(plexCfg?.server_url || read("plex.server_url", "") || "")
      : prov === "emby"
      ? String(embyCfg?.server || read("emby.server", "") || "")
      : String(jellyCfg?.server || read("jellyfin.server", "") || "");
  const lbl = prov === "plex" ? "Plex Server" : prov === "emby" ? "Emby Server" : "Jellyfin Server";
  const req = $("#sc-server-required", STATE.mount);
  if (req) req.style.display = prov === "plex" ? "" : "none";
  const lab = $("#sc-server-label", STATE.mount);
  if (lab) lab.textContent = lbl;

  const loadBtn = $("#sc-load-users", STATE.mount);
  if (loadBtn) {
    loadBtn.style.display = "";
    loadBtn.textContent = "Pick";
    loadBtn.title = prov === "plex" ? "Pick Plex user" : prov === "emby" ? "Pick Emby user" : "Pick Jellyfin user";
  }
  const fetchUuid = $("#sc-fetch-uuid", STATE.mount);
  if (fetchUuid) fetchUuid.disabled = false;
  const uuidLabel = $("#sc-uuid-label", STATE.mount);
  if (uuidLabel) uuidLabel.textContent = prov === "plex" ? "Server UUID" : "User ID";
  const uuidInput = $("#sc-server-uuid", STATE.mount);
  if (uuidInput) uuidInput.placeholder = prov === "plex" ? "e.g. abcd1234..." : "e.g. 80ee72c0...";

  const plexTokenOk = !!String(plexCfg?.account_token || read("plex.account_token", "") || "").trim();
  const embyTokenOk = !!String(embyCfg?.access_token || read("emby.access_token", "") || "").trim();
  const jellyTokenOk = !!String(jellyCfg?.access_token || read("jellyfin.access_token", "") || "").trim();

  const sinkRaw = read("scrobble.watch.sink", "trakt");
  const sink = normSinkCsv(sinkRaw == null ? "trakt" : sinkRaw);
  const hasSink = !!sink;

  const traktCfg = overlayCfgFor("trakt", sinkInst);
  const simklCfg = overlayCfgFor("simkl", sinkInst);
  const mdblCfg = overlayCfgFor("mdblist", sinkInst);

  const traktTokenOk = !!String(traktCfg?.access_token || read("trakt.access_token", "") || "").trim();
  const simklTokenOk = !!String(simklCfg?.access_token || read("simkl.access_token", "") || "").trim();
  const mdblTokenOk = !!String(mdblCfg?.api_key || read("mdblist.api_key", "") || "").trim();

  let sinkOk = true;
  let sinkErr = "";

  if (!hasSink) {
    sinkOk = false;
  } else {
    const wantsTrakt = sink.includes("trakt");
    const wantsSimkl = sink.includes("simkl");
    const wantsMDBList = sink.includes("mdblist");

    const missing = [];
    if (wantsTrakt && !traktTokenOk) missing.push("Trakt");
    if (wantsSimkl && !simklTokenOk) missing.push("SIMKL");
    if (wantsMDBList && !mdblTokenOk) missing.push("MDBList");

    if (missing.length) {
      sinkOk = false;
      const plural = missing.length > 1 ? "are" : "is";
      sinkErr = `${missing.join(" and ")} ${plural} not configured. Go to Authentication and configure it, or refresh your browser if you already configured it.`;
    }
  }

  rebuildPlexRatingsDropdown();

  if (watcherOn && !hasSink) setNote("sc-note", "You must select at least one sink to start the watcher.", "warn");
  else setNote("sc-note", "");

  if (watcherOn) {
    if (prov === "plex") {
      if (!plexTokenOk) setNote("sc-pms-note", "Not connected to Plex. Go to Authentication - Plex, or refresh your browser if you already configured it", "err");
      else if (!isValidServerUrl(srv)) setNote("sc-pms-note", "Plex Server is required (http(s)://…)", "err");
      else if (hasSink && !sinkOk) setNote("sc-pms-note", sinkErr, "err");
      else setNote("sc-pms-note", "");
    } else if (prov === "emby") {
      if (!embyTokenOk) setNote("sc-pms-note", "Not connected to Emby. Go to Authentication - Emby, or refresh your browser if you already configured it", "err");
      else if (hasSink && !sinkOk) setNote("sc-pms-note", sinkErr, "err");
      else setNote("sc-pms-note", "");
    } else {
      if (!jellyTokenOk) setNote("sc-pms-note", "Not connected to Jellyfin. Go to Authentication - Jellyfin, or refresh your browser if you already configured it", "err");
      else if (hasSink && !sinkOk) setNote("sc-pms-note", sinkErr, "err");
      else setNote("sc-pms-note", "");
    }
  } else {
    setNote("sc-pms-note", "");
    setNote("sc-note", "");
  }

  if (loadBtn) {
    if (prov === "plex" && !plexTokenOk) loadBtn.disabled = true;
    else if (prov === "emby" && !embyTokenOk) loadBtn.disabled = true;
    else if (prov === "jellyfin" && !jellyTokenOk) loadBtn.disabled = true;
    else loadBtn.disabled = !watcherOn;
  }
  if (fetchUuid) {
    if (prov === "plex" && !plexTokenOk) fetchUuid.disabled = true;
    else if (prov === "emby" && !embyTokenOk) fetchUuid.disabled = true;
    else if (prov === "jellyfin" && !jellyTokenOk) fetchUuid.disabled = true;
    else fetchUuid.disabled = !watcherOn;
  }

  const auto = $("#sc-autostart", STATE.mount);
  if (auto) {
    if (watcherOn && !hasSink) {
      auto.checked = false;
      auto.disabled = true;
      if (!!read("scrobble.watch.autostart", false)) {
        write("scrobble.watch.autostart", false);
        STATE.ui.watchAutostart = false;
        if (!STATE._noSinkAutostartFixApplied) {
          STATE._noSinkAutostartFixApplied = true;
          persistConfigPaths([["scrobble.watch.autostart", false]], "sc-pms-note");
        }
      }
    } else {
      auto.disabled = !watcherOn;
    }
  }

  const startBtn = $("#sc-watch-start", STATE.mount);
  if (startBtn) {
    if (isRoutesMode()) {
      startBtn.disabled = !watcherOn || !anyStartableRoute();
    } else {
      const providerOk = prov === "plex" ? plexTokenOk && isValidServerUrl(srv) : prov === "emby" ? embyTokenOk : jellyTokenOk;
      startBtn.disabled = !watcherOn || !providerOk || !sinkOk;
    }
  }
}


  function buildAdvField(id, label, tipId, placeholder, opts = {}) {
    const min = Number.isFinite(+opts.min) ? +opts.min : 1;
    const max = Number.isFinite(+opts.max) ? +opts.max : 100;
    const step = Number.isFinite(+opts.step) ? +opts.step : 1;
    return `<div class="field"><label for="${id}">${label}</label>${helpBtn(tipId)}<input id="${id}" class="input" type="number" inputmode="numeric" min="${min}" max="${max}" step="${step}" placeholder="${placeholder}"></div>`;
  }
  

  function buildUI() {
    injectStyles();

        if (STATE.webhookHost) {
      STATE.webhookHost.innerHTML = `
        <div class="cw-panel">
          <div class="cw-meta-provider-panel active" data-provider="webhook">
            <div class="cw-panel-head">
              <div>
                <div class="cw-panel-title">Webhooks</div>
                <div class="muted">Legacy endpoints that scrobble to Trakt.</div>
              </div>
            </div>

            <div class="cw-subtiles" style="margin-top:2px">
              <button type="button" class="cw-subtile active" data-sub="plex">Plex</button>
              <button type="button" class="cw-subtile" data-sub="jellyfin">Jellyfin</button>
              <button type="button" class="cw-subtile" data-sub="emby">Emby</button>
              <button type="button" class="cw-subtile" data-sub="advanced">Advanced</button>
            </div>

            <div id="sc-webhook-warning" class="micro-note" style="margin-top:10px"></div>
            <div id="sc-endpoint-note" class="micro-note"></div>

            <div class="cw-subpanels">
              <div class="cw-subpanel active" data-sub="plex">
                <div class="row" style="justify-content:space-between;align-items:center;margin-top:6px">
                  <label class="cx-toggle">
                    <input type="checkbox" id="sc-enable-webhook">
                    <span class="cx-toggle-ui" aria-hidden="true"></span>
                    <span class="cx-toggle-text">Enable</span>
                    <span class="cx-toggle-state" aria-hidden="true"></span>
                  </label>
                  <div class="codepair right" style="margin-left:auto">
                    <img class="wh-logo" src="/assets/img/PLEX-log.svg" alt="Plex">
                    <code id="sc-webhook-url-plex"></code>
                    <button id="sc-copy-plex" class="btn small">Copy</button>
                  </div>
                </div>

                <div class="sc-subbox">
                  <div class="head">Options</div>
                  <div class="body">
                    <span class="cx-switch-wrap">
                      <label class="sc-toggle"><input type="checkbox" id="sc-delete-plex-webhook"><span class="one-line">Auto-remove from Watchlists</span></label>
                      ${helpBtn("sc-help-auto-remove")}
                    </span>
                  </div>
                </div>

                <div class="sc-subbox">
                  <div class="head">Filters</div>
                  <div class="body">
                    <div class="sc-filter-grid">
                      <div>
                        <div class="muted">Username whitelist</div>
                        <div id="sc-whitelist-webhook" class="chips" style="margin-top:4px"></div>
                        <div id="sc-users-note-webhook" class="micro-note"></div>
                        <div style="display:flex;gap:8px;margin-top:6px">
                          <input id="sc-user-input-webhook" class="input" placeholder="Add username..." style="flex:1">
                          <button id="sc-add-user-webhook" class="btn small">Add</button>
                          <button id="sc-load-users-webhook" class="btn small">Pick</button>
                        </div>
                      </div>
                      <div>
                        <div class="muted">Server UUID</div>
                        <div id="sc-uuid-note-webhook" class="micro-note"></div>
                        <div style="display:flex;gap:8px;align-items:center;margin-top:6px">
                          <input id="sc-server-uuid-webhook" class="input" placeholder="e.g. abcd1234..." style="flex:1">
                          <button id="sc-fetch-uuid-webhook" class="btn small">Fetch</button>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>

                <div class="sc-subbox">
                  <div class="head">Plex settings</div>
                  <div class="body">
                    <span class="cx-switch-wrap">
                      <label class="sc-toggle"><input type="checkbox" id="sc-webhook-plex-ratings"><span class="one-line">Enable ratings</span></label>
                      ${helpBtn("sc-help-webhook-plex-ratings")}
                    </span>
                  </div>
                </div>
              </div>

              <div class="cw-subpanel" data-sub="jellyfin">
                <div class="row" style="justify-content:space-between;align-items:center;margin-top:6px">
                  <label class="cx-toggle">
                    <input type="checkbox" id="sc-enable-webhook-jf">
                    <span class="cx-toggle-ui" aria-hidden="true"></span>
                    <span class="cx-toggle-text">Enable</span>
                    <span class="cx-toggle-state" aria-hidden="true"></span>
                  </label>
                  <div class="codepair right" style="margin-left:auto">
                    <img class="wh-logo" src="/assets/img/JELLYFIN-log.svg" alt="Jellyfin">
                    <code id="sc-webhook-url-jf"></code>
                    <button id="sc-copy-jf" class="btn small">Copy</button>
                  </div>
                </div>

                <div class="sc-subbox">
                  <div class="head">Options</div>
                  <div class="body">
                    <span class="cx-switch-wrap">
                      <label class="sc-toggle"><input type="checkbox" id="sc-delete-plex-webhook-jf"><span class="one-line">Auto-remove from Watchlists</span></label>
                      ${helpBtn("sc-help-auto-remove")}
                    </span>
                  </div>
                </div>
              </div>

              <div class="cw-subpanel" data-sub="emby">
                <div class="row" style="justify-content:space-between;align-items:center;margin-top:6px">
                  <label class="cx-toggle">
                    <input type="checkbox" id="sc-enable-webhook-emby">
                    <span class="cx-toggle-ui" aria-hidden="true"></span>
                    <span class="cx-toggle-text">Enable</span>
                    <span class="cx-toggle-state" aria-hidden="true"></span>
                  </label>
                  <div class="codepair right" style="margin-left:auto">
                    <img class="wh-logo" src="/assets/img/EMBY-log.svg" alt="Emby">
                    <code id="sc-webhook-url-emby"></code>
                    <button id="sc-copy-emby" class="btn small">Copy</button>
                  </div>
                </div>

                <div class="sc-subbox">
                  <div class="head">Options</div>
                  <div class="body">
                    <span class="cx-switch-wrap">
                      <label class="sc-toggle"><input type="checkbox" id="sc-delete-plex-webhook-emby"><span class="one-line">Auto-remove from Watchlists</span></label>
                      ${helpBtn("sc-help-auto-remove")}
                    </span>
                  </div>
                </div>
              </div>

              <div class="cw-subpanel" data-sub="advanced">
                <div class="row" style="justify-content:flex-start;align-items:center;margin-top:6px">
                  <label class="cx-toggle">
                    <input type="checkbox" id="sc-enable-webhook-adv">
                    <span class="cx-toggle-ui" aria-hidden="true"></span>
                    <span class="cx-toggle-text">Enable</span>
                    <span class="cx-toggle-state" aria-hidden="true"></span>
                  </label>
                </div>

                <div class="sc-subbox">
                  <div class="head">Advanced</div>
                  <div class="body">
                    <div class="sc-adv-grid">
                      ${buildAdvField("sc-pause-debounce-webhook", "Pause", "sc-help-adv-pause", DEFAULTS.watch.pause_debounce_seconds)}
                      ${buildAdvField("sc-suppress-start-webhook", "Suppress", "sc-help-adv-suppress", DEFAULTS.watch.suppress_start_at)}
                      ${buildAdvField("sc-regress-webhook", "Regress %", "sc-help-adv-regress", DEFAULTS.trakt.regress_tolerance_percent)}
                      ${buildAdvField("sc-stop-pause-webhook", "Stop pause ≥", "sc-help-adv-stop-pause", DEFAULTS.trakt.stop_pause_threshold)}
                      ${buildAdvField("sc-force-stop-webhook", "Force stop", "sc-help-adv-force-stop", DEFAULTS.trakt.force_stop_at)}
                    </div>
                    <div class="micro-note" style="margin-top:6px">Empty resets to defaults. Values are 1–100.</div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      `;

      // Tabs: Plex / Jellyfin / Emby / Advanced
      const tabKey = "cw.ui.scrobbler.webhook.tab.v1";
      const root = STATE.webhookHost;
      const selectTab = (sub, opts = {}) => {
        const want = (sub || "plex").toLowerCase();
        root.querySelectorAll('.cw-subtile[data-sub]').forEach((btn) => {
          btn.classList.toggle("active", (btn.dataset.sub || "").toLowerCase() === want);
        });
        root.querySelectorAll('.cw-subpanel[data-sub]').forEach((sp) => {
          sp.classList.toggle("active", (sp.dataset.sub || "").toLowerCase() === want);
        });
        if (opts.persist !== false) {
          try { localStorage.setItem(tabKey, want); } catch {}
        }
      };
      STATE._watcherSelectTab = selectTab;

      root.querySelectorAll('.cw-subtile[data-sub]').forEach((btn) => {
        btn.addEventListener("click", () => selectTab(btn.dataset.sub || "plex"));
      });

      try { selectTab(localStorage.getItem(tabKey) || "plex", { persist: false }); } catch { selectTab("plex", { persist: false }); }
    }

    if (STATE.watcherHost) {
      STATE.watcherHost.innerHTML = `
        <style>
          .cc-wrap{display:grid;grid-template-columns:1fr 1fr;gap:16px}
          .cc-card{padding:14px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset}
          .cc-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}
          .cc-body{display:grid;gap:14px}
          .cc-gauge{width:100%;min-height:68px;display:flex;align-items:center;gap:14px;flex-wrap:wrap;padding:14px 16px;border-radius:14px;background:rgba(255,255,255,.05);box-shadow:inset 0 0 0 1px rgba(255,255,255,.08)}
          .cc-state{display:flex;flex-direction:column;line-height:1.15}
          .cc-state .lbl{font-size:12px;opacity:.75}
          .cc-state .val{font-size:22px;font-weight:800;letter-spacing:.2px}
          .cc-meta{display:flex;gap:16px;flex-wrap:wrap;font-size:12px;opacity:.85}
          .cc-actions{display:flex;gap:12px;justify-content:center;flex-wrap:wrap}
          .cc-auto{display:flex;justify-content:center;margin-top:2px}
          .status-dot{width:16px;height:16px;border-radius:50%;box-shadow:0 0 18px currentColor}
          .status-dot.on{background:#22c55e;color:#22c55e}
          .status-dot.off{background:#ef4444;color:#ef4444}
          @media (max-width:900px){.cc-wrap{grid-template-columns:1fr}}
        
          .sc-box{display:block;margin-top:12px;border-radius:12px;background:var(--panel,#111);box-shadow:0 0 0 1px rgba(255,255,255,.05) inset}
          .sc-box>.body{padding:12px 14px}

        </style>

        <div class="cw-panel">
          <div class="cw-meta-provider-panel active" data-provider="watcher">
            <div class="cw-panel-head">
              <div>
                <div class="cw-panel-title">Watcher</div>
                <div class="muted">Monitor playback and scrobble automatically.</div>
              </div>
            </div>

            <div class="cw-subtiles" style="margin-top:2px">
              <button type="button" class="cw-subtile active" data-sub="watcher">Watcher</button>
              <button type="button" class="cw-subtile" data-sub="filters">Filters</button>
              <button type="button" class="cw-subtile" data-sub="advanced">Advanced</button>
            </div>

            <div class="cw-subpanels">
              <div class="cw-subpanel active" data-sub="watcher">


        <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;flex-wrap:wrap">
	          <label class="cx-toggle">
	            <input type="checkbox" id="sc-enable-watcher">
	            <span class="cx-toggle-ui" aria-hidden="true"></span>
	            <span class="cx-toggle-text">Enable</span>
	            <span class="cx-toggle-state" aria-hidden="true"></span>
	          </label>
          <div id="sc-legacy-picks" style="margin-left:auto;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
            <span style="opacity:.75;font-size:12px">Sink</span>
            <div id="sc-sink-pills" class="sc-pillbar" role="group" aria-label="Sink"></div>
            <select id="sc-sink" class="input" style="display:none;width:240px">
              <option value="">None</option>
              <option value="trakt">Trakt</option>
              <option value="simkl">SIMKL</option>
              <option value="mdblist">MDBList</option>
              <option value="trakt,simkl">Trakt & SIMKL</option>
              <option value="trakt,mdblist">Trakt & MDBList</option>
              <option value="simkl,mdblist">SIMKL & MDBList</option>
              <option value="trakt,simkl,mdblist">Trakt & SIMKL & MDBList</option>
            </select>
<span style="opacity:.75;font-size:12px">Provider</span>
            <div class="sc-prov-wrap">
              <button type="button" id="sc-provider-btn" class="input sc-prov-btn" aria-haspopup="listbox" aria-expanded="false">
                <span class="sc-prov-left">
                  <img class="wh-logo sc-prov-ico" id="sc-provider-icon" src="/assets/img/PLEX-log.svg" alt="Plex">
                  <span id="sc-provider-label">Plex</span>
                </span>
                <span class="sc-prov-caret" aria-hidden="true">▾</span>
              </button>
              <div id="sc-provider-menu" class="sc-prov-menu hidden" role="listbox" aria-label="Provider">
                <button type="button" class="sc-prov-item" role="option" data-value="plex" aria-selected="true">
                  <img class="wh-logo sc-prov-ico" src="/assets/img/PLEX-log.svg" alt="Plex">
                  <span>Plex</span>
                </button>
                <button type="button" class="sc-prov-item" role="option" data-value="emby" aria-selected="false">
                  <img class="wh-logo sc-prov-ico" src="/assets/img/EMBY-log.svg" alt="Emby">
                  <span>Emby</span>
                </button>
                <button type="button" class="sc-prov-item" role="option" data-value="jellyfin" aria-selected="false">
                  <img class="wh-logo sc-prov-ico" src="/assets/img/JELLYFIN-log.svg" alt="Jellyfin">
                  <span>Jellyfin</span>
                </button>
              </div>
              <select id="sc-provider" class="input" style="display:none">
                <option value="plex">Plex</option>
                <option value="emby">Emby</option>
                <option value="jellyfin">Jellyfin</option>
              </select>
            </div>

          </div>
        </div>

        
        <div id="sc-routes-wrap" class="sc-box" style="display:none;margin:8px 0 10px">
          <div class="body">
            <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:8px">
              <div style="font-size:12px;opacity:.8">Routes</div>
              <div style="margin-left:auto;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
                <button type="button" id="sc-route-add" class="btn small">Add Route</button>
              </div>
            </div>
            <div id="sc-routes" class="sc-route-table"></div>
            <div id="sc-migrate-banner" class="micro-note" style="margin-top:8px;display:none"></div>
          </div>
        </div>
<div id="sc-note" class="micro-note" style="margin:6px 0 10px"></div>

	        <div class="cc-wrap">
	          <div class="cc-card" id="sc-card-status">
            <div class="cc-head">
              <div>Watcher Status</div>
              <span id="sc-status-badge" class="badge is-off">Stopped</span>
            </div>
            <div class="cc-body">
              <div class="cc-gauge">
                <span id="sc-status-dot" class="status-dot off"></span>
                <div class="cc-state">
                  <span class="lbl">Status</span>
                  <span id="sc-status-text" class="val">Inactive</span>
                </div>
              </div>
              <div class="cc-meta">
                <span id="sc-status-last" class="micro-note"></span>
                <span id="sc-status-up" class="micro-note"></span>
              </div>
              <div class="cc-actions">
                <button id="sc-watch-start" class="btn small">Start</button>
                <button id="sc-watch-stop" class="btn small">Stop</button>
                <button id="sc-watch-refresh" class="btn small">Refresh</button>
              </div>
              <div class="cc-auto">
                <label class="cx-toggle"><input type="checkbox" id="sc-autostart"><span class="cx-toggle-ui" aria-hidden="true"></span><span class="cx-toggle-text">Autostart on boot</span><span class="cx-toggle-state" aria-hidden="true"></span></label>
              </div>
            </div>
          </div>

	          <div class="cc-card" id="sc-card-server">
	            <div class="cc-head">
	              <div>
	                <span id="sc-server-label">Media Server</span>
	                <span id="sc-server-required" class="pill req"></span>
	              </div>
	            </div>
	            <div id="sc-pms-note" class="micro-note" style="margin-top:2px"></div>
	            <div style="margin-top:12px">
	              <div class="muted">Server URL (http(s)://host[:port])</div>
	              <input id="sc-pms-input" class="input" placeholder="http://192.168.1.10:32400" readonly/>
	            </div>
	            <div style="margin-top:12px">
	              <div class="muted">Options</div>
	              <div class="sc-opt-col" style="margin-top:6px">
	                <span class="cx-switch-wrap">
	                  <label class="sc-toggle"><input type="checkbox" id="sc-delete-plex-watch"><span class="one-line">Auto-remove from Watchlists</span></label>
	              ${helpBtn("sc-help-auto-remove")}
	            </span>
	                                <div id="sc-plex-ratings-wrap" style="display:none">
	                  <div class="sc-opt-row">
			                    <div class="muted" style="margin:0">Enable ratings</div>
			                    ${helpBtn("sc-help-watch-plex-ratings")}
			                    <div id="sc-plex-ratings-pills" class="sc-pillbar" role="group" aria-label="Ratings"></div>
			                  </div>
			                  <div class="sc-opt-row" style="margin-top:6px">
			                    <select id="sc-plex-ratings" class="input" style="display:none;width:240px">
                          <option value="none">None</option>
                          <option value="trakt">Trakt</option>
                          <option value="simkl">SIMKL</option>
                          <option value="mdblist">MDBList</option>
                          <option value="trakt,simkl">Trakt & SIMKL</option>
                          <option value="trakt,mdblist">Trakt & MDBList</option>
                          <option value="simkl,mdblist">SIMKL & MDBList</option>
                          <option value="trakt,simkl,mdblist">Trakt & SIMKL & MDBList</option>
                        </select>
<div id="sc-plexwatcher-url-wrap" class="codepair" style="display:none">
	                      <code id="sc-plexwatcher-url"></code>
	                      <button id="sc-copy-plexwatcher" class="btn small">Copy</button>
	                    </div>
			                  </div>
			                  <div id="sc-plexwatcher-note" class="micro-note" style="margin-top:6px"></div>
			                </div>
	              </div>
	            </div>
	          </div>
        </div>

        
              </div>

              <div class="cw-subpanel" data-sub="filters">
                <div class="sc-box" id="sc-filters">
                  <div style="display:flex;justify-content:flex-end;margin-bottom:10px">${helpBtn("sc-help-watch-filters")}</div>
                  <div class="body">
<div id="sc-route-filter-wrap" style="display:none;margin-bottom:10px">
              <div class="muted">Filters for</div>
              <select id="sc-route-select" class="input" style="width:100%;max-width:100%;margin-top:6px"></select>
            </div>
<div class="sc-filter-grid">
              <div>
                <div class="muted">Username whitelist</div>
                <div id="sc-whitelist" class="chips" style="margin-top:4px"></div>
                <div id="sc-users-note" class="micro-note"></div>
                <div style="display:flex; gap:8px; margin-top:6px">
                  <input id="sc-user-input" class="input" placeholder="Add username..." style="flex:1">
                  <button id="sc-add-user" class="btn small">Add</button>
                  <button id="sc-load-users" class="btn small">Pick</button>
                </div>
              </div>
              <div>
                <div class="muted" id="sc-uuid-label">Server UUID</div>
                <div id="sc-uuid-note" class="micro-note"></div>
                <div style="display:flex; gap:8px; align-items:center; margin-top:6px">
                  <input id="sc-server-uuid" class="input" placeholder="e.g. abcd1234..." style="flex:1">
                  <button id="sc-fetch-uuid" class="btn small">Fetch</button>
                </div>
              </div>
            </div>
                  </div>
                </div>
              </div>

              <div class="cw-subpanel" data-sub="advanced">
                <div class="sc-box sc-advanced" id="sc-advanced">
                  <div style="display:flex;justify-content:flex-end;margin-bottom:10px">${helpBtn("sc-help-watch-advanced")}</div>
                  <div class="body">
<div class="sc-adv-grid">
              ${buildAdvField("sc-pause-debounce", "Pause", "sc-help-adv-pause", DEFAULTS.watch.pause_debounce_seconds)}
              ${buildAdvField("sc-suppress-start", "Suppress", "sc-help-adv-suppress", DEFAULTS.watch.suppress_start_at)}
              ${buildAdvField("sc-regress", "Regress", "sc-help-adv-regress", DEFAULTS.trakt.regress_tolerance_percent)}
              ${buildAdvField("sc-stop-pause", "Stop pause ≥", "sc-help-adv-stop-pause", DEFAULTS.trakt.stop_pause_threshold)}
              ${buildAdvField("sc-force-stop", "Force stop", "sc-help-adv-force-stop", DEFAULTS.trakt.force_stop_at)}
            </div>
            <div class="sc-adv-grid" style="grid-template-columns:repeat(1,minmax(0,1fr));margin-top:10px">
              ${buildAdvField("sc-progress-step", "Progress step", "sc-help-adv-progress-step", DEFAULTS.trakt.progress_step, { min: 1, max: 25, step: 1 })}
            </div>
            <div class="micro-note" style="margin-top:6px">Empty resets to defaults. Percent fields are 1–100. Progress step is 1–25.</div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
`;

      // Tabs: Watcher / Filters / Advanced
      const tabKey = "cw.ui.scrobbler.watcher.tab.v1";
      const root = STATE.watcherHost;
      const selectTab = (sub, opts = {}) => {
        const want = (sub || "watcher").toLowerCase();
        root.querySelectorAll('.cw-subtile[data-sub]').forEach((btn) => {
          btn.classList.toggle("active", (btn.dataset.sub || "").toLowerCase() === want);
        });
        root.querySelectorAll('.cw-subpanel[data-sub]').forEach((sp) => {
          sp.classList.toggle("active", (sp.dataset.sub || "").toLowerCase() === want);
        });
        if (opts.persist !== false) {
          try { localStorage.setItem(tabKey, want); } catch {}
        }
      };
      STATE._watcherSelectTab = selectTab;


      root.querySelectorAll('.cw-subtile[data-sub]').forEach((btn) => {
        btn.addEventListener("click", () => selectTab(btn.dataset.sub || "watcher"));
      });

      try { selectTab(localStorage.getItem(tabKey) || "watcher", { persist: false }); } catch { selectTab("watcher", { persist: false }); }

    }

    bindHelpTips(STATE.mount || d);
  }

  function ensureHiddenServerInputs() {
  const form = d.querySelector("form#settings, form#settings-form, form[data-settings]") || (STATE.mount || d.body);
  let h1 = d.getElementById("cfg-plex-server-url");
  if (!h1) {
    h1 = el("input", { type: "hidden", id: "cfg-plex-server-url", name: "plex.server_url" });
    form.appendChild(h1);
  }
  let h2 = d.getElementById("cfg-emby-server-url");
  if (!h2) {
    h2 = el("input", { type: "hidden", id: "cfg-emby-server-url", name: "emby.server" });
    form.appendChild(h2);
  }
  let h3 = d.getElementById("cfg-jellyfin-server-url");
  if (!h3) {
    h3 = el("input", { type: "hidden", id: "cfg-jellyfin-server-url", name: "jellyfin.server" });
    form.appendChild(h3);
  }
  let h4 = d.getElementById("cfg-trakt-progress-step");
  if (!h4) {
    h4 = el("input", { type: "hidden", id: "cfg-trakt-progress-step", name: "scrobble.trakt.progress_step" });
    form.appendChild(h4);
  }
  let h5 = d.getElementById("cfg-trakt-stop-pause-threshold");
  if (!h5) {
    h5 = el("input", { type: "hidden", id: "cfg-trakt-stop-pause-threshold", name: "scrobble.trakt.stop_pause_threshold" });
    form.appendChild(h5);
  }
  let h6 = d.getElementById("cfg-trakt-force-stop-at");
  if (!h6) {
    h6 = el("input", { type: "hidden", id: "cfg-trakt-force-stop-at", name: "scrobble.trakt.force_stop_at" });
    form.appendChild(h6);
  }
  let h7 = d.getElementById("cfg-trakt-regress-tolerance");
  if (!h7) {
    h7 = el("input", { type: "hidden", id: "cfg-trakt-regress-tolerance", name: "scrobble.trakt.regress_tolerance_percent" });
    form.appendChild(h7);
  }
  syncHiddenServerInputs();
}


  function syncHiddenServerInputs() {
  const h1 = d.getElementById("cfg-plex-server-url");
  if (h1) h1.value = String(read("plex.server_url", "") || "");
  const h2 = d.getElementById("cfg-emby-server-url");
  if (h2) h2.value = String(read("emby.server", "") || "");
  const h3 = d.getElementById("cfg-jellyfin-server-url");
  if (h3) h3.value = String(read("jellyfin.server", "") || "");
  const h4 = d.getElementById("cfg-trakt-progress-step");
  if (h4) h4.value = String(read("scrobble.trakt.progress_step", DEFAULTS.trakt.progress_step) ?? DEFAULTS.trakt.progress_step);
  const h5 = d.getElementById("cfg-trakt-stop-pause-threshold");
  if (h5) h5.value = String(read("scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold) ?? DEFAULTS.trakt.stop_pause_threshold);
  const h6 = d.getElementById("cfg-trakt-force-stop-at");
  if (h6) h6.value = String(read("scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at) ?? DEFAULTS.trakt.force_stop_at);
  const h7 = d.getElementById("cfg-trakt-regress-tolerance");
  if (h7) h7.value = String(read("scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent) ?? DEFAULTS.trakt.regress_tolerance_percent);
}

  function restoreDetailsState(sel, def, key) {
    const n = $(sel, STATE.mount);
    if (!n) return;
    let open = def;
    try {
      const v = localStorage.getItem(key);
      if (v != null) open = v === "1";
    } catch {}
    n.open = !!open;
    on(n, "toggle", () => {
      try {
        localStorage.setItem(key, n.open ? "1" : "0");
      } catch {}
    });
  }

  const readNum = (sel, dflt) => {
    const n = $(sel, STATE.mount);
    if (!n) return null;
    const raw = String(n.value ?? "").trim();
    return raw === "" ? clamp100(dflt) : norm100(raw, dflt);
  };

  const readRange = (sel, dflt, min, max) => {
    const n = $(sel, STATE.mount);
    if (!n) return null;
    const raw = String(n.value ?? "").trim();
    return raw === "" ? clampRange(dflt, min, max) : normRange(raw, dflt, min, max);
  };

  async function copyText(s) {
    try {
      await navigator.clipboard.writeText(s);
      return true;
    } catch {
      try {
        const ta = el("textarea", { style: "position:fixed;left:-9999px;top:-9999px" });
        ta.value = s;
        d.body.appendChild(ta);
        ta.select();
        const ok = d.execCommand ? d.execCommand("copy") : document.execCommand("copy");
        d.body.removeChild(ta);
        return !!ok;
      } catch {
        return false;
      }
    }
  }

  function commitAdvancedInputsWatch() {
    const pd = readNum("#sc-pause-debounce", DEFAULTS.watch.pause_debounce_seconds);
    if (pd != null) write("scrobble.watch.pause_debounce_seconds", pd);
    const ss = readNum("#sc-suppress-start", DEFAULTS.watch.suppress_start_at);
    if (ss != null) write("scrobble.watch.suppress_start_at", ss);
  }

  function commitAdvancedInputsWebhook() {
    const pd = readNum("#sc-pause-debounce-webhook", DEFAULTS.watch.pause_debounce_seconds);
    if (pd != null) write("scrobble.webhook.pause_debounce_seconds", pd);
    const ss = readNum("#sc-suppress-start-webhook", DEFAULTS.watch.suppress_start_at);
    if (ss != null) write("scrobble.webhook.suppress_start_at", ss);
  }

  function commitAdvancedInputsTrakt() {
    const mode = String(read("scrobble.mode", "webhook")).toLowerCase();
    const preferWebhook = mode === "webhook";
    const keys = preferWebhook
      ? [
          ["#sc-stop-pause-webhook", "scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold],
          ["#sc-force-stop-webhook", "scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at],
          ["#sc-regress-webhook", "scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent],
          ["#sc-stop-pause", "scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold],
          ["#sc-force-stop", "scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at],
          ["#sc-regress", "scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent],
        ]
      : [
          ["#sc-stop-pause", "scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold],
          ["#sc-force-stop", "scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at],
          ["#sc-regress", "scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent],
          ["#sc-stop-pause-webhook", "scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold],
          ["#sc-force-stop-webhook", "scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at],
          ["#sc-regress-webhook", "scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent],
        ];

    const wrote = new Set();
    for (const [sel, path, dflt] of keys) {
      if (wrote.has(path)) continue;
      const v = readNum(sel, dflt);
      if (v == null) continue;
      write(path, v);
      wrote.add(path);
    }

    const ps = readRange("#sc-progress-step", DEFAULTS.trakt.progress_step, 1, 25);
    if (ps != null) write("scrobble.trakt.progress_step", ps);
  }

  function bindPercentInput(sel, path, dflt) {
    const n = $(sel, STATE.mount);
    if (!n) return;
    const set = (val, commitEmpty = false) => {
      const raw = String(val ?? n.value ?? "").trim();
      if (raw === "") {
        if (commitEmpty) {
          const v = clamp100(dflt);
          write(path, v);
          n.value = v;
        }
        return;
      }
      const v = norm100(raw, dflt);
      write(path, v);
      n.value = v;
    };
    on(n, "input", () => set(n.value, false));
    on(n, "change", () => set(n.value, true));
    on(n, "blur", () => set(n.value, true));
  }

  function bindRangeInput(sel, path, dflt, min, max) {
    const n = $(sel, STATE.mount);
    if (!n) return;
    const set = (val, commitEmpty = false) => {
      const raw = String(val ?? n.value ?? "").trim();
      if (raw === "") {
        if (commitEmpty) {
          const v = clampRange(dflt, min, max);
          write(path, v);
          n.value = v;
        }
        return;
      }
      const v = normRange(raw, dflt, min, max);
      write(path, v);
      n.value = v;
    };
    on(n, "input", () => set(n.value, false));
    on(n, "change", () => set(n.value, true));
    on(n, "blur", () => set(n.value, true));
  }


  function namesFromChips(hostId) {
    const host = $(hostId, STATE.mount);
    if (!host) return [];
    return $all(".chip > span:first-child", host)
      .map((s) => String(s.textContent || "").trim())
      .filter(Boolean);
  }

  const USER_PICK = { mode: "watch", anchor: null, users: [], all: [], prov: "plex" };

  function ensureUserPickerPop() {
    if (d.getElementById("sc_user_pop")) return;
    const pop = el("div", { id: "sc_user_pop", className: "sc-user-pop hidden" });

    const head = el("div", { className: "head" });
    const title = el("div", { className: "title", id: "sc_user_title", textContent: "Pick user" });
    const closeBtn = el("button", { type: "button", id: "sc_user_close", className: "btn small", textContent: "Close" });
    head.append(title, closeBtn);

    const body = el("div", { className: "body" });
    const filter = el("input", { id: "sc_user_filter", className: "input", placeholder: "Filter users..." });
    const list = el("div", { id: "sc_user_list", className: "list" });
    body.append(filter, list);

    pop.append(head, body);
    d.body.appendChild(pop);

    on(closeBtn, "click", (e) => {
      e.preventDefault();
      closeUserPicker();
    });
    on(filter, "input", () => renderUserPickerList());

    if (!STATE.__scUserAwayBound) {
      STATE.__scUserAwayBound = true;
      d.addEventListener("click", (e) => {
        const p = d.getElementById("sc_user_pop");
        if (!p || p.classList.contains("hidden")) return;
        if (p.contains(e.target)) return;
        const a = USER_PICK.anchor;
        if (a && (a === e.target || a.contains(e.target))) return;
        closeUserPicker();
      });
      d.addEventListener("keydown", (e) => {
        if (e.key === "Escape") closeUserPicker();
      });
    }
    if (!STATE.__scUserPosBound) {
      STATE.__scUserPosBound = true;
      let raf = null;
      const safe = () => {
        const p = d.getElementById("sc_user_pop");
        if (!p || p.classList.contains("hidden")) return;
        if (raf) return;
        raf = requestAnimationFrame(() => {
          raf = null;
          try {
            placeUserPickerPop();
          } catch {}
        });
      };
      w.addEventListener("resize", safe, { passive: true });
      w.addEventListener("scroll", safe, { passive: true, capture: true });
      d.addEventListener("scroll", safe, { passive: true, capture: true });
    }
  }

  function placeUserPickerPop() {
    const pop = d.getElementById("sc_user_pop");
    const anchor = USER_PICK.anchor;
    if (!pop || !anchor) return;
    const r = anchor.getBoundingClientRect();
    const wPop = pop.offsetWidth || 360;
    const left = Math.max(12, Math.min(w.innerWidth - wPop - 12, r.left));
    const hPop = pop.offsetHeight || 320;
    const preferAbove = r.top - hPop - 8;
    const minTop = 12;
    const below = r.bottom + 8;
    let top = preferAbove >= minTop ? preferAbove : below;
    top = Math.max(minTop, Math.min(w.innerHeight - hPop - 12, top));
    pop.style.left = left + "px";
    pop.style.top = top + "px";
  }

  function closeUserPicker() {
    const pop = d.getElementById("sc_user_pop");
    if (pop) pop.classList.add("hidden");
  }

  function userNameFromObj(u) {
    return String(u?.username || u?.title || u?.Name || u?.name || u?.user?.Name || "").trim();
  }

  function userIdFromObj(u) {
    return String(u?.id || u?.Id || u?.user_id || u?.user?.Id || "").trim();
  }

  function userTagFromObj(u, prov) {
    if (prov === "plex") return String(u?.type || "").trim();
    const isAdmin =
      u?.IsAdministrator === true ||
      u?.Policy?.IsAdministrator === true ||
      u?.is_admin === true ||
      u?.admin === true;
    const tags = [];
    if (isAdmin) tags.push("admin");
    if (u?.IsHidden === true) tags.push("hidden");
    if (u?.IsDisabled === true) tags.push("disabled");
    return tags.join(" ");
  }

  function renderUserPickerList() {
    const listEl = d.getElementById("sc_user_list");
    const q = String(d.getElementById("sc_user_filter")?.value || "")
      .toLowerCase()
      .trim();
    if (!listEl) return;

    listEl.innerHTML = "";
    const items = (USER_PICK.users || []).filter((u) => !q || String(u.name || "").toLowerCase().includes(q));
    if (!items.length) {
      listEl.appendChild(el("div", { className: "sub", textContent: "No users found." }));
      return;
    }

    for (const u of items) {
      const btn = el("button", { type: "button", className: "userrow" });
      const row = el("div", { className: "row1" });
      const name = el("strong", { textContent: u.name || "" });
      row.appendChild(name);
      if (u.tag) row.appendChild(el("span", { className: "tag", textContent: u.tag }));
      btn.appendChild(row);
      on(btn, "click", (e) => {
        e.preventDefault();
        applyPickedUser(u);
      });
      listEl.appendChild(btn);
    }
  }

  function addToWhitelist(hostSel, path, name, removeFn, onClick) {
    const clean = String(name || "").trim();
    if (!clean) return false;
    const cur = asArray(read(path, []));
    if (cur.includes(clean)) return false;
    const next = [...cur, clean];
    write(path, next);
    const host = $(hostSel, STATE.mount);
    if (host) host.append(chip(clean, removeFn, onClick));
    return true;
  }

  function applyPickedUser(u) {
    const prov = provider();
    const name = String(u?.name || "").trim();
    const uid = String(u?.id || "").trim();

    if (USER_PICK.mode === "webhook") {
      const added = addToWhitelist("#sc-whitelist-webhook", "scrobble.webhook.filters_plex.username_whitelist", name, removeUserWebhook);
      setNote("sc-users-note-webhook", added ? `Picked ${name}` : `${name} already added`);
      closeUserPicker();
      return;
    }

    const added = addToWhitelist(
      "#sc-whitelist",
      "scrobble.watch.filters.username_whitelist",
      name,
      removeUserWatch,
      prov === "emby" || prov === "jellyfin" ? onSelectWatchUser : undefined
    );

    if ((prov === "emby" || prov === "jellyfin") && uid) {
      const inp = $("#sc-server-uuid", STATE.mount);
      if (inp) inp.value = uid;
      write("scrobble.watch.filters.server_uuid", uid);
      write("scrobble.watch.filters.user_id", uid);
      setNote("sc-uuid-note", "User ID set");
    }

    setNote("sc-users-note", added ? `Picked ${name}` : `${name} already added`);
    closeUserPicker();
  }


  async function fetchUsersForPicker(mode) {
    if (mode === "webhook") {
      const x = await j(`/api/plex/users?instance=${encodeURIComponent("default")}`);
      const a = Array.isArray(x) ? x : Array.isArray(x?.users) ? x.users : [];
      return Array.isArray(a) ? a : [];
    }
    return API.users(activeProviderInstance());
  }

  async function openUserPicker(mode, anchorEl) {
    USER_PICK.mode = mode === "webhook" ? "webhook" : "watch";
    USER_PICK.anchor = anchorEl || null;
    USER_PICK.prov = USER_PICK.mode === "webhook" ? "plex" : provider();

    if (USER_PICK.mode === "watch" && (USER_PICK.prov === "emby" || USER_PICK.prov === "jellyfin") && window.cwMediaUserPicker && typeof window.cwMediaUserPicker.open === "function") {
      window.cwMediaUserPicker.open({
        provider: USER_PICK.prov,
        instance: activeProviderInstance(),
        anchorEl: USER_PICK.anchor,
        title: USER_PICK.prov === "emby" ? "Pick Emby user" : "Pick Jellyfin user",
        onPick: (u) => applyPickedUser({ name: u?.name, id: u?.id }),
      });
      return;
    }

    ensureUserPickerPop();

    const pop = d.getElementById("sc_user_pop");
    const title = d.getElementById("sc_user_title");
    const filter = d.getElementById("sc_user_filter");
    const listEl = d.getElementById("sc_user_list");
    if (!pop || !title || !filter || !listEl) return;

    const provLabel = USER_PICK.prov === "plex" ? "Plex" : USER_PICK.prov === "emby" ? "Emby" : "Jellyfin";
    title.textContent = USER_PICK.mode === "webhook" ? "Pick Plex user" : `Pick ${provLabel} user`;
    filter.value = "";
    listEl.innerHTML = "";
    listEl.appendChild(el("div", { className: "sub", textContent: "Loading…" }));

    pop.classList.remove("hidden");
    try {
      placeUserPickerPop();
    } catch {}

    let list = [];
    try {
      list = await fetchUsersForPicker(USER_PICK.mode);
    } catch (e) {
      console.warn("[scrobbler] users fetch failed:", e);
      listEl.innerHTML = "";
      listEl.appendChild(el("div", { className: "sub", textContent: "Couldn’t load users. Check Authentication + logs." }));
      return;
    }

    const prov = USER_PICK.prov;
    const all = Array.isArray(list) ? list : [];

    const prio = (x) => {
      if (prov === "plex") {
        const t = String(x?.type || "").toLowerCase().trim();
        if (t === "owner" || x?.owned === true) return 0;
        if (t === "managed" || x?.isHomeUser === true) return 1;
        return 2;
      }
      const isAdmin =
        x?.IsAdministrator === true ||
        x?.Policy?.IsAdministrator === true ||
        x?.is_admin === true ||
        x?.admin === true;
      return isAdmin ? 0 : 1;
    };

    const mapped = all
      .map((raw) => ({
        raw,
        name: userNameFromObj(raw),
        id: userIdFromObj(raw),
        tag: userTagFromObj(raw, prov),
        prio: prio(raw),
        hidden: raw?.IsHidden === true ? 1 : 0,
        disabled: raw?.IsDisabled === true ? 1 : 0,
      }))
      .filter((x) => x.name)
      .sort(
        (a, b) =>
          a.prio - b.prio ||
          a.disabled - b.disabled ||
          a.hidden - b.hidden ||
          a.name.localeCompare(b.name, undefined, { sensitivity: "base" })
      );

    USER_PICK.all = all;
    USER_PICK.users = mapped;
    STATE.users = USER_PICK.all;

    renderUserPickerList();
    try {
      placeUserPickerPop();
      filter.focus();
    } catch {}
  }


  function onSelectWatchUser(name) {
  const prov = provider();
  if (prov !== "emby" && prov !== "jellyfin") return;
  const list = Array.isArray(STATE.users) ? STATE.users : [];
  const hit = list.find((u) => String(u?.username || u?.Name || u?.name || "").toLowerCase() === String(name || "").toLowerCase());
  const id = hit?.id || hit?.Id;
  if (id) {
    const inp = $("#sc-server-uuid", STATE.mount);
    if (inp) inp.value = id;
    write("scrobble.watch.filters.server_uuid", id);
    write("scrobble.watch.filters.user_id", id);
    setNote("sc-uuid-note", "User ID set from username");
  } else {
    setNote("sc-uuid-note", "User not found", "err");
  }
}


  function normalizeRatingsSelection(v) {
    const val = String(v || "none").toLowerCase().trim();
    if (val === "both") return "trakt,simkl";
    if (val === "none") return "none";
    return normSinkCsvOrDefault(val, "");
  }

  function getPlexRatingsSelectionFromCfg() {
    const parts = [];
    if (!!read("scrobble.watch.plex_trakt_ratings", false)) parts.push("trakt");
    if (!!read("scrobble.watch.plex_simkl_ratings", false)) parts.push("simkl");
    if (!!read("scrobble.watch.plex_mdblist_ratings", false)) parts.push("mdblist");
    return parts.length ? normSinkCsv(parts.join(",")) : "none";
  }

  function setPlexRatingsFlagsFromSelection(v) {
    const sel = normalizeRatingsSelection(v);
    const on = sel !== "none";
    write("scrobble.watch.plex_trakt_ratings", on && sel.includes("trakt"));
    write("scrobble.watch.plex_simkl_ratings", on && sel.includes("simkl"));
    write("scrobble.watch.plex_mdblist_ratings", on && sel.includes("mdblist"));
  }

  function rebuildPlexRatingsDropdown() {
    const wrap = $("#sc-plex-ratings-wrap", STATE.mount);
    const sel = $("#sc-plex-ratings", STATE.mount);
    if (!wrap || !sel) return;

    const prov = provider();
    wrap.style.display = prov === "plex" ? "" : "none";
    if (prov !== "plex") return;

    const options = [
      ["none", "None"],
      ["trakt", "Trakt"],
      ["simkl", "SIMKL"],
      ["mdblist", "MDBList"],
      ["trakt,simkl", "Trakt & SIMKL"],
      ["trakt,mdblist", "Trakt & MDBList"],
      ["simkl,mdblist", "SIMKL & MDBList"],
      ["trakt,simkl,mdblist", "Trakt & SIMKL & MDBList"],
    ];

    const cfgSel = getPlexRatingsSelectionFromCfg();
    const rawSel = normalizeRatingsSelection(sel.value || "");
    const cur = sel.dataset.cxUserChanged === "1" ? (rawSel || cfgSel) : cfgSel;

    sel.innerHTML = options.map(([v, label]) => `<option value="${v}">${label}</option>`).join("");
    const allowed = new Set(options.map((o) => o[0]));
    const next = allowed.has(cur) ? cur : "none";

    sel.value = next;
    setPlexRatingsFlagsFromSelection(next);
    try { syncPlexRatingsPillsFromSelect(); } catch {}

    try {
      updatePlexWatcherWebhookUrl();
    } catch {}
  }

  function updatePlexWatcherWebhookUrl() {
    const sel = $("#sc-plex-ratings", STATE.mount);
    const wrap = $("#sc-plexwatcher-url-wrap", STATE.mount);
    const code = $("#sc-plexwatcher-url", STATE.mount);
    if (!sel || !wrap || !code) return;

    const v = String(sel.value || "none").toLowerCase();
    const on = v !== "none";
    wrap.style.display = on ? "flex" : "none";

    try {
      const btn = $("#sc-copy-plexwatcher", STATE.mount);
      wrap.style.width = "100%";
      wrap.style.maxWidth = "100%";
      wrap.style.minWidth = "0";
      wrap.style.flex = "1 1 100%";
      wrap.style.alignItems = "center";
      wrap.style.gap = "8px";
      wrap.style.flexWrap = "nowrap";
      wrap.style.overflow = "hidden";
      code.style.flex = "1 1 0";
      code.style.width = "0";
      code.style.minWidth = "0";
      code.style.maxWidth = "100%";
      code.style.display = "block";
      code.style.boxSizing = "border-box";
      code.style.overflow = "hidden";
      code.style.textOverflow = "ellipsis";
      code.style.whiteSpace = "nowrap";

      if (btn) {
        btn.style.flex = "0 0 auto";
        btn.style.whiteSpace = "nowrap";
      }
    } catch {}

    if (on) code.textContent = `${location.origin}/webhook/plexwatcher`;
    else setNote("sc-plexwatcher-note", "");
  }

  async function refreshCfgBeforePopulate() {
    try {
      const fresh = await API.cfgGet();
      if (fresh && typeof fresh === "object") {
        const backendProv = String(fresh?.scrobble?.watch?.provider || "").toLowerCase().trim();
        const backendSinkRaw = fresh?.scrobble?.watch?.sink;
        const backendSink = normSinkCsv(backendSinkRaw == null ? "trakt" : backendSinkRaw);

        STATE.cfg = fresh;

        // Provider instances (profiles) can change outside this view; drop instance options cache.
        try { delete STATE._routesCache; } catch {}

        const uiProv = String(STATE.ui?.watchProvider || "").toLowerCase().trim();
        const uiSinkRaw = STATE.ui?.watchSink;
        const uiEnabled = STATE.ui?.scrobbleEnabled;
        const uiModeRaw = String(STATE.ui?.scrobbleMode || "").toLowerCase().trim();
        const uiAutostart = STATE.ui?.watchAutostart;

        const backendEnabled = !!fresh?.scrobble?.enabled;
        const backendMode = String(fresh?.scrobble?.mode || "webhook").toLowerCase().trim();
        const backendAutostart = !!fresh?.scrobble?.watch?.autostart;

        if (uiProv) {
          if (backendProv === uiProv) STATE.ui.watchProvider = null;
          else deepSet(STATE.cfg, "scrobble.watch.provider", uiProv);
        }
        if (uiSinkRaw != null) {
          const uiSink = normSinkCsv(uiSinkRaw);
          if (backendSink === uiSink) STATE.ui.watchSink = null;
          else deepSet(STATE.cfg, "scrobble.watch.sink", uiSink);
        }
        if (typeof uiEnabled === "boolean") {
          if (backendEnabled === uiEnabled) STATE.ui.scrobbleEnabled = null;
          else deepSet(STATE.cfg, "scrobble.enabled", uiEnabled);
        }
        if (uiModeRaw) {
          if (backendMode === uiModeRaw) STATE.ui.scrobbleMode = null;
          else deepSet(STATE.cfg, "scrobble.mode", uiModeRaw);
        }
        if (typeof uiAutostart === "boolean") {
          if (backendAutostart === uiAutostart) STATE.ui.watchAutostart = null;
          else deepSet(STATE.cfg, "scrobble.watch.autostart", uiAutostart);
        }

        try {
          w._cfgCache = STATE.cfg;
        } catch {}
      }
    } catch {}
  }

  function populate() {
  const mig = legacyToRoutesIfMissing();
  try {
    if (isRoutesMode()) {
      const ar = getActiveRoute() || getRoutes()[0] || null;
      if (ar) applyRouteView(ar);
      renderRoutesUi().catch(() => {});
      renderMigrateBanner(!!mig?.migrated);
    }
  } catch {}
  const enabled = !!read("scrobble.enabled", false);
  const mode = String(read("scrobble.mode", "webhook")).toLowerCase();
  const useWebhook = enabled && mode === "webhook";
  const useWatch = enabled && mode === "watch";
  const prov = provider();

  const whEl = $("#sc-enable-webhook", STATE.mount);
  const waEl = $("#sc-enable-watcher", STATE.mount);
  const pvSel = $("#sc-provider", STATE.mount);
  const skSel = $("#sc-sink", STATE.mount);

    if (whEl) whEl.checked = useWebhook;
  ["#sc-enable-webhook-jf", "#sc-enable-webhook-emby", "#sc-enable-webhook-adv"].forEach((id) => { const n = $(id, STATE.mount); if (n) n.checked = useWebhook; });
  if (waEl) waEl.checked = useWatch;
  if (pvSel) pvSel.value = prov;
  try { syncProviderPickerUi(); } catch {}
  if (skSel) {
    const raw = read("scrobble.watch.sink", "trakt");
    skSel.value = normSinkCsv(raw == null ? "trakt" : raw);
  }
  try { syncSinkPillsFromSelect(); } catch {}

  let wlWatch = asArray(read("scrobble.watch.filters.username_whitelist", []));
  

  const hostW = $("#sc-whitelist", STATE.mount);
  if (hostW) {
    hostW.innerHTML = "";
    wlWatch.forEach((u) => hostW.append(chip(u, removeUserWatch, (prov === "emby" || prov === "jellyfin") ? onSelectWatchUser : undefined)));
  }

  const suWatch = read("scrobble.watch.filters.server_uuid", "");
  const suInpW = $("#sc-server-uuid", STATE.mount);
  if (suInpW) suInpW.value = suWatch || "";

  const wlWeb = asArray(read("scrobble.webhook.filters_plex.username_whitelist", []));
  const hostWB = $("#sc-whitelist-webhook", STATE.mount);
  if (hostWB) {
    hostWB.innerHTML = "";
    wlWeb.forEach((u) => hostWB.append(chip(u, removeUserWebhook)));
  }

  const suWeb = read("scrobble.webhook.filters_plex.server_uuid", "");
  const suInpWB = $("#sc-server-uuid-webhook", STATE.mount);
  if (suInpWB) suInpWB.value = suWeb || "";

  const base = location.origin;
  const plexCode = $("#sc-webhook-url-plex", STATE.mount);
  const jfCode = $("#sc-webhook-url-jf", STATE.mount);
  const embyCode = $("#sc-webhook-url-emby", STATE.mount);
  if (plexCode) plexCode.textContent = `${base}/webhook/plextrakt`;
  if (jfCode) jfCode.textContent = `${base}/webhook/jellyfintrakt`;
  if (embyCode) embyCode.textContent = `${base}/webhook/embytrakt`;

  const autostart = !!read("scrobble.watch.autostart", false);
  const auto = $("#sc-autostart", STATE.mount);
  if (auto) auto.checked = !!autostart;

  try { syncServerPreviewUi(); } catch {}

  const set = (id, v) => {
    const n = $(id, STATE.mount);
    if (n) n.value = norm100(v, v);
  };

  const setRange = (id, v, dflt, min, max) => {
    const n = $(id, STATE.mount);
    if (n) n.value = normRange(v, dflt, min, max);
  };

  set("#sc-pause-debounce", read("scrobble.watch.pause_debounce_seconds", DEFAULTS.watch.pause_debounce_seconds));
  set("#sc-suppress-start", read("scrobble.watch.suppress_start_at", DEFAULTS.watch.suppress_start_at));
  set("#sc-pause-debounce-webhook", read("scrobble.webhook.pause_debounce_seconds", DEFAULTS.watch.pause_debounce_seconds));
  set("#sc-suppress-start-webhook", read("scrobble.webhook.suppress_start_at", DEFAULTS.watch.suppress_start_at));
  set("#sc-stop-pause", read("scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold));
  set("#sc-force-stop", read("scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at));
  set("#sc-regress", read("scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent));
  set("#sc-stop-pause-webhook", read("scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold));
  set("#sc-force-stop-webhook", read("scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at));
  set("#sc-regress-webhook", read("scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent));
  setRange("#sc-progress-step", read("scrobble.trakt.progress_step", DEFAULTS.trakt.progress_step), DEFAULTS.trakt.progress_step, 1, 25);

  const delEnabled = !!read("scrobble.delete_plex", false);
  const delWh = $("#sc-delete-plex-webhook", STATE.mount);
    if (delWh) delWh.checked = delEnabled;
  ["#sc-delete-plex-webhook-jf", "#sc-delete-plex-webhook-emby"].forEach((id) => { const n = $(id, STATE.mount); if (n) n.checked = delEnabled; });
  const delW = $("#sc-delete-plex-watch", STATE.mount);
  if (delW) delW.checked = delEnabled;

  const whRat = $("#sc-webhook-plex-ratings", STATE.mount);
  if (whRat) whRat.checked = !!read("scrobble.webhook.plex_trakt_ratings", false);

  rebuildPlexRatingsDropdown();

  updatePlexWatcherWebhookUrl();

  restoreDetailsState("#sc-filters", false, "sc-filters-open-v2");
  restoreDetailsState("#sc-advanced", false, "sc-advanced-open-v2");

  restoreDetailsState("#sc-options-webhook", false, "sc-wh-options-open-v1");
  restoreDetailsState("#sc-plex-specifics-webhook", true, "sc-wh-plexspec-open-v1");
  restoreDetailsState("#sc-plex-options-webhook", false, "sc-wh-plexopts-open-v1");
  restoreDetailsState("#sc-advanced-webhook", false, "sc-wh-advanced-open-v1");

  syncHiddenServerInputs();
  applyModeDisable();
}


  async function refreshWatcher() {
    try {
      setWatcherStatus((await API.watch.status()) || {});
    } catch {
      setWatcherStatus({ alive: false });
    }
  }

  async function onWatchStart() {
  const routesMode = isRoutesMode();
  if (routesMode) {
    const routes = getRoutes();
    if (!routes.length) return setNote("sc-note", "Add at least one route before starting the watcher.", "warn");
    if (!anyStartableRoute()) return setNote("sc-note", "No startable routes. Check provider/sink authentication (and Plex server URL for that profile).", "warn");
    const pick = routes.find(r => r.enabled && providerAuthOkForRoute(r) && sinkAuthOkForRoute(r)) || routes[0];
    setActiveRouteId(pick.id);
    applyRouteView(pick);
    await renderRouteSelector();
  }

  const prov = String($("#sc-provider", STATE.mount)?.value || provider() || "plex")
    .toLowerCase()
    .trim();

  const sinkRawUi = $("#sc-sink", STATE.mount)?.value;
  const sinkRawCfg = read("scrobble.watch.sink", "trakt");
  const sink = normSinkCsv(sinkRawUi != null ? sinkRawUi : (sinkRawCfg == null ? "trakt" : sinkRawCfg));

  write("scrobble.watch.provider", prov);
  write("scrobble.watch.sink", sink);

  if (!sink) {
    write("scrobble.watch.autostart", false);
    STATE.ui.watchAutostart = false;
    const auto = $("#sc-autostart", STATE.mount);
    if (auto) {
      auto.checked = false;
      auto.disabled = true;
    }
    setNote("sc-note", "You must select at least one sink to start the watcher.", "warn");
    try { await API.watch.stop(); } catch {}
    try { await persistConfigPaths([["scrobble.watch.sink", ""], ["scrobble.watch.autostart", false]], "sc-pms-note"); } catch {}
    applyModeDisable();
    refreshWatcher();
    return;
  }

  setNote("sc-note", "");

  const srvProv = prov === "plex" ? "plex.server_url" : prov === "emby" ? "emby.server" : "jellyfin.server";
  const srv = String(read(srvProv, "") || "");
  const plexTokenOk = !!String(read("plex.account_token", "") || "").trim();
  const embyTokenOk = !!String(read("emby.access_token", "") || "").trim();
  const jellyTokenOk = !!String(read("jellyfin.access_token", "") || "").trim();

  if (prov === "plex") {
    if (!plexTokenOk) return setNote("sc-pms-note", "Not connected to Plex. Go to Authentication → Plex.", "err");
    if (!isValidServerUrl(srv)) return setNote("sc-pms-note", "Plex Server is required (http(s)://…)", "err");
  } else if (prov === "emby") {
    if (!embyTokenOk) return setNote("sc-pms-note", "Not connected to Emby. Go to Authentication → Emby.", "err");
  } else {
    if (!jellyTokenOk) return setNote("sc-pms-note", "Not connected to Jellyfin. Go to Authentication → Jellyfin.", "err");
  }

  try {
    const nextScrobble = getScrobbleConfig();
    const rootPatch = getRootPatch();

    const serverCfg = await API.cfgGet();
    const cfg = typeof structuredClone === "function" ? structuredClone(serverCfg || {}) : JSON.parse(JSON.stringify(serverCfg || {}));

    cfg.scrobble = nextScrobble;
    cfg.plex = Object.assign({}, cfg.plex || {}, rootPatch.plex || {});
    cfg.emby = Object.assign({}, cfg.emby || {}, rootPatch.emby || {});
    cfg.jellyfin = Object.assign({}, cfg.jellyfin || {}, rootPatch.jellyfin || {});

    const r = await fetch("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      cache: "no-store",
      body: JSON.stringify(cfg),
    });
    if (!r.ok) throw new Error(`POST /api/config ${r.status}`);

    w._cfgCache = cfg;
    STATE.cfg = cfg;
    try {
      syncHiddenServerInputs();
    } catch {}
  } catch (e) {
    console.warn("[scrobbler] pre-start save failed:", e);
    return setNote("sc-pms-note", "Couldn’t save settings. Hit Save or check logs.", "err");
  }

  try {
    await API.watch.start(routesMode ? null : prov, routesMode ? null : sink);
  } catch {
    setNote("sc-pms-note", "Start failed", "err");
  }

  refreshWatcher();
}


  async function onWatchStop() {
    try {
      await API.watch.stop();
    } catch {
      setNote("sc-pms-note", "Stop failed", "err");
    }
    refreshWatcher();
  }

  async function fetchServerUUID() {
  try {
    const prov = provider();
    const x = await API.serverUUID(activeProviderInstance());
    const v = x?.server_uuid || x?.uuid || x?.id || "";
    const inp = $("#sc-server-uuid", STATE.mount);
    if (inp && v) {
      inp.value = v;
      write("scrobble.watch.filters.server_uuid", v);
      if (prov === "emby" || prov === "jellyfin") write("scrobble.watch.filters.user_id", v);
      setNote("sc-uuid-note", prov === "plex" ? "Server UUID fetched" : "User ID fetched");
    } else setNote("sc-uuid-note", prov === "plex" ? "No server UUID" : "No user ID", "err");
  } catch {
    setNote("sc-uuid-note", "Fetch failed", "err");
  }
}


  function onAddUserWatch() {
    const inp = $("#sc-user-input", STATE.mount);
    const v = String((inp?.value || "").trim());
    if (!v) return;
    const cur = asArray(read("scrobble.watch.filters.username_whitelist", []));
    if (!cur.includes(v)) {
      const next = [...cur, v];
      write("scrobble.watch.filters.username_whitelist", next);
      $("#sc-whitelist", STATE.mount).append(chip(v, removeUserWatch, (provider() === "emby" || provider() === "jellyfin") ? onSelectWatchUser : undefined));
      inp.value = "";
    }
  }

  function removeUserWatch(u) {
    const cur = asArray(read("scrobble.watch.filters.username_whitelist", []));
    const next = cur.filter((x) => String(x) !== String(u));
    write("scrobble.watch.filters.username_whitelist", next);
    const host = $("#sc-whitelist", STATE.mount);
    host.innerHTML = "";
    next.forEach((v) => host.append(chip(v, removeUserWatch, (provider() === "emby" || provider() === "jellyfin") ? onSelectWatchUser : undefined)));
  }

  async function loadUsers() {
    try {
      const list = await fetchUsersForPicker(USER_PICK.mode);
      const filtered = list.filter(
        (u) =>
          ["managed", "owner"].includes(String(u?.type || "").toLowerCase()) ||
          u?.owned === true ||
          u?.isHomeUser === true ||
          u?.IsAdministrator === true ||
          u?.IsHidden === false ||
          u?.IsDisabled === false
      );
      STATE.users = Array.isArray(list) ? list : [];
      const names = filtered.map((u) => u?.username || u?.title || u?.Name || u?.name).filter(Boolean);
      const host = $("#sc-whitelist", STATE.mount);
      let added = 0;
      for (const n of names) {
        const cur = asArray(read("scrobble.watch.filters.username_whitelist", []));
        if (!cur.includes(n)) {
          write("scrobble.watch.filters.username_whitelist", [...cur, n]);
          host.append(chip(n, removeUserWatch, (provider() === "emby" || provider() === "jellyfin") ? onSelectWatchUser : undefined));
          added++;
        }
      }
      setNote("sc-users-note", added ? `Loaded ${added} user(s)` : "No eligible users");
    } catch {
      setNote("sc-users-note", "Load users failed", "err");
    }
  }

  async function fetchServerUUIDWebhook() {
    try {
      const x = await j("/api/plex/server_uuid");
      const v = x?.server_uuid || x?.uuid || x?.id || "";
      const inp = $("#sc-server-uuid-webhook", STATE.mount);
      if (inp && v) {
        inp.value = v;
        write("scrobble.webhook.filters_plex.server_uuid", v);
        setNote("sc-uuid-note-webhook", "Server UUID fetched");
      } else setNote("sc-uuid-note-webhook", "No server UUID", "err");
    } catch {
      setNote("sc-uuid-note-webhook", "Fetch failed", "err");
    }
  }

  function onAddUserWebhook() {
    const inp = $("#sc-user-input-webhook", STATE.mount);
    const v = String((inp?.value || "").trim());
    if (!v) return;
    const cur = asArray(read("scrobble.webhook.filters_plex.username_whitelist", []));
    if (!cur.includes(v)) {
      const next = [...cur, v];
      write("scrobble.webhook.filters_plex.username_whitelist", next);
      $("#sc-whitelist-webhook", STATE.mount).append(chip(v, removeUserWebhook));
      inp.value = "";
    }
  }

  function removeUserWebhook(u) {
    const cur = asArray(read("scrobble.webhook.filters_plex.username_whitelist", []));
    const next = cur.filter((x) => String(x) !== String(u));
    write("scrobble.webhook.filters_plex.username_whitelist", next);
    const host = $("#sc-whitelist-webhook", STATE.mount);
    host.innerHTML = "";
    next.forEach((v) => host.append(chip(v, removeUserWebhook)));
  }

  async function loadUsersWebhook() {
    try {
      const x = await j("/api/plex/users");
      const list = Array.isArray(x) ? x : Array.isArray(x?.users) ? x.users : [];
      const filtered = list.filter(
        (u) => ["managed", "owner"].includes(String(u?.type || "").toLowerCase()) || u?.owned === true || u?.isHomeUser === true
      );
      const names = filtered.map((u) => u?.username || u?.title).filter(Boolean);
      const host = $("#sc-whitelist-webhook", STATE.mount);
      let added = 0;
      for (const n of names) {
        const cur = asArray(read("scrobble.webhook.filters_plex.username_whitelist", []));
        if (!cur.includes(n)) {
          write("scrobble.webhook.filters_plex.username_whitelist", [...cur, n]);
          host.append(chip(n, removeUserWebhook));
          added++;
        }
      }
      setNote("sc-users-note-webhook", added ? `Loaded ${added} user(s)` : "No eligible managed/owner users");
    } catch {
      setNote("sc-users-note-webhook", "Load users failed", "err");
    }
  }

  async function hydrateEmby() {
    try {
      const info = await j("/api/emby/inspect");
      const server = String(info?.server || "").trim();
      const username = String(info?.username || info?.user?.Name || "").trim();
      const uid = String(info?.user_id || info?.user?.Id || "").trim();
      if (server) {
        write("emby.server", server);
        const inp = $("#sc-pms-input", STATE.mount);
        if (inp) inp.value = server;
      }
      if (uid) {
        const inp = $("#sc-server-uuid", STATE.mount);
        if (inp) inp.value = uid;
        write("scrobble.watch.filters.server_uuid", uid);
        write("scrobble.watch.filters.user_id", uid);
        setNote("sc-uuid-note", "User ID detected");
      }
    } catch {}
  }
async function hydrateJellyfin() {
  try {
    const info = await j("/api/jellyfin/inspect");
    const server = String(info?.server || "").trim();
    const username = String(info?.username || info?.user?.Name || "").trim();
    const uid = String(info?.user_id || info?.user?.Id || "").trim();
    if (server) {
      write("jellyfin.server", server);
      const inp = $("#sc-pms-input", STATE.mount);
      if (inp && provider() === "jellyfin") inp.value = server;
    }
    if (uid) {
      const inp = $("#sc-server-uuid", STATE.mount);
      if (inp) inp.value = uid;
      write("scrobble.watch.filters.server_uuid", uid);
      write("scrobble.watch.filters.user_id", uid);
      setNote("sc-uuid-note", "User ID detected");
    }
  } catch {}
}


  function wire() {
    ensureHiddenServerInputs();
    setNote("sc-webhook-warning", "These legacy webhooks scrobble only to Trakt and will be removed in a future release; they’re no longer maintained or supported, please switch to Watcher ASAP.", "warn");

    on($("#sc-copy-plex", STATE.mount), "click", async () => {
      const ok = await copyText(`${location.origin}/webhook/plextrakt`);
      setNote("sc-endpoint-note", ok ? "Plex endpoint copied" : "Copy failed", ok ? "" : "err");
    });
    on($("#sc-copy-jf", STATE.mount), "click", async () => {
      const ok = await copyText(`${location.origin}/webhook/jellyfintrakt`);
      setNote("sc-endpoint-note", ok ? "Jellyfin endpoint copied" : "Copy failed", ok ? "" : "err");
    });
    on($("#sc-copy-emby", STATE.mount), "click", async () => {
      const ok = await copyText(`${location.origin}/webhook/embytrakt`);
      setNote("sc-endpoint-note", ok ? "Emby endpoint copied" : "Copy failed", ok ? "" : "err");
    });

    on($("#sc-copy-plexwatcher", STATE.mount), "click", async () => {
      const url = `${location.origin}/webhook/plexwatcher`;
      const ok = await copyText(url);
      setNote("sc-plexwatcher-note", ok ? "Watcher endpoint copied" : "Copy failed", ok ? "" : "err");
    });

    on($("#sc-add-user", STATE.mount), "click", onAddUserWatch);
    on($("#sc-load-users", STATE.mount), "click", (e) => {
      e.preventDefault();
      openUserPicker("watch", e.currentTarget);
    });
    on($("#sc-watch-start", STATE.mount), "click", onWatchStart);
    on($("#sc-watch-stop", STATE.mount), "click", onWatchStop);
    on($("#sc-watch-refresh", STATE.mount), "click", () => {
      refreshWatcher();
      try {
        w.refreshWatchLogs?.();
      } catch {}
    });

    // Routes UI
    on($("#sc-route-add", STATE.mount), "click", async (e) => {
      e.preventDefault();
      if (!isRoutesMode()) return;
      const routes = getRoutes().map((r, i) => normalizeRoute(r, `R${i + 1}`));
      const id = nextRouteId();
      const nr = normalizeRoute({ id, enabled: true, provider: "", provider_instance: "default", sink: "", sink_instance: "default", filters: {} }, id);
      routes.push(nr);
      setRoutes(routes);
      setActiveRouteId(id);
      await renderRoutesUi();
    });

    const rSel = $("#sc-route-select", STATE.mount);
    on(rSel, "change", async (e) => {
      if (!isRoutesMode()) return;
      syncActiveRouteFromView();
      const rid = String(e.target.value || "").trim();
      if (!rid) return;
      setActiveRouteId(rid);
      const r = getActiveRoute();
      if (r) applyRouteView(r);
      populate();
      await renderRoutesUi();
    });

    const rHost = $("#sc-routes", STATE.mount);
    on(rHost, "change", async (e) => {
      if (!isRoutesMode()) return;
      const t = e.target;
      const rid = String(t?.dataset?.rid || "").trim();
      const f = String(t?.dataset?.f || "").trim();
      if (!rid || !f) return;
      const routes = getRoutes().map((r, i) => normalizeRoute(r, `R${i + 1}`));
      const r = routes.find(x => x.id === rid);
      if (!r) return;
      if (f === "enabled") r.enabled = !!t.checked;
      else r[f] = String(t.value || "").trim() || (f.endsWith("_instance") ? "default" : r[f]);
      if (f === "provider") { r.provider_instance = "default"; r.filters ||= {}; }
      if (f === "sink") { r.sink_instance = "default"; }
      setRoutes(routes);
      try {
        const dd = findDuplicateRouteKeys(getRoutes());
        if (!dd.length) clearStickyNote("sc-note");
      } catch {}

      if (activeRouteId() !== rid) { try { syncActiveRouteFromView(); } catch {} }
      setActiveRouteId(rid);
      applyRouteView(r);
      try { syncRouteActiveRowUi(rid); } catch {}
      try { syncServerPreviewUi(); } catch {}
      try { applyModeDisable(); } catch {}
      await renderRoutesUi();
    });

    on(rHost, "click", async (e) => {
      if (!isRoutesMode()) return;
      const btn = e.target?.closest?.("button[data-act]");
      if (!btn) {
        const tr = e.target?.closest?.("tr.sc-route-row");
        const rid = String(tr?.dataset?.rid || "").trim();
        if (rid) setActiveRouteFromUi(rid);
        return;
      }
      e.preventDefault();
      const act = String(btn.dataset.act || "");
      const rid = String(btn.dataset.rid || "");
      if (!act || !rid) return;
      if (act === "remove") {
        const routes = getRoutes().filter(r => String(r?.id || "") !== rid);
        setRoutes(routes);
        try {
          const dd = findDuplicateRouteKeys(getRoutes());
          if (!dd.length) clearStickyNote("sc-note");
        } catch {}

        if (activeRouteId() === rid) setActiveRouteId(routes[0]?.id || "");
        await renderRoutesUi();
        populate();
        return;
      }
      if (act === "filters") {
        syncActiveRouteFromView();
        setActiveRouteId(rid);
        const r = getRoutes().find(x => x.id === rid) || null;
        if (r) applyRouteView(r);
        await renderRouteSelector();
        try { STATE._watcherSelectTab?.("filters"); } catch {}
        populate();
      }
    });
    on($("#sc-fetch-uuid", STATE.mount), "click", () => {
      fetchServerUUID();
    });
    on($("#sc-server-uuid", STATE.mount), "input", (e) => {
      const v = String(e.target.value || "").trim();
      write("scrobble.watch.filters.server_uuid", v);
      if (provider() === "emby" || provider() === "jellyfin") write("scrobble.watch.filters.user_id", v);
    });

    on($("#sc-add-user-webhook", STATE.mount), "click", onAddUserWebhook);
    on($("#sc-load-users-webhook", STATE.mount), "click", (e) => {
      e.preventDefault();
      openUserPicker("webhook", e.currentTarget);
    });
    on($("#sc-fetch-uuid-webhook", STATE.mount), "click", fetchServerUUIDWebhook);
    on($("#sc-server-uuid-webhook", STATE.mount), "input", (e) => write("scrobble.webhook.filters_plex.server_uuid", String(e.target.value || "").trim()));

    bindPercentInput("#sc-pause-debounce", "scrobble.watch.pause_debounce_seconds", DEFAULTS.watch.pause_debounce_seconds);
    bindPercentInput("#sc-suppress-start", "scrobble.watch.suppress_start_at", DEFAULTS.watch.suppress_start_at);
    bindPercentInput("#sc-pause-debounce-webhook", "scrobble.webhook.pause_debounce_seconds", DEFAULTS.watch.pause_debounce_seconds);
    bindPercentInput("#sc-suppress-start-webhook", "scrobble.webhook.suppress_start_at", DEFAULTS.watch.suppress_start_at);

    bindPercentInput("#sc-stop-pause", "scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold);
    bindPercentInput("#sc-force-stop", "scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at);
    bindPercentInput("#sc-regress", "scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent);

    bindPercentInput("#sc-stop-pause-webhook", "scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold);
    bindPercentInput("#sc-force-stop-webhook", "scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at);
    bindPercentInput("#sc-regress-webhook", "scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent);
    bindRangeInput("#sc-progress-step", "scrobble.trakt.progress_step", DEFAULTS.trakt.progress_step, 1, 25);

    const wh = $("#sc-enable-webhook", STATE.mount);
    const wa = $("#sc-enable-watcher", STATE.mount);
    const pv = $("#sc-provider", STATE.mount);

    const mirrorToggle = (masterSel, clones) => {
      const master = $(masterSel, STATE.mount);
      if (!master) return;
      let mute = false;
      const sync = () => {
        if (mute) return;
        mute = true;
        clones.forEach((sel) => {
          const c = $(sel, STATE.mount);
          if (c) c.checked = !!master.checked;
        });
        mute = false;
      };
      clones.forEach((sel) => {
        const c = $(sel, STATE.mount);
        if (!c) return;
        on(c, "change", () => {
          if (mute) return;
          mute = true;
          master.checked = !!c.checked;
          master.dispatchEvent(new Event("change", { bubbles: true }));
          mute = false;
        });
      });
      on(master, "change", sync);
      sync();
    };

    mirrorToggle("#sc-enable-webhook", ["#sc-enable-webhook-jf", "#sc-enable-webhook-emby", "#sc-enable-webhook-adv"]);
    mirrorToggle("#sc-delete-plex-webhook", ["#sc-delete-plex-webhook-jf", "#sc-delete-plex-webhook-emby"]);

    const pvBtn = $("#sc-provider-btn", STATE.mount);
    const pvMenu = $("#sc-provider-menu", STATE.mount);

    on(pvBtn, "click", (e) => {
      e.preventDefault();
      toggleProviderMenu();
    });

    if (pvMenu) {
      $all(".sc-prov-item[data-value]", pvMenu).forEach((it) => {
        on(it, "click", (e) => {
          e.preventDefault();
          const v = String(it.getAttribute("data-value") || "").toLowerCase().trim();
          const sel = $("#sc-provider", STATE.mount);
          if (sel && v) {
            sel.value = v;
            sel.dispatchEvent(new Event("change", { bubbles: true }));
          }
          closeProviderMenu();
        });
      });
    }

    if (!STATE.__scProvAwayBound) {
      STATE.__scProvAwayBound = true;
      d.addEventListener("click", (e) => {
        const menu = $("#sc-provider-menu", STATE.mount);
        const btn = $("#sc-provider-btn", STATE.mount);
        if (!menu || menu.classList.contains("hidden")) return;
        if (menu.contains(e.target)) return;
        if (btn && (btn === e.target || btn.contains(e.target))) return;
        closeProviderMenu();
      });
      d.addEventListener("keydown", (e) => {
        if (e.key === "Escape") closeProviderMenu();
      });
    }
    const sk = $("#sc-sink", STATE.mount);

    const skPills = $("#sc-sink-pills", STATE.mount);
    if (sk && skPills) {
      ensureSinkPillBar(skPills);
      syncPillBar(skPills, normSinkCsv(sk.value));
      on(skPills, "click", (e) => {
        const btn = e.target && e.target.closest ? e.target.closest("button[data-sink]") : null;
        if (!btn || btn.disabled) return;
        const key = String(btn.getAttribute("data-sink") || "").toLowerCase().trim();
        if (!key) return;
        const curArr = normSinkCsv(sk.value || "").split(",").filter(Boolean);
        const has = curArr.includes(key);
        const nextArr = has ? curArr.filter((x) => x !== key) : [...curArr, key];
        const next = normSinkCsv(nextArr.join(","));
        if (sk.value !== next) {
          sk.value = next;
          sk.dispatchEvent(new Event("change", { bubbles: true }));
        } else {
          syncPillBar(skPills, next);
        }
      });
    }

    const ratSel = $("#sc-plex-ratings", STATE.mount);
    const ratPills = $("#sc-plex-ratings-pills", STATE.mount);
    if (ratSel && ratPills) {
      ensureSinkPillBar(ratPills);
      syncPillBar(ratPills, csvFromSelect(ratSel, true));
      on(ratPills, "click", (e) => {
        const btn = e.target && e.target.closest ? e.target.closest("button[data-sink]") : null;
        if (!btn || btn.disabled) return;
        const key = String(btn.getAttribute("data-sink") || "").toLowerCase().trim();
        if (!key) return;
        const curArr = csvFromSelect(ratSel, true).split(",").filter(Boolean);
        const has = curArr.includes(key);
        const nextArr = has ? curArr.filter((x) => x !== key) : [...curArr, key];
        const nextCsv = normSinkCsv(nextArr.join(","));
        const nextSel = nextCsv ? nextCsv : "none";
        if (ratSel.value !== nextSel) {
          ratSel.value = nextSel;
          ratSel.dispatchEvent(new Event("change", { bubbles: true }));
        } else {
          syncPillBar(ratPills, nextCsv);
        }
      });
    }

    const syncExclusive = async (src) => {
      const webOn = !!wh?.checked;
      const watOn = !!wa?.checked;
      if (src === "webhook" && webOn && wa) wa.checked = false;
      if (src === "watch" && watOn && wh) wh.checked = false;

      write("scrobble.enabled", (!!wh?.checked) || (!!wa?.checked));
      write("scrobble.mode", (!!wa?.checked) ? "watch" : "webhook");

      if (src === "watch" && !wa.checked) {
        write("scrobble.watch.autostart", false);
        const auto = $("#sc-autostart", STATE.mount);
        if (auto) auto.checked = false;
      }
      applyModeDisable();

      const enabled = (!!wh?.checked) || (!!wa?.checked);
      const mode = (!!wa?.checked) ? "watch" : "webhook";
      const pairs = [
        ["scrobble.enabled", enabled],
        ["scrobble.mode", mode],
      ];
      if (src === "watch" && !wa.checked) pairs.push(["scrobble.watch.autostart", false]);
      const noteId = mode === "watch" ? "sc-pms-note" : "sc-endpoint-note";
      STATE.ui.scrobbleEnabled = enabled;
      STATE.ui.scrobbleMode = mode;
      if (src === "watch" && !wa.checked) STATE.ui.watchAutostart = false;
      await persistConfigPaths(pairs, noteId);
    };

    if (wh) on(wh, "change", () => syncExclusive("webhook"));
    if (wa) on(wa, "change", () => syncExclusive("watch"));

    on($("#sc-autostart", STATE.mount), "change", (e) => {
      const v = !!e.target.checked;
      write("scrobble.watch.autostart", v);
      STATE.ui.watchAutostart = v;
      persistConfigPaths([["scrobble.watch.autostart", v]], "sc-pms-note");
    });

    on(pv, "change", async (e) => {
      const prev = provider();
      const val = String(e.target.value || "plex").toLowerCase();
      try {
        await API.watch.stop();
      } catch {}
      try {
        refreshWatcher();
      } catch {}
      try {
        if (!isRoutesMode()) saveCurrentProviderFilters(prev);
      } catch {}
      STATE.ui.watchProvider = val;
      write("scrobble.watch.provider", val);
      persistConfigPaths([["scrobble.watch.provider", val]], "sc-pms-note");
      try {
        if (!isRoutesMode()) applyProviderFilters(val);
      } catch {}
      try {
        closeUserPicker();
      } catch {}
      populate();
      try { syncProviderPickerUi(); } catch {}
      if (val === "emby") await hydrateEmby();
      if (val === "jellyfin") await hydrateJellyfin();
      applyModeDisable();
    });

    on(sk, "change", async (e) => {
      const val = normSinkCsv(e.target?.value ?? "");
      try { await refreshWatcher(); } catch {}
      STATE.ui.watchSink = val;
      write("scrobble.watch.sink", val);

      const pairs = [["scrobble.watch.sink", val]];
      if (!val) {
        write("scrobble.watch.autostart", false);
        STATE.ui.watchAutostart = false;
        pairs.push(["scrobble.watch.autostart", false]);
        const auto = $("#sc-autostart", STATE.mount);
        if (auto) auto.checked = false;
        setNote("sc-note", "You must select at least one sink to start the watcher.", "warn");
        try { await API.watch.stop(); } catch {}
        try { await refreshWatcher(); } catch {}
      } else {
        setNote("sc-note", "");
      }

      await persistConfigPaths(pairs, "sc-pms-note");
      try { syncSinkPillsFromSelect(); } catch {}
      rebuildPlexRatingsDropdown();
      applyModeDisable();
    });

    const ratingsSel = $("#sc-plex-ratings", STATE.mount);
    if (ratingsSel) {
      on(ratingsSel, "change", (e) => {
        ratingsSel.dataset.cxUserChanged = "1";
        setPlexRatingsFlagsFromSelection(e.target.value);
        try { syncPlexRatingsPillsFromSelect(); } catch {}
        try {
          updatePlexWatcherWebhookUrl();
        } catch {}
      });
    }

    const whRatings = $("#sc-webhook-plex-ratings", STATE.mount);
    if (whRatings) {
      on(whRatings, "change", (e) => {
        const v = !!e.target.checked;
        write("scrobble.webhook.plex_trakt_ratings", v);
        persistConfigPaths([["scrobble.webhook.plex_trakt_ratings", v]], "sc-endpoint-note");
      });
    }

    on($("#sc-delete-plex-webhook", STATE.mount), "change", (e) => {
      const v = !!e.target.checked;
      write("scrobble.delete_plex", v);
      const other = $("#sc-delete-plex-watch", STATE.mount);
      if (other) other.checked = v;
      persistConfigPaths([["scrobble.delete_plex", v]], "sc-endpoint-note");
    });

    on($("#sc-delete-plex-watch", STATE.mount), "change", (e) => {
      const v = !!e.target.checked;
      write("scrobble.delete_plex", v);
      const other = $("#sc-delete-plex-webhook", STATE.mount);
      if (other) other.checked = v;
      persistConfigPaths([["scrobble.delete_plex", v]], "sc-pms-note");
    });
  }

  function getScrobbleConfig() {
  commitAdvancedInputsWatch();
  commitAdvancedInputsWebhook();
  commitAdvancedInputsTrakt();

  const enabled = !!read("scrobble.enabled", false);
  const mode = String(read("scrobble.mode", "webhook")).toLowerCase();
  const prov = provider();

  const wlWebHost = $("#sc-whitelist-webhook", STATE.mount);
  const wlWeb = wlWebHost ? namesFromChips("#sc-whitelist-webhook") : asArray(read("scrobble.webhook.filters_plex.username_whitelist", []));
  const suWeb = String($("#sc-server-uuid-webhook", STATE.mount)?.value ?? read("scrobble.webhook.filters_plex.server_uuid", "")).trim();

  const wlWatchHost = $("#sc-whitelist", STATE.mount);
  const wlWatch = wlWatchHost ? namesFromChips("#sc-whitelist") : asArray(read("scrobble.watch.filters.username_whitelist", []));
  const suWatch = String($("#sc-server-uuid", STATE.mount)?.value ?? read("scrobble.watch.filters.server_uuid", "")).trim();
  const userIdWatch = String(read("scrobble.watch.filters.user_id", "") || "").trim();

  const filtersWatch = {
    username_whitelist: wlWatch,
  };

  if (prov === "plex") {
    filtersWatch.server_uuid = suWatch || "";
  } else {
    const uid = suWatch || userIdWatch || "";
    if (uid) filtersWatch.user_id = uid;
  }


    const routesMode = isRoutesMode();
    let routesOut = undefined;

    if (routesMode) {
      syncActiveRouteFromView(filtersWatch);
      const routesRaw = getRoutes().map((r, i) => normalizeRoute(r, `R${i + 1}`));
      const routesKeep = routesRaw.filter((r) => {
        const p = String(r?.provider || "").trim();
        const s = String(r?.sink || "").trim();
        return !!(p && s);
      });
      const dropped = routesRaw.length - routesKeep.length;
      if (dropped > 0) {
        const msg = `Ignored ${dropped} incomplete route${dropped === 1 ? "" : "s"} (pick Provider + Sink).`;
        if (!(STICKY_NOTES["sc-note"] && STICKY_NOTES["sc-note"].kind === "err")) setNote("sc-note", msg, "warn");
      }

      routesOut = routesKeep;

const dups = findDuplicateRouteKeys(routesOut);
if (dups.length) {
  const msg = "Duplicate routes are not allowed. Fix these before saving: " + dups.map(d => d.key).join(" · ");
  setStickyNote("sc-note", msg, "err");
  throw new Error(msg);
}
      setRoutes(routesRaw);
    }

  return {
    enabled,
    mode: mode === "watch" ? "watch" : "webhook",
    delete_plex: !!read("scrobble.delete_plex", false),
    delete_plex_types: read("scrobble.delete_plex_types", ["movie"]),

    webhook: {
      pause_debounce_seconds: read("scrobble.webhook.pause_debounce_seconds", DEFAULTS.watch.pause_debounce_seconds),
      suppress_start_at: read("scrobble.webhook.suppress_start_at", DEFAULTS.watch.suppress_start_at),
      plex_trakt_ratings: !!read("scrobble.webhook.plex_trakt_ratings", false),
      filters_plex: {
        username_whitelist: wlWeb,
        server_uuid: suWeb || "",
      },
      filters_jellyfin: read("scrobble.webhook.filters_jellyfin", {}) || { username_whitelist: [] },
    },

    watch: {
      provider: prov,
      sink: normSinkCsv(read("scrobble.watch.sink", "trakt")),
      routes: routesOut,
      autostart: !!read("scrobble.watch.autostart", false),
      plex_simkl_ratings: !!read("scrobble.watch.plex_simkl_ratings", false),
      plex_trakt_ratings: !!read("scrobble.watch.plex_trakt_ratings", false),
      plex_mdblist_ratings: !!read("scrobble.watch.plex_mdblist_ratings", false),
      pause_debounce_seconds: read("scrobble.watch.pause_debounce_seconds", DEFAULTS.watch.pause_debounce_seconds),
      suppress_start_at: read("scrobble.watch.suppress_start_at", DEFAULTS.watch.suppress_start_at),
      filters: filtersWatch,
    },

    trakt: {
      progress_step: read("scrobble.trakt.progress_step", DEFAULTS.trakt.progress_step),
      stop_pause_threshold: read("scrobble.trakt.stop_pause_threshold", DEFAULTS.trakt.stop_pause_threshold),
      force_stop_at: read("scrobble.trakt.force_stop_at", DEFAULTS.trakt.force_stop_at),
      regress_tolerance_percent: read("scrobble.trakt.regress_tolerance_percent", DEFAULTS.trakt.regress_tolerance_percent),
    },
  };
}


  const getRootPatch = () => ({
  plex: { server_url: String(read("plex.server_url", "") || "") },
  emby: { server: String(read("emby.server", "") || "") },
  jellyfin: { server: String(read("jellyfin.server", "") || "") },
});
async function init(opts = {}) {
    STATE.mount = opts.mountId ? d.getElementById(opts.mountId) : d;
    STATE.cfg = opts.cfg || w._cfgCache || {};
    STATE.webhookHost = $("#scrob-webhook", STATE.mount);
    STATE.watcherHost = $("#scrob-watcher", STATE.mount);

    if (!STATE.webhookHost || !STATE.watcherHost) {
      const root = STATE.mount || d.body;
      const makeSec = (id, title) => {
        const sec = el("div", { className: "section", id });
        sec.innerHTML = `<div class="head"><strong>${title}</strong></div><div class="body"><div id="${id === "sc-sec-webhook" ? "scrob-webhook" : "scrob-watcher"}"></div></div>`;
        root.append(sec);
      };
      if (!STATE.webhookHost) {
        makeSec("sc-sec-webhook", "Webhook");
        STATE.webhookHost = $("#scrob-webhook", STATE.mount);
      }
      if (!STATE.watcherHost) {
        makeSec("sc-sec-watch", "Watcher");
        STATE.watcherHost = $("#scrob-watcher", STATE.mount);
      }
    }

    buildUI();
    wire();
    if (!STATE.__authChangedBound) {
      STATE.__authChangedBound = true;
      let t = null;
      let busy = false;

      const run = async () => {
        if (busy) return;
        busy = true;
        try {
          if (!d.hidden) {
            try { await refreshCfgBeforePopulate(); } catch {}
            try { if (isRoutesMode()) await renderRoutesUi(); } catch {}
            try { await refreshWatcher(); } catch {}
            try { applyModeDisable(); } catch {}
          }
        } finally {
          busy = false;
        }
      };

      window.addEventListener("auth-changed", () => {
        if (t) return;
        t = setTimeout(() => {
          t = null;
          run();
        }, 400);
      });
    }

    await refreshCfgBeforePopulate();
    try { legacyToRoutesIfMissing(); } catch {}
    try {
      if (!isRoutesMode()) {
        pfLoadStore();
        const prov = provider();
        if (STATE.pf.store?.[prov]) applyProviderFilters(prov);
        else saveCurrentProviderFilters(prov);
      }
    } catch {}
    populate();
    await refreshWatcher();
    if (provider() === "emby") await hydrateEmby();
    if (provider() === "jellyfin") await hydrateJellyfin();
  }

  function mountLegacy(targetEl, cfg) {
    return init({ mountId: targetEl?.id, cfg: cfg || (w._cfgCache || {}) });
  }

  w.ScrobUI = { $, $all, el, on, setNote, injectStyles, DEFAULTS, STATE, read, write, asArray, clamp100, norm100, API };
  w.Scrobbler = { init, mount: mountLegacy, getConfig: getScrobbleConfig, getRootPatch };
  w.getScrobbleConfig = getScrobbleConfig;
  w.getRootPatch = getRootPatch;

  d.addEventListener("DOMContentLoaded", () => {
    const root = d.getElementById("scrobble-mount");
    if (!root) return;
    init({ mountId: "scrobble-mount" });
  });
})(window, document);
