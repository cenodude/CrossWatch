/* assets/settings-insight.js */
(function (w, d) {
  "use strict";

  if (w.__CW_SETTINGS_INSIGHT_STARTED__) return;
  w.__CW_SETTINGS_INSIGHT_STARTED__ = 1;

  // Helpers
  const $  = (sel, root) => (root || d).querySelector(sel);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  async function fetchJSON(url){ try{ const r=await fetch(url,{cache:"no-store"}); if(!r.ok) return null; return await r.json(); }catch{ return null; } }
  function toLocal(v){
    if (v===undefined||v===null||v==="") return "—";
    if (typeof v==="number"){ const ms = v<10_000_000_000 ? v*1000 : v; const dt=new Date(ms); return isNaN(+dt)?"—":dt.toLocaleString(undefined,{hour12:false}); }
    const dt=new Date(v); return isNaN(+dt)?"—":dt.toLocaleString(undefined,{hour12:false});
  }
  const coalesceNextRun = o => o ? (o.next_run_at ?? o.next_run ?? o.next ?? null) : null;

  function prettyWatchProvider(v){
    const k = String(v || "").toLowerCase().trim();
    if(!k) return "";
    if(k === "plex") return "Plex";
    if(k === "emby") return "Emby";
    if(k === "jellyfin") return "Jellyfin";
    return k.toUpperCase();
  }


  const _normInst = (v) => {
    const s = String(v || "").trim();
    return (!s || s.toLowerCase() === "default") ? "default" : s;
  };
  const _instLabel = (v) => {
    const s = _normInst(v);
    return (s === "default") ? "Default" : s;
  };
  const _has = (v) => (typeof v === "string") ? (v.trim().length > 0) : !!v;

  function _profileConfigured(provider, blk, cfg) {
    const p = String(provider || "").toLowerCase().trim();
    const b = (blk && typeof blk === "object") ? blk : {};
    if (p === "plex") return _has(b.account_token) || _has(b.token) || _has(b.access_token);
    if (p === "emby") return _has(b.access_token) || _has(b.api_key) || _has(b.token);
    if (p === "jellyfin") return _has(b.access_token) || _has(b.api_key) || _has(b.token);
    if (p === "trakt") return _has(b.access_token) || _has(b.refresh_token);
    if (p === "simkl") return _has(b.access_token) || _has(b.refresh_token);
    if (p === "anilist") return _has(b.access_token) || _has(b.token);
    if (p === "mdblist") return _has(b.api_key);
    const t = (p === "tautulli") ? b : (cfg?.tautulli || cfg?.auth?.tautulli || {});
    if (p === "tautulli") return _has(t.server_url || t.server);
    if (p === "tmdb") {
      const tm = b || {};
      return _has(tm.api_key) && (_has(tm.session_id) || _has(tm.session));
    }
    return _has(b.access_token) || _has(b.account_token) || _has(b.api_key) || _has(b.token);
  }

  function _providerBlock(cfg, provider, instanceId) {
    const p = String(provider || "").toLowerCase().trim();
    const base = (cfg && cfg[p] && typeof cfg[p] === "object") ? cfg[p] : {};
    const inst = _normInst(instanceId);
    if (inst === "default") return base;
    const insts = base.instances;
    if (insts && typeof insts === "object" && !Array.isArray(insts)) {
      const blk = insts[inst];
      if (blk && typeof blk === "object") return blk;
    }
    return {};
  }

  function _countConfiguredProfiles(cfg, provider) {
    const p = String(provider || "").toLowerCase().trim();
    const base = _providerBlock(cfg, p, "default");
    let n = _profileConfigured(p, base, cfg) ? 1 : 0;

    const insts = (cfg?.[p] || {})?.instances;
    if (insts && typeof insts === "object" && !Array.isArray(insts)) {
      Object.keys(insts).forEach((id) => {
        const blk = insts[id];
        if (blk && typeof blk === "object" && _profileConfigured(p, blk, cfg)) n += 1;
      });
    }
    return n;
  }

  function _configuredKeysFromCfg(cfg) {
    const out = new Set();
    const map = [
      ["plex","PLEX"], ["emby","EMBY"], ["jellyfin","JELLYFIN"],
      ["trakt","TRAKT"], ["simkl","SIMKL"], ["anilist","ANILIST"],
      ["mdblist","MDBLIST"], ["tmdb","TMDB"], ["tautulli","TAUTULLI"],
    ];
    map.forEach(([p, k]) => { if (_countConfiguredProfiles(cfg, p) > 0) out.add(k); });

    // TMDB legacy location (tmdb_sync)
    const tm = cfg?.tmdb_sync || cfg?.auth?.tmdb_sync || null;
    if (!out.has("TMDB") && tm && typeof tm === "object" && _profileConfigured("tmdb", tm, cfg)) out.add("TMDB");

    out.add("crosswatch");
    return out;
  }

  function _authProfileEntries(cfg) {
    const order = [
      { key: "PLEX", prov: "plex", label: "Plex" },
      { key: "EMBY", prov: "emby", label: "Emby" },
      { key: "JELLYFIN", prov: "jellyfin", label: "Jellyfin" },
      { key: "TRAKT", prov: "trakt", label: "Trakt" },
      { key: "SIMKL", prov: "simkl", label: "SIMKL" },
      { key: "MDBLIST", prov: "mdblist", label: "MDBList" },
      { key: "ANILIST", prov: "anilist", label: "AniList" },
      { key: "TMDB", prov: "tmdb", label: "TMDB" },
      { key: "TAUTULLI", prov: "tautulli", label: "Tautulli" },
    ];
    const entries = [];
    let total = 0;
    order.forEach((it) => {
      let c = 0;
      if (it.prov === "tmdb") {
        c = _countConfiguredProfiles(cfg, "tmdb");
        if (c === 0) {
          const tm = cfg?.tmdb_sync || cfg?.auth?.tmdb_sync || null;
          if (tm && typeof tm === "object" && _profileConfigured("tmdb", tm, cfg)) c = 1;
        }
      } else if (it.prov === "tautulli") {
        const t = cfg?.tautulli || cfg?.auth?.tautulli || null;
        c = (t && typeof t === "object" && _profileConfigured("tautulli", t, cfg)) ? 1 : 0;
      } else {
        c = _countConfiguredProfiles(cfg, it.prov);
      }
      if (c > 0) {
        total += c;
        entries.push({ key: it.key, label: it.label, count: c });
      }
    });
    return { entries, total };
  }

  function authProfilesHTML(auth) {
    const arr = Array.isArray(auth?.profiles) ? auth.profiles : [];
    if (!arr.length) return "No profiles configured";
    const chips = arr.map((p) => {
      const key = String(p.key || "").toUpperCase();
      const label = String(p.label || key);
      const n = Number(p.count || 0) || 0;
      const src = `/assets/img/${key}-log.svg`;
      const title = `${label}: ${n} configured ${n === 1 ? "profile" : "profiles"}`;
      return `<span class="si-pchip" title="${title}"><img loading="lazy" decoding="async" src="${src}" alt="${label}"><span class="n">${n}</span></span>`;
    }).join("");
    return `<div class="si-pchips">${chips}</div>`;
  }


  // Styles
  const css = `
  #cw-settings-grid{
    width:100%; margin-top:12px; display:grid;
    grid-template-columns:minmax(560px,1fr) 380px;
    gap:28px; align-items:start;
  }
  #cw-settings-grid > *{ margin-top:0 !important; }
  @media (max-width:1280px){ #cw-settings-grid{ display:block; } }

  #cw-settings-insight{ position:sticky; top:12px; }

  .si-card{
    position: relative;
    border-radius:18px;
    background: rgba(13,15,22,0.96);
    border: 1px solid rgba(120,128,160,0.14);
    box-shadow:
      0 0 0 1px rgba(160,140,255,0.06),
      0 0 22px rgba(160,140,255,0.10),
      0 10px 24px rgba(0,0,0,0.45),
      inset 0 1px 0 rgba(255,255,255,0.02);
    overflow:hidden;
    isolation:isolate;
  }
  .si-card::before {
    content: "";
    position: absolute;
    inset: -2px;
    background: url("/assets/img/background.svg") no-repeat 50% 96% / cover;
    opacity: 0.14;
    mix-blend-mode: screen;
    pointer-events: none;
    z-index: 0;
  }

  .si-header{
    padding:14px 16px;
    background: linear-gradient(180deg, rgba(16,18,26,0.98), rgba(13,15,22,0.96));
    border-bottom: 1px solid rgba(120,128,160,0.14);
    position: relative; z-index: 2;
  }
  .si-title{ font-size:16px; font-weight:800; letter-spacing:.2px; color:#E6EAFF; }

  #cw-si-scroll{ overflow:auto; overscroll-behavior:contain; position: relative; z-index: 2; }
  .si-body{ padding:6px 10px; }

  .si-row{ display:flex; align-items:center; gap:12px; padding:10px 6px; }
  .si-row + .si-row{ border-top:1px solid rgba(120,128,160,0.10); }

  .si-ic {
    display: flex;
    align-items: center;
    justify-content: center;
    flex: 0 0 auto;
    margin-right: 4px;
    background:none!important;
    border:none!important;
  }
  .si-ic .material-symbols-rounded {
    font-size: 30px;
    color: #d0d4e8;
    opacity: 0.95;
  }

  .si-col { display: flex; flex-direction: column; }
  .si-h { color: #E6EAFD; font-weight: 700; line-height: 1.2; }
  .si-one { color: #C3CAE3; font-size: 13px; margin-top: 2px; }

  /* Whitelisting block */
  .si-wl { display:flex; flex-direction:column; gap:8px; margin-top:2px; }
  .si-wl-level { display:flex; flex-direction:column; gap:6px; }
  .si-wl-level-title{
    font-size:12px; font-weight:700; letter-spacing:.2px;
    color:#D6DBF0; opacity:.9; margin-left:2px;
  }
  .si-wl-list{ display:flex; flex-direction:column; gap:4px; padding-left:2px; }
  .si-wl-item{
    display:flex; align-items:center; justify-content:space-between; gap:8px;
    padding:2px 0;
  }
  .si-wl-name{
    color:#E6EAFD; font-weight:600; font-size:13px; white-space:nowrap;
  }
  .si-wl-chips{ display:flex; flex-wrap:wrap; gap:6px; justify-content:flex-end; }
  .si-chip{
    display:inline-flex; align-items:center; gap:4px;
    font-size:11px; font-weight:700; letter-spacing:.2px;
    color:#C9CFF0;
    padding:2px 6px;
    border-radius:6px;
    background: rgba(140,120,255,0.08);
    border: 1px solid rgba(140,120,255,0.22);
    line-height:1.2;
  }

  /* Auth profile chips */
  .si-pchips{ display:flex; flex-wrap:wrap; gap:6px; align-items:center; margin-top:2px; }
  .si-pchip{
    display:inline-flex; align-items:center; gap:6px;
    font-size:12px; font-weight:800; letter-spacing:.2px;
    color:#E6EAFD;
    padding:2px 6px;
    border-radius:8px;
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(120,128,160,0.16);
    line-height:1.2;
  }
  .si-pchip img{ height:14px; width:auto; opacity:.95; }
  .si-pchip .n{ font-size:12px; font-weight:900; color:#E6EAFD; line-height:1; }

  /* Wizard  */
  .si-empty {
    display: flex;
    align-items: flex-start;
    gap: 16px;
    padding: 26px 22px 28px 22px;
    text-align: left;
  }
  .si-empty .hero-ic { flex: 0 0 auto; display: flex; align-items: center; justify-content: center; }
  .si-empty .hero-ic .material-symbols-rounded { font-size: 42px; color: #e6eaff; opacity: 0.95; }
  .si-empty .hero-text { display: flex; flex-direction: column; gap: 6px; }
  .si-empty .h1 { color:#E8ECFF; font-size:18px; font-weight:800; margin:0; }
  .si-empty .p  { color:#C3CAE3; font-size:14px; line-height:1.45; margin:0; }
  .si-empty .tip{
    margin-top:10px; font-size:13px; color:#CFCFF5;
    background: rgba(140,120,255,0.08);
    border: 1px solid rgba(140,120,255,0.18);
    border-radius: 8px; padding: 6px 10px;
  }
  `;

  function ensureStyle(){ if($("#cw-settings-insight-style")) return; const s=d.createElement("style"); s.id="cw-settings-insight-style"; s.textContent=css; d.head.appendChild(s); }

  function ensureGrid(){
    const page=$("#page-settings"); if(!page) return null;
    let left=page.querySelector("#cw-settings-left, .settings-wrap, .settings, .accordion, .content, .page-inner");
    if(!left) left=page.firstElementChild||page;

    let grid=page.querySelector("#cw-settings-grid");
    if(!grid){
      grid=d.createElement("div"); grid.id="cw-settings-grid";
      left.parentNode.insertBefore(grid, left); grid.appendChild(left);
      left.style.marginTop="0"; if(!left.style.paddingTop) left.style.paddingTop="0";
      left.id=left.id||"cw-settings-left";
    }

    let aside=$("#cw-settings-insight", grid);
    if(!aside){ aside=d.createElement("aside"); aside.id="cw-settings-insight"; grid.appendChild(aside); }
    return { grid, left, right:aside };
  }

  function ensureCard(){
    const nodes=ensureGrid(); if(!nodes) return null;
    const { right }=nodes;
    if(!$(".si-card", right)){
      right.innerHTML=`
        <div class="si-card">
          <div class="si-header"><div class="si-title">Settings Insight</div></div>
          <div id="cw-si-scroll"><div class="si-body" id="cw-si-body"></div></div>
        </div>`;
    }
    return nodes;
  }

  // Data fetching
  async function readConfig() {
    const cfg = await fetchJSON("/api/config?t=" + Date.now());
    return cfg || {};
  }

  function _providerKeyFromImg(img) {
    if (!img) return "";
    const src = String(img.getAttribute?.("src") || "");
    const m = src.match(/\/([A-Za-z0-9]+)-log\.svg(?:\?|#|$)/i);
    if (m && m[1]) return String(m[1]).toUpperCase();
    return String(img.getAttribute?.("alt") || "").trim().toUpperCase();
  }

  function _findIconStrip(head) {
    if (!head) return null;
    const spans = Array.from(head.querySelectorAll(":scope > span") || []);
    for (let i = spans.length - 1; i >= 0; i--) {
      const sp = spans[i];
      if (sp && sp.querySelector && sp.querySelector('img[src*="-log.svg"]')) return sp;
    }
    const any = head.querySelector('img[src*="-log.svg"]');
    return any ? any.closest("span") : null;
  }

  function _ensureStripIcon(strip, key) {
    if (!strip || !key) return null;
    const k = String(key).toUpperCase();
    const existing = strip.querySelector(`img[data-cw-key="${k}"], img[src*="/assets/img/${k}-log.svg"]`);
    if (existing) return existing;

    const img = d.createElement("img");
    img.src = `/assets/img/${k}-log.svg`;
    img.alt = k;
    img.dataset.cwKey = k;
    img.style.width = "auto";
    img.style.opacity = ".9";
    img.style.height = (k === "EMBY") ? "24px" : "18px";
    strip.appendChild(img);
    return img;
  }

  function _applyIconStrip(strip, keysOrdered) {
    if (!strip) return;

    const order = Array.isArray(keysOrdered) ? keysOrdered : [];
    const want = new Set(order.map((k) => String(k).toUpperCase()));
    const imgs = Array.from(strip.querySelectorAll('img[src*="-log.svg"]') || []);

    const byKey = new Map();
    imgs.forEach((img) => {
      const k = (img.dataset.cwKey || _providerKeyFromImg(img) || "").toUpperCase();
      if (!k) return;
      img.dataset.cwKey = k;
      byKey.set(k, img);
    });

    // Ensure icons exist for wanted keys (supports sinks like SIMKL not hard-coded in HTML).
    order.forEach((k) => {
      const key = String(k).toUpperCase();
      if (!byKey.has(key)) {
        const img = _ensureStripIcon(strip, key);
        if (img) byKey.set(key, img);
      }
    });

    // Show/hide
    Array.from(byKey.entries()).forEach(([k, img]) => {
      img.style.display = want.has(k) ? "" : "none";
    });

    // Re-order (wanted keys first, in order)
    order.forEach((k) => {
      const key = String(k).toUpperCase();
      const img = byKey.get(key);
      if (img) strip.appendChild(img);
    });

    if (!strip.dataset.cwDisp) strip.dataset.cwDisp = strip.style.display || "flex";
    const anyVisible = Array.from(strip.querySelectorAll('img[src*="-log.svg"]') || []).some((i) => i.style.display !== "none");
    strip.style.display = anyVisible ? strip.dataset.cwDisp : "none";
  }

  function _parseSinkNames(v) {
    const raw = String(v || "").trim();
    if (!raw) return [];
    const parts = raw.split(/[,&+]/g).map((s) => s.trim().toLowerCase()).filter(Boolean);
    return Array.from(new Set(parts));
  }

  
  function _scrobblerHeaderKeys(cfg, configured) {
    const keys = [];
    const sc = (cfg?.scrobble || {}) || {};
    if (!sc.enabled) return keys;

    const mode = String(sc.mode || "webhook").toLowerCase();
    const provMap = { plex: "PLEX", emby: "EMBY", jellyfin: "JELLYFIN" };
    const sinkMap = { trakt: "TRAKT", simkl: "SIMKL", mdblist: "MDBLIST" };

    if (mode === "watch") {
      const w = (sc.watch || {}) || {};
      const routes = Array.isArray(w.routes) ? w.routes : [];

      const provs = [];
      const sinks = [];
      routes.forEach((r) => {
        const p = String(r?.provider || "").trim().toLowerCase();
        const s = String(r?.sink || "").trim().toLowerCase();
        if (p && !provs.includes(p)) provs.push(p);
        if (s && !sinks.includes(s)) sinks.push(s);
      });

      if (!provs.length) {
        const prov = String(w?.provider || "plex").toLowerCase().trim();
        if (prov) provs.push(prov);
      }
      if (!sinks.length) sinks.push(..._parseSinkNames(w?.sink || "trakt"));

      provs.forEach((p) => {
        const k = provMap[p] || p.toUpperCase();
        if (configured.has(k)) keys.push(k);
      });

      ["trakt", "simkl", "mdblist"].forEach((name) => {
        if (!sinks.includes(name)) return;
        const skey = sinkMap[name];
        if (skey && configured.has(skey)) keys.push(skey);
      });

      return keys;
    }

    ["PLEX", "JELLYFIN", "EMBY"].forEach((k) => { if (configured.has(k)) keys.push(k); });
    if (configured.has("TRAKT")) keys.push("TRAKT");
    return keys;
  }


  function _updateSettingsHeaderIcons(cfg) {
    const page = d.getElementById("page-settings");
    if (!page) return;

    const configured = _configuredKeysFromCfg(cfg || {});

    const authHead = page.querySelector("#sec-auth > .head");
    const authStrip = _findIconStrip(authHead);
    _applyIconStrip(authStrip, ["PLEX","JELLYFIN","SIMKL","TRAKT","MDBLIST","TMDB","TAUTULLI","ANILIST","EMBY"].filter((k) => configured.has(k)));

    const scHead = page.querySelector("#sec-scrobbler > .head");
    const scStrip = _findIconStrip(scHead);
    _applyIconStrip(scStrip, _scrobblerHeaderKeys(cfg, configured));
  }



  
  async function getAuthSummary(cfg) {
    const { entries, total } = _authProfileEntries(cfg || {});
    return { configured: entries.length, profiles: entries, total_profiles: total };
  }

  async function getPairsSummary(cfg) {
    let list = await fetchJSON("/api/pairs?t=" + Date.now());
    if (!Array.isArray(list)) {
      const a = cfg?.pairs || cfg?.connections || [];
      list = Array.isArray(a) ? a : [];
    }
    return { count: list.length };
  }

  async function getMetadataSummary(cfg) {
    const mansRaw = await fetchJSON("/api/metadata/providers?t=" + Date.now());
    const mans = Array.isArray(mansRaw) ? mansRaw : [];

    let detected = mans.length;
    const rawKey = String(cfg?.tmdb?.api_key ?? "").trim();
    const isMasked   = rawKey.length > 0 && /^[•]+$/.test(rawKey);
    const hasTmdbKey = rawKey.length > 0 || isMasked;

    let configured = hasTmdbKey ? 1 : 0;

    if (detected > 0) {
      configured = 0;
      let hasTmdbProvider = false;

      for (const m of mans) {
        const id     = String(m?.id || m?.name || "").toLowerCase();
        const isTmdb = id.includes("tmdb");
        if (isTmdb) hasTmdbProvider = true;

        const enabled = isTmdb && hasTmdbKey ? true : (m?.enabled !== false);

        let ready = (typeof m?.ready === "boolean") ? m.ready
                : (typeof m?.ok    === "boolean") ? m.ok
                : undefined;

        if (isTmdb && hasTmdbKey) ready = true;

        if (enabled && ready === true) configured++;
      }

      if (hasTmdbKey && !hasTmdbProvider) configured += 1;
    }

    if (detected === 0 && hasTmdbKey) detected = 1;

    return { detected, configured };
  }

  async function getSchedulingSummary(){
    const cfg = await fetchJSON("/api/scheduling?t=" + Date.now());
    const st  = await fetchJSON("/api/scheduling/status?t=" + Date.now());

    const stdEnabled = !!(cfg && cfg.enabled);
    const advEnabled = !!(cfg && cfg.advanced && cfg.advanced.enabled);
    const enabled = stdEnabled || advEnabled;

    let next = coalesceNextRun(st); if (next===null) next = coalesceNextRun(cfg);
    return { enabled, advanced: advEnabled, nextRun: next };
  }

  
  async function getScrobblerSummary(cfg){
    const sc = cfg?.scrobble || {};
    const mode = String(sc?.mode || "").toLowerCase();
    const enabled = !!sc?.enabled;

    let watcher = { alive:false, has_watch:false, stop_set:false };
    let providers = [];
    let sinks = [];

    if (enabled && mode === "watch") {
      const s = await fetchJSON("/api/watch/status");
      watcher = { alive:!!s?.alive, has_watch:!!s?.has_watch, stop_set:!!s?.stop_set };

      const groups = Array.isArray(s?.groups) ? s.groups : [];
      const fromGroups = [];
      groups.forEach((g) => {
        const p = prettyWatchProvider(g?.provider);
        if (p && !fromGroups.includes(p)) fromGroups.push(p);
      });

      const routesCfg = Array.isArray(sc?.watch?.routes) ? sc.watch.routes : [];
      const fromCfg = [];
      routesCfg.forEach((r) => {
        const p = prettyWatchProvider(r?.provider);
        if (p && !fromCfg.includes(p)) fromCfg.push(p);
      });

      providers = (fromGroups.length ? fromGroups : fromCfg);

      const stSinks = Array.isArray(s?.sinks) ? s.sinks : [];
      if (stSinks.length) {
        sinks = stSinks.map((x) => String(x || "").trim().toLowerCase()).filter(Boolean);
      } else {
        const cfgSinks = [];
        routesCfg.forEach((r) => {
          const sk = String(r?.sink || "").trim().toLowerCase();
          if (sk && !cfgSinks.includes(sk)) cfgSinks.push(sk);
        });
        sinks = cfgSinks;
      }
    }

    return { mode: enabled ? (mode || "webhook") : "", enabled, watcher, providers, sinks };
  }


  // Whitelisting summary
  
  function getWhitelistingSummary(cfg){
    const providers = [
      { key:"plex", label:"Plex", up:"PLEX" },
      { key:"emby", label:"Emby", up:"EMBY" },
      { key:"jellyfin", label:"Jellyfin", up:"JELLYFIN" }
    ];
    const cats = ["history","ratings","scrobble","watchlist","playlists"];

    const serverActive = [];
    for (const p of providers) {
      const base = cfg?.[p.key] || {};
      const insts = (base && typeof base === "object") ? (base.instances || {}) : {};
      const ids = ["default"].concat(
        (insts && typeof insts === "object" && !Array.isArray(insts))
          ? Object.keys(insts).map((x) => String(x))
          : []
      );

      for (const inst of ids) {
        const blk = _providerBlock(cfg, p.key, inst);
        if (!_profileConfigured(p.key, blk, cfg)) continue;

        const byCat = {};
        for (const c of cats) {
          const libs = blk?.[c]?.libraries;
          if (Array.isArray(libs) && libs.length > 0) byCat[c] = libs.length;
        }
        if (!Object.keys(byCat).length) continue;

        serverActive.push({ label: `${p.label} (${_instLabel(inst)})`, byCat });
      }
    }

    const pairsRaw = cfg?.pairs || cfg?.connections || [];
    const pairs = Array.isArray(pairsRaw) ? pairsRaw : [];

    const pairActive = [];
    const seen = new Set();
    const provKeys = ["PLEX","EMBY","JELLYFIN"];
    const provLabel = { PLEX:"Plex", EMBY:"Emby", JELLYFIN:"Jellyfin" };

    for (const pair of pairs) {
      const feats = pair?.features || {};
      const provSets = { PLEX:new Set(), EMBY:new Set(), JELLYFIN:new Set() };

      for (const fk of Object.keys(feats)) {
        const libsObj = feats?.[fk]?.libraries;
        if (!libsObj || typeof libsObj !== "object") continue;

        for (const pk of provKeys) {
          const libs = libsObj?.[pk];
          if (Array.isArray(libs) && libs.length > 0) libs.forEach(x => provSets[pk].add(String(x)));
        }
      }

      const s = String(pair?.source || pair?.a || "").toUpperCase();
      const t = String(pair?.target || pair?.b || "").toUpperCase();
      const si = _normInst(pair?.source_instance || pair?.a_instance || "default");
      const ti = _normInst(pair?.target_instance || pair?.b_instance || "default");

      const byProv = {};
      for (const pk of provKeys) {
        if (provSets[pk].size <= 0) continue;
        let name = provLabel[pk] || pk;
        if (pk === s && si !== "default") name += ` (${_instLabel(si)})`;
        if (pk === t && ti !== "default") name += ` (${_instLabel(ti)})`;
        byProv[name] = provSets[pk].size;
      }

      if (Object.keys(byProv).length) {
        const label = (s && t)
          ? `${s}${si !== "default" ? `(${_instLabel(si)})` : ""}→${t}${ti !== "default" ? `(${_instLabel(ti)})` : ""}`
          : String(pair?.id || "pair");
        if (!seen.has(label)) {
          seen.add(label);
          pairActive.push({ label, byProv });
        }
      }
    }

    return { serverActive, pairActive };
  }


  // UI Rendering
  const I = (name, size) => `<span class="material-symbols-rounded" style="font-size:${size||30}px">${name}</span>`;

  function row(iconName, title, oneLine){
    const el=d.createElement("div"); el.className="si-row";
    el.innerHTML=`
      <div class="si-ic">${I(iconName, 30)}</div>
      <div class="si-col">
        <div class="si-h">${title}</div>
        <div class="si-one">${oneLine}</div>
      </div>`;
    return el;
  }

  function whitelistHTML(wl){
    if (!wl) return "";
    const { serverActive, pairActive } = wl;
    if (!serverActive.length && !pairActive.length) return "";

    const catShort = { history:"H", ratings:"R", scrobble:"S", watchlist:"W", playlists:"P" };

    const out = [];
    out.push(`<div class="si-wl">`);

    if (serverActive.length) {
      out.push(`<div class="si-wl-level">`);
      out.push(`<div class="si-wl-level-title">Server-level</div>`);
      out.push(`<div class="si-wl-list">`);
      for (const s of serverActive) {
        const chips = Object.keys(s.byCat)
          .map(c => `<span class="si-chip">${catShort[c] || c[0].toUpperCase()} ${s.byCat[c]}</span>`)
          .join("");
        out.push(`
          <div class="si-wl-item">
            <span class="si-wl-name">${s.label}</span>
            <span class="si-wl-chips">${chips}</span>
          </div>
        `);
      }
      out.push(`</div></div>`);
    }

    if (pairActive.length) {
      out.push(`<div class="si-wl-level">`);
      out.push(`<div class="si-wl-level-title">Pair-level</div>`);
      out.push(`<div class="si-wl-list">`);
      for (const p of pairActive) {
        const chips = Object.keys(p.byProv)
          .map(k => `<span class="si-chip">${k} ${p.byProv[k]}</span>`)
          .join("");
        out.push(`
          <div class="si-wl-item">
            <span class="si-wl-name">${p.label}</span>
            <span class="si-wl-chips">${chips}</span>
          </div>
        `);
      }
      out.push(`</div></div>`);
    }

    out.push(`</div>`);
    return out.join("");
  }

  function renderWizard() {
    const body = $("#cw-si-body"); if (!body) return;
    body.innerHTML = `
      <div class="si-empty">
        <div class="hero-ic">${I("lock", 42)}</div>
        <div class="hero-text">
          <div class="h1">No authentication providers configured</div>
          <p class="p">Hey! You don’t have any authentication providers configured yet.<br>
          To start syncing, you need at least two of them.</p>
          <div class="tip">💡 Tip: configure at least one provider (e.g. Plex, Trakt, SIMKL or Jellyfin). The local CrossWatch tracker can work on top of that.</div>
        </div>
      </div>
    `;
  }

  function renderPairsWizard() {
    const body = $("#cw-si-body"); if (!body) return;
    body.innerHTML = `
      <div class="si-empty">
        <div class="hero-ic">${I("link", 42)}</div>
        <div class="hero-text">
          <div class="h1">No synchronization pairs/Scrobbler configured</div>
          <p class="p">Authentication looks good. Next step: create at least one pair under <b>Synchronization providers</b><br>
          <i>or/and</i> enable the <b>Scrobbler</b> (Webhook or Watcher) if you only want live progress tracking.</p>
          <div class="tip">💡 Tip: pick a source and a target (e.g., <b>Plex → Trakt</b>), or/and toggle Webhook/Watcher under <b>Scrobbler</b>.</div>
        </div>
      </div>
    `;
  }

  // Rendering optimizations
  let _siLastRenderKey = "";

  function _siStableStringify(v) {
    if (v === null || v === undefined) return String(v);
    const t = typeof v;
    if (t === "number" || t === "boolean") return String(v);
    if (t === "string") return JSON.stringify(v);
    if (Array.isArray(v)) return "[" + v.map(_siStableStringify).join(",") + "]";
    if (t === "object") {
      const keys = Object.keys(v).sort();
      return "{" + keys.map((k) => JSON.stringify(k) + ":" + _siStableStringify(v[k])).join(",") + "}";
    }
    return JSON.stringify(String(v));
  }

  function _siComputeRenderKey(data) {
    if (!data?.auth?.configured) return "wizard";
    const scrobReady = !!(data?.scrob?.enabled);
    if (data.auth.configured && data?.pairs?.count === 0 && !scrobReady) return "pairsWizard";
    return "main:" + _siStableStringify(data);
  }



  function render(data){
    const body=$("#cw-si-body"); if(!body) return;

    const key = _siComputeRenderKey(data);
    if (key === _siLastRenderKey) return;
    _siLastRenderKey = key;

    if (key === "wizard") return renderWizard();
    if (key === "pairsWizard") return renderPairsWizard();

    body.innerHTML="";
        body.appendChild(row("lock","Authentication Providers", authProfilesHTML(data.auth)));
    body.appendChild(row("link","Synchronization Pairs",  `Pairs: ${data.pairs.count}`));

    const wlBlock = whitelistHTML(data.whitelist);
    if (wlBlock) body.appendChild(row("filter_alt","Whitelisting", wlBlock));

    if (data.meta.configured === 0) {
      body.appendChild(row(
        "image",
        "Metadata Providers",
        `You're missing out on some great stuff.<br><b>Configure a Metadata Provider</b> ✨`
      ));
    } else {
      body.appendChild(row(
        "image",
        "Metadata Providers",
        `Detected providers: ${data.meta.detected}, Configured: ${data.meta.configured}`
      ));
    }
    body.appendChild(row("schedule","Scheduling",
      data.sched.enabled ? `${data.sched.advanced ? "Enabled (Advanced)" : "Enabled"} | Next run: ${toLocal(data.sched.nextRun)}` : "Disabled"));
        const scMode = !data.scrob.enabled ? "Disabled" : (data.scrob.mode==="watch" ? "Watcher mode" : "Webhook mode");
    const scStatus = !data.scrob.enabled ? "" : (data.scrob.mode==="watch" ? (data.scrob.watcher.alive ? "Running" : "Stopped") : "—");
    const provs = Array.isArray(data.scrob.providers) ? data.scrob.providers.filter(Boolean) : [];
    const parts = [scMode, scStatus].concat(provs);
    body.appendChild(row("sensors","Scrobbler", parts.filter(Boolean).join(" | ")));
  }

  // Layout sync
  function syncHeight(){
    const left=$("#cw-settings-left") || $("#page-settings .settings-wrap, #page-settings .settings, #page-settings .accordion, #page-settings .content, #page-settings .page-inner");
    const scroll=$("#cw-si-scroll");
    if(!left || !scroll) return;
    const rect=left.getBoundingClientRect();
    const top=12, maxViewport=Math.max(200,(w.innerHeight-top-16)), maxByLeft=Math.max(200,rect.height);
    scroll.style.maxHeight = `${Math.min(maxByLeft, maxViewport)}px`;
  }

  // Main loop
  let _loopTimer = null;

  let _tickBusy = false;

  function _scheduleTick(ms) {
    if (_loopTimer) clearTimeout(_loopTimer);
    _loopTimer = setTimeout(tick, ms);
  }

  async function tick() {
    if (_tickBusy) return;
    _tickBusy = true;
    try {
      const nodes = ensureCard();
      if (!nodes) { _scheduleTick(1200); return; }

      const page = $("#page-settings");
      const visible = !!(page && !page.classList.contains("hidden"));

      if (visible) {
        const cfg = await readConfig();
        _updateSettingsHeaderIcons(cfg);
        const [auth, pairs, meta, sched, scrob] = await Promise.all([
          getAuthSummary(cfg),
          getPairsSummary(cfg),
          getMetadataSummary(cfg),
          getSchedulingSummary(),
          getScrobblerSummary(cfg),
        ]);
        const whitelist = getWhitelistingSummary(cfg);
        render({ auth, pairs, meta, sched, scrob, whitelist });
        syncHeight();
        _scheduleTick(10000);
      } else {
        _scheduleTick(60000);
      }
    } finally {
      _tickBusy = false;
    }
  }

  // Bootstrapping
  (async function boot(){
    if(!$("#cw-settings-insight-style")){ const s=d.createElement("style"); s.id="cw-settings-insight-style"; s.textContent=css; d.head.appendChild(s); }

    d.addEventListener("tab-changed",(e)=>{ if(e?.detail?.id==="settings") setTimeout(()=>{ tick(); syncHeight(); },150); });

    d.addEventListener("config-saved", (e) => {
      const sec = e?.detail?.section;
      if (!sec || sec === "scheduling" || sec === "auth" || sec === "sync" || sec === "pairs" || sec === "connections" || sec === "scrobble") {
        tick();
      }
    });

    w.addEventListener("auth-changed", () => {
      try { tick(); } catch {}
    });

    d.addEventListener("scheduling-status-refresh", () => tick());
    d.addEventListener("visibilitychange", () => { if (!d.hidden) tick(); });
    w.addEventListener("focus", () => tick());

    let tries=0; while(!$("#page-settings") && tries<40){ tries++; await sleep(250); }
    tick();
    w.addEventListener("resize", syncHeight);
    w.addEventListener("scroll", syncHeight, { passive:true });

    w.refreshSettingsInsight = () => tick();
  })();
})(window, document);
