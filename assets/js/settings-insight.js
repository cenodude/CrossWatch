/* assets/js/settings-insight.js */
/* Refactored and expanded settings insight panel */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(function (w, d) {
  'use strict';
  if (w.__CW_SETTINGS_INSIGHT_STARTED__) return;
  w.__CW_SETTINGS_INSIGHT_STARTED__ = 1;

  const API = () => (w.CW && w.CW.API) || null;
  const Cache = () => (w.CW && w.CW.Cache) || null;
  const $ = (sel, root) => (root || d).querySelector(sel);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const Meta = () => (w.CW && w.CW.ProviderMeta) || null;
  const PROVIDERS = ['plex', 'emby', 'jellyfin', 'trakt', 'simkl', 'mdblist', 'anilist', 'tmdb', 'tautulli'];

  const css = `
#cw-settings-insight{display:block;min-width:0}
.si-card{border:1px solid rgba(255,255,255,.08);border-radius:22px;background:linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.015));box-shadow:0 18px 36px rgba(0,0,0,.18);overflow:hidden}
.si-header{padding:16px 18px 14px;border-bottom:1px solid rgba(255,255,255,.08)}
.si-header-kicker{display:block;font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:rgba(185,193,230,.72);line-height:1.2}
#cw-si-scroll{overflow:auto;overscroll-behavior:contain}
.si-body{padding:12px;display:grid;gap:10px}
.si-row{display:grid;grid-template-columns:22px minmax(0,1fr);gap:12px;align-items:start;padding:14px;border-radius:18px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.025);cursor:pointer;transition:transform .14s ease,border-color .14s ease,background .14s ease,box-shadow .14s ease}
.si-row:hover{transform:translateY(-1px);border-color:rgba(124,92,255,.24);background:rgba(255,255,255,.04);box-shadow:0 10px 20px rgba(0,0,0,.18)}
.si-ic{display:flex;align-items:center;justify-content:center;min-height:20px}.si-ic .material-symbols-rounded{font-size:20px;color:rgba(224,230,255,.92)}
.si-col{min-width:0}
.si-h{margin:0 0 6px;color:#F2F4FF;font-weight:800;font-size:14px;line-height:1.2}
.si-one{color:rgba(185,193,230,.84);font-size:12px;line-height:1.5}
.si-one b,.si-one strong{color:#F2F4FF}
.si-line{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.si-sep{color:rgba(185,193,230,.45);font-weight:800}
.si-status{color:#F2F4FF;font-weight:700}
.si-text{display:inline-flex;align-items:center}
.si-to{color:rgba(185,193,230,.72);font-weight:700}
.si-pchips,.si-inline-logos{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
.si-pchip{display:inline-flex;align-items:center;gap:8px;padding:6px 9px;border-radius:999px;background:rgba(255,255,255,.04);border:1px solid rgba(120,128,160,.16);font-size:12px;font-weight:800;color:#E6EAFD}
.si-count{display:inline-flex;align-items:center;justify-content:center;min-width:18px;height:18px;padding:0 5px;border-radius:999px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08);font-size:11px;line-height:1}
.si-logo{height:18px;width:auto;display:block;opacity:.95;flex:0 0 auto}
.si-logo.si-logo--small{height:16px}
.si-inline-text{display:inline-flex;align-items:center}
.si-empty{padding:20px 18px;color:#C8D0F3}.si-empty .h1{font-size:16px;font-weight:800;color:#E6EAFD;margin-bottom:8px}.si-empty .p{font-size:13px;line-height:1.55;margin:0 0 10px}
`;

  function ensureGrid() {
    const page = $('#page-settings');
    if (!page) return null;
    const host = $('#cw-settings-overview-grid', page) || $('#cw-settings-left', page) || page;
    let aside = $('#cw-settings-insight', page);
    if (!aside) {
      aside = d.createElement('aside');
      aside.id = 'cw-settings-insight';
      host.appendChild(aside);
    }
    return { left: $('#cw-settings-left', page) || host, aside };
  }

  function ensureCard() {
      const nodes = ensureGrid();
      if (!nodes) return null;
      if (!$('.si-card', nodes.aside)) {
        nodes.aside.innerHTML = '<div class="si-card"><div class="si-header"><span class="si-header-kicker">Status overview</span></div><div id="cw-si-scroll"><div class="si-body" id="cw-si-body"></div></div></div>';
      }
      return nodes;
    }

  function isVisible() {
    const page = $('#page-settings');
    return !!(page && !page.classList.contains('hidden') && page.offsetParent !== null);
  }

  function toLocal(v) {
    if (v === undefined || v === null || v === '') return '—';
    const n = Number(v);
    const dt = new Date(Number.isFinite(n) && n > 0 ? (n < 1e10 ? n * 1000 : n) : v);
    return isNaN(+dt) ? '—' : dt.toLocaleString(undefined, { hour12: false });
  }

  function esc(v) {
    return String(v ?? '').replace(/[&<>"']/g, (m) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]));
  }

  function has(v) { return typeof v === 'string' ? v.trim().length > 0 : !!v; }
  function normInst(v) { const s = String(v || '').trim(); return !s || s.toLowerCase() === 'default' ? 'default' : s; }

  function profileConfigured(provider, blk, cfg) {
    const p = String(provider || '').toLowerCase();
    const b = (blk && typeof blk === 'object') ? blk : {};
    if (p === 'plex') return has(b.account_token) || has(b.token) || has(b.access_token);
    if (p === 'emby' || p === 'jellyfin') return has(b.access_token) || has(b.api_key) || has(b.token);
    if (p === 'trakt' || p === 'simkl') return has(b.access_token) || has(b.refresh_token);
    if (p === 'anilist') return has(b.access_token) || has(b.token);
    if (p === 'mdblist') return has(b.api_key);
    if (p === 'tautulli') {
      const t = b || cfg?.tautulli || cfg?.auth?.tautulli || {};
      return has(t.server_url || t.server);
    }
    if (p === 'tmdb') return has(b.api_key) && has(b.session_id || b.session);
    return has(b.access_token) || has(b.api_key) || has(b.token);
  }

  function providerBlock(cfg, provider, instanceId) {
    const base = (cfg && cfg[provider] && typeof cfg[provider] === 'object') ? cfg[provider] : {};
    const inst = normInst(instanceId);
    if (inst === 'default') return base;
    const blk = base?.instances?.[inst];
    return blk && typeof blk === 'object' ? blk : {};
  }

  function countProfiles(cfg, provider) {
    let n = profileConfigured(provider, providerBlock(cfg, provider, 'default'), cfg) ? 1 : 0;
    const insts = cfg?.[provider]?.instances;
    if (insts && typeof insts === 'object') {
      Object.keys(insts).forEach((id) => { if (profileConfigured(provider, insts[id], cfg)) n += 1; });
    }
    if (!n && provider === 'tmdb') {
      const legacy = cfg?.tmdb_sync || cfg?.auth?.tmdb_sync;
      if (legacy && profileConfigured('tmdb', legacy, cfg)) n = 1;
    }
    return n;
  }

  function providerMeta(provider) {
    const key = String(provider || '').trim();
    const meta = Meta();
    const upper = key.toUpperCase();
    return {
      key: upper,
      label: meta?.label?.(upper) || meta?.label?.(key) || upper || '?',
      logo: meta?.logo?.(upper) || meta?.logo?.(key) || '',
    };
  }

  function providerLogo(provider) {
    const meta = providerMeta(provider);
    return meta.logo || `/assets/img/${meta.key}-log.svg`;
  }

  function providerIconHTML(provider, cls = '') {
    const meta = providerMeta(provider);
    const src = providerLogo(provider);
    const className = ['si-logo', cls].filter(Boolean).join(' ');
    if (src) return `<img loading="lazy" class="${className}" src="${esc(src)}" alt="${esc(meta.label)}" title="${esc(meta.label)}">`;
    return `<span class="si-inline-text">${esc(meta.label)}</span>`;
  }

  function providerBadgeHTML(provider, count) {
    return `<span class="si-pchip" title="${esc(providerMeta(provider).label)}: ${count}">${providerIconHTML(provider)}<span class="si-count">${count}</span></span>`;
  }

  function authSummary(cfg) {
    let total = 0;
    const profiles = PROVIDERS.map((prov) => {
      const count = countProfiles(cfg, prov);
      if (!count) return null;
      total += count;
      return { ...providerMeta(prov), provider: prov, count };
    }).filter(Boolean);
    return { configured: profiles.length, profiles, total_profiles: total };
  }

  function authProfilesHTML(auth) {
    if (!auth?.profiles?.length) return 'No profiles configured';
    return `<div class="si-pchips">${auth.profiles.map((p) => providerBadgeHTML(p.provider, p.count)).join('')}</div>`;
  }

  async function readConfig(force = false) {
    const api = API();
    if (!api) return {};
    try { return await api.Config.load(force); } catch { return Cache()?.getCfg() || {}; }
  }

  async function getPairsSummary(cfg, force = false) {
    const api = API();
    try {
      const list = await api.Pairs.list(force);
      return { count: Array.isArray(list) ? list.length : 0 };
    } catch {
      const list = Array.isArray(cfg?.pairs) ? cfg.pairs : (Array.isArray(cfg?.connections) ? cfg.connections : []);
      return { count: list.length };
    }
  }

  async function getMetadataSummary(cfg, force = false) {
    const api = API();
    let list = [];
    try { list = await api.Metadata.providers(force); } catch {}
    list = Array.isArray(list) ? list : [];
    const rawKey = String(cfg?.tmdb?.api_key ?? '').trim();
    const hasTmdbKey = !!rawKey;
    let configured = 0;
    let detected = list.length;
    if (detected) {
      let hasTmdbProvider = false;
      for (const m of list) {
        const id = String(m?.id || m?.name || '').toLowerCase();
        const isTmdb = id.includes('tmdb');
        if (isTmdb) hasTmdbProvider = true;
        const enabled = isTmdb && hasTmdbKey ? true : (m?.enabled !== false);
        const ready = (typeof m?.ready === 'boolean') ? m.ready : (typeof m?.ok === 'boolean' ? m.ok : false);
        if (enabled && (ready || (isTmdb && hasTmdbKey))) configured += 1;
      }
      if (hasTmdbKey && !hasTmdbProvider) configured += 1;
    } else if (hasTmdbKey) {
      detected = configured = 1;
    }
    return { detected, configured };
  }

  function scheduleEnabled(s) {
    const sc = s || {};
    return !!(sc.enabled || sc?.advanced?.enabled);
  }

  async function getSchedulingSummary(cfg, force = false) {
    const api = API();
    const fallback = cfg?.scheduling || {};
    try {
      const st = await api.Scheduling.status(force);
      const sc = st?.config || fallback;
      return {
        enabled: scheduleEnabled(sc),
        advanced: !!sc?.advanced?.enabled,
        running: !!st?.running,
        nextRun: st?.next_run_at ?? st?.next_run ?? sc?.next_run_at ?? sc?.next_run ?? null,
      };
    } catch {
      return { enabled: scheduleEnabled(fallback), advanced: !!fallback?.advanced?.enabled, running: false, nextRun: fallback?.next_run_at ?? fallback?.next_run ?? null };
    }
  }

  function logosHTML(list) {
    const items = Array.isArray(list) ? list.filter(Boolean) : [];
    if (!items.length) return '';
    return `<span class="si-inline-logos">${items.map((provider) => providerIconHTML(provider)).join('')}</span>`;
  }

  function metadataSummaryHTML(meta) {
    if (!meta?.configured) return `You're missing out on some great stuff.<br><strong>Configure a Metadata Provider</strong> ✨`;
    return `<div class="si-line"><span class="si-status">Detected:</span><span class="si-text">${meta.detected}</span><span class="si-sep">•</span><span class="si-status">Configured:</span><span class="si-text">${meta.configured}</span></div>`;
  }

  function schedulingSummaryHTML(sched) {
    if (!sched?.enabled) return 'Disabled';
    return `<div class="si-line"><span class="si-status">${sched.advanced ? 'Enabled (Advanced)' : 'Enabled'}</span>${sched.running ? '<span class="si-sep">|</span><span class="si-text">Running</span>' : ''}${sched.nextRun ? `<span class="si-sep">|</span><span class="si-text">Next run: ${esc(toLocal(sched.nextRun))}</span>` : ''}</div>`;
  }

  function scrobblerSummaryHTML(scrob) {
    if (!scrob?.enabled) return 'Disabled';

    const modeLabel = scrob.mode === 'watch' ? 'Watcher' : 'Webhook';
    const status = scrob.mode === 'watch'
      ? (scrob.watcher.alive ? 'Running' : 'Stopped')
      : 'Listening';
    const providers = logosHTML(scrob.providers);
    const sinks = logosHTML(scrob.sinks);

    let route = '';
    if (providers) route += providers;
    if (sinks) route += `${route ? '<span class="si-to">to</span>' : ''}${sinks}`;
    if (!route) route = '<span class="si-inline-text">No routes configured</span>';

    return `<div class="si-line"><span class="si-text">${modeLabel}</span><span class="si-sep">|</span><span class="si-status">${status}:</span>${route}</div>`;
  }

  async function getScrobblerSummary(cfg, force = false) {
    const sc = cfg?.scrobble || {};
    const enabled = !!sc?.enabled;
    const mode = String(sc?.mode || 'webhook').toLowerCase();
    const out = { enabled, mode: enabled ? mode : '', watcher: { alive: false }, providers: [], sinks: [] };
    if (!enabled) return out;

    const routes = Array.isArray(sc?.watch?.routes) ? sc.watch.routes : [];
    out.providers = routes.map((r) => String(r?.provider || '').trim().toLowerCase()).filter(Boolean).filter((v, i, a) => a.indexOf(v) === i);
    out.sinks = routes.map((r) => String(r?.sink || '').trim().toLowerCase()).filter(Boolean).filter((v, i, a) => a.indexOf(v) === i);

    if (mode !== 'watch') return out;
    try {
      const st = await API().Watch.status(force);
      out.watcher.alive = !!st?.alive;
      const groups = Array.isArray(st?.groups) ? st.groups : [];
      if (groups.length) out.providers = groups.map((g) => String(g?.provider || '').trim().toLowerCase()).filter(Boolean).filter((v, i, a) => a.indexOf(v) === i);
      const sinks = Array.isArray(st?.sinks) ? st.sinks : [];
      if (sinks.length) out.sinks = sinks.map((x) => String(x || '').trim().toLowerCase()).filter(Boolean).filter((v, i, a) => a.indexOf(v) === i);
    } catch {}
    return out;
  }

  function getWhitelistSummary(cfg) {
    const txt = JSON.stringify(cfg || {});
    const active = (txt.match(/"whitelist"\s*:/g) || []).length + (txt.match(/"whitelisting"\s*:/g) || []).length;
    return active ? { active } : null;
  }

  const I = (name) => `<span class="material-symbols-rounded">${name}</span>`;
  function row(icon, title, body, pane) {
    const el = d.createElement('div');
    el.className = 'si-row';
    if (pane) el.dataset.pane = pane;
    el.innerHTML = `<div class="si-ic">${I(icon)}</div><div class="si-col"><div class="si-h">${title}</div><div class="si-one">${body}</div></div>`;
    return el;
  }

  function renderWizard() {
    const body = $('#cw-si-body');
    if (!body) return;
    body.innerHTML = '<div class="si-empty"><div class="h1">No authentication providers</div><p class="p">Configure at least one authentication provider to get started. To sync, you need at least two sides in play.</p></div>';
  }

  function renderPairsWizard() {
    const body = $('#cw-si-body');
    if (!body) return;
    body.innerHTML = '<div class="si-empty"><div class="h1">No synchronization pairs or scrobbler</div><p class="p">Authentication looks good. Next step: add a sync pair or enable the scrobbler.</p></div>';
  }

  let lastKey = '';
  function render(data) {
    const body = $('#cw-si-body');
    if (!body) return;
    const key = JSON.stringify(data || {});
    if (key === lastKey) return;
    lastKey = key;

    if (!data?.auth?.configured) return renderWizard();
    if (!data?.pairs?.count && !data?.scrob?.enabled) return renderPairsWizard();

    body.innerHTML = '';
    body.appendChild(row('lock', 'Authentication Providers', authProfilesHTML(data.auth), 'providers'));
    body.appendChild(row('link', 'Synchronization Pairs', `<div class="si-line"><span class="si-status">Pairs:</span><span class="si-text">${data.pairs.count}</span></div>`, 'providers'));
    if (data.whitelist?.active) body.appendChild(row('filter_alt', 'Whitelisting', `<div class="si-line"><span class="si-status">Active blocks:</span><span class="si-text">${data.whitelist.active}</span></div>`, 'providers'));
    body.appendChild(row('image', 'Metadata Providers', metadataSummaryHTML(data.meta), 'providers'));
    body.appendChild(row('schedule', 'Scheduling', schedulingSummaryHTML(data.sched), 'scheduling'));
    body.appendChild(row('sensors', 'Scrobbler', scrobblerSummaryHTML(data.scrob), 'scrobbler'));
  }

  function syncHeight() {
    const scroll = $('#cw-si-scroll');
    if (!scroll) return;
    const maxViewport = Math.max(260, w.innerHeight - 220);
    scroll.style.maxHeight = `${maxViewport}px`;
  }

  const state = { cfg: null, staticData: null, liveData: null, liveTimer: null, busyStatic: false, busyLive: false, queuedStatic: false, queuedLive: false };

  function applyRender() {
    if (!state.staticData || !state.liveData) return;
    render({ ...state.staticData, ...state.liveData });
    syncHeight();
  }

  function scheduleLive() {
    clearTimeout(state.liveTimer);
    state.liveTimer = null;
    if (!isVisible()) return;
    const cfg = state.cfg || {};
    const schedOn = scheduleEnabled(cfg?.scheduling);
    const scrobOn = !!cfg?.scrobble?.enabled;
    if (!(schedOn || scrobOn)) return;
    state.liveTimer = setTimeout(() => refreshLive(false), 30000);
  }

  async function refreshStatic(force = false) {
    if (!isVisible() || state.busyStatic) { state.queuedStatic = true; return; }
    state.busyStatic = true;
    try {
      ensureCard();
      const cfg = await readConfig(force);
      state.cfg = cfg || {};
      const [auth, pairs, meta] = await Promise.all([
        Promise.resolve(authSummary(state.cfg)),
        getPairsSummary(state.cfg, force),
        getMetadataSummary(state.cfg, force),
      ]);
      state.staticData = { auth, pairs, meta, whitelist: getWhitelistSummary(state.cfg) };
      applyRender();
      scheduleLive();
    } finally {
      state.busyStatic = false;
      if (state.queuedStatic) { state.queuedStatic = false; setTimeout(() => refreshStatic(force), 0); }
    }
  }

  async function refreshLive(force = false) {
    if (!isVisible() || state.busyLive) { state.queuedLive = true; return; }
    state.busyLive = true;
    try {
      if (!state.cfg) state.cfg = await readConfig(force);
      const [sched, scrob] = await Promise.all([
        getSchedulingSummary(state.cfg, force),
        getScrobblerSummary(state.cfg, force),
      ]);
      state.liveData = { sched, scrob };
      applyRender();
      scheduleLive();
    } finally {
      state.busyLive = false;
      if (state.queuedLive) { state.queuedLive = false; setTimeout(() => refreshLive(force), 0); }
    }
  }

  async function refreshAll(force = false) {
    await refreshStatic(force);
    await refreshLive(force);
  }

  function invalidateAll() { try { Cache()?.invalidate(); } catch {} state.cfg = null; state.staticData = null; state.liveData = null; }

  (async function boot() {
    if (!$('#cw-settings-insight-style')) {
      const s = d.createElement('style');
      s.id = 'cw-settings-insight-style';
      s.textContent = css;
      d.head.appendChild(s);
    }

    let tries = 0;
    while (!$('#page-settings') && tries < 40) { tries += 1; await sleep(250); }
    ensureCard();

    d.addEventListener('tab-changed', (e) => { if (e?.detail?.id === 'settings') setTimeout(() => refreshAll(true), 120); });
    d.addEventListener('config-saved', () => { invalidateAll(); refreshAll(true); });
    w.addEventListener('auth-changed', () => { invalidateAll(); refreshAll(true); });
    d.addEventListener('scheduling-status-refresh', () => refreshLive(true));
    d.addEventListener('watcher-status-refresh', () => refreshLive(true));
    d.addEventListener('visibilitychange', () => { if (!d.hidden && isVisible()) refreshLive(false); });
    d.addEventListener('click', (e) => {
      const row = e.target?.closest?.('.si-row[data-pane]');
      const pane = row?.dataset?.pane;
      if (pane) w.cwSettingsSelect?.(pane);
    });
    w.addEventListener('focus', () => { if (isVisible()) refreshLive(false); });
    w.addEventListener('resize', syncHeight);
    w.addEventListener('scroll', syncHeight, { passive: true });

    if (isVisible()) refreshAll(true);
    w.refreshSettingsInsight = () => refreshAll(true);
  })();
})(window, document);
