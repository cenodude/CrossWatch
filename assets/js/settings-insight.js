/* assets/js/settings-insight.js */
/* refactored */
/* Compact settings insight panel */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(function (w, d) {
  'use strict';
  if (w.__CW_SETTINGS_INSIGHT_STARTED__) return;
  w.__CW_SETTINGS_INSIGHT_STARTED__ = 1;

  const API = () => w.CW?.API || null;
  const Cache = () => w.CW?.Cache || null;
  const Meta = () => w.CW?.ProviderMeta || null;
  const $ = (s, r = d) => r.querySelector(s);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const PROVIDERS = ['plex', 'emby', 'jellyfin', 'trakt', 'simkl', 'mdblist', 'anilist', 'tmdb', 'tautulli'];
  const EMPTY = {
    auth: '<div class="si-empty"><div class="h1">No authentication providers</div><p class="p">Configure at least one authentication provider to get started. To sync, you need at least two sides in play.</p></div>',
    pairs: '<div class="si-empty"><div class="h1">No synchronization pairs or scrobbler</div><p class="p">Authentication looks good. Next step: add a sync pair or enable the scrobbler.</p></div>'
  };
  const css = `#cw-settings-insight{display:block;min-width:0;--si-bg:linear-gradient(180deg,rgba(9,12,18,.96),rgba(4,6,10,.98));--si-panel:linear-gradient(180deg,rgba(12,15,22,.94),rgba(6,8,12,.97));--si-panel-hover:linear-gradient(180deg,rgba(15,18,26,.96),rgba(8,10,15,.98));--si-border:rgba(255,255,255,.075);--si-border-strong:rgba(255,255,255,.12);--si-shadow:0 22px 44px rgba(0,0,0,.40),inset 0 1px 0 rgba(255,255,255,.03);--si-fg:#f3f5ff;--si-soft:rgba(196,204,223,.74)}.si-card{position:relative;border:1px solid var(--si-border);border-radius:22px;overflow:hidden;background:radial-gradient(125% 140% at 0% 0%,rgba(84,92,132,.10),transparent 38%),radial-gradient(120% 140% at 100% 100%,rgba(50,56,84,.08),transparent 46%),var(--si-bg);box-shadow:var(--si-shadow);backdrop-filter:blur(16px) saturate(118%);-webkit-backdrop-filter:blur(16px) saturate(118%)}.si-card::before{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(180deg,rgba(255,255,255,.028),transparent 22%,rgba(255,255,255,.012) 100%)}.si-header{position:relative;padding:16px 18px 14px;border-bottom:1px solid rgba(255,255,255,.06)}.si-header-kicker{display:block;font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--si-soft);line-height:1.2}#cw-si-scroll{overflow:auto;overscroll-behavior:contain}.si-body{padding:12px;display:grid;gap:10px}.si-row{position:relative;display:grid;grid-template-columns:20px minmax(0,1fr);gap:12px;align-items:start;padding:14px 14px 13px;border-radius:18px;border:1px solid var(--si-border);background:var(--si-panel);box-shadow:0 12px 26px rgba(0,0,0,.18);cursor:pointer;transition:transform .14s ease,border-color .14s ease,background .14s ease,box-shadow .14s ease}.si-row::before{content:"";position:absolute;inset:0;pointer-events:none;border-radius:inherit;background:linear-gradient(135deg,rgba(255,255,255,.035),transparent 56%);opacity:.8}.si-row:hover{transform:translateY(-1px);border-color:var(--si-border-strong);background:var(--si-panel-hover);box-shadow:0 16px 28px rgba(0,0,0,.26)}.si-ic{display:flex;align-items:center;justify-content:center;min-height:20px}.si-ic .material-symbols-rounded{font-size:19px;color:rgba(230,235,248,.88)}.si-col{min-width:0}.si-h{margin:0 0 6px;color:var(--si-fg);font-weight:800;font-size:14px;line-height:1.2}.si-one{color:var(--si-soft);font-size:12px;line-height:1.5}.si-one b,.si-one strong{color:var(--si-fg)}.si-line{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.si-sep{color:rgba(196,204,223,.42);font-weight:800}.si-status{color:var(--si-fg);font-weight:700}.si-text,.si-inline-text{display:inline-flex;align-items:center}.si-to{color:rgba(196,204,223,.70);font-weight:700}.si-pchips,.si-inline-logos{display:flex;flex-wrap:wrap;gap:8px;align-items:center}.si-pchip{display:inline-flex;align-items:center;gap:8px;padding:6px 9px;border-radius:999px;background:rgba(255,255,255,.035);border:1px solid rgba(255,255,255,.08);font-size:12px;font-weight:800;color:#e7ecfb;box-shadow:inset 0 1px 0 rgba(255,255,255,.02)}.si-count{display:inline-flex;align-items:center;justify-content:center;min-width:18px;height:18px;padding:0 5px;border-radius:999px;background:rgba(255,255,255,.055);border:1px solid rgba(255,255,255,.08);font-size:11px;line-height:1}.si-logo{height:18px;width:auto;display:block;opacity:.95;flex:0 0 auto;filter:saturate(.92) brightness(.98)}.si-empty{padding:20px 18px;color:var(--si-soft)}.si-empty .h1{font-size:16px;font-weight:800;color:#e7ecfb;margin-bottom:8px}.si-empty .p{font-size:13px;line-height:1.55;margin:0 0 10px}`;

  const esc = (v) => String(v ?? '').replace(/[&<>"']/g, (m) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]));
  const has = (v) => typeof v === 'string' ? v.trim().length > 0 : !!v;
  const uniq = (arr) => [...new Set((Array.isArray(arr) ? arr : []).map((v) => String(v || '').trim().toLowerCase()).filter(Boolean))];
  const scheduleEnabled = (s) => !!(s?.enabled || s?.advanced?.enabled);
  const isVisible = () => { const p = $('#page-settings'); return !!(p && !p.classList.contains('hidden') && p.offsetParent !== null); };
  const toLocal = (v) => {
    if (v === undefined || v === null || v === '') return '—';
    const n = Number(v), dt = new Date(Number.isFinite(n) && n > 0 ? (n < 1e10 ? n * 1000 : n) : v);
    return isNaN(+dt) ? '—' : dt.toLocaleString(undefined, { hour12: false });
  };

  function ensureCard() {
    const page = $('#page-settings');
    if (!page) return null;
    const host = $('#cw-settings-overview-grid', page) || $('#cw-settings-left', page) || page;
    let aside = $('#cw-settings-insight', page);
    if (!aside) {
      aside = d.createElement('aside');
      aside.id = 'cw-settings-insight';
      host.appendChild(aside);
    }
    if (!$('.si-card', aside)) aside.innerHTML = '<div class="si-card"><div class="si-header"><span class="si-header-kicker">Status overview</span></div><div id="cw-si-scroll"><div class="si-body" id="cw-si-body"></div></div></div>';
    return aside;
  }

  function providerMeta(provider) {
    const raw = String(provider || '').trim(), key = raw.toUpperCase(), meta = Meta();
    return { key, label: meta?.label?.(key) || meta?.label?.(raw) || key || '?', logo: meta?.logo?.(key) || meta?.logo?.(raw) || '' };
  }

  function providerIcon(provider) {
    const meta = providerMeta(provider), src = meta.logo || `/assets/img/${meta.key}-log.svg`;
    return src ? `<img loading="lazy" class="si-logo" src="${esc(src)}" alt="${esc(meta.label)}" title="${esc(meta.label)}">` : `<span class="si-inline-text">${esc(meta.label)}</span>`;
  }

  const logosHTML = (list) => (list = uniq(list)).length ? `<span class="si-inline-logos">${list.map(providerIcon).join('')}</span>` : '';
  const badgeHTML = (provider, count) => `<span class="si-pchip" title="${esc(providerMeta(provider).label)}: ${count}">${providerIcon(provider)}<span class="si-count">${count}</span></span>`;
  const line = (...items) => `<div class="si-line">${items.filter(Boolean).join('')}</div>`;
  const kv = (k, v) => `<span class="si-status">${k}</span><span class="si-text">${v}</span>`;
  const sep = (s = '•') => `<span class="si-sep">${s}</span>`;

  function profileConfigured(provider, blk, cfg) {
    const p = String(provider || '').toLowerCase(), b = blk && typeof blk === 'object' ? blk : {};
    if (p === 'plex') return has(b.account_token) || has(b.token) || has(b.access_token);
    if (p === 'emby' || p === 'jellyfin') return has(b.access_token) || has(b.api_key) || has(b.token);
    if (p === 'trakt' || p === 'simkl') return has(b.access_token) || has(b.refresh_token);
    if (p === 'anilist') return has(b.access_token) || has(b.token);
    if (p === 'mdblist') return has(b.api_key);
    if (p === 'tautulli') return has((b || cfg?.tautulli || cfg?.auth?.tautulli || {}).server_url || (b || cfg?.tautulli || cfg?.auth?.tautulli || {}).server);
    if (p === 'tmdb') return has(b.api_key) && has(b.session_id || b.session);
    return has(b.access_token) || has(b.api_key) || has(b.token);
  }

  function countProfiles(cfg, provider) {
    const base = cfg?.[provider] && typeof cfg[provider] === 'object' ? cfg[provider] : {};
    let n = profileConfigured(provider, base, cfg) ? 1 : 0;
    Object.values(base.instances || {}).forEach((blk) => { if (profileConfigured(provider, blk, cfg)) n += 1; });
    if (!n && provider === 'tmdb' && profileConfigured('tmdb', cfg?.tmdb_sync || cfg?.auth?.tmdb_sync, cfg)) n = 1;
    return n;
  }

  function authSummary(cfg) {
    const profiles = PROVIDERS.map((provider) => ({ provider, count: countProfiles(cfg, provider) })).filter((x) => x.count);
    return { configured: profiles.length, profiles };
  }

  const authProfilesHTML = (auth) => auth?.profiles?.length ? `<div class="si-pchips">${auth.profiles.map((p) => badgeHTML(p.provider, p.count)).join('')}</div>` : 'No profiles configured';

  async function readConfig(force) {
    const api = API();
    if (!api) return {};
    try { return await api.Config.load(force); } catch { return Cache()?.getCfg() || {}; }
  }

  async function getPairsSummary(cfg, force) {
    try {
      const list = await API()?.Pairs?.list(force);
      return { count: Array.isArray(list) ? list.length : 0 };
    } catch {
      const list = Array.isArray(cfg?.pairs) ? cfg.pairs : Array.isArray(cfg?.connections) ? cfg.connections : [];
      return { count: list.length };
    }
  }

  async function getMetadataSummary(cfg, force) {
    let list = [];
    try { list = await API()?.Metadata?.providers(force); } catch {}
    list = Array.isArray(list) ? list : [];
    const hasTmdbKey = !!String(cfg?.tmdb?.api_key ?? '').trim();
    let detected = list.length, configured = 0, hasTmdbProvider = false;
    for (const m of list) {
      const id = String(m?.id || m?.name || '').toLowerCase(), isTmdb = id.includes('tmdb');
      hasTmdbProvider ||= isTmdb;
      if ((isTmdb && hasTmdbKey ? true : m?.enabled !== false) && ((typeof m?.ready === 'boolean' ? m.ready : !!m?.ok) || (isTmdb && hasTmdbKey))) configured += 1;
    }
    if (hasTmdbKey && !hasTmdbProvider) configured += 1;
    if (!detected && hasTmdbKey) detected = configured = 1;
    return { detected, configured };
  }

  async function getSchedulingSummary(cfg, force) {
    const fallback = cfg?.scheduling || {};
    try {
      const st = await API()?.Scheduling?.status(force), sc = st?.config || fallback;
      return { enabled: scheduleEnabled(sc), advanced: !!sc?.advanced?.enabled, running: !!st?.running, nextRun: st?.next_run_at ?? st?.next_run ?? sc?.next_run_at ?? sc?.next_run ?? null };
    } catch {
      return { enabled: scheduleEnabled(fallback), advanced: !!fallback?.advanced?.enabled, running: false, nextRun: fallback?.next_run_at ?? fallback?.next_run ?? null };
    }
  }

  async function getScrobblerSummary(cfg, force) {
    const sc = cfg?.scrobble || {}, enabled = !!sc.enabled, mode = String(sc.mode || 'webhook').toLowerCase();
    const out = { enabled, mode: enabled ? mode : '', watcher: { alive: false }, providers: [], sinks: [] };
    if (!enabled) return out;
    const routes = Array.isArray(sc?.watch?.routes) ? sc.watch.routes : [];
    out.providers = uniq(routes.map((r) => r?.provider));
    out.sinks = uniq(routes.map((r) => r?.sink));
    if (mode !== 'watch') return out;
    try {
      const st = await API()?.Watch?.status(force);
      out.watcher.alive = !!st?.alive;
      if (st?.groups?.length) out.providers = uniq(st.groups.map((g) => g?.provider));
      if (st?.sinks?.length) out.sinks = uniq(st.sinks);
    } catch {}
    return out;
  }

  const getWhitelistSummary = (cfg) => {
    const txt = JSON.stringify(cfg || ''), active = (txt.match(/"whitelist"\s*:/g) || []).length + (txt.match(/"whitelisting"\s*:/g) || []).length;
    return active ? { active } : null;
  };

  const metadataHTML = (meta) => meta?.configured ? line(kv('Detected:', meta.detected), sep(), kv('Configured:', meta.configured)) : `You're missing out on some great stuff.<br><strong>Configure a Metadata Provider</strong> ✨`;
  const schedulingHTML = (sched) => !sched?.enabled ? 'Disabled' : line(`<span class="si-status">${sched.advanced ? 'Enabled (Advanced)' : 'Enabled'}</span>`, sched.running && `${sep('|')}<span class="si-text">Running</span>`, sched.nextRun && `${sep('|')}<span class="si-text">Next run: ${esc(toLocal(sched.nextRun))}</span>`);
  function scrobblerHTML(scrob) {
    if (!scrob?.enabled) return 'Disabled';
    const mode = scrob.mode === 'watch' ? 'Watcher' : 'Webhook';
    const status = scrob.mode === 'watch' ? (scrob.watcher.alive ? 'Running' : 'Stopped') : 'Listening';
    const route = [logosHTML(scrob.providers), logosHTML(scrob.sinks)].filter(Boolean);
    return line(`<span class="si-text">${mode}</span>`, sep('|'), `<span class="si-status">${status}:</span>`, route[0] || route[1] ? `${route[0] || ''}${route[0] && route[1] ? '<span class="si-to">to</span>' : ''}${route[1] || ''}` : '<span class="si-inline-text">No routes configured</span>');
  }

  function row(icon, title, body, pane) {
    const el = d.createElement('div');
    el.className = 'si-row';
    if (pane) el.dataset.pane = pane;
    el.innerHTML = `<div class="si-ic"><span class="material-symbols-rounded">${icon}</span></div><div class="si-col"><div class="si-h">${title}</div><div class="si-one">${body}</div></div>`;
    return el;
  }

  const state = { cfg: null, staticData: null, liveData: null, liveTimer: null, busyStatic: false, busyLive: false, queuedStatic: false, queuedLive: false, lastKey: '' };
  const syncHeight = () => { const el = $('#cw-si-scroll'); if (el) el.style.maxHeight = `${Math.max(260, w.innerHeight - 220)}px`; };
  const applyRender = () => state.staticData && state.liveData && render({ ...state.staticData, ...state.liveData });

  function render(data) {
    const body = $('#cw-si-body'), key = JSON.stringify(data || {});
    if (!body || key === state.lastKey) return;
    state.lastKey = key;
    if (!data?.auth?.configured) return void (body.innerHTML = EMPTY.auth);
    if (!data?.pairs?.count && !data?.scrob?.enabled) return void (body.innerHTML = EMPTY.pairs);
    body.innerHTML = '';
    [
      row('lock', 'Authentication Providers', authProfilesHTML(data.auth), 'providers'),
      row('link', 'Synchronization Pairs', line(kv('Pairs:', data.pairs.count)), 'providers'),
      data.whitelist?.active && row('filter_alt', 'Whitelisting', line(kv('Active blocks:', data.whitelist.active)), 'providers'),
      row('image', 'Metadata Providers', metadataHTML(data.meta), 'providers'),
      row('schedule', 'Scheduling', schedulingHTML(data.sched), 'scheduling'),
      row('sensors', 'Scrobbler', scrobblerHTML(data.scrob), 'scrobbler')
    ].filter(Boolean).forEach((el) => body.appendChild(el));
    syncHeight();
  }

  function scheduleLive() {
    clearTimeout(state.liveTimer);
    state.liveTimer = null;
    if (!isVisible() || !(scheduleEnabled(state.cfg?.scheduling) || state.cfg?.scrobble?.enabled)) return;
    state.liveTimer = setTimeout(() => refreshLive(false), 30000);
  }

  async function guarded(kind, force, work) {
    const busy = `busy${kind}`, queued = `queued${kind}`;
    if (!isVisible() || state[busy]) return void (state[queued] = true);
    state[busy] = true;
    try { await work(); } finally {
      state[busy] = false;
      if (state[queued]) {
        state[queued] = false;
        setTimeout(() => kind === 'Static' ? refreshStatic(force) : refreshLive(force), 0);
      }
    }
  }

  async function refreshStatic(force = false) {
    await guarded('Static', force, async () => {
      ensureCard();
      state.cfg = await readConfig(force) || {};
      const [pairs, meta] = await Promise.all([getPairsSummary(state.cfg, force), getMetadataSummary(state.cfg, force)]);
      state.staticData = { auth: authSummary(state.cfg), pairs, meta, whitelist: getWhitelistSummary(state.cfg) };
      applyRender();
      scheduleLive();
    });
  }

  async function refreshLive(force = false) {
    await guarded('Live', force, async () => {
      if (!state.cfg) state.cfg = await readConfig(force);
      const [sched, scrob] = await Promise.all([getSchedulingSummary(state.cfg, force), getScrobblerSummary(state.cfg, force)]);
      state.liveData = { sched, scrob };
      applyRender();
      scheduleLive();
    });
  }

  const refreshAll = async (force = false) => { await refreshStatic(force); await refreshLive(force); };
  const invalidateAll = () => { try { Cache()?.invalidate(); } catch {} Object.assign(state, { cfg: null, staticData: null, liveData: null, lastKey: '' }); };

  (async function boot() {
    if (!$('#cw-settings-insight-style')) {
      const s = d.createElement('style');
      s.id = 'cw-settings-insight-style';
      s.textContent = css;
      d.head.appendChild(s);
    }
    for (let i = 0; !$('#page-settings') && i < 40; i += 1) await sleep(250);
    ensureCard();

    d.addEventListener('tab-changed', (e) => e?.detail?.id === 'settings' && setTimeout(() => refreshAll(true), 120));
    d.addEventListener('config-saved', () => { invalidateAll(); refreshAll(true); });
    w.addEventListener('auth-changed', () => { invalidateAll(); refreshAll(true); });
    d.addEventListener('scheduling-status-refresh', () => refreshLive(true));
    d.addEventListener('watcher-status-refresh', () => refreshLive(true));
    d.addEventListener('visibilitychange', () => !d.hidden && isVisible() && refreshLive(false));
    d.addEventListener('click', (e) => { const pane = e.target?.closest?.('.si-row[data-pane]')?.dataset?.pane; if (pane) w.cwSettingsSelect?.(pane); });
    w.addEventListener('focus', () => isVisible() && refreshLive(false));
    w.addEventListener('resize', syncHeight);
    w.addEventListener('scroll', syncHeight, { passive: true });

    if (isVisible()) refreshAll(true);
    w.refreshSettingsInsight = () => refreshAll(true);
  })();
})(window, document);
