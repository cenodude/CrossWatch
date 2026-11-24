/* assets/js/settings-insight.js */
/* Settings insight panel */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */

(function (w, d) {
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const TH = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.html?.(key, vars) ?? key;
  'use strict';
  if (w.__CW_SETTINGS_INSIGHT_STARTED__) return;
  w.__CW_SETTINGS_INSIGHT_STARTED__ = 1;

  const API = () => w.CW?.API || null;
  const Cache = () => w.CW?.Cache || null;
  const Meta = () => w.CW?.ProviderMeta || null;
  const $ = (s, r = d) => r.querySelector(s);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  const esc = (v) => String(v ?? '').replace(/[&<>"']/g, (m) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]));
  const has = (v) => typeof v === 'string' ? v.trim().length > 0 : !!v;
  const uniq = (arr) => [...new Set((Array.isArray(arr) ? arr : []).map((v) => String(v || '').trim().toLowerCase()).filter(Boolean))];
  const scheduleEnabled = (s) => !!(s?.enabled || s?.advanced?.enabled);
  const scheduleAdvancedEnabled = (s) => !!s?.advanced?.enabled;
  const activeEventTriggers = (s) => (((s?.advanced?.event_rules) || (s?.advanced?.eventRules) || []).filter((r) =>
    r && typeof r === 'object' &&
    r.active !== false &&
    String(r?.action?.kind || 'sync_pair') === 'sync_pair' &&
    String(r?.action?.pair_id || r?.action?.pairId || r?.pair_id || '').trim() &&
    String(r?.filters?.route_id || r?.filters?.routeId || '').trim()
  ).length);
  const activeCaptureSchedules = (s) => (((s?.advanced?.capture_jobs) || (s?.advanced?.captureJobs) || []).filter((r) =>
    r && typeof r === 'object' &&
    r.active !== false &&
    String(r?.provider || '').trim() &&
    String(r?.feature || '').trim() &&
    String(r?.at || '').trim()
  ).length);
  const isVisible = () => {
    const p = $('#page-settings');
    const pane = $('#cw-settings-overview') || $('#page-settings .cw-settings-pane[data-pane="overview"]');
    return !!(p && !p.classList.contains('hidden') && p.offsetParent !== null && pane && pane.classList.contains('active') && pane.offsetParent !== null);
  };
  const toLocal = (v) => {
    if (v === undefined || v === null || v === '') return '—';
    const n = Number(v);
    if (Number.isFinite(n) && n <= 0) return '—';
    const dt = new Date(Number.isFinite(n) ? (n < 1e10 ? n * 1000 : n) : v);
    return isNaN(+dt) ? '—' : dt.toLocaleString((w.CW?.I18n || w.__cwI18n)?.locale, { hour12: false });
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
    if (!$('.si-ops', aside)) aside.innerHTML = '<div class="si-ops"><div id="cw-si-scroll"><div class="si-body" id="cw-si-body"></div></div></div>';
    $('.si-ops-kicker', aside)?.remove?.();
    return aside;
  }

  const sep = (s = '•') => `<span class="si-sep">${s}</span>`;
  const wait = (ms = 0) => new Promise((r) => setTimeout(r, ms));

  function profileConfigured(provider, blk, cfg) {
    const p = String(provider || '').toLowerCase(), b = blk && typeof blk === 'object' ? blk : {};
    if (p === 'plex') return has(b.account_token) || has(b.token) || has(b.access_token);
    if (p === 'emby' || p === 'jellyfin') return has(b.access_token) || has(b.api_key) || has(b.token);
    if (p === 'trakt' || p === 'simkl' || p === 'wetrakr') return has(b.access_token) || has(b.refresh_token);
    if (p === 'anilist') return has(b.access_token) || has(b.token);
    if (p === 'mdblist') return has(b.api_key) || has(b.access_token);
    if (p === 'tracearr') return has(b.server_url || b.server) && has(b.api_key);
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
    const providers = (Meta()?.authProviders?.() || []).map(({ key }) => key.toLowerCase());
    const profiles = providers.map((provider) => ({ provider, count: countProfiles(cfg, provider) })).filter((x) => x.count);
    return { configured: profiles.length, profiles };
  }

  const getWhitelistSummary = (cfg) => {
    const txt = JSON.stringify(cfg || ''), active = (txt.match(/"whitelist"\s*:/g) || []).length + (txt.match(/"whitelisting"\s*:/g) || []).length;
    return active ? { active } : null;
  };

  async function fetchJSON(url) {
    const api = API();
    if (api?.j) return api.j(url);
    const r = await fetch(url, { cache: 'no-store', credentials: 'same-origin' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  }

  function relTime(epoch) {
    const n = Number(epoch || 0);
    if (!n) return T("settings.insight.noRecent");
    if (typeof w.relTimeFromEpoch === 'function') return w.relTimeFromEpoch(n);
    const diff = Math.max(0, Math.floor(Date.now() / 1000) - n);
    if (diff < 60) return T("settings.insight.justNow");
    if (diff < 3600) return T('settings.insight.minutesAgo', {count: Math.floor(diff / 60)});
    if (diff < 86400) return T('settings.insight.hoursAgo', {count: Math.floor(diff / 3600)});
    return T('settings.insight.daysAgo', {count: Math.floor(diff / 86400)});
  }

  function dateTimeCompact(v) {
    if (v === undefined || v === null || v === '') return T("settings.insight.notScheduled");
    const n = Number(v);
    if (Number.isFinite(n) && n <= 0) return T("settings.insight.notScheduled");
    const dt = new Date(Number.isFinite(n) ? (n < 1e10 ? n * 1000 : n) : v);
    return isNaN(+dt) ? T("settings.insight.notScheduled") : dt.toLocaleString((w.CW?.I18n || w.__cwI18n)?.locale, {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false
    }).replace(',', '');
  }

  const plural = (n, one, many = `${one}s`) => `${Number(n || 0)} ${Number(n || 0) === 1 ? one : many}`;

  function fact(icon, text, good = false) {
    return `<div class="si-fact ${good ? 'is-good' : ''}"><span class="material-symbols-rounded">${icon}</span><span>${esc(text)}</span></div>`;
  }

  function operationsHTML(data) {
    const providerCount = Number(data?.auth?.configured || 0);
    const pairTotal = Number(data?.pairs?.total ?? data?.pairs?.count ?? 0);
    const pairActive = Number(data?.pairs?.enabled ?? data?.pairs?.active ?? data?.pairs?.count ?? 0);
    const pairDisabled = Number(data?.pairs?.disabled ?? Math.max(0, pairTotal - pairActive));
    const metaReady = !!data?.meta?.configured;
    const scrob = data?.scrob || {};
    const sched = data?.sched || {};
    const lastEpoch = Math.max(0, Math.floor(Number(data?.last_sync_at || 0))) || 0;
    const watcherOn = !!(scrob.enabled && scrob.sources?.watcher);
    const webhookOn = !!(scrob.enabled && scrob.sources?.webhook);
    const watcherKey = watcherOn ? (scrob.watcher?.alive ? 'listening' : 'configured') : 'inactive';
    const webhookKey = webhookOn ? 'active' : 'inactive';
    const watcherState = T(`settings.insight.state.${watcherKey}`);
    const webhookState = T(`settings.insight.state.${webhookKey}`);
    const watcherStatusClass = watcherKey === 'listening' ? 'is-good' : 'is-inactive is-warn';
    const webhookStatusClass = webhookKey === 'active' ? 'is-good' : 'is-inactive is-warn';
    const pairHealthText = pairTotal
      ? `${T('settings.insight.pairsActive', {count: pairActive})}${pairDisabled ? `, ${T('settings.insight.disabledN', {n: pairDisabled})}` : ''}`
      : T("settings.insight.noPairsConfigured");
    const nextPairText = pairTotal
      ? (pairDisabled ? T('settings.insight.activeOf', {active: pairActive, total: pairTotal}) : T('settings.insight.activePairs', {count: pairActive}))
      : T("settings.insight.noPairs");
    const healthy = providerCount > 0 && metaReady && (pairActive > 0 || scrob.enabled || sched.enabled);
    const ready = {
      auth: providerCount > 0,
      meta: metaReady,
      sync: pairActive > 0,
      scheduling: !!(scrob.enabled || sched.enabled)
    };
    const next = !ready.auth
      ? { target: 'auth', title: T("settings.insight.connect"), copy: T("settings.insight.connectSub") }
      : !ready.meta
        ? { target: 'meta', title: T("settings.insight.meta"), copy: T("settings.insight.metaSub") }
        : !ready.sync
          ? { target: 'sync', title: T("settings.insight.pairs"), copy: T("settings.insight.pairsSub") }
          : !ready.scheduling
            ? { target: 'scheduling', title: T("settings.insight.automation"), copy: T("settings.insight.automationSub") }
            : { target: 'scheduling', title: dateTimeCompact(sched.nextRun), copy: nextPairText };
    return `
      <section class="si-op-card si-health ${healthy ? 'is-health' : 'is-health is-warn'}">
        <div class="si-op-head"><span class="material-symbols-rounded">${healthy ? 'verified_user' : 'shield'}</span><strong>${TH("settings.insight.health")}</strong></div>
        <div class="si-health-main"><span class="si-health-check"><span class="material-symbols-rounded">${healthy ? 'check' : 'priority_high'}</span></span><div class="si-health-title">${healthy ? T("settings.insight.operational") : T("settings.insight.attention")}</div></div>
        <div class="si-facts">
          ${fact('groups', T('settings.insight.providersConnected', {count: providerCount}), providerCount > 0)}
          ${fact('link', pairHealthText, pairActive > 0)}
          ${fact('description', metaReady ? T("settings.insight.metaReady") : T("settings.insight.metaNotReady"), metaReady)}
          ${fact('schedule', lastEpoch ? T('settings.insight.lastSync', {when: relTime(lastEpoch)}) : T('settings.insight.lastSyncNone'), !!lastEpoch)}
        </div>
      </section>
      <section class="si-op-card">
        <div class="si-op-head"><span class="material-symbols-rounded">rocket_launch</span><strong>${TH("settings.insight.nextAction")}</strong></div>
        <button type="button" class="si-next-main" data-target="${esc(next.target)}">
          <span class="si-next-icon"><span class="material-symbols-rounded">${healthy ? 'calendar_month' : 'check_circle'}</span></span>
          <span><span class="si-next-label">${healthy ? T("settings.insight.nextSync") : T("settings.insight.complete")}</span><strong class="si-next-time">${esc(next.title)}</strong><small class="si-next-sub">${esc(next.copy)}</small></span>
          <span class="material-symbols-rounded si-next-chev" aria-hidden="true">chevron_right</span>
        </button>
        <div class="si-mini-grid">
          <div class="si-mini-status ${watcherStatusClass}"><span class="material-symbols-rounded">sensors</span><span class="si-mini-copy"><strong>${TH("settings.insight.watcher")}</strong><small>${esc(watcherState)}</small></span></div>
          <div class="si-mini-status ${webhookStatusClass}"><span class="material-symbols-rounded">rss_feed</span><span class="si-mini-copy"><strong>${TH("settings.insight.webhook")}</strong><small>${esc(webhookState)}</small></span></div>
        </div>
        <button type="button" class="si-action" data-target="scheduling">${TH("settings.insight.viewSchedule")} <span class="material-symbols-rounded">arrow_forward</span></button>
      </section>
    `;
  }

  async function openPaneSection(pane, sectionId) {
    w.cwSettingsSelect?.(pane);
    await wait(60);
    w.openSection?.(sectionId);
    await wait(0);
    d.getElementById(sectionId)?.scrollIntoView?.({ behavior: 'smooth', block: 'start' });
  }

  async function openScrobblerSection(mode) {
    const scrob = state.liveData?.scrob || {};
    const sources = scrob.sources || {};
    const isWatch = sources.watcher && !sources.webhook ? true : String(mode || '').toLowerCase() === 'watch';
    const sectionId = isWatch ? 'sc-sec-watch' : 'sc-sec-webhook';
    await openPaneSection('scrobbler', sectionId);
    for (let i = 0; !d.getElementById(sectionId) && i < 40; i += 1) await wait(50);
    d.getElementById(sectionId)?.scrollIntoView?.({ behavior: 'smooth', block: 'start' });
  }

  async function openTmdbMetadataModal() {
    w.cwSettingsSelect?.('providers');
    for (let i = 0; String(d.querySelector('#page-settings .cw-settings-pane.active')?.dataset?.pane || '').toLowerCase() !== 'providers' && i < 20; i += 1) await wait(50);
    for (let i = 0; (!w.CW?.ProvidersUI?.openMetadataProviderForm || !w.CW?.ProvidersUI?.ensureProvidersPaneReady || !d.getElementById('auth-providers')) && i < 20; i += 1) await wait(50);
    if (w.CW?.ProvidersUI?.ensureProvidersPaneReady) {
      await w.CW.ProvidersUI.ensureProvidersPaneReady(false);
      await wait(0);
    }
    if (w.CW?.ProvidersUI?.openMetadataProviderForm && d.getElementById('auth-providers')) return w.CW.ProvidersUI.openMetadataProviderForm('TMDB_METADATA');
    return w.openAddMetadata?.();
  }

  async function openAddProviderModal() {
    w.cwSettingsSelect?.('providers');
    for (let i = 0; String(d.querySelector('#page-settings .cw-settings-pane.active')?.dataset?.pane || '').toLowerCase() !== 'providers' && i < 20; i += 1) await wait(50);
    for (let i = 0; (!w.CW?.ProvidersUI?.openAddConnection || !w.CW?.ProvidersUI?.ensureProvidersPaneReady || !d.getElementById('auth-providers')) && i < 20; i += 1) await wait(50);
    if (w.CW?.ProvidersUI?.ensureProvidersPaneReady) {
      await w.CW.ProvidersUI.ensureProvidersPaneReady(false);
      await wait(0);
    }
    if (w.CW?.ProvidersUI?.openAddConnection && d.getElementById('auth-providers')) return w.CW.ProvidersUI.openAddConnection();
    if (typeof w.openAddConnection === 'function') return w.openAddConnection();
    return openPaneSection('providers', 'sec-auth');
  }

  async function handleRowOpen(target) {
    const key = String(target || '').toLowerCase();
    if (key === 'auth') return openAddProviderModal();
    if (key === 'sync') return openPaneSection('sync', 'sec-sync');
    if (key === 'meta') return openTmdbMetadataModal();
    if (key === 'scheduling') return openPaneSection('scheduling', 'sec-scheduling');
    if (key === 'scrobbler') return openScrobblerSection(state.liveData?.scrob?.mode);
    if (key) w.cwSettingsSelect?.(key);
  }

  const STALE_MS = 30000;
  const state = { data: null, liveData: null, liveTimer: null, localTimer: null, busy: false, queued: false, queuedForce: false, dirty: false, lastKey: '', lastFetch: 0 };
  const syncHeight = () => { const el = $('#cw-si-scroll'); if (el) el.style.maxHeight = `${Math.max(260, w.innerHeight - 220)}px`; };

  function render(data) {
    const body = $('#cw-si-body'), key = JSON.stringify(data || {});
    if (!body || key === state.lastKey) return;
    state.lastKey = key;
    try { d.dispatchEvent(new CustomEvent('cw-settings-overview-data', { detail: { data } })); } catch {}
    body.innerHTML = operationsHTML(data);
    syncHeight();
  }

  function normalizeOverview(raw) {
    const data = raw && typeof raw === 'object' ? raw : {};
    const meta = data.meta || data.metadata || {};
    const sched = data.sched || data.scheduling || {};
    const scrob = data.scrob || data.scrobbler || {};
    return { ...data, meta, metadata: meta, sched, scheduling: sched, scrob, scrobbler: scrob };
  }

  function applyOverview(data, fetched = false) {
    const overview = normalizeOverview(data);
    state.data = overview;
    state.liveData = { sched: overview.sched, scrob: overview.scrob };
    if (fetched) {
      state.lastFetch = Date.now();
      state.dirty = false;
    }
    render(overview);
    scheduleLive();
  }

  function automationActive(data = state.data) {
    const overview = normalizeOverview(data || {});
    return !!(overview.sched?.enabled || overview.scrob?.enabled);
  }

  function isStale() {
    return !state.lastFetch || Date.now() - state.lastFetch >= STALE_MS;
  }

  function scheduleLive() {
    clearTimeout(state.liveTimer);
    state.liveTimer = null;
    if (d.hidden || !isVisible() || !automationActive()) return;
    state.liveTimer = setTimeout(() => refreshAll(false), 30000);
  }

  async function readOverview(force = false) {
    const api = API();
    if (api?.Settings?.overview) return api.Settings.overview(force);
    return fetchJSON(`/api/settings/overview${force ? `?t=${Date.now()}` : ''}`);
  }

  async function refreshAll(force = false) {
    if (d.hidden || !isVisible()) {
      clearTimeout(state.liveTimer);
      state.liveTimer = null;
      return;
    }
    if (!force && !state.dirty && !automationActive() && !isStale()) {
      scheduleLive();
      return;
    }
    if (state.busy) {
      state.queued = true;
      state.queuedForce = state.queuedForce || force;
      return;
    }
    state.busy = true;
    try {
      ensureCard();
      applyOverview(await readOverview(force), true);
    } catch {
      scheduleLive();
    } finally {
      state.busy = false;
      if (state.queued) {
        const nextForce = state.queuedForce;
        state.queued = false;
        state.queuedForce = false;
        setTimeout(() => refreshAll(nextForce), 0);
      }
    }
  }

  function localPairsSummary(cfg) {
    const list = Array.isArray(cfg?.pairs) ? cfg.pairs : Array.isArray(cfg?.connections) ? cfg.connections : [];
    const total = list.length;
    const enabled = list.filter((p) => !p || typeof p !== 'object' || p.enabled !== false).length;
    return { count: total, total, enabled, active: enabled, disabled: Math.max(0, total - enabled) };
  }

  function localSchedulingSummary(cfg) {
    const scfg = cfg?.scheduling || {};
    const advanced = scheduleAdvancedEnabled(scfg);
    const rawNext = scfg?.next_run_at ?? scfg?.next_run ?? null;
    const n = Number(rawNext);
    const nextRun = Number.isFinite(n) && n <= 0 ? null : rawNext;
    return {
      ...(state.data?.sched || {}),
      enabled: scheduleEnabled(scfg),
      advanced,
      nextRun,
      next_run_at: nextRun || 0,
      eventTriggers: advanced ? activeEventTriggers(scfg) : 0,
      captureSchedules: advanced ? activeCaptureSchedules(scfg) : 0
    };
  }

  function localScrobblerSummary(cfg) {
    const sc = cfg?.scrobble || {}, enabled = !!sc.enabled, mode = String(sc.mode || 'webhook').toLowerCase();
    const rawSources = sc?.sources && typeof sc.sources === 'object' ? sc.sources : null;
    const sources = rawSources
      ? { webhook: !!rawSources.webhook, watcher: !!(rawSources.watcher ?? rawSources.watch) }
      : { webhook: mode === 'webhook', watcher: mode === 'watch' };
    const routes = Array.isArray(sc?.watch?.routes) ? sc.watch.routes : [];
    const prev = state.data?.scrob || {};
    return {
      ...prev,
      enabled,
      mode: enabled ? mode : '',
      sources,
      watcher: enabled && sources.watcher ? (prev.watcher || { alive: false }) : { alive: false },
      providers: enabled ? uniq(routes.map((r) => r?.provider)) : [],
      sinks: enabled ? uniq(routes.map((r) => r?.sink)) : []
    };
  }

  function applyLocalSnapshot() {
    const cfg = Cache()?.getCfg?.();
    if (!cfg || typeof cfg !== 'object' || !state.data) return;
    applyOverview({
      ...state.data,
      auth: authSummary(cfg),
      pairs: localPairsSummary(cfg),
      sched: localSchedulingSummary(cfg),
      scrob: localScrobblerSummary(cfg),
      whitelist: getWhitelistSummary(cfg)
    }, false);
  }

  function queueLocalRefresh(force = true) {
    state.dirty = true;
    applyLocalSnapshot();
    clearTimeout(state.localTimer);
    state.localTimer = setTimeout(() => {
      try { Cache()?.invalidate?.('settingsOverview'); } catch {}
      refreshAll(force);
    }, 180);
  }

  function visibleRefresh(force = false) {
    if (d.hidden || !isVisible()) {
      clearTimeout(state.liveTimer);
      state.liveTimer = null;
      return;
    }
    if (force || state.dirty || automationActive() || isStale()) refreshAll(force || state.dirty);
    else scheduleLive();
  }

  function refreshWhenVisible(force = false, tries = 10) {
    if (d.hidden) return;
    if (isVisible()) return void refreshAll(force);
    if (tries > 0) setTimeout(() => refreshWhenVisible(force, tries - 1), 80);
  }

  function invalidateAll() {
    try { Cache()?.invalidate?.('settingsOverview'); } catch {}
    state.dirty = true;
    state.lastKey = '';
  }

  w.SettingsInsight = { refresh: () => refreshAll(true) };

  (async function boot() {
    for (let i = 0; !$('#page-settings') && i < 40; i += 1) await sleep(250);
    ensureCard();

    d.addEventListener('tab-changed', (e) => {
      if (e?.detail?.id === 'settings') setTimeout(() => refreshWhenVisible(true), 120);
      else {
        clearTimeout(state.liveTimer);
        state.liveTimer = null;
      }
    });
    d.addEventListener('cw-settings-pane-changed', (e) => {
      if (String(e?.detail?.pane || '').toLowerCase() === 'overview') setTimeout(() => refreshWhenVisible(true), 80);
      else {
        clearTimeout(state.liveTimer);
        state.liveTimer = null;
      }
    });
    d.addEventListener('config-saved', () => { invalidateAll(); queueLocalRefresh(true); }, true);
    w.addEventListener('auth-changed', () => { invalidateAll(); queueLocalRefresh(true); }, true);
    w.addEventListener('settings-changed', () => { invalidateAll(); queueLocalRefresh(true); }, true);
    w.addEventListener('cx:pairs:changed', () => { invalidateAll(); queueLocalRefresh(true); }, true);
    d.addEventListener('cw-provider-connected', () => { invalidateAll(); queueLocalRefresh(true); }, true);
    d.addEventListener('scheduling-status-refresh', () => queueLocalRefresh(true), true);
    d.addEventListener('watcher-status-refresh', () => queueLocalRefresh(true), true);
    d.addEventListener('visibilitychange', () => visibleRefresh(false));
    d.addEventListener('click', (e) => {
      const action = e.target?.closest?.('.si-action[data-target],.si-next-main[data-target]');
      if (action) return void handleRowOpen(action.dataset.target);
      const row = e.target?.closest?.('.si-row[data-pane]');
      const target = row?.dataset?.target;
      if (target) return void handleRowOpen(target);
      const pane = row?.dataset?.pane;
      if (pane) w.cwSettingsSelect?.(pane);
    });
    w.addEventListener('focus', () => visibleRefresh(false));
    w.addEventListener('resize', syncHeight);
    w.addEventListener('scroll', syncHeight, { passive: true });

    if (isVisible()) refreshAll(true);
    w.refreshSettingsInsight = () => refreshAll(true);
  })();
})(window, document);
