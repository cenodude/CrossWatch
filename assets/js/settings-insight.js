/* assets/js/settings-insight.js */
/* refactored */
/* Settings insight panel */
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
  const PROVIDERS = ['plex', 'emby', 'jellyfin', 'trakt', 'simkl', 'mdblist', 'publicmetadb', 'anilist', 'tmdb', 'tautulli'];
  const EMPTY = {
    auth: '<div class="si-empty"><div class="h1">No authentication providers</div><p class="p">Configure at least one authentication provider to get started. To sync, you need at least two sides in play.</p></div>',
    pairs: '<div class="si-empty"><div class="h1">No synchronization pairs or scrobbler</div><p class="p">Authentication looks good. Next step: add a sync pair or enable the scrobbler.</p></div>'
  };
  const css = `#cw-settings-insight{display:block;min-width:0;--si-bg:linear-gradient(180deg,rgba(9,12,18,.96),rgba(4,6,10,.98));--si-panel:linear-gradient(180deg,rgba(12,15,22,.94),rgba(6,8,12,.97));--si-panel-hover:linear-gradient(180deg,rgba(15,18,26,.96),rgba(8,10,15,.98));--si-border:rgba(255,255,255,.075);--si-border-strong:rgba(255,255,255,.12);--si-shadow:0 22px 44px rgba(0,0,0,.40),inset 0 1px 0 rgba(255,255,255,.03);--si-fg:#f3f5ff;--si-soft:rgba(196,204,223,.74)}.si-card{position:relative;border:1px solid var(--si-border);border-radius:22px;overflow:hidden;background:radial-gradient(125% 140% at 0% 0%,rgba(84,92,132,.08),transparent 36%),radial-gradient(120% 140% at 100% 100%,rgba(50,56,84,.06),transparent 44%),var(--si-bg);box-shadow:var(--si-shadow);backdrop-filter:blur(16px) saturate(118%);-webkit-backdrop-filter:blur(16px) saturate(118%)}.si-card::before{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(180deg,rgba(255,255,255,.024),transparent 22%,rgba(255,255,255,.01) 100%)}.si-header{position:relative;padding:14px 16px 12px;border-bottom:1px solid rgba(255,255,255,.055)}.si-header-kicker{display:block;font-size:10px;letter-spacing:.14em;text-transform:uppercase;color:var(--si-soft);line-height:1.2}#cw-si-scroll{overflow:auto;overscroll-behavior:contain}.si-body{padding:10px;display:grid;gap:8px}.si-row{position:relative;display:grid;grid-template-columns:18px minmax(0,1fr);gap:10px;align-items:start;padding:12px 12px 11px;border-radius:16px;border:1px solid var(--si-border);background:var(--si-panel);box-shadow:0 10px 20px rgba(0,0,0,.16);cursor:pointer;transition:transform .14s ease,border-color .14s ease,background .14s ease,box-shadow .14s ease}.si-row::before{content:"";position:absolute;inset:0;pointer-events:none;border-radius:inherit;background:linear-gradient(135deg,rgba(255,255,255,.03),transparent 56%);opacity:.78}.si-row:hover{transform:translateY(-1px);border-color:var(--si-border-strong);background:var(--si-panel-hover);box-shadow:0 14px 24px rgba(0,0,0,.22)}.si-ic{display:flex;align-items:center;justify-content:center;min-height:18px}.si-ic .material-symbols-rounded{font-size:18px;color:rgba(230,235,248,.84)}.si-col{min-width:0}.si-h{margin:0 0 5px;color:var(--si-fg);font-weight:800;font-size:13px;line-height:1.18}.si-one{color:var(--si-soft);font-size:11px;line-height:1.42}.si-one b,.si-one strong{color:var(--si-fg)}.si-stack{display:grid;gap:4px}.si-line{display:flex;align-items:center;gap:7px;flex-wrap:wrap}.si-sep{color:rgba(196,204,223,.38);font-weight:800}.si-status{color:var(--si-fg);font-weight:700}.si-text,.si-inline-text{display:inline-flex;align-items:center}.si-to{color:rgba(196,204,223,.66);font-weight:700}.si-pchips,.si-inline-logos{display:flex;flex-wrap:wrap;gap:7px;align-items:center}.si-pchip{display:inline-flex;align-items:center;gap:7px;padding:5px 8px;border-radius:999px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.07);font-size:11px;font-weight:800;color:#e7ecfb;box-shadow:inset 0 1px 0 rgba(255,255,255,.02)}.si-count{display:inline-flex;align-items:center;justify-content:center;min-width:17px;height:17px;padding:0 5px;border-radius:999px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.07);font-size:10px;line-height:1}.si-logo{height:17px;width:auto;display:block;opacity:.95;flex:0 0 auto;filter:saturate(.92) brightness(.98)}.si-empty{padding:18px 16px;color:var(--si-soft)}.si-empty .h1{font-size:15px;font-weight:800;color:#e7ecfb;margin-bottom:8px}.si-empty .p{font-size:12px;line-height:1.5;margin:0 0 10px}`;
  const opsCss = `#cw-settings-insight{--si-ops-bg:#0d131d;--si-ops-panel:#101722;--si-ops-panel-2:#121a26;--si-ops-border:rgba(111,132,170,.18);--si-ops-border-strong:rgba(84,148,255,.34);--si-ops-text:#e7edf9;--si-ops-muted:#95a1b5;--si-ops-blue:#4e92ff;--si-ops-green:#55d889;--si-ops-cyan:#4ed8c6;--si-ops-purple:#b66cff;align-self:start;min-width:0}.si-ops{display:grid;gap:11px}.si-ops-kicker{margin:7px 0 5px 0;color:#7f899c;font-size:11px;font-weight:800;letter-spacing:.17em;text-transform:uppercase}.si-op-card{position:relative;overflow:hidden;border:1px solid var(--si-ops-border);border-radius:7px;background:linear-gradient(180deg,rgba(17,25,36,.94),rgba(11,17,26,.96));box-shadow:inset 0 1px 0 rgba(255,255,255,.025);padding:14px}.si-op-card::before{content:"";position:absolute;inset:0;pointer-events:none;background:radial-gradient(500px 160px at 0 0,rgba(72,140,255,.09),transparent 58%);opacity:.8}.si-op-card>*{position:relative}.si-op-card.is-health{border-color:rgba(74,213,132,.42);background:linear-gradient(180deg,rgba(13,29,29,.94),rgba(10,18,26,.96))}.si-op-card.is-health::before{background:radial-gradient(420px 180px at 0 0,rgba(72,213,132,.12),transparent 58%)}.si-op-head{display:flex;align-items:center;gap:10px;margin-bottom:14px}.si-op-head .material-symbols-rounded{font-size:22px;line-height:1;color:#b9c6dc;font-variation-settings:"FILL"0,"wght"500,"GRAD"0,"opsz"24}.si-op-head strong{color:var(--si-ops-text);font-size:16px;line-height:1.15}.si-health .si-op-head .material-symbols-rounded,.si-good{color:var(--si-ops-green)!important;-webkit-text-fill-color:var(--si-ops-green)!important}.si-health-main{display:grid;grid-template-columns:52px minmax(0,1fr);gap:14px;align-items:center;margin:6px 0 16px}.si-health-check{display:grid;place-items:center;width:52px;height:52px;border-radius:999px;background:rgba(69,205,126,.15);color:var(--si-ops-green)}.si-health-check .material-symbols-rounded{font-size:35px;font-variation-settings:"FILL"0,"wght"500,"GRAD"0,"opsz"36}.si-health-title{color:var(--si-ops-green);font-size:18px;font-weight:850;line-height:1.2}.si-facts{display:grid;gap:10px}.si-fact{display:grid;grid-template-columns:22px minmax(0,1fr);gap:10px;align-items:center;color:#aeb9cc;font-size:14px;line-height:1.25}.si-fact .material-symbols-rounded{font-size:19px;color:#9eacc2;font-variation-settings:"FILL"0,"wght"450,"GRAD"0,"opsz"20}.si-fact.is-good .material-symbols-rounded{color:var(--si-ops-green)}.si-next-main{appearance:none;font:inherit;display:grid;grid-template-columns:42px minmax(0,1fr) 22px;gap:12px;align-items:center;width:100%;padding:12px;border:1px solid rgba(122,142,176,.13);border-radius:7px;background:rgba(9,15,24,.42);color:inherit;text-align:left;cursor:pointer}.si-next-main:hover{border-color:var(--si-ops-border-strong);background:rgba(14,22,34,.56)}.si-next-icon{display:grid;place-items:center;width:36px;height:36px;border-radius:999px;border:1px solid rgba(58,107,220,.28);background:linear-gradient(180deg,rgba(46,82,172,.30),rgba(22,42,92,.28));color:var(--si-ops-green)}.si-next-icon .material-symbols-rounded{font-size:23px}.si-next-label{font-size:13px;color:#aab5c8;line-height:1.2}.si-next-time{display:block;margin-top:4px;color:#e7edf9;font-size:14px;font-weight:850;line-height:1.2}.si-next-sub{display:block;margin-top:4px;color:#9ba6b9;font-size:12px}.si-next-chev{justify-self:end;color:#9ba6b9}.si-mini-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:10px}.si-mini-status{display:grid;grid-template-columns:18px minmax(0,1fr);gap:9px;align-items:center;min-height:58px;padding:10px 12px;border:1px solid rgba(122,142,176,.12);border-radius:7px;background:rgba(9,15,24,.32)}.si-mini-status .material-symbols-rounded{font-size:20px;color:#3bd98d}.si-mini-copy strong{display:block;color:#dbe4f2;font-size:13px;line-height:1.2}.si-mini-copy small{display:flex;align-items:center;gap:7px;margin-top:7px;color:#62df91;font-size:12px;font-weight:800}.si-mini-copy small::before{content:"";width:7px;height:7px;border-radius:50%;background:#62df91}.si-action{display:flex;align-items:center;justify-content:center;gap:8px;width:100%;min-height:34px;margin-top:9px;border:0;background:transparent;color:var(--si-ops-blue);font-size:14px;font-weight:800;cursor:pointer}.si-action .material-symbols-rounded{font-size:19px}.si-action:hover{color:#75abff}.si-activity-list{display:grid;gap:0;border:1px solid rgba(122,142,176,.12);border-radius:7px;overflow:hidden;background:rgba(9,15,24,.28)}.si-activity-row{display:grid;grid-template-columns:42px minmax(0,1fr);gap:11px;align-items:center;min-height:54px;padding:8px 10px;border-bottom:1px solid rgba(122,142,176,.10)}.si-activity-row:last-child{border-bottom:0}.si-activity-icon{display:grid;place-items:center;width:36px;height:36px;border-radius:8px;background:rgba(56,118,255,.16);color:#5d94ff}.si-activity-icon.is-watch{background:rgba(177,84,255,.14);color:#c875ff}.si-activity-icon.is-meta{background:rgba(52,119,255,.16);color:#5e9aff}.si-activity-icon.is-sync{background:rgba(55,205,134,.13);color:#56d889}.si-activity-icon .material-symbols-rounded{font-size:23px}.si-activity-main{min-width:0}.si-activity-main strong{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#cfd8e8;font-size:12px;line-height:1.2}.si-activity-main small{display:block;margin-top:3px;color:#8f9bae;font-size:11px;line-height:1.2}.si-muted{color:#8793a6}.si-empty.is-ops{padding:10px 2px;color:#8995a8;font-size:13px;line-height:1.4}.si-health.is-warn{border-color:rgba(231,175,78,.38)}.si-health.is-warn .si-health-title{color:#f0c56f}.si-health.is-warn .si-health-check{background:rgba(231,175,78,.14);color:#f0c56f}@media(max-width:1320px){#cw-settings-insight{position:static!important}.si-ops{max-width:none}}@media(max-width:760px){.si-mini-grid{grid-template-columns:1fr}.si-op-card{padding:13px}.si-next-main{grid-template-columns:38px minmax(0,1fr) 20px}.si-next-icon{width:34px;height:34px}.si-health-main{grid-template-columns:46px minmax(0,1fr)}.si-health-check{width:46px;height:46px}}`;
  const opsLayoutCss = `.si-ops #cw-si-scroll{overflow:visible}.si-ops .si-body{padding:0;gap:11px}`;
  const flatOpsCss = `html[data-cw-theme="flat-dark"] #page-settings #cw-settings-insight{--si-ops-border:rgba(255,255,255,.13);--si-ops-blue:#5f8dff;--si-ops-green:#57b58a}html[data-cw-theme="flat-dark"] #page-settings .si-op-card{background:#121820!important;background-image:none!important;border-color:rgba(255,255,255,.13)!important;box-shadow:none!important}html[data-cw-theme="flat-dark"] #page-settings .si-op-card::before{content:none!important;display:none!important}html[data-cw-theme="flat-dark"] #page-settings .si-op-card.is-health{background:#0f1c1a!important;border-color:rgba(87,181,138,.42)!important}html[data-cw-theme="flat-dark"] #page-settings .si-health-check{background:#183529!important;background-image:none!important;box-shadow:none!important}html[data-cw-theme="flat-dark"] #page-settings :is(.si-next-main,.si-mini-status,.si-activity-list){background:#0f141d!important;background-image:none!important;border-color:rgba(255,255,255,.10)!important;box-shadow:none!important}html[data-cw-theme="flat-dark"] #page-settings .si-activity-row{border-bottom-color:rgba(255,255,255,.08)!important}html[data-cw-theme="flat-dark"] #page-settings :is(.si-next-icon,.si-activity-icon){background:#18213a!important;background-image:none!important;border-color:rgba(95,141,255,.34)!important;box-shadow:none!important}html[data-cw-theme="flat-dark"] #page-settings .si-activity-icon.is-watch{background:#241b38!important}html[data-cw-theme="flat-dark"] #page-settings .si-activity-icon.is-sync{background:#173225!important}html[data-cw-theme="flat-dark"] #page-settings .si-action{color:#5f8dff!important}html[data-cw-theme="flat-dark"] #page-settings .si-mini-copy small::before{box-shadow:none!important}`;

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
    if (!$('.si-ops', aside)) aside.innerHTML = '<div class="si-ops"><div id="cw-si-scroll"><div class="si-body" id="cw-si-body"></div></div></div>';
    $('.si-ops-kicker', aside)?.remove?.();
    return aside;
  }

  function providerMeta(provider) {
    const raw = String(provider || '').trim(), key = raw.toUpperCase(), meta = Meta();
    return { key, label: meta?.label?.(key) || meta?.label?.(raw) || key || '?', logo: meta?.logoPath?.(key) || meta?.logoPath?.(raw) || '' };
  }

  function providerIcon(provider) {
    const meta = providerMeta(provider), src = meta.logo;
    return src ? `<img loading="lazy" class="si-logo" src="${esc(src)}" alt="${esc(meta.label)}" title="${esc(meta.label)}">` : `<span class="si-inline-text">${esc(meta.label)}</span>`;
  }

  const logosHTML = (list) => (list = uniq(list)).length ? `<span class="si-inline-logos">${list.map(providerIcon).join('')}</span>` : '';
  const badgeHTML = (provider, count) => `<span class="si-pchip" title="${esc(providerMeta(provider).label)}: ${count}">${providerIcon(provider)}<span class="si-count">${count}</span></span>`;
  const line = (...items) => `<div class="si-line">${items.filter(Boolean).join('')}</div>`;
  const stack = (...rows) => `<div class="si-stack">${rows.filter(Boolean).join('')}</div>`;
  const kv = (k, v) => `<span class="si-status">${k}</span><span class="si-text">${v}</span>`;
  const sep = (s = '•') => `<span class="si-sep">${s}</span>`;
  const wait = (ms = 0) => new Promise((r) => setTimeout(r, ms));

  function profileConfigured(provider, blk, cfg) {
    const p = String(provider || '').toLowerCase(), b = blk && typeof blk === 'object' ? blk : {};
    if (p === 'plex') return has(b.account_token) || has(b.token) || has(b.access_token);
    if (p === 'emby' || p === 'jellyfin') return has(b.access_token) || has(b.api_key) || has(b.token);
    if (p === 'trakt' || p === 'simkl') return has(b.access_token) || has(b.refresh_token);
    if (p === 'anilist') return has(b.access_token) || has(b.token);
    if (p === 'mdblist') return has(b.api_key) || has(b.access_token);
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
    const summarize = (raw) => {
      const list = Array.isArray(raw) ? raw : [];
      const total = list.length;
      const enabled = list.filter((p) => !p || typeof p !== 'object' || p.enabled !== false).length;
      return { count: total, total, enabled, active: enabled, disabled: Math.max(0, total - enabled) };
    };
    try {
      const list = await API()?.Pairs?.list(force);
      return summarize(list);
    } catch {
      const list = Array.isArray(cfg?.pairs) ? cfg.pairs : Array.isArray(cfg?.connections) ? cfg.connections : [];
      return summarize(list);
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
      const advanced = scheduleAdvancedEnabled(sc);
      return {
        enabled: scheduleEnabled(sc),
        advanced,
        running: !!st?.running,
        nextRun: st?.next_run_at ?? st?.next_run ?? sc?.next_run_at ?? sc?.next_run ?? null,
        eventTriggers: advanced ? activeEventTriggers(sc) : 0,
        captureSchedules: advanced ? activeCaptureSchedules(sc) : 0
      };
    } catch {
      const advanced = scheduleAdvancedEnabled(fallback);
      return {
        enabled: scheduleEnabled(fallback),
        advanced,
        running: false,
        nextRun: fallback?.next_run_at ?? fallback?.next_run ?? null,
        eventTriggers: advanced ? activeEventTriggers(fallback) : 0,
        captureSchedules: advanced ? activeCaptureSchedules(fallback) : 0
      };
    }
  }

  async function getScrobblerSummary(cfg, force) {
    const sc = cfg?.scrobble || {}, enabled = !!sc.enabled, mode = String(sc.mode || 'webhook').toLowerCase();
    const rawSources = sc?.sources && typeof sc.sources === 'object' ? sc.sources : null;
    const sources = rawSources
      ? { webhook: !!rawSources.webhook, watcher: !!(rawSources.watcher ?? rawSources.watch) }
      : { webhook: mode === 'webhook', watcher: mode === 'watch' };
    const out = { enabled, mode: enabled ? mode : '', sources, watcher: { alive: false }, providers: [], sinks: [] };
    if (!enabled) return out;
    const routes = Array.isArray(sc?.watch?.routes) ? sc.watch.routes : [];
    out.providers = uniq(routes.map((r) => r?.provider));
    out.sinks = uniq(routes.map((r) => r?.sink));
    if (!sources.watcher) return out;
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
  const schedulingHTML = (sched) => !sched?.enabled
    ? 'Disabled'
    : line(
      `<span class="si-status">${sched.advanced ? 'Enabled (Advanced)' : 'Enabled'}</span>`,
      sched.advanced && typeof sched.eventTriggers === 'number' && `${sep('|')}<span class="si-text">Event triggers: ${sched.eventTriggers}</span>`,
      sched.advanced && typeof sched.captureSchedules === 'number' && `${sep('|')}<span class="si-text">Capture schedules: ${sched.captureSchedules}</span>`,
      sched.running && `${sep('|')}<span class="si-text">Running</span>`,
      sched.nextRun && `${sep('|')}<span class="si-text">Next run: ${esc(toLocal(sched.nextRun))}</span>`
    );
  function scrobblerHTML(scrob) {
    if (!scrob?.enabled) return 'Disabled';
    const sources = scrob.sources || { webhook: scrob.mode !== 'watch', watcher: scrob.mode === 'watch' };
    const route = [logosHTML(scrob.providers), logosHTML(scrob.sinks)].filter(Boolean);
    const routeHtml = route[0] || route[1]
      ? `${route[0] || ''}${route[0] && route[1] ? '<span class="si-to">to</span>' : ''}${route[1] || ''}`
      : '<span class="si-inline-text">No routes configured</span>';
    return stack(
      sources.webhook && line('<span class="si-text">Webhook</span>', '<span class="si-status">Listening</span>'),
      sources.watcher && line('<span class="si-text">Watcher</span>', `<span class="si-status">${scrob.watcher.alive ? 'Running' : 'Stopped'}</span>`, sep('|'), routeHtml)
    );
  }

  async function fetchJSON(url) {
    const api = API();
    if (api?.j) return api.j(url);
    const r = await fetch(url, { cache: 'no-store', credentials: 'same-origin' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  }

  async function getActivitySummary(force) {
    try {
      const data = await fetchJSON(`/api/activity/recent?limit=3${force ? `&t=${Date.now()}` : ''}`);
      return { items: Array.isArray(data?.items) ? data.items.slice(0, 3) : [] };
    } catch {
      return { items: [] };
    }
  }

  const epochOf = (item) => {
    const raw = item?.captured_at ?? item?.watched_at ?? item?.updated_at ?? 0;
    const n = Number(raw || 0);
    return Number.isFinite(n) && n > 0 ? (n > 1e10 ? Math.floor(n / 1000) : Math.floor(n)) : 0;
  };

  function relTime(epoch) {
    const n = Number(epoch || 0);
    if (!n) return 'No recent sync yet';
    if (typeof w.relTimeFromEpoch === 'function') return w.relTimeFromEpoch(n);
    const diff = Math.max(0, Math.floor(Date.now() / 1000) - n);
    if (diff < 60) return 'just now';
    if (diff < 3600) return `${Math.floor(diff / 60)} minutes ago`;
    if (diff < 86400) return `${Math.floor(diff / 3600)} hours ago`;
    return `${Math.floor(diff / 86400)} days ago`;
  }

  function dateTimeCompact(v) {
    if (v === undefined || v === null || v === '') return 'Not scheduled';
    const n = Number(v);
    const dt = new Date(Number.isFinite(n) && n > 0 ? (n < 1e10 ? n * 1000 : n) : v);
    return isNaN(+dt) ? 'Not scheduled' : dt.toLocaleString(undefined, {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false
    }).replace(',', '');
  }

  const plural = (n, one, many = `${one}s`) => `${Number(n || 0)} ${Number(n || 0) === 1 ? one : many}`;
  const providerLabel = (value) => providerMeta(value).label || String(value || '').trim().toUpperCase() || 'Provider';

  function activityIcon(item) {
    const method = String(item?.method || '').toLowerCase();
    const kind = String(item?.kind || '').toLowerCase();
    if (method === 'watcher' || method === 'webhook' || kind === 'scrobble') return { icon: method === 'webhook' ? 'rss_feed' : 'sensors', cls: 'is-watch' };
    if (kind.includes('meta') || String(item?.source || '').toLowerCase().includes('tmdb')) return { icon: 'database', cls: 'is-meta' };
    return { icon: 'sync', cls: 'is-sync' };
  }

  function activityTitle(item, index) {
    const method = String(item?.method || '').toLowerCase();
    const kind = String(item?.kind || '').toLowerCase();
    const source = providerLabel(item?.source || item?.target);
    if (method === 'watcher') return `Watcher event received, ${source}`;
    if (method === 'webhook') return `Webhook event received, ${source}`;
    if (kind.includes('meta')) return `Metadata lookup completed, ${source}`;
    if (item?.targets?.length) return `Sync completed, ${item.targets.length} routes processed`;
    if (index === 0) return 'Sync completed';
    return item?.title ? `Activity recorded, ${item.title}` : 'Activity recorded';
  }

  function activityRows(items) {
    const rows = Array.isArray(items) ? items : [];
    if (!rows.length) return '<div class="si-empty is-ops">No recent activity yet.</div>';
    return `<div class="si-activity-list">${rows.map((item, index) => {
      const ic = activityIcon(item);
      return `<div class="si-activity-row"><span class="si-activity-icon ${ic.cls}"><span class="material-symbols-rounded">${ic.icon}</span></span><span class="si-activity-main"><strong>${esc(activityTitle(item, index))}</strong><small>${esc(relTime(epochOf(item)))}</small></span></div>`;
    }).join('')}</div>`;
  }

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
    const items = data?.activity?.items || [];
    const lastEpoch = epochOf(items[0]);
    const watcherOn = !!(scrob.enabled && scrob.sources?.watcher);
    const webhookOn = !!(scrob.enabled && scrob.sources?.webhook);
    const watcherState = watcherOn ? (scrob.watcher?.alive ? 'Listening' : 'Configured') : 'Inactive';
    const webhookState = webhookOn ? 'Active' : 'Inactive';
    const watcherInactive = watcherState === 'Inactive';
    const webhookInactive = webhookState === 'Inactive';
    const pairHealthText = pairTotal
      ? `${pairActive} sync pairs active${pairDisabled ? `, ${pairDisabled} disabled` : ''}`
      : 'No sync pairs configured';
    const nextPairText = pairTotal
      ? (pairDisabled ? `${pairActive} active of ${pairTotal} pairs` : plural(pairActive, 'active pair'))
      : 'No pairs';
    const healthy = providerCount > 0 && metaReady && (pairActive > 0 || scrob.enabled || sched.enabled);
    const ready = {
      auth: providerCount > 0,
      meta: metaReady,
      sync: pairActive > 0,
      scheduling: !!(scrob.enabled || sched.enabled)
    };
    const next = !ready.auth
      ? { target: 'auth', title: 'Connect providers', copy: 'Add your first media server or tracker.' }
      : !ready.meta
        ? { target: 'meta', title: 'Configure metadata', copy: 'Add TMDb or anime mapping support.' }
        : !ready.sync
          ? { target: 'sync', title: 'Create sync pairs', copy: 'Connect two services with a sync route.' }
          : !ready.scheduling
            ? { target: 'scheduling', title: 'Enable automation', copy: 'Turn on scheduling or scrobbler.' }
            : { target: 'scheduling', title: dateTimeCompact(sched.nextRun), copy: nextPairText };
    return `
      <section class="si-op-card si-health ${healthy ? 'is-health' : 'is-health is-warn'}">
        <div class="si-op-head"><span class="material-symbols-rounded">${healthy ? 'verified_user' : 'shield'}</span><strong>System health</strong></div>
        <div class="si-health-main"><span class="si-health-check"><span class="material-symbols-rounded">${healthy ? 'check' : 'priority_high'}</span></span><div class="si-health-title">${healthy ? 'All systems operational' : 'Setup needs attention'}</div></div>
        <div class="si-facts">
          ${fact('groups', `${providerCount} providers connected`, providerCount > 0)}
          ${fact('link', pairHealthText, pairActive > 0)}
          ${fact('description', metaReady ? 'Metadata ready' : 'Metadata not ready', metaReady)}
          ${fact('schedule', lastEpoch ? `Last successful sync, ${relTime(lastEpoch)}` : 'Last successful sync not available', !!lastEpoch)}
        </div>
      </section>
      <section class="si-op-card">
        <div class="si-op-head"><span class="material-symbols-rounded">rocket_launch</span><strong>Next action</strong></div>
        <button type="button" class="si-next-main" data-target="${esc(next.target)}">
          <span class="si-next-icon"><span class="material-symbols-rounded">${healthy ? 'calendar_month' : 'check_circle'}</span></span>
          <span><span class="si-next-label">${healthy ? 'Next synchronization' : 'Complete setup'}</span><strong class="si-next-time">${esc(next.title)}</strong><small class="si-next-sub">${esc(next.copy)}</small></span>
          <span class="material-symbols-rounded si-next-chev" aria-hidden="true">chevron_right</span>
        </button>
        <div class="si-mini-grid">
          <div class="si-mini-status ${watcherInactive ? 'is-inactive' : ''}"><span class="material-symbols-rounded">sensors</span><span class="si-mini-copy"><strong>Watcher</strong><small>${esc(watcherState)}</small></span></div>
          <div class="si-mini-status ${webhookInactive ? 'is-inactive' : ''}"><span class="material-symbols-rounded">rss_feed</span><span class="si-mini-copy"><strong>Webhook</strong><small>${esc(webhookState)}</small></span></div>
        </div>
        <button type="button" class="si-action" data-target="scheduling">View schedule <span class="material-symbols-rounded">arrow_forward</span></button>
      </section>
      <section class="si-op-card">
        <div class="si-op-head"><span class="material-symbols-rounded si-good">sync</span><strong>Recent activity</strong></div>
        ${activityRows(items)}
      </section>
    `;
  }

  function row(icon, title, body, pane, target) {
    const el = d.createElement('div');
    el.className = 'si-row';
    if (pane) el.dataset.pane = pane;
    if (target) el.dataset.target = target;
    el.innerHTML = `<div class="si-ic"><span class="material-symbols-rounded">${icon}</span></div><div class="si-col"><div class="si-h">${title}</div><div class="si-one">${body}</div></div>`;
    return el;
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
    const hostId = isWatch ? 'scrob-watcher' : 'scrob-webhook';
    const sub = isWatch ? 'watcher' : 'plex';
    await openPaneSection('scrobbler', sectionId);
    await wait(80);
    const host = d.getElementById(hostId);
    const tab = host?.querySelector?.(`.cw-subtile[data-sub="${sub}"]`);
    tab?.click?.();
  }

  async function handleRowOpen(target) {
    const key = String(target || '').toLowerCase();
    if (key === 'auth') return openPaneSection('providers', 'sec-auth');
    if (key === 'sync') return openPaneSection('sync', 'sec-sync');
    if (key === 'meta') return openPaneSection('providers', 'sec-meta');
    if (key === 'scheduling') return openPaneSection('scheduling', 'sec-scheduling');
    if (key === 'scrobbler') return openScrobblerSection(state.liveData?.scrob?.mode);
    if (key) w.cwSettingsSelect?.(key);
  }

  const state = { cfg: null, staticData: null, liveData: null, liveTimer: null, busyStatic: false, busyLive: false, queuedStatic: false, queuedLive: false, lastKey: '' };
  const syncHeight = () => { const el = $('#cw-si-scroll'); if (el) el.style.maxHeight = `${Math.max(260, w.innerHeight - 220)}px`; };
  const applyRender = () => state.staticData && state.liveData && render({ ...state.staticData, ...state.liveData });

  function render(data) {
    const body = $('#cw-si-body'), key = JSON.stringify(data || {});
    if (!body || key === state.lastKey) return;
    state.lastKey = key;
    try { d.dispatchEvent(new CustomEvent('cw-settings-overview-data', { detail: { data } })); } catch {}
    body.innerHTML = operationsHTML(data);
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
      const [sched, scrob, activity] = await Promise.all([getSchedulingSummary(state.cfg, force), getScrobblerSummary(state.cfg, force), getActivitySummary(force)]);
      state.liveData = { sched, scrob, activity };
      applyRender();
      scheduleLive();
    });
  }

  const refreshAll = async (force = false) => { await refreshStatic(force); await refreshLive(force); };
  const invalidateAll = () => { try { Cache()?.invalidate(); } catch {} Object.assign(state, { cfg: null, staticData: null, liveData: null, lastKey: '' }); };
  w.SettingsInsight = { refresh: () => refreshAll(true) };

  (async function boot() {
    if (!$('#cw-settings-insight-style')) {
      const s = d.createElement('style');
      s.id = 'cw-settings-insight-style';
      s.textContent = css + opsCss + opsLayoutCss + flatOpsCss;
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
    d.addEventListener('click', (e) => {
      const action = e.target?.closest?.('.si-action[data-target],.si-next-main[data-target]');
      if (action) return void handleRowOpen(action.dataset.target);
      const row = e.target?.closest?.('.si-row[data-pane]');
      const target = row?.dataset?.target;
      if (target) return void handleRowOpen(target);
      const pane = row?.dataset?.pane;
      if (pane) w.cwSettingsSelect?.(pane);
    });
    w.addEventListener('focus', () => isVisible() && refreshLive(false));
    w.addEventListener('resize', syncHeight);
    w.addEventListener('scroll', syncHeight, { passive: true });

    if (isVisible()) refreshAll(true);
    w.refreshSettingsInsight = () => refreshAll(true);
  })();
})(window, document);
