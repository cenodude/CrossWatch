/* assets/js/schedulerbanner.js */
/* Refactored and expanded scheduler/watcher banner with currently watching events */
/* Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch) */
(()=>{
  if (window.__SCHED_BANNER_INIT__) return;
  window.__SCHED_BANNER_INIT__ = 1;

  const API = () => (window.CW && window.CW.API) || null;
  const Cache = () => (window.CW && window.CW.Cache) || null;
  const $ = (s, r = document) => r.querySelector(s);

  (() => {
    if ($('#sched-banner-css')) return;
    const st = document.createElement('style');
    st.id = 'sched-banner-css';
    st.textContent = `
#sched-inline-log{position:absolute;right:16px;bottom:10px;z-index:4;pointer-events:none;display:flex;gap:8px;align-items:center;flex-wrap:wrap}
#sched-inline-log .sched{position:relative;display:inline-flex;align-items:center;gap:8px;white-space:nowrap;max-width:92vw;padding:3px 12px;border-radius:999px;font-size:11px;line-height:1;background:linear-gradient(180deg,rgba(16,18,26,.78),rgba(16,18,26,.92));backdrop-filter:blur(5px) saturate(110%);border:1px solid rgba(140,160,255,.15);box-shadow:0 2px 8px rgba(0,0,0,.22),0 0 10px rgba(110,140,255,.06);overflow:visible;pointer-events:auto}
#sched-inline-log .ic{position:relative;display:inline-flex;align-items:center;justify-content:center;flex:0 0 auto}
#sched-inline-log .ic.dot{width:9px;height:9px;border-radius:50%;background:#ef4444;box-shadow:0 0 0 1px rgba(0,0,0,.6),0 0 8px rgba(239,68,68,.4)}
#sched-inline-log .sched.ok .ic.dot{background:#22c55e;box-shadow:0 0 0 1px rgba(0,0,0,.6),0 0 8px rgba(34,197,94,.4)}
#sched-inline-log .sched.live .ic.dot::after{content:"";position:absolute;inset:-3px;border-radius:50%;border:1px solid rgba(34,197,94,.6);opacity:.8;animation:ringPulse 1.6s ease-out infinite}
#sched-inline-log .sub{display:flex;align-items:center;line-height:1;font-weight:700;letter-spacing:.06em;opacity:.9;text-transform:uppercase;transform:translateY(4px)}
@keyframes ringPulse{0%{transform:scale(.7);opacity:.8}80%{transform:scale(1.25);opacity:0}100%{transform:scale(1.25);opacity:0}}`;
    document.head.appendChild(st);
  })();

  function findBox() {
    const picks = ['#ops-out', '#ops_log', '#ops-card', '#sync-output', '.sync-output', '#ops'];
    for (const s of picks) { const n = $(s); if (n) return n; }
    const h = [...document.querySelectorAll('h2,h3,h4,div.head,.head')].find((x) => (x.textContent || '').trim().toUpperCase() === 'SYNC OUTPUT');
    return h ? h.parentElement?.querySelector('pre,textarea,.box,.card,div') : null;
  }

  function ensureBanner() {
    const host = findBox();
    if (!host) return null;
    if (getComputedStyle(host).position === 'static') host.style.position = 'relative';
    let wrap = $('#sched-inline-log', host);
    if (!wrap) {
      wrap = document.createElement('div');
      wrap.id = 'sched-inline-log';
      wrap.innerHTML = `
        <div class="sched" id="chip-sched"><span class="ic dot"></span><span class="sub" id="sched-sub">Scheduler: —</span></div>
        <div class="sched" id="chip-watch"><span class="ic dot"></span><span class="sub" id="watch-sub">Watcher: —</span></div>
        <div class="sched" id="chip-hook"><span class="ic dot"></span><span class="sub" id="hook-sub">Webhook: —</span></div>`;
      host.appendChild(wrap);
    }
    return wrap;
  }

  const state = {
    cfg: null,
    sched: { enabled: false, running: false, next: 0, advanced: false },
    watch: { enabled: false, alive: false, title: '', state: null, streams: 0 },
    hook: { enabled: false, title: '', state: null, streams: 0 },
    timers: { sched: null, scrob: null, wait: null },
    debounce: null,
    lastEvent: { watcher: '', webhook: '' },
  };

  function effectiveScheduling(cfg) {
    const s = cfg?.scheduling || cfg || {};
    return !!(s.enabled || s?.advanced?.enabled);
  }

  function clearTimer(name) { if (state.timers[name]) { clearTimeout(state.timers[name]); state.timers[name] = null; } }
  function stopLoops() { clearTimer('sched'); clearTimer('scrob'); }

  function tClock(v, withDay = false) {
    if (!v) return '—';
    const n = Number(v);
    const dt = new Date(Number.isFinite(n) && n > 0 ? (n < 1e10 ? n * 1000 : n) : v);
    if (isNaN(+dt)) return '—';
    const time = dt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    if (!withDay) return time;
    const now = new Date();
    if (dt.toDateString() === now.toDateString()) return time;
    const tom = new Date(now); tom.setDate(now.getDate() + 1);
    if (dt.toDateString() === tom.toDateString()) return `tomorrow ${time}`;
    return `${dt.toLocaleDateString([], { weekday: 'short' })} ${time}`;
  }

  function emitCurrentlyWatching(source, detail) {
    const payload = { source, ...detail };
    const key = JSON.stringify(payload);
    if (state.lastEvent[source] === key) return;
    state.lastEvent[source] = key;
    try { window.dispatchEvent(new CustomEvent('currently-watching-updated', { detail: payload })); } catch {}
  }

  function render() {
    const host = ensureBanner();
    if (!host) return;
    const chips = {
      sched: $('#chip-sched', host), watch: $('#chip-watch', host), hook: $('#chip-hook', host),
      schedSub: $('#sched-sub', host), watchSub: $('#watch-sub', host), hookSub: $('#hook-sub', host)
    };

    chips.sched.style.display = state.sched.enabled ? 'inline-flex' : 'none';
    if (state.sched.enabled) {
      chips.sched.classList.toggle('ok', true);
      chips.sched.classList.toggle('live', !!state.sched.running);
      chips.schedSub.textContent = `Scheduler: ${state.sched.advanced ? 'advanced ' : ''}${state.sched.running ? 'running' : 'scheduled'}${state.sched.next ? ` (next ${tClock(state.sched.next, true)})` : ''}`;
    }

    chips.watch.style.display = state.watch.enabled ? 'inline-flex' : 'none';
    if (state.watch.enabled) {
      const live = !!state.watch.alive;
      const hasPlay = !!(live && state.watch.state === 'playing' && state.watch.title);
      chips.watch.classList.toggle('ok', live);
      chips.watch.classList.toggle('live', hasPlay);
      const scLabel = hasPlay && state.watch.streams > 1 ? ` (${state.watch.streams} streams)` : '';
      chips.watchSub.textContent = hasPlay ? `Watcher: ${state.watch.title}${scLabel}` : `Watcher: ${live ? 'running' : 'not running'}`;
      emitCurrentlyWatching('watcher', hasPlay ? { title: state.watch.title, progress: 0, state: state.watch.state || 'playing', _streams_count: state.watch.streams } : { state: 'stopped' });
    }

    chips.hook.style.display = state.hook.enabled ? 'inline-flex' : 'none';
    if (state.hook.enabled) {
      const hasPlay = !!(state.hook.state === 'playing' && state.hook.title);
      chips.hook.classList.toggle('ok', true);
      chips.hook.classList.toggle('live', hasPlay);
      const scLabel = hasPlay && state.hook.streams > 1 ? ` (${state.hook.streams} streams)` : '';
      chips.hookSub.textContent = hasPlay ? `Webhook: ${state.hook.title}${scLabel}` : 'Webhook: enabled';
      emitCurrentlyWatching('webhook', hasPlay ? { title: state.hook.title, progress: 0, state: state.hook.state || 'playing', _streams_count: state.hook.streams } : { state: 'stopped' });
    }

    host.style.display = (state.sched.enabled || state.watch.enabled || state.hook.enabled) ? 'flex' : 'none';
  }

  async function readConfig(force = false) {
    const api = API();
    if (!api) return {};
    try { return await api.Config.load(force); } catch { return Cache()?.getCfg() || {}; }
  }

  async function pollSched(force = false) {
    clearTimer('sched');
    if (document.hidden) return scheduleSched();
    try {
      const st = await API().Scheduling.status(force);
      const sc = st?.config || state.cfg?.scheduling || {};
      state.sched.enabled = effectiveScheduling(sc);
      state.sched.advanced = !!sc?.advanced?.enabled;
      state.sched.running = !!st?.running;
      state.sched.next = Number(st?.next_run_at || st?.next_run || 0) || 0;
    } catch {
      state.sched = { enabled: false, running: false, next: 0, advanced: false };
    }
    render();
    scheduleSched();
  }

  async function pollScrob(force = false) {
    clearTimer('scrob');
    const sc = state.cfg?.scrobble || {};
    const enabled = !!sc?.enabled;
    const mode = String(sc?.mode || 'webhook').toLowerCase();
    state.watch = { enabled: enabled && mode === 'watch', alive: false, title: '', state: null, streams: 0 };
    state.hook = { enabled: enabled && mode === 'webhook', title: '', state: null, streams: 0 };
    if (!enabled) { render(); return; }
    if (document.hidden) return scheduleScrob();

    try {
      if (state.watch.enabled) {
        const st = await API().Watch.status(force);
        state.watch.alive = !!st?.alive;
      }
      const cw = await API().Watch.currentlyWatching(force).catch(() => null);
      const cur = cw && (cw.currently_watching || cw);
      const streams = Number(cw?.streams_count) || 0;
      if (cur && cur.state && cur.state !== 'stopped') {
        const src = String(cur.source || '').toLowerCase();
        const target = (src.includes('webhook') && state.hook.enabled) ? state.hook : ((src.includes('watch') || src.includes('watcher')) && state.watch.enabled ? state.watch : (state.watch.enabled ? state.watch : state.hook));
        target.title = cur.title || '';
        target.state = cur.state || null;
        target.streams = streams;
      }
    } catch {}
    render();
    scheduleScrob();
  }

  function scheduleSched() { if (state.sched.enabled) state.timers.sched = setTimeout(() => pollSched(false), 30000); }
  function scheduleScrob() {
    const sc = state.cfg?.scrobble || {};
    if (!sc?.enabled) return;
    const mode = String(sc?.mode || 'webhook').toLowerCase();
    state.timers.scrob = setTimeout(() => pollScrob(false), mode === 'watch' ? 15000 : 60000);
  }

  async function refresh(forceCfg = false) {
    stopLoops();
    state.cfg = await readConfig(forceCfg);
    const schedOn = effectiveScheduling(state.cfg?.scheduling);
    const scrobOn = !!state.cfg?.scrobble?.enabled;
    if (!schedOn && !scrobOn) {
      state.sched = { enabled: false, running: false, next: 0, advanced: false };
      state.watch = { enabled: false, alive: false, title: '', state: null, streams: 0 };
      state.hook = { enabled: false, title: '', state: null, streams: 0 };
      render();
      return;
    }
    if (schedOn) await pollSched(true); else { state.sched = { enabled: false, running: false, next: 0, advanced: false }; }
    if (scrobOn) await pollScrob(true); else { state.watch.enabled = false; state.hook.enabled = false; }
    render();
  }

  function queueRefresh(forceCfg = false) {
    clearTimeout(state.debounce);
    state.debounce = setTimeout(() => {
      if (forceCfg) try { Cache()?.invalidate('config'); } catch {}
      refresh(forceCfg);
    }, 150);
  }

  function bootOnce() {
    if (window.__SCHED_BANNER_STARTED__) return;
    window.__SCHED_BANNER_STARTED__ = true;
    queueRefresh(true);

    document.addEventListener('visibilitychange', () => { if (!document.hidden) queueRefresh(false); }, { passive: true });
    document.addEventListener('config-saved', () => queueRefresh(true));
    window.addEventListener('auth-changed', () => queueRefresh(true));
    document.addEventListener('scheduling-status-refresh', () => pollSched(true));
    document.addEventListener('watcher-status-refresh', () => pollScrob(true));
    document.addEventListener('tab-changed', (e) => { const id = e?.detail?.id; if (id === 'main' || id === 'settings') queueRefresh(false); });
    window.addEventListener('focus', () => queueRefresh(false));
    window.refreshSchedulingBanner = queueRefresh;
  }

  document.addEventListener('DOMContentLoaded', () => {
    clearTimer('wait');
    state.timers.wait = setInterval(() => {
      if (findBox()) {
        clearTimer('wait');
        bootOnce();
      }
    }, 300);
  });
})();
