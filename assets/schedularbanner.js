// /assets/schedularbanner.js
// Displays scheduler status in the SYNC OUTPUT area (bottom-right). Calculates next run time if not provided by the API.

(() => {
  const $ = (s, r = document) => r.querySelector(s);

  // ---------- Locate SYNC OUTPUT container ----------
  function findSyncOutputBox() {
    const picks = ['#ops-out','#ops_log','#ops-card','#sync-output','.sync-output','#ops'];
    for (const sel of picks) { const n = $(sel); if (n) return n; }
    const heads = Array.from(document.querySelectorAll('h2,h3,h4,div.head,.head'));
    const head = heads.find(h => (h.textContent || '').trim().toUpperCase() === 'SYNC OUTPUT');
    if (head) return head.parentElement?.querySelector('pre,textarea,.box,.card,div') || null;
    return null;
  }

  // ---------- Ensure scheduler status footer exists ----------
  function ensureFooter() {
    const host = findSyncOutputBox();
    if (!host) return null;
    if (getComputedStyle(host).position === 'static') host.style.position = 'relative';
    let f = host.querySelector('#sched-inline-log');
    if (!f) {
      f = document.createElement('div');
      f.id = 'sched-inline-log';
      f.style.cssText = `
        position:absolute; right:10px; bottom:8px; font-size:12px;
        color:var(--muted,#a7a7a7); opacity:.95; pointer-events:none;
      `;
      host.appendChild(f);
    }
    return f;
  }

  // ---------- Time formatting helpers ----------
  function fmtClock(dt) {
    try { return dt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }); }
    catch { return dt.toISOString().slice(11,16); }
  }
  function fmtLeft(sec) {
    if (sec <= 0) return 'due';
    const h = Math.floor(sec/3600), m = Math.floor((sec%3600)/60), s = Math.floor(sec%60);
    if (h) return `${h}h ${m}m ${s}s`;
    if (m) return `${m}m ${s}s`;
    return `${s}s`;
  }

  // Returns the timezone offset (in minutes) for a given date
  function tzOffsetMin(timeZone, date = new Date()) {
    // Format the date in the target TZ, then reconstruct a UTC date and diff
    const dtf = new Intl.DateTimeFormat('en-US', {
      timeZone, hour12:false, year:'numeric', month:'2-digit', day:'2-digit',
      hour:'2-digit', minute:'2-digit', second:'2-digit'
    });
    const parts = Object.fromEntries(dtf.formatToParts(date).map(x => [x.type, x.value]));
    const y = +parts.year, m = +parts.month, d = +parts.day;
    const hh = +parts.hour, mm = +parts.minute, ss = +parts.second;
    // UTC epoch if that clock time occurred in the target TZ
    const tzEpoch = Date.UTC(y, m-1, d, hh, mm, ss);
    // Offset = local real epoch - tzEpoch (in minutes)
    return Math.round((tzEpoch - date.getTime()) / 60000);
  }

  // Calculates the next run epoch (seconds) from config if the API does not provide it
  function computeNextFromConfig(cfg) {
    const mode = (cfg?.mode || 'hourly').toLowerCase();
    const n = parseInt(cfg?.every_n_hours || '2', 10) || 2;
    const daily = String(cfg?.daily_time || '03:30');
    const tz = cfg?.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';

    const now = new Date();
    if (mode === 'hourly') {
      const next = new Date(now.getTime());
      next.setMinutes(0, 0, 0);
      next.setHours(next.getHours() + 1);
      return Math.floor(next.getTime() / 1000);
    }
    if (mode === 'every_n_hours') {
      const next = new Date(now.getTime() + n * 3600 * 1000);
      return Math.floor(next.getTime() / 1000);
    }
    // daily_time
    const [H, M] = daily.split(':').map(x => parseInt(x || '0', 10));
    // Build a Date representing today HH:MM in the target timezone, then convert to epoch
    const offNowMin = tzOffsetMin(tz, now); // minutes
    const tzNow = new Date(now.getTime() + offNowMin * 60000);
    const y = tzNow.getUTCFullYear(), m = tzNow.getUTCMonth(), d = tzNow.getUTCDate();
    const tzTarget = new Date(Date.UTC(y, m, d, H, M, 0)); // this is "today HH:MM" in tz clock
    // Convert back to real epoch by subtracting the same offset at that target instant
    const offTargetMin = tzOffsetMin(tz, tzTarget);
    const candidate = new Date(tzTarget.getTime() - offTargetMin * 60000);
    if (candidate.getTime() <= now.getTime()) {
      // tomorrow
      const tzTomorrow = new Date(Date.UTC(y, m, d + 1, H, M, 0));
      const offTomorrowMin = tzOffsetMin(tz, tzTomorrow);
      const tomorrowReal = new Date(tzTomorrow.getTime() - offTomorrowMin * 60000);
      return Math.floor(tomorrowReal.getTime() / 1000);
    }
    return Math.floor(candidate.getTime() / 1000);
  }

  // ---------- State variables and render function ----------
  let enabledFlag = false;
  let nextEpochSec = 0;
  let runningFlag = false;

  function render() {
    const el = ensureFooter(); if (!el) return;
    if (!enabledFlag) { el.textContent = ''; el.style.display = 'none'; return; }

    const now = Math.floor(Date.now()/1000);
    const left = nextEpochSec ? (nextEpochSec - now) : 0;
    const label = nextEpochSec ? fmtClock(new Date(nextEpochSec*1000)) : '—';
    const prefix = runningFlag ? '⏳ Scheduler running' : '⏳ Scheduler scheduled';
    el.textContent = `${prefix} — next at ${label} (in ${fmtLeft(left)})`;
    el.style.display = 'block';
  }

  // ---------- Poll backend for scheduler status ----------
  async function fetchStatus() {
    try {
      const r = await fetch('/api/scheduling/status', { cache: 'no-store' });
      if (!r.ok) throw new Error(String(r.status));
      const j = await r.json();

      enabledFlag = !!j?.config?.enabled;
      runningFlag = !!j?.running;

      const now = Math.floor(Date.now() / 1000);
      let epoch =
        j?.next_run_at ??
        j?.next_run_ts ??
        j?.next_run_epoch ??
        j?.next_at ?? 0;

      if (!epoch || epoch <= 0) {
        // compute locally from config
        epoch = computeNextFromConfig(j?.config || {});
      }

      // sanity check (avoid past value)
      if (epoch && +epoch > now - 86400) {
        nextEpochSec = +epoch;
      } else {
        nextEpochSec = 0;
      }
    } catch {
      enabledFlag = false;
      runningFlag = false;
      nextEpochSec = 0;
    }
    render();
  }

  window.refreshSchedulingBanner = fetchStatus;

  document.addEventListener('DOMContentLoaded', () => {
    const wait = setInterval(() => {
      if (findSyncOutputBox()) {
        clearInterval(wait);
        fetchStatus();
        if (window._schedPoll) clearInterval(window._schedPoll);
        window._schedPoll = setInterval(fetchStatus, 30000);
        if (window._schedTick) clearInterval(window._schedTick);
        window._schedTick = setInterval(render, 1000);
      }
    }, 300);
    document.addEventListener('visibilitychange', () => { if (!document.hidden) fetchStatus(); });
  });
})();
