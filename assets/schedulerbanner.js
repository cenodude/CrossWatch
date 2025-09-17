// /assets/schedulerbanner.js
(() => {
  const $ = (s, r = document) => r.querySelector(s);

  // ---------- locate the Sync Output container ----------
  function findSyncOutputBox() {
    const picks = ['#ops-out', '#ops_log', '#ops-card', '#sync-output', '.sync-output', '#ops'];
    for (const sel of picks) { const n = $(sel); if (n) return n; }
    const heads = Array.from(document.querySelectorAll('h2,h3,h4,div.head,.head'));
    const head = heads.find(h => (h.textContent || '').trim().toUpperCase() === 'SYNC OUTPUT');
    if (head) return head.parentElement?.querySelector('pre,textarea,.box,.card,div') || null;
    return null;
  }

  // ---------- ensure an inline footer inside the output box ----------
  function ensureFooter() {
    const host = findSyncOutputBox();
    if (!host) return null;
  // Ensure the host can be positioned so our label can be absolutely positioned
    const st = getComputedStyle(host);
    if (st.position === 'static') host.style.position = 'relative';

    let f = host.querySelector('#sched-inline-log');
    if (!f) {
      f = document.createElement('div');
      f.id = 'sched-inline-log';
      f.style.cssText = `
        position:absolute; right:10px; bottom:8px; font-size:12px;
        color:var(--muted, #a7a7a7); opacity:.95; pointer-events:none;
        line-height:1.2; background:transparent;
      `;
      host.appendChild(f);
    }
    return f;
  }

  // ---------- format helpers ----------
  function fmtClockFromEpochSec(epochSec) {
    if (!epochSec) return '—';
    const ms = epochSec < 10_000_000_000 ? epochSec * 1000 : epochSec;
    const dt = new Date(ms);
    if (isNaN(+dt)) return '—';
    try { return dt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }); }
    catch { return dt.toISOString().slice(11, 16); }
  }

  function fmtLeft(sec) {
    if (!sec || sec <= 0) return 'due';
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = Math.floor(sec % 60);
    if (h) return `${h}h ${m}m ${s}s`;
    if (m) return `${m}m ${s}s`;
    return `${s}s`;
  }

  // ---------- state ----------
  let enabled = false;
  let running = false;
  let nextRunAt = 0; // epoch seconds

  function render() {
    const el = ensureFooter();
    if (!el) return;

    if (!enabled) {
      el.textContent = '';
      el.style.display = 'none';
      return;
    }

    const now = Math.floor(Date.now() / 1000);
    const left = nextRunAt ? (nextRunAt - now) : 0;
    const label = fmtClockFromEpochSec(nextRunAt);
    const prefix = running ? '⏳ Scheduler running' : '⏳ Scheduler scheduled';
    el.textContent = `${prefix} — next at ${label}${nextRunAt ? ` (in ${fmtLeft(left)})` : ''}`;
    el.style.display = 'block';
  }

  // ---------- backend poll (status endpoint) ----------
  let _inFlight = false;
  async function fetchStatus() {
    if (_inFlight) return;
    _inFlight = true;
    try {
      const r = await fetch('/api/scheduling/status?t=' + Date.now(), { cache: 'no-store' });
      if (!r.ok) throw new Error(String(r.status));
      const j = await r.json();

      enabled   = !!(j?.config?.enabled);
      running   = !!j?.running;
      nextRunAt = Number(j?.next_run_at || 0) || 0; // 0 = not scheduled/unknown
    } catch {
      enabled = false;
      running = false;
      nextRunAt = 0;
    } finally {
      _inFlight = false;
    }
    render();
  }

  // Expose manual refresh for other modules
  window.refreshSchedulingBanner = fetchStatus;

  // ---------- boot & event wiring ----------
  document.addEventListener('DOMContentLoaded', () => {
    const wait = setInterval(() => {
      if (findSyncOutputBox()) {
        clearInterval(wait);
        fetchStatus();

  // Poll: refresh status every 30s; update countdown every second
        try { clearInterval(window._schedPoll); } catch {}
        window._schedPoll = setInterval(fetchStatus, 30000);

        try { clearInterval(window._schedTick); } catch {}
        window._schedTick = setInterval(render, 1000);

  // Announce readiness to other modules (for example: crosswatch.js showTab/settings)
        try { window.dispatchEvent(new Event('sched-banner-ready')); } catch {}
      }
    }, 300);

  // Refresh when the document becomes visible again
    document.addEventListener('visibilitychange', () => { if (!document.hidden) fetchStatus(); });

  // React immediately after settings are saved (triggered by saveSettings())
    document.addEventListener('config-saved', (e) => {
      const sec = e?.detail?.section;
      if (!sec || sec === 'scheduling') fetchStatus();
    });

  // Optional broadcast event that other modules may emit
    document.addEventListener('scheduling-status-refresh', fetchStatus);

  // If the app emits tab-change events, refresh when switching to main or settings
    document.addEventListener('tab-changed', (e) => {
      const id = e?.detail?.id;
      if (id === 'main' || id === 'settings') fetchStatus();
    });

  // On window focus (Alt+Tab back)
    window.addEventListener('focus', fetchStatus);
  });
})();
