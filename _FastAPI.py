"""
_FastAPI.py
Renders the complete HTML for the web UI (self-contained) served by the backend.
"""

def get_index_html() -> str:
    """Return the full, self-contained HTML for the CrossWatch UI."""
    return r"""<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>CrossWatch | Sync-licious</title>

<link rel="icon" type="image/svg+xml" href="/favicon.svg">
<link rel="alternate icon" href="/favicon.ico">
<link rel="stylesheet" href="/assets/crosswatch.css">
<link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Rounded" rel="stylesheet" />

<style>
  /* Icon font helper */
  .material-symbol{font-family:'Material Symbols Rounded';font-weight:normal;font-style:normal;font-size:1em;line-height:1;display:inline-block;vertical-align:middle;letter-spacing:normal;text-transform:none;white-space:nowrap;direction:ltr;-webkit-font-feature-settings:'liga';-webkit-font-smoothing:antialiased}

  /* Publication tidy: collapse legacy controls not used in this build */
  .pair-selectors,button[onclick="addPair()"],#batches_list,button[onclick="addBatch()"],button[onclick="runAllBatches()"]{display:none!important}
  #providers_list.grid2{display:block!important}
  #providers_list .pairs-board{display:flex;flex-direction:column;align-items:flex-start;text-align:left}

  /* Frosted footer + sticky Save (full viewport width) */
  #save-frost{position:fixed;left:0;right:0;bottom:0;height:84px;background:linear-gradient(0deg,rgba(10,10,14,.85) 0%,rgba(10,10,14,.60) 35%,rgba(10,10,14,0) 100%);border-top:1px solid var(--border);backdrop-filter:blur(6px) saturate(110%);-webkit-backdrop-filter:blur(6px) saturate(110%);pointer-events:none;z-index:9998}
  #save-fab{position:fixed;left:0;right:0;bottom:max(12px,env(safe-area-inset-bottom));z-index:10000;display:flex;justify-content:center;align-items:center;pointer-events:none;background:transparent}
  #save-fab .btn{pointer-events:auto;position:relative;z-index:10001;padding:14px 22px;border-radius:14px;font-weight:800;text-transform:uppercase;letter-spacing:.02em;background:linear-gradient(135deg,#ff4d4f,#ff7a7a);border:1px solid #ff9a9a55;box-shadow:0 10px 28px rgba(0,0,0,.35),0 0 14px #ff4d4f55}
  #save-fab.hidden,#save-frost.hidden{display:none}
</style>
</head><body>

<header>
  <div class="brand">
    <svg class="logo" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-label="CrossWatch" tabindex="0" role="button" title="Go to Main" onclick="showTab('main')" onkeypress="if(event.key==='Enter'||event.key===' ')showTab('main')">
      <defs><linearGradient id="cw-g" x1="0" y1="0" x2="24" y2="24" gradientUnits="userSpaceOnUse">
        <stop offset="0" stop-color="#2de2ff"/><stop offset="0.5" stop-color="#7c5cff"/><stop offset="1" stop-color="#ff7ae0"/>
      </linearGradient></defs>
      <rect x="3" y="4" width="18" height="12" rx="2" ry="2" stroke="url(#cw-g)" stroke-width="1.7"/>
      <rect x="8" y="18" width="8" height="1.6" rx="0.8" fill="url(#cw-g)"/>
      <circle cx="8" cy="9" r="1" fill="url(#cw-g)"/>
      <circle cx="12" cy="11" r="1" fill="url(#cw-g)"/>
      <circle cx="16" cy="8" r="1" fill="url(#cw-g)"/>
      <path d="M8 9 L12 11 L16 8" stroke="url(#cw-g)" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>
    </svg>
    <span class="name" role="link" tabindex="0" title="Go to Main" onclick="showTab('main')" onkeypress="if(event.key==='Enter'||event.key===' ')showTab('main')">CrossWatch</span>
  </div>

  <div class="tabs">
    <div id="tab-main" class="tab active" onclick="showTab('main')">Main</div>
    <div id="tab-watchlist" class="tab" onclick="showTab('watchlist')">Watchlist</div>
    <div id="tab-settings" class="tab" onclick="showTab('settings')">Settings</div>
    <div id="tab-about" class="tab" onclick="openAbout()">About</div>
  </div>

  <style id="prehide-wl">#tab-watchlist{display:none!important}</style>
  <script>
  (function(){
    // Show the Watchlist tab when a TMDb API key is configured
    fetch("/api/config",{cache:"no-store"}).then(r=>r.json()).then(cfg=>{
      if ((cfg?.tmdb?.api_key||"").trim()) document.getElementById("prehide-wl")?.remove();
    }).catch(()=>{});
  })();
  </script>
</header>

<main id="layout">
  <section id="ops-card" class="card">
    <div class="title">Synchronization</div>
    <div class="ops-header">
      <div class="badges" id="conn-badges" style="margin-left:auto">
        <span id="badge-plex" class="badge no"><span class="dot no"></span>Plex: Not connected</span>
        <span id="badge-simkl" class="badge no"><span class="dot no"></span>SIMKL: Not connected</span>
        <span id="badge-trakt" class="badge no"><span class="dot no"></span>Trakt: Not connected</span>
        <div id="update-banner" class="hidden">
          <span id="update-text">A new version is available.</span>
          <a id="update-link" href="https://github.com/cenodude/crosswatch/releases" target="_blank" rel="noopener noreferrer">Get update</a>
        </div>
        <button id="btn-status-refresh" class="iconbtn" title="Re-check Plex &amp; SIMKL status" aria-label="Refresh status" onclick="manualRefreshStatus()">
          <svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">
            <path d="M21 12a9 9 0 1 1-2.64-6.36" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
            <path d="M21 5v5h-5" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
        </button>
      </div>
    </div>

    <div class="sync-status" style="display:none">
      <div id="sync-icon"></div>
      <div id="sync-status-text"></div>
      <span id="sched-inline" style="display:none"></span>
    </div>

    <div id="ux-progress"></div>
    <div id="ux-lanes"></div>
    <div id="ux-spotlight"></div>

    <div class="action-row">
      <div class="action-buttons">
        <button id="run" class="btn acc" onclick="runSync()">
          <span class="label">Synchronize</span><span class="spinner" aria-hidden="true"></span>
        </button>
        <button class="btn" onclick="toggleDetails()">View details</button>
        <button class="btn" onclick="copySummary(this)">Copy summary</button>
        <button class="btn" onclick="downloadSummary()">Download report</button>
      </div>
    </div>

    <div id="details" class="details hidden">
      <div class="details-grid">
        <div class="det-left">
          <div class="title" style="margin-bottom:6px;font-weight:700">Sync output</div>
          <div id="det-log" class="log"></div>
        </div>
        <div class="det-right">
          <div class="meta-card">
            <div class="meta-grid">
              <div class="meta-label">Module</div>
              <div class="meta-value"><span id="det-cmd" class="pillvalue truncate">–</span></div>
              <div class="meta-label">Version</div>
              <div class="meta-value"><span id="det-ver" class="pillvalue">–</span></div>
              <div class="meta-label">Started</div>
              <div class="meta-value"><span id="det-start" class="pillvalue mono">–</span></div>
              <div class="meta-label">Finished</div>
              <div class="meta-value"><span id="det-finish" class="pillvalue mono">–</span></div>
            </div>
            <div class="meta-actions">
              <button class="btn" onclick="copySummary(this)">Copy summary</button>
              <button class="btn" onclick="downloadSummary()">Download</button>
            </div>
          </div>
        </div>
      </div>
    </div>
  </section>

  <section id="stats-card" class="card collapsed">
    <div class="title">Statistics</div>

    <div class="stats-modern v2">
      <div class="now">
        <div class="label">Now</div>
        <div id="stat-now" class="value" data-v="0">0</div>
        <div class="chips">
          <span id="trend-week" class="chip trend flat">no change</span>
        </div>
      </div>

      <div class="facts">
        <div class="fact"><span class="k">Last Week</span><span id="stat-week" class="v" data-v="0">0</span></div>
        <div class="fact"><span class="k">Last Month</span><span id="stat-month" class="v" data-v="0">0</span></div>

        <div class="mini-legend">
          <span class="dot add"></span><span class="l">Added</span><span id="stat-added" class="n">0</span>
          <span class="dot del"></span><span class="l">Removed</span><span id="stat-removed" class="n">0</span>
        </div>

        <div class="stat-meter" aria-hidden="true"><span id="stat-fill"></span></div>
      </div>
    </div>

    <div class="stat-tiles" id="stat-providers">
      <div class="tile plex"  id="tile-plex"><div class="k">Plex</div><div class="n" id="stat-plex">0</div></div>
      <div class="tile simkl" id="tile-simkl"><div class="k">SIMKL</div><div class="n" id="stat-simkl">0</div></div>
      <div class="tile trakt" id="tile-trakt"><div class="k">Trakt</div><div class="n" id="stat-trakt">0</div></div>
    </div>

    <div class="stat-block">
      <div class="stat-block-header">
        <span class="pill plain">Recent syncs</span>
        <button class="ghost refresh-insights" onclick="refreshInsights()" title="Refresh">⟲</button>
      </div>
      <div id="sync-history" class="history-list"></div>
    </div>
  </section>

  <section id="placeholder-card" class="card hidden">
    <div class="title">Watchlist Preview</div>

    <div id="wall-msg" class="wall-msg">Loading…</div>

    <div class="wall-wrap">
      <div id="edgeL" class="edge left"></div>
      <div id="edgeR" class="edge right"></div>

      <div id="poster-row" class="row-scroll" aria-label="Watchlist preview"></div>

      <button class="nav prev" type="button" onclick="scrollWall(-1)" aria-label="Scroll left">‹</button>
      <button class="nav next" type="button" onclick="scrollWall(1)"  aria-label="Scroll right">›</button>
    </div>
  </section>

  <section id="page-watchlist" class="card hidden">
    <div class="title">Watchlist</div>
    <div id="watchlist-root"></div>
  </section>

  <section id="page-settings" class="card hidden">
    <div class="title">Settings</div>

    <div id="cw-settings-grid">
      <div id="cw-settings-left">

        <div class="section" id="sec-auth">
          <div class="head" onclick="toggleSection('sec-auth')">
            <span class="chev">▶</span><strong>Authentication Providers</strong>
          </div>
          <div class="body"><div id="auth-providers"></div></div>
        </div>

        <div class="section" id="sec-sync">
          <div class="head" onclick="toggleSection('sec-sync')">
            <span class="chev">▶</span><strong>Synchronization Providers</strong>
          </div>
          <div class="body">
            <div class="sub">Providers</div>
            <div id="providers_list" class="grid2"></div>

            <div class="sep"></div>
            <div class="sub">Pairs</div>
            <div id="pairs_list"></div>

            <div class="footer">
              <div class="pair-selectors" style="margin-top:1em;">
                <label style="margin-right:1em;">Source:
                  <select id="source-provider" style="margin-left:0.5em;"></select>
                </label>
                <label>Target:
                  <select id="target-provider" style="margin-left:0.5em;"></select>
                </label>
              </div>
            </div>
          </div>
        </div>

        <div class="section" id="sec-meta">
          <div class="head" onclick="toggleSection('sec-meta')">
            <span class="chev">▶</span><strong>Metadata Providers</strong>
          </div>
          <div class="body"><div id="metadata-providers"></div></div>
        </div>

        <div class="section" id="sec-scheduling">
          <div class="head" onclick="toggleSection('sec-scheduling')">
            <span class="chev">▶</span><strong>Scheduling</strong>
          </div>
          <div class="body">
            <div class="grid2">
              <div><label>Enable</label>
                <select id="schEnabled"><option value="false">Disabled</option><option value="true">Enabled</option></select>
              </div>
              <div><label>Frequency</label>
                <select id="schMode">
                  <option value="hourly">Every hour</option>
                  <option value="every_n_hours">Every N hours</option>
                  <option value="daily_time">Daily at…</option>
                </select>
              </div>
              <div><label>Every N hours</label><input id="schN" type="number" min="1" max="24" value="2"></div>
              <div><label>Time</label><input id="schTime" type="time" value="03:30"></div>
            </div>
          </div>
        </div>

        <div class="section" id="sec-scrobbler">
          <div class="head" onclick="toggleSection('sec-scrobbler')">
            <span class="chev">▶</span><strong>Scrobbler</strong>
          </div>
          <div class="body">
            <div id="scrobble-mount">
              <div class="section" id="sc-sec-webhook">
                <div class="head" onclick="toggleSection('sc-sec-webhook')">
                  <span class="chev">▶</span><strong>Webhook</strong>
                </div>
                <div class="body"><div id="scrob-webhook"></div></div>
              </div>

              <div class="section" id="sc-sec-watch">
                <div class="head" onclick="toggleSection('sc-sec-watch')">
                  <span class="chev">▶</span><strong>Watcher</strong>
                </div>
                <div class="body"><div id="scrob-watcher"></div></div>
              </div>
            </div>
          </div>
        </div>

        <div class="section" id="sec-troubleshoot">
          <div class="head" onclick="toggleSection('sec-troubleshoot')">
            <span class="chev">▶</span><strong>Troubleshoot</strong>
          </div>
          <div class="body">
            <div class="sub">Use these actions to reset application state. They are safe but cannot be undone.</div>
            <div><label>Debug</label><select id="debug"><option value="false">off</option><option value="true">on</option></select></div>
            <div class="chiprow">
              <button class="btn danger" onclick="clearState()">Clear State</button>
              <button class="btn danger" onclick="clearCache()">Clear Cache</button>
              <button class="btn danger" onclick="resetStats()">Reset Statistics</button>
            </div>
            <div id="tb_msg" class="msg ok hidden">Done ✓</div>
          </div>
        </div>

      </div>

      <aside id="cw-settings-insight" aria-label="Settings Insight"></aside>
    </div>
  </section>

  <div id="about-backdrop" class="modal-backdrop hidden" onclick="closeAbout(event)">
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="about-title" onclick="event.stopPropagation()">
      <div class="modal-header">
        <div class="title-wrap">
          <div class="app-logo">🎬</div>
          <div>
            <div id="about-title" class="app-name">CrossWatch</div>
            <div class="app-sub"><span id="about-version">Version …</span></div>
          </div>
        </div>
        <button class="btn-ghost" aria-label="Close" onclick="closeAbout()">✕</button>
      </div>

      <div class="modal-body">
        <div class="about-grid">
          <div class="about-item"><div class="k">Repository</div><div class="v"><a id="about-repo" href="https://github.com/cenodude/crosswatch" target="_blank" rel="noopener">GitHub</a></div></div>
          <div class="about-item"><div class="k">Latest Release</div><div class="v"><a id="about-latest" href="#" target="_blank" rel="noopener">—</a></div></div>
          <div class="about-item"><div class="k">Update</div><div class="v"><span id="about-update" class="badge upd hidden"></span></div></div>
        </div>

        <div class="sep"></div>
        <div class="sub" role="note">
          <strong>Disclaimer:</strong> This is open-source software provided “as is,” without any warranties or guarantees. Use at your own risk.
          This project is not affiliated with, sponsored by, or endorsed by Plex, Inc., TRAKT, SIMKL, or The Movie Database (TMDb).
          All product names, logos, and brands are property of their respective owners.
        </div>
      </div>

      <div class="modal-footer">
        <button class="btn" onclick="window.open(document.getElementById('about-latest').href,'_blank')">Open Releases</button>
        <button class="btn alt" onclick="closeAbout()">Close</button>
      </div>
    </div>
  </div>

</main>

<script src="/assets/client-formatter.js" defer></script>
<script src="/assets/auth.plex-simkl.js" defer></script>
<script src="/assets/auth.trakt.js" defer></script>
<script src="/assets/client-formatter.js" defer></script>
<script src="/assets/crosswatch.js" defer></script>
<script src="/assets/scrobbler.js" defer></script>
<script src="/assets/auth.trakt.js" defer></script>
<script src="/assets/watchlist.js" defer></script>
<script src="/assets/insights.js" defer></script>
<script src="/assets/settings-insight.js" defer></script>
<script src="/assets/main.js" defer></script>
<script src="/assets/modals.js" defer></script>
<script src="/assets/connections.overlay.js" defer></script>
<script src="/assets/connections.pairs.overlay.js" defer></script>
<script src="/assets/scheduler.js" defer></script>
<script src="/assets/schedulerbanner.js" defer></script>

<script>
  document.addEventListener('DOMContentLoaded', () => {
    try { if (typeof openSummaryStream === 'function') openSummaryStream(); } catch (e) {}
  });
</script>

<!-- Frosted footer layer + sticky Save button -->
<div id="save-frost" class="hidden" aria-hidden="true"></div>
<div id="save-fab" class="hidden" role="toolbar" aria-label="Sticky save">
  <button id="save-fab-btn" class="btn" onclick="saveSettings(this)">
    <span class="btn-ic">✔</span> <span class="btn-label">Save</span>
  </button>
</div>

<script>
(function(){
  const fab   = document.getElementById('save-fab');
  const frost = document.getElementById('save-frost');

  // Always query fresh in case the node was replaced
  function getSettingsEl(){ return document.getElementById('page-settings'); }

  function visibleOnSettings(){
    const s = getSettingsEl();
    // Extra guard: also check the tab state
    const tabActive = !!document.getElementById('tab-settings')?.classList.contains('active');
    return !!(s && !s.classList.contains('hidden') && tabActive);
  }

  function updateFooter(){
    const show = visibleOnSettings();
    fab?.classList.toggle('hidden', !show);
    frost?.classList.toggle('hidden', !show);
  }

  // (Re)attach a MutationObserver to the current #page-settings
  let mo = null;
  function bindObserver(){
    try { mo?.disconnect(); } catch {}
    const s = getSettingsEl();
    if (!s) return;
    mo = new MutationObserver(updateFooter);
    mo.observe(s, { attributes: true, attributeFilter: ['class'] });
  }

  // Boot
  document.addEventListener('DOMContentLoaded', () => {
    bindObserver();
    updateFooter();
  }, { once:true });

  // When tabs change via app events (not only clicks)
  document.addEventListener('tab-changed', updateFooter);

  // Fallback for hash navigation / programmatic DOM swaps
  window.addEventListener('hashchange', updateFooter);
  window.addEventListener('sched-banner-ready', updateFooter);

  // If something replaces #page-settings later, try to rebind
  const rebinder = setInterval(() => {
    const s = getSettingsEl();
    if (!s) return;               // wait until exists
    if (mo && mo._target === s) return;
    // Tag current target so we don't rebind too often
    if (mo) mo._target = s;
    bindObserver();
    updateFooter();
  }, 1000);

  // Optional: stop the rebinder when page unloads
  window.addEventListener('beforeunload', () => clearInterval(rebinder));
})();
</script>


<script>
(function(){
  function install(){
    const orig = window.saveSettings;
    if (typeof orig !== 'function' || orig._wrapped) return;

    async function wrapped(){
      let btn = (arguments[0] instanceof HTMLElement) ? arguments[0] : document.getElementById('save-fab-btn');
      if (btn && !btn.dataset.defaultHtml) btn.dataset.defaultHtml = btn.innerHTML;
      if (btn) btn.disabled = true;

      try{
        const ret = orig.apply(this, arguments);
        await (ret && typeof ret.then === 'function' ? ret : Promise.resolve());
        if (btn){
          btn.innerHTML = 'Settings saved ✓';
          setTimeout(()=>{ btn.innerHTML = btn.dataset.defaultHtml || '<span class="btn-ic">✔</span> <span class="btn-label">Save</span>'; btn.disabled = false; }, 1600);
        }
        return ret;
      }catch(err){
        if (btn){
          btn.innerHTML = 'Save failed';
          setTimeout(()=>{ btn.innerHTML = btn.dataset.defaultHtml || '<span class="btn-ic">✔</span> <span class="btn-label">Save</span>'; btn.disabled = false; }, 2000);
        }
        throw err;
      }
    }
    wrapped._wrapped = true;
    window.saveSettings = wrapped;
  }

  if (document.readyState === 'complete') install();
  window.addEventListener('load', install);
})();
</script>

</body></html>
"""
