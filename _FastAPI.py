# _FastAPI.py
# Renders the full HTML for the web UI. Keep this file self‑contained 

def get_index_html() -> str:
    return r"""<!doctype html><html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>CrossWatch</title>
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
<link rel="alternate icon" href="/favicon.ico">


  <link rel="stylesheet" href="/assets/crosswatch.css">

<!-- Auth providers are listed dynamically via /api/platform/providers -->
</head><body>

<header>
  <div class="brand">
    <svg class="logo" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-label="CrossWatch">
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
    <span class="name">CrossWatch</span>
  </div>

  <div class="tabs">
    <div id="tab-main" class="tab active" onclick="showTab('main')">Main</div>
    <div id="tab-watchlist" class="tab" onclick="showTab('watchlist')">Watchlist</div>
    <div id="tab-settings" class="tab" onclick="showTab('settings')">Settings</div>
    <div id="tab-about" class="tab" onclick="openAbout()">About</div>
  </div>
</header>

<main id="layout">
  <!-- MAIN: Synchronization -->
  <section id="ops-card" class="card">
    <div class="title">Synchronization</div>
    <div class="ops-header">
      <div class="badges" id="conn-badges" style="margin-left:auto">
        <span id="badge-plex" class="badge no"><span class="dot no"></span>Plex: Not connected</span>
        <span id="badge-simkl" class="badge no"><span class="dot no"></span>SIMKL: Not connected</span>
        <div id="update-banner" class="hidden">
          <span id="update-text">A new version is available.</span>
          <a id="update-link" href="https://github.com/cenodude/plex-simkl-watchlist-sync/releases"
            target="_blank" rel="noopener noreferrer">Get update</a>
        </div>
        <button id="btn-status-refresh" class="iconbtn" title="Re-check Plex &amp; SIMKL status" aria-label="Refresh status" onclick="manualRefreshStatus()">
          <svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">
            <path d="M21 12a9 9 0 1 1-2.64-6.36" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
            <path d="M21 5v5h-5" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
        </button>
      </div>
    </div>

    <div class="sync-status">
      <div id="sync-icon" class="sync-icon sync-warn"></div>
      <div id="sync-status-text" class="sub">Idle — run a sync to see results</div>
      <span id="sched-inline" class="sub muted" style="margin-left:8px; white-space:nowrap; display:none"></span>
    </div>

    <div class="chiprow">
      <div class="chip">Plex: <span id="chip-plex">–</span></div>
      <div class="chip">SIMKL: <span id="chip-simkl">–</span></div>
      <div class="chip">Duration: <span id="chip-dur">–</span></div>
      <div class="chip">Exit: <span id="chip-exit">–</span></div>
      <span id="st-update" class="badge upd hidden"></span>
    </div>

    <div class="sep"></div>
    <div class="action-row">
      <div class="action-buttons">
        <button id="run" class="btn acc" onclick="runSync()"><span class="label">Synchronize</span><span class="spinner" aria-hidden="true"></span></button>
        <button class="btn" onclick="toggleDetails()">View details</button>
        <button class="btn" onclick="copySummary(this)">Copy summary</button>
        <button class="btn" onclick="downloadSummary()">Download report</button>
      </div>
      <div class="stepper" aria-label="Sync progress">
        <div class="step"><div class="tl-dot" id="tl-start"></div><div class="muted">Start</div></div>
        <div class="step"><div class="tl-dot" id="tl-pre"></div><div class="muted">Pre-counts</div></div>
        <div class="step"><div class="tl-dot" id="tl-post"></div><div class="muted">Post</div></div>
        <div class="step"><div class="tl-dot" id="tl-done"></div><div class="muted">Done</div></div>
      </div>
    </div>

    <div id="details" class="details hidden">
      <div class="details-grid">
        <div class="det-left">
          <div class="title" style="margin-bottom:6px;font-weight:700">Sync output</div>
          <pre id="det-log" class="log"></pre>
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

  <!-- MAIN: Statistics card -->
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

    <div class="stat-tiles">
      <div class="tile g3" id="tile-new" hidden>
        <div class="k">New</div><div class="n" id="stat-new">0</div>
      </div>
      <div class="tile g3" id="tile-del" hidden>
        <div class="k">Removed</div><div class="n" id="stat-del">0</div>
      </div>

      <div class="tile plex" id="tile-plex">
        <div class="k">Plex</div><div class="n" id="stat-plex">0</div>
      </div>
      <div class="tile simkl" id="tile-simkl">
        <div class="k">SIMKL</div><div class="n" id="stat-simkl">0</div>
      </div>
    </div>

    <!-- Insights: Recent syncs -->
    <div class="stat-block">
      <div class="stat-block-header">
        <span class="pill plain">Recent syncs</span>
        <button class="ghost refresh-insights" onclick="refreshInsights()" title="Refresh">⟲</button>
      </div>
      <div id="sync-history" class="history-list"></div>
    </div>

    <!-- Insights: Trend 
    <div class="stat-block">
      <div class="stat-block-header">
        <span class="pill plain">Trend</span>
        <span class="muted">Last 30 samples</span>
      </div>
      <div id="sparkline" class="sparkline"></div>
    </div>
    -->
	
    <!-- TMDB estimated watch time
    Insights: Estimated watch time 
    <div class="stat-block">
      <div class="stat-block-header"><span class="pill plain">Estimated watch time</span></div>
      <div id="watchtime" class="watchtime"></div>
      <div class="micro-note" id="watchtime-note"></div>
    </div>
    -->

  </section>

  <!-- MAIN: Watchlist Preview (carousel) -->
  <section id="placeholder-card" class="card hidden">
    <div class="title">Watchlist Preview</div>

    <div id="wall-msg" class="wall-msg">Loading…</div>

    <div class="wall-wrap">
      <!-- gradient edges -->
      <div id="edgeL" class="edge left"></div>
      <div id="edgeR" class="edge right"></div>

      <!-- posters row -->
      <div id="poster-row" class="row-scroll" aria-label="Watchlist preview"></div>

      <!-- nav buttons -->
      <button class="nav prev" type="button" onclick="scrollWall(-1)" aria-label="Scroll left">‹</button>
      <button class="nav next" type="button" onclick="scrollWall(1)"  aria-label="Scroll right">›</button>
    </div>
  </section>

  <!-- WATCHLIST (grid) -->
  <section id="page-watchlist" class="card hidden">
    <div class="title">Watchlist</div>
    <div id="wl-msg" class="wall-msg">Loading…</div>
    <div id="wl-grid" class="wl-grid hidden"></div>
  </section>

  <!-- SETTINGS -->
  <section id="page-settings" class="card hidden">
    <div class="title">Settings</div>

    <div class="section" id="sec-sync">
      <div class="head" onclick="toggleSection('sec-sync')"><span class="chev">▶</span><strong>Sync Options</strong></div>
      <div class="body">
        <div class="grid2">
          <div><label>Mode</label><select id="mode"><option value="two-way">two-way</option><option value="mirror">mirror</option></select></div>
          <div><label>Source of truth (mirror only)</label><select id="source"><option value="plex">plex</option><option value="simkl">simkl</option></select></div>
        </div>
      </div>
    </div>

    <div class="section" id="sec-scheduling">
      <div class="head" onclick="toggleSection('sec-scheduling')"><span class="chev">▶</span><strong>Scheduling</strong></div>
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

    <div class="section" id="sec-troubleshoot">
      <div class="head" onclick="toggleSection('sec-troubleshoot')"><span class="chev">▶</span><strong>Troubleshoot</strong></div>
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

    <div class="section" id="sec-auth">
      <div class="head" onclick="toggleSection('sec-auth')">
        <span class="chev">▶</span><strong>Authentication</strong>
      </div>
      <div class="body">
        <!-- Dynamic auth providers will mount here -->
        <div id="auth-providers"></div>

        </div>
        </div>

        <!-- TMDb -->
        <div class="section" id="sec-tmdb">
          <div class="head" onclick="toggleSection('sec-tmdb')">
            <span class="chev"></span><strong>TMDb</strong>
          </div>
          <div class="body">
            <div class="grid2">
              <div style="grid-column:1 / -1">
                <label>API key</label>
                <input id="tmdb_api_key" placeholder="Your TMDb API key"
                      oninput="this.dataset.dirty='1'; updateTmdbHint()">
                <div id="tmdb_hint" class="msg warn hidden">
                  TMDb is optional but recommended to enrich posters & metadata in the preview.
                  Get an API key at
                  <a href="https://www.themoviedb.org/settings/api" target="_blank" rel="noopener">TMDb API settings</a>.
                </div>
                <div class="sub">This product uses the TMDb API but is not endorsed by TMDb.</div>
              </div>
            </div>
            <div class="sep"></div>
          </div>
        </div>


      </div>
    </div>

    <div class="footer">
      <button class="btn btn-save" onclick="saveSettings()"><span class="btn-ic">✔</span> Save</button>
      <button class="btn btn-exit" onclick="showTab('main')"><span class="btn-ic">↩</span> Exit</button>
      <span id="save_msg" class="msg ok hidden">Settings saved ✓</span>
    </div>
  </section>

<!-- Debug log -->
<aside id="log-panel" class="card hidden">
  <div class="title">Raw log (Debug)</div>
  <div id="log" class="log"></div>
</aside>

  <!-- About modal -->
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
          <div class="about-item">
            <div class="k">Repository</div>
            <div class="v"><a id="about-repo" href="https://github.com/cenodude/plex-simkl-watchlist-sync" target="_blank" rel="noopener">GitHub</a></div>
          </div>
          <div class="about-item">
            <div class="k">Latest Release</div>
            <div class="v"><a id="about-latest" href="#" target="_blank" rel="noopener">—</a></div>
          </div>
          <div class="about-item">
            <div class="k">Update</div>
            <div class="v"><span id="about-update" class="badge upd hidden"></span></div>
          </div>
        </div>

        <div class="sep"></div>
        <div class="sub" role="note">
          <strong>Disclaimer:</strong> This is open-source software provided “as is,” without any warranties or guarantees. Use at your own risk.
          This project is not affiliated with, sponsored by, or endorsed by Plex, Inc., SIMKL, or The Movie Database (TMDb).
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


  <script src="/assets/crosswatch.js"></script>

    <!-- Provider-agnostic Authentication Cards (rendered by crosswatch.js /api/platform/providers) -->
    <section id="settings-auth" style="margin-top:24px;">
      <h2>Authentication</h2>
      <div id="auth-cards"></div>
    </section>
    
</body></html>
"""
