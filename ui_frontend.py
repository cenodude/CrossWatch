# ui_frontend.py
# Refactored
# CrossWatch - UI Frontend Registration
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response
from starlette.staticfiles import StaticFiles
from api.versionAPI import CURRENT_VERSION
from cw_platform.config_base import load_config

__all__ = ["register_assets_and_favicons", "register_ui_root", "get_index_html"]

# Static favicon
FAVICON_SVG: str = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
<defs><linearGradient id="g" x1="0" y1="0" x2="64" y2="64" gradientUnits="userSpaceOnUse">
<stop offset="0" stop-color="#2de2ff"/><stop offset="0.5" stop-color="#5f69d6"/><stop offset="1" stop-color="#7a6aa8"/></linearGradient></defs>
<rect width="64" height="64" rx="14" fill="#0b0b0f"/>
<rect x="10" y="16" width="44" height="28" rx="6" fill="none" stroke="url(#g)" stroke-width="3"/>
<rect x="24" y="46" width="16" height="3" rx="1.5" fill="url(#g)"/>
<circle cx="20" cy="30" r="2.5" fill="url(#g)"/>
<circle cx="32" cy="26" r="2.5" fill="url(#g)"/>
<circle cx="44" cy="22" r="2.5" fill="url(#g)"/>
<path d="M20 30 L32 26 L44 22" fill="none" stroke="url(#g)" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>
</svg>"""


DEFAULT_MANIFEST: str = r"""{
  "name": "CrossWatch",
  "short_name": "CrossWatch",
  "start_url": "/?ui=compact",
  "scope": "/",
  "display": "standalone",
  "background_color": "#0b0b0f",
  "theme_color": "#0b0b0f",
  "icons": [
    { "src": "/assets/pwa/icon-192.png", "sizes": "192x192", "type": "image/png" },
    { "src": "/assets/pwa/icon-512.png", "sizes": "512x512", "type": "image/png" }
  ]
}"""

DEFAULT_SW: str = r"""/* sw.js - service worker */
self.addEventListener("install", (event) => {
  self.skipWaiting();
});
self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});
self.addEventListener("fetch", (event) => {
});
"""

GITBOOK_EMBED_BLOCK: str = r"""<script id="cw-gitbook-embed" src="https://wiki.crosswatch.app/~gitbook/embed/script.js"></script><script>window.__cwGitBookConfig={siteUrl:"https://wiki.crosswatch.app",reportUrl:"https://github.com/cenodude/CrossWatch/issues/new"};</script><script src="/assets/js/gitbook.js?v=__CW_VERSION__" defer></script>"""


def register_assets_and_favicons(app: FastAPI, root: Path) -> None:
    assets_dir = root / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    def asset_response(name: str, fallback: str, media_type: str, **headers: str) -> Response:
        try:
            content = (assets_dir / name).read_text(encoding="utf-8")
        except Exception:
            content = fallback
        return Response(content=content, media_type=media_type, headers=headers)

    @app.get("/favicon.svg", include_in_schema=False, tags=["ui"])
    @app.get("/favicon.ico", include_in_schema=False, tags=["ui"])
    def favicon() -> Response:
        return Response(FAVICON_SVG, media_type="image/svg+xml", headers={"Cache-Control": "public, max-age=86400"})

    @app.get("/manifest.webmanifest", include_in_schema=False, tags=["ui"])
    def manifest_webmanifest() -> Response:
        return asset_response("manifest.webmanifest", DEFAULT_MANIFEST, "application/manifest+json", **{"Cache-Control": "public, max-age=3600"})

    @app.get("/sw.js", include_in_schema=False, tags=["ui"])
    def service_worker() -> Response:
        return asset_response("sw.js", DEFAULT_SW, "text/javascript", **{"Cache-Control": "no-store", "Service-Worker-Allowed": "/"})

def _ui_show_ai_enabled() -> bool:
    try:
        cfg = load_config()
        ui = (cfg if isinstance(cfg, dict) else {}).get("ui", {})
        return bool(ui.get("show_AI", True)) if isinstance(ui, dict) else True
    except Exception:
        return True


def register_ui_root(app: FastAPI) -> None:
    @app.get("/", include_in_schema=False, tags=["ui"])
    def ui_root(request: Request) -> HTMLResponse:
        show_ai = _ui_show_ai_enabled()
        return HTMLResponse(get_index_html(include_gitbook_embed=_is_https_request(request) and show_ai, ui_show_ai=show_ai), headers={"Cache-Control": "no-store"})


def _is_https_request(request: Request) -> bool:
    return request.headers.get("x-forwarded-proto", request.url.scheme).split(",")[0].strip().lower() == "https"


_HELPER_SCRIPTS = (
    "provider-meta.js", "page-loader.js", "dom.js", "events.js", "api.js", "core.js", "details-log.js",
    "watchlist-preview.js", "providers-ui.js", "settings-ui.js", "settings-save.js", "maintenance.js",
    "restart_apply.js", "legacy-bridge.js",
)
_APP_SCRIPTS = (
    "syncbar.js", "main.js", "connections.overlay.js", "connections.pairs.overlay.js", "scheduler.js",
    "schedulerbanner.js", "playingcard.js", "insights.js", "settings-insight.js", "scrobbler.js",
    "editor.js", "snapshots.js",
)


def _asset_block() -> str:
    helper_tags = "\n".join(f'<script src="/assets/helpers/{name}?v=__CW_VERSION__"></script>' for name in _HELPER_SCRIPTS)
    app_tags = "\n".join(f'<script src="/assets/js/{name}?v=__CW_VERSION__" defer></script>' for name in _APP_SCRIPTS)
    return "\n".join((
        helper_tags,
        '<script src="/assets/helpers/media_user_picker.js?v=__CW_VERSION__" defer></script>',
        '<script src="/assets/crosswatch.js?v=__CW_VERSION__"></script>',
        app_tags,
        '<script src="/assets/auth/auth_loader.js?v=__CW_VERSION__" defer></script>',
        '<script src="/assets/auth/auth.tmdb.js?v=__CW_VERSION__" defer></script>',
        '<script src="/assets/js/client-formatter.js?v=__CW_VERSION__" defer></script>',
        '<link rel="stylesheet" href="/assets/js/modals/core/styles.css?v=__CW_VERSION__">',
        '<script type="module" src="/assets/js/modals.js?v=__CW_VERSION__"></script>',
    ))


def _get_index_html_static() -> str:
    return r"""<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CrossWatch | Sync-licious</title>
<link rel="icon" type="image/svg+xml" href="/favicon.svg"><link rel="alternate icon" href="/favicon.ico">
<meta name="theme-color" content="#0b0b0f">
<link rel="manifest" href="/manifest.webmanifest">
<link rel="apple-touch-icon" href="/assets/pwa/apple-touch-icon.png">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">

<link rel="stylesheet" href="/assets/crosswatch.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/ui-shell.css?v=__CW_VERSION__">
<style id="cw-dark-shell-overrides">
:root{--cw-ov-shell:linear-gradient(180deg,rgba(8,10,14,.96),rgba(3,5,8,.98));--cw-ov-shell-soft:linear-gradient(180deg,rgba(11,14,20,.94),rgba(5,7,10,.97));--cw-ov-shell-strong:linear-gradient(180deg,rgba(13,16,23,.96),rgba(6,8,11,.98));--cw-ov-border:rgba(255,255,255,.075);--cw-ov-border-strong:rgba(255,255,255,.12);--cw-ov-shadow:0 20px 48px rgba(0,0,0,.36),inset 0 1px 0 rgba(255,255,255,.03);--cw-ov-fg:rgba(242,245,255,.96);--cw-ov-soft:rgba(194,202,222,.72)}
header{background:radial-gradient(120% 150% at 0% 0%,rgba(72,80,120,.10),transparent 36%),radial-gradient(120% 150% at 100% 100%,rgba(52,58,86,.08),transparent 42%),var(--cw-ov-shell);border:1px solid var(--cw-ov-border);box-shadow:var(--cw-ov-shadow)}
header .tab,header .cw-ui-btn,header .iconbtn,header .cw-menu{background:var(--cw-ov-shell-soft)!important;border-color:var(--cw-ov-border)!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.025),0 10px 22px rgba(0,0,0,.16)}
header .tab:hover,header .cw-ui-btn:hover,header .iconbtn:hover,header .cw-menu-item:hover{background:var(--cw-ov-shell-strong)!important;border-color:var(--cw-ov-border-strong)!important}
header .tab.active,header .cw-ui-btn.active{background:linear-gradient(180deg,rgba(49,55,78,.82),rgba(12,15,22,.98))!important;border-color:rgba(148,156,194,.18)!important;box-shadow:0 14px 28px rgba(0,0,0,.22),inset 0 1px 0 rgba(255,255,255,.04)}
#page-settings,#page-settings .cw-settings-nav-card,#page-settings .cw-settings-overview-card,#page-settings .cw-settings-pane-head,#page-settings .cw-settings-section,#page-settings .cw-settings-action,#page-settings .cw-settings-jump,#page-settings .cw-settings-nav-btn,#page-settings .cw-hub-tile,#page-settings .cw-settings-panel,#page-settings .cw-panel,#page-settings .cw-menu{background:radial-gradient(125% 150% at 0% 0%,rgba(72,80,120,.10),transparent 38%),var(--cw-ov-shell)!important;border-color:var(--cw-ov-border)!important;box-shadow:var(--cw-ov-shadow)!important}
#page-settings .cw-settings-section .head,#page-settings .cw-settings-section .body,#page-settings .cw-settings-pane-stack,#page-settings .cw-settings-panels,#page-settings .cw-settings-hub{background:transparent!important}
#page-settings .cw-settings-nav-btn:hover,#page-settings .cw-settings-action:hover,#page-settings .cw-settings-jump:hover,#page-settings .cw-hub-tile:hover{background:radial-gradient(125% 150% at 0% 0%,rgba(82,90,132,.12),transparent 34%),var(--cw-ov-shell-strong)!important;border-color:var(--cw-ov-border-strong)!important;transform:translateY(-1px)}
#page-settings .cw-settings-nav-btn.active,#page-settings .cw-settings-jump.active,#page-settings .cw-hub-tile.active,#page-settings .cw-hub-tile[aria-selected="true"],#page-settings .cw-settings-panel.active{background:linear-gradient(180deg,rgba(20,24,34,.98),rgba(7,9,13,.99))!important;border-color:rgba(154,162,198,.18)!important;box-shadow:0 18px 34px rgba(0,0,0,.24),inset 0 1px 0 rgba(255,255,255,.04)!important}
#page-settings input,#page-settings select,#page-settings textarea{background:rgba(4,6,10,.94)!important;border:1px solid rgba(255,255,255,.08)!important;color:var(--cw-ov-fg)!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.02)!important}
#page-settings input:focus,#page-settings select:focus,#page-settings textarea:focus{outline:none;box-shadow:0 0 0 3px rgba(112,122,170,.14),inset 0 1px 0 rgba(255,255,255,.03)!important;border-color:rgba(160,168,202,.18)!important;background:rgba(6,8,12,.98)!important}
#page-settings .sub,#page-settings p,#page-settings small,#page-settings label,#page-settings .cw-settings-pane-kicker,#page-settings .cw-settings-overview-kicker,#page-settings .cw-settings-jumpbar,#page-settings .cw-hub-desc{color:var(--cw-ov-soft)!important}
#page-settings h3,#page-settings h4,#page-settings strong,#page-settings .cw-panel-title,#page-settings .cw-settings-nav-title{color:var(--cw-ov-fg)!important}
#page-settings .chip,#page-settings .pill,#page-settings .cw-provider-head-icon,#page-settings .auth-dot{filter:saturate(.88)}
</style>
<script>
(() => {
  try {
    const q = new URLSearchParams(window.location.search || "");
    const ui = String(q.get("ui") || "").toLowerCase();
    const explicit = ui === "compact" || ui === "full" || q.get("compact") === "1" || q.get("full") === "1";
    const wantCompact = ui === "compact" || q.get("compact") === "1" || (!explicit && window.matchMedia?.("(max-width: 680px)")?.matches);
    if (wantCompact) document.documentElement.classList.add("cw-compact");
  } catch {}
})();
</script>

<link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Rounded" rel="stylesheet" />
</head><body>

<header>
  <div class="brand" role="button" tabindex="0" title="Go to Main" onclick="showTab('main')" onkeypress="if(event.key==='Enter'||event.key===' ')showTab('main')">
    <svg class="logo" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-label="CrossWatch">
      <defs><linearGradient id="cw-g" x1="0" y1="0" x2="24" y2="24"><stop offset="0" stop-color="#2de2ff"/><stop offset=".5" stop-color="#5f69d6"/><stop offset="1" stop-color="#7a6aa8"/></linearGradient></defs>
      <rect x="3" y="4" width="18" height="12" rx="2" ry="2" stroke="url(#cw-g)" stroke-width="1.7"/>
      <rect x="8" y="18" width="8" height="1.6" rx=".8" fill="url(#cw-g)"/>
      <circle cx="8" cy="9" r="1" fill="url(#cw-g)"/><circle cx="12" cy="11" r="1" fill="url(#cw-g)"/><circle cx="16" cy="8" r="1" fill="url(#cw-g)"/>
      <path d="M8 9 L12 11 L16 8" stroke="url(#cw-g)" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>
    </svg>
    <span class="brand-text">
      <span class="name">CrossWatch</span>
      <span class="version">__CW_VERSION__</span>
    </span>
  </div>

  <nav class="tabs" aria-label="Primary navigation">
    <button id="tab-main" class="tab active" type="button" onclick="showTab('main')">Main</button>
    <button id="tab-watchlist" class="tab" type="button" onclick="showTab('watchlist')">Watchlist</button>
    <button id="tab-snapshots" class="tab" type="button" onclick="showTab('snapshots')">Captures</button>
    <button id="tab-editor" class="tab" type="button" onclick="showTab('editor')">Editor</button>
    <button id="tab-settings" class="tab" type="button" onclick="showTab('settings')">Settings</button>
    <div class="cw-tabmenu" id="tab-about-menu">
      <button id="tab-about" class="tab" type="button"
              aria-haspopup="menu" aria-expanded="false"
              onclick="window.cwToggleAboutMenu(event)">
        <span>About</span>
        <span class="tab-caret" aria-hidden="true">▾</span>
      </button>
      <div class="cw-menu hidden" id="cw-about-menu" role="menu" aria-labelledby="tab-about">
        <button class="cw-menu-item" type="button" role="menuitem" onclick="window.cwAboutMenuSelect('about')">About</button>
        <button class="cw-menu-item" type="button" role="menuitem" onclick="window.cwAboutMenuSelect('help')">Help</button>
      </div>
    </div>
  </nav>

  <div class="cw-ui-toggle" aria-label="UI mode">
    <button class="cw-ui-btn btn-full" type="button" onclick="cwSetUiMode('full')">Full UI</button>
    <button class="cw-ui-btn btn-compact" type="button" onclick="cwSetUiMode('compact')">Compact</button>
  </div>
</header>

<main id="layout">
  <section id="ops-card" class="card cw-main-card cw-main-card--sync">
    <div class="title">Synchronization</div>
    <div class="ops-header cw-main-card-head">
      <div class="cw-main-card-head-copy">
        <h2>Synchronization</h2>
      </div>
      <div class="cw-main-card-head-side">
        <div id="conn-badges" class="vip-badges"></div>
        <div class="cw-main-card-head-actions">
          <div id="update-banner" class="hidden"><span id="update-text">A new version is available.</span>
            <a id="update-link" href="https://github.com/cenodude/crosswatch/releases" target="_blank" rel="noopener">Get update</a>
          </div>
          <button id="btn-status-refresh" class="iconbtn" title="Re-check status" aria-label="Refresh status">
            <svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">
              <path d="M21 12a9 9 0 1 1-2.64-6.36" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
              <path d="M21 5v5h-5" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
          </button>
        </div>
      </div>
    </div>

    <div class="sync-status" style="display:none"><div id="sync-icon"></div><div id="sync-status-text"></div><span id="sched-inline" style="display:none"></span></div>
    <div id="ux-progress"></div><div id="ux-lanes"></div><div id="ux-spotlight"></div>

    <div class="action-row">
      <div class="action-buttons">
        <div class="cw-split-run" id="cw-sync-split">
        <button id="run" class="btn acc cw-split-main" onclick="runSync()"><span class="label">Synchronize</span><span class="spinner" aria-hidden="true"></span></button>
        <button id="run-menu" class="btn acc cw-split-edge" type="button" title="Sync options" aria-haspopup="menu" aria-expanded="false" onclick="window.cwToggleSyncMenu(event)">▾</button>
        <div class="cw-menu cw-sync-menu hidden" id="cw-sync-menu" role="menu" aria-labelledby="run-menu"></div>
      </div>
        <button class="btn" onclick="toggleDetails()">View details</button>
        <button class="btn" onclick="openAnalyzer()">Analyzer</button>
        <button class="btn" onclick="openExporter()">Exporter</button>
      </div>
    </div>

    <div id="details" class="details hidden">
      <div class="details-grid">
        <div class="det-left">
          <div class="det-head">
            <div class="det-title">Output</div>
            <div class="det-tabs" role="tablist" aria-label="Output tabs">
              <button id="det-tab-sync" class="det-tab active" type="button"
                role="tab" aria-selected="true" aria-controls="det-panel-sync" data-tab="sync">Sync</button>
              <button id="det-tab-watcher" class="det-tab" type="button"
                role="tab" aria-selected="false" aria-controls="det-panel-watcher" data-tab="watcher">Watcher</button>
            </div>
            <div class="det-tools">
              <button id="det-copy" class="ghost" type="button" title="Copy current output">Copy</button>
              <button id="det-clear" class="ghost" type="button" title="Clear current output">Clear</button>
              <button id="det-follow" class="ghost" type="button" title="Toggle auto-follow">Follow</button>
            </div>
          </div>
          <div class="det-panels">
            <div id="det-panel-sync" class="det-panel" role="tabpanel" aria-labelledby="det-tab-sync">
              <div id="det-log" class="log"></div>
            </div>
            <div id="det-panel-watcher" class="det-panel hidden" role="tabpanel" aria-labelledby="det-tab-watcher">
              <div id="det-watch-log" class="log wlog"></div>
            </div>
          </div>

        </div>
        <div class="det-right">
          <div class="meta-card">
            <div class="meta-grid">
              <div class="meta-label">Module</div><div class="meta-value"><span id="det-cmd" class="pillvalue truncate">–</span></div>
              <div class="meta-label">Version</div><div class="meta-value"><span id="det-ver" class="pillvalue">–</span></div>
              <div class="meta-label">Started</div><div class="meta-value"><span id="det-start" class="pillvalue mono">–</span></div>
              <div class="meta-label">Finished</div><div class="meta-value"><span id="det-finish" class="pillvalue mono">–</span></div>
            </div>
            <div class="meta-actions"><button class="btn" onclick="copySummary(this)">Copy summary</button></div>
          </div>
        </div>
      </div>
    </div>
  </section>

  <section id="stats-card" class="card cw-main-card cw-main-card--stats">
    <div class="title">Statistics</div>
    <div class="cw-main-card-head cw-main-card-head--compact">
      <div class="cw-main-card-head-copy">
        <div class="cw-main-card-kicker">Statistics</div>
      </div>
    </div>

    <div class="stats-modern v2">
      <div class="now"><div class="label">Now</div><div id="stat-now" class="value" data-v="0">0</div><div class="chips"><span id="trend-week" class="chip trend flat">no change</span></div></div>
      <div class="facts">
        <div class="fact"><span class="k">Last Week</span><span id="stat-week" class="v" data-v="0">0</span></div>
        <div class="fact"><span class="k">Last Month</span><span id="stat-month" class="v" data-v="0">0</span></div>
        <div class="mini-legend"><span class="dot add"></span><span class="l">Added</span><span id="stat-added" class="n">0</span><span class="dot del"></span><span class="l">Removed</span><span id="stat-removed" class="n">0</span></div>
        <div class="stat-meter" aria-hidden="true"><span id="stat-fill"></span></div>
      </div>
    </div>

    <div class="stat-tiles" id="stat-providers"></div>

    <div class="stat-block">
      <div class="stat-block-header"><span class="pill plain">Recent syncs</span><button class="ghost refresh-insights" onclick="refreshInsights()" title="Refresh">⟲</button></div>
      <div id="sync-history" class="history-list"></div>
    </div>
  </section>

  <section id="placeholder-card" class="card cw-main-card cw-main-card--wall hidden">
    <div class="title">Watchlist Preview</div>
    <div class="cw-main-card-head cw-main-card-head--compact">
      <div class="cw-main-card-head-copy">
        <div class="cw-main-card-kicker">Watchlist Preview</div>
      </div>
    </div>
    <div id="wall-msg" class="wall-msg">Loading…</div>
    <div class="wall-wrap">
      <div id="edgeL" class="edge left"></div><div id="edgeR" class="edge right"></div>
      <div id="poster-row" class="row-scroll" aria-label="Watchlist preview"></div>
      <button class="nav prev" type="button" onclick="scrollWall(-1)" aria-label="Scroll left">‹</button>
      <button class="nav next" type="button" onclick="scrollWall(1)" aria-label="Scroll right">›</button>
    </div>
  </section>

  <section id="page-watchlist" class="card hidden">
    <div class="title">Watchlist</div><div id="watchlist-root"></div>
  </section>

  <section id="page-snapshots" class="card hidden"></section>

  <section id="page-editor" class="card hidden"></section>

  <section id="page-settings" class="card hidden">
    <div id="cw-settings-shell">
      <aside id="cw-settings-nav" aria-label="Settings navigation">
        <div class="cw-settings-nav-card">
          <div class="cw-settings-nav-title">Settings</div>
        </div>

        <div class="cw-settings-nav-list" role="tablist" aria-label="Settings sections">
          <button type="button" class="cw-settings-nav-btn active" data-pane="overview" onclick="cwSettingsSelect?.('overview')">
            <span class="material-symbols-rounded">dashboard</span>
            <span><strong>Overview</strong><small>Health, status, quick actions</small></span>
          </button>
          <button type="button" class="cw-settings-nav-btn" data-pane="providers" onclick="cwSettingsSelect?.('providers')">
            <span class="material-symbols-rounded">hub</span>
            <span><strong>Providers</strong><small>Auth, sync and metadata</small></span>
          </button>
          <button type="button" class="cw-settings-nav-btn" data-pane="scheduling" onclick="cwSettingsSelect?.('scheduling')">
            <span class="material-symbols-rounded">schedule</span>
            <span><strong>Scheduling</strong><small>Standard and advanced jobs</small></span>
          </button>
          <button type="button" class="cw-settings-nav-btn" data-pane="scrobbler" onclick="cwSettingsSelect?.('scrobbler')">
            <span class="material-symbols-rounded">sensors</span>
            <span><strong>Scrobbler</strong><small>Webhook and watcher routes</small></span>
          </button>
          <button type="button" class="cw-settings-nav-btn" data-pane="app" onclick="cwSettingsSelect?.('app')">
            <span class="material-symbols-rounded">tune</span>
            <span><strong>UI settings</strong><small>UI, security and tracker</small></span>
          </button>
          <button type="button" class="cw-settings-nav-btn" data-pane="maintenance" onclick="cwSettingsSelect?.('maintenance')">
            <span class="material-symbols-rounded">build</span>
            <span><strong>Maintenance</strong><small>Debug and recovery tools</small></span>
          </button>
        </div>

      </aside>

      <div id="cw-settings-left">
        <section id="cw-settings-overview" class="cw-settings-pane active" data-pane="overview">
          <div id="cw-settings-overview-grid">
            <div class="cw-settings-overview-main">
              <section class="cw-settings-overview-card cw-settings-overview-actions">
                <div class="cw-settings-overview-head">
                  <div>
                    <div class="cw-settings-overview-kicker">Quick actions</div>
                    <h4>Jump straight to the settings</h4>
                  </div>
                </div>
                <div class="cw-settings-action-grid">
                  <button type="button" class="cw-settings-action" onclick="cwSettingsSelect?.('providers')">
                    <span class="material-symbols-rounded">hub</span>
                    <strong>Open providers</strong>
                    <small>Auth, sync pairs and metadata</small>
                  </button>
                  <button type="button" class="cw-settings-action" onclick="cwSettingsSelect?.('scheduling')">
                    <span class="material-symbols-rounded">schedule</span>
                    <strong>Open scheduling</strong>
                    <small>Standard and advanced scheduling jobs</small>
                  </button>
                  <button type="button" class="cw-settings-action" onclick="cwSettingsSelect?.('scrobbler')">
                    <span class="material-symbols-rounded">sensors</span>
                    <strong>Open scrobbler</strong>
                    <small>Watcher routes, webhook and filters</small>
                  </button>
                  <button type="button" class="cw-settings-action" onclick="cwSettingsSelect?.('app')">
                    <span class="material-symbols-rounded">tune</span>
                    <strong>Open UI settings</strong>
                    <small>UI, security and CW Tracker</small>
                  </button>
                </div>
              </section>

              <div class="cw-settings-overview-duo">
                <section class="cw-settings-overview-card">
                  <div class="cw-settings-overview-kicker">Recommended flow</div>
                  <h4>Configure in this order</h4>
                  <ol class="cw-settings-flow-list">
                    <li><strong>Connect providers first</strong><span>Configure auth providers, optional profiles and metadata keys in place.</span></li>
                    <li><strong>Build sync pairs</strong><span>Create optional source/target pairs and confirm the direction before enabling automation.</span></li>
                    <li><strong>Then enable automation</strong><span>Scheduling and scrobbler if you're ready.</span></li>
                  </ol>
                </section>

                <section class="cw-settings-overview-card">
                  <div class="cw-settings-overview-kicker">Sanity checks</div>
                  <h4>Some good defaults</h4>
                  <ul class="cw-settings-check-list">
                    <li>Metadata works best when TMDb is configured.</li>
                    <li>Leave scheduling off until sync pairs are correct.</li>
                    <li>Use the floating save button after changes.</li>
                    <li>Sync pairs are optional. You can also rely solely on webhooks/watchers.</li>
                  </ul>
                </section>
              </div>
            </div>
            <aside id="cw-settings-insight" aria-label="Settings Insight"></aside>
          </div>
        </section>

        <section class="cw-settings-pane" data-pane="providers">
          <div class="cw-settings-pane-head">
            <div>
              <div class="cw-settings-pane-kicker">Providers</div>
              <h3>Authentication, synchronization and metadata</h3>
              <p>Start with authentication, then synchronization (if needed), and finally metadata.</p>
            </div>
            <div class="cw-settings-jumpbar" aria-label="Provider sections">
              <button type="button" class="cw-settings-jump" data-target="sec-auth" onclick="cwProvidersJump?.('sec-auth')">Authentication</button>
              <button type="button" class="cw-settings-jump" data-target="sec-sync" onclick="cwProvidersJump?.('sec-sync')">Synchronization</button>
              <button type="button" class="cw-settings-jump" data-target="sec-meta" onclick="cwProvidersJump?.('sec-meta')">Metadata</button>
            </div>
          </div>
          <div class="cw-settings-pane-stack cw-settings-providers-stack">
            <div class="section cw-settings-section cw-settings-provider-section" id="sec-auth">
              <div class="head" onclick="toggleSection('sec-auth')">
                <span class="chev">▶</span><strong>Authentication Providers</strong>
                <span id="auth-providers-icons" class="cw-provider-head-icons">
                  <img data-prov="PLEX" src="/assets/img/PLEX-log.svg" alt="Plex" class="cw-provider-head-icon">
                  <img data-prov="JELLYFIN" src="/assets/img/JELLYFIN-log.svg" alt="Jellyfin" class="cw-provider-head-icon">
                  <img data-prov="SIMKL" src="/assets/img/SIMKL-log.svg" alt="SIMKL" class="cw-provider-head-icon">
                  <img data-prov="TRAKT" src="/assets/img/TRAKT-log.svg" alt="Trakt" class="cw-provider-head-icon">
                  <img data-prov="MDBLIST" src="/assets/img/MDBLIST-log.svg" alt="MDBList" class="cw-provider-head-icon">
                  <img data-prov="TMDB" src="/assets/img/TMDB-log.svg" alt="TMDb" class="cw-provider-head-icon cw-provider-head-icon--tmdb">
                  <img data-prov="TAUTULLI" src="/assets/img/TAUTULLI-log.svg" alt="TAUTULLI" class="cw-provider-head-icon">
                  <img data-prov="ANILIST" src="/assets/img/ANILIST-log.svg" alt="AniList" class="cw-provider-head-icon">
                  <img data-prov="EMBY" src="/assets/img/EMBY-log.svg" alt="Emby" class="cw-provider-head-icon cw-provider-head-icon--emby">
                </span>
              </div>
              <div class="body"><div id="auth-providers"></div></div>
            </div>

            <div class="section cw-settings-section cw-settings-provider-section" id="sec-sync">
              <div class="head" onclick="toggleSection('sec-sync')"><span class="chev">▶</span><strong>Synchronization Providers</strong></div>
              <div class="body">
                <div class="sub">Providers</div><div id="providers_list" class="grid2"></div>
                <div class="sep"></div><div class="sub">Pairs</div><div id="pairs_list"></div>
                <div class="footer"><div class="pair-selectors" style="margin-top:1em;">
                  <label for="source-provider" style="margin-right:1em;">Source:</label><select id="source-provider" name="source_provider" style="margin-left:.5em;"></select>
                  <label for="target-provider">Target:</label><select id="target-provider" name="target_provider" style="margin-left:.5em;"></select>
                </div></div>
              </div>
            </div>

            <div class="section cw-settings-section cw-settings-provider-section" id="sec-meta"><div class="head" onclick="toggleSection('sec-meta')"><span class="chev">▶</span><strong>Metadata Providers</strong></div><div class="body">
<div id="metadata-providers">
  <div class="cw-settings-hub" id="meta_provider_tiles">
    <button type="button" class="cw-hub-tile tmdb" data-provider="tmdb" aria-selected="false">
      <div class="cw-meta-provider-row">
        <div class="cw-hub-title">TMDb</div>
        <span class="auth-dot" id="meta-tmdb-dot" aria-hidden="true"></span>
      </div>
      <span class="hidden" id="hub_tmdb_key" aria-hidden="true">API key: —</span>
    </button>
  </div>

  <div id="meta-provider-panel" class="cw-panel hidden"></div>
  <div id="meta-provider-raw" class="hidden"></div>
</div>
</div></div>
          </div>
        </section>

        <section class="cw-settings-pane" data-pane="scheduling">
          <div class="cw-settings-pane-head">
            <div>
              <div class="cw-settings-pane-kicker">Scheduling</div>
              <h3>Run automation</h3>
              <p>Use standard for simple scheduling tasks or advanced for pair-based scheduling</p>
            </div>
          </div>
          <div class="section open cw-settings-section" id="sec-scheduling" data-accordion="off">
            <div class="head"><span class="chev">▶</span><strong>Scheduling</strong></div>
            <div class="body">
              <div id="sched-provider-panel" class="cw-panel hidden"></div>
              <div id="sched-provider-raw" class="hidden">
                <div class="grid2">
                  <div><label for="schEnabled">Enable</label><select id="schEnabled" name="schEnabled"><option value="false">Disabled</option><option value="true">Enabled</option></select></div>
                  <div><label for="schMode">Frequency</label><select id="schMode" name="schMode"><option value="hourly">Every hour</option><option value="every_n_hours">Every N hours</option><option value="daily_time">Daily at…</option></select></div>
                  <div><label for="schN">Every N hours</label><input id="schN" name="schN" type="number" min="1" max="24" value="2"></div>
                  <div><label for="schTime">Time</label><input id="schTime" name="schTime" type="time" value="03:30"></div>
                </div>
                <div id="sched_advanced_mount"></div>
              </div>
            </div>
          </div>
        </section>

        <section class="cw-settings-pane" data-pane="scrobbler">
          <div class="cw-settings-pane-head">
            <div>
              <div class="cw-settings-pane-kicker">Scrobbler</div>
              <h3>Webhook and watcher routing</h3>
              <p>Webhooks (legacy) and watcher mode with route ingestion and filters. Only one mode can be active.</p>
            </div>
          </div>
          <div id="sec-scrobbler" class="cw-settings-pane-stack cw-settings-scrobbler-stack" data-accordion="off">
            <div id="scrobble-mount" class="cw-settings-pane-stack cw-settings-scrobbler-stack-inner">
              <div class="section cw-settings-section cw-settings-provider-section" id="sc-sec-webhook">
                <div class="head" onclick="toggleSection('sc-sec-webhook')">
                  <span class="chev">▶</span><strong>Webhook</strong>
                </div>
                <div class="body"><div id="scrob-webhook"></div></div>
              </div>
              <div class="section open cw-settings-section cw-settings-provider-section" id="sc-sec-watch">
                <div class="head" onclick="toggleSection('sc-sec-watch')">
                  <span class="chev">▶</span><strong>Watcher</strong>
                </div>
                <div class="body"><div id="scrob-watcher"></div></div>
              </div>
            </div>
          </div>
        </section>

        <section class="cw-settings-pane" data-pane="app">
          <div class="cw-settings-pane-head">
            <div>
              <div class="cw-settings-pane-kicker">App settings</div>
              <h3>UI, security and CW Tracker</h3>
              <p>Configure CrossWatch settings and security.</p>
            </div>
          </div>
          <div class="section open cw-settings-section" id="sec-ui" data-accordion="off">
            <div class="head" style="display:flex;align-items:center">
              <span class="chev">▶</span>
              <strong>Settings (UI / Security / CW Tracker)</strong>
            </div>
            <div class="body">

              <div class="cw-settings-hub" id="ui_settings_hub">
                <button type="button" class="cw-hub-tile active" data-tab="ui" onclick="cwUiSettingsSelect?.('ui')">
                  <div class="cw-hub-title">User Interface</div>
                  <div class="cw-hub-desc">Dashboard visuals</div>
                  <div class="chips">
                    <span class="chip" id="hub_ui_watchlist">Watchlist: -</span>
                    <span class="chip" id="hub_ui_playing">Playing: -</span>
                    <span class="chip" id="hub_ui_askai">ASK AI: -</span>
                    <span class="chip" id="hub_ui_proto">Proto: -</span>
                  </div>
                </button>

                <button type="button" class="cw-hub-tile" data-tab="security" onclick="cwUiSettingsSelect?.('security')">
                  <div class="cw-hub-title">Security</div>
                  <div class="cw-hub-desc">Protect CrossWatch</div>
                  <div class="chips">
                    <span class="chip" id="hub_sec_auth">Auth: -</span>
                    <span class="chip" id="hub_sec_session">Session: -</span>
                    <span class="chip" id="hub_sec_proxy">Proxy: -</span>
                  </div>
                </button>

                <button type="button" class="cw-hub-tile" data-tab="tracker" onclick="cwUiSettingsSelect?.('tracker')">
                  <div class="cw-hub-title">CW Tracker</div>
                  <div class="cw-hub-desc">Local snapshots</div>
                  <div class="chips">
                    <span class="chip" id="hub_cw_enabled">Tracker: -</span>
                    <span class="chip" id="hub_cw_retention">Retention: -</span>
                  </div>
                </button>
              </div>

              <div class="cw-settings-panels" id="ui_settings_panels">

                <!-- Panel: User Interface -->
                <div class="cw-settings-panel active" data-tab="ui">
                  <div class="cw-panel-head">
                    <div>
                      <div class="cw-panel-title">User Interface</div>
                      <div class="sub" style="margin-top:0.25rem">Dashboard visuals.</div>
                    </div>
                  </div>

                  <div class="grid2">
                    <div>
                      <label for="ui_show_watchlist">Watchlist</label>
                      <select id="ui_show_watchlist" name="ui_show_watchlist">
                        <option value="true">Show</option>
                        <option value="false">Hide</option>
                      </select>
                    </div>

                    <div>
                      <label for="ui_show_playing">Playing card</label>
                      <select id="ui_show_playing" name="ui_show_playing">
                        <option value="true">Show</option>
                        <option value="false">Hide</option>
                      </select>
                    </div>

                    <div>
                      <label for="ui_show_AI">Help ASK AI</label>
                      <select id="ui_show_AI" name="ui_show_AI">
                        <option value="true">Show</option>
                        <option value="false">Hide</option>
                      </select>
                    </div>

                    <div>
                      <label for="ui_protocol">Protocol</label>
                      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
                        <select id="ui_protocol" name="ui_protocol" style="min-width:220px;flex:1">
                          <option value="http">HTTP</option>
                          <option value="https">HTTPS (self-signed)</option>
                        </select>
                        <button type="button" class="btn" id="ui_tls_advanced" onclick="openTlsCertModal?.()">Advanced</button>
                      </div>
                      <div class="sub" style="margin-top:0.25rem">
                        HTTPS uses a self-signed certificate, so your browser will warn unless you trust it.
                      </div>
                    </div>
                  </div>
                </div>

                <!-- Panel: Security -->
                <div class="cw-settings-panel" data-tab="security">
                  <div class="cw-panel-head">
                    <div>
                      <div class="cw-panel-title">Security</div>
                      <div class="sub" style="margin-top:0.25rem">
                        Sign-in authentication. Sessions are cached for 30 days.
                      </div>
                    </div>
                  </div>

                  <div class="grid2">
                    <div>
                      <label for="app_auth_enabled">Enabled</label>
                      <select id="app_auth_enabled" name="app_auth_enabled">
                        <option value="false">Disabled</option>
                        <option value="true">Enabled</option>
                      </select>
                    </div>

                    <div>
                      <label for="app_auth_username">Username</label>
                      <input id="app_auth_username" name="app_auth_username" type="text" autocomplete="username" placeholder="admin">
                    </div>
                  </div>

                  <div id="app_auth_fields" class="grid2" style="margin-top:12px">
                    <div>
                      <label for="app_auth_password">New password</label>
                      <input id="app_auth_password" name="app_auth_password" type="password" autocomplete="new-password" placeholder="(leave blank to keep)">
                      <div class="sub" style="margin-top:0.25rem">Leave blank to keep the current password</div>
                    </div>

                    <div>
                      <label for="app_auth_password2">Confirm password</label>
                      <input id="app_auth_password2" name="app_auth_password2" type="password" autocomplete="new-password" placeholder="(repeat)">
                    </div>
                  </div>

                  <div style="margin-top:10px;display:flex;gap:10px;align-items:center;flex-wrap:wrap">
                    <button class="btn" id="btn-auth-logout" onclick="cwAppLogout?.()">Log out</button>
                    <div class="sub" id="app_auth_state" style="margin:0">—</div>
                  </div>

                  <div style="margin-top:14px">
                    <label for="trusted_proxies">Trusted reverse proxies (optional)</label>
                    <input id="trusted_proxies" name="trusted_proxies" type="text" placeholder="127.0.0.1;192.168.2.1;192.168.2.0/16">
                    <div class="sub" style="margin-top:0.25rem">
                      Only needed when behind a reverse proxy and you want accurate IP-based login rate limiting.
                      Enter proxy IPs or CIDR ranges separated by <code>;</code>
                    </div>
                  </div>
                </div>

                <!-- Panel: CW Tracker -->
                <div class="cw-settings-panel" data-tab="tracker">
                  <div class="cw-panel-head">
                    <div>
                      <div class="cw-panel-title">CW Tracker</div>
                      <div class="sub" style="margin-top:0.25rem">
                        Local backup tracker for Watchlist, Ratings and History snapshots (stored under <code>/config/.cw_provider</code>).
                      </div>
                    </div>
                  </div>

                  <div class="grid2">
                    <div>
                      <label for="cw_enabled">Enabled</label>
                      <select id="cw_enabled" name="cw_enabled">
                        <option value="true">Enabled</option>
                        <option value="false">Disabled</option>
                      </select>
                    </div>

                    <div>
                      <label for="cw_retention_days">Retention (days)</label>
                      <input id="cw_retention_days" name="cw_retention_days" type="number" min="0" step="1" placeholder="30">
                      <div class="sub" style="margin-top:0.25rem">0 = keep snapshots forever</div>
                    </div>

                    <div>
                      <label for="cw_auto_snapshot">Auto snapshot</label>
                      <select id="cw_auto_snapshot" name="cw_auto_snapshot">
                        <option value="true">On (before writes)</option>
                        <option value="false">Off</option>
                      </select>
                    </div>

                    <div>
                      <label for="cw_max_snapshots">Max snapshots per feature</label>
                      <input id="cw_max_snapshots" name="cw_max_snapshots" type="number" min="0" step="1" placeholder="64">
                      <div class="sub" style="margin-top:0.25rem">0 = unlimited</div>
                    </div>
                  </div>

                  <div class="sub" style="margin-top:1.25rem">Restore snapshots</div>
                  <div class="grid2" id="cw_restore_fields">
                    <div>
                      <label for="cw_restore_watchlist">Watchlist snapshot</label>
                      <select id="cw_restore_watchlist" name="cw_restore_watchlist"></select>
                    </div>

                    <div>
                      <label for="cw_restore_history">History snapshot</label>
                      <select id="cw_restore_history" name="cw_restore_history"></select>
                    </div>

                    <div>
                      <label for="cw_restore_ratings">Ratings snapshot</label>
                      <select id="cw_restore_ratings" name="cw_restore_ratings"></select>
                    </div>
                  </div>

                  <div class="sub" style="margin-top:0.5rem">
                    Select <code>latest</code> to use the most recent snapshot, or choose a specific file name for each feature.
                  </div>
                </div>

              </div>
            </div>
          </div>
        </section>

        <section class="cw-settings-pane" data-pane="maintenance">
          <div class="cw-settings-pane-head">
            <div>
              <div class="cw-settings-pane-kicker">Maintenance</div>
              <h3>Maintenance zone, Debug and Restart</h3>
              <p>Use these actions to reset CrossWatch states. They are safe but cannot be undone.</p>
            </div>
          </div>
          <div class="section open cw-settings-section" id="sec-troubleshoot" data-accordion="off">
            <div class="head"><span class="chev">▶</span><strong>Maintenance</strong></div>
            <div class="body">
              <div>
                <label for="debug">Debug</label>
                <select id="debug" name="debug">
                  <option value="off">off</option>
                  <option value="on">on</option>
                  <option value="mods">on - including MOD debug - best option for debug</option>
                  <option value="full">on - full (requires restart) - use with caution</option>
                </select>
              </div>
              <div class="chiprow">
                <button class="btn danger" onclick="openMaintenanceModal()">Maintenance Tools</button>
                <button class="btn danger" onclick="restartCrossWatch()">Restart CrossWatch</button>
              </div>
              <div id="tb_msg" class="msg ok hidden">Done ✓</div>
            </div>
          </div>
        </section>
      </div>
    </div>
  </section>


</main>

<div id="cw-help-overlay" class="hidden" aria-hidden="true">
  <div id="cw-help-card">
    <button id="cw-help-close" class="btn" type="button" onclick="window.cwCloseHelp()">Close</button>
    <iframe id="cw-help-frame" title="CrossWatch Help" loading="lazy" referrerpolicy="no-referrer"></iframe>
  </div>
</div>


<script>window.__CW_BUILD__="0.2.5-20251014-02";</script>
<script>(()=>{const $=id=>document.getElementById(id),closeMenu=()=>{const m=$("cw-about-menu"),b=$("tab-about");m?.classList.add("hidden");b?.setAttribute("aria-expanded","false")},setHelp=open=>{const o=$("cw-help-overlay");if(!o)return;if(open){const f=$("cw-help-frame");if(f&&!f.src)f.src="https://wiki.crosswatch.app";o.classList.remove("hidden");o.setAttribute("aria-hidden","false")}else{o.classList.add("hidden");o.setAttribute("aria-hidden","true")}};window.APP_VERSION="__CW_VERSION__";window["__CW_"+"VERSION__"]=window.APP_VERSION;window.cwOpenHelp=()=>setHelp(true);window.cwCloseHelp=()=>setHelp(false);window.openHelp=()=>window.location?.protocol==="https:"?window.cwOpenHelp?.():window.open("https://wiki.crosswatch.app","_blank","noopener,noreferrer");window.cwCloseAboutMenu=closeMenu;window.cwToggleAboutMenu=e=>{e?.preventDefault?.();e?.stopPropagation?.();const m=$("cw-about-menu"),b=$("tab-about");if(!m||!b)return;const open=m.classList.contains("hidden");document.querySelectorAll(".cw-menu").forEach(x=>x.classList.add("hidden"));m.classList.toggle("hidden",!open);b.setAttribute("aria-expanded",String(open))};window.cwAboutMenuSelect=w=>(closeMenu(),w==="about"?window.openAbout?.():w==="help"?window.openHelp?.():undefined);document.addEventListener("click",e=>{const o=$("cw-help-overlay"),c=$("cw-help-card"),h=$("tab-about-menu");if(o&&!o.classList.contains("hidden")&&c&&!c.contains(e.target))window.cwCloseHelp?.();if(h&&!h.contains(e.target))closeMenu()},true);document.addEventListener("keydown",e=>{if(e.key!=="Escape")return;window.cwCloseHelp?.();closeMenu()},true)})();</script>

__CW_ASSET_BLOCK__

<script>document.addEventListener('DOMContentLoaded',()=>{try{if(typeof openSummaryStream==='function')openSummaryStream()}catch(e){}});</script>

<div id="save-frost" class="hidden" aria-hidden="true"></div>
<div id="save-fab" class="hidden" role="toolbar" aria-label="Sticky save"><button id="save-fab-btn" class="btn" onclick="saveSettings(this)"><span class="btn-ic">✔</span> <span class="btn-label">Save</span></button></div>

<script>(()=>{const list=p=>[...p.querySelectorAll(':scope>.section')].filter(s=>s.dataset.accordion!=='off'),set=(s,on)=>{s.classList.toggle('open',!!on);s.querySelector('.head')?.setAttribute('aria-expanded',String(!!on));const c=s.querySelector('.chev');if(c)c.textContent=on?'▼':'▶'},siblings=s=>s?.parentElement?list(s.parentElement):[];window.toggleSection=id=>{const s=document.getElementById(id);if(!s||s.dataset.accordion==='off')return;const on=!s.classList.contains('open');siblings(s).forEach(x=>set(x,x===s&&on))};window.openSection=id=>{const s=document.getElementById(id);if(!s||s.dataset.accordion==='off')return;siblings(s).forEach(x=>set(x,x===s))};document.addEventListener('DOMContentLoaded',()=>[...new Set([...document.querySelectorAll('.section')].map(s=>s.parentElement).filter(Boolean))].forEach(p=>{const open=list(p).find(s=>s.classList.contains('open'));list(p).forEach(s=>set(s,s===open))}),{once:true})})();</script>


<script>(()=>{const panes='#page-settings .cw-settings-pane',nav='#cw-settings-nav .cw-settings-nav-btn',norm=v=>String(v||'overview').trim().toLowerCase(),apply=p=>{const name=norm(p);let found=false;document.querySelectorAll(panes).forEach(n=>{const on=norm(n.dataset.pane)===name;n.classList.toggle('active',on);found=found||on});if(!found&&name!=='overview')return apply('overview');document.querySelectorAll(nav).forEach(b=>{const on=norm(b.dataset.pane)===name;b.classList.toggle('active',on);b.setAttribute('aria-current',on?'page':'false')});window.__cwSettingsPane=name;document.dispatchEvent(new CustomEvent('cw-settings-pane-changed',{detail:{pane:name}}))};window.cwSettingsSelect=p=>{apply(p);const main=document.getElementById('cw-settings-left');if(main&&window.innerWidth<1200)main.scrollIntoView({behavior:'smooth',block:'start'})};document.addEventListener('DOMContentLoaded',()=>apply(window.__cwSettingsPane||'overview'),{once:true});document.addEventListener('tab-changed',e=>((e?.detail?.id||e?.detail?.tab)==='settings')&&setTimeout(()=>apply(window.__cwSettingsPane||'overview'),0))})();</script>

<script>(()=>{window.cwProvidersJump=sectionId=>(window.cwSettingsSelect?.('providers'),setTimeout(()=>{window.openSection?.(sectionId);document.getElementById(sectionId)?.scrollIntoView({behavior:'smooth',block:'start'})},0))})();</script>

<script>(()=>{const $=id=>document.getElementById(id),txt=v=>String(v??'').trim(),up=v=>txt(v).toUpperCase(),mask=v=>v==='*****'||/^[•]+$/.test(v),pretty=v=>txt(v).toLowerCase()==='default'?'Default':txt(v),CROWN='<svg viewBox="0 0 64 64" fill="currentColor" aria-hidden="true"><path d="M8 20l10 8 10-14 10 14 10-8 4 26H4l4-26zM10 52h44v4H10z"/></svg>',LABELS={PLEX:'Plex',TRAKT:'Trakt',SIMKL:'SIMKL',ANILIST:'AniList',JELLYFIN:'Jellyfin',EMBY:'Emby',MDBLIST:'MDBlist',TMDB:'TMDb',TAUTULLI:'Tautulli'},AUTH_MAP=[['sec-plex','PLEX'],['sec-emby','EMBY'],['sec-jellyfin','JELLYFIN'],['sec-trakt','TRAKT'],['sec-simkl','SIMKL'],['sec-anilist','ANILIST'],['sec-mdblist','MDBLIST'],['sec-tmdb-sync','TMDB'],['sec-tautulli','TAUTULLI']];let cfg=null,lastCfg=null,authMo=null,metaMo=null;async function getConfig(force=false){if(cfg&&!force)return cfg;const prev=lastCfg||cfg;try{const r=await fetch('/api/config?ts='+Date.now(),{cache:'no-store'});if(r?.ok){const j=await r.json();return lastCfg=cfg=j&&typeof j==='object'?j:{}}}catch{}return cfg=prev||{}}window.invalidateConfigCache=()=>{cfg=null};function isProviderConfigured(key,c=cfg||{}){switch(up(key)){case'PLEX':return!!c?.plex?.account_token;case'TRAKT':return!!(c?.trakt?.access_token||c?.auth?.trakt?.access_token);case'SIMKL':return!!c?.simkl?.access_token;case'ANILIST':return!!(c?.anilist?.access_token||c?.auth?.anilist?.access_token);case'JELLYFIN':return!!c?.jellyfin?.access_token;case'EMBY':return!!(c?.emby?.access_token||c?.auth?.emby?.access_token);case'MDBLIST':return!!c?.mdblist?.api_key;case'TMDB':{const tm=c?.tmdb_sync||c?.auth?.tmdb_sync||{};return!!(tm?.account_id||tm?.session_id)}case'TAUTULLI':return!!((c?.tautulli?.server_url||c?.auth?.tautulli?.server_url)&&(c?.tautulli?.api_key||c?.auth?.tautulli?.api_key));default:return false}}const setDot=(id,on)=>{const sec=$(id),head=sec?.querySelector('.head')||sec?.firstElementChild;if(!head)return false;if(getComputedStyle(head).display!=='flex')Object.assign(head.style,{display:'flex',alignItems:'center'});const dot=head.querySelector('.auth-dot')||head.appendChild(Object.assign(document.createElement('span'),{className:'auth-dot'}));dot.classList.toggle('on',!!on);dot.title=on?'Configured':'Not configured';dot.setAttribute('aria-label',dot.title);return true};async function refreshAuthDots(force=false){const c=await getConfig(force),host=$('auth-providers-icons');host?.querySelectorAll('img[data-prov]').forEach(img=>img.style.display=isProviderConfigured(img.dataset.prov,c)?'inline-block':'none');return AUTH_MAP.reduce((any,[id,key])=>setDot(id,isProviderConfigured(key,c))||any,false)}window.refreshAuthDots=refreshAuthDots;function syncMetadataProviderDot(){const chip=$('hub_tmdb_key'),dot=$('meta-tmdb-dot'),tile=dot?.closest?.('.cw-hub-tile.tmdb');if(!chip||!dot||!tile)return false;const cfgKey=txt(cfg?.tmdb?.api_key),cfgHas=cfgKey.length>0||mask(cfgKey),keyEl=$('tmdb_api_key');let uiHas=false,touched=false;if(keyEl){const v=txt(keyEl.value);touched=keyEl.dataset?.touched==='1';uiHas=v.length>0||mask(v)||keyEl.dataset?.masked==='1';if(touched)uiHas=v.length>0||mask(v)}const raw=txt(chip.textContent).toLowerCase(),chipHas=/\bset\b/.test(raw)&&!/\bmissing\b|\bnot set\b|\bunset\b|\bempty\b|—/.test(raw),on=uiHas||(!touched&&(cfgHas||chipHas));dot.classList.toggle('on',on);dot.title=on?'Configured':'Not configured';dot.setAttribute('aria-label',dot.title);tile.classList.toggle('is-configured',on);return true}window.syncMetadataProviderDot=syncMetadataProviderDot;const observe=(hostId,slot,delay,fn,opts)=>{const host=$(hostId);if(!host)return slot==='meta'?setTimeout(()=>observe(hostId,slot,delay,fn,opts),150):undefined;fn();if((slot==='auth'&&authMo)||(slot==='meta'&&metaMo))return;const mo=new MutationObserver(()=>{clearTimeout(mo._t);mo._t=setTimeout(fn,delay)});mo.observe(host,opts);if(slot==='auth')authMo=mo;else metaMo=mo};document.addEventListener('settings-collect',()=>{refreshAuthDots(true).catch(()=>{});syncMetadataProviderDot()},true);document.addEventListener('tab-changed',()=>{refreshAuthDots(false).catch(()=>{});syncMetadataProviderDot()},true);document.addEventListener('DOMContentLoaded',()=>observe('hub_tmdb_key','meta',0,syncMetadataProviderDot,{childList:true,characterData:true,subtree:true}),{once:true});const titleCase=v=>{v=txt(v);return v?v[0]+v.slice(1).toLowerCase():v},instancesDetail=d=>{const inst=d&&typeof d==='object'?d.instances:null,sum=d&&typeof d==='object'?d.instances_summary:null;if(!inst||typeof inst!=='object')return'';const total=Number(sum?.total);if(!Number.isFinite(total)||total<=1)return'';const lines=[Number.isFinite(Number(sum?.ok))?`Profiles: ${Number(sum.ok)}/${total} connected`:`Profiles: ${total}`],used=Array.isArray(sum?.used)?sum.used:[];if(used.length){const labs=used.slice(0,4).map(pretty);lines.push(`Used: ${labs.join(', ')}${used.length>4?'…':''}`)}const entries=Object.entries(inst).slice(0,6).map(([id,v])=>`${pretty(id)}=${!!(v&&typeof v==='object'?v.connected:v)?'OK':'NO'}`);if(entries.length)lines.push(entries.join(' · '));const rep=txt(sum?.rep);if(rep&&rep.toLowerCase()!=='default')lines.push(`Rep: ${rep}`);return lines.join('\n')},usageDetail=d=>{const hint=txt(d?.usage_hint).replace(/\s*\+\s*/g,' and ');if(hint)return hint;const usedBy=Array.isArray(d?.used_by)?d.used_by:[];return usedBy.length?`Used by: ${usedBy.map(x=>txt(x).toLowerCase()==='pair'?'Sync':'Watcher').join(' and ')}`:''};function providerMeta(K,d){switch(up(K)){case'PLEX':{const vip=!!(d.plexpass||d.subscription?.plan);return{vip,detail:vip?`Plex Pass - ${d.subscription?.plan||'Active'}`:''}}case'TRAKT':{const vip=!!d.vip,wl=d?.limits?.watchlist||{},col=d?.limits?.collection||{},parts=[vip?'VIP status':'Free account'],add=(label,obj)=>{const used=Number(obj.used),max=Number(obj.item_count);if(Number.isFinite(used)&&Number.isFinite(max)&&max>0)parts.push(`${label}: ${used}/${max}`)};add('Watchlist',wl);add('Collection',col);if(d?.last_limit_error?.feature&&d?.last_limit_error?.ts)parts.push(`Last limit: ${d.last_limit_error.feature} @ ${d.last_limit_error.ts}`);return{vip,detail:parts.join(' · ')}}case'EMBY':return{vip:!!d.premiere,detail:d.premiere?'Premiere — Active':''};case'MDBLIST':{const vip=!!d.vip,lim=d?.limits||{},used=Number(lim.api_requests_count),max=Number(lim.api_requests),pat=txt(d.patron_status);return{vip,detail:`API requests: ${Number.isFinite(used)?used.toLocaleString():'-'}/${Number.isFinite(max)?max.toLocaleString():'-'}${pat?` - Status: ${pat}`:''}`}}case'ANILIST':{const nm=d?.user?.name||d?.user?.username||d?.user?.id;return{vip:false,detail:nm?`User: ${nm}`:''}}default:return{vip:false,detail:''}}}const makeConn=({name,connected,vip,detail,key})=>{const wrap=document.createElement('div'),pill=document.createElement('div');wrap.className='conn-item';pill.className=`conn-pill ${connected?'ok':'no'}${vip?' has-vip':''}`;if(key)pill.dataset.prov=up(key||name);pill.role='status';pill.ariaLabel=`${name} ${connected?'connected':'disconnected'}`;if(detail)pill.title=detail;pill.innerHTML=`<div class="conn-brand"><span class="conn-logo"></span>${vip?`<span class="conn-slot">${CROWN}</span>`:''}</div><span class="conn-text"></span><span class="dot ${connected?'ok':'no'}" aria-hidden="true"></span>`;pill.querySelector('.conn-text').textContent=name;wrap.appendChild(pill);return wrap};function render(payload){const host=$('conn-badges'),btn=$('btn-status-refresh');if(!host)return;host.classList.add('vip-badges');if(btn&&host.contains(btn))host.removeChild(btn);host.querySelectorAll('.conn-item').forEach(n=>n.remove());const providers=payload?.providers||{},keys=Object.keys(providers).filter(k=>isProviderConfigured(k,cfg||{})).sort(),none=!keys.length;host.classList.toggle('hidden',none);if(none){const hdr=document.querySelector('.cw-main-card-head-actions')||document.querySelector('.ops-header');if(btn&&hdr)hdr.appendChild(btn);return}keys.map(K=>{const d=providers[K]||{},name=LABELS[K]||titleCase(K),connected=!!d.connected,meta=providerMeta(K,d),detail=connected?[instancesDetail(d),meta.detail,usageDetail(d)].filter(Boolean).join('\n'):(d.reason||`${name} not connected`),el=makeConn({name,connected,vip:meta.vip,detail,key:K});el.style.margin='0';return el}).forEach(el=>host.appendChild(el))}async function fetchAndRender(e,opts){e?.preventDefault?.();const btn=e?.currentTarget||$('btn-status-refresh');if(!btn||btn.dataset.busy==='1')return;btn.dataset.busy='1';btn.classList.remove('spinning');void btn.offsetWidth;btn.classList.add('spinning');btn.setAttribute('aria-busy','true');btn.disabled=true;const minSpin=new Promise(r=>setTimeout(r,600)),ctl=new AbortController(),t=setTimeout(()=>ctl.abort(),4500);try{await getConfig(true);refreshAuthDots(false).catch(()=>{});const r=await fetch(`/api/status${opts?.fresh===true?'?fresh=1':''}`,{cache:'no-store',signal:ctl.signal}),d=r.ok?await r.json():null;render(d?.providers?d:{providers:{}})}catch(err){console.error('Status refresh failed:',err);render({providers:{}})}finally{clearTimeout(t);await minSpin;btn.classList.remove('spinning');btn.removeAttribute('aria-busy');btn.disabled=false;delete btn.dataset.busy}}window.manualRefreshStatus=e=>fetchAndRender(e,{fresh:true});function init(){getConfig().catch(()=>{});const btn=$('btn-status-refresh');if(btn&&btn.dataset.boundClick!=='1'){btn.dataset.boundClick='1';btn.addEventListener('click',window.manualRefreshStatus)}observe('auth-providers','auth',200,()=>refreshAuthDots(false).catch(()=>{}),{childList:true,subtree:false});let tries=0;(function retry(){refreshAuthDots(false).then(ok=>ok||++tries>=50||setTimeout(retry,200)).catch(()=>++tries<50&&setTimeout(retry,200))})();fetchAndRender(null,{fresh:false})}document.readyState==='loading'?document.addEventListener('DOMContentLoaded',init,{once:true}):init()})();</script>

<script>(()=>{const $=id=>document.getElementById(id),fab=$('save-fab'),frost=$('save-frost'),page=$('page-settings'),tab=$('tab-settings'),visible=()=>{if(!page)return false;const cs=getComputedStyle(page);return!page.classList.contains('hidden')&&cs.display!=='none'&&cs.visibility!=='hidden'},update=()=>{const on=visible();fab?.classList.toggle('hidden',!on);frost?.classList.toggle('hidden',!on)},watch=(el,attrs)=>el&&new MutationObserver(update).observe(el,{attributes:true,attributeFilter:attrs});document.addEventListener('DOMContentLoaded',()=>{watch(page,['class','style']);watch(tab,['class']);update()},{once:true});document.addEventListener('tab-changed',update);window.addEventListener('hashchange',update);document.querySelector('.tabs')?.addEventListener('click',update,true)})();</script>

<script>(()=>{const install=()=>{const orig=window.saveSettings;if(typeof orig!=='function'||orig._wrapped)return;window.saveSettings=Object.assign(async function(btnOrEvent){const btn=btnOrEvent instanceof HTMLElement?btnOrEvent:document.getElementById('save-fab-btn');if(btn&&!btn.dataset.defaultHtml)btn.dataset.defaultHtml=btn.innerHTML;if(btn)btn.disabled=true;try{const ret=orig.apply(this,arguments);await(ret&&typeof ret.then==='function'?ret:Promise.resolve());window.invalidateConfigCache?.();window.manualRefreshStatus?.();if(btn){btn.innerHTML='Settings saved ✓';setTimeout(()=>{btn.innerHTML=btn.dataset.defaultHtml||'<span class="btn-ic">✔</span> <span class="btn-label">Save</span>';btn.disabled=false},1600)}return ret}catch(e){if(btn){btn.innerHTML='Save failed';setTimeout(()=>{btn.innerHTML=btn.dataset.defaultHtml||'<span class="btn-ic">✔</span> <span class="btn-label">Save</span>';btn.disabled=false},2000)}throw e}},{_wrapped:true})};document.readyState==='complete'?install():window.addEventListener('load',install,{once:true})})();</script>
<script>(()=>{const origShowTab=window.showTab,toggle=(id,on)=>document.getElementById(id)?.classList.toggle('hidden',!on),active=(id,on)=>document.getElementById(id)?.classList.toggle('active',!!on);window.showTab=name=>{if(typeof origShowTab==='function')try{origShowTab(name)}catch{}const tab=String(name||'main'),flags={main:tab==='main',watchlist:tab==='watchlist',snapshots:tab==='snapshots',editor:tab==='editor',settings:tab==='settings'};toggle('ops-card',flags.main);toggle('stats-card',flags.main);toggle('placeholder-card',flags.main);toggle('page-watchlist',flags.watchlist);toggle('page-snapshots',flags.snapshots);toggle('page-editor',flags.editor);toggle('page-settings',flags.settings);Object.entries(flags).forEach(([k,on])=>active(`tab-${k}`,on));try{document.dispatchEvent(new CustomEvent('tab-changed',{detail:{tab}}))}catch{}}})();</script>
</body></html>

"""

def get_index_html(include_gitbook_embed: bool = True, ui_show_ai: bool = True) -> str:
    html = _get_index_html_static().replace("__CW_ASSET_BLOCK__", _asset_block())
    core_script = '<script src="/assets/helpers/core.js?v=__CW_VERSION__"></script>'
    if core_script in html and "__cwUiShowAI" not in html:
        html = html.replace(core_script, f'<script>window.__cwUiShowAI={"true" if ui_show_ai else "false"};</script>\n{core_script}', 1)
    if include_gitbook_embed and "gitbook/embed/script.js" not in html:
        html = html.replace(core_script, f"{GITBOOK_EMBED_BLOCK}\n\n{core_script}", 1) if core_script in html else f"{html}\n\n{GITBOOK_EMBED_BLOCK}\n"
    return html.replace("__CW_VERSION__", CURRENT_VERSION)
