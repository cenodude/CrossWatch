# ui_frontend.py
# CrossWatch - UI Frontend Registration
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from pathlib import Path
import time

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response
from starlette.staticfiles import StaticFiles
from api.versionAPI import CURRENT_VERSION
from cw_platform.config_base import load_config

__all__ = ["register_assets_and_favicons", "register_ui_root", "get_index_html"]

_ASSET_VERSION_CACHE: dict[str, float | str] = {"ts": 0.0, "val": CURRENT_VERSION}

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
    "provider-meta.js", "icon-select.js", "scrobbler-ui.js", "scrobbler-user-picker.js", "page-loader.js", "dom.js", "events.js", "api.js", "core.js", "details-log.js",
    "watchlist-preview.js", "providers-ui.js", "settings-ui.js", "settings-save.js", "maintenance.js",
    "restart_apply.js",
)
_APP_SCRIPTS = (
    "syncbar.js", "main.js", "connections.overlay.js", "connections.pairs.overlay.js", "scheduler.js",
    "schedulerbanner.js", "playingcard.js", "insights.js", "main-status.js", "settings-insight.js", "scrobbler.js",
    "editor.js", "snapshots.js",
)
_AUTH_HEADER_ICONS = (
    {"prov": "PLEX", "label": "Plex"},
    {"prov": "JELLYFIN", "label": "Jellyfin"},
    {"prov": "SIMKL", "label": "SIMKL"},
    {"prov": "TRAKT", "label": "Trakt"},
    {"prov": "MDBLIST", "label": "MDBList"},
    {"prov": "TMDB", "label": "TMDb", "extra_class": "cw-provider-head-icon--tmdb"},
    {"prov": "TAUTULLI", "label": "TAUTULLI"},
    {"prov": "ANILIST", "label": "AniList"},
    {"prov": "EMBY", "label": "Emby", "extra_class": "cw-provider-head-icon--emby"},
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


def _auth_header_icons_html() -> str:
    lines: list[str] = []
    for item in _AUTH_HEADER_ICONS:
        prov = str(item["prov"])
        label = str(item["label"])
        extra = str(item.get("extra_class", "")).strip()
        cls = "cw-provider-head-icon"
        if extra:
            cls = f"{cls} {extra}"
        lines.append(
            f'<img data-prov="{prov}" src="/assets/img/{prov}-log.svg" alt="{label}" class="{cls}">'
        )
    return "\n".join(lines)


def _asset_version_token() -> str:
    now = time.time()
    cached_at = float(_ASSET_VERSION_CACHE.get("ts") or 0.0)
    cached_val = str(_ASSET_VERSION_CACHE.get("val") or CURRENT_VERSION)
    if now - cached_at < 2.0:
        return cached_val

    root = Path(__file__).resolve().parent
    latest_mtime = 0
    try:
        candidates = [root / "assets", root / "ui_frontend.py"]
        for candidate in candidates:
            if candidate.is_file():
                latest_mtime = max(latest_mtime, int(candidate.stat().st_mtime))
                continue
            if not candidate.exists():
                continue
            for path in candidate.rglob("*"):
                if path.is_file():
                    latest_mtime = max(latest_mtime, int(path.stat().st_mtime))
    except Exception:
        latest_mtime = 0

    token = f"{CURRENT_VERSION}.{latest_mtime}" if latest_mtime > 0 else CURRENT_VERSION
    _ASSET_VERSION_CACHE["ts"] = now
    _ASSET_VERSION_CACHE["val"] = token
    return token


def _get_index_html_static() -> str:
    return r"""<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CrossWatch</title>
<script>
(() => {
  const APP_NAME = "CrossWatch";
  const TITLES = {
    main: "Main",
    watchlist: "Watchlist",
    snapshots: "Captures",
    editor: "Editor",
    settings: "Settings",
  };

  const normalize = (value) => String(value || "").trim().toLowerCase();

  const setTitle = (page) => {
    const label = TITLES[normalize(page)];
    document.title = label ? `${label} | ${APP_NAME}` : APP_NAME;
  };

  const currentPage = () => {
    const dataTab = normalize(document.body?.dataset?.tab || document.documentElement?.dataset?.tab);
    if (dataTab) return dataTab;

    const activeTab = document.querySelector('.tabs .tab.active[id^="tab-"]')?.id || "";
    return normalize(activeTab.replace(/^tab-/, "")) || "main";
  };

  window.cwSetDocumentTitle = setTitle;
  document.addEventListener("DOMContentLoaded", () => setTitle(currentPage()), { once: true });
  document.addEventListener("tab-changed", (event) => setTitle(event?.detail?.id || event?.detail?.tab));
  document.addEventListener("cw-settings-pane-changed", () => {
    if (currentPage() === "settings") setTitle("settings");
  });
})();
</script>
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
header{background:radial-gradient(120% 150% at 0% 0%,rgba(72,80,120,.10),transparent 36%),radial-gradient(120% 150% at 100% 100%,rgba(52,58,86,.08),transparent 42%),var(--cw-ov-shell);border:0;box-shadow:none}
header .tab,header .cw-ui-btn,header .iconbtn,header .cw-menu{background:var(--cw-ov-shell-soft)!important;border-color:var(--cw-ov-border)!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.025),0 10px 22px rgba(0,0,0,.16)}
header .tab:hover,header .cw-ui-btn:hover,header .iconbtn:hover,header .cw-menu-item:hover{background:var(--cw-ov-shell-strong)!important;border-color:var(--cw-ov-border-strong)!important}
header .tab.active,header .cw-ui-btn.active{background:linear-gradient(180deg,rgba(49,55,78,.82),rgba(12,15,22,.98))!important;border-color:rgba(148,156,194,.18)!important;box-shadow:0 14px 28px rgba(0,0,0,.22),inset 0 1px 0 rgba(255,255,255,.04)}
#page-settings,#page-settings .cw-settings-nav-card,#page-settings .cw-settings-overview-card,#page-settings .cw-settings-pane-head,#page-settings .cw-settings-section,#page-settings .cw-settings-action,#page-settings .cw-settings-jump,#page-settings .cw-settings-nav-btn,#page-settings .cw-hub-tile,#page-settings .cw-settings-panel,#page-settings .cw-panel,#page-settings .cw-menu,#page-settings .cw-settings-setup-step,#page-settings .cw-settings-mini-action,#page-settings .cw-settings-hero-panel,#page-settings .cw-settings-metric{background:radial-gradient(125% 150% at 0% 0%,rgba(72,80,120,.10),transparent 38%),var(--cw-ov-shell)!important;border-color:var(--cw-ov-border)!important;box-shadow:var(--cw-ov-shadow)!important}
#page-settings .cw-settings-section .head,#page-settings .cw-settings-section .body,#page-settings .cw-settings-pane-stack,#page-settings .cw-settings-panels,#page-settings .cw-settings-hub{background:transparent!important}
#page-settings .cw-settings-nav-btn:hover,#page-settings .cw-settings-action:hover,#page-settings .cw-settings-jump:hover,#page-settings .cw-hub-tile:hover,#page-settings .cw-settings-setup-step:hover,#page-settings .cw-settings-mini-action:hover{background:radial-gradient(125% 150% at 0% 0%,rgba(82,90,132,.12),transparent 34%),var(--cw-ov-shell-strong)!important;border-color:var(--cw-ov-border-strong)!important;transform:translateY(-1px)}
#page-settings .cw-settings-nav-btn.active,#page-settings .cw-settings-jump.active,#page-settings .cw-hub-tile.active,#page-settings .cw-hub-tile[aria-selected="true"],#page-settings .cw-settings-panel.active{background:linear-gradient(180deg,rgba(20,24,34,.98),rgba(7,9,13,.99))!important;border-color:rgba(154,162,198,.18)!important;box-shadow:0 18px 34px rgba(0,0,0,.24),inset 0 1px 0 rgba(255,255,255,.04)!important}
#page-settings .cw-settings-hero{background:radial-gradient(125% 145% at 0% 0%,rgba(36,116,255,.18),transparent 38%),radial-gradient(115% 135% at 100% 100%,rgba(15,201,172,.10),transparent 48%),var(--cw-ov-shell)!important;border-color:rgba(96,132,255,.18)!important}
#page-settings .cw-settings-hero .cw-settings-hero-panel{background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.022))!important;border-color:rgba(132,182,255,.16)!important}
#page-settings .cw-settings-progress-track{background:rgba(255,255,255,.05)!important;border-color:rgba(255,255,255,.08)!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.03)!important}
#page-settings .cw-settings-progress-card,#page-settings .cw-settings-shortcuts-card{background:radial-gradient(125% 145% at 0% 0%,rgba(74,84,148,.10),transparent 36%),linear-gradient(180deg,rgba(255,255,255,.035),rgba(255,255,255,.015))!important;border-color:rgba(255,255,255,.09)!important;box-shadow:0 22px 42px rgba(0,0,0,.22),inset 0 1px 0 rgba(255,255,255,.03)!important}
#page-settings .cw-settings-nav-card{background:radial-gradient(120% 150% at 0% 0%,rgba(68,76,120,.07),transparent 34%),var(--cw-ov-shell-soft)!important;opacity:.94}
#page-settings .cw-settings-nav-btn{background:linear-gradient(180deg,rgba(255,255,255,.024),rgba(255,255,255,.012))!important;border-color:rgba(255,255,255,.07)!important}
#page-settings .cw-settings-nav-btn.active{background:radial-gradient(860px 240px at 4% 0%,rgba(124,92,255,.18),transparent 52%),linear-gradient(180deg,rgba(20,24,34,.98),rgba(7,9,13,.99))!important;border-color:rgba(156,140,255,.22)!important;box-shadow:0 0 0 1px rgba(124,92,255,.12),0 12px 22px rgba(0,0,0,.22)!important}
#page-settings .cw-settings-setup-step.is-done{background:radial-gradient(900px 220px at 0% 0%,rgba(44,144,110,.18),transparent 58%),var(--cw-ov-shell)!important;border-color:rgba(76,176,136,.26)!important}
#page-settings .cw-settings-setup-step.is-active{background:radial-gradient(900px 220px at 0% 0%,rgba(124,92,255,.18),transparent 58%),var(--cw-ov-shell)!important;border-color:rgba(150,132,255,.24)!important}
#page-settings input,#page-settings select,#page-settings textarea{background:rgba(4,6,10,.94)!important;border:1px solid rgba(255,255,255,.08)!important;color:var(--cw-ov-fg)!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.02)!important}
#page-settings input:focus,#page-settings select:focus,#page-settings textarea:focus{outline:none;box-shadow:0 0 0 3px rgba(112,122,170,.14),inset 0 1px 0 rgba(255,255,255,.03)!important;border-color:rgba(160,168,202,.18)!important;background:rgba(6,8,12,.98)!important}
#page-settings select option,#page-settings select optgroup{background:#06080c!important;color:#eef3ff!important}
#page-settings .sub,#page-settings p,#page-settings small,#page-settings label,#page-settings .cw-settings-pane-kicker,#page-settings .cw-settings-overview-kicker,#page-settings .cw-settings-jumpbar,#page-settings .cw-hub-desc{color:var(--cw-ov-soft)!important}
#page-settings h3,#page-settings h4,#page-settings strong,#page-settings .cw-panel-title,#page-settings .cw-settings-nav-title{color:var(--cw-ov-fg)!important}
#page-settings .chip,#page-settings .pill,#page-settings .cw-provider-head-icon,#page-settings .auth-dot{filter:saturate(.88)}
#cw-settings-menu.cw-menu,#cw-about-menu.cw-menu{position:absolute;top:calc(100% + 6px);right:0;min-width:190px;padding:6px;display:flex;flex-direction:column;gap:4px;border-radius:16px;border:1px solid rgba(255,255,255,.10);background:rgba(12,14,23,.96);box-shadow:0 18px 42px rgba(0,0,0,.38);backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px)}
#cw-settings-menu.cw-menu{right:-18px}
#cw-settings-menu .cw-menu-item,#cw-about-menu .cw-menu-item{display:flex;align-items:center;min-height:32px;padding:0 10px;border-radius:10px;border:1px solid transparent;background:rgba(255,255,255,.03);color:var(--fg);font-weight:700;font-size:13px}
#cw-settings-menu .cw-menu-item:hover,#cw-about-menu .cw-menu-item:hover{background:rgba(255,255,255,.07);border-color:rgba(124,92,255,.28)}
#cw-settings-menu .cw-menu-item.danger{color:rgba(255,186,194,.96);background:rgba(255,92,112,.08);border-color:rgba(255,92,112,.12)}
#cw-settings-menu .cw-menu-item.danger:hover{color:#fff1f4;background:rgba(255,92,112,.14);border-color:rgba(255,120,138,.28)}
#cw-settings-menu .cw-menu-sep{height:1px;margin:4px 2px;border:0;background:rgba(255,255,255,.08)}
.cw-field-inline-error{margin-top:8px;padding:10px 12px;border-radius:14px;border:1px solid rgba(255,120,120,.22);background:linear-gradient(180deg,rgba(7,9,13,.98),rgba(3,5,8,.99));color:rgba(245,247,255,.96);font-size:12px;line-height:1.45;box-shadow:0 14px 28px rgba(0,0,0,.24),inset 0 1px 0 rgba(255,255,255,.03)}
.cw-field-inline-error.hidden{display:none}
#page-settings .cw-settings-panel.cw-settings-shell{padding:18px;border-radius:26px;background:radial-gradient(120% 140% at 0% 0%,rgba(92,96,182,.12),transparent 38%),radial-gradient(90% 120% at 100% 100%,rgba(54,120,210,.08),transparent 48%),linear-gradient(180deg,rgba(11,14,21,.96),rgba(6,8,12,.985))!important;border:1px solid rgba(255,255,255,.08)!important;box-shadow:0 24px 46px rgba(0,0,0,.24),inset 0 1px 0 rgba(255,255,255,.03)!important}
#page-settings .cw-settings-panel.cw-settings-shell.active{display:grid}
#page-settings .cw-settings-shell .cw-panel-head{margin-bottom:0}
#page-settings .cw-settings-head{padding-bottom:2px}
#page-settings .cw-settings-copy{margin-top:10px;max-width:72ch}
#page-settings .cw-settings-layout{display:grid;gap:14px}
#page-settings .cw-settings-block{padding:16px 16px 14px;border-radius:22px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.02));box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
#page-settings .cw-settings-block-title{margin:0 0 12px;font-size:12px;font-weight:900;letter-spacing:.12em;text-transform:uppercase;color:rgba(224,230,246,.7)}
#page-settings .cw-settings-stack{display:grid;gap:10px}
#page-settings .cw-settings-2col{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
#page-settings .cw-settings-split{display:grid;grid-template-columns:1.15fr .85fr;gap:12px}
#page-settings .cw-settings-statusrow{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;padding:14px 16px;border-radius:20px;border:1px solid rgba(255,255,255,.08);background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.018))}
#page-settings .cw-settings-status{display:grid;gap:3px;min-width:220px}
#page-settings .cw-settings-status strong{font-size:12px;letter-spacing:.12em;text-transform:uppercase;color:rgba(228,234,248,.72)}
#page-settings .cw-settings-status .sub{margin:0!important}
#page-settings .cw-settings-shell label{font-size:12px;font-weight:900;letter-spacing:.08em;text-transform:uppercase;color:rgba(226,232,248,.74)!important}
#page-settings .cw-settings-shell input,#page-settings .cw-settings-shell select{height:34px;min-height:34px;padding:0 12px;border-radius:12px!important;background:linear-gradient(180deg,rgba(3,5,9,.96),rgba(1,3,6,.985))!important;color:#eef3ff!important;line-height:1.1;font-size:14px}
#page-settings .cw-settings-shell textarea{min-height:96px;padding:10px 12px;border-radius:12px!important;background:linear-gradient(180deg,rgba(3,5,9,.96),rgba(1,3,6,.985))!important;color:#eef3ff!important;line-height:1.4;font-size:14px}
#page-settings .cw-settings-shell input::placeholder,#page-settings .cw-settings-shell textarea::placeholder{color:rgba(196,204,222,.42)}
#page-settings .cw-settings-shell .sub{line-height:1.5}
#page-settings .cw-settings-shell .btn{min-height:46px;border-radius:16px}
#page-settings .cw-settings-shell .btn.primary,#page-settings .cw-settings-shell #btn-auth-logout,#page-settings .cw-settings-shell #btn-auth-logout-others{min-width:144px;background:linear-gradient(135deg,rgba(86,60,180,.42),rgba(56,106,208,.42))!important;border-color:rgba(124,92,255,.24)!important;box-shadow:0 14px 28px rgba(22,24,40,.24)}
#page-settings .cw-settings-shell .btn.primary:hover,#page-settings .cw-settings-shell #btn-auth-logout:hover,#page-settings .cw-settings-shell #btn-auth-logout-others:hover{filter:brightness(1.05)}
#page-settings .cw-settings-inline-action{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
#page-settings .cw-settings-panel.cw-settings-shell[data-tab="security"]{background:radial-gradient(120% 140% at 0% 0%,rgba(124,92,255,.16),transparent 38%),linear-gradient(180deg,rgba(11,14,21,.96),rgba(6,8,12,.985))!important}
#page-settings .cw-settings-panel.cw-settings-shell[data-tab="tracker"]{background:radial-gradient(120% 140% at 0% 0%,rgba(45,161,255,.14),transparent 38%),linear-gradient(180deg,rgba(11,14,21,.96),rgba(6,8,12,.985))!important}
#page-settings .cw-settings-hub{gap:14px;align-items:start}
#page-settings .cw-hub-tile{position:relative;overflow:hidden;min-height:148px;padding:18px;border-radius:24px;display:grid;align-content:start}
#page-settings .cw-hub-tile::before{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(135deg,rgba(255,255,255,.05),transparent 42%)}
#page-settings .cw-hub-tile>*{position:relative;z-index:1}
#page-settings .cw-hub-tile[data-tab="ui"]{background:radial-gradient(125% 145% at 0% 0%,rgba(92,96,182,.16),transparent 40%),linear-gradient(180deg,rgba(11,14,21,.96),rgba(6,8,12,.985))!important}
#page-settings .cw-hub-tile[data-tab="security"]{background:radial-gradient(125% 145% at 0% 0%,rgba(124,92,255,.18),transparent 40%),linear-gradient(180deg,rgba(11,14,21,.96),rgba(6,8,12,.985))!important}
#page-settings .cw-hub-tile[data-tab="tracker"]{background:radial-gradient(125% 145% at 0% 0%,rgba(45,161,255,.16),transparent 40%),linear-gradient(180deg,rgba(11,14,21,.96),rgba(6,8,12,.985))!important}
#page-settings .cw-hub-top{display:flex;align-items:flex-start;gap:14px}
#page-settings .cw-hub-icon{width:44px;height:44px;flex:0 0 44px;display:grid;place-items:center;border-radius:14px;border:1px solid rgba(255,255,255,.10);background:linear-gradient(180deg,rgba(255,255,255,.07),rgba(255,255,255,.03));box-shadow:inset 0 1px 0 rgba(255,255,255,.05)}
#page-settings .cw-hub-icon .material-symbols-rounded{font-size:21px;line-height:1;color:#f1f5ff}
#page-settings .cw-hub-copy{min-width:0;display:grid;align-content:start;gap:4px;padding-top:1px}
#page-settings .cw-hub-title{font-size:15px;font-weight:850;letter-spacing:-.01em}
#page-settings .cw-hub-desc{margin-top:0;font-size:12px;line-height:1.3}
#page-settings .cw-hub-tile .chips{margin-top:14px;align-self:start}
#page-settings .cw-hub-tile .chip{padding:6px 10px;border-radius:999px;border-color:rgba(255,255,255,.09);background:rgba(0,0,0,.24);font-size:12px}
@media (max-width:900px){#page-settings .cw-settings-2col,#page-settings .cw-settings-split{grid-template-columns:1fr}}
@media (max-width:640px){#page-settings .cw-settings-statusrow{align-items:stretch}#page-settings .cw-settings-status{min-width:0}#page-settings .cw-settings-shell .btn.primary,#page-settings .cw-settings-shell #btn-auth-logout,#page-settings .cw-settings-shell #btn-auth-logout-others,#page-settings .cw-settings-inline-action .btn{width:100%}}
</style>
<script>
(() => {
  try {
    const q = new URLSearchParams(window.location.search || "");
    const ui = String(q.get("ui") || "").toLowerCase();
    const explicit = ui === "compact" || ui === "full" || q.get("compact") === "1" || q.get("full") === "1";
    const hasCompactViewport = !!window.matchMedia?.("(max-width: 680px)")?.matches;
    const uaLooksMobile = /Android|iPhone|iPad|iPod/i.test(navigator.userAgent || "");
    const likelyHandheld = (() => {
      try {
        if (typeof navigator.userAgentData?.mobile === "boolean") return navigator.userAgentData.mobile;
      } catch {}
      if (uaLooksMobile) return true;
      try {
        const coarse = !!window.matchMedia?.("(pointer: coarse)")?.matches;
        const fine = !!window.matchMedia?.("(pointer: fine)")?.matches;
        const points = Number(navigator.maxTouchPoints || 0);
        return points > 1 && coarse && !fine && hasCompactViewport;
      } catch {}
      return false;
    })();
    const wantCompact = ui === "compact" || q.get("compact") === "1" || (!explicit && hasCompactViewport && likelyHandheld);
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
      <span class="version">__CW_CURRENT_VERSION__</span>
    </span>
  </div>

  <nav class="tabs" aria-label="Primary navigation">
    <button id="tab-main" class="tab active" type="button" onclick="showTab('main')">Main</button>
    <button id="tab-watchlist" class="tab" type="button" onclick="showTab('watchlist')">Watchlist</button>
    <button id="tab-snapshots" class="tab" type="button" onclick="showTab('snapshots')">Captures</button>
    <button id="tab-editor" class="tab" type="button" onclick="showTab('editor')">Editor</button>
    <div class="cw-tabmenu" id="tab-settings-menu">
      <button id="tab-settings" class="tab" type="button"
              aria-haspopup="menu" aria-expanded="false"
              onclick="window.cwToggleSettingsMenu(event)">
        <span>Settings</span>
        <span class="tab-caret" aria-hidden="true">▾</span>
      </button>
      <div class="cw-menu hidden" id="cw-settings-menu" role="menu" aria-labelledby="tab-settings">
        <button class="cw-menu-item" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect('overview')">Settings overview</button>
        <div class="cw-menu-sep" role="separator" aria-hidden="true"></div>
        <button class="cw-menu-item" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect('providers')">Connections</button>
        <button class="cw-menu-item" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect('pairs')">Sync pairs</button>
        <button class="cw-menu-item" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect('scrobbler')">Scrobbler</button>
        <button class="cw-menu-item" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect('scheduling')">Scheduling</button>
        <button class="cw-menu-item" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect('app')">UI and Security</button>
        <button class="cw-menu-item" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect('maintenance')">Maintenance</button>
        <div class="cw-menu-sep" role="separator" aria-hidden="true"></div>
        <button class="cw-menu-item danger" type="button" role="menuitem" onclick="window.cwSettingsMenuLogout()">Log out</button>
      </div>
    </div>
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
    <div class="title">Sync Hub</div>
    <div class="ops-header cw-main-card-head">
      <div class="cw-main-card-head-copy">
        <h2>Sync Hub</h2>
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
            <span><strong>Setup</strong><small>Progress, status and next steps</small></span>
          </button>
          <button type="button" class="cw-settings-nav-btn" data-pane="providers" onclick="cwSettingsSelect?.('providers')">
            <span class="material-symbols-rounded">hub</span>
            <span><strong>Connections</strong><small>Providers, pairs and metadata</small></span>
          </button>
          <button type="button" class="cw-settings-nav-btn" data-pane="scrobbler" onclick="cwSettingsSelect?.('scrobbler')">
            <span class="material-symbols-rounded">sensors</span>
            <span><strong>Scrobbler</strong><small>Webhook and watcher routes</small></span>
          </button>
          <button type="button" class="cw-settings-nav-btn" data-pane="scheduling" onclick="cwSettingsSelect?.('scheduling')">
            <span class="material-symbols-rounded">schedule</span>
            <span><strong>Scheduling</strong><small>Standard and advanced jobs</small></span>
          </button>
          <button type="button" class="cw-settings-nav-btn" data-pane="app" onclick="cwSettingsSelect?.('app')">
            <span class="material-symbols-rounded">tune</span>
            <span><strong>UI and Security</strong><small>Interface, auth and tracker</small></span>
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
              <section class="cw-settings-overview-card cw-settings-hero">
                <div class="cw-settings-hero-main">
                  <div class="cw-settings-overview-kicker">Getting started</div>
                  <h4 id="cw-settings-hero-title">Set up CrossWatch</h4>
                  <p id="cw-settings-hero-copy">Connect services, add metadata, then choose sync pairs, scrobbler, or both.</p>
                  <div class="cw-settings-hero-progress">
                    <div class="cw-settings-hero-progress-top">
                      <span class="cw-settings-hero-progress-label">Completion</span>
                      <span class="cw-settings-hero-progress-value" id="cw-settings-progress-text">0 of 4 steps ready</span>
                    </div>
                    <div class="cw-settings-progress-track" aria-hidden="true">
                      <span id="cw-settings-progress-bar"></span>
                    </div>
                  </div>
                  <div class="cw-settings-hero-actions">
                    <button type="button" class="btn primary cw-settings-hero-btn" id="cw-settings-primary-cta" onclick="cwSettingsOverviewGo?.('primary')">Continue setup</button>
                    <button type="button" class="btn cw-settings-hero-btn" id="cw-settings-scrobbler-cta" onclick="cwSettingsOverviewGo?.('scrobbler')">Open scrobbler</button>
                  </div>
                </div>
                <div class="cw-settings-hero-panel">
                  <div class="cw-settings-hero-panel-kicker">Live snapshot</div>
                  <div class="cw-settings-hero-panel-title">What is already configured</div>
                  <div class="cw-settings-metric-grid">
                    <div class="cw-settings-metric">
                      <span class="cw-settings-metric-label">Connected services</span>
                      <strong id="cw-settings-stat-auth">0</strong>
                      <small id="cw-settings-stat-auth-copy">No providers connected yet</small>
                    </div>
                    <div class="cw-settings-metric">
                      <span class="cw-settings-metric-label">Sync pairs</span>
                      <strong id="cw-settings-stat-pairs">0</strong>
                      <small id="cw-settings-stat-pairs-copy">No synchronization pairs yet</small>
                    </div>
                    <div class="cw-settings-metric">
                      <span class="cw-settings-metric-label">Automation</span>
                      <strong id="cw-settings-stat-automation">Off</strong>
                      <small id="cw-settings-stat-automation-copy">Scheduling and scrobbler are idle</small>
                    </div>
                  </div>
                </div>
              </section>

              <section class="cw-settings-overview-card cw-settings-progress-card">
                <div class="cw-settings-overview-head cw-settings-overview-head--stack">
                  <div>
                    <div class="cw-settings-overview-kicker">Setup</div>
                    <h4>What to set up</h4>
                    <p>These are the main things most people use.</p>
                  </div>
                </div>
                <div class="cw-settings-setup-grid">
                  <article class="cw-settings-setup-step" data-step="auth" role="button" tabindex="0" onclick="cwSettingsOverviewGo?.('auth')" onkeydown="cwSettingsStepKey?.(event,'auth')">
                    <span class="cw-settings-setup-step-top">
                      <span class="cw-settings-step-index">01</span>
                      <span class="cw-settings-step-state" id="cw-settings-step-auth-state">Needs setup</span>
                    </span>
                    <strong>Connect services</strong>
                    <span class="cw-settings-step-copy" id="cw-settings-step-auth-copy">Link Plex, Jellyfin, Emby, or your trackers.</span>
                    <span class="cw-settings-step-links">
                      <button type="button" class="cw-settings-step-link" id="cw-settings-step-auth-link" onclick="event.stopPropagation(); cwSettingsOverviewGo?.('auth')">Open connections</button>
                    </span>
                  </article>
                  <article class="cw-settings-setup-step" data-step="meta" role="button" tabindex="0" onclick="cwSettingsOverviewGo?.('meta')" onkeydown="cwSettingsStepKey?.(event,'meta')">
                    <span class="cw-settings-setup-step-top">
                      <span class="cw-settings-step-index">02</span>
                      <span class="cw-settings-step-state" id="cw-settings-step-meta-state">Missing</span>
                    </span>
                    <strong>Add metadata</strong>
                    <span class="cw-settings-step-copy" id="cw-settings-step-meta-copy">Bring in TMDb so matching and enrichment feel complete.</span>
                    <span class="cw-settings-step-links">
                      <button type="button" class="cw-settings-step-link" id="cw-settings-step-meta-link" onclick="event.stopPropagation(); cwSettingsOverviewGo?.('meta')">Open metadata</button>
                    </span>
                  </article>
                  <article class="cw-settings-setup-step" data-step="sync" role="button" tabindex="0" onclick="cwSettingsOverviewGo?.('sync')" onkeydown="cwSettingsStepKey?.(event,'sync')">
                    <span class="cw-settings-setup-step-top">
                      <span class="cw-settings-step-index">03</span>
                      <span class="cw-settings-step-state" id="cw-settings-step-sync-state">Needs setup</span>
                    </span>
                    <strong>Sync pairs or scrobbler</strong>
                    <span class="cw-settings-step-copy" id="cw-settings-step-sync-copy">You can use sync pairs, scrobbler, or both. Most people set up at least one.</span>
                    <span class="cw-settings-step-links">
                      <button type="button" class="cw-settings-step-link" id="cw-settings-step-sync-link" onclick="event.stopPropagation(); cwSettingsOverviewGo?.('sync')">Sync pairs</button>
                      <button type="button" class="cw-settings-step-link" id="cw-settings-step-sync-alt-link" onclick="event.stopPropagation(); cwSettingsOverviewGo?.('scrobbler')">Scrobbler</button>
                    </span>
                  </article>
                  <article class="cw-settings-setup-step" data-step="scheduling" role="button" tabindex="0" onclick="cwSettingsOverviewGo?.('scheduling')" onkeydown="cwSettingsStepKey?.(event,'scheduling')">
                    <span class="cw-settings-setup-step-top">
                      <span class="cw-settings-step-index">04</span>
                      <span class="cw-settings-step-state" id="cw-settings-step-scheduling-state">Optional</span>
                    </span>
                    <strong>Scheduling and/or scrobbler</strong>
                    <span class="cw-settings-step-copy" id="cw-settings-step-scheduling-copy">Optional. Turn on scheduling, scrobbler, or both.</span>
                    <span class="cw-settings-step-links">
                      <button type="button" class="cw-settings-step-link" id="cw-settings-step-scheduling-link" onclick="event.stopPropagation(); cwSettingsOverviewGo?.('scheduling')">Open scheduling</button>
                    </span>
                  </article>
                </div>
              </section>

            </div>
            <aside id="cw-settings-insight" aria-label="Settings Insight"></aside>
          </div>
        </section>

        <section class="cw-settings-pane" data-pane="providers">
          <div class="cw-settings-pane-head">
            <div>
              <div class="cw-settings-pane-kicker">Connections</div>
              <h3>Providers, sync pairs and metadata</h3>
              <p>Connect services first, then shape how data moves between them.</p>
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
                <span class="chev">▶</span><strong>Authentication</strong>
                <span id="auth-providers-icons" class="cw-provider-head-icons">__CW_AUTH_HEADER_ICONS__</span>
              </div>
              <div class="body"><div id="auth-providers"></div></div>
            </div>

            <div class="section cw-settings-section cw-settings-provider-section" id="sec-sync">
              <div class="head" onclick="toggleSection('sec-sync')"><span class="chev">▶</span><strong>Synchronization</strong></div>
              <div class="body">
                <div class="sub">Providers</div><div id="providers_list" class="grid2"></div>
                <div class="sep"></div><div class="sub">Pairs</div><div id="pairs_list"></div>
                <div class="footer"><div class="pair-selectors" style="margin-top:1em;">
                  <label for="source-provider" style="margin-right:1em;">Source:</label><select id="source-provider" name="source_provider" style="margin-left:.5em;"></select>
                  <label for="target-provider">Target:</label><select id="target-provider" name="target_provider" style="margin-left:.5em;"></select>
                </div></div>
              </div>
            </div>

            <div class="section cw-settings-section cw-settings-provider-section" id="sec-meta"><div class="head" onclick="toggleSection('sec-meta')"><span class="chev">▶</span><strong>Metadata</strong></div><div class="body">
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
            <div class="cw-settings-pane-head-actions">
              <div class="cw-settings-jumpbar" id="sched-pane-tabs" aria-label="Scheduling sections">
                <button type="button" class="cw-settings-jump active" data-sub="basic">Standard</button>
                <button type="button" class="cw-settings-jump" data-sub="advanced">Advanced</button>
              </div>
            </div>
          </div>
          <div class="section open cw-settings-section" id="sec-scheduling" data-accordion="off">
            <div class="head"><span class="chev">▶</span><strong>Scheduling</strong></div>
            <div class="body">
              <div id="sched-provider-panel" class="cw-panel hidden"></div>
              <div id="sched-provider-raw" class="hidden">
                <div class="grid2">
                  <div><label for="schEnabled">Enable</label><select id="schEnabled" name="schEnabled"><option value="false">Disabled</option><option value="true">Enabled</option></select></div>
                  <div><label for="schMode">Frequency</label><select id="schMode" name="schMode"><option value="hourly">Every hour</option><option value="every_n_hours">Every N hours</option><option value="daily_time">Daily at…</option><option value="custom_interval">Custom</option></select></div>
                  <div><label for="schN">Every N hours</label><input id="schN" name="schN" type="number" min="2" value="12"></div>
                  <div><label for="schTime">Time</label><input id="schTime" name="schTime" type="time" value="03:30"></div>
                  <div><label for="schCustomValue">Custom interval</label><input id="schCustomValue" name="schCustomValue" type="number" min="15" step="15" value="60"></div>
                  <div><label for="schCustomUnit">Custom unit</label><select id="schCustomUnit" name="schCustomUnit"><option value="minutes">Minutes</option><option value="hours">Hours</option></select></div>
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
            <div class="cw-settings-jumpbar" aria-label="Scrobbler sections">
              <button type="button" class="cw-settings-jump" data-target="sc-sec-webhook" onclick="cwScrobblerJump?.('sc-sec-webhook')">Webhook</button>
              <button type="button" class="cw-settings-jump" data-target="sc-sec-watch" onclick="cwScrobblerJump?.('sc-sec-watch')">Watcher</button>
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
              <div class="cw-settings-pane-kicker">UI and Security</div>
              <h3>Interface, authentication and CW Tracker</h3>
              <p>Shape the experience, lock things down, and manage tracker behavior.</p>
            </div>
            <div class="cw-settings-jumpbar" aria-label="UI settings sections">
              <button type="button" class="cw-settings-jump" data-target="ui" onclick="cwUiSettingsJump?.('ui')">User Interface</button>
              <button type="button" class="cw-settings-jump" data-target="security" onclick="cwUiSettingsJump?.('security')">Security</button>
              <button type="button" class="cw-settings-jump" data-target="tracker" onclick="cwUiSettingsJump?.('tracker')">CW Tracker</button>
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
                  <div class="cw-hub-top">
                    <div class="cw-hub-icon" aria-hidden="true"><span class="material-symbols-rounded">palette</span></div>
                    <div class="cw-hub-copy">
                      <div class="cw-hub-title">User Interface</div>
                      <div class="cw-hub-desc">Dashboard visuals</div>
                    </div>
                  </div>
                  <div class="chips">
                    <span class="chip" id="hub_ui_watchlist">Watchlist: -</span>
                    <span class="chip" id="hub_ui_playing">Playing: -</span>
                    <span class="chip" id="hub_ui_askai">ASK AI: -</span>
                    <span class="chip" id="hub_ui_proto">Proto: -</span>
                  </div>
                </button>

                <button type="button" class="cw-hub-tile" data-tab="security" onclick="cwUiSettingsSelect?.('security')">
                  <div class="cw-hub-top">
                    <div class="cw-hub-icon" aria-hidden="true"><span class="material-symbols-rounded">shield_lock</span></div>
                    <div class="cw-hub-copy">
                      <div class="cw-hub-title">Security</div>
                      <div class="cw-hub-desc">Protect CrossWatch</div>
                    </div>
                  </div>
                  <div class="chips">
                    <span class="chip" id="hub_sec_auth">Auth: -</span>
                    <span class="chip" id="hub_sec_session">Session: -</span>
                    <span class="chip" id="hub_sec_proxy">Proxy: -</span>
                  </div>
                </button>

                <button type="button" class="cw-hub-tile" data-tab="tracker" onclick="cwUiSettingsSelect?.('tracker')">
                  <div class="cw-hub-top">
                    <div class="cw-hub-icon" aria-hidden="true"><span class="material-symbols-rounded">inventory_2</span></div>
                    <div class="cw-hub-copy">
                      <div class="cw-hub-title">CW Tracker</div>
                      <div class="cw-hub-desc">Local snapshots</div>
                    </div>
                  </div>
                  <div class="chips">
                    <span class="chip" id="hub_cw_enabled">Tracker: -</span>
                    <span class="chip" id="hub_cw_retention">Retention: -</span>
                  </div>
                </button>
              </div>

              <div class="cw-settings-panels" id="ui_settings_panels">

                <!-- Panel: User Interface -->
                <div class="cw-settings-panel cw-settings-shell active" data-tab="ui">
                  <div class="cw-panel-head cw-settings-head">
                    <div>
                      <div class="cw-panel-title" style="margin-top:10px">User Interface</div>
                      <div class="sub cw-settings-copy">Choose which dashboard elements stay visible and how CrossWatch serves the UI.</div>
                    </div>
                  </div>

                  <div class="cw-settings-layout">
                    <div class="cw-settings-block">
                      <div class="cw-settings-block-title">Visibility</div>
                      <div class="cw-settings-2col">
                        <div>
                          <label for="ui_show_watchlist_preview">Watchlist</label>
                          <select id="ui_show_watchlist_preview" name="ui_show_watchlist_preview">
                            <option value="true">Show</option>
                            <option value="false">Hide</option>
                          </select>
                        </div>

                        <div>
                          <label for="ui_show_playingcard">Playing card</label>
                          <select id="ui_show_playingcard" name="ui_show_playingcard">
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
                      </div>
                    </div>

                    <div class="cw-settings-block">
                      <div class="cw-settings-block-title">Protocol</div>
                      <div>
                        <label for="ui_protocol">UI protocol</label>
                        <div class="cw-settings-inline-action">
                          <select id="ui_protocol" name="ui_protocol" style="min-width:220px;flex:1">
                            <option value="http">HTTP</option>
                            <option value="https">HTTPS (self-signed)</option>
                          </select>
                          <button type="button" class="btn primary" id="ui_tls_advanced" onclick="openTlsCertModal?.()">Advanced</button>
                        </div>
                        <div class="sub" style="margin-top:0.35rem">
                          HTTPS uses a self-signed certificate, so your browser will warn unless you trust it.
                        </div>
                      </div>
                    </div>
                  </div>
                </div>

                <!-- Panel: Security -->
                <div class="cw-settings-panel cw-settings-shell" data-tab="security">
                  <div class="cw-panel-head cw-settings-head">
                    <div>
                      <div class="cw-panel-title" style="margin-top:10px">Security</div>
                      <div class="sub cw-settings-copy">
                        Manage your sign-in details, session persistence, and reverse-proxy trust settings from one place.
                      </div>
                    </div>
                  </div>

                  <div class="cw-settings-layout">
                    <div id="app_auth_fields" class="cw-settings-block">
                      <div class="cw-settings-block-title">Sign-in</div>
                      <div class="cw-settings-stack">
                        <div>
                          <label for="app_auth_username">Username</label>
                          <input id="app_auth_username" name="app_auth_username" type="text" autocomplete="username" placeholder="admin">
                        </div>

                        <div class="cw-settings-2col">
                          <div>
                            <label for="app_auth_password">New password</label>
                            <input id="app_auth_password" name="app_auth_password" type="password" autocomplete="new-password" placeholder="(leave blank to keep)">
                            <div class="sub" style="margin-top:0.35rem">Leave blank to keep the current password.</div>
                          </div>

                          <div>
                            <label for="app_auth_password2">Confirm password</label>
                            <input id="app_auth_password2" name="app_auth_password2" type="password" autocomplete="new-password" placeholder="(repeat)">
                            <div class="sub" style="margin-top:0.35rem">Repeat the new password exactly before saving.</div>
                          </div>
                        </div>
                      </div>
                    </div>

                    <div id="app_auth_session_fields" class="cw-settings-block">
                      <div class="cw-settings-block-title">Session</div>
                      <div class="cw-settings-split">
                        <div>
                          <label for="app_auth_remember_enabled">Session caching</label>
                          <select id="app_auth_remember_enabled" name="app_auth_remember_enabled">
                            <option value="true">Enabled</option>
                            <option value="false">Browser session only</option>
                          </select>
                          <div class="sub" style="margin-top:0.35rem">Browser session only means sign-in is required again after closing the browser.</div>
                        </div>

                        <div id="app_auth_remember_days_wrap">
                          <label for="app_auth_remember_days">Cached for days</label>
                          <input id="app_auth_remember_days" name="app_auth_remember_days" type="text" inputmode="numeric" pattern="[0-9]{1,3}" maxlength="3" autocomplete="off" placeholder="30">
                          <div id="app_auth_remember_days_error" class="cw-field-inline-error hidden" role="alert"></div>
                          <div class="sub" style="margin-top:0.35rem">Used only when session caching is enabled. Maximum 365 days.</div>
                        </div>
                      </div>
                    </div>

                    <div class="cw-settings-block">
                      <div class="cw-settings-block-title">Plex sign-in</div>
                      <div class="cw-settings-stack">
                        <div>
                          <strong>Linked Plex account</strong>
                          <div class="sub" id="app_auth_plex_state">Not linked</div>
                        </div>
                        <div class="cw-settings-inline-action">
                          <button class="btn primary" type="button" id="btn-app-auth-plex-link" onclick="cwAppAuthPlexLink?.()">Link Plex account</button>
                          <button class="btn" type="button" id="btn-app-auth-plex-unlink" onclick="cwAppAuthPlexUnlink?.()">Unlink</button>
                        </div>
                        <div class="sub" style="margin-top:0.35rem">
                          Optional. This adds a <code>Sign in with Plex</code> button to the login screen while keeping local CrossWatch password sign-in as your fallback.
                        </div>
                      </div>
                    </div>

                    <div class="cw-settings-statusrow">
                      <div class="cw-settings-status">
                        <strong>Current session</strong>
                        <div class="sub" id="app_auth_state">&mdash;</div>
                      </div>
                      <button class="btn" id="btn-auth-logout" onclick="cwAppLogout?.()">Log out</button>
                    </div>

                    <div class="cw-settings-statusrow">
                      <div class="cw-settings-status">
                        <strong>Other browser sessions</strong>
                        <div class="sub" id="app_auth_other_sessions_state">Logged in from: 0 browser sessions</div>
                        <div class="sub" id="app_auth_other_sessions_detail"></div>
                      </div>
                      <button class="btn" id="btn-auth-logout-others" onclick="cwAppLogoutOthers?.()">Log out other sessions</button>
                    </div>

                    <div class="cw-settings-block">
                      <div class="cw-settings-block-title">Reverse proxy</div>
                      <label for="trusted_proxies">Trusted reverse proxies (optional)</label>
                      <input id="trusted_proxies" name="trusted_proxies" type="text" placeholder="127.0.0.1;192.168.2.1;192.168.2.0/16">
                      <div class="sub" style="margin-top:0.35rem">
                        Only needed when behind a reverse proxy and you want accurate IP-based login rate limiting.
                        Enter proxy IPs or CIDR ranges separated by <code>;</code>
                      </div>
                    </div>
                  </div>
                </div>

                <!-- Panel: CW Tracker -->
                <div class="cw-settings-panel cw-settings-shell" data-tab="tracker">
                  <div class="cw-panel-head cw-settings-head">
                    <div>
                      <div class="cw-panel-title" style="margin-top:10px">CW Tracker</div>
                      <div class="sub cw-settings-copy">
                        Local backup tracker for Watchlist, Ratings and History snapshots stored under <code>/config/.cw_provider</code>.
                      </div>
                    </div>
                  </div>

                  <div class="cw-settings-layout">
                    <div class="cw-settings-block">
                      <div class="cw-settings-block-title">Retention and capture</div>
                      <div class="cw-settings-2col">
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
                          <div class="sub" style="margin-top:0.35rem">0 = keep snapshots forever.</div>
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
                          <div class="sub" style="margin-top:0.35rem">0 = unlimited.</div>
                        </div>
                      </div>
                    </div>

                    <div class="cw-settings-block">
                      <div class="cw-settings-block-title">Restore snapshots</div>
                      <div class="cw-settings-2col" id="cw_restore_fields">
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
                      <div class="sub" style="margin-top:0.75rem">
                        Select <code>latest</code> to use the most recent snapshot, or choose a specific file name for each feature.
                      </div>
                    </div>
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


<script>(()=>{const $=id=>document.getElementById(id),closeMenu=id=>{const m=$(id==="settings"?"cw-settings-menu":"cw-about-menu"),b=$(id==="settings"?"tab-settings":"tab-about");m?.classList.add("hidden");b?.setAttribute("aria-expanded","false")},closeAll=()=>{closeMenu("settings");closeMenu("about")},toggleMenu=(id,e)=>{e?.preventDefault?.();e?.stopPropagation?.();const menuId=id==="settings"?"cw-settings-menu":"cw-about-menu",btnId=id==="settings"?"tab-settings":"tab-about",m=$(menuId),b=$(btnId);if(!m||!b)return;const open=m.classList.contains("hidden");closeAll();m.classList.toggle("hidden",!open);b.setAttribute("aria-expanded",String(open))},setHelp=open=>{const o=$("cw-help-overlay");if(!o)return;if(open){const f=$("cw-help-frame");if(f&&!f.src)f.src="https://wiki.crosswatch.app";o.classList.remove("hidden");o.setAttribute("aria-hidden","false")}else{o.classList.add("hidden");o.setAttribute("aria-hidden","true")}},openSettings=pane=>{window.showTab?.("settings");setTimeout(()=>window.cwSettingsSelect?.(pane),0)},logout=()=>{closeMenu("settings");if(typeof window.cwAppLogout==="function")return window.cwAppLogout();window.location.href="/logout"};window.APP_VERSION="__CW_VERSION__";window["__CW_"+"VERSION__"]=window.APP_VERSION;window.cwOpenHelp=()=>setHelp(true);window.cwCloseHelp=()=>setHelp(false);window.openHelp=()=>window.location?.protocol==="https:"?window.cwOpenHelp?.():window.open("https://wiki.crosswatch.app","_blank","noopener,noreferrer");window.cwCloseAboutMenu=()=>closeMenu("about");window.cwCloseSettingsMenu=()=>closeMenu("settings");window.cwToggleAboutMenu=e=>toggleMenu("about",e);window.cwToggleSettingsMenu=e=>toggleMenu("settings",e);window.cwAboutMenuSelect=w=>(closeMenu("about"),w==="about"?window.openAbout?.():w==="help"?window.openHelp?.():undefined);window.cwSettingsMenuLogout=logout;window.cwSettingsMenuSelect=w=>{closeMenu("settings");if(w==="overview")return openSettings("overview");if(w==="providers")return openSettings("providers");if(w==="scheduling")return openSettings("scheduling");if(w==="pairs")return(window.showTab?.("settings"),window.cwProvidersJump?.("sec-sync"));if(w==="scrobbler")return openSettings("scrobbler");if(w==="app")return openSettings("app");if(w==="maintenance")return window.openMaintenanceModal?.()};document.addEventListener("click",e=>{const o=$("cw-help-overlay"),c=$("cw-help-card"),aboutHost=$("tab-about-menu"),settingsHost=$("tab-settings-menu");if(o&&!o.classList.contains("hidden")&&c&&!c.contains(e.target))window.cwCloseHelp?.();if(aboutHost&&!aboutHost.contains(e.target))closeMenu("about");if(settingsHost&&!settingsHost.contains(e.target))closeMenu("settings")},true);document.addEventListener("keydown",e=>{if(e.key!=="Escape")return;window.cwCloseHelp?.();closeAll()},true)})();</script>

__CW_ASSET_BLOCK__

<script>document.addEventListener('DOMContentLoaded',()=>{try{if(typeof openSummaryStream==='function')openSummaryStream()}catch(e){}});</script>

<div id="save-frost" class="hidden" aria-hidden="true"></div>
<div id="save-fab" class="hidden" role="toolbar" aria-label="Sticky save"><button id="save-fab-btn" class="btn" onclick="saveSettings(this)"><span class="btn-ic">✔</span> <span class="btn-label">Save</span></button></div>

<script>(()=>{const list=p=>[...p.querySelectorAll(':scope>.section')].filter(s=>s.dataset.accordion!=='off'),set=(s,on)=>{s.classList.toggle('open',!!on);s.querySelector('.head')?.setAttribute('aria-expanded',String(!!on));const c=s.querySelector('.chev');if(c)c.textContent=on?'▼':'▶'},siblings=s=>s?.parentElement?list(s.parentElement):[];window.toggleSection=id=>{const s=document.getElementById(id);if(!s||s.dataset.accordion==='off')return;const on=!s.classList.contains('open');siblings(s).forEach(x=>set(x,x===s&&on))};window.openSection=id=>{const s=document.getElementById(id);if(!s||s.dataset.accordion==='off')return;siblings(s).forEach(x=>set(x,x===s))};document.addEventListener('DOMContentLoaded',()=>[...new Set([...document.querySelectorAll('.section')].map(s=>s.parentElement).filter(Boolean))].forEach(p=>{const open=list(p).find(s=>s.classList.contains('open'));list(p).forEach(s=>set(s,s===open))}),{once:true})})();</script>


<script>(()=>{const panes='#page-settings .cw-settings-pane',nav='#cw-settings-nav .cw-settings-nav-btn',norm=v=>String(v||'overview').trim().toLowerCase(),apply=p=>{const name=norm(p);let found=false;document.querySelectorAll(panes).forEach(n=>{const on=norm(n.dataset.pane)===name;n.classList.toggle('active',on);found=found||on});if(!found&&name!=='overview')return apply('overview');document.querySelectorAll(nav).forEach(b=>{const on=norm(b.dataset.pane)===name;b.classList.toggle('active',on);b.setAttribute('aria-current',on?'page':'false')});window.__cwSettingsPane=name;document.dispatchEvent(new CustomEvent('cw-settings-pane-changed',{detail:{pane:name}}))};window.cwSettingsSelect=p=>{apply(p);const main=document.getElementById('cw-settings-left');if(main&&window.innerWidth<1200)main.scrollIntoView({behavior:'smooth',block:'start'})};document.addEventListener('DOMContentLoaded',()=>apply(window.__cwSettingsPane||'overview'),{once:true});document.addEventListener('tab-changed',e=>((e?.detail?.id||e?.detail?.tab)==='settings')&&setTimeout(()=>apply(window.__cwSettingsPane||'overview'),0))})();</script>

<script>(()=>{const scrollTo=id=>document.getElementById(id)?.scrollIntoView({behavior:'smooth',block:'start'});window.cwProvidersJump=sectionId=>(window.cwSettingsSelect?.('providers'),setTimeout(()=>{window.openSection?.(sectionId);scrollTo(sectionId)},0));window.cwOverviewJump=(sectionId,authGroupId='')=>(window.cwSettingsSelect?.('providers'),setTimeout(async()=>{if(sectionId==='sec-auth'){window.openSection?.('sec-auth');try{await window.mountAuthProviders?.()}catch{}if(authGroupId){window.openSection?.(authGroupId);scrollTo(authGroupId)}else scrollTo('sec-auth');return}window.openSection?.(sectionId);scrollTo(sectionId)},0))})();</script>
<script>
(() => {
  const $ = (id) => document.getElementById(id);
  const setText = (id, text) => {
    const el = $(id);
    if (el) el.textContent = text;
  };
  const setWidth = (id, value) => {
    const el = $(id);
    if (el) el.style.width = value;
  };
  const plural = (n, singular, pluralForm) => `${n} ${n === 1 ? singular : (pluralForm || `${singular}s`)}`;
  const state = { primary: "auth" };

  const open = (key) => {
    if (key === "auth") return window.cwOverviewJump?.("sec-auth");
    if (key === "meta") return window.cwOverviewJump?.("sec-meta");
    if (key === "sync") return window.cwProvidersJump?.("sec-sync");
    if (key === "scheduling" || key === "automation") return window.cwSettingsSelect?.("scheduling");
    if (key === "scrobbler") return window.cwSettingsSelect?.("scrobbler");
    if (key === "app") return window.cwSettingsSelect?.("app");
    if (key === "maintenance") return window.openMaintenanceModal?.();
  };

  window.cwSettingsOverviewGo = (key) => {
    const target = key === "primary" ? state.primary : key;
    if (target) open(target);
  };

  window.cwSettingsStepKey = (event, key) => {
    if (!event) return;
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      window.cwSettingsOverviewGo?.(key);
    }
  };

  const setStep = (step, opts = {}) => {
    const card = document.querySelector(`.cw-settings-setup-step[data-step="${step}"]`);
    if (!card) return;
    card.classList.toggle("is-done", !!opts.done);
    card.classList.toggle("is-active", !!opts.active);
    card.classList.toggle("is-optional", !!opts.optional);

    const statusEl = card.querySelector(".cw-settings-step-state");
    const copyEl = card.querySelector(".cw-settings-step-copy");
    const primaryLinkEl = card.querySelector(`#cw-settings-step-${step}-link`);
    const secondaryLinkEl = card.querySelector(`#cw-settings-step-${step}-alt-link`);

    if (statusEl) statusEl.textContent = opts.status || "";
    if (copyEl) copyEl.textContent = opts.copy || "";
    if (primaryLinkEl) primaryLinkEl.textContent = opts.link || "";
    if (secondaryLinkEl) secondaryLinkEl.textContent = opts.altLink || "";
  };

  const render = (data = {}) => {
    const authCount = Number(data?.auth?.configured || 0);
    const pairCount = Number(data?.pairs?.count || 0);
    const metaConfigured = Number(data?.meta?.configured || 0);
    const metaDetected = Number(data?.meta?.detected || 0);
    const scheduleOn = !!data?.sched?.enabled;
    const scrobOn = !!data?.scrob?.enabled;
    const automationOn = scheduleOn || scrobOn;

    const steps = {
      auth: authCount > 0,
      meta: metaConfigured > 0,
      sync: pairCount > 0 || scrobOn,
      scheduling: scheduleOn || scrobOn
    };
    const order = ["auth", "meta", "sync", "scheduling"];
    const next = order.find((step) => !steps[step]) || "scheduling";
    const doneCount = order.filter((step) => steps[step]).length;
    const overviewGrid = document.getElementById("cw-settings-overview-grid");

    state.primary = doneCount === 4 ? "scheduling" : next;
    overviewGrid?.classList.toggle("cw-settings-overview-complete", doneCount === 4);
    setText("cw-settings-progress-text", `${doneCount} of 4 steps ready`);
    setWidth("cw-settings-progress-bar", `${Math.max(6, Math.min(100, (doneCount / 4) * 100))}%`);
    setText("cw-settings-scrobbler-cta", scrobOn ? "Scrobbler settings" : "Open scrobbler");

    if (!steps.auth) {
      setText("cw-settings-hero-title", "Connect a service");
      setText("cw-settings-hero-copy", "Start by signing into at least one media server or tracker.");
      setText("cw-settings-primary-cta", "Open connections");
    } else if (!steps.meta) {
      setText("cw-settings-hero-title", "Add metadata");
      setText("cw-settings-hero-copy", "TMDb is enough to get started and makes matching work better.");
      setText("cw-settings-primary-cta", "Open metadata");
    } else if (!steps.sync) {
      setText("cw-settings-hero-title", "Set up sync pairs or scrobbler");
      setText("cw-settings-hero-copy", "Both are optional, but most people use at least one of them.");
      setText("cw-settings-primary-cta", "Open sync pairs");
    } else if (!steps.scheduling) {
      setText("cw-settings-hero-title", "Add scheduling or scrobbler");
      setText("cw-settings-hero-copy", "Optional. Turn on scheduling, scrobbler, or both.");
      setText("cw-settings-primary-cta", "Open scheduling");
    } else {
      setText("cw-settings-hero-title", "You're set");
      setText("cw-settings-hero-copy", "The basic setup is done. Anything else is extra.");
      setText("cw-settings-primary-cta", "Open scheduling");
    }

    setText("cw-settings-stat-auth", String(authCount));
    setText("cw-settings-stat-auth-copy", authCount ? `${plural(authCount, "provider profile")} connected` : "No providers connected yet");
    setText("cw-settings-stat-pairs", String(pairCount));
    setText("cw-settings-stat-pairs-copy", pairCount ? `${plural(pairCount, "pair")} ready` : "No sync pairs yet");
    setText("cw-settings-stat-automation", automationOn ? "Live" : "Off");
    setText("cw-settings-stat-automation-copy", scheduleOn && scrobOn ? "Scheduling and scrobbler are on" : scheduleOn ? "Scheduling is on" : scrobOn ? "Scrobbler is on" : "Scheduling and scrobbler are off");

    setStep("auth", {
      status: steps.auth ? `${authCount} connected` : "Needs setup",
      copy: steps.auth ? `${plural(authCount, "provider profile")} saved.` : "Link Plex, Jellyfin, Emby, or your trackers.",
      link: "Open connections",
      done: steps.auth,
      active: next === "auth"
    });

    setStep("meta", {
      status: steps.meta ? (metaDetected > metaConfigured ? `Partial ${metaConfigured}/${Math.max(metaDetected, metaConfigured)}` : "Ready") : "Missing",
      copy: steps.meta ? (metaDetected > metaConfigured ? `${metaConfigured} of ${Math.max(metaDetected, metaConfigured)} metadata providers are configured.` : "Metadata is set up.") : "Add TMDb to improve matching and metadata lookups.",
      link: "Open metadata",
      done: steps.meta,
      active: next === "meta"
    });

    const syncStatus = pairCount > 0 && scrobOn ? "Both on" : pairCount > 0 ? "Sync pairs ready" : scrobOn ? "Scrobbler ready" : "Optional";
    const syncCopy = pairCount > 0 && scrobOn ? `${plural(pairCount, "pair")} set up and scrobbler is on.` : pairCount > 0 ? `${plural(pairCount, "pair")} set up. You can still add scrobbler if you want.` : scrobOn ? "Scrobbler is on. Sync pairs are optional." : "You can use sync pairs, scrobbler, or both. Most people set up at least one.";
    setStep("sync", {
      status: syncStatus,
      copy: syncCopy,
      link: "Sync pairs",
      altLink: "Scrobbler",
      done: steps.sync,
      active: next === "sync",
      optional: !steps.sync
    });

    setStep("scheduling", {
      status: scheduleOn && scrobOn ? "Both on" : scheduleOn ? "Scheduling on" : scrobOn ? "Scrobbler on" : "Optional",
      copy: scheduleOn && scrobOn ? "Scheduling and scrobbler are both on." : scheduleOn ? "Scheduling is on." : scrobOn ? "Scrobbler is on." : "Optional. Turn on scheduling, scrobbler, or both.",
      link: "Open scheduling",
      done: steps.scheduling,
      active: next === "scheduling",
      optional: !steps.scheduling
    });
  };

  document.addEventListener("cw-settings-overview-data", (e) => render(e?.detail?.data || {}));
  document.addEventListener("DOMContentLoaded", () => render({}), { once: true });
})();
</script>
<script>(()=>{window.cwScrobblerJump=sectionId=>(window.cwSettingsSelect?.('scrobbler'),setTimeout(()=>{window.openSection?.(sectionId);document.getElementById(sectionId)?.scrollIntoView({behavior:'smooth',block:'start'})},0));window.cwUiSettingsJump=tab=>(window.cwSettingsSelect?.('app'),setTimeout(()=>{window.cwUiSettingsSelect?.(tab);document.querySelector(`#ui_settings_hub .cw-hub-tile[data-tab="${String(tab||'').trim().toLowerCase()}"]`)?.scrollIntoView({behavior:'smooth',block:'nearest',inline:'center'})},0))})();</script>

<script>(()=>{const origFetch=window.fetch;if(typeof origFetch!=='function'||origFetch.__cwAuthPendingWrapped)return;const pending=()=>window.cwIsAuthSetupPending?.()===true,allowPath=p=>p.startsWith('/api/app-auth/')||p==='/api/config/meta'||p.startsWith('/api/config/meta?')||p.startsWith('/assets/')||p==='/favicon.svg';const emptyJson=(body='{}')=>new Response(body,{status:200,headers:{'Content-Type':'application/json','Cache-Control':'no-store'}});window.fetch=Object.assign(async function(resource,init){try{if(!pending())return await origFetch(resource,init);const url=typeof resource==='string'?resource:String(resource?.url||'');const u=new URL(url,location.origin);if(u.origin!==location.origin||!u.pathname.startsWith('/api/')||allowPath(u.pathname)||allowPath(u.pathname+u.search))return await origFetch(resource,init);const method=String(init?.method||resource?.method||'GET').toUpperCase();if(method!=='GET'&&method!=='HEAD')return await origFetch(resource,init);if(u.pathname.startsWith('/api/config'))return emptyJson('{}');if(u.pathname.startsWith('/api/status'))return emptyJson('{"providers":{}}');if(u.pathname.startsWith('/api/pairs'))return emptyJson('[]');if(u.pathname.startsWith('/api/scheduling'))return emptyJson('{}');if(u.pathname.startsWith('/api/insights'))return emptyJson('{}');if(u.pathname.startsWith('/api/watch/'))return emptyJson('{}');if(u.pathname.startsWith('/api/webhooks/'))return emptyJson('{}');return emptyJson('{}')}catch{return await origFetch(resource,init)}},{__cwAuthPendingWrapped:true})})();</script>

<script>(()=>{const $=id=>document.getElementById(id),fab=$('save-fab'),frost=$('save-frost'),page=$('page-settings'),tab=$('tab-settings'),visible=()=>{if(!page)return false;const cs=getComputedStyle(page);return!page.classList.contains('hidden')&&cs.display!=='none'&&cs.visibility!=='hidden'},update=()=>{const on=visible();fab?.classList.toggle('hidden',!on);frost?.classList.toggle('hidden',!on)},watch=(el,attrs)=>el&&new MutationObserver(update).observe(el,{attributes:true,attributeFilter:attrs});document.addEventListener('DOMContentLoaded',()=>{watch(page,['class','style']);watch(tab,['class']);update()},{once:true});document.addEventListener('tab-changed',update);window.addEventListener('hashchange',update);document.querySelector('.tabs')?.addEventListener('click',update,true)})();</script>

<script>(()=>{const install=()=>{const orig=window.saveSettings;if(typeof orig!=='function'||orig._wrapped)return;window.saveSettings=Object.assign(async function(btnOrEvent){const btn=btnOrEvent instanceof HTMLElement?btnOrEvent:document.getElementById('save-fab-btn');if(btn&&!btn.dataset.defaultHtml)btn.dataset.defaultHtml=btn.innerHTML;if(btn)btn.disabled=true;try{const ret=orig.apply(this,arguments);await(ret&&typeof ret.then==='function'?ret:Promise.resolve());window.invalidateConfigCache?.();window.manualRefreshStatus?.();if(btn){btn.innerHTML='Settings saved ✓';setTimeout(()=>{btn.innerHTML=btn.dataset.defaultHtml||'<span class="btn-ic">✔</span> <span class="btn-label">Save</span>';btn.disabled=false},1600)}return ret}catch(e){if(btn){btn.innerHTML='Save failed';setTimeout(()=>{btn.innerHTML=btn.dataset.defaultHtml||'<span class="btn-ic">✔</span> <span class="btn-label">Save</span>';btn.disabled=false},2000)}throw e}},{_wrapped:true})};document.readyState==='complete'?install():window.addEventListener('load',install,{once:true})})();</script>
</body></html>

"""

def get_index_html(include_gitbook_embed: bool = True, ui_show_ai: bool = True) -> str:
    html = _get_index_html_static().replace("__CW_ASSET_BLOCK__", _asset_block())
    html = html.replace("__CW_AUTH_HEADER_ICONS__", _auth_header_icons_html())
    core_script = '<script src="/assets/helpers/core.js?v=__CW_VERSION__"></script>'
    if core_script in html and "__cwUiShowAI" not in html:
        html = html.replace(core_script, f'<script>window.__cwUiShowAI={"true" if ui_show_ai else "false"};</script>\n{core_script}', 1)
    if include_gitbook_embed and "gitbook/embed/script.js" not in html:
        html = html.replace(core_script, f"{GITBOOK_EMBED_BLOCK}\n\n{core_script}", 1) if core_script in html else f"{html}\n\n{GITBOOK_EMBED_BLOCK}\n"
    return (
        html
        .replace("__CW_CURRENT_VERSION__", CURRENT_VERSION)
        .replace("__CW_VERSION__", _asset_version_token())
    )
