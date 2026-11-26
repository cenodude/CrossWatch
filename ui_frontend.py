# ui_frontend.py
# CrossWatch - UI Frontend Registration
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import html as html_lib
from html.parser import HTMLParser
import logging
import re
from pathlib import Path
import time

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response
from starlette.staticfiles import StaticFiles
from api.versionAPI import CURRENT_VERSION
from cw_platform.access_policy import clean_managed_permissions, managed_profile_instances, managed_settings_access, provider_display_key, reconnect_connection_keys
from cw_platform import i18n

__all__ = ["register_assets_and_favicons", "register_ui_root", "get_index_html"]

_LOG = logging.getLogger("crosswatch.ui_frontend")

_ASSET_VERSION_CACHE: dict[str, float | str] = {"ts": 0.0, "val": CURRENT_VERSION}

DEFAULT_MANIFEST: str = r"""{
  "name": "CrossWatch",
  "short_name": "CrossWatch",
  "description": "Sync watchlists, history and ratings across Plex, Trakt, SIMKL, Jellyfin and more.",
  "id": "/",
  "start_url": "/",
  "scope": "/",
  "display": "standalone",
  "display_override": ["standalone", "minimal-ui", "browser"],
  "orientation": "any",
  "background_color": "#090b10",
  "theme_color": "#090b10",
  "icons": [
    { "src": "/assets/pwa/icon-192.png?v=__CW_VERSION__", "sizes": "192x192", "type": "image/png", "purpose": "any" },
    { "src": "/assets/pwa/icon-512.png?v=__CW_VERSION__", "sizes": "512x512", "type": "image/png", "purpose": "any" },
    { "src": "/assets/pwa/icon-192-maskable.png?v=__CW_VERSION__", "sizes": "192x192", "type": "image/png", "purpose": "maskable" },
    { "src": "/assets/pwa/icon-512-maskable.png?v=__CW_VERSION__", "sizes": "512x512", "type": "image/png", "purpose": "maskable" }
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

_REVALIDATE_SUFFIXES = {".js", ".mjs", ".css", ".html"}


class _AssetStaticFiles(StaticFiles):
    def file_response(self, full_path, stat_result, scope, status_code: int = 200) -> Response:
        response = super().file_response(full_path, stat_result, scope, status_code)
        if Path(full_path).suffix.lower() in _REVALIDATE_SUFFIXES:
            response.headers["Cache-Control"] = "no-cache"
        return response


def register_assets_and_favicons(app: FastAPI, root: Path) -> None:
    assets_dir = root / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    @app.get("/assets/locales/{code}.js", include_in_schema=False, tags=["ui"])
    def locale_bundle(code: str) -> Response:
        lang = i18n.language_or_none(code)
        if lang is None:
            return Response(status_code=404, content="", media_type="text/javascript", headers={"Cache-Control": "no-store"})
        return Response(content=i18n.bundle_js(lang), media_type="text/javascript", headers={"Cache-Control": "no-cache"})

    app.mount("/assets", _AssetStaticFiles(directory=str(assets_dir)), name="assets")

    def asset_response(name: str, fallback: str, media_type: str, **headers: str) -> Response:
        try:
            content = (assets_dir / name).read_text(encoding="utf-8")
        except Exception:
            content = fallback
        return Response(content=content, media_type=media_type, headers=headers)

    @app.get("/favicon.ico", include_in_schema=False, tags=["ui"])
    def favicon_ico() -> FileResponse:
        return FileResponse(assets_dir / "pwa" / "favicon.ico", media_type="image/x-icon", headers={"Cache-Control": "public, max-age=86400"})

    @app.get("/favicon.svg", include_in_schema=False, tags=["ui"])
    def favicon_svg_compat() -> FileResponse:
        return FileResponse(assets_dir / "pwa" / "favicon-64.png", media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})

    @app.get("/manifest.webmanifest", include_in_schema=False, tags=["ui"])
    def manifest_webmanifest() -> Response:
        try:
            content = (assets_dir / "manifest.webmanifest").read_text(encoding="utf-8")
        except Exception:
            content = DEFAULT_MANIFEST
        content = content.replace("__CW_VERSION__", _asset_version_token())
        return Response(content=content, media_type="application/manifest+json", headers={"Cache-Control": "public, max-age=3600"})

    @app.get("/sw.js", include_in_schema=False, tags=["ui"])
    def service_worker() -> Response:
        return asset_response("sw.js", DEFAULT_SW, "text/javascript", **{"Cache-Control": "no-store", "Service-Worker-Allowed": "/"})

def register_ui_root(app: FastAPI) -> None:
    @app.get("/", include_in_schema=False, tags=["ui"])
    def ui_root(request: Request) -> Response:
        user = getattr(getattr(request, "state", None), "cw_user", None)
        if isinstance(user, dict) and user.get("is_admin") is False:
            perms = _managed_user_permissions(user)
            view = str(request.query_params.get("view") or "").strip().lower()
            if view == "playback_progress":
                return RedirectResponse(url="/hub#playback", status_code=302)
            if view == "watchlist":
                return RedirectResponse(url="/hub#watchlist", status_code=302)
            if not perms.get("write"):
                return RedirectResponse(url="/hub", status_code=302)
            if perms.get("write") and request.query_params.get("main") != "1":
                return RedirectResponse(url="/hub", status_code=302)
        include_admin = not (isinstance(user, dict) and user.get("is_admin") is False)
        return HTMLResponse(get_index_html(include_admin=include_admin, user=user, language=i18n.request_language(request, user)), headers={"Cache-Control": "no-store"})

    @app.get("/hub", include_in_schema=False, tags=["ui"])
    @app.get("/profile", include_in_schema=False, tags=["ui"])
    def ui_profile(request: Request) -> Response:
        user = getattr(getattr(request, "state", None), "cw_user", None)
        if not isinstance(user, dict):
            try:
                from api.appAuthAPI import COOKIE_NAME, auth_required as app_auth_required, current_user as app_current_user
                from cw_platform.config_base import load_config

                cfg = load_config()
                user = app_current_user(cfg, request.cookies.get(COOKIE_NAME)) if app_auth_required(cfg) else None
            except Exception:
                user = None
        if not isinstance(user, dict):
            return RedirectResponse(url="/login", status_code=302)
        language = i18n.request_language(request, user)
        return HTMLResponse(get_hub_html(user=user, language=language) if request.url.path == "/hub" else get_profile_html(user=user, language=language), headers={"Cache-Control": "no-store"})


_HELPER_SCRIPTS = (
    "mobile-experience.js", "selection-bar.js", "card-actions.js", "airing-card.js", "help-links.js", "touch-help.js", "provider-meta.js", "feature-meta.js", "icon-select.js", "profile-select.js", "page-loader.js", "dom.js", "events.js", "auth-state.js", "pwa-install.js", "account-menu.js", "mobile-settings.js", "notifications.js", "api.js", "view-cache.js", "core.js", "details-log.js",
    "media-meta.js", "trailer.js", "playing-card.js", "watchlist-preview.js", "profile-datetime.js", "month-bar.js", "providers-ui.js", "settings-ui.js", "settings-save.js", "maintenance.js", "backups.js",
    "restart_apply.js",
)
_APP_SCRIPTS = (
    "syncbar.js", "run-summary-stream.js", "overview-profile.js", "settings-scope.js", "sync-reviews.js", "notification-feed.js", "main.js", "connections.overlay.js", "connections.pairs.overlay.js", "scheduler.js", "scheduler-profiles.js",
    "schedulerbanner.js", "playingcard.js", "insights.js", "activity.js", "item-actions.js", "profile-media-modal.js", "dashboard-widgets.js", "auth-dots.js", "main-status.js",
    "scrobbler.js", "user-profiles.js", "app-users.js", "mcp-access.js", "assistant-settings.js",
)
_USER_HELPER_SCRIPTS = (
    "mobile-experience.js", "selection-bar.js", "card-actions.js", "airing-card.js", "help-links.js", "touch-help.js", "provider-meta.js", "icon-select.js", "profile-select.js", "page-loader.js", "dom.js", "events.js", "auth-state.js", "pwa-install.js", "account-menu.js", "mobile-settings.js", "notifications.js", "api.js", "view-cache.js", "core.js",
    "media-meta.js", "trailer.js", "playing-card.js", "watchlist-preview.js", "profile-datetime.js",
)
_USER_APP_SCRIPTS = (
    "overview-profile.js", "sync-reviews.js", "notification-feed.js", "main.js", "playingcard.js", "insights.js", "activity.js", "item-actions.js", "profile-media-modal.js", "dashboard-widgets.js", "auth-dots.js", "main-status.js",
)
_FULL_USER_HELPER_SCRIPTS = tuple(dict.fromkeys((*_USER_HELPER_SCRIPTS, "details-log.js")))
_FULL_USER_APP_SCRIPTS = tuple(dict.fromkeys(("syncbar.js", "run-summary-stream.js", "schedulerbanner.js", *_USER_APP_SCRIPTS)))


def _remove_html_range(html: str, start: str, end: str) -> str:
    start_index = html.find(start)
    if start_index < 0:
        return html
    end_index = html.find(end, start_index)
    if end_index < 0:
        return html
    return html[:start_index] + html[end_index:]


def _replace_html_range(html: str, start: str, end: str, replacement: str) -> str:
    start_index = html.find(start)
    if start_index < 0:
        return html
    end_index = html.find(end, start_index)
    if end_index < 0:
        return html
    return html[:start_index] + replacement + html[end_index:]


def _remove_html_section(html: str, marker: str) -> str:
    start_index = html.find(marker)
    if start_index < 0:
        return html
    depth = 0
    scan = start_index
    while scan < len(html):
        next_open = html.find("<section", scan)
        next_close = html.find("</section>", scan)
        if next_close < 0:
            return html
        if next_open >= 0 and next_open < next_close:
            depth += 1
            scan = next_open + 8
            continue
        depth -= 1
        scan = next_close + len("</section>")
        if depth <= 0:
            while scan < len(html) and html[scan] in "\r\n":
                scan += 1
            return html[:start_index] + html[scan:]
    return html


def _managed_user_permissions(user: dict | None) -> dict[str, bool]:
    return clean_managed_permissions(user.get("permissions") if isinstance(user, dict) else {})


def _remove_line(html: str, line: str) -> str:
    return html.replace(line + "\n", "")


_ADMIN_ONLY_MARKERS: tuple[tuple[str, str], ...] = (
    ('id="tab-settings-menu"', "settings tab menu"),
    ('id="page-settings"', "settings pane"),
    ('id="app_user_manager"', "user manager"),
    ("app_auth_oidc_client_secret", "OIDC credentials form"),
    ("cwAboutMenuSelect('assistant')", "assistant menu item"),
    ('id="page-tracker"', "tracker page"),
    ('id="tab-tracker"', "tracker tab"),
)
_ASSISTANT_MENU_ITEM = (
    '        <button class="cw-menu-item" type="button" role="menuitem" onclick="window.cwAboutMenuSelect(\'assistant\')">'
    '<span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">auto_awesome</span><span>[[t:navigation.aboutMenu.assistant]]</span></button>'
)


_SETTINGS_PANE_STARTS: tuple[tuple[str, str], ...] = (
    ("overview", '        <section id="cw-settings-overview" class="cw-settings-pane active" data-pane="overview">'),
    ("providers", '        <section class="cw-settings-pane" data-pane="providers">'),
    ("sync", '        <section class="cw-settings-pane" data-pane="sync">'),
    ("scheduling", '        <section class="cw-settings-pane" data-pane="scheduling">'),
    ("scrobbler", '        <section class="cw-settings-pane" data-pane="scrobbler">'),
    ("app", '        <section class="cw-settings-pane cw-app-settings-pane" data-pane="app">'),
    ("maintenance", '        <section class="cw-settings-pane cw-page-maintenance" data-pane="maintenance">'),
)
_SETTINGS_LEFT_END = "      </div>\n    </div>\n  </section>\n\n\n</main>"
_MANAGED_SETTINGS_PANES = (("connections", "providers"), ("pairs", "sync"), ("schedule", "scheduling"), ("scrobbler", "scrobbler"))
_SETTINGS_PANE_CLASSES = frozenset({"cw-settings-pane", "cw-settings-nav-btn"})
_SETTINGS_NAV_BUTTON = re.compile(r'          <button type="button" class="cw-settings-nav-btn(?: active)?" data-pane="(?P<pane>[a-z]+)"[^\n]*\n.*?          </button>\n', re.S)


def _managed_tracker_allowed(user: dict | None) -> bool:
    if not _managed_user_permissions(user).get("write"):
        return False
    try:
        from cw_platform.config_base import load_config

        return bool(managed_profile_instances(load_config() or {}, user).get(provider_display_key("CROSSWATCH")))
    except Exception:
        return False


def _managed_settings_panes(user: dict | None) -> set[str]:
    perms = _managed_user_permissions(user)
    return {pane for key, pane in _MANAGED_SETTINGS_PANES if perms.get(key)}


def _strip_settings_panes(html: str, keep: set[str]) -> str:
    for index, (pane, start) in enumerate(_SETTINGS_PANE_STARTS):
        if pane in keep:
            continue
        end = _SETTINGS_PANE_STARTS[index + 1][1] if index + 1 < len(_SETTINGS_PANE_STARTS) else _SETTINGS_LEFT_END
        html = _remove_html_range(html, start, end)
    return _SETTINGS_NAV_BUTTON.sub(lambda m: m.group(0) if m.group("pane") in keep else "", html)


class _SettingsPaneScanner(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.panes: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {name.lower(): value or "" for name, value in attrs}
        if "data-pane" in values:
            self.panes.add(values["data-pane"].strip().lower())
        elif _SETTINGS_PANE_CLASSES.intersection(values.get("class", "").split()):
            self.panes.add("")


def _remaining_settings_panes(html: str) -> set[str]:
    scanner = _SettingsPaneScanner()
    scanner.feed(html)
    scanner.close()
    return scanner.panes


def _assert_admin_shell_stripped(html: str, *, settings_panes: set[str] | None = None, tracker: bool = False) -> None:
    allowed = settings_panes or set()
    kept = {'id="page-tracker"', 'id="tab-tracker"'} if tracker else set()
    leaked = [label for marker, label in _ADMIN_ONLY_MARKERS if marker in html and marker not in kept and not (allowed and marker == 'id="page-settings"')]
    if allowed:
        leaked += [f"settings pane {pane or '?'}" for pane in sorted(_remaining_settings_panes(html) - allowed)]
    if not leaked:
        return
    _LOG.error(
        "managed-user shell still contains admin-only markup (%s) - the index template changed and the strip markers no longer match",
        ", ".join(leaked),
    )
    raise RuntimeError("managed_user_shell_strip_failed:" + ",".join(sorted(leaked)))


_NAV_TAB_LABELS = {"main": "navigation.tabs.main", "profile": "navigation.tabs.hub", "snapshots": "navigation.tabs.captures", "playlists": "navigation.tabs.playlists", "tracker": "navigation.tabs.tracker"}


def _profile_nav_tab(route: str, href: str, active: bool = False) -> str:
    cls = "tab active" if active else "tab"
    label = _NAV_TAB_LABELS[route]
    return f"<button id=\"tab-{route}\" class=\"{cls}\" type=\"button\" onclick=\"location.href='{href}'\">[[t:{label}]]</button>"


_TOOLS_MENU_ITEMS: tuple[tuple[str, str, str, int], ...] = (
    ("events", "history", "navigation.pages.events", 0),
    ("logs", "receipt_long", "navigation.pages.logs", 0),
    ("editor", "edit_note", "navigation.pages.editor", 1),
    ("analyzer", "monitoring", "navigation.pages.analyzer", 1),
    ("import_export", "import_export", "navigation.pages.importExport", 1),
    ("backups", "backup", "navigation.pages.backups", 2),
)


def _tools_menu(*, maintenance: bool = True, href_prefix: str = "") -> str:
    items: list[str] = []
    group: int | None = None
    for route, icon, label, item_group in _TOOLS_MENU_ITEMS:
        if route == "backups" and not maintenance:
            continue
        if group is not None and item_group != group:
            items.append('<div class="cw-menu-sep" role="separator" aria-hidden="true"></div>')
        group = item_group
        items.append(
            f'<button id="tab-{route}" class="cw-menu-item" type="button" role="menuitem" onclick="window.cwToolsMenuSelect(\'{route}\')">'
            f'<span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">{icon}</span><span>[[t:{label}]]</span></button>'
        )
    prefix = f' data-href-prefix="{html_lib.escape(href_prefix, quote=True)}"' if href_prefix else ""
    return (
        '<div class="cw-tabmenu" id="tab-tools-menu">'
        '<button id="tab-tools" class="tab" type="button" aria-haspopup="menu" aria-expanded="false" onclick="window.cwToggleToolsMenu(event)">'
        '<span>[[t:navigation.tabs.tools]]</span><span class="tab-caret" aria-hidden="true"></span></button>'
        f'<div class="cw-menu hidden" id="cw-tools-menu" role="menu" aria-labelledby="tab-tools"{prefix}>'
        + "".join(items)
        + "</div></div>"
    )


def _profile_bulk_bar(bar_id: str, scope: str) -> str:
    return (
        f'    <div id="{bar_id}" class="cw-tl-bulk cw-selection-bar" role="group" aria-label="[[t:common.selection.label]]" hidden>\n'
        f'      <span class="cw-selection-count" aria-live="polite"><span class="material-symbols-rounded" aria-hidden="true">check_box_outline_blank</span><strong data-{scope}-count>0</strong><span>[[t:common.selection.selected]]</span></span>\n'
        f'      <button class="cw-selection-button" type="button" data-{scope}-select="visible"><span class="material-symbols-rounded" aria-hidden="true">visibility</span>[[t:common.selection.visible]]</button>\n'
        f'      <button class="cw-selection-button" type="button" data-{scope}-select="all"><span class="material-symbols-rounded" aria-hidden="true">checklist</span>[[t:common.selection.allResults]]</button>\n'
        f'      <button class="cw-selection-button" type="button" data-{scope}-select="none"><span class="material-symbols-rounded" aria-hidden="true">close</span>[[t:common.actions.clear]]</button>\n'
        '      <span class="cw-selection-actions">\n'
        f'        <button class="cw-selection-button is-danger" type="button" data-{scope}-remove><span class="material-symbols-rounded" aria-hidden="true">delete</span>[[t:common.actions.remove]]</button>\n'
        '      </span>\n'
        '    </div>\n'
    )


def _profile_remove_dialog() -> str:
    return """  <dialog id="profile-remove-dialog" class="cw-tl-dialog cw-tl-dialog--wide" aria-labelledby="profile-remove-title">
    <form method="dialog" class="cw-tl-dialog-body">
      <div class="cw-tl-remove-head">
        <span class="cw-tl-remove-icon" aria-hidden="true"><span class="material-symbols-rounded">delete</span></span>
        <div class="cw-tl-remove-heading"><strong id="profile-remove-title">[[t:profile.remove.title]]</strong><small id="profile-remove-sub"></small></div>
        <button class="cw-tl-remove-x" value="cancel" data-remove-close aria-label="[[t:common.actions.close]]" title="[[t:common.actions.close]]"><span class="material-symbols-rounded" aria-hidden="true">close</span></button>
      </div>
      <div id="profile-remove-targets" class="cw-tl-remove-list" aria-live="polite"></div>
      <div id="profile-remove-note" class="cw-tl-remove-info" hidden>
        <span class="material-symbols-rounded" aria-hidden="true">info</span>
        <div><strong id="profile-remove-note-main"></strong><small id="profile-remove-note-detail"></small></div>
      </div>
      <div id="profile-remove-error" class="cw-tl-dialog-error" role="alert"></div>
      <div id="profile-remove-result" class="cw-tl-remove-result" aria-live="polite"></div>
      <div class="cw-tl-dialog-actions">
        <button class="cw-tl-ghost" type="button" data-remove-all>[[t:common.selection.selectAll]]</button>
        <span class="cw-tl-spacer"></span>
        <button class="cw-tl-ghost" value="cancel" data-remove-close>[[t:common.actions.close]]</button>
        <button class="cw-tl-primary cw-tl-remove-submit cw-danger-confirm" type="button" data-remove-submit disabled><span class="material-symbols-rounded" aria-hidden="true">delete</span><span>[[t:common.actions.remove]]</span></button>
      </div>
    </form>
  </dialog>
"""


def _profile_timeline_panel(
    key: str,
    *,
    title: str,
    lede: str,
    search: str,
    sources: tuple[tuple[str, str], ...],
    coverage: tuple[tuple[str, str], ...],
    scores: bool = False,
    bulk: bool = False,
    timeline: bool = True,
) -> str:
    timeline_button = (
        '          <button class="cw-view-timeline" type="button" data-timeline-toggle title="[[t:profile.records.showTimeline]]" aria-label="[[t:profile.records.showTimeline]]" aria-pressed="false"><span class="material-symbols-rounded" aria-hidden="true">timeline</span></button>\n'
        if timeline else ""
    )
    source_block = ""
    lede_text = f'<p id="profile-{key}-lede"{" hidden" if not lede else ""}>{f"[[t:{lede}]]" if lede else ""}</p>'
    if len(sources) > 1:
        buttons = "\n".join(
            f'      <button class="{"active" if index == 0 else ""}" type="button" role="tab" aria-selected="{"true" if index == 0 else "false"}" data-timeline-source="{value}">\n'
            f'        <span>[[t:{label}]]</span>\n'
            "      </button>"
            for index, (value, label) in enumerate(sources)
        )
        source_block = f'    <div class="cw-hub-source-line"><div class="cw-hub-sources" role="tablist" aria-label="[[t:profile.records.sourceLabel|title@{title}]]">\n{buttons}\n    </div>{lede_text}</div>\n'
    scores_block = f'    <div id="profile-{key}-scores" class="cw-hist-scores" aria-label="[[t:profile.records.filterByScore]]"></div>\n' if scores else ""
    options = "\n".join(f'          <option value="{value}">[[t:{label}]]</option>' for value, label in coverage)
    bulk_block = _profile_bulk_bar(f"profile-{key}-bulk", "timeline") if bulk else ""
    default_source = sources[0][0] if sources else ""
    lede_block = "" if source_block else lede_text
    timeline_block = f'<div id="profile-{key}-timeline" class="cw-hist-timeline" aria-label="[[t:profile.records.perMonth|title@{title}]]" hidden></div>'
    coverage_select = f'<select id="profile-{key}-coverage" class="cw-hist-coverage-select" aria-label="[[t:profile.records.coverageFilter]]">{options}</select>'
    filter_button = f'<button type="button" class="cw-hub-filter-toggle" data-timeline-filters-toggle aria-controls="profile-{key}-filters" aria-expanded="false">[[t:common.labels.filters]]</button>'
    filter_block = f'<div id="profile-{key}-filters" class="cw-hub-records-filters" hidden><label>[[t:profile.records.coverage]] {coverage_select}</label></div>'
    return f"""  <section id="profile-panel-{key}" class="cw-profile-panel cw-timeline-panel cw-hub-records cw-page-panel" data-source="{default_source}" data-coverage="on" data-dated="{"on" if timeline else "off"}">
    <div class="cw-hub-records-head">
      <div class="cw-hub-records-headline">
        <h2 id="profile-{key}-title">[[t:{title}]]</h2>
        {lede_block}
      </div>
      <div class="cw-hub-records-tiles" id="profile-{key}-metrics"></div>
    </div>
{source_block}
{scores_block}    <div class="cw-hub-records-toolbar">
      <label class="cw-hub-records-search">
        <span class="material-symbols-rounded" aria-hidden="true">search</span>
        <input id="profile-{key}-search" type="search" placeholder="[[t:{search}]]">
      </label>
      <div class="cw-hub-records-chips" id="profile-{key}-types" aria-label="[[t:profile.records.typeFilters|title@{title}]]"></div>
      <div class="cw-hub-records-controls">
        <select id="profile-{key}-provider" aria-label="[[t:profile.records.providerFilter]]"><option value="">[[t:profile.records.allProviders]]</option></select>
        {filter_button}
        <div class="cw-hub-records-views" role="group" aria-label="[[t:profile.records.viewMode]]">
{timeline_button}          <button class="active" type="button" data-timeline-view="grid" title="[[t:profile.records.gridView]]" aria-label="[[t:profile.records.gridView]]" aria-pressed="true"><span class="material-symbols-rounded" aria-hidden="true">grid_view</span></button>
          <button type="button" data-timeline-view="list" title="[[t:profile.records.listView]]" aria-label="[[t:profile.records.listView]]" aria-pressed="false"><span class="material-symbols-rounded" aria-hidden="true">view_list</span></button>
        </div>
        <button id="profile-{key}-refresh" class="cw-tl-ghost cw-collection-refresh" type="button" data-timeline-refresh title="[[t:profile.records.refresh|title@{title}]]" aria-label="[[t:profile.records.refresh|title@{title}]]"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button>
      </div>
    </div>
    {filter_block}
    {timeline_block}
    <div id="profile-{key}-list" class="cw-hist-list" data-view="grid" aria-live="polite"></div>
    <div id="profile-{key}-footer" class="cw-collection-footer" hidden>
      <nav id="profile-{key}-pages" class="cw-collection-pages" aria-label="[[t:profile.records.pages|title@{title}]]"></nav>
      <div class="cw-collection-summary">
        <span id="profile-{key}-page-label"></span>
        <select id="profile-{key}-page-size" aria-label="[[t:profile.records.perPage]]">
          <option value="24">24</option>
          <option value="48">48</option>
          <option value="96">96</option>
        </select>
      </div>
    </div>
{bulk_block}  </section>"""


def _profile_playback_panel() -> str:
    return """  <section id="profile-panel-playback" class="cw-profile-panel cw-timeline-panel cw-playback-panel cw-hub-records cw-page-panel" data-source="playback">
    <div class="cw-hub-records-head">
      <h2>[[t:profile.playback.title]]</h2>
      <div class="cw-hub-records-tiles" id="profile-playback-metrics"></div>
    </div>
    <div class="cw-hub-progress-summary">
      <span id="profile-playback-sync" class="cw-play-sync" data-state="ready" aria-live="polite"><span class="cw-play-sync-dot" aria-hidden="true"></span><span>[[t:profile.playback.updated]] <strong>[[t:profile.playback.notYet]]</strong></span></span>
    </div>
    <div id="profile-playback-status" class="cw-play-status">
      <span id="profile-playback-errors" class="cw-play-issues" role="status" hidden></span>
    </div>
    <div class="cw-hub-records-toolbar">
      <label class="cw-hub-records-search">
        <span class="material-symbols-rounded" aria-hidden="true">search</span>
        <input id="profile-playback-search" type="search" placeholder="[[t:profile.playback.search]]">
      </label>
      <div class="cw-hub-records-chips" id="profile-playback-types" aria-label="[[t:profile.playback.typeFilters]]"></div>
      <div class="cw-hub-records-controls">
        <div class="cw-play-filters">
          <button id="profile-playback-filters-btn" class="cw-hub-filter-toggle cw-play-filterbtn" type="button" data-playback-filters-toggle aria-expanded="false" aria-haspopup="true" aria-controls="profile-playback-filters-panel" title="[[t:profile.playback.filter]]" aria-label="[[t:profile.playback.filter]]"><span class="material-symbols-rounded" aria-hidden="true">filter_list</span><span>[[t:common.labels.filters]]</span><span id="profile-playback-filters-count" class="cw-play-filtercount" aria-live="polite" hidden>0</span></button>
          <div id="profile-playback-filters-panel" class="cw-play-filterpanel" role="group" aria-labelledby="profile-playback-filters-btn" hidden>
            <label class="cw-play-filterrow"><span>[[t:common.labels.provider]]</span>
              <select id="profile-playback-provider" aria-label="[[t:profile.records.providerFilter]]"><option value="">[[t:profile.records.allProviders]]</option></select>
            </label>
            <label class="cw-play-filterrow"><span>[[t:profile.playback.title]]</span>
              <select id="profile-playback-progress" aria-label="[[t:profile.playback.progressFilter]]">
                <option value="">[[t:profile.playback.anyProgress]]</option>
                <option value="0:24.99">[[t:profile.playback.under25]]</option>
                <option value="25:50">[[t:profile.playback.range25to50]]</option>
                <option value="50:75">[[t:profile.playback.range50to75]]</option>
                <option value="75:100">[[t:profile.playback.over75]]</option>
                <option value="90:100">[[t:profile.playback.almostDone]]</option>
              </select>
            </label>
            <label class="cw-play-filterrow"><span>[[t:profile.playback.paused]]</span>
              <select id="profile-playback-age" aria-label="[[t:profile.playback.pausedFilter]]">
                <option value="">[[t:profile.playback.anyTime]]</option>
                <option value="today">[[t:common.time.today]]</option>
                <option value="7d">[[t:common.time.last7Days]]</option>
                <option value="30d">[[t:common.time.last30Days]]</option>
                <option value="older_30d">[[t:common.time.olderThan30Days]]</option>
              </select>
            </label>
            <label class="cw-play-filterrow"><span>[[t:common.labels.sort]]</span>
              <select id="profile-playback-sort" aria-label="[[t:common.labels.sort]]">
                <option value="last_updated">[[t:profile.playback.sort.recent]]</option>
                <option value="progress_high">[[t:profile.playback.sort.mostWatched]]</option>
                <option value="progress_low">[[t:profile.playback.sort.leastWatched]]</option>
                <option value="remaining_time">[[t:profile.playback.sort.shortestRemaining]]</option>
                <option value="rating_high">[[t:profile.playback.sort.highestRated]]</option>
                <option value="title">[[t:profile.playback.sort.title]]</option>
                <option value="provider">[[t:common.labels.provider]]</option>
              </select>
            </label>
            <div class="cw-play-filterfoot">
              <button class="cw-tl-ghost" type="button" data-playback-filters-clear>[[t:common.actions.clearAll]]</button>
            </div>
          </div>
        </div>
        <div class="cw-hub-records-views" role="group" aria-label="[[t:profile.records.viewMode]]">
          <button class="cw-view-timeline" type="button" data-playback-timeline title="[[t:profile.records.showTimeline]]" aria-label="[[t:profile.records.showTimeline]]" aria-pressed="false"><span class="material-symbols-rounded" aria-hidden="true">timeline</span></button>
          <button class="active" type="button" data-playback-view="grid" title="[[t:profile.records.gridView]]" aria-label="[[t:profile.records.gridView]]" aria-pressed="true"><span class="material-symbols-rounded" aria-hidden="true">grid_view</span></button>
          <button type="button" data-playback-view="list" title="[[t:profile.records.listView]]" aria-label="[[t:profile.records.listView]]" aria-pressed="false"><span class="material-symbols-rounded" aria-hidden="true">view_list</span></button>
        </div>
        <button id="profile-playback-settings" class="cw-hub-filter-toggle cw-play-toolbtn" type="button" data-playback-open-settings hidden title="[[t:profile.playback.chooseProviders]]" aria-label="[[t:profile.playback.chooseProviders]]"><span class="material-symbols-rounded" aria-hidden="true">tune</span></button>
        <button id="profile-playback-refresh" class="cw-hub-filter-toggle cw-play-toolbtn" type="button" data-playback-retry title="[[t:profile.playback.refresh]]" aria-label="[[t:profile.playback.refresh]]"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button>
      </div>
    </div>
    <div id="profile-playback-timeline" class="cw-hist-timeline" aria-label="[[t:profile.playback.perMonth]]" hidden></div>
    <div id="profile-playback-list" class="cw-hist-list" data-view="grid" aria-live="polite"></div>
    <div id="profile-playback-footer" class="cw-collection-footer" hidden>
      <nav id="profile-playback-pages" class="cw-collection-pages" aria-label="[[t:profile.playback.pages]]"></nav>
      <div class="cw-collection-summary">
        <span id="profile-playback-page-label"></span>
        <select id="profile-playback-page-size" aria-label="[[t:profile.records.perPage]]">
          <option value="24">24</option>
          <option value="48">48</option>
          <option value="96">96</option>
        </select>
      </div>
    </div>
    <div id="profile-playback-bulk" class="cw-tl-bulk cw-selection-bar" role="group" aria-label="[[t:common.selection.label]]" hidden>
      <span class="cw-selection-count" aria-live="polite"><span class="material-symbols-rounded" aria-hidden="true">check_box_outline_blank</span><strong data-playback-count>0</strong><span>[[t:common.selection.selected]]</span></span>
      <button class="cw-selection-button" type="button" data-playback-bulk-select="visible"><span class="material-symbols-rounded" aria-hidden="true">visibility</span>[[t:common.selection.visible]]</button>
      <button class="cw-selection-button" type="button" data-playback-bulk-select="all"><span class="material-symbols-rounded" aria-hidden="true">checklist</span>[[t:common.selection.allResults]]</button>
      <button class="cw-selection-button" type="button" data-playback-bulk-select="none"><span class="material-symbols-rounded" aria-hidden="true">close</span>[[t:common.actions.clear]]</button>
      <span class="cw-selection-actions">
        <button class="cw-selection-button" type="button" data-playback-bulk="update_progress" title="[[t:profile.playback.edit]]"><span class="material-symbols-rounded" aria-hidden="true">edit</span>[[t:profile.playback.edit]]</button>
        <button class="cw-selection-button is-good" type="button" data-playback-bulk="mark_watched" title="[[t:profile.playback.markWatchedTitle]]"><span class="material-symbols-rounded" aria-hidden="true">check_circle</span>[[t:profile.playback.markWatched]]</button>
        <button class="cw-selection-button is-danger" type="button" data-playback-bulk="remove_progress" title="[[t:profile.playback.removeTitle]]"><span class="material-symbols-rounded" aria-hidden="true">delete</span>[[t:common.actions.remove]]</button>
      </span>
    </div>
    <dialog id="profile-playback-edit" class="cw-tl-dialog" aria-labelledby="profile-playback-edit-title">
      <form method="dialog" class="cw-tl-dialog-body">
        <div class="cw-tl-dialog-head">
          <span class="material-symbols-rounded" aria-hidden="true">edit</span>
          <span><strong id="profile-playback-edit-title">[[t:profile.playback.edit]]</strong><small id="profile-playback-edit-sub"></small></span>
        </div>
        <div class="cw-play-edit">
          <input id="profile-playback-edit-range" type="range" min="2" max="99" step="1" aria-label="[[t:profile.playback.title]]">
          <span class="cw-play-edit-value"><input id="profile-playback-edit-value" type="number" min="2" max="99" step="0.01" aria-label="[[t:profile.playback.percent]]"><span>%</span></span>
        </div>
        <div id="profile-playback-edit-error" class="cw-tl-dialog-error" role="alert"></div>
        <div class="cw-tl-dialog-actions">
          <button class="cw-tl-ghost" type="button" data-playback-dialog-cancel>[[t:common.actions.cancel]]</button>
          <button class="cw-tl-primary" type="submit">[[t:common.actions.apply]]</button>
        </div>
      </form>
    </dialog>
    <dialog id="profile-playback-providers" class="cw-page-playback-settings" aria-labelledby="profile-playback-providers-title">
      <form method="dialog" class="cw-page-preferences">
        <header class="pf-head"><div><h2 id="profile-playback-providers-title">[[t:profile.playback.settings.title]]</h2><p>[[t:profile.playback.settings.intro]]</p></div><button class="pf-button" type="button" data-playback-dialog-cancel>[[t:common.actions.close]]</button></header>
        <div class="pf-body">
          <label class="pf-grouping"><span><strong>[[t:profile.playback.settings.group]]</strong><small id="profile-playback-grouping-help">[[t:profile.playback.settings.groupHelp]]</small></span><select id="profile-playback-grouping" class="cw-page-input" aria-describedby="profile-playback-grouping-help"><option value="automatic">[[t:profile.playback.settings.automatic]]</option><option value="separate">[[t:profile.playback.settings.separate]]</option></select></label>
          <section class="pf-section"><h3>[[t:profile.playback.settings.included]]</h3><div id="profile-playback-provider-list" class="pf-provider-list"></div></section>
          <details class="pf-advanced"><summary>[[t:common.labels.advanced]]</summary><label class="pf-timeout"><span><strong>[[t:profile.playback.settings.timeout]]</strong><small>[[t:profile.playback.settings.timeoutHelp]]</small></span><span class="pf-timeout-value"><input id="profile-playback-timeout" class="cw-page-input" type="number" min="3" max="60" step="1" aria-label="[[t:profile.playback.settings.timeoutSeconds]]"><span>[[t:common.units.secShort]]</span></span></label></details>
        </div>
        <footer class="pf-footer"><div id="profile-playback-providers-error" class="pf-error" role="alert"></div><button class="pf-button pf-reset" type="button" data-playback-settings-reset>[[t:common.actions.resetDefaults]]</button><div class="pf-actions"><button class="pf-button" type="button" data-playback-dialog-cancel>[[t:common.actions.cancel]]</button><button class="pf-button primary" type="submit">[[t:common.actions.apply]]</button></div></footer>
      </form>
    </dialog>
  </section>"""


def _current_account_avatar_url(user: dict | None) -> str:
    url = str((user or {}).get("avatar_url") or "").strip() if isinstance(user, dict) else ""
    if url.startswith("/api/profile/avatar/"):
        q = url.find("?")
        return f"/api/profile/avatar{url[q:]}" if q >= 0 else "/api/profile/avatar"
    return url


def _avatar_img_html(url: str) -> str:
    safe = html_lib.escape(url, quote=True)
    return f'<img src="{safe}" alt="">'


def _nav_profile_avatar(user: dict | None = None) -> str:
    url = _current_account_avatar_url(user)
    if url:
        safe = html_lib.escape(url, quote=True)
        return f'<span id="cw-nav-profile-avatar" class="cw-nav-profile-avatar" aria-hidden="true" data-cw-avatar-url="{safe}">{_avatar_img_html(url)}</span>'
    return '<span id="cw-nav-profile-avatar" class="cw-nav-profile-avatar material-symbols-rounded" aria-hidden="true">person</span>'


def _seed_nav_profile_avatar(html: str, user: dict | None = None) -> str:
    placeholder = '<span id="cw-nav-profile-avatar" class="cw-nav-profile-avatar material-symbols-rounded" aria-hidden="true">person</span>'
    return html.replace(placeholder, _nav_profile_avatar(user), 1)


def _profile_avatar_inner(user: dict | None = None) -> str:
    url = _current_account_avatar_url(user)
    if url:
        return _avatar_img_html(url)
    return '<span class="material-symbols-rounded" aria-hidden="true">person</span>'


def _profile_menu_items(user: dict | None = None) -> str:
    user = user or {}
    name = html_lib.escape(str(user.get("display_name") or user.get("label") or user.get("username") or "")) or "[[t:navigation.account.profileFallback]]"
    username = html_lib.escape(str(user.get("username") or ""))
    return f"""<div class="cw-account-identity"><strong>{name}</strong><span>@{username}</span></div>
        <div data-cw-account-panel="main">
          <button class="cw-menu-item" type="button" role="menuitem" data-cw-profile-menu-action="account"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">person</span><span>[[t:navigation.account.myProfile]]</span></button>
          <button class="cw-menu-item" type="button" role="menuitem" data-cw-account-view="help"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">help</span><span>[[t:navigation.account.helpAbout]]</span></button>
          <div class="cw-menu-sep" role="separator"></div>
          <button class="cw-menu-item danger" type="button" role="menuitem" data-cw-profile-menu-action="logout"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">logout</span><span>[[t:common.actions.logOut]]</span></button>
        </div>
        <div data-cw-account-panel="help" hidden>
          <button class="cw-menu-item" type="button" role="menuitem" data-cw-account-view="main"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">arrow_back</span><span>[[t:common.actions.back]]</span></button>
          <button class="cw-menu-item" type="button" role="menuitem" data-cw-profile-menu-action="help"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">help</span><span>[[t:navigation.aboutMenu.help]]</span></button>
          <button class="cw-menu-item" type="button" role="menuitem" data-cw-profile-menu-action="about"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">info</span><span>[[t:navigation.account.aboutCrossWatch]]</span></button>
        </div>"""


def _nav_profile_link(user: dict | None = None) -> str:
    return """    <div class="cw-nav-profile-menu" id="cw-nav-profile-menu">
      <button id="cw-nav-profile-link" class="cw-nav-profile-link" type="button" title="[[t:navigation.account.openMenu]]" aria-label="[[t:navigation.account.openMenu]]" aria-haspopup="menu" aria-expanded="false">
        __CW_NAV_PROFILE_AVATAR__
        <span class="tab-caret" aria-hidden="true"></span>
      </button>
      <div class="cw-menu cw-profile-menu cw-page-account-menu hidden" id="cw-profile-menu" role="menu" aria-labelledby="cw-nav-profile-link">
__CW_NAV_PROFILE_ITEMS__
      </div>
    </div>""".replace("__CW_NAV_PROFILE_AVATAR__", _nav_profile_avatar(user)).replace("__CW_NAV_PROFILE_ITEMS__", _profile_menu_items(user))


def _managed_user_shell(html: str, user: dict | None = None) -> str:
    html = html.replace('  <section id="page-maintenance" class="card hidden tab-page"></section>\n\n', "")
    html = html.replace('  <section id="page-backups" class="hidden tab-page cw-page-backups" aria-labelledby="br-title"></section>\n\n', "")
    tracker = _managed_tracker_allowed(user)
    if not tracker:
        html = html.replace('  <section id="page-tracker" class="card hidden tab-page cw-themed-page"></section>\n\n', "")
    html = html.replace('    <button id="tab-tracker" class="tab" type="button" onclick="showTab(\'tracker\')">[[t:navigation.tabs.tracker]]</button>\n', "")
    perms = _managed_user_permissions(user)
    write_allowed = bool(perms.get("write"))
    dashboard_allowed = bool(perms.get("dashboard")) and write_allowed
    settings_panes = _managed_settings_panes(user) if write_allowed else set()
    if write_allowed:
        tabs = [
            '    <button id="tab-main" class="tab active" type="button" onclick="showTab(\'main\')">[[t:navigation.tabs.main]]</button>',
            *(['    <button id="tab-tracker" class="tab" type="button" onclick="showTab(\'tracker\')">[[t:navigation.tabs.tracker]]</button>'] if tracker else []),
            _profile_nav_tab("profile", "/hub"),
            '    <button id="tab-snapshots" class="tab" type="button" onclick="showTab(\'snapshots\')">[[t:navigation.tabs.captures]]</button>',
            '    <button id="tab-playlists" class="tab" type="button" onclick="showTab(\'playlists\')">[[t:navigation.tabs.playlists]]</button>',
            "    " + _tools_menu(maintenance=False),
            *(['    <button id="tab-settings" class="tab" type="button" onclick="showTab(\'settings\')">[[t:navigation.tabs.settings]]</button>'] if settings_panes else []),
            _nav_profile_link(user),
        ]
        html = _replace_html_range(
            html,
            '  <nav class="tabs" aria-label="[[t:navigation.primary]]">\n',
            '  </nav>',
            '  <nav class="tabs" aria-label="[[t:navigation.primary]]">\n' + "\n".join(tabs) + "\n",
        )
    else:
        tabs = [_profile_nav_tab("profile", "/hub")]
        tabs.append(_nav_profile_link(user))
        html = html.replace(
            '<div class="brand" role="button" tabindex="0" title="[[t:navigation.brand.goMain]]" onclick="showTab(\'main\')" onkeypress="if(event.key===\'Enter\'||event.key===\' \')showTab(\'main\')">',
            '<div class="brand" role="button" tabindex="0" title="[[t:navigation.brand.goHub]]" onclick="location.href=\'/hub\'" onkeypress="if(event.key===\'Enter\'||event.key===\' \')location.href=\'/hub\'">',
        )
        html = _replace_html_range(
            html,
            '  <nav class="tabs" aria-label="[[t:navigation.primary]]">\n',
            '  </nav>',
            '  <nav class="tabs" aria-label="[[t:navigation.primary]]">\n' + "\n".join(tabs) + "\n",
        )
        html = html.replace('    <button id="tab-snapshots" class="tab" type="button" onclick="showTab(\'snapshots\')">[[t:navigation.tabs.captures]]</button>\n', "")
        html = html.replace('    <button id="tab-playlists" class="tab" type="button" onclick="showTab(\'playlists\')">[[t:navigation.tabs.playlists]]</button>\n', "")
        for fragment in (
            '  <section id="page-snapshots" class="card hidden tab-page cw-themed-page"></section>\n\n',
            '  <section id="page-capture_compare" class="card hidden tab-page"></section>\n\n',
            '  <section id="page-playlists" class="card hidden tab-page cw-themed-page"></section>\n\n',
            '  <section id="page-editor" class="card hidden tab-page cw-themed-page"></section>\n\n',
            '  <section id="page-analyzer" class="card hidden tab-page"></section>\n\n',
            '  <section id="page-events" class="card hidden tab-page"></section>\n\n',
            '  <section id="page-logs" class="card hidden tab-page"></section>\n\n',
            '  <section id="page-import_export" class="card hidden tab-page"></section>\n\n',
            '  <section id="page-interactive_sync" class="card hidden tab-page"></section>\n\n',
        ):
            html = html.replace(fragment, "")
    if not dashboard_allowed:
        html = _remove_line(html, '    <button id="tab-main" class="tab active" type="button" onclick="showTab(\'main\')">[[t:navigation.tabs.main]]</button>')
        html = _remove_html_section(html, '  <section id="ops-card"')
        html = _remove_html_section(html, '  <section id="stats-card"')
        html = _remove_html_section(html, '  <section id="dashboard-widgets-card"')
    if settings_panes:
        html = _strip_settings_panes(html, settings_panes)
        html = _remove_line(html, '<div id="save-frost" class="hidden" aria-hidden="true"></div>')
        html = _remove_line(html, '<div id="save-fab" class="hidden" role="toolbar" aria-label="[[t:common.actions.stickySave]]"><button id="save-fab-btn" class="btn" onclick="saveSettings(this)"><span class="btn-label">[[t:settings.saveFab]]</span></button></div>')
    else:
        html = _remove_html_range(
            html,
            '  <section id="page-settings" class="card hidden">',
            "\n\n</main>",
        )
    html = _remove_line(html, _ASSISTANT_MENU_ITEM)
    _assert_admin_shell_stripped(html, settings_panes=settings_panes, tracker=tracker)
    return html


def _asset_block(include_admin: bool = True, user: dict | None = None) -> str:
    full_user = not include_admin and bool(_managed_user_permissions(user).get("write"))
    helper_scripts = _HELPER_SCRIPTS if include_admin else (_FULL_USER_HELPER_SCRIPTS if full_user else _USER_HELPER_SCRIPTS)
    app_scripts = _APP_SCRIPTS if include_admin else (_FULL_USER_APP_SCRIPTS if full_user else _USER_APP_SCRIPTS)
    managed_auth_tags: tuple[str, ...] = ()
    if full_user and managed_settings_access(user):
        panes = _managed_settings_panes(user)
        if panes & {"providers", "sync"}:
            helper_scripts = (*helper_scripts, "providers-ui.js")
        if "providers" in panes:
            managed_auth_tags = (
                '<script src="/assets/auth/auth.shared.js?v=__CW_VERSION__" defer></script>',
                '<script src="/assets/auth/auth_loader.js?v=__CW_VERSION__" defer></script>',
            )
        app_scripts = (
            *app_scripts,
            "settings-scope.js",
            *(("connections.overlay.js", "connections.pairs.overlay.js") if "sync" in panes else ()),
            *(("scrobbler.js",) if "scrobbler" in panes else ()),
            *(("scheduler-profiles.js",) if "scheduling" in panes else ()),
        )
    if full_user and _managed_tracker_allowed(user):
        helper_scripts = (*helper_scripts, "month-bar.js")
    helper_tags = "\n".join(f'<script src="/assets/helpers/{name}?v=__CW_VERSION__" defer></script>' for name in helper_scripts)
    app_tags = "\n".join(f'<script src="/assets/js/{name}?v=__CW_VERSION__" defer></script>' for name in app_scripts)
    admin_only_tags = (
        '<script src="/assets/helpers/media_user_picker.js?v=__CW_VERSION__" defer></script>',
        '<script src="/assets/helpers/whitelist_table.js?v=__CW_VERSION__" defer></script>',
        '<script src="/assets/auth/auth.shared.js?v=__CW_VERSION__" defer></script>',
        '<script src="/assets/auth/auth.sync-users.js?v=__CW_VERSION__" defer></script>',
        '<script src="/assets/auth/auth_loader.js?v=__CW_VERSION__" defer></script>',
        '<script src="/assets/auth/auth.tmdb.js?v=__CW_VERSION__" defer></script>',
        '<script type="module" src="/assets/js/modals.js?v=__CW_VERSION__"></script>',
        '<script type="module" src="/assets/js/topology/advisor.js?v=__CW_VERSION__"></script>',
    ) if include_admin else (('<script type="module" src="/assets/js/modals.js?v=__CW_VERSION__"></script>',) if full_user else ())
    return "\n".join((
        '<script src="/assets/helpers/dialog-focus.js?v=__CW_VERSION__" defer></script>',
        '<script src="/assets/helpers/clipboard.js?v=__CW_VERSION__" defer></script>',
        helper_tags,
        '<script src="/assets/crosswatch.js?v=__CW_VERSION__" defer></script>',
        app_tags,
        *admin_only_tags,
        *managed_auth_tags,
        '<script src="/assets/js/theme-flat-runtime.js?v=__CW_VERSION__" defer></script>',
    ))


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
__CW_I18N_HEAD__
<script>
(() => {
  const APP_NAME = "CrossWatch";
  const TITLES = {
    main: "navigation.tabs.main",
    watchlist: "common.media.watchlist",
    snapshots: "navigation.tabs.captures",
    capture_compare: "navigation.pages.captureCompare",
    playlists: "navigation.tabs.playlists",
    tracker: "navigation.tabs.tracker",
    editor: "navigation.pages.editor",
    analyzer: "navigation.pages.analyzer",
    events: "navigation.pages.events",
    logs: "navigation.pages.logs",
    import_export: "navigation.pages.importExport",
    maintenance: "navigation.pages.maintenance",
    backups: "navigation.pages.backups",
    interactive_sync: "navigation.pages.interactiveSync",
    settings: "navigation.tabs.settings",
  };

  const normalize = (value) => String(value || "").trim().toLowerCase();

  const setTitle = (page) => {
    const key = TITLES[normalize(page)];
    const label = key && window.CW?.I18n ? window.CW.I18n.t(key) : "";
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
<link rel="icon" type="image/png" sizes="64x64" href="/assets/pwa/favicon-64.png?v=__CW_VERSION__"><link rel="alternate icon" href="/favicon.ico?v=__CW_VERSION__">
<meta name="theme-color" content="#090b10">
<link rel="manifest" href="/manifest.webmanifest">
<link rel="apple-touch-icon" sizes="180x180" href="/assets/pwa/apple-touch-icon.png?v=__CW_VERSION__">
<meta name="application-name" content="CrossWatch">
<meta name="apple-mobile-web-app-title" content="CrossWatch">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black">
<script>
(() => {
  try {
    const raw = localStorage.getItem("cw.ui.theme") || "flat-dark";
    const theme = raw === "flat-light" ? "flat-light" : raw === "original" ? "original" : "flat-dark";
    if (theme === "original") {
      delete document.documentElement.dataset.cwTheme;
    } else {
      document.documentElement.dataset.cwTheme = theme;
    }
    document.documentElement.classList.toggle("cw-theme-light", theme === "flat-light");
    document.documentElement.classList.toggle("cw-theme-dark", theme === "flat-dark");
    document.documentElement.classList.toggle("cw-theme-original", theme === "original");
  } catch {
    document.documentElement.dataset.cwTheme = "flat-dark";
  }
})();
</script>

<link rel="stylesheet" href="/assets/themes/tokens.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/base.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/providers.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/crosswatch.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/auth-providers.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/layout.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/components.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/pages.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/page-theme.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/ui-shell.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/account-menu.css?v=__CW_VERSION__">
<link id="cw-notifications-css" rel="stylesheet" href="/assets/css/notifications.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/app-users.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/user-profile-wizard.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/ai-settings.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/topology.css?v=__CW_VERSION__">
<script>
(() => {
  try {
    const segment = (value) => {
      try { value = decodeURIComponent(String(value || "")); } catch {}
      return String(value || "").trim().toLowerCase().replace(/-/g, "_");
    };
    const [path, query = ""] = String(window.location.hash || "").replace(/^#\/?/, "").split("?");
    const parts = path.split("/").filter(Boolean);
    const route = segment(parts[0]);
    if (route === "playback_progress") {
      window.location.replace("/hub#playback");
      return;
    }
    if (route === "watchlist") {
      window.location.replace("/hub#watchlist");
      return;
    }
    const tabs = new Set(["snapshots", "capture_compare", "playlists", "editor", "analyzer", "events", "logs", "import_export", "interactive_sync", "maintenance", "backups", "settings"]);
    let tab = tabs.has(route) ? route : "main";
    document.documentElement.dataset.cwInitialTab = tab;
    document.documentElement.dataset.tab = tab;
    if (tab === "settings") {
      const aliases = { pairs: "sync", automation: "scheduling", ui: "app", security: "app" };
      const requested = segment(parts[1] || new URLSearchParams(query).get("pane"));
      const pane = Object.hasOwn(aliases, requested) ? aliases[requested] : requested;
      const panes = new Set(["overview", "providers", "sync", "scrobbler", "scheduling", "app", "maintenance"]);
      window.__cwSettingsPane = panes.has(pane) ? pane : "overview";
      document.documentElement.dataset.cwInitialPane = window.__cwSettingsPane;
    }
  } catch {
    document.documentElement.dataset.cwInitialTab = "main";
  }
})();
</script>
<style>
/* Keep Main's unfinished shell out of the first paint. */
#cw-main-loading{display:none}
html[data-cw-main-pending][data-tab="main"] #layout{visibility:hidden;pointer-events:none}
html[data-cw-main-pending][data-tab="main"] #cw-main-loading{display:flex;position:fixed;inset:120px 0 auto;justify-content:center;color:var(--muted);padding:24px}
/* Match the routed layout before the deferred router starts. */
html[data-tab]:not([data-tab="main"]) #layout{grid-template-columns:minmax(0,1fr)}
html[data-cw-initial-pane] #page-settings .cw-settings-pane{display:none!important}
html[data-cw-initial-pane="overview"] #page-settings .cw-settings-pane[data-pane="overview"],
html[data-cw-initial-pane="providers"] #page-settings .cw-settings-pane[data-pane="providers"],
html[data-cw-initial-pane="sync"] #page-settings .cw-settings-pane[data-pane="sync"],
html[data-cw-initial-pane="scrobbler"] #page-settings .cw-settings-pane[data-pane="scrobbler"],
html[data-cw-initial-pane="scheduling"] #page-settings .cw-settings-pane[data-pane="scheduling"],
html[data-cw-initial-pane="app"] #page-settings .cw-settings-pane[data-pane="app"],
html[data-cw-initial-pane="maintenance"] #page-settings .cw-settings-pane[data-pane="maintenance"]{display:block!important}
html[data-cw-initial-tab]:not([data-cw-initial-tab="main"]) #ops-card,
html[data-cw-initial-tab]:not([data-cw-initial-tab="main"]) #stats-card,
html[data-cw-initial-tab]:not([data-cw-initial-tab="main"]) #dashboard-widgets-card,
html[data-cw-initial-tab]:not([data-cw-initial-tab="main"]) #log-panel{display:none!important}
html[data-cw-initial-tab="snapshots"] #page-snapshots,
html[data-cw-initial-tab="capture_compare"] #page-capture_compare,
html[data-cw-initial-tab="playlists"] #page-playlists,
html[data-cw-initial-tab="tracker"] #page-tracker,
html[data-cw-initial-tab="editor"] #page-editor,
html[data-cw-initial-tab="analyzer"] #page-analyzer,
html[data-cw-initial-tab="maintenance"] #page-maintenance,
html[data-cw-initial-tab="backups"] #page-backups,
html[data-cw-initial-tab="events"] #page-events,
html[data-cw-initial-tab="logs"] #page-logs,
html[data-cw-initial-tab="import_export"] #page-import_export,
html[data-cw-initial-tab="interactive_sync"] #page-interactive_sync,
html[data-cw-initial-tab="settings"] #page-settings{display:block!important}
</style>

<link rel="preload" href="/assets/fonts/material-symbols-rounded-full-v355.woff2" as="font" type="font/woff2" crossorigin>
<link rel="stylesheet" href="/assets/fonts/material-symbols-rounded.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/js/modals/core/styles.css?v=__CW_VERSION__">
<link id="cw-theme-flat-css" rel="stylesheet" href="/assets/themes/flat.css?v=__CW_VERSION__">
<link id="cw-theme-original-css" rel="stylesheet" href="/assets/themes/original-coverage.css?v=__CW_VERSION__">
<script>
(() => {
  const original = document.documentElement.classList.contains("cw-theme-original");
  const flatLink = document.getElementById("cw-theme-flat-css");
  const originalLink = document.getElementById("cw-theme-original-css");
  flatLink.disabled = original;
  flatLink.media = original ? "not all" : "all";
  originalLink.disabled = !original;
  originalLink.media = original ? "all" : "not all";
})();
</script>
<script>
(() => {
  const root = document.documentElement;
  if (root.dataset.tab !== "main") return;
  root.dataset.cwMainPending = "1";
  const reveal = () => {
    delete root.dataset.cwMainPending;
    clearTimeout(fallback);
  };
  // A failed or stalled script must not leave the page hidden.
  const fallback = setTimeout(reveal, 5000);
  // Let all deferred scripts and ready listeners build the dashboard first.
  document.addEventListener("DOMContentLoaded", () => requestAnimationFrame(reveal), { once: true });
})();
</script>
</head><body>

<header>
  <div class="brand" role="button" tabindex="0" title="[[t:navigation.brand.goMain]]" onclick="showTab('main')" onkeypress="if(event.key==='Enter'||event.key===' ')showTab('main')">
    <img class="logo" src="/assets/img/CROSSWATCH.svg?v=__CW_VERSION__" alt="CrossWatch">
    <span class="brand-text">
      <span class="name cw-wordmark">CrossWatch<span class="brand-dot" aria-hidden="true">.</span></span>
      <span class="version">__CW_CURRENT_VERSION__</span>
    </span>
  </div>

  <nav class="tabs" aria-label="[[t:navigation.primary]]">
    <button id="tab-main" class="tab active" type="button" onclick="showTab('main')">[[t:navigation.tabs.main]]</button>
    <button id="tab-tracker" class="tab" type="button" onclick="showTab('tracker')">[[t:navigation.tabs.tracker]]</button>
    <button id="tab-profile" class="tab" type="button" onclick="location.href='/hub'">[[t:navigation.tabs.hub]]</button>
    <button id="tab-snapshots" class="tab" type="button" onclick="showTab('snapshots')">[[t:navigation.tabs.captures]]</button>
    <button id="tab-playlists" class="tab" type="button" onclick="showTab('playlists')">[[t:navigation.tabs.playlists]]</button>
    __CW_TOOLS_MENU__
    <div class="cw-tabmenu" id="tab-settings-menu">
      <button id="tab-settings" class="tab" type="button"
              aria-haspopup="menu" aria-expanded="false"
              onclick="window.cwToggleSettingsMenu(event)">
        <span>[[t:navigation.tabs.settings]]</span>
        <span class="tab-caret" aria-hidden="true"></span>
      </button>
      <div class="cw-menu hidden" id="cw-settings-menu" role="menu" aria-labelledby="tab-settings">
        <button class="cw-menu-item active" data-settings-pane="overview" type="button" role="menuitem" aria-current="page" onclick="window.cwSettingsMenuSelect('overview')"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">grid_view</span><span>[[t:navigation.settingsMenu.overview]]</span></button>
        <div class="cw-menu-sep" role="separator" aria-hidden="true"></div>
        <button class="cw-menu-item" data-settings-pane="providers" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect('providers')"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">device_hub</span><span>[[t:navigation.settingsMenu.connections]]</span></button>
        <button class="cw-menu-item" data-settings-pane="sync" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect('pairs')"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">sync_alt</span><span>[[t:navigation.settingsMenu.pairs]]</span></button>
        <button class="cw-menu-item" data-settings-pane="scrobbler" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect('scrobbler')"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">sensors</span><span>[[t:navigation.settingsMenu.scrobbler]]</span></button>
        <button class="cw-menu-item" data-settings-pane="scheduling" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect('scheduling')"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">schedule</span><span>[[t:navigation.settingsMenu.scheduling]]</span></button>
        <button class="cw-menu-item" data-settings-pane="app" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect('app')"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">security</span><span>[[t:navigation.settingsMenu.app]]</span></button>
        <button class="cw-menu-item" data-settings-pane="maintenance" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect('maintenance')"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">build</span><span>[[t:navigation.settingsMenu.maintenance]]</span></button>
      </div>
    </div>
__CW_NAV_PROFILE_MENU__
  </nav>

</header>

<div id="cw-main-loading" role="status">[[t:dashboard.loading]]</div>
<main id="layout">
  <section id="ops-card" class="card cw-main-card cw-main-card--sync">
    <div class="title">[[t:dashboard.syncHub.title]]</div>
    <div class="ops-header cw-main-card-head">
      <div class="cw-main-card-head-copy">
        <h2>[[t:dashboard.syncHub.title]]</h2>
      </div>
      <div class="cw-main-card-head-side">
        <div id="conn-badges" class="vip-badges"></div>
        <div class="cw-main-card-head-actions">
          <div id="update-banner" class="hidden"><span id="update-text">[[t:dashboard.update.available]]</span>
            <a id="update-link" href="https://github.com/cenodude/crosswatch/releases" target="_blank" rel="noopener">[[t:dashboard.update.get]]</a>
          </div>
          <button id="btn-status-refresh" class="iconbtn" title="[[t:dashboard.status.recheck]]" aria-label="[[t:dashboard.status.refresh]]">
            <svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">
              <path d="M21 12a9 9 0 1 1-2.64-6.36" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
              <path d="M21 5v5h-5" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
          </button>
        </div>
      </div>
    </div>

    <div class="sync-status" style="display:none"><div id="sync-icon"></div><div id="sync-status-text"></div><span id="sched-inline" style="display:none"></span></div>
    <div id="ux-progress">
      <div class="sb-head">
        <div class="sb-head-main">
          <div>
            <div class="sb-label">[[t:dashboard.progress.title]]</div>
            <div class="sb-phase"><div class="sb-phase-text">[[t:dashboard.progress.waiting]]</div></div>
          </div>
        </div>
        <div class="sb-head-side"><div class="sb-note hide"></div><div class="sb-badge">[[t:dashboard.progress.idle]]</div></div>
      </div>
      <div class="sb-rail"><div class="sb-fill"></div><div class="sb-fly hide"></div></div>
      <div class="sb-steps"><span data-step="start">[[t:dashboard.progress.phase.start]]</span><span data-step="discovering">[[t:dashboard.progress.phase.discovering]]</span><span data-step="syncing">[[t:dashboard.progress.phase.syncing]]</span><span data-step="done">[[t:dashboard.progress.phase.done]]</span></div>
    </div><div id="ux-lanes"></div><div id="ux-spotlight"></div>

    <div class="action-row">
      <div class="action-buttons">
        <div class="cw-split-run" id="cw-sync-split">
        <button id="run" class="btn acc cw-split-main" onclick="runSync()"><span class="material-symbols-rounded cw-action-icon cw-sync-action-icon" aria-hidden="true">sync</span><span class="label">[[t:dashboard.sync.run]]</span></button>
        <button id="run-menu" class="btn acc cw-split-edge" type="button" title="[[t:dashboard.sync.options]]" aria-label="[[t:dashboard.sync.options]]" aria-haspopup="menu" aria-expanded="false" onclick="window.cwToggleSyncMenu(event)"><span class="material-symbols-rounded" aria-hidden="true">expand_more</span></button>
        <div class="cw-menu cw-sync-menu hidden" id="cw-sync-menu" role="menu" aria-labelledby="run-menu"></div>
      </div>
        <button id="btn-details" class="btn cw-hub-action" onclick="toggleDetails()" title="[[t:dashboard.details.showTitle]]" aria-label="[[t:dashboard.details.showLabel]]"><span class="material-symbols-rounded cw-action-icon" aria-hidden="true">description</span><span>[[t:dashboard.details.view]]</span></button>
        <button class="btn cw-hub-action cw-hub-tools-start" onclick="openAnalyzer()" title="[[t:dashboard.tools.analyzerTitle]]" aria-label="[[t:dashboard.tools.analyzerLabel]]"><span class="material-symbols-rounded cw-action-icon" aria-hidden="true">monitoring</span><span>[[t:navigation.pages.analyzer]]</span></button>
        <button class="btn cw-hub-action" onclick="openEvents()" title="[[t:dashboard.tools.eventsTitle]]" aria-label="[[t:dashboard.tools.eventsTitle]]"><span class="material-symbols-rounded cw-action-icon" aria-hidden="true">history</span><span>[[t:navigation.pages.events]]</span></button>
        <button class="btn cw-hub-action" onclick="openExporter()" title="[[t:dashboard.tools.importExportTitle]]" aria-label="[[t:dashboard.tools.importExportTitle]]"><span class="material-symbols-rounded cw-action-icon" aria-hidden="true">import_export</span><span>[[t:navigation.pages.importExport]]</span></button>
      </div>
      <div class="cw-status-dock"></div>
    </div>

    <div id="details" class="details hidden">
      <div class="details-grid">
        <div class="det-left">
          <div class="det-head">
            <div class="det-tabs" role="tablist" aria-label="[[t:dashboard.details.outputTabs]]">
              <button id="det-tab-sync" class="det-tab active" type="button"
                role="tab" aria-selected="true" aria-controls="det-panel-sync" data-tab="sync">[[t:dashboard.details.tabs.sync]]</button>
              <button id="det-tab-watcher" class="det-tab" type="button"
                role="tab" aria-selected="false" aria-controls="det-panel-watcher" data-tab="watcher">[[t:dashboard.details.tabs.watcher]]</button>
              <button id="det-tab-debug" class="det-tab" type="button"
                role="tab" aria-selected="false" aria-controls="det-panel-debug" data-tab="debug" hidden disabled aria-hidden="true" aria-disabled="true">[[t:dashboard.details.tabs.debug]]</button>
            </div>
            <div class="det-tools">
              <button id="det-ai" class="ghost det-ai" type="button" title="[[t:dashboard.details.askAssistant]]" aria-label="[[t:dashboard.details.analyzeWithAi]]" data-ai-analyze><span class="material-symbols-rounded" aria-hidden="true">auto_awesome</span><span>[[t:dashboard.details.analyze]]</span></button>
              <button id="det-open-logs" class="ghost det-tool-icon" type="button" title="[[t:dashboard.details.openLogs]]" aria-label="[[t:dashboard.details.openLogs]]" onclick="openLogs({channel: window._detailsTab || 'sync'})"><span class="material-symbols-rounded" aria-hidden="true">open_in_full</span></button>
              <button id="det-copy" class="ghost det-tool-icon" type="button" title="[[t:dashboard.details.copyOutput]]" aria-label="[[t:dashboard.details.copyOutput]]"><span class="material-symbols-rounded" aria-hidden="true">content_copy</span></button>
              <button id="det-clear" class="ghost det-tool-icon" type="button" title="[[t:dashboard.details.clearOutput]]" aria-label="[[t:dashboard.details.clearOutput]]"><span class="material-symbols-rounded" aria-hidden="true">delete</span></button>
              <button id="det-follow" class="ghost det-follow" type="button" title="[[t:dashboard.details.toggleFollow]]" aria-pressed="true"><span class="material-symbols-rounded det-follow-icon" aria-hidden="true">podcasts</span><span>[[t:dashboard.details.follow]]</span><span class="material-symbols-rounded det-follow-caret" aria-hidden="true">expand_more</span></button>
              <button id="det-more" class="ghost det-tool-icon" type="button" aria-label="[[t:dashboard.details.logActions]]" aria-haspopup="menu"><span class="material-symbols-rounded" aria-hidden="true">more_horiz</span></button>
            </div>
          </div>
          <div class="det-panels">
            <div id="det-panel-sync" class="det-panel" role="tabpanel" aria-labelledby="det-tab-sync">
              <div id="det-log" class="log"></div>
            </div>
            <div id="det-panel-watcher" class="det-panel hidden" role="tabpanel" aria-labelledby="det-tab-watcher">
              <div id="det-watch-log" class="log wlog"></div>
            </div>
            <div id="det-panel-debug" class="det-panel hidden" role="tabpanel" aria-labelledby="det-tab-debug" hidden aria-hidden="true">
              <div id="det-debug-log" class="log wlog"></div>
            </div>
          </div>
          <div class="det-console-footer" aria-live="polite">
            <span id="det-live-state" class="det-live-state"><span class="det-live-dot" aria-hidden="true"></span><span class="det-live-label">[[t:dashboard.details.live]]</span></span>
            <span id="det-follow-state" class="det-follow-state">[[t:dashboard.details.autoScrollOn]]</span>
            <span class="det-footer-spacer"></span>
            <span id="det-line-count" class="det-line-count">[[t:dashboard.details.noLines]]</span>
            <span class="material-symbols-rounded det-list-icon" aria-hidden="true">format_list_bulleted</span>
          </div>

        </div>
        <div class="det-right">
          <div class="meta-card">
            <div class="meta-grid">
              <div class="meta-label">[[t:dashboard.details.summary.module]]</div><div class="meta-value"><span id="det-cmd" class="pillvalue truncate">-</span></div>
              <div class="meta-label">[[t:dashboard.details.summary.version]]</div><div class="meta-value"><span id="det-ver" class="pillvalue">-</span></div>
              <div class="meta-label">[[t:dashboard.details.summary.started]]</div><div class="meta-value"><span id="det-start" class="pillvalue mono">-</span></div>
              <div class="meta-label">[[t:dashboard.details.summary.finished]]</div><div class="meta-value"><span id="det-finish" class="pillvalue mono">-</span></div>
            </div>
            <div class="meta-actions"><button class="btn" onclick="copySummary(this)">[[t:dashboard.details.copySummary]]</button></div>
          </div>
        </div>
      </div>
    </div>
  </section>

  <section id="stats-card" class="card cw-main-card cw-main-card--stats collapsed">
    <div class="title">[[t:dashboard.stats.title]]</div>
    <div class="cw-main-card-head cw-main-card-head--compact">
      <div class="cw-main-card-head-copy">
        <div class="cw-main-card-kicker">[[t:dashboard.stats.title]]</div>
      </div>
    </div>

    <div class="stats-modern v2">
      <div class="now"><div class="label">[[t:dashboard.stats.now]]</div><div id="stat-now" class="value" data-v="0">0</div><div class="chips"><span id="trend-week" class="stat-trend flat"></span></div></div>
      <div class="facts">
        <div class="fact"><span class="k">[[t:dashboard.stats.lastWeek]]</span><span id="stat-week" class="v" data-v="0">0</span></div>
        <div class="fact"><span class="k">[[t:dashboard.stats.lastMonth]]</span><span id="stat-month" class="v" data-v="0">0</span></div>
        <div class="mini-legend"><span class="dot add"></span><span class="l">[[t:dashboard.stats.added]]</span><span id="stat-added" class="n">0</span><span class="dot del"></span><span class="l">[[t:dashboard.stats.removed]]</span><span id="stat-removed" class="n">0</span></div>
        <div class="stat-meter" aria-hidden="true"><span id="stat-fill"></span></div>
      </div>
    </div>

    <div class="stat-tiles" id="stat-providers"></div>

    <div class="stat-block cw-recent-feed" id="recent-activity-block">
      <div class="stat-block-header"><span class="pill plain">[[t:dashboard.widgets.recentScrobble]]</span><button id="activity-view-all" class="cw-page-feed-action refresh-insights" type="button">[[t:common.actions.viewAll]]</button></div>
      <div id="recent-activity" class="history-list"></div>
    </div>

    <div class="stat-block cw-recent-feed">
      <div class="stat-block-header"><span class="pill plain">[[t:dashboard.stats.recentSyncs]]</span><button class="cw-page-feed-action refresh-insights" type="button" onclick="refreshInsights()" title="[[t:common.actions.refresh]]" aria-label="[[t:dashboard.stats.refreshRecentSyncs]]"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button></div>
      <div id="sync-history" class="history-list"></div>
    </div>
  </section>

  <section id="dashboard-widgets-card" class="cw-dashboard-widgets hidden" aria-label="[[t:dashboard.widgets.label]]">
    <article id="placeholder-card" class="card cw-main-card cw-main-card--wall cw-dash-widget cw-dash-widget--watchlist cw-dash-widget--wide hidden">
      <div class="title">[[t:common.media.watchlist]]</div>
      <div class="cw-main-card-head cw-main-card-head--compact">
        <div class="cw-main-card-head-copy">
          <div class="cw-dash-title-row">
            <span class="material-symbols-rounded" aria-hidden="true">movie</span>
            <h3>[[t:common.media.watchlist]]</h3>
          </div>
        </div>
        <span id="watchlist-count-chip" class="cw-widget-count-chip hidden" aria-live="polite"></span>
        <button class="cw-watchlist-see-all" type="button" onclick="location.href='/hub#watchlist'" title="[[t:common.actions.viewAll]]" aria-label="[[t:dashboard.widgets.openWatchlist]]"><span class="material-symbols-rounded" aria-hidden="true">arrow_forward</span></button>
      </div>
      <div id="wall-msg" class="wall-msg">[[t:common.status.loading]]</div>
      <div class="wall-wrap">
        <div id="edgeL" class="edge left"></div><div id="edgeR" class="edge right"></div>
        <div id="poster-row" class="row-scroll" aria-label="[[t:common.media.watchlist]]"></div>
        <button class="nav prev" type="button" onclick="scrollWall(-1)" aria-label="[[t:common.actions.scrollLeft]]"><span class="material-symbols-rounded" aria-hidden="true">chevron_left</span></button>
        <button class="nav next" type="button" onclick="scrollWall(1)" aria-label="[[t:common.actions.scrollRight]]"><span class="material-symbols-rounded" aria-hidden="true">chevron_right</span></button>
      </div>
    </article>

    <article id="recent-history-widget" class="cw-dash-widget cw-dash-widget--history">
      <div class="cw-dash-widget-head">
        <div class="cw-dash-title-row">
          <span class="material-symbols-rounded" aria-hidden="true">play_arrow</span>
          <h3>[[t:dashboard.widgets.recentHistory]]</h3>
        </div>
        <div class="cw-dash-head-actions">
          <span id="recent-history-count-chip" class="cw-widget-count-chip hidden" aria-live="polite"></span>
          <button id="recent-history-refresh" class="cw-dash-ghost" type="button" title="[[t:dashboard.widgets.refreshRecentHistory]]" aria-label="[[t:dashboard.widgets.refreshRecentHistory]]"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button>
        </div>
      </div>
      <div id="recent-history-list" class="cw-history-widget-list cw-widget-scrollbar" aria-live="polite"></div>
    </article>

    <article id="latest-ratings-widget" class="cw-dash-widget cw-dash-widget--ratings">
      <div class="cw-dash-widget-head">
        <div class="cw-dash-title-row">
          <span class="material-symbols-rounded" aria-hidden="true">star</span>
          <h3>[[t:dashboard.widgets.latestRatings]]</h3>
        </div>
        <div class="cw-dash-head-actions">
          <span id="latest-ratings-count-chip" class="cw-widget-count-chip hidden" aria-live="polite"></span>
          <button id="latest-ratings-refresh" class="cw-dash-ghost" type="button" title="[[t:dashboard.widgets.refreshLatestRatings]]" aria-label="[[t:dashboard.widgets.refreshLatestRatings]]"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button>
        </div>
      </div>
      <div id="latest-ratings-grid" class="cw-ratings-widget-grid" aria-live="polite"></div>
    </article>

    <article id="recent-scrobble-widget" class="cw-dash-widget cw-dash-widget--scrobble">
      <div class="cw-dash-widget-head">
        <div class="cw-dash-title-row">
          <span class="material-symbols-rounded" aria-hidden="true">sensors</span>
          <h3>[[t:dashboard.widgets.recentScrobble]]</h3>
        </div>
        <div class="cw-dash-head-actions">
          <span id="recent-scrobble-count-chip" class="cw-widget-count-chip hidden" aria-live="polite"></span>
          <button id="recent-scrobble-refresh" class="cw-dash-ghost" type="button" title="[[t:dashboard.widgets.refreshRecentScrobble]]" aria-label="[[t:dashboard.widgets.refreshRecentScrobble]]"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button>
        </div>
      </div>
      <div id="recent-scrobble-list" class="cw-history-widget-list cw-widget-scrollbar" aria-live="polite"></div>
    </article>

    <article id="recent-progress-widget" class="cw-dash-widget cw-dash-widget--progress">
      <div class="cw-dash-widget-head">
        <div class="cw-dash-title-row">
          <span class="material-symbols-rounded" aria-hidden="true">timelapse</span>
          <h3>[[t:dashboard.widgets.recentProgress]]</h3>
        </div>
        <div class="cw-dash-head-actions">
          <span id="recent-progress-count-chip" class="cw-widget-count-chip hidden" aria-live="polite"></span>
          <button id="recent-progress-refresh" class="cw-dash-ghost" type="button" title="[[t:dashboard.widgets.refreshRecentProgress]]" aria-label="[[t:dashboard.widgets.refreshRecentProgress]]"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button>
        </div>
      </div>
      <div id="recent-progress-list" class="cw-history-widget-list cw-widget-scrollbar" aria-live="polite"></div>
    </article>

    <article id="recent-playlists-widget" class="cw-dash-widget cw-dash-widget--playlists">
      <div class="cw-dash-widget-head">
        <div class="cw-dash-title-row">
          <span class="material-symbols-rounded" aria-hidden="true">queue_music</span>
          <h3>[[t:dashboard.widgets.recentPlaylists]]</h3>
        </div>
        <div class="cw-dash-head-actions">
          <span id="recent-playlists-count-chip" class="cw-widget-count-chip hidden" aria-live="polite"></span>
          <button id="recent-playlists-refresh" class="cw-dash-ghost" type="button" title="[[t:dashboard.widgets.refreshRecentPlaylists]]" aria-label="[[t:dashboard.widgets.refreshRecentPlaylists]]"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button>
        </div>
      </div>
      <div id="recent-playlists-list" class="cw-history-widget-list cw-widget-scrollbar" aria-live="polite"></div>
    </article>
  </section>

  <section id="page-snapshots" class="card hidden tab-page cw-themed-page"></section>

  <section id="page-capture_compare" class="card hidden tab-page"></section>

  <section id="page-playlists" class="card hidden tab-page cw-themed-page"></section>

  <section id="page-tracker" class="card hidden tab-page cw-themed-page"></section>

  <section id="page-editor" class="card hidden tab-page cw-themed-page"></section>

  <section id="page-analyzer" class="card hidden tab-page"></section>

  <section id="page-maintenance" class="card hidden tab-page"></section>

  <section id="page-backups" class="hidden tab-page cw-page-backups" aria-labelledby="br-title"></section>

  <section id="page-events" class="card hidden tab-page"></section>

  <section id="page-logs" class="card hidden tab-page"></section>

  <section id="page-import_export" class="card hidden tab-page"></section>

  <section id="page-interactive_sync" class="card hidden tab-page"></section>

  <section id="page-settings" class="card hidden">
    <div id="cw-settings-shell">
      <aside id="cw-settings-nav" aria-label="[[t:settings.nav.label]]">
        <div class="cw-settings-nav-card">
          <div class="cw-settings-nav-title">[[t:navigation.tabs.settings]]</div>
          <div class="cw-settings-nav-gear" aria-hidden="true"><span class="material-symbols-rounded">settings</span></div>
        </div>

        <div class="cw-settings-nav-list" role="tablist" aria-label="[[t:settings.nav.sections]]">
          <button type="button" class="cw-settings-nav-btn active" data-pane="overview" onclick="cwSettingsSelect?.('overview')">
            <span class="material-symbols-rounded">grid_view</span>
            <span><strong>[[t:settings.panes.overview.title]]</strong><small>[[t:settings.panes.overview.sub]]</small></span>
            <span class="cw-settings-nav-chev" aria-hidden="true">chevron_right</span>
          </button>
          <button type="button" class="cw-settings-nav-btn" data-pane="providers" onclick="cwSettingsSelect?.('providers')">
            <span class="material-symbols-rounded">device_hub</span>
            <span><strong>[[t:settings.panes.providers.title]]</strong><small>[[t:settings.panes.providers.sub]]</small></span>
            <span class="cw-settings-nav-chev" aria-hidden="true">chevron_right</span>
          </button>
          <button type="button" class="cw-settings-nav-btn" data-pane="sync" onclick="cwSettingsSelect?.('sync')">
            <span class="material-symbols-rounded">sync_alt</span>
            <span><strong>[[t:settings.panes.sync.title]]</strong><small>[[t:settings.panes.sync.sub]]</small></span>
            <span class="cw-settings-nav-chev" aria-hidden="true">chevron_right</span>
          </button>
          <button type="button" class="cw-settings-nav-btn" data-pane="scrobbler" onclick="cwSettingsSelect?.('scrobbler')">
            <span class="material-symbols-rounded">sensors</span>
            <span><strong>[[t:settings.panes.scrobbler.title]]</strong><small>[[t:settings.panes.scrobbler.sub]]</small></span>
            <span class="cw-settings-nav-chev" aria-hidden="true">chevron_right</span>
          </button>
          <button type="button" class="cw-settings-nav-btn" data-pane="scheduling" onclick="cwSettingsSelect?.('scheduling')">
            <span class="material-symbols-rounded">schedule</span>
            <span><strong>[[t:settings.panes.scheduling.title]]</strong><small>[[t:settings.panes.scheduling.sub]]</small></span>
            <span class="cw-settings-nav-chev" aria-hidden="true">chevron_right</span>
          </button>
          <button type="button" class="cw-settings-nav-btn" data-pane="app" onclick="cwSettingsSelect?.('app')">
            <span class="material-symbols-rounded">security</span>
            <span><strong>[[t:settings.panes.app.title]]</strong><small>[[t:settings.panes.app.sub]]</small></span>
            <span class="cw-settings-nav-chev" aria-hidden="true">chevron_right</span>
          </button>
          <button type="button" class="cw-settings-nav-btn" data-pane="maintenance" onclick="cwSettingsSelect?.('maintenance')">
            <span class="material-symbols-rounded">build</span>
            <span><strong>[[t:settings.panes.maintenance.title]]</strong><small>[[t:settings.panes.maintenance.sub]]</small></span>
            <span class="cw-settings-nav-chev" aria-hidden="true">chevron_right</span>
          </button>
        </div>

        <div class="cw-settings-nav-footer">
          <button type="button" class="cw-settings-help-card" onclick="openHelp?.()">
            <span><strong>[[t:settings.nav.needHelp]]</strong><small>[[t:settings.nav.viewDocs]]</small></span>
            <span class="material-symbols-rounded" aria-hidden="true">open_in_new</span>
          </button>
        </div>

      </aside>

      <div id="cw-settings-left">
        <section id="cw-settings-overview" class="cw-settings-pane active" data-pane="overview">
          <div class="cw-settings-pane-head cw-settings-hero cw-settings-hero-overview">
            <div>
              <div class="cw-settings-pane-kicker">[[t:settings.panes.overview.title]]</div>
              <h3>[[t:settings.panes.overview.sub]]</h3>
              <p>[[t:settings.overview.intro]]</p>
            </div>
            <span class="material-symbols-rounded cw-settings-hero-shape" aria-hidden="true">grid_view</span>
          </div>
          <div id="cw-settings-overview-grid">
            <div class="cw-settings-overview-main">
              <section class="cw-settings-overview-card cw-settings-progress-card">
                <div class="cw-settings-progress-summary">
                  <div class="cw-settings-progress-ring" id="cw-settings-progress-ring">
                    <span id="cw-settings-progress-count">0/4</span>
                  </div>
                  <div class="cw-settings-progress-copy">
                    <strong id="cw-settings-hero-title">[[t:settings.overview.progressTitle]]</strong>
                    <span id="cw-settings-hero-copy">[[t:settings.overview.checking]]</span>
                    <span class="cw-settings-hero-progress-value" id="cw-settings-progress-text">[[t:settings.overview.progressInitial]]</span>
                    <span class="cw-settings-progress-track" aria-hidden="true"><span id="cw-settings-progress-bar"></span></span>
                  </div>
                </div>
                <div class="cw-settings-progress-steps" aria-label="[[t:settings.overview.progressAreas]]">
                  <span class="cw-settings-progress-node" data-step-node="auth"><span class="material-symbols-rounded">check</span><small>[[t:settings.overview.areas.connections]]</small></span>
                  <span class="cw-settings-progress-node" data-step-node="meta"><span class="material-symbols-rounded">check</span><small>[[t:settings.overview.areas.metadata]]</small></span>
                  <span class="cw-settings-progress-node" data-step-node="sync"><span class="material-symbols-rounded">check</span><small>[[t:settings.overview.areas.sync]]</small></span>
                  <span class="cw-settings-progress-node" data-step-node="scheduling"><span class="material-symbols-rounded">check</span><small>[[t:settings.overview.areas.automation]]</small></span>
                </div>
              </section>

              <section class="cw-settings-setup-card-list" aria-label="[[t:settings.overview.setupAreas]]">
                <div class="cw-settings-setup-table">
                  <article class="cw-settings-setup-step cw-settings-setup-row" data-step="auth" role="button" tabindex="0" onclick="cwSettingsOverviewGo?.('auth')" onkeydown="cwSettingsStepKey?.(event,'auth')">
                    <span class="cw-settings-setup-area">
                      <span class="cw-settings-management-icon material-symbols-rounded" aria-hidden="true">device_hub</span>
                      <span>
                        <strong>[[t:settings.overview.areas.connections]]</strong>
                        <span class="cw-settings-step-copy" id="cw-settings-step-auth-copy">[[t:settings.overview.steps.auth.copy]]</span>
                      </span>
                    </span>
                    <span class="cw-settings-step-state" id="cw-settings-step-auth-state">[[t:settings.overview.state.needsSetup]]</span>
                    <span class="cw-settings-step-detail" id="cw-settings-step-auth-detail">[[t:settings.overview.steps.auth.none]]</span>
                    <button type="button" class="cw-settings-step-link" id="cw-settings-step-auth-link" onclick="event.stopPropagation(); cwSettingsOverviewGo?.('auth')">[[t:common.actions.manage]]</button>
                  </article>
                  <article class="cw-settings-setup-step cw-settings-setup-row" data-step="meta" role="button" tabindex="0" onclick="cwSettingsOverviewGo?.('meta')" onkeydown="cwSettingsStepKey?.(event,'meta')">
                    <span class="cw-settings-setup-area">
                      <span class="cw-settings-management-icon material-symbols-rounded" aria-hidden="true">database</span>
                      <span>
                        <strong>[[t:settings.overview.areas.metadata]]</strong>
                        <span class="cw-settings-step-copy" id="cw-settings-step-meta-copy">[[t:settings.overview.steps.meta.copy]]</span>
                      </span>
                    </span>
                    <span class="cw-settings-step-state" id="cw-settings-step-meta-state">[[t:settings.overview.state.missing]]</span>
                    <span class="cw-settings-step-detail" id="cw-settings-step-meta-detail">[[t:settings.overview.steps.meta.none]]</span>
                    <button type="button" class="cw-settings-step-link" id="cw-settings-step-meta-link" onclick="event.stopPropagation(); cwSettingsOverviewGo?.('meta')">[[t:common.actions.manage]]</button>
                  </article>
                  <article class="cw-settings-setup-step cw-settings-setup-row" data-step="sync" role="button" tabindex="0" onclick="cwSettingsOverviewGo?.('sync')" onkeydown="cwSettingsStepKey?.(event,'sync')">
                    <span class="cw-settings-setup-area">
                      <span class="cw-settings-management-icon material-symbols-rounded" aria-hidden="true">sync_alt</span>
                      <span>
                        <strong>[[t:settings.overview.areas.sync]]</strong>
                        <span class="cw-settings-step-copy" id="cw-settings-step-sync-copy">[[t:settings.overview.steps.sync.copy]]</span>
                      </span>
                    </span>
                    <span class="cw-settings-step-state" id="cw-settings-step-sync-state">[[t:settings.overview.state.optional]]</span>
                    <span class="cw-settings-step-detail" id="cw-settings-step-sync-detail">[[t:settings.overview.steps.sync.none]]</span>
                    <button type="button" class="cw-settings-step-link" id="cw-settings-step-sync-link" onclick="event.stopPropagation(); cwSettingsOverviewGo?.('sync')">[[t:common.actions.manage]]</button>
                  </article>
                  <article class="cw-settings-setup-step cw-settings-setup-row" data-step="scheduling" role="button" tabindex="0" onclick="cwSettingsOverviewGo?.('scheduling')" onkeydown="cwSettingsStepKey?.(event,'scheduling')">
                    <span class="cw-settings-setup-area">
                      <span class="cw-settings-management-icon material-symbols-rounded" aria-hidden="true">schedule</span>
                      <span>
                        <strong>[[t:settings.overview.areas.automation]]</strong>
                        <span class="cw-settings-step-copy" id="cw-settings-step-scheduling-copy">[[t:settings.overview.steps.scheduling.copy]]</span>
                      </span>
                    </span>
                    <span class="cw-settings-step-state" id="cw-settings-step-scheduling-state">[[t:settings.overview.state.optional]]</span>
                    <span class="cw-settings-step-detail" id="cw-settings-step-scheduling-detail">[[t:settings.overview.steps.scheduling.none]]</span>
                    <button type="button" class="cw-settings-step-link" id="cw-settings-step-scheduling-link" onclick="event.stopPropagation(); cwSettingsOverviewGo?.('scheduling')">[[t:common.actions.manage]]</button>
                  </article>
                </div>
              </section>

              <div class="hidden" aria-hidden="true">
                <span id="cw-settings-primary-cta"></span>
                <span id="cw-settings-scrobbler-cta"></span>
                <span id="cw-settings-stat-auth"></span>
                <span id="cw-settings-stat-auth-copy"></span>
                <span id="cw-settings-stat-pairs"></span>
                <span id="cw-settings-stat-pairs-copy"></span>
                <span id="cw-settings-stat-automation"></span>
                <span id="cw-settings-stat-automation-copy"></span>
              </div>

            </div>
            <aside id="cw-settings-insight" aria-label="[[t:settings.overview.insight]]"></aside>
          </div>
        </section>

        <section class="cw-settings-pane" data-pane="providers">
          <div class="cw-settings-pane-head cw-settings-hero cw-settings-hero-connections">
            <div>
              <div class="cw-settings-pane-kicker">[[t:settings.panes.providers.title]]</div>
              <h3>[[t:settings.panes.providers.sub]]</h3>
              <p>[[t:settings.providers.intro]]</p>
            </div>
            <div class="cw-settings-pane-head-actions">
              <div class="cw-conn-hero-summary" aria-label="[[t:settings.providers.actions]]">
                <button type="button" class="cw-conn-seg" onclick="window.cwUserProfilesNew?.()"><span class="material-symbols-rounded" aria-hidden="true">add</span><span>[[t:settings.providers.addProfile]]</span></button>
                <button type="button" class="cw-conn-seg" onclick="window.openAddMetadata?.()"><span class="material-symbols-rounded" aria-hidden="true">add</span><span>[[t:settings.providers.addMetadata]]</span></button>
                <button type="button" class="cw-conn-seg" onclick="window.openAddConnection?.()"><span class="material-symbols-rounded" aria-hidden="true">add</span><span>[[t:settings.providers.addProvider]]</span></button>
                <button type="button" id="cw-conn-refresh" class="cw-conn-refresh" title="[[t:settings.providers.recheck]]" aria-label="[[t:settings.providers.recheck]]"><span class="material-symbol" aria-hidden="true">refresh</span></button>
              </div>
            </div>
            <span class="material-symbols-rounded cw-settings-hero-shape" aria-hidden="true">hub</span>
          </div>
          <div class="cw-settings-pane-stack cw-settings-providers-stack">
            <div class="section open cw-settings-section cw-settings-provider-section" id="sec-auth" data-accordion="off">
              <div class="body"><div id="auth-providers"></div></div>
            </div>
            <div id="metadata-providers" class="hidden" aria-hidden="true">
              <div id="meta-provider-panel" class="cw-meta-provider-stack"></div>
              <div id="meta-provider-raw" class="hidden"></div>
            </div>
          </div>
        </section>

        <section class="cw-settings-pane" data-pane="sync">
          <div class="cw-settings-pane-head cw-settings-hero cw-settings-hero-sync">
            <div>
              <div class="cw-settings-pane-kicker">[[t:settings.panes.sync.title]]</div>
              <h3>[[t:settings.sync.title]]</h3>
              <p>[[t:settings.sync.intro]]</p>
            </div>
            <div class="cw-settings-pane-head-actions">
              <div class="cw-conn-hero-summary" aria-label="[[t:settings.sync.summary]]">
                <div class="cw-conn-seg is-stat"><strong id="cw-sync-stat-providers">0</strong><span>[[t:settings.sync.providers]]</span></div>
                <div class="cw-conn-seg is-stat"><strong id="cw-sync-stat-active">0</strong><span>[[t:settings.sync.activePairs]]</span></div>
                <div class="cw-conn-seg is-stat"><strong id="cw-sync-stat-disabled">0</strong><span>[[t:settings.sync.disabledPairs]]</span></div>
                <button type="button" id="cw-sync-refresh" class="cw-conn-refresh" title="[[t:settings.sync.refresh]]" aria-label="[[t:settings.sync.refresh]]"><span class="material-symbol" aria-hidden="true">refresh</span></button>
              </div>
            </div>
            <span class="material-symbols-rounded cw-settings-hero-shape" aria-hidden="true">sync_alt</span>
          </div>
          <div class="cw-settings-pane-stack cw-settings-sync-stack">
            <div class="section open cw-settings-section" id="sec-sync" data-accordion="off">
              <div class="body">
                <div id="providers_list" class="grid2"></div>
                <div class="sep"></div><h4 class="cw-sync-subhead">[[t:settings.sync.pairs]]</h4>
                <section id="sync-topology-health" class="topology-summary" aria-label="[[t:settings.sync.topologyHealth]]"></section>
                <div id="pairs_list"></div>
                <div class="footer"><div class="pair-selectors" style="margin-top:1em;">
                  <label for="source-provider" style="margin-right:1em;">[[t:settings.sync.source]]</label><select id="source-provider" name="source_provider" style="margin-left:.5em;"></select>
                  <label for="target-provider">[[t:settings.sync.target]]</label><select id="target-provider" name="target_provider" style="margin-left:.5em;"></select>
                </div></div>
              </div>
            </div>
          </div>
        </section>

        <section class="cw-settings-pane" data-pane="scheduling">
          <div class="cw-settings-pane-head cw-settings-hero cw-settings-hero-scheduling">
            <div>
              <div class="cw-settings-pane-kicker">[[t:settings.panes.scheduling.title]]</div>
              <h3>[[t:sync.schedule.runAutomation]]</h3>
            </div>
            <div class="cw-settings-pane-head-actions">
              <div class="cw-conn-hero-summary" id="sched-pane-tabs" aria-label="[[t:sync.schedule.sections]]">
                <button type="button" class="cw-conn-seg active" data-sub="basic">[[t:sync.schedule.tabs.standard]]</button>
                <button type="button" class="cw-conn-seg" data-sub="advanced">[[t:sync.schedule.tabs.advanced]]</button>
                <button type="button" class="cw-conn-seg" data-sub="webhooks">[[t:sync.schedule.tabs.webhooks]]</button>
                <button type="button" id="cw-sched-refresh" class="cw-conn-refresh" title="[[t:sync.schedule.refresh]]" aria-label="[[t:sync.schedule.refresh]]"><span class="material-symbol" aria-hidden="true">refresh</span></button>
              </div>
            </div>
            <span class="material-symbols-rounded cw-settings-hero-shape" aria-hidden="true">event_repeat</span>
          </div>
          <div class="section open cw-settings-section cw-scheduling-section" id="sec-scheduling" data-accordion="off">
            <div class="body">
              <div id="sched-provider-panel" class="cw-panel hidden"></div>
              <div id="sched-provider-raw" class="hidden">
                <div class="grid2">
                  <div><label for="schEnabled">[[t:common.actions.enable]]</label><select id="schEnabled" name="schEnabled"><option value="false">[[t:common.status.disabled]]</option><option value="true">[[t:common.status.enabled]]</option></select></div>
                  <div><label for="schMode">[[t:sync.schedule.frequency]]</label><select id="schMode" name="schMode"><option value="hourly">[[t:sync.schedule.mode.hourly]]</option><option value="every_n_hours">[[t:sync.schedule.mode.everyN]]</option><option value="daily_time">[[t:sync.schedule.mode.daily]]</option><option value="custom_interval">[[t:sync.schedule.mode.custom]]</option></select></div>
                  <div><label for="schN">[[t:sync.schedule.mode.everyN]]</label><input id="schN" name="schN" type="number" min="2" value="12"></div>
                  <div><label for="schTime">[[t:sync.schedule.time]]</label><input id="schTime" name="schTime" type="time" value="03:30"></div>
                  <div><label for="schCustomValue">[[t:sync.schedule.customInterval]]</label><input id="schCustomValue" name="schCustomValue" type="number" min="15" step="15" value="60"></div>
                  <div><label for="schCustomUnit">[[t:sync.schedule.customUnit]]</label><select id="schCustomUnit" name="schCustomUnit"><option value="minutes">[[t:common.units.minutes]]</option><option value="hours">[[t:common.units.hours]]</option></select></div>
                </div>
                <div id="sched_advanced_mount"></div>
                <div id="sched_webhooks_mount"></div>
              </div>
            </div>
          </div>
        </section>

        <section class="cw-settings-pane" data-pane="scrobbler">
          <div id="sec-scrobbler" class="cw-settings-pane-stack cw-settings-scrobbler-stack" data-accordion="off">
            <div id="scrobble-mount" class="cw-settings-pane-stack cw-settings-scrobbler-stack-inner">
              <div class="sc2-page">
                <div class="cw-settings-pane-head cw-settings-hero cw-settings-hero-scrobbler sc2-pane-head">
                  <div>
                    <div class="cw-settings-pane-kicker">[[t:settings.panes.scrobbler.title]]</div>
                    <h3>[[t:scrobbler.pane.title]]</h3>
                    <p>[[t:scrobbler.pane.intro]] <b>[[t:scrobbler.pane.recommended]]</b> [[t:scrobbler.pane.bothWarning]]</p>
                  </div>
                  <span class="material-symbols-rounded cw-settings-hero-shape" aria-hidden="true">sensors</span>
                </div>
                <div class="sc2-empty">[[t:scrobbler.pane.loading]]</div>
              </div>
            </div>
          </div>
        </section>

        <section class="cw-settings-pane cw-app-settings-pane" data-pane="app">
          <div class="cw-settings-pane-head cw-app-hero">
            <div class="cw-app-hero-copy">
              <div class="cw-settings-pane-kicker">[[t:settings.panes.app.title]]</div>
              <h3>[[t:settings.app.subtitle]]</h3>
              <p>[[t:settings.app.intro]]</p>
            </div>
            <div class="cw-conn-hero-summary" aria-label="[[t:settings.app.sections]]">
              <button type="button" class="cw-conn-seg active" aria-current="page" data-target="ui" onclick="cwUiSettingsJump?.('ui')">[[t:settings.app.tabs.ui]]</button>
              <button type="button" class="cw-conn-seg" data-target="security" onclick="cwUiSettingsJump?.('security')">[[t:settings.app.tabs.security]]</button>
              <button type="button" id="cw-app-refresh" class="cw-conn-refresh" title="[[t:settings.app.refresh]]" aria-label="[[t:settings.app.refresh]]"><span class="material-symbol" aria-hidden="true">refresh</span></button>
            </div>
            <span class="material-symbols-rounded cw-app-hero-shape active" data-app-hero-shape="ui" aria-hidden="true">settings</span>
            <span class="material-symbols-rounded cw-app-hero-shape" data-app-hero-shape="security" aria-hidden="true">settings</span>
          </div>
          <div class="section open cw-settings-section cw-app-section" id="sec-ui" data-accordion="off">
            <div class="head" style="display:flex;align-items:center">
              <span class="chev"></span>
              <strong>[[t:settings.app.heading]]</strong>
            </div>
            <div class="body">

              <div class="cw-settings-panels" id="ui_settings_panels">

                <!-- Panel: User Interface -->
                <div class="cw-settings-panel cw-settings-shell cw-app-panel active" data-tab="ui">
                  <div class="cw-settings-layout cw-app-ui-layout">
                    <div class="cw-settings-block cw-app-card cw-page-settings-preferences cw-app-card-widgets">
                      <div class="cw-app-card-head">
                        <span class="material-symbols-rounded cw-app-card-icon" aria-hidden="true">dashboard_customize</span>
                        <div>
                          <h4 class="cw-page-settings-title">[[t:settings.ui.widgets.title]]</h4>
                          <div class="sub">[[t:settings.ui.widgets.intro]]</div>
                        </div>
                      </div>
                      <div class="cw-page-settings-columns">
                        <div class="cw-page-settings-column">
                          <div class="cw-page-settings-row">
                            <div class="cw-field-label-row">
                              <label class="cw-page-settings-label" for="ui_show_watchlist_preview">[[t:settings.ui.widgets.watchlist]]</label>
                              <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.ui.widgets.watchlistHelp]]" aria-label="[[t:settings.ui.widgets.watchlistHelpLabel]]">help</button>
                            </div>
                            <span class="cw-page-settings-switch"><input id="ui_show_watchlist_preview" name="ui_show_watchlist_preview" type="checkbox" role="switch" checked><span aria-hidden="true"></span></span>
                          </div>
                          <div class="cw-page-settings-row">
                            <div class="cw-field-label-row">
                              <label class="cw-page-settings-label" for="ui_show_latest_ratings_widget">[[t:settings.ui.widgets.ratings]]</label>
                              <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.ui.widgets.ratingsHelp]]" aria-label="[[t:settings.ui.widgets.ratingsHelpLabel]]">help</button>
                            </div>
                            <span class="cw-page-settings-switch"><input id="ui_show_latest_ratings_widget" name="ui_show_latest_ratings_widget" type="checkbox" role="switch" checked><span aria-hidden="true"></span></span>
                          </div>
                          <div class="cw-page-settings-row">
                            <div class="cw-field-label-row">
                              <label class="cw-page-settings-label" for="ui_show_recent_progress_widget">[[t:settings.ui.widgets.progress]]</label>
                              <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.ui.widgets.progressHelp]]" aria-label="[[t:settings.ui.widgets.progressHelpLabel]]">help</button>
                            </div>
                            <span class="cw-page-settings-switch"><input id="ui_show_recent_progress_widget" name="ui_show_recent_progress_widget" type="checkbox" role="switch"><span aria-hidden="true"></span></span>
                          </div>
                        </div>
                        <div class="cw-page-settings-column">
                          <div class="cw-page-settings-row">
                            <div class="cw-field-label-row">
                              <label class="cw-page-settings-label" for="ui_show_recent_history_widget">[[t:settings.ui.widgets.history]]</label>
                              <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.ui.widgets.historyHelp]]" aria-label="[[t:settings.ui.widgets.historyHelpLabel]]">help</button>
                            </div>
                            <span class="cw-page-settings-switch"><input id="ui_show_recent_history_widget" name="ui_show_recent_history_widget" type="checkbox" role="switch" checked><span aria-hidden="true"></span></span>
                          </div>
                          <div class="cw-page-settings-row">
                            <div class="cw-field-label-row">
                              <label class="cw-page-settings-label" for="ui_show_recent_scrobble_widget">[[t:settings.ui.widgets.scrobble]]</label>
                              <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.ui.widgets.scrobbleHelp]]" aria-label="[[t:settings.ui.widgets.scrobbleHelpLabel]]">help</button>
                            </div>
                            <span class="cw-page-settings-switch"><input id="ui_show_recent_scrobble_widget" name="ui_show_recent_scrobble_widget" type="checkbox" role="switch" checked><span aria-hidden="true"></span></span>
                          </div>
                          <div class="cw-page-settings-row">
                            <div class="cw-field-label-row">
                              <label class="cw-page-settings-label" for="ui_show_recent_playlists_widget">[[t:settings.ui.widgets.playlists]]</label>
                              <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.ui.widgets.playlistsHelp]]" aria-label="[[t:settings.ui.widgets.playlistsHelpLabel]]">help</button>
                            </div>
                            <span class="cw-page-settings-switch"><input id="ui_show_recent_playlists_widget" name="ui_show_recent_playlists_widget" type="checkbox" role="switch"><span aria-hidden="true"></span></span>
                          </div>
                        </div>
                      </div>
                    </div>

                    <div class="cw-settings-block cw-app-card cw-page-settings-preferences cw-app-card-visibility">
                      <div class="cw-app-card-head">
                        <span class="material-symbols-rounded cw-app-card-icon" aria-hidden="true">visibility</span>
                        <div>
                          <h4 class="cw-page-settings-title">[[t:settings.ui.visibility.title]]</h4>
                          <div class="sub">[[t:settings.ui.visibility.intro]]</div>
                        </div>
                      </div>
                      <div class="cw-page-settings-columns">
                        <div class="cw-page-settings-column">
                          <div class="cw-page-settings-row">
                            <div class="cw-field-label-row">
                              <label class="cw-page-settings-label" for="ui_show_playingcard">[[t:settings.ui.visibility.playingCard]]</label>
                              <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.ui.visibility.playingCardHelp]]" aria-label="[[t:settings.ui.visibility.playingCardHelpLabel]]">help</button>
                            </div>
                            <span class="cw-page-settings-switch"><input id="ui_show_playingcard" name="ui_show_playingcard" type="checkbox" role="switch" checked><span aria-hidden="true"></span></span>
                          </div>
                          <div class="cw-page-settings-row">
                            <div class="cw-field-label-row">
                              <label class="cw-page-settings-label" for="ui_show_recent_activity">[[t:settings.ui.visibility.scrobbleList]]</label>
                              <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.ui.visibility.scrobbleListHelp]]" aria-label="[[t:settings.ui.visibility.scrobbleListHelpLabel]]">help</button>
                            </div>
                            <span class="cw-page-settings-switch"><input id="ui_show_recent_activity" name="ui_show_recent_activity" type="checkbox" role="switch" checked><span aria-hidden="true"></span></span>
                          </div>
                          <div class="cw-page-settings-row">
                            <div class="cw-field-label-row">
                              <label class="cw-page-settings-label" for="ui_show_quick_add_desktop">[[t:settings.ui.visibility.quickAddDesktop]]</label>
                              <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.ui.visibility.quickAddDesktopHelp]]" aria-label="[[t:settings.ui.visibility.quickAddDesktopHelpLabel]]">help</button>
                            </div>
                            <span class="cw-page-settings-switch"><input id="ui_show_quick_add_desktop" name="ui_show_quick_add_desktop" type="checkbox" role="switch" checked><span aria-hidden="true"></span></span>
                          </div>
                          <div class="cw-page-settings-row">
                            <div class="cw-field-label-row">
                              <label class="cw-page-settings-label" for="ui_show_quick_add_mobile">[[t:settings.ui.visibility.quickAddMobile]]</label>
                              <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.ui.visibility.quickAddMobileHelp]]" aria-label="[[t:settings.ui.visibility.quickAddMobileHelpLabel]]">help</button>
                            </div>
                            <span class="cw-page-settings-switch"><input id="ui_show_quick_add_mobile" name="ui_show_quick_add_mobile" type="checkbox" role="switch" checked><span aria-hidden="true"></span></span>
                          </div>
                        </div>
                        <div class="cw-page-settings-column">
                          <div class="cw-page-settings-row">
                            <div class="cw-field-label-row">
                              <label class="cw-page-settings-label" for="ui_recent_activity_display">[[t:settings.ui.visibility.scrobbleDisplay]]</label>
                              <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.ui.visibility.scrobbleDisplayHelp]]" aria-label="[[t:settings.ui.visibility.scrobbleDisplayHelpLabel]]">help</button>
                            </div>
                            <select id="ui_recent_activity_display" name="ui_recent_activity_display">
                              <option value="count:3">[[t:settings.ui.recent.last3]]</option>
                              <option value="count:4">[[t:settings.ui.recent.last4]]</option>
                              <option value="count:5">[[t:settings.ui.recent.last5]]</option>
                              <option value="hours:24">[[t:settings.ui.recent.hours24]]</option>
                              <option value="hours:48">[[t:settings.ui.recent.hours48]]</option>
                              <option value="hours:72">[[t:settings.ui.recent.hours72]]</option>
                            </select>
                          </div>
                          <div class="cw-page-settings-row">
                            <div class="cw-field-label-row">
                              <label class="cw-page-settings-label" for="ui_recent_syncs_display">[[t:settings.ui.visibility.syncsDisplay]]</label>
                              <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.ui.visibility.syncsDisplayHelp]]" aria-label="[[t:settings.ui.visibility.syncsDisplayHelpLabel]]">help</button>
                            </div>
                            <select id="ui_recent_syncs_display" name="ui_recent_syncs_display">
                              <option value="count:3">[[t:settings.ui.recent.last3]]</option>
                              <option value="count:4">[[t:settings.ui.recent.last4]]</option>
                              <option value="count:5">[[t:settings.ui.recent.last5]]</option>
                              <option value="hours:24">[[t:settings.ui.recent.hours24]]</option>
                              <option value="hours:48">[[t:settings.ui.recent.hours48]]</option>
                              <option value="hours:72">[[t:settings.ui.recent.hours72]]</option>
                            </select>
                          </div>
                        </div>
                      </div>
                    </div>

                    <div class="cw-settings-block cw-app-card cw-page-settings-preferences cw-app-card-theme">
                      <div class="cw-app-card-head">
                        <span class="material-symbols-rounded cw-app-card-icon" aria-hidden="true">palette</span>
                        <div>
                          <h4 class="cw-page-settings-title">[[t:settings.ui.appearance.title]]</h4>
                          <div class="sub">[[t:settings.ui.appearance.intro]]</div>
                        </div>
                      </div>
                      <div class="cw-settings-2col">
                        <div>
                          <div class="cw-field-label-row">
                            <label class="cw-page-settings-label" for="ui_theme">[[t:settings.ui.appearance.theme]]</label>
                            <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.ui.appearance.themeHelp]]" aria-label="[[t:settings.ui.appearance.themeHelpLabel]]">help</button>
                          </div>
                          <select id="ui_theme" name="ui_theme">
                            <option value="flat-dark">[[t:settings.ui.appearance.flatDark]]</option>
                            <option value="flat-light">[[t:settings.ui.appearance.flatLight]]</option>
                            <option value="original">[[t:settings.ui.appearance.original]]</option>
                          </select>
                        </div>
                        <div>
                          <div class="cw-field-label-row"><label class="cw-page-settings-label" for="ui_media_card">[[t:settings.ui.appearance.mediaDetails]]</label></div>
                          <select id="ui_media_card" data-cw-media-card>
                            <option value="full">[[t:settings.ui.appearance.mediaCardFull]]</option>
                            <option value="compact">[[t:settings.ui.appearance.mediaCardCompact]]</option>
                          </select>
                          <div class="sub">[[t:settings.ui.appearance.mediaDetailsHint]]</div>
                        </div>
                        <div>
                          <div class="cw-field-label-row"><label class="cw-page-settings-label" for="ui_language">[[t:common.language.label]]</label></div>
                          <select id="ui_language" data-cw-language-select aria-describedby="ui_language_hint">__CW_LANGUAGE_OPTIONS__</select>
                          <div class="sub" id="ui_language_hint" data-cw-language-status>__CW_LANGUAGE_HINT__</div>
                        </div>
                        <div>
                          <div class="cw-field-label-row">
                            <label class="cw-page-settings-label" for="ui_protocol">[[t:settings.ui.appearance.protocol]]</label>
                            <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.ui.appearance.protocolHelp]]" aria-label="[[t:settings.ui.appearance.protocolHelpLabel]]">help</button>
                          </div>
                          <div class="cw-settings-inline-action">
                            <select id="ui_protocol" name="ui_protocol">
                              <option value="http">HTTP</option>
                              <option value="https">[[t:settings.ui.appearance.httpsSelfSigned]]</option>
                            </select>
                            <button type="button" class="btn primary" id="ui_tls_advanced" onclick="openTlsCertModal?.()">[[t:common.labels.advanced]]</button>
                          </div>
                          <div class="sub" style="margin-top:0.35rem">
                            [[t:settings.ui.appearance.httpsWarning]]
                          </div>
                        </div>
                      </div>
                    </div>

                  </div>
                </div>

                <!-- Panel: Security -->
                <div class="cw-settings-panel cw-settings-shell cw-app-panel" data-tab="security">
                  <div class="cw-settings-layout cw-app-security-layout">
                    <form class="cw-page-security-column cw-password-form-scope" id="app_auth_fields" autocomplete="off" onsubmit="return false">
                      <section class="cw-settings-block cw-app-card cw-page-security-card">
                        <div class="cw-app-card-head">
                          <span class="material-symbols-rounded cw-app-card-icon" aria-hidden="true">lock</span>
                          <div>
                            <h4 class="cw-page-settings-title">[[t:settings.security.account.title]]</h4>
                            <div class="sub">[[t:settings.security.account.intro]]</div>
                          </div>
                        </div>
                        <div class="cw-page-security-row">
                          <span class="material-symbols-rounded cw-page-security-icon" aria-hidden="true">person</span>
                          <div class="cw-auth-username-field">
                            <div class="cw-field-label-row">
                              <label class="cw-page-settings-label" for="app_auth_username">[[t:settings.security.username]]</label>
                              <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.security.usernameHelp]]" aria-label="[[t:settings.security.usernameHelpLabel]]">help</button>
                            </div>
                            <div class="sub">[[t:settings.security.usernameHint]]</div>
                            <input id="app_auth_username" name="app_auth_username" type="text" autocomplete="username" placeholder="admin">
                          </div>
                        </div>
                        <details class="cw-page-security-disclosure" id="app_auth_password_details">
                          <summary>
                            <span class="material-symbols-rounded cw-page-security-icon" aria-hidden="true">key</span>
                            <span class="cw-page-security-copy">
                              <strong>[[t:settings.security.password.title]]</strong>
                              <span class="sub">[[t:settings.security.password.intro]]</span>
                            </span>
                            <span class="cw-page-security-action">[[t:settings.security.password.change]]</span>
                            <span class="material-symbols-rounded cw-page-security-chevron" aria-hidden="true">expand_more</span>
                          </summary>
                          <div class="cw-settings-2col cw-page-security-expanded">
                            <div class="cw-auth-password-field">
                              <div class="cw-field-label-row">
                                <label class="cw-page-settings-label" for="app_auth_password">[[t:settings.security.password.new]]</label>
                                <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.security.password.newHelp]]" aria-label="[[t:settings.security.password.newHelpLabel]]">help</button>
                              </div>
                              <input id="app_auth_password" name="app_auth_password" type="password" autocomplete="new-password" placeholder="[[t:settings.security.password.keepPlaceholder]]">
                              <div class="sub" style="margin-top:0.35rem">[[t:settings.security.password.keepHint]]</div>
                            </div>
                            <div class="cw-auth-confirm-field">
                              <div class="cw-field-label-row">
                                <label class="cw-page-settings-label" for="app_auth_password2">[[t:settings.security.password.confirm]]</label>
                                <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.security.password.confirmHelp]]" aria-label="[[t:settings.security.password.confirmHelpLabel]]">help</button>
                              </div>
                              <input id="app_auth_password2" name="app_auth_password2" type="password" autocomplete="new-password" placeholder="[[t:settings.security.password.repeatPlaceholder]]">
                              <div class="sub" style="margin-top:0.35rem">[[t:settings.security.password.repeatHint]]</div>
                            </div>
                          </div>
                        </details>
                        <div class="cw-page-security-row">
                          <span class="material-symbols-rounded cw-page-security-icon" aria-hidden="true">verified_user</span>
                          <div class="cw-auth-totp-field">
                            <div class="cw-page-security-method">
                              <div class="cw-field-label-row">
                                <strong>[[t:settings.security.twoFactor.title]]</strong>
                                <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.security.twoFactor.help]]" aria-label="[[t:settings.security.twoFactor.helpLabel]]">help</button>
                              </div>
                              <div class="sub" id="app_auth_totp_state">[[t:settings.security.twoFactor.stateOff]]</div>
                              <div class="cw-auth-totp-setup hidden" id="app_auth_totp_setup">
                                <div class="sub">[[t:settings.security.twoFactor.setupHint]]</div>
                                <input id="app_auth_totp_secret" type="text" readonly autocomplete="off" aria-label="[[t:settings.security.twoFactor.secret]]">
                                <input id="app_auth_totp_code" aria-label="[[t:settings.security.twoFactor.code]]" type="text" inputmode="numeric" pattern="[0-9]*" maxlength="6" autocomplete="one-time-code" placeholder="123456">
                              </div>
                              <div class="cw-page-security-actions">
                                <button class="btn primary" type="button" id="btn-app-auth-totp-setup" onclick="cwAppAuthTotpSetup?.()">[[t:settings.security.twoFactor.setup]]</button>
                                <button class="btn primary hidden" type="button" id="btn-app-auth-totp-verify" onclick="cwAppAuthTotpVerify?.()">[[t:settings.security.twoFactor.verify]]</button>
                                <button class="btn danger" type="button" id="btn-app-auth-totp-disable" disabled onclick="cwAppAuthTotpDisable?.()">[[t:settings.security.twoFactor.disable]]</button>
                              </div>
                            </div>
                          </div>
                        </div>
                      </section>
                      <section class="cw-settings-block cw-app-card cw-page-security-card">
                        <div class="cw-app-card-head">
                          <span class="material-symbols-rounded cw-app-card-icon" aria-hidden="true">login</span>
                          <div>
                            <h4 class="cw-page-settings-title">[[t:settings.security.signIn.title]]</h4>
                            <div class="sub">[[t:settings.security.signIn.intro]]</div>
                          </div>
                        </div>
                        <div class="cw-page-security-row">
                          <span class="material-symbols-rounded cw-page-security-icon" aria-hidden="true">play_arrow</span>
                          <div class="cw-auth-plex-field">
                            <div class="cw-page-security-method">
                              <div class="cw-field-label-row">
                                <strong>[[t:settings.security.plex.title]]</strong>
                                <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.security.plex.help]]" aria-label="[[t:settings.security.plex.helpLabel]]">help</button>
                              </div>
                              <div class="sub" id="app_auth_plex_state">[[t:settings.security.plex.notLinked]]</div>
                              <div class="cw-page-security-actions">
                                <button class="btn primary" type="button" id="btn-app-auth-plex-link" onclick="cwAppAuthPlexLink?.()">[[t:settings.security.plex.link]]</button>
                                <button class="btn" type="button" id="btn-app-auth-plex-unlink" disabled onclick="cwAppAuthPlexUnlink?.()">[[t:common.actions.unlink]]</button>
                              </div>
                            </div>
                          </div>
                        </div>
                        <div class="cw-auth-oidc-field">
                          <details class="cw-page-security-disclosure" id="app_auth_oidc_details">
                            <summary>
                              <span class="material-symbols-rounded cw-page-security-icon" aria-hidden="true">tune</span>
                              <span class="cw-page-security-copy">
                                <strong>[[t:settings.security.oidc.title]]</strong>
                                <span class="sub" id="app_auth_oidc_state">[[t:settings.security.oidc.notConfigured]]</span>
                              </span>
                              <span class="cw-page-security-action">[[t:settings.security.oidc.configure]]</span>
                              <span class="material-symbols-rounded cw-page-security-chevron" aria-hidden="true">expand_more</span>
                            </summary>
                            <div class="cw-app-oidc-grid">
                              <label class="cw-page-settings-field" for="app_auth_oidc_enabled">
                                <span>[[t:settings.security.oidc.enabled]]</span>
                                <select id="app_auth_oidc_enabled" aria-label="[[t:settings.security.oidc.enabled]]" title="[[t:settings.security.oidc.enabledHelp]]">
                                  <option value="false">[[t:common.status.disabled]]</option>
                                  <option value="true">[[t:common.status.enabled]]</option>
                                </select>
                              </label>
                              <label class="cw-page-settings-field" for="app_auth_oidc_issuer">
                                <span>[[t:settings.security.oidc.issuer]]</span>
                                <input id="app_auth_oidc_issuer" type="url" autocomplete="off" placeholder="[[t:settings.security.oidc.issuer]]" aria-label="[[t:settings.security.oidc.issuer]]" title="[[t:settings.security.oidc.issuerHelp]]">
                              </label>
                              <label class="cw-page-settings-field" for="app_auth_oidc_client_id">
                                <span>[[t:settings.security.oidc.clientId]]</span>
                                <input id="app_auth_oidc_client_id" type="text" autocomplete="off" placeholder="[[t:settings.security.oidc.clientId]]" aria-label="[[t:settings.security.oidc.clientId]]" title="[[t:settings.security.oidc.clientIdHelp]]">
                              </label>
                              <label class="cw-page-settings-field" for="app_auth_oidc_client_secret">
                                <span>[[t:settings.security.oidc.clientSecret]]</span>
                                <input id="app_auth_oidc_client_secret" type="password" autocomplete="new-password" placeholder="[[t:settings.security.oidc.clientSecret]]" aria-label="[[t:settings.security.oidc.clientSecret]]" title="[[t:settings.security.oidc.clientSecretHelp]]">
                              </label>
                              <label class="cw-page-settings-field" for="app_auth_oidc_scopes">
                                <span>[[t:settings.security.oidc.scopes]]</span>
                                <input id="app_auth_oidc_scopes" type="text" autocomplete="off" placeholder="openid profile email" aria-label="[[t:settings.security.oidc.scopes]]" title="[[t:settings.security.oidc.scopesHelp]]">
                              </label>
                              <label class="cw-page-settings-field" for="app_auth_oidc_button_label">
                                <span>[[t:settings.security.oidc.buttonLabel]]</span>
                                <input id="app_auth_oidc_button_label" type="text" maxlength="40" autocomplete="off" placeholder="[[t:settings.security.oidc.buttonLabelPlaceholder]]" aria-label="[[t:settings.security.oidc.buttonLabel]]" title="[[t:settings.security.oidc.buttonLabelHelp]]">
                              </label>
                              <label class="cw-page-settings-field" for="app_auth_oidc_button_icon">
                                <span>[[t:settings.security.oidc.buttonIcon]]</span>
                                <select id="app_auth_oidc_button_icon" aria-label="[[t:settings.security.oidc.buttonIcon]]" title="[[t:settings.security.oidc.buttonIconHelp]]">
                                  <option value="">[[t:settings.security.oidc.noIcon]]</option>
                                  <option value="authentik">Authentik</option>
                                  <option value="authelia">Authelia</option>
                                  <option value="keycloak">Keycloak</option>
                                  <option value="pocketid">Pocket ID</option>
                                  <option value="google">Google</option>
                                  <option value="microsoft">Microsoft</option>
                                  <option value="okta">Okta</option>
                                  <option value="auth0">Auth0</option>
                                </select>
                              </label>
                            </div>
                            <div class="cw-field-label-row" style="margin-top:0.75rem">
                              <strong>[[t:settings.security.oidc.provision.title]]</strong>
                              <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.security.oidc.provision.help]]" aria-label="[[t:settings.security.oidc.provision.helpLabel]]">help</button>
                            </div>
                            <div class="cw-app-oidc-grid">
                              <label class="cw-page-settings-field" for="app_auth_oidc_provision">
                                <span>[[t:settings.security.oidc.provision.create]]</span>
                                <select id="app_auth_oidc_provision" aria-label="[[t:settings.security.oidc.provision.create]]" title="[[t:settings.security.oidc.provision.createHelp]]">
                                  <option value="false">[[t:settings.security.oidc.provision.off]]</option>
                                  <option value="true">[[t:settings.security.oidc.provision.on]]</option>
                                </select>
                              </label>
                            </div>
                            <div id="app_auth_oidc_provision_fields" hidden>
                              <div class="cw-app-oidc-grid">
                                <label class="cw-page-settings-field" for="app_auth_oidc_provision_access">
                                  <span>[[t:settings.security.oidc.provision.access]]</span>
                                  <select id="app_auth_oidc_provision_access" aria-label="[[t:settings.security.oidc.provision.access]]" title="[[t:settings.security.oidc.provision.accessHelp]]">
                                    <option value="full">[[t:settings.security.oidc.provision.full]]</option>
                                    <option value="readonly">[[t:settings.security.oidc.provision.readOnly]]</option>
                                  </select>
                                </label>
                                <label class="cw-page-settings-field" for="app_auth_oidc_allowed_groups">
                                  <span>[[t:settings.security.oidc.provision.groups]]</span>
                                  <input id="app_auth_oidc_allowed_groups" type="text" autocomplete="off" placeholder="[[t:settings.security.oidc.provision.groupsPlaceholder]]" aria-label="[[t:settings.security.oidc.provision.groups]]" title="[[t:settings.security.oidc.provision.groupsHelp]]">
                                </label>
                                <label class="cw-page-settings-field" for="app_auth_oidc_groups_claim">
                                  <span>[[t:settings.security.oidc.provision.groupsClaim]]</span>
                                  <input id="app_auth_oidc_groups_claim" type="text" autocomplete="off" placeholder="[[t:settings.security.oidc.provision.groupsClaimPlaceholder]]" aria-label="[[t:settings.security.oidc.provision.groupsClaim]]" title="[[t:settings.security.oidc.provision.groupsClaimHelp]]">
                                </label>
                                <label class="cw-page-settings-field" for="app_auth_oidc_match_identities">
                                  <span>[[t:settings.security.oidc.provision.match]]</span>
                                  <select id="app_auth_oidc_match_identities" aria-label="[[t:settings.security.oidc.provision.match]]" title="[[t:settings.security.oidc.provision.matchHelp]]">
                                    <option value="false">[[t:settings.security.oidc.provision.matchOff]]</option>
                                    <option value="true">[[t:settings.security.oidc.provision.matchByName]]</option>
                                  </select>
                                </label>
                                <label class="cw-page-settings-field" for="app_auth_oidc_identity_claim">
                                  <span>[[t:settings.security.oidc.provision.identityClaim]]</span>
                                  <input id="app_auth_oidc_identity_claim" type="text" maxlength="64" autocomplete="off" placeholder="[[t:settings.security.oidc.provision.identityClaimPlaceholder]]" aria-label="[[t:settings.security.oidc.provision.identityClaim]]" title="[[t:settings.security.oidc.provision.identityClaimHelp]]">
                                </label>
                              </div>
                            </div>
                            <div class="cw-app-oidc-actions">
                              <button class="btn primary" type="button" id="btn-app-auth-oidc-link" onclick="cwAppAuthOidcLink?.()" title="[[t:settings.security.oidc.linkHelp]]">[[t:settings.security.oidc.link]]</button>
                              <button class="btn" type="button" id="btn-app-auth-oidc-unlink" onclick="cwAppAuthOidcUnlink?.()" title="[[t:settings.security.oidc.unlinkHelp]]">[[t:common.actions.unlink]]</button>
                            </div>
                          </details>
                        </div>
                      </section>
                    </form>
                    <div class="cw-page-security-column">
                      <div class="cw-settings-block cw-app-card cw-app-user-card cw-page-security-card" id="app_user_manager">
                        <div class="cw-app-card-head">
                          <span class="material-symbols-rounded cw-app-card-icon" aria-hidden="true">group</span>
                          <div>
                            <h4 class="cw-page-settings-title">[[t:settings.security.logins.title]]</h4>
                            <div class="sub">[[t:settings.security.logins.intro]]</div>
                          </div>
                          <button class="btn primary cw-app-user-add" type="button" id="btn-app-user-profile-create">
                            <span class="material-symbols-rounded" aria-hidden="true">person_add</span>
                            <span>[[t:settings.providers.addProfile]]</span>
                          </button>
                        </div>
                        <div class="cw-app-user-list" id="app_users_list">
                        </div>
                      </div>
                      <div class="cw-settings-block cw-app-card cw-page-security-card cw-page-session-card">
                        <div class="cw-app-card-head">
                          <span class="material-symbols-rounded cw-app-card-icon" aria-hidden="true">admin_panel_settings</span>
                          <div>
                            <h4 class="cw-page-settings-title">[[t:settings.security.sessions.title]] &amp; network</h4>
                            <div class="sub">[[t:settings.security.sessions.intro]]</div>
                          </div>
                        </div>
                        <div class="cw-page-session-list">
                          <div class="cw-page-session-row">
                            <div class="cw-page-session-copy">
                              <div class="cw-field-label-row">
                                <strong>[[t:settings.security.sessions.current]]</strong>
                                <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.security.sessions.currentHelp]]" aria-label="[[t:settings.security.sessions.currentHelpLabel]]">help</button>
                              </div>
                              <div class="sub" id="app_auth_state">&mdash;</div>
                            </div>
                            <button class="btn" id="btn-auth-logout" onclick="cwAppLogout?.()">[[t:common.actions.logOut]]</button>
                          </div>
                          <div class="cw-page-session-row">
                            <div class="cw-page-session-copy">
                              <div class="cw-field-label-row">
                                <strong>[[t:settings.security.sessions.others]]</strong>
                                <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.security.sessions.othersHelp]]" aria-label="[[t:settings.security.sessions.othersHelpLabel]]">help</button>
                              </div>
                              <div class="sub" id="app_auth_other_sessions_state">[[t:settings.security.sessions.othersInitial]]</div>
                              <div class="sub" id="app_auth_other_sessions_detail">
                              </div>
                            </div>
                            <button class="btn" id="btn-auth-logout-others" onclick="cwAppLogoutOthers?.()">[[t:settings.security.sessions.logoutOthers]]</button>
                          </div>
                          <div class="cw-page-security-expanded">
                            <div class="cw-auth-session-row">
                              <div id="app_auth_session_fields">
                                <div class="cw-field-label-row">
                                  <label class="cw-page-settings-label" for="app_auth_remember_enabled">[[t:settings.security.sessions.staySignedIn]]</label>
                                  <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.security.sessions.cachingHelp]]" aria-label="[[t:settings.security.sessions.cachingHelpLabel]]">help</button>
                                </div>
                                <select id="app_auth_remember_enabled" name="app_auth_remember_enabled">
                                  <option value="true">[[t:common.status.enabled]]</option>
                                  <option value="false">[[t:settings.security.sessions.browserOnly]]</option>
                                </select>
                                <div class="sub" style="margin-top:0.35rem">[[t:settings.security.sessions.browserOnlyHint]]</div>
                              </div>
                              <div id="app_auth_remember_days_wrap">
                                <div class="cw-field-label-row">
                                  <label class="cw-page-settings-label" for="app_auth_remember_days">[[t:settings.security.sessions.rememberDays]]</label>
                                  <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.security.sessions.timeoutHelp]]" aria-label="[[t:settings.security.sessions.timeoutHelpLabel]]">help</button>
                                </div>
                                <input id="app_auth_remember_days" name="app_auth_remember_days" type="text" inputmode="numeric" pattern="[0-9]{1,3}" maxlength="3" autocomplete="off" placeholder="30">
                                <div id="app_auth_remember_days_error" class="cw-field-inline-error hidden" role="alert">
                                </div>
                                <div class="sub" style="margin-top:0.35rem">[[t:settings.security.sessions.rememberDaysHint]]</div>
                              </div>
                            </div>
                          </div>
                          <details class="cw-page-security-disclosure" id="app_auth_network_details">
                            <summary>
                              <span class="material-symbols-rounded cw-page-security-icon" aria-hidden="true">lan</span>
                              <span class="cw-page-security-copy">
                                <strong>[[t:settings.security.network.title]]</strong>
                                <span class="sub">[[t:settings.security.network.proxies]]</span>
                              </span>

                              <span class="material-symbols-rounded cw-page-security-chevron" aria-hidden="true">expand_more</span>
                            </summary>
                            <div class="cw-page-security-expanded">
                              <div class="cw-auth-proxy-field">
                                <div class="cw-field-label-row">
                                  <label class="cw-page-settings-label" for="trusted_proxies">[[t:settings.security.network.proxiesOptional]]</label>
                                  <button type="button" class="cw-field-help material-symbols-rounded" title="[[t:settings.security.network.proxiesHelp]]" aria-label="[[t:settings.security.network.proxiesHelpLabel]]">help</button>
                                </div>
                                <input id="trusted_proxies" name="trusted_proxies" type="text" placeholder="127.0.0.1;192.168.2.1;192.168.2.0/16">
                                <div class="sub" style="margin-top:0.35rem">
                                  [[t:settings.security.network.proxiesHint]] <code>;</code>
                                </div>
                              </div>
                            </div>
                          </details>
                        </div>
                      </div>
                    </div>
                    <details class="cw-settings-block cw-app-card cw-ai-card cw-page-security-card cw-page-ai-disclosure" id="ai_section">
                      <summary>
                        <span class="material-symbols-rounded cw-page-security-icon" aria-hidden="true">auto_awesome</span>
                        <span class="cw-page-security-copy">
                          <strong>[[t:settings.ai.title]]</strong>
                          <span class="sub">[[t:settings.ai.intro]]</span>
                        </span>

                        <span class="material-symbols-rounded cw-page-security-chevron" aria-hidden="true">expand_more</span>
                      </summary>
                      <div class="cw-ai-grid">
                        <form class="cw-ai-panel" id="assistant_settings" autocomplete="off" onsubmit="return false">
                          <div class="cw-ai-panel-head">
                            <div>
                              <h4>[[t:settings.ai.assistant.title]]</h4>
                              <p>[[t:settings.ai.assistant.intro]]</p>
                            </div>
                            <span class="cw-ai-pill" id="assistant_pill">[[t:common.status.off]]</span>
                          </div>
                          <div class="cw-ai-caption" id="assistant_presets_label">[[t:settings.ai.assistant.provider]]</div>
                          <div class="cw-ai-choices" id="assistant_presets" role="radiogroup" aria-labelledby="assistant_presets_label">
                            <button type="button" class="cw-ai-choice" role="radio" aria-checked="false" data-assistant-preset="gemini">
                              <strong>Google Gemini</strong>
                              <small>[[t:settings.ai.presets.gemini]]</small>
                            </button>
                            <button type="button" class="cw-ai-choice" role="radio" aria-checked="false" data-assistant-preset="openai">
                              <strong>OpenAI</strong>
                              <small>[[t:settings.ai.presets.openai]]</small>
                            </button>
                            <button type="button" class="cw-ai-choice" role="radio" aria-checked="false" data-assistant-preset="openrouter">
                              <strong>OpenRouter</strong>
                              <small>[[t:settings.ai.presets.openrouter]]</small>
                            </button>
                            <button type="button" class="cw-ai-choice" role="radio" aria-checked="false" data-assistant-preset="ollama">
                              <strong>Ollama</strong>
                              <small>[[t:settings.ai.presets.local]]</small>
                            </button>
                            <button type="button" class="cw-ai-choice" role="radio" aria-checked="false" data-assistant-preset="lmstudio">
                              <strong>LM Studio</strong>
                              <small>[[t:settings.ai.presets.local]]</small>
                            </button>
                            <button type="button" class="cw-ai-choice" role="radio" aria-checked="false" data-assistant-preset="custom">
                              <strong>[[t:common.labels.other]]</strong>
                              <small>[[t:settings.ai.presets.other]]</small>
                            </button>
                          </div>
                          <div class="cw-ai-fields">
                            <div class="cw-ai-field" id="assistant_url_field">
                              <label class="cw-page-settings-label" for="assistant_base_url">[[t:settings.ai.assistant.address]]</label>
                              <input id="assistant_base_url" type="url" autocomplete="off" spellcheck="false" placeholder="http://192.168.1.10:11434/v1">
                              <small>[[t:settings.ai.assistant.addressHint]]</small>
                            </div>
                            <div class="cw-ai-field" id="assistant_key_field">
                              <label class="cw-page-settings-label" for="assistant_api_key">[[t:settings.ai.assistant.apiKey]]</label>
                              <input id="assistant_api_key" type="password" autocomplete="new-password" spellcheck="false" placeholder="[[t:settings.ai.assistant.apiKeyPlaceholder]]">
                              <small id="assistant_key_note">[[t:settings.ai.assistant.apiKeyNote]]</small>
                            </div>
                            <div class="cw-ai-field">
                              <label class="cw-page-settings-label" for="assistant_model">[[t:settings.ai.assistant.model]]</label>
                              <input id="assistant_model" type="text" list="assistant_models" autocomplete="off" spellcheck="false" placeholder="[[t:settings.ai.assistant.modelPlaceholder]]">
                              <datalist id="assistant_models">
                              </datalist>
                              <details>
                                <summary>[[t:common.labels.advanced]]</summary>
                                <div class="cw-ai-field">
                                  <div class="cw-ai-inline">
                                    <button type="button" class="btn" id="btn-assistant-models">
                                      <span class="material-symbols-rounded" aria-hidden="true">refresh</span>
                                      <span>[[t:settings.ai.assistant.loadModels]]</span>
                                    </button>
                                  </div>
                                  <small>[[t:settings.ai.assistant.loadModelsHint]]</small>
                                  <label class="cw-page-settings-label" for="assistant_models_all">[[t:settings.ai.assistant.modelList]]</label>
                                  <select id="assistant_models_all">
                                    <option value="chat">[[t:settings.ai.assistant.chatModels]]</option>
                                    <option value="all">[[t:settings.ai.assistant.allModels]]</option>
                                  </select>
                                  <small>[[t:settings.ai.assistant.filterHint]]</small>
                                  <div class="cw-ai-inline">
                                    <button type="button" class="btn" id="btn-assistant-test">[[t:settings.ai.assistant.testModel]]</button>
                                  </div>
                                  <small id="assistant_model_test" role="status">[[t:settings.ai.assistant.testHint]]</small>
                                </div>
                              </details>
                            </div>
                            <div class="cw-ai-field">
                              <label class="cw-page-settings-label" for="assistant_wiki_help">[[t:settings.ai.assistant.withoutProvider]]</label>
                              <select id="assistant_wiki_help">
                                <option value="on">[[t:settings.ai.assistant.wikiAnswers]]</option>
                                <option value="off">[[t:settings.ai.assistant.turnOffAsk]]</option>
                              </select>
                            </div>
                          </div>
                          <div class="cw-ai-status" id="assistant_state" role="status">
                          </div>
                          <div class="cw-ai-actions">
                            <button type="button" class="btn primary" id="btn-assistant-save">[[t:settings.ai.assistant.saveOn]]</button>
                            <button type="button" class="btn" id="btn-assistant-open" disabled>
                              <span class="material-symbols-rounded" aria-hidden="true">forum</span>
                              <span>[[t:settings.ai.assistant.ask]]</span>
                            </button>
                            <button type="button" class="btn cw-ai-quiet hidden" id="btn-assistant-off">[[t:settings.ai.assistant.turnOff]]</button>
                          </div>
                          <p class="cw-ai-note">
                            <span class="material-symbols-rounded" aria-hidden="true">shield</span>
                            <span>[[t:settings.ai.assistant.privacy]]</span>
                          </p>
                        </form>
                        <div class="cw-ai-panel" id="mcp_access">
                          <div class="cw-ai-panel-head">
                            <div>
                              <h4>[[t:settings.ai.mcp.title]]</h4>
                              <p>[[t:settings.ai.mcp.intro]]</p>
                            </div>
                          </div>
                          <div class="cw-ai-label">
                            <span class="cw-ai-step">1</span>[[t:settings.ai.mcp.chooseApp]]</div>
                          <div class="cw-ai-choices cw-ai-choices-compact" id="mcp_clients" role="radiogroup" aria-label="[[t:settings.ai.mcp.app]]">
                            <button type="button" class="cw-ai-choice" role="radio" aria-checked="false" data-mcp-client="claude-code">
                              <strong>Claude Code</strong>
                            </button>
                            <button type="button" class="cw-ai-choice" role="radio" aria-checked="false" data-mcp-client="claude-desktop">
                              <strong>Claude Desktop</strong>
                            </button>
                            <button type="button" class="cw-ai-choice" role="radio" aria-checked="false" data-mcp-client="codex">
                              <strong>Codex CLI</strong>
                            </button>
                            <button type="button" class="cw-ai-choice" role="radio" aria-checked="false" data-mcp-client="cursor">
                              <strong>Cursor</strong>
                            </button>
                            <button type="button" class="cw-ai-choice" role="radio" aria-checked="false" data-mcp-client="gemini-cli">
                              <strong>Gemini CLI</strong>
                            </button>
                            <button type="button" class="cw-ai-choice" role="radio" aria-checked="false" data-mcp-client="lm-studio">
                              <strong>LM Studio</strong>
                            </button>
                            <button type="button" class="cw-ai-choice" role="radio" aria-checked="false" data-mcp-client="vscode">
                              <strong>VS Code</strong>
                            </button>
                            <button type="button" class="cw-ai-choice" role="radio" aria-checked="false" data-mcp-client="other">
                              <strong>[[t:common.labels.other]]</strong>
                            </button>
                          </div>
                          <div class="cw-ai-where hidden" id="mcp_desktop_where" role="radiogroup" aria-label="[[t:settings.ai.mcp.where]]">
                            <span>[[t:settings.ai.mcp.runsOn]]</span>
                            <button type="button" class="cw-ai-toggle" role="radio" aria-checked="true" data-mcp-where="remote">[[t:settings.ai.mcp.nas]]</button>
                            <button type="button" class="cw-ai-toggle" role="radio" aria-checked="false" data-mcp-where="local">[[t:settings.ai.mcp.docker]]</button>
                          </div>
                          <div class="cw-ai-label">
                            <span class="cw-ai-step">2</span>[[t:settings.ai.mcp.createToken]]</div>
                          <div class="cw-ai-tokens" id="mcp_tokens">
                          </div>
                          <div class="cw-ai-status" id="mcp_token_state" role="status">
                          </div>
                          <div class="cw-ai-actions">
                            <button type="button" class="btn primary" id="btn-mcp-token-create" disabled>
                              <span class="material-symbols-rounded" aria-hidden="true">key</span>
                              <span>[[t:settings.ai.mcp.createTokenButton]]</span>
                            </button>
                          </div>
                          <div class="cw-ai-label">
                            <span class="cw-ai-step">3</span>
                            <span id="mcp_client_note">[[t:settings.ai.mcp.paste]]</span>
                          </div>
                          <div class="cw-ai-code">
                            <pre>
                              <code id="mcp_snippet">
                              </code>
                            </pre>
                            <button type="button" class="cw-ai-copy" id="btn-mcp-copy">
                              <span class="material-symbols-rounded" aria-hidden="true">content_copy</span>
                              <span>[[t:common.actions.copy]]</span>
                            </button>
                          </div>
                        </div>
                      </div>
                    </details>
                  </div>
                </div>

              </div>
            </div>
          </div>
        </section>

        <section class="cw-settings-pane cw-page-maintenance" data-pane="maintenance">
          <div class="mt-page-head"><h3>[[t:settings.panes.maintenance.title]]</h3><p>[[t:settings.maintenance.intro]]</p></div>
          <div id="sec-troubleshoot" data-accordion="off">
            <section class="cw-page-panel mt-debug">
              <div><h4>[[t:settings.maintenance.debug.title]]</h4><p>[[t:settings.maintenance.debug.intro]]</p></div>
              <label class="mt-field" for="debug"><span>[[t:settings.maintenance.debug.level]]</span>
                <select id="debug" name="debug" class="cw-page-input">
                  <option value="off">[[t:common.status.off]]</option><option value="on">[[t:settings.maintenance.debug.standard]]</option>
                  <option value="mods">[[t:settings.maintenance.debug.provider]]</option><option value="full">[[t:settings.maintenance.debug.full]]</option>
                </select>
              </label>
            </section>
            <div class="cw-page-panel mt-tool-list" aria-label="[[t:settings.maintenance.actions]]">
              <button class="mt-tool" type="button" onclick="openMaintenance()">
                <span><strong>[[t:navigation.pages.maintenance]]</strong><small>[[t:settings.maintenance.toolsCopy]]</small></span><span class="mt-open" aria-hidden="true">[[t:common.actions.open]]</span>
              </button>
              <button class="mt-tool" type="button" onclick="openProviderCleanupModal()">
                <span><strong>[[t:settings.maintenance.cleanup]]</strong><small>[[t:settings.maintenance.cleanupCopy]]</small></span><span class="mt-open" aria-hidden="true">[[t:common.actions.open]]</span>
              </button>
              <button class="mt-tool" type="button" onclick="openSupportModal()">
                <span><strong>[[t:settings.maintenance.support]]</strong><small>[[t:settings.maintenance.supportCopy]]</small></span><span class="mt-open" aria-hidden="true">[[t:common.actions.open]]</span>
              </button>
            </div>
            <div class="mt-restart">
              <div><h4>[[t:settings.maintenance.restart]]</h4><p>[[t:settings.maintenance.restartCopy]]</p></div>
              <button class="cw-page-button mt-button mt-danger" type="button" onclick="restartCrossWatch()">[[t:common.actions.restart]]</button>
            </div>
            <div id="tb_msg" class="msg ok hidden">[[t:common.status.done]]</div>
          </div>
        </section>
      </div>
    </div>
  </section>


</main>

<div id="cw-help-overlay" class="hidden" aria-hidden="true">
  <div id="cw-help-card">
    <button id="cw-help-close" class="btn" type="button" onclick="window.cwCloseHelp()">[[t:common.actions.close]]</button>
    <iframe id="cw-help-frame" title="[[t:navigation.help.title]]" loading="lazy" referrerpolicy="no-referrer"></iframe>
  </div>
</div>


<script>(()=>{const $=id=>document.getElementById(id),closeMenu=id=>{const m=$("cw-"+id+"-menu"),b=$("tab-"+id);m?.classList.add("hidden");b?.setAttribute("aria-expanded","false")},closeAll=()=>{closeMenu("settings");closeMenu("about");closeMenu("tools")},toggleMenu=(id,e)=>{e?.preventDefault?.();e?.stopPropagation?.();const m=$("cw-"+id+"-menu"),b=$("tab-"+id);if(!m||!b)return;const open=m.classList.contains("hidden");closeAll();m.classList.toggle("hidden",!open);b.setAttribute("aria-expanded",String(open))},setHelp=open=>{const o=$("cw-help-overlay");if(!o)return;if(open){const f=$("cw-help-frame");if(f&&!f.src)f.src="https://wiki.crosswatch.app";o.classList.remove("hidden");o.setAttribute("aria-hidden","false")}else{o.classList.add("hidden");o.setAttribute("aria-hidden","true")}},openSettings=pane=>{window.showTab?.("settings");setTimeout(()=>window.cwSettingsSelect?.(pane),0)};window.CW_CURRENT_VERSION="__CW_CURRENT_VERSION__";window.APP_VERSION="__CW_VERSION__";window["__CW_"+"VERSION__"]=window.APP_VERSION;window.cwOpenHelp=()=>setHelp(true);window.cwCloseHelp=()=>setHelp(false);window.openHelp=()=>window.location?.protocol==="https:"?window.cwOpenHelp?.():window.open("https://wiki.crosswatch.app","_blank","noopener,noreferrer");window.cwCloseAboutMenu=()=>closeMenu("about");window.cwCloseSettingsMenu=()=>closeMenu("settings");window.cwToggleAboutMenu=e=>toggleMenu("about",e);window.cwToggleSettingsMenu=e=>toggleMenu("settings",e);window.cwCloseToolsMenu=()=>closeMenu("tools");window.cwToggleToolsMenu=e=>toggleMenu("tools",e);window.cwToolsMenuSelect=r=>{closeMenu("tools");const f={events:"openEvents",logs:"openLogs",analyzer:"openAnalyzer",import_export:"openExporter",maintenance:"openMaintenance"}[r];return f&&typeof window[f]==="function"?window[f]():window.showTab?.(r)};window.cwAboutMenuSelect=w=>(closeMenu("about"),w==="about"?window.openAbout?.():w==="help"?window.openHelp?.():w==="assistant"?window.openAssistant?.():undefined);window.cwSettingsMenuSelect=w=>{closeMenu("settings");if(w==="overview")return openSettings("overview");if(w==="providers")return openSettings("providers");if(w==="scheduling")return openSettings("scheduling");if(w==="pairs"||w==="sync")return openSettings("sync");if(w==="scrobbler")return openSettings("scrobbler");if(w==="app")return openSettings("app");if(w==="maintenance")return openSettings("maintenance")};document.addEventListener("click",e=>{const o=$("cw-help-overlay"),c=$("cw-help-card"),aboutHost=$("tab-about-menu"),settingsHost=$("tab-settings-menu"),toolsHost=$("tab-tools-menu");if(o&&!o.classList.contains("hidden")&&c&&!c.contains(e.target))window.cwCloseHelp?.();if(aboutHost&&!aboutHost.contains(e.target))closeMenu("about");if(settingsHost&&!settingsHost.contains(e.target))closeMenu("settings");if(toolsHost&&!toolsHost.contains(e.target))closeMenu("tools")},true);document.addEventListener("keydown",e=>{if(e.key!=="Escape")return;window.cwCloseHelp?.();closeAll()},true)})();</script>

__CW_ASSET_BLOCK__

<script>document.addEventListener('DOMContentLoaded',()=>{try{if(typeof openSummaryStream==='function')openSummaryStream()}catch(e){}});</script>

<div id="save-frost" class="hidden" aria-hidden="true"></div>
<div id="save-fab" class="hidden" role="toolbar" aria-label="[[t:common.actions.stickySave]]"><button id="save-fab-btn" class="btn" onclick="saveSettings(this)"><span class="btn-label">[[t:settings.saveFab]]</span></button></div>

<script>(()=>{const list=p=>[...p.querySelectorAll(':scope>.section')].filter(s=>s.dataset.accordion!=='off'),set=(s,on)=>{s.classList.toggle('open',!!on);s.querySelector('.head')?.setAttribute('aria-expanded',String(!!on));const c=s.querySelector('.chev');if(c)c.textContent=''},siblings=s=>s?.parentElement?list(s.parentElement):[];window.toggleSection=id=>{const s=document.getElementById(id);if(!s||s.dataset.accordion==='off')return;const on=!s.classList.contains('open');siblings(s).forEach(x=>set(x,x===s&&on))};window.openSection=id=>{const s=document.getElementById(id);if(!s||s.dataset.accordion==='off')return;siblings(s).forEach(x=>set(x,x===s))};document.addEventListener('click',e=>{const h=e.target?.closest?.('[data-toggle-section]');if(!h)return;e.preventDefault();window.toggleSection?.(h.dataset.toggleSection)},true);document.addEventListener('DOMContentLoaded',()=>[...new Set([...document.querySelectorAll('.section')].map(s=>s.parentElement).filter(Boolean))].forEach(p=>{const open=list(p).find(s=>s.classList.contains('open'));list(p).forEach(s=>set(s,s===open))}),{once:true})})();</script>


<script>(()=>{const panes='#page-settings .cw-settings-pane',nav='#cw-settings-nav .cw-settings-nav-btn',norm=v=>String(v||'overview').trim().toLowerCase(),apply=p=>{const name=norm(p);let found=false;document.querySelectorAll(panes).forEach(n=>{const on=norm(n.dataset.pane)===name;n.classList.toggle('active',on);found=found||on});if(!found){const first=norm(document.querySelector(panes)?.dataset?.pane);if(first&&first!==name)return apply(first)}document.querySelectorAll(nav).forEach(b=>{const on=norm(b.dataset.pane)===name;b.classList.toggle('active',on);b.setAttribute('aria-current',on?'page':'false')});window.__cwSettingsPane=name;delete document.documentElement.dataset.cwInitialPane;document.dispatchEvent(new CustomEvent('cw-settings-pane-changed',{detail:{pane:name}}))};window.cwSettingsSelect=p=>{const changed=norm(p)!==window.__cwSettingsPane;apply(p);const main=document.getElementById('cw-settings-left');if(changed&&main&&window.innerWidth<1200)main.scrollIntoView({behavior:'instant',block:'start'})};apply(window.__cwSettingsPane||'overview');document.addEventListener('DOMContentLoaded',()=>apply(window.__cwSettingsPane||'overview'),{once:true});document.addEventListener('tab-changed',e=>((e?.detail?.id||e?.detail?.tab)==='settings')&&setTimeout(()=>apply(window.__cwSettingsPane||'overview'),0))})();</script>
<script>(()=>{const sync=e=>{const pane=String(e?.detail?.pane||window.__cwSettingsPane||'overview').toLowerCase();document.querySelectorAll('#cw-settings-menu .cw-menu-item[data-settings-pane]').forEach(item=>{const active=item.dataset.settingsPane===pane;item.classList.toggle('active',active);if(active)item.setAttribute('aria-current','page');else item.removeAttribute('aria-current')})};document.addEventListener('cw-settings-pane-changed',sync);document.addEventListener('DOMContentLoaded',sync,{once:true})})();</script>

<script>(()=>{const ready=()=>{const input=document.querySelector('#cw-settings-nav .cw-settings-search input'),items=[...document.querySelectorAll('#cw-settings-nav .cw-settings-nav-btn')];if(!input||!items.length)return;const apply=()=>{const q=input.value.trim().toLowerCase();items.forEach(btn=>{btn.hidden=!!q&&!btn.textContent.toLowerCase().includes(q)})};input.addEventListener('input',apply);document.addEventListener('keydown',e=>{if((e.ctrlKey||e.metaKey)&&String(e.key).toLowerCase()==='k'){e.preventDefault();window.showTab?.('settings');setTimeout(()=>input.focus(),0)}})};document.readyState==='loading'?document.addEventListener('DOMContentLoaded',ready,{once:true}):ready()})();</script>

<script>(()=>{const scrollTo=id=>document.getElementById(id)?.scrollIntoView({behavior:'smooth',block:'start'});window.cwSyncJump=()=>window.cwSettingsSelect?.('sync');window.cwProvidersJump=sectionId=>{if(sectionId==='sec-sync')return window.cwSyncJump?.();window.cwSettingsSelect?.('providers');setTimeout(()=>{window.openSection?.(sectionId);scrollTo(sectionId)},0)};window.cwOverviewJump=(sectionId,authGroupId='')=>{if(sectionId==='sec-sync')return window.cwSyncJump?.();return(window.cwSettingsSelect?.('providers'),setTimeout(async()=>{if(sectionId==='sec-auth'){window.openSection?.('sec-auth');try{await window.mountAuthProviders?.()}catch{}if(authGroupId){window.openSection?.(authGroupId);scrollTo(authGroupId)}else scrollTo('sec-auth');return}window.openSection?.(sectionId);scrollTo(sectionId)},0))}})();</script>
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
  const setDone = (selector, done) => document.querySelector(selector)?.classList.toggle("is-done", !!done);
  const T = (key, vars) => (window.CW?.I18n || window.__cwI18n)?.t?.(key, vars) ?? key;
  const state = { primary: "auth" };

  const open = (key) => {
    if (key === "auth") return window.cwOverviewJump?.("sec-auth");
    if (key === "meta") {
      if (typeof window.openMetadataProviderForm === "function") {
        window.cwSettingsSelect?.("providers");
        setTimeout(() => window.openMetadataProviderForm?.("TMDB_METADATA")?.catch?.(() => window.openAddMetadata?.()), 0);
        return;
      }
      window.cwSettingsSelect?.("providers");
      return window.openAddMetadata?.();
    }
    if (key === "sync") return window.cwSyncJump?.();
    if (key === "scheduling" || key === "automation") return window.cwSettingsSelect?.("scheduling");
    if (key === "scrobbler") return window.cwSettingsSelect?.("scrobbler");
    if (key === "app") return window.cwSettingsSelect?.("app");
    if (key === "maintenance") return window.cwSettingsSelect?.("maintenance");
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
    const statusKey = String(opts.tone || "").trim().toLowerCase();
    card.classList.toggle("is-inactive", statusKey === "inactive");
    card.classList.toggle("is-warning", ["missing", "needs setup", "disabled", "inactive"].includes(statusKey));

    const statusEl = card.querySelector(".cw-settings-step-state");
    const detailEl = card.querySelector(".cw-settings-step-detail");
    const primaryLinkEl = card.querySelector(`#cw-settings-step-${step}-link`);
    const secondaryLinkEl = card.querySelector(`#cw-settings-step-${step}-alt-link`);

    if (statusEl) statusEl.textContent = opts.status || "";
    if (detailEl) detailEl.textContent = opts.detail || "";
    if (primaryLinkEl) primaryLinkEl.textContent = opts.link || "";
    if (secondaryLinkEl) secondaryLinkEl.textContent = opts.altLink || "";
  };

  const render = (data = {}) => {
    const authCount = Number(data?.auth?.configured || 0);
    const pairTotal = Number(data?.pairs?.total ?? data?.pairs?.count ?? 0);
    const pairActive = Number(data?.pairs?.enabled ?? data?.pairs?.active ?? data?.pairs?.count ?? 0);
    const pairDisabled = Number(data?.pairs?.disabled ?? Math.max(0, pairTotal - pairActive));
    const metaConfigured = Number(data?.meta?.configured || 0);
    const metaDetected = Number(data?.meta?.detected || 0);
    const scheduleOn = !!data?.sched?.enabled;
    const scrobOn = !!data?.scrob?.enabled;
    const automationOn = scheduleOn || scrobOn;

    const steps = {
      auth: authCount > 0,
      meta: metaConfigured > 0,
      sync: pairActive > 0,
      scheduling: scheduleOn || scrobOn
    };
    const order = ["auth", "meta", "sync", "scheduling"];
    const next = order.find((step) => !steps[step]);
    const doneCount = order.filter((step) => steps[step]).length;
    const overviewGrid = document.getElementById("cw-settings-overview-grid");

    state.primary = doneCount === 4 ? "scheduling" : next;
    overviewGrid?.classList.toggle("cw-settings-overview-complete", doneCount === 4);
    const percent = Math.round((doneCount / 4) * 100);
    const ring = $("cw-settings-progress-ring");
    if (ring) ring.style.setProperty("--cw-setup-progress", `${percent}%`);
    setText("cw-settings-progress-count", `${doneCount}/4`);
    setText("cw-settings-progress-percent", `${percent}%`);
    setText("cw-settings-progress-text", T("settings.overviewLive.stepsReady", { done: doneCount, total: 4 }));
    setWidth("cw-settings-progress-bar", `${Math.max(6, Math.min(100, percent))}%`);
    setText("cw-settings-scrobbler-cta", scrobOn ? T("settings.overviewLive.scrobblerSettings") : T("settings.overviewLive.openScrobbler"));
    order.forEach((step) => setDone(`[data-step-node="${step}"]`, steps[step]));

    if (!steps.auth) {
      setText("cw-settings-hero-title", T("settings.overviewLive.hero.connectTitle"));
      setText("cw-settings-hero-copy", T("settings.overviewLive.hero.connectCopy"));
      setText("cw-settings-primary-cta", T("settings.overviewLive.openConnections"));
    } else if (!steps.meta) {
      setText("cw-settings-hero-title", T("settings.overviewLive.hero.metaTitle"));
      setText("cw-settings-hero-copy", T("settings.overviewLive.hero.metaCopy"));
      setText("cw-settings-primary-cta", T("settings.overviewLive.openMetadata"));
    } else if (!steps.sync) {
      setText("cw-settings-hero-title", T("settings.overviewLive.hero.syncTitle"));
      setText("cw-settings-hero-copy", T("settings.overviewLive.hero.syncCopy"));
      setText("cw-settings-primary-cta", T("settings.overviewLive.openSyncPairs"));
    } else if (!steps.scheduling) {
      setText("cw-settings-hero-title", T("settings.overviewLive.hero.schedTitle"));
      setText("cw-settings-hero-copy", T("settings.overviewLive.hero.schedCopy"));
      setText("cw-settings-primary-cta", T("settings.overviewLive.openScheduling"));
    } else {
      setText("cw-settings-hero-title", T("settings.overviewLive.hero.doneTitle"));
      setText("cw-settings-hero-copy", T("settings.overviewLive.hero.doneCopy"));
      setText("cw-settings-primary-cta", T("settings.overviewLive.openScheduling"));
    }

    setText("cw-settings-stat-auth", String(authCount));
    setText("cw-settings-stat-auth-copy", authCount ? T("settings.overviewLive.profilesConnected", { count: authCount }) : T("settings.overviewLive.noProvidersYet"));
    setText("cw-settings-stat-pairs", String(pairActive));
    setText("cw-settings-stat-pairs-copy", pairTotal ? (pairDisabled ? T("settings.overviewLive.activeDisabled", { active: pairActive, disabled: pairDisabled }) : T("settings.overviewLive.pairsActive", { count: pairActive })) : T("settings.overviewLive.noPairsYet"));
    setText("cw-settings-stat-automation", automationOn ? T("settings.overviewLive.live") : T("settings.overviewLive.off"));
    setText("cw-settings-stat-automation-copy", T(scheduleOn && scrobOn ? "settings.overviewLive.bothOn" : scheduleOn ? "settings.overviewLive.schedOn" : scrobOn ? "settings.overviewLive.scrobOn" : "settings.overviewLive.bothOff"));
    setStep("auth", {
      tone: steps.auth ? "connected" : "needs setup",
      status: T(steps.auth ? "settings.overviewLive.status.connected" : "settings.overviewLive.status.needsSetup"),
      copy: steps.auth ? T("settings.overviewLive.profilesConnected", { count: authCount }) : T("settings.overviewLive.connectFirst"),
      detail: steps.auth ? T("settings.overviewLive.providersConnected", { count: authCount }) : T("settings.overviewLive.noProviders"),
      link: T("settings.overviewLive.manage"),
      done: steps.auth,
      active: next === "auth"
    });

    setStep("meta", {
      tone: steps.meta ? (metaDetected > metaConfigured ? "partial" : "ready") : "missing",
      status: steps.meta ? (metaDetected > metaConfigured ? T("settings.overviewLive.status.partial", { done: metaConfigured, total: Math.max(metaDetected, metaConfigured) }) : T("settings.overviewLive.status.ready")) : T("settings.overviewLive.status.missing"),
      copy: steps.meta ? (metaDetected > metaConfigured ? T("settings.overviewLive.metaPartial", { done: metaConfigured, total: Math.max(metaDetected, metaConfigured) }) : T("settings.overviewLive.tmdbReady")) : T("settings.overviewLive.configureTmdb"),
      detail: steps.meta ? (metaDetected > metaConfigured ? T("settings.overviewLive.partialReady", { done: metaConfigured, total: Math.max(metaDetected, metaConfigured) }) : T("settings.overviewLive.tmdbConfigured")) : T("settings.overviewLive.metaNeeded"),
      link: T("settings.overviewLive.manage"),
      done: steps.meta,
      active: next === "meta"
    });

    const syncTone = pairActive > 0 ? "active" : pairTotal > 0 ? "disabled" : "optional";
    const syncStatus = T(`settings.overviewLive.status.${syncTone}`);
    const syncCopy = pairTotal ? (pairDisabled ? T("settings.overviewLive.activeDisabled", { active: pairActive, disabled: pairDisabled }) : T("settings.overviewLive.syncPairsActive", { count: pairActive })) : T("settings.overviewLive.noPairsActive");
    setStep("sync", {
      tone: syncTone,
      status: syncStatus,
      copy: syncCopy,
      detail: pairTotal ? (pairDisabled ? T("settings.overviewLive.activeDisabled", { active: pairActive, disabled: pairDisabled }) : T("settings.overviewLive.activePairs", { count: pairActive })) : T("settings.overviewLive.noPairsConfigured"),
      link: T("settings.overviewLive.manage"),
      done: steps.sync,
      active: next === "sync",
      optional: !steps.sync
    });

    setStep("scheduling", {
      tone: scheduleOn || scrobOn ? "on" : "optional",
      status: T(scheduleOn && scrobOn ? "settings.overviewLive.status.bothOn" : scheduleOn ? "settings.overviewLive.status.schedOn" : scrobOn ? "settings.overviewLive.status.scrobOn" : "settings.overviewLive.status.optional"),
      copy: T(scheduleOn && scrobOn ? "settings.overviewLive.bothEnabled" : scheduleOn ? "settings.overviewLive.schedEnabled" : scrobOn ? "settings.overviewLive.scrobEnabled" : "settings.overviewLive.bothOptional"),
      detail: scheduleOn || scrobOn ? T("settings.overviewLive.enabledOf", { done: scheduleOn && scrobOn ? 2 : 1, total: 2 }) : T("settings.overviewLive.noAutomation"),
      link: T("settings.overviewLive.manage"),
      done: steps.scheduling,
      active: next === "scheduling",
      optional: !steps.scheduling
    });
  };

  document.addEventListener("cw-settings-overview-data", (e) => render(e?.detail?.data || {}));
  document.addEventListener("DOMContentLoaded", () => render({}), { once: true });
})();
</script>
<script>(()=>{window.cwScrobblerJump=sectionId=>{window.cwSettingsSelect?.('scrobbler');let tries=0;const jump=()=>{const el=document.getElementById(sectionId);if(el){el.scrollIntoView({behavior:'smooth',block:'start'});return}if(++tries<40)setTimeout(jump,50)};setTimeout(jump,0)};window.cwUiSettingsJump=tab=>(window.cwSettingsSelect?.('app'),setTimeout(()=>{const t=String(tab||'').trim().toLowerCase();window.cwUiSettingsSelect?.(t)},0))})();</script>

<script>(()=>{const origFetch=window.fetch;if(typeof origFetch!=='function'||origFetch.__cwAuthPendingWrapped)return;const pending=()=>window.cwIsAuthSetupPending?.()===true,allowPath=p=>p.startsWith('/api/app-auth/')||p==='/api/config/meta'||p.startsWith('/api/config/meta?')||p.startsWith('/assets/')||p==='/favicon.svg';const emptyJson=(body='{}')=>new Response(body,{status:200,headers:{'Content-Type':'application/json','Cache-Control':'no-store'}});window.fetch=Object.assign(async function(resource,init){try{if(!pending())return await origFetch(resource,init);const url=typeof resource==='string'?resource:String(resource?.url||'');const u=new URL(url,location.origin);if(u.origin!==location.origin||!u.pathname.startsWith('/api/')||allowPath(u.pathname)||allowPath(u.pathname+u.search))return await origFetch(resource,init);const method=String(init?.method||resource?.method||'GET').toUpperCase();if(method!=='GET'&&method!=='HEAD')return await origFetch(resource,init);if(u.pathname.startsWith('/api/config'))return emptyJson('{}');if(u.pathname.startsWith('/api/status'))return emptyJson('{"providers":{}}');if(u.pathname.startsWith('/api/pairs'))return emptyJson('[]');if(u.pathname.startsWith('/api/scheduling'))return emptyJson('{}');if(u.pathname.startsWith('/api/insights'))return emptyJson('{}');if(u.pathname.startsWith('/api/watch/'))return emptyJson('{}');if(u.pathname.startsWith('/api/webhooks/'))return emptyJson('{}');return emptyJson('{}')}catch{return await origFetch(resource,init)}},{__cwAuthPendingWrapped:true})})();</script>

<script>(()=>{const $=id=>document.getElementById(id),fab=$('save-fab'),frost=$('save-frost'),page=$('page-settings'),tab=$('tab-settings'),noSave=new Set(['overview','providers','sync']),pane=()=>String(window.__cwSettingsPane||'overview').toLowerCase(),visible=()=>{if(!page||noSave.has(pane()))return false;const cs=getComputedStyle(page);return!page.classList.contains('hidden')&&cs.display!=='none'&&cs.visibility!=='hidden'},update=()=>{const on=visible();fab?.classList.toggle('hidden',!on);frost?.classList.toggle('hidden',!on)},watch=(el,attrs)=>el&&new MutationObserver(update).observe(el,{attributes:true,attributeFilter:attrs});document.addEventListener('DOMContentLoaded',()=>{watch(page,['class','style']);watch(tab,['class']);update()},{once:true});document.addEventListener('tab-changed',update);document.addEventListener('cw-settings-pane-changed',update);window.addEventListener('hashchange',update);document.querySelector('.tabs')?.addEventListener('click',update,true)})();</script>

<script>(()=>{const install=()=>{const orig=window.saveSettings;if(typeof orig!=='function'||orig._wrapped)return;window.saveSettings=Object.assign(async function(btnOrEvent){const btn=btnOrEvent instanceof HTMLElement?btnOrEvent:document.getElementById('save-fab-btn');if(btn&&!btn.dataset.defaultHtml)btn.dataset.defaultHtml=btn.innerHTML;if(btn)btn.disabled=true;try{const ret=orig.apply(this,arguments);await(ret&&typeof ret.then==='function'?ret:Promise.resolve());window.invalidateConfigCache?.();window.manualRefreshStatus?.();const T=k=>window.CW.I18n.t(k),label=()=>{const s=document.createElement('span');s.className='btn-label';s.textContent=T('settings.saveFab');return s.outerHTML};if(btn){btn.textContent=T('settings.saved');setTimeout(()=>{btn.innerHTML=btn.dataset.defaultHtml||label();btn.disabled=false},1600)}return ret}catch(e){if(btn){btn.textContent=T('settings.saveFailed');setTimeout(()=>{btn.innerHTML=btn.dataset.defaultHtml||label();btn.disabled=false},2000)}throw e}},{_wrapped:true})};document.readyState==='complete'?install():window.addEventListener('load',install,{once:true})})();</script>
</body></html>

"""

def _seed_media_card_style(html: str, user: dict | None = None) -> str:
    from cw_platform.config_base import load_config
    from providers.metadata._meta_FANART import art_version

    cfg = load_config() or {}
    value = (cfg.get("ui") or {}).get("media_card", "full")
    if isinstance(user, dict) and user.get("is_admin") is False:
        value = (user.get("preferences") or {}).get("media_card", value)
    style = "compact" if value == "compact" else "full"
    try:
        from services.artwork_choices import revisions

        chosen = " ".join(f"{key}={rev}" for key, rev in sorted(revisions().items()))
    except Exception:
        chosen = ""
    return html.replace('<html lang="en"', f'<html lang="en" data-cw-media-card="{style}" data-cw-art="{art_version(cfg)}" data-cw-art-ov="{html_lib.escape(chosen, quote=True)}"', 1)


def _localize_page(html: str, user: dict | None, language: str | None) -> str:
    lang = i18n.clean_language(language) if language else i18n.request_language(None, user)
    signed_in = isinstance(user, dict) and bool(user.get("username") or user.get("id"))
    head = i18n.head_tags(lang, _asset_version_token(), signed_in=signed_in)
    html = html.replace("__CW_I18N_HEAD__", head, 1).replace('<html lang="en"', f'<html lang="{lang}"', 1)
    html = html.replace("__CW_LANGUAGE_OPTIONS__", i18n.language_options_html(lang))
    html = html.replace("__CW_LANGUAGE_HINT__", "[[t:common.language.hintAccount]]" if signed_in else "[[t:common.language.hintBrowser]]")
    return i18n.localize_html(html, lang)


def get_index_html(include_admin: bool = True, user: dict | None = None, language: str | None = None) -> str:
    html = _get_index_html_static().replace("__CW_ASSET_BLOCK__", _asset_block(include_admin=include_admin, user=user))
    html = html.replace("__CW_NAV_PROFILE_MENU__", _nav_profile_link(user if user is not None else {"is_admin": include_admin}))
    html = html.replace("__CW_TOOLS_MENU__", _tools_menu(maintenance=include_admin))
    html = html.replace('<html lang="en"', f'<html lang="en" data-cw-reconnect="{" ".join(reconnect_connection_keys())}"', 1)
    if not include_admin:
        perms = _managed_user_permissions(user)
        profile_id = html_lib.escape(str((user or {}).get("profile_id") or ""), quote=True) if isinstance(user, dict) else ""
        dashboard_allowed = bool(perms.get("dashboard")) and bool(perms.get("write"))
        attrs = (
            f'<html lang="en" data-cw-role="user" '
            f'data-cw-perm-dashboard="{"on" if dashboard_allowed else "off"}" '
            f'data-cw-perm-watchlist="{"on" if perms.get("watchlist") else "off"}" '
            f'data-cw-perm-playback="{"on" if perms.get("playback") else "off"}" '
            f'data-cw-perm-write="{"on" if perms.get("write") else "off"}" '
            f'data-cw-perm-tracker="{"on" if _managed_tracker_allowed(user) else "off"}" '
            f'data-cw-profile-id="{profile_id}"'
        )
        html = html.replace('<html lang="en"', attrs, 1)
        html = _managed_user_shell(html, user)
    else:
        html = _seed_nav_profile_avatar(html, user)
    return _localize_page(
        _seed_media_card_style(html, user)
        .replace("__CW_CURRENT_VERSION__", CURRENT_VERSION)
        .replace("__CW_VERSION__", _asset_version_token()),
        user,
        language,
    )


def get_profile_html(user: dict | None = None, language: str | None = None) -> str:
    return _get_user_page_html(user, hub=False, language=language)


def get_hub_html(user: dict | None = None, language: str | None = None) -> str:
    return _get_user_page_html(user, hub=True, language=language)


def _get_user_page_html(user: dict | None, *, hub: bool, language: str | None = None) -> str:
    is_admin = bool(isinstance(user, dict) and user.get("is_admin"))
    shared_header_scripts = ""
    if is_admin:
        shared_header_scripts += '<script src="/assets/js/assistant-settings.js?v=__CW_VERSION__" defer></script>'
    perms = {"dashboard": True, "watchlist": True, "playback": True, "write": True} if is_admin else _managed_user_permissions(user)
    esc = lambda value: html_lib.escape(str(value or ""), quote=True)
    role = "admin" if is_admin else "user"
    role_label = "[[t:profile.role.admin]]" if is_admin else "[[t:profile.role.managed]]"
    profile_id = "" if is_admin else esc((user or {}).get("profile_id"))
    username = esc((user or {}).get("username"))
    display_name = esc((user or {}).get("display_name") or (user or {}).get("label") or (user or {}).get("username") or "") or ("[[t:profile.role.admin]]" if is_admin else "[[t:navigation.account.profileFallback]]")
    write = "on" if perms.get("write") else "off"
    preferences_allowed = is_admin or bool(perms.get("write"))
    settings_interface_nav = (
        '    <button type="button" data-settings-nav="interface"><span class="material-symbols-rounded" aria-hidden="true">tune</span><span>[[t:profile.settings.display.title]]</span></button>'
    )
    settings_interface_controls = """          <label class="cw-set-row cw-set-toggle">
            <span class="cw-set-copy"><strong>[[t:profile.settings.display.playingCard]]</strong><small>[[t:profile.settings.display.playingCardHint]]</small></span>
            <input id="profile-pref-playing-card" type="checkbox" checked role="switch"><span class="cw-set-switch" aria-hidden="true"></span>
          </label>
          <label class="cw-set-row cw-set-toggle">
            <span class="cw-set-copy"><strong>[[t:profile.settings.display.quickAdd]]</strong><small>[[t:profile.settings.display.quickAddHint]]</small></span>
            <input id="profile-pref-quick-add" type="checkbox" checked role="switch"><span class="cw-set-switch" aria-hidden="true"></span>
          </label>
""" if preferences_allowed else ""
    settings_interface = f"""        <section id="settings-interface" class="cw-set-card" data-settings-section="interface" data-cw-autosave hidden>
          <header class="cw-set-head"><div><h2>[[t:profile.settings.display.title]]</h2><p>[[t:profile.settings.display.intro]]</p></div></header>
{settings_interface_controls}          <div class="cw-set-row">
            <label class="cw-set-copy" for="profile-media-card"><strong>[[t:settings.ui.appearance.mediaDetails]]</strong><small>[[t:profile.settings.display.mediaDetailsHint]]</small></label>
            <span class="cw-set-inline"><select id="profile-media-card" class="cw-set-input" data-cw-media-card><option value="full">[[t:settings.ui.appearance.mediaCardFull]]</option><option value="compact">[[t:settings.ui.appearance.mediaCardCompact]]</option></select></span>
          </div>
        </section>"""
    dashboard = "on" if perms.get("dashboard") else "off"
    watchlist = "on" if perms.get("watchlist") else "off"
    playback = "on" if perms.get("playback") else "off"
    write_nav = is_admin or bool(perms.get("write"))
    app_href_prefix = "/" if is_admin else "/?main=1"
    watchlist_href = "/hub#watchlist"
    playback_href = "/hub#playback"
    profile_avatar_inner = _profile_avatar_inner(user)
    settings_nav = (
        '<div class="cw-tabmenu" id="tab-settings-menu">'
        '<button id="tab-settings" class="tab" type="button" aria-haspopup="menu" aria-expanded="false" onclick="window.cwToggleSettingsMenu(event)"><span>[[t:navigation.tabs.settings]]</span><span class="tab-caret" aria-hidden="true"></span></button>'
        '<div class="cw-menu hidden" id="cw-settings-menu" role="menu" aria-labelledby="tab-settings">'
        '<button class="cw-menu-item active" data-settings-pane="overview" type="button" role="menuitem" aria-current="page" onclick="window.cwSettingsMenuSelect(\'overview\')"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">grid_view</span><span>[[t:navigation.settingsMenu.overview]]</span></button>'
        '<div class="cw-menu-sep" role="separator" aria-hidden="true"></div>'
        '<button class="cw-menu-item" data-settings-pane="providers" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect(\'providers\')"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">device_hub</span><span>[[t:navigation.settingsMenu.connections]]</span></button>'
        '<button class="cw-menu-item" data-settings-pane="sync" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect(\'pairs\')"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">sync_alt</span><span>[[t:navigation.settingsMenu.pairs]]</span></button>'
        '<button class="cw-menu-item" data-settings-pane="scrobbler" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect(\'scrobbler\')"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">sensors</span><span>[[t:navigation.settingsMenu.scrobbler]]</span></button>'
        '<button class="cw-menu-item" data-settings-pane="scheduling" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect(\'scheduling\')"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">schedule</span><span>[[t:navigation.settingsMenu.scheduling]]</span></button>'
        '<button class="cw-menu-item" data-settings-pane="app" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect(\'app\')"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">security</span><span>[[t:navigation.settingsMenu.app]]</span></button>'
        '<button class="cw-menu-item" data-settings-pane="maintenance" type="button" role="menuitem" onclick="window.cwSettingsMenuSelect(\'maintenance\')"><span class="material-symbols-rounded cw-menu-icon" aria-hidden="true">build</span><span>[[t:navigation.settingsMenu.maintenance]]</span></button>'
        '</div></div>'
    )
    nav_items: list[str] = []
    if is_admin:
        nav_items.extend([
            _profile_nav_tab("main", "/"),
            _profile_nav_tab("tracker", "/#tracker"),
            _profile_nav_tab("profile", "/hub", active=hub),
            _profile_nav_tab("snapshots", "/#snapshots"),
            _profile_nav_tab("playlists", "/#playlists"),
            _tools_menu(href_prefix="/"),
            settings_nav,
        ])
    elif write_nav:
        nav_items.extend([
            _profile_nav_tab("main", "/?main=1#main"),
            *([_profile_nav_tab("tracker", "/?main=1#tracker")] if _managed_tracker_allowed(user) else []),
            _profile_nav_tab("profile", "/hub", active=hub),
            _profile_nav_tab("snapshots", "/?main=1#snapshots"),
            _profile_nav_tab("playlists", "/?main=1#playlists"),
            _tools_menu(maintenance=False, href_prefix="/?main=1"),
        ])
    else:
        nav_items.append(_profile_nav_tab("profile", "/hub", active=hub))
    nav_items.append(_nav_profile_link(user).strip())
    profile_nav = "\n    ".join(nav_items)
    history_panel = _profile_timeline_panel(
        "history",
        title="profile.history.title",
        lede="",
        search="profile.history.search",
        sources=(
            ("synced", "profile.history.sourceSynced"),
            ("scrobble", "profile.history.sourceScrobbles"),
        ),
        coverage=(("all", "profile.records.coverageAny"), ("partial", "profile.records.coveragePartial"), ("full", "profile.records.coverageFull")),
        bulk=True,
    )
    ratings_panel = _profile_timeline_panel(
        "ratings",
        title="profile.ratings.title",
        lede="",
        search="profile.ratings.search",
        sources=(("ratings", "profile.ratings.title"),),
        coverage=(("all", "profile.records.coverageAny"), ("partial", "profile.records.coveragePartial"), ("full", "profile.records.coverageFull"), ("mismatch", "profile.records.coverageMismatch")),
        scores=True,
        bulk=True,
    )
    playback_allowed = bool(perms.get("playback"))
    playback_tab = '    <button id="profile-tab-playback" type="button" data-profile-tab="playback"><span class="material-symbols-rounded" aria-hidden="true">play_circle</span><span data-tab-label>[[t:profile.playback.title]]</span></button>' if playback_allowed else ""
    playback_panel = _profile_playback_panel() if playback_allowed else ""
    watchlist_allowed = bool(perms.get("watchlist"))
    watchlist_tab = '    <button id="profile-tab-watchlist" type="button" data-profile-tab="watchlist"><span class="material-symbols-rounded" aria-hidden="true">bookmark</span><span data-tab-label>[[t:common.media.watchlist]]</span></button>' if watchlist_allowed else ""
    watchlist_panel = _profile_timeline_panel(
        "watchlist",
        title="common.media.watchlist",
        lede="",
        search="profile.watchlist.search",
        sources=(("watchlist", "common.media.watchlist"),),
        coverage=(("all", "profile.records.coverageAny"), ("partial", "profile.records.coveragePartial"), ("full", "profile.records.coverageFull")),
        bulk=True,
        timeline=False,
    ) if watchlist_allowed else ""
    collection_bulk = _profile_bulk_bar("profile-collection-bulk", "collection")
    remove_dialog = _profile_remove_dialog()
    view_as_banner = """  <div id="profile-view-as-banner" class="cw-profile-viewas" role="status" hidden>
    <span class="material-symbols-rounded" aria-hidden="true">visibility</span>
    <span class="cw-profile-viewas-copy">[[t:profile.viewAs.viewing]] <strong id="profile-view-as-name"></strong> [[t:profile.viewAs.readOnly]]</span>
    <button id="profile-view-as-exit" class="cw-profile-viewas-exit" type="button">[[t:profile.viewAs.back]]</button>
  </div>""" if is_admin else ""
    page_content = f"""  <section class="cw-profile-hero cw-media-hero" id="profile-hero">
    <div class="cw-profile-hero-art" id="profile-hero-art"></div>
    <div class="cw-profile-hero-now" id="profile-hero-now"></div>
    <div class="cw-profile-hero-shade"></div>
    <div class="cw-media-hero-topbar">
      <div class="cw-media-hero-brand">
        <h1 class="cw-media-hero-kicker">[[t:profile.hero.title]]</h1>
        <div class="cw-profile-identity cw-media-hero-identity">
          <a id="profile-hero-avatar" class="cw-media-hero-avatar" href="/profile" aria-label="[[t:profile.hero.openUserSettings]]" title="[[t:profile.hero.userSettings]]">{profile_avatar_inner}</a>
          <div class="cw-profile-nameblock">
            <span id="profile-role" class="cw-profile-role" hidden>{role_label}</span>
            <div id="profile-display-name">{display_name}</div>
            <div id="profile-username" class="cw-profile-username">@{username}</div>
            <div id="profile-member-since" class="cw-profile-since hidden"><span class="material-symbols-rounded" aria-hidden="true">person</span><span></span></div>
            <div id="profile-hero-chips" class="cw-profile-chips"></div>
          </div>
        </div>
      </div>
    <article class="cw-profile-last hidden" role="button" tabindex="0" aria-label="[[t:profile.hero.lastWatchedDetails]]">
      <div class="cw-profile-last-copy">
        <div class="cw-profile-last-head">
          <span class="cw-profile-last-kicker">[[t:profile.hero.lastWatched]]</span>
          <strong id="profile-last-title">[[t:profile.hero.nothingWatched]]</strong>
          <span id="profile-last-meta">[[t:profile.hero.historyAppears]]</span>
        </div>
      </div>
    </article>
    </div>
    <div id="profile-hero-media" class="cw-media-hero-context" aria-live="polite">
      <span class="cw-media-hero-heading"><strong>[[t:profile.hero.tagline]]</strong><img class="cw-media-hero-logo" alt="" hidden></span>
      <div class="cw-media-hero-details"><span id="profile-hero-summary"></span><span id="profile-hero-extra" class="cw-media-hero-extra"></span></div>
    </div>
    <div id="profile-hub-summary" class="cw-hub-summary" aria-label="[[t:profile.hero.summary]]">
      <div id="profile-hub-metrics" class="cw-hub-summary-metrics"></div>
      <div class="cw-hub-summary-tools">
        <span id="profile-hub-freshness" role="status">[[t:profile.hero.notChecked]]</span>
        <button id="profile-hub-refresh" type="button" class="cw-btn" aria-label="[[t:profile.hero.refreshOverview]]"><span class="material-symbols-rounded" aria-hidden="true">refresh</span><span>[[t:common.actions.refresh]]</span></button>
        <details id="profile-hub-issues" hidden><summary></summary><div></div></details>
      </div>
    </div>
    <div id="profile-now" class="cw-profile-now hidden" aria-live="polite">
      <div class="cw-profile-now-copy">
        <div class="cw-profile-now-head">
          <span class="cw-profile-now-dot" aria-hidden="true"></span>
          <span class="cw-profile-now-kicker">[[t:profile.hero.nowPlaying]]</span>
          <strong id="profile-now-title"></strong>
          <span id="profile-now-meta"></span>
          <span id="profile-now-provider"></span>
        </div>
        <div class="cw-profile-now-track"><span id="profile-now-fill"></span></div>
        <div class="cw-profile-now-times"><span id="profile-now-pct"></span><span id="profile-now-left"></span></div>
      </div>
      <img id="profile-now-poster" class="cw-profile-now-poster" src="/assets/img/placeholder_poster.svg?v=2" alt="">
    </div>
  </section>
{view_as_banner}
  <div class="cw-profile-tabs" role="tablist" aria-label="[[t:profile.tabs.label]]">
    <button id="profile-tab-overview" class="active" type="button" data-profile-tab="overview"><span class="material-symbols-rounded" aria-hidden="true">space_dashboard</span><span data-tab-label>[[t:profile.tabs.overview]]</span></button>
{watchlist_tab}
    <button id="profile-tab-history" type="button" data-profile-tab="history"><span class="material-symbols-rounded" aria-hidden="true">history</span><span data-tab-label>[[t:profile.history.title]]</span></button>
    <button id="profile-tab-ratings" type="button" data-profile-tab="ratings"><span class="material-symbols-rounded" aria-hidden="true">star</span><span data-tab-label>[[t:profile.ratings.title]]</span></button>
{playback_tab}
    <button id="profile-tab-collection" type="button" data-profile-tab="collection"><span class="material-symbols-rounded" aria-hidden="true">video_library</span><span data-tab-label>[[t:profile.tabs.collection]]</span></button>
  </div>
  <section id="profile-panel-overview" class="cw-profile-panel active">
    <article class="cw-hub-section cw-hub-continue">
      <div class="cw-hub-section-head"><h2>[[t:profile.overview.continueWatching]]</h2><a href="{playback_href}">[[t:common.actions.viewAll]]</a></div>
      <div id="profile-progress"></div>
    </article>
    <article id="profile-airing" class="cw-hub-section" aria-labelledby="profile-airing-title">
      <div class="cw-hub-section-head"><h2 id="profile-airing-title">[[t:profile.overview.airingSoon]]</h2><div class="trk-calendar-actions">
        <button type="button" class="cw-btn" data-hub-calendar="today">[[t:common.time.today]]</button>
        <button type="button" class="cw-btn" data-hub-calendar="previous" aria-label="[[t:profile.overview.previousDays]]"><span class="material-symbols-rounded" aria-hidden="true">chevron_left</span></button>
        <button type="button" class="cw-btn" data-hub-calendar="next" aria-label="[[t:profile.overview.nextDays]]"><span class="material-symbols-rounded" aria-hidden="true">chevron_right</span></button>
      </div></div>
      <div id="profile-airing-days" class="trk-calendar" role="region" aria-label="[[t:profile.overview.upcoming30]]" tabindex="0"></div>
      <p id="profile-airing-notice" role="status" hidden></p>
    </article>
    <div class="cw-hub-secondary">
      <article class="cw-hub-section">
        <div class="cw-hub-section-head"><h2>[[t:profile.overview.recentWatchlist]]</h2><a href="{watchlist_href}">[[t:common.actions.viewAll]]</a></div>
        <div id="profile-watchlist"></div>
      </article>
      <article class="cw-hub-section">
        <div class="cw-hub-section-head"><h2><span class="material-symbols-rounded cw-profile-activity-icon" aria-hidden="true">layers</span>[[t:profile.overview.recentActivity]]</h2><button id="profile-activity-view-all" type="button">[[t:common.actions.viewAll]]</button></div>
        <div id="profile-activity" class="cw-profile-activity-list"></div>
      </article>
    </div>
  <section id="dashboard-widgets-card" class="cw-dashboard-widgets hidden" aria-label="[[t:dashboard.widgets.label]]">
    <article id="recent-history-widget" class="cw-dash-widget cw-dash-widget--history">
      <div class="cw-dash-widget-head">
        <div class="cw-dash-title-row">
          <span class="material-symbols-rounded" aria-hidden="true">play_arrow</span>
          <h3>[[t:dashboard.widgets.recentHistory]]</h3>
        </div>
        <div class="cw-dash-head-actions">
          <span id="recent-history-count-chip" class="cw-widget-count-chip hidden" aria-live="polite"></span>
          <button id="recent-history-refresh" class="cw-dash-ghost" type="button" title="[[t:dashboard.widgets.refreshRecentHistory]]" aria-label="[[t:dashboard.widgets.refreshRecentHistory]]"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button>
        </div>
      </div>
      <div id="recent-history-list" class="cw-history-widget-list cw-widget-scrollbar" aria-live="polite"></div>
    </article>

    <article id="latest-ratings-widget" class="cw-dash-widget cw-dash-widget--ratings">
      <div class="cw-dash-widget-head">
        <div class="cw-dash-title-row">
          <span class="material-symbols-rounded" aria-hidden="true">star</span>
          <h3>[[t:dashboard.widgets.latestRatings]]</h3>
        </div>
        <div class="cw-dash-head-actions">
          <span id="latest-ratings-count-chip" class="cw-widget-count-chip hidden" aria-live="polite"></span>
          <button id="latest-ratings-refresh" class="cw-dash-ghost" type="button" title="[[t:dashboard.widgets.refreshLatestRatings]]" aria-label="[[t:dashboard.widgets.refreshLatestRatings]]"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button>
        </div>
      </div>
      <div id="latest-ratings-grid" class="cw-ratings-widget-grid" aria-live="polite"></div>
    </article>

    <article id="recent-scrobble-widget" class="cw-dash-widget cw-dash-widget--scrobble">
      <div class="cw-dash-widget-head">
        <div class="cw-dash-title-row">
          <span class="material-symbols-rounded" aria-hidden="true">sensors</span>
          <h3>[[t:dashboard.widgets.recentScrobble]]</h3>
        </div>
        <div class="cw-dash-head-actions">
          <span id="recent-scrobble-count-chip" class="cw-widget-count-chip hidden" aria-live="polite"></span>
          <button id="recent-scrobble-refresh" class="cw-dash-ghost" type="button" title="[[t:dashboard.widgets.refreshRecentScrobble]]" aria-label="[[t:dashboard.widgets.refreshRecentScrobble]]"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button>
        </div>
      </div>
      <div id="recent-scrobble-list" class="cw-history-widget-list cw-widget-scrollbar" aria-live="polite"></div>
    </article>

    <article id="recent-progress-widget" class="cw-dash-widget cw-dash-widget--progress">
      <div class="cw-dash-widget-head">
        <div class="cw-dash-title-row">
          <span class="material-symbols-rounded" aria-hidden="true">timelapse</span>
          <h3>[[t:dashboard.widgets.recentProgress]]</h3>
        </div>
        <div class="cw-dash-head-actions">
          <span id="recent-progress-count-chip" class="cw-widget-count-chip hidden" aria-live="polite"></span>
          <button id="recent-progress-refresh" class="cw-dash-ghost" type="button" title="[[t:dashboard.widgets.refreshRecentProgress]]" aria-label="[[t:dashboard.widgets.refreshRecentProgress]]"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button>
        </div>
      </div>
      <div id="recent-progress-list" class="cw-history-widget-list cw-widget-scrollbar" aria-live="polite"></div>
    </article>

    <article id="recent-playlists-widget" class="cw-dash-widget cw-dash-widget--playlists">
      <div class="cw-dash-widget-head">
        <div class="cw-dash-title-row">
          <span class="material-symbols-rounded" aria-hidden="true">queue_music</span>
          <h3>[[t:dashboard.widgets.recentPlaylists]]</h3>
        </div>
        <div class="cw-dash-head-actions">
          <span id="recent-playlists-count-chip" class="cw-widget-count-chip hidden" aria-live="polite"></span>
          <button id="recent-playlists-refresh" class="cw-dash-ghost" type="button" title="[[t:dashboard.widgets.refreshRecentPlaylists]]" aria-label="[[t:dashboard.widgets.refreshRecentPlaylists]]"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button>
        </div>
      </div>
      <div id="recent-playlists-list" class="cw-history-widget-list cw-widget-scrollbar" aria-live="polite"></div>
    </article>
  </section>
  </section>
  <section id="profile-panel-collection" class="cw-profile-panel cw-hub-records cw-page-panel">
    <div class="cw-hub-records-head">
      <div class="cw-hub-records-headline">
        <h2>[[t:profile.collection.title]]</h2>
      </div>
      <div class="cw-hub-records-tiles" id="profile-collection-metrics"></div>
    </div>
    <div class="cw-hub-records-toolbar">
      <label class="cw-hub-records-search">
        <span class="material-symbols-rounded" aria-hidden="true">search</span>
        <input id="profile-collection-search" type="search" placeholder="[[t:profile.collection.search]]">
      </label>
      <div class="cw-hub-records-chips" id="profile-collection-types" aria-label="[[t:profile.collection.typeFilters]]"></div>
      <div class="cw-hub-records-controls">
        <select id="profile-collection-shelf" aria-label="[[t:profile.collection.source]]">
          <option value="synced">[[t:profile.collection.synced]]</option>
          <option value="shelf">[[t:profile.collection.myShelf]]</option>
          <option value="all">[[t:profile.collection.syncedAndShelf]]</option>
          <option value="unwatched">[[t:profile.collection.shelfNeverWatched]]</option>
          <option value="not_on_server">[[t:profile.collection.shelfNotOnServer]]</option>
          <option value="not_on_shelf">[[t:profile.collection.serverNotOnShelf]]</option>
        </select>
        <select id="profile-collection-shelf-filter" aria-label="[[t:profile.collection.shelfFilter]]" hidden><option value="">[[t:profile.collection.wholeShelf]]</option></select>
        <select id="profile-collection-provider" aria-label="[[t:profile.records.providerFilter]]"><option value="">[[t:profile.records.allProviders]]</option></select>
        <select id="profile-collection-sort" aria-label="[[t:profile.collection.sort]]">
          <option value="collected_at">[[t:profile.collection.sortRecent]]</option>
          <option value="collected_at_asc">[[t:profile.collection.sortOldest]]</option>
          <option value="title">[[t:profile.playback.sort.title]]</option>
          <option value="title_desc">[[t:profile.collection.sortTitleDesc]]</option>
          <option value="year_desc">[[t:profile.collection.sortYearNew]]</option>
          <option value="year_asc">[[t:profile.collection.sortYearOld]]</option>
          <option value="price_desc">[[t:profile.collection.sortPrice]]</option>
          <option value="purchased_desc">[[t:profile.collection.sortBought]]</option>
        </select>
        <div class="cw-hub-records-views" role="group" aria-label="[[t:profile.records.viewMode]]">
          <button class="cw-view-shelf" type="button" data-collection-display="shelf" title="[[t:profile.collection.showShelf]]" aria-label="[[t:profile.collection.showShelf]]" aria-pressed="false"><span class="material-symbols-rounded" aria-hidden="true">shelves</span></button>
          <button class="cw-view-timeline" type="button" data-collection-timeline title="[[t:profile.records.showTimeline]]" aria-label="[[t:profile.records.showTimeline]]" aria-pressed="false"><span class="material-symbols-rounded" aria-hidden="true">timeline</span></button>
          <button class="active" type="button" data-collection-view="grid" title="[[t:profile.records.gridView]]" aria-label="[[t:profile.records.gridView]]" aria-pressed="true"><span class="material-symbols-rounded" aria-hidden="true">grid_view</span></button>
          <button type="button" data-collection-view="list" title="[[t:profile.records.listView]]" aria-label="[[t:profile.records.listView]]" aria-pressed="false"><span class="material-symbols-rounded" aria-hidden="true">view_list</span></button>
        </div>
        <button id="profile-collection-refresh" class="cw-tl-ghost cw-collection-refresh" type="button" data-collection-refresh title="[[t:profile.collection.refresh]]" aria-label="[[t:profile.collection.refresh]]"><span class="material-symbols-rounded" aria-hidden="true">refresh</span></button>
      </div>
    </div>
    <div id="profile-collection-timeline" class="cw-hist-timeline" aria-label="[[t:profile.collection.perMonth]]" hidden></div>
    <div id="profile-collection-shelfstats" class="cw-shelf-stats-host" hidden></div>
    <div id="profile-collection-grid" class="cw-collection-grid" data-view="grid" aria-live="polite"></div>
    <div id="profile-collection-footer" class="cw-collection-footer" hidden>
      <nav id="profile-collection-pages" class="cw-collection-pages" aria-label="[[t:profile.collection.pages]]"></nav>
      <div class="cw-collection-summary">
        <span id="profile-collection-page-label"></span>
        <select id="profile-collection-page-size" aria-label="[[t:profile.records.perPage]]">
          <option value="24">24</option>
          <option value="48">48</option>
          <option value="72">72</option>
          <option value="96">96</option>
        </select>
      </div>
    </div>
{collection_bulk}  </section>
{history_panel}
{ratings_panel}
{playback_panel}
{watchlist_panel}
{remove_dialog}
""" if hub else f"""  <section class="cw-profile-hero cw-media-hero cw-account-hero" id="profile-account-hero" aria-label="[[t:profile.account.yourProfile]]">
    <div class="cw-profile-hero-art" id="profile-hero-art"></div>
    <div class="cw-profile-hero-shade"></div>
    <div class="cw-media-hero-topbar">
      <div class="cw-media-hero-brand"><h1 class="cw-media-hero-kicker">[[t:profile.account.kicker]]</h1></div>
    </div>
    <div class="cw-media-hero-context cw-account-hero-identity">
      <div class="cw-account-picture">
        <span id="profile-settings-avatar" class="cw-account-avatar" aria-hidden="true">{profile_avatar_inner}</span>
        <button id="profile-avatar-replace" class="cw-account-camera" type="button" aria-label="[[t:profile.account.changePicture]]" title="[[t:profile.account.changePicture]]"><span class="material-symbols-rounded" aria-hidden="true">photo_camera</span></button>
        <input id="profile-avatar-input" type="file" accept="image/png,image/jpeg,image/webp" hidden>
      </div>
      <div class="cw-account-copy">
        <span class="cw-media-hero-heading"><strong id="profile-display-name">{display_name}</strong></span>
        <div class="cw-media-hero-details cw-account-meta"><span id="profile-username">@{username}</span><span id="profile-role" class="cw-set-tag">{role_label}</span><span id="profile-member-since" class="cw-account-since hidden"><span class="material-symbols-rounded" aria-hidden="true">event</span><span></span></span></div>
        <button id="profile-avatar-remove" class="cw-account-remove cw-danger-confirm" type="button">[[t:profile.account.removePicture]]</button>
      </div>
    </div>
    <div class="cw-hub-summary" aria-label="[[t:profile.account.summary]]"><div id="profile-account-metrics" class="cw-hub-summary-metrics"></div></div>
  </section>
  <div id="profile-avatar-upload-status" class="cw-profile-upload hidden" role="status" aria-live="polite">
    <div class="cw-profile-upload-head"><span id="profile-avatar-upload-label">[[t:profile.account.uploadingPicture]]</span><strong id="profile-avatar-upload-percent">0%</strong></div>
    <div class="cw-profile-upload-bar"><span id="profile-avatar-upload-bar"></span></div>
  </div>
  <nav class="cw-profile-tabs" data-account-tabs aria-label="[[t:settings.nav.sections]]">
    <button type="button" class="active" data-settings-nav="profile"><span class="material-symbols-rounded" aria-hidden="true">person</span><span>[[t:profile.account.sections.profile]]</span></button>
    <button type="button" data-settings-nav="security"><span class="material-symbols-rounded" aria-hidden="true">lock</span><span>[[t:profile.account.sections.security]]</span></button>
    <button type="button" data-settings-nav="linked"><span class="material-symbols-rounded" aria-hidden="true">link</span><span>[[t:profile.account.sections.linked]]</span></button>
    <button type="button" data-settings-nav="sessions"><span class="material-symbols-rounded" aria-hidden="true">devices</span><span>[[t:profile.account.sections.devices]]</span></button>
{settings_interface_nav}
  </nav>
  <section id="profile-panel-account" class="cw-profile-panel active">
    <div class="cw-set">
      <div class="cw-set-main">
        <div class="cw-set-col">
        <section id="settings-profile" class="cw-set-card" data-settings-section="profile">
          <header class="cw-set-head"><div><h2>[[t:profile.account.personal]]</h2></div></header>
          <form id="profile-name-form" class="cw-set-row">
            <label class="cw-set-copy" for="profile-display-input"><strong>[[t:profile.account.displayName]]</strong><small>[[t:profile.account.displayNameHint]]</small></label>
            <span class="cw-set-inline"><input id="profile-display-input" class="cw-set-input" maxlength="64" autocomplete="name"><button class="cw-set-btn cw-set-btn--primary" type="submit">[[t:common.actions.save]]</button></span>
          </form>
          <div class="cw-set-row">
            <label class="cw-set-copy" for="profile-username-input"><strong>[[t:settings.security.username]]</strong></label>
            <span class="cw-set-inline"><input id="profile-username-input" class="cw-set-input" value="@{username}" readonly aria-readonly="true"></span>
          </div>
        </section>
        <section id="settings-datetime" class="cw-set-card" data-settings-section="profile">
          <header class="cw-set-head"><div><h2>[[t:profile.settings.dateTime]]</h2></div></header>
          <div class="cw-set-row">
            <label class="cw-set-copy" for="profile-pref-language"><strong>[[t:common.language.label]]</strong><small id="profile-pref-language-hint" data-cw-language-status>[[t:common.language.hintAccount]]</small></label>
            <span class="cw-set-inline"><select id="profile-pref-language" class="cw-set-input" data-cw-language-select aria-describedby="profile-pref-language-hint">__CW_LANGUAGE_OPTIONS__</select></span>
          </div>
          <div class="cw-set-row">
            <label class="cw-set-copy" for="profile-pref-timezone"><strong>[[t:profile.settings.timezone]]</strong><small>[[t:profile.settings.timezoneHint]]</small></label>
            <div class="cw-set-inline cw-set-field">
              <input id="profile-timezone-search" class="cw-set-input" type="search" placeholder="[[t:profile.settings.searchTimezones]]" aria-label="[[t:profile.settings.searchTimezones]]" aria-controls="profile-pref-timezone" autocomplete="off">
              <select id="profile-pref-timezone" class="cw-set-input" data-cw-native-select="true"><option value="auto">[[t:profile.settings.autoBrowser]]</option><option value="UTC">UTC</option></select>
              <small id="profile-timezone-results" role="status" hidden></small>
            </div>
          </div>
          <div class="cw-set-row">
            <label class="cw-set-copy" for="profile-pref-time-format"><strong>[[t:profile.settings.timeFormat]]</strong><small>[[t:profile.settings.timeFormatHint]]</small></label>
            <span class="cw-set-inline"><select id="profile-pref-time-format" class="cw-set-input"><option value="auto">[[t:profile.settings.autoBrowser]]</option><option value="12h">[[t:profile.settings.time12]]</option><option value="24h">[[t:profile.settings.time24]]</option></select></span>
          </div>
        </section>
        <section id="settings-security" class="cw-set-card" data-settings-section="security" hidden>
          <header class="cw-set-head"><div><h2>[[t:profile.account.sections.security]]</h2><p>[[t:profile.security.intro]]</p></div></header>
          <div class="cw-set-row">
            <span class="cw-set-copy"><strong>[[t:settings.security.password.title]]</strong><small>[[t:profile.security.passwordHint]]</small></span>
            <span class="cw-set-actions"><button id="profile-password-toggle" class="cw-set-btn" type="button" aria-expanded="false" aria-controls="profile-password-form">[[t:common.actions.change]]</button></span>
          </div>
          <form id="profile-password-form" class="cw-set-drawer" hidden>
            <input type="text" name="username" value="{username}" autocomplete="username" hidden>
            <label class="cw-set-field"><span>[[t:profile.security.currentPassword]]</span><input id="profile-current-password" class="cw-set-input" type="password" autocomplete="current-password"></label>
            <label class="cw-set-field"><span>[[t:settings.security.password.new]]</span><input id="profile-new-password" class="cw-set-input" type="password" autocomplete="new-password"></label>
            <span class="cw-set-drawer-actions"><button id="profile-password-cancel" class="cw-set-btn" type="button">[[t:common.actions.cancel]]</button><button class="cw-set-btn cw-set-btn--primary" type="submit">[[t:profile.security.savePassword]]</button></span>
          </form>
          <div class="cw-set-row">
            <span class="cw-set-copy"><strong>[[t:profile.security.twoStep]]</strong><small id="profile-2fa-copy">[[t:profile.security.twoStepHint]]</small></span>
            <span class="cw-set-actions">
              <span id="profile-2fa-state" class="cw-set-status">[[t:common.status.off]]</span>
              <button id="profile-2fa-setup-btn" class="cw-set-btn cw-set-btn--primary" type="button">[[t:common.actions.turnOn]]</button>
              <button id="profile-recovery-btn" class="cw-set-btn" type="button" hidden>[[t:profile.security.backupCodes]]</button>
              <button id="profile-2fa-disable-btn" class="cw-set-btn cw-set-btn--danger" type="button" hidden>[[t:settings.ai.assistant.turnOff]]</button>
            </span>
          </div>
          <div id="profile-2fa-confirm" class="cw-set-drawer" hidden>
            <label class="cw-set-field"><span id="profile-2fa-confirm-label">[[t:profile.security.enterPassword]]</span><input id="profile-2fa-confirm-password" class="cw-set-input" type="password" autocomplete="current-password"></label>
            <span class="cw-set-drawer-actions"><button id="profile-2fa-confirm-cancel" class="cw-set-btn" type="button">[[t:common.actions.cancel]]</button><button id="profile-2fa-confirm-go" class="cw-set-btn cw-set-btn--primary" type="button">[[t:common.actions.continue]]</button></span>
          </div>
          <div id="profile-2fa-setup" class="cw-profile-2fa"></div>
        </section>
        <section id="settings-linked" class="cw-set-card" data-settings-section="linked" hidden>
          <header class="cw-set-head"><div><h2>[[t:profile.account.sections.linked]]</h2><p>[[t:profile.linked.intro]]</p></div></header>
          <div class="cw-set-row">
            <span class="cw-set-logo"><img id="profile-plex-logo" alt="" hidden><span class="material-symbols-rounded" aria-hidden="true">play_circle</span></span>
            <span class="cw-set-copy"><strong>Plex</strong><small id="profile-plex-summary">[[t:settings.security.plex.notLinked]]</small></span>
            <span class="cw-set-actions">
              <span id="profile-plex-state" class="cw-set-status">[[t:common.status.off]]</span>
              <button id="profile-plex-link" class="cw-set-btn cw-set-btn--primary" type="button">[[t:common.actions.link]]</button>
              <button id="profile-plex-unlink" class="cw-set-btn cw-set-btn--danger cw-danger-confirm" type="button" hidden>[[t:common.actions.unlink]]</button>
            </span>
          </div>
          <div class="cw-set-row">
            <span class="cw-set-logo"><span class="material-symbols-rounded" aria-hidden="true">key</span></span>
            <span class="cw-set-copy"><strong>[[t:profile.linked.sso]]</strong><small id="profile-oidc-summary">[[t:settings.security.plex.notLinked]]</small></span>
            <span class="cw-set-actions">
              <span id="profile-oidc-state" class="cw-set-status">[[t:common.status.off]]</span>
              <button id="profile-oidc-link" class="cw-set-btn cw-set-btn--primary" type="button">[[t:common.actions.link]]</button>
              <button id="profile-oidc-unlink" class="cw-set-btn cw-set-btn--danger cw-danger-confirm" type="button" hidden>[[t:common.actions.unlink]]</button>
            </span>
          </div>
        </section>
        </div>
        <div class="cw-set-col">
{settings_interface}
        <section id="settings-sessions" class="cw-set-card" data-settings-section="sessions" hidden>
          <header class="cw-set-head"><div><h2>[[t:profile.account.sections.devices]]</h2><p>[[t:profile.devices.intro]]</p></div><button id="profile-revoke-sessions" class="cw-set-btn cw-set-btn--danger cw-danger-confirm" type="button" hidden>[[t:profile.devices.signOutOthers]]</button></header>
          <div id="profile-sessions" class="cw-set-sessions"></div>
        </section>
        </div>
      </div>
    </div>
  </section>
"""
    page_name = "hub" if hub else "profile"
    page_title = "[[t:navigation.tabs.hub]]" if hub else "[[t:navigation.account.profileFallback]]"
    media_scripts = """<script src="/assets/helpers/selection-bar.js?v=__CW_VERSION__" defer></script>
<script src="/assets/helpers/card-actions.js?v=__CW_VERSION__" defer></script>
<script src="/assets/helpers/airing-card.js?v=__CW_VERSION__" defer></script>
<script src="/assets/helpers/profile-select.js?v=__CW_VERSION__" defer></script>
<script src="/assets/js/overview-profile.js?v=__CW_VERSION__" defer></script>
<script src="/assets/helpers/view-cache.js?v=__CW_VERSION__" defer></script>
<script src="/assets/helpers/media-meta.js?v=__CW_VERSION__" defer></script>
<script src="/assets/helpers/trailer.js?v=__CW_VERSION__" defer></script>
<script src="/assets/helpers/playing-card.js?v=__CW_VERSION__" defer></script>
<script src="/assets/helpers/watchlist-preview.js?v=__CW_VERSION__" defer></script>
<script src="/assets/js/dashboard-widgets.js?v=__CW_VERSION__" defer></script>
<script src="/assets/js/activity.js?v=__CW_VERSION__" defer></script>
<script src="/assets/js/item-actions.js?v=__CW_VERSION__" defer></script>
<script src="/assets/js/profile-media-modal.js?v=__CW_VERSION__" defer></script>
<script src="/assets/helpers/month-bar.js?v=__CW_VERSION__" defer></script>""" if hub else ""
    html = f"""<!DOCTYPE html>
<html lang="en" data-cw-role="{role}" data-cw-page="{page_name}" data-cw-perm-dashboard="{dashboard}" data-cw-perm-watchlist="{watchlist}" data-cw-perm-playback="{playback}" data-cw-perm-write="{write}" data-cw-profile-id="{profile_id}">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="color-scheme" content="dark light">
<title>{page_title} | CrossWatch</title>
__CW_I18N_HEAD__
<link rel="manifest" href="/manifest.webmanifest">
<link rel="apple-touch-icon" sizes="180x180" href="/assets/pwa/apple-touch-icon.png?v=__CW_VERSION__">
<meta name="application-name" content="CrossWatch">
<meta name="apple-mobile-web-app-title" content="CrossWatch">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<link rel="icon" type="image/png" sizes="64x64" href="/assets/pwa/favicon-64.png?v=__CW_VERSION__">
<meta name="theme-color" content="#090b10">
<script>
(() => {{
  try {{
    const raw = localStorage.getItem("cw.ui.theme") || "flat-dark";
    const theme = raw === "flat-light" ? "flat-light" : raw === "original" ? "original" : "flat-dark";
    if (theme === "original") delete document.documentElement.dataset.cwTheme;
    else document.documentElement.dataset.cwTheme = theme;
    document.documentElement.classList.toggle("cw-theme-light", theme === "flat-light");
    document.documentElement.classList.toggle("cw-theme-dark", theme === "flat-dark");
    document.documentElement.classList.toggle("cw-theme-original", theme === "original");
  }} catch {{
    document.documentElement.dataset.cwTheme = "flat-dark";
  }}
}})();
</script>
<link rel="stylesheet" href="/assets/themes/tokens.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/base.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/providers.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/crosswatch.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/layout.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/components.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/pages.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/page-theme.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/ui-shell.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/account-menu.css?v=__CW_VERSION__">
<link id="cw-notifications-css" rel="stylesheet" href="/assets/css/notifications.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/profile-page.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/css/ai-settings.css?v=__CW_VERSION__">
<link rel="preload" href="/assets/fonts/material-symbols-rounded-full-v355.woff2" as="font" type="font/woff2" crossorigin>
<link rel="stylesheet" href="/assets/fonts/material-symbols-rounded.css?v=__CW_VERSION__">
<link rel="stylesheet" href="/assets/js/modals/core/styles.css?v=__CW_VERSION__">
<link id="cw-theme-flat-css" rel="stylesheet" href="/assets/themes/flat.css?v=__CW_VERSION__">
<link id="cw-theme-original-css" rel="stylesheet" href="/assets/themes/original-coverage.css?v=__CW_VERSION__">
<script>
(() => {{
  const original = document.documentElement.classList.contains("cw-theme-original");
  const flatLink = document.getElementById("cw-theme-flat-css");
  const originalLink = document.getElementById("cw-theme-original-css");
  if (flatLink) {{ flatLink.disabled = original; flatLink.media = original ? "not all" : "all"; }}
  if (originalLink) {{ originalLink.disabled = !original; originalLink.media = original ? "all" : "not all"; }}
}})();
</script>
</head>
<body class="cw-profile-page{' cw-account-page' if not hub else ''}">
<header>
  <div class="brand" role="button" tabindex="0" title="[[t:navigation.brand.goHub]]" onclick="location.href='/hub'" onkeypress="if(event.key==='Enter'||event.key===' ')location.href='/hub'">
    <img class="logo" src="/assets/img/CROSSWATCH.svg?v=__CW_VERSION__" alt="CrossWatch">
    <span class="brand-text">
      <span class="name cw-wordmark">CrossWatch<span class="brand-dot" aria-hidden="true">.</span></span>
      <span class="version">__CW_CURRENT_VERSION__</span>
    </span>
  </div>
  <nav class="tabs" aria-label="[[t:navigation.primary]]">
    {profile_nav}
  </nav>
</header>
<main class="cw-profile-shell" data-profile-id="{profile_id}" data-username="{username}" data-display-name="{display_name}">
{page_content}
</main>
<div id="profile-toast" class="cw-profile-toast hidden" role="status" aria-live="polite"></div>
<div id="cw-help-overlay" class="hidden" aria-hidden="true">
  <div id="cw-help-card">
    <button id="cw-help-close" class="btn" type="button" onclick="window.cwCloseHelp()">[[t:common.actions.close]]</button>
    <iframe id="cw-help-frame" title="[[t:navigation.help.title]]" loading="lazy" referrerpolicy="no-referrer"></iframe>
  </div>
</div>
<script>
(() => {{
  const $ = id => document.getElementById(id);
  const closeMenu = id => {{
    const menu = $(`cw-${{id}}-menu`);
    const button = $(`tab-${{id}}`);
    menu?.classList.add("hidden");
    button?.setAttribute("aria-expanded", "false");
  }};
  const closeAll = () => {{ closeMenu("settings"); closeMenu("about"); closeMenu("tools"); }};
  const toggleMenu = (id, event) => {{
    event?.preventDefault?.();
    event?.stopPropagation?.();
    const menu = $(`cw-${{id}}-menu`);
    const button = $(`tab-${{id}}`);
    if (!menu || !button) return;
    const open = menu.classList.contains("hidden");
    closeAll();
    menu.classList.toggle("hidden", !open);
    button.setAttribute("aria-expanded", String(open));
  }};
  const setHelp = open => {{
    const overlay = $("cw-help-overlay");
    if (!overlay) return;
    if (open) {{
      const frame = $("cw-help-frame");
      if (frame && !frame.src) frame.src = "https://wiki.crosswatch.app";
      overlay.classList.remove("hidden");
      overlay.setAttribute("aria-hidden", "false");
    }} else {{
      overlay.classList.add("hidden");
      overlay.setAttribute("aria-hidden", "true");
    }}
  }};
  const ensureModals = async () => {{
    if (typeof window.openAbout === "function") return true;
    try {{
      const version = encodeURIComponent(String(window.APP_VERSION || window["__CW_" + "VERSION__"] || Date.now()));
      await import(`/assets/js/modals.js?v=${{version}}`);
      return typeof window.openAbout === "function";
    }} catch (error) {{
      console.warn("[profile] modals.js failed to load", error);
      return false;
    }}
  }};
  const openSettings = pane => {{
    const paths = {{
      overview: "settings",
      providers: "settings/providers",
      pairs: "settings/sync",
      sync: "settings/sync",
      scrobbler: "settings/scrobbler",
      scheduling: "settings/scheduling",
      app: "settings/app",
      maintenance: "settings/maintenance"
    }};
    window.location.href = "/#" + (paths[pane] || "settings");
  }};
  window.CW_CURRENT_VERSION = "__CW_CURRENT_VERSION__";
  window.APP_VERSION = "__CW_VERSION__";
  window["__CW_" + "VERSION__"] = window.APP_VERSION;
  window.cwOpenHelp = () => setHelp(true);
  window.cwCloseHelp = () => setHelp(false);
  window.openHelp = () => window.cwOpenHelp?.();
  window.cwToggleSettingsMenu = event => toggleMenu("settings", event);
  window.cwToggleAboutMenu = event => toggleMenu("about", event);
  window.cwSettingsMenuSelect = pane => {{ closeMenu("settings"); openSettings(pane); }};
  window.cwToggleToolsMenu = event => toggleMenu("tools", event);
  window.cwCloseToolsMenu = () => closeMenu("tools");
  window.cwToolsMenuSelect = route => {{
    closeMenu("tools");
    const prefix = $("cw-tools-menu")?.dataset.hrefPrefix || "/";
    window.location.href = `${{prefix}}#${{route}}`;
  }};
  window.cwAboutMenuSelect = async what => {{
    closeMenu("about");
    if (what === "help") window.openHelp?.();
    else if (await ensureModals()) window.openAbout?.();
  }};
  document.addEventListener("click", event => {{
    const overlay = $("cw-help-overlay");
    const card = $("cw-help-card");
    if (overlay && !overlay.classList.contains("hidden") && card && !card.contains(event.target)) window.cwCloseHelp?.();
    if (!$("tab-settings-menu")?.contains(event.target)) closeMenu("settings");
    if (!$("tab-about-menu")?.contains(event.target)) closeMenu("about");
    if (!$("tab-tools-menu")?.contains(event.target)) closeMenu("tools");
  }}, true);
  document.addEventListener("keydown", event => {{
    if (event.key === "Escape") {{ window.cwCloseHelp?.(); closeAll(); }}
  }}, true);
}})();
</script>
<script type="module" src="/assets/js/modals.js?v=__CW_VERSION__"></script>
<script src="/assets/helpers/provider-meta.js?v=__CW_VERSION__" defer></script>
<script src="/assets/helpers/clipboard.js?v=__CW_VERSION__" defer></script>
<script src="/assets/helpers/icon-select.js?v=__CW_VERSION__" defer></script>
<script src="/assets/helpers/auth-state.js?v=__CW_VERSION__" defer></script>
<script src="/assets/helpers/pwa-install.js?v=__CW_VERSION__" defer></script>
<script src="/assets/helpers/mobile-experience.js?v=__CW_VERSION__" defer></script>
<script src="/assets/helpers/account-menu.js?v=__CW_VERSION__" defer></script>
<script src="/assets/helpers/mobile-settings.js?v=__CW_VERSION__" defer></script>
<script src="/assets/helpers/notifications.js?v=__CW_VERSION__" defer></script>
<script src="/assets/js/sync-reviews.js?v=__CW_VERSION__" defer></script>
{shared_header_scripts}
<script src="/assets/helpers/api.js?v=__CW_VERSION__" defer></script>
<script src="/assets/js/notification-feed.js?v=__CW_VERSION__" defer></script>
<script>window.cwIsAuthSetupPending = window.cwIsAuthSetupPending || (() => window.__cwAuthSetupPending === true);</script>
<script src="/assets/helpers/profile-datetime.js?v=__CW_VERSION__" defer></script>
{media_scripts}
<script src="/assets/js/profile-page.js?v=__CW_VERSION__" defer></script>
<script src="/assets/js/theme-flat-runtime.js?v=__CW_VERSION__" defer></script>
</body>
</html>"""
    return _localize_page(
        _seed_media_card_style(html, user)
        .replace("__CW_CURRENT_VERSION__", CURRENT_VERSION)
        .replace("__CW_VERSION__", _asset_version_token()),
        user,
        language,
    )
