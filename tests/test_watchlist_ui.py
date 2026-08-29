# tests/test_watchlist_ui.py
# CrossWatch - Watchlist UI regression checks

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_watchlist_toolbar_matches_editor_control_pattern() -> None:
    js = (ROOT / "assets" / "js" / "watchlist.js").read_text(encoding="utf-8")
    css = (ROOT / "assets" / "css" / "pages.css").read_text(encoding="utf-8")

    assert 'pageWrap.id = "wl-page-size"' in js
    assert 'viewField.className = "cw-page-size-control wl-toolbar-field wl-view-field"' in js
    assert "wl-toolbar-menu" in css
    assert "#page-watchlist .wl-toolbar-field" in css
    assert "#page-watchlist input[type=\"checkbox\"]" in css
    assert 'toolbar?.classList.add("cw-controls")' in js
    assert 'qEl.classList.add("cw-input", "wl-toolbar-search")' in js
    assert 'className = "cw-btn wl-btn wl-toolbar-menu wl-page-size-control"' in js
    assert "min-height:44px" in css
    assert 'columnsBtn.id = "wl-columns-btn"' in js
    assert 'wideBtn.id = "wl-wide-btn"' in js
    assert 'qEl.placeholder = "Filter by title / id / provider..."' in js
    assert "#page-watchlist .wl-toolbar-search" in css
    assert "#page-watchlist .wl-toolbar-menu-value{color:var(--wl-fg);font-size:13px;font-weight:500}" in css
    assert "#page-watchlist #wl-filter-state{appearance:none;-webkit-appearance:none;display:inline-flex;align-items:center;justify-content:center;cursor:pointer;font:inherit;font-weight:500}" in css
    assert "#page-watchlist .wl-main-shell{display:flex;flex-direction:column;gap:8px;overflow:visible}" in css
    assert "#page-watchlist.wl-wide .wl-side{display:none!important}" in css
    assert 'tr.addEventListener("click"' in js
    assert '.wl-table tbody tr.selected' in css


def test_watchlist_columns_expose_database_backed_fields() -> None:
    js = (ROOT / "assets" / "js" / "watchlist.js").read_text(encoding="utf-8")

    for column in ('"tmdb"', '"imdb"', '"tvdb"', '"trakt"', '"simkl"', '"anilist"', '"mal"', '"added"', '"key"'):
        assert column in js
    assert 'idValue(it, "tmdb")' in js
    assert 'idValue(it, "imdb")' in js
    assert "prefs.columnOrder" in js
    assert "startColumnResize" in js
    assert "applyResizeWidths" in js


def test_watchlist_repaints_art_after_metadata_hydration() -> None:
    js = (ROOT / "assets" / "js" / "watchlist.js").read_text(encoding="utf-8")

    assert "tmdbIdForArt" in js
    assert 'if (viewMode === "posters")' in js
    assert "renderPosters();" in js


def test_watchlist_retries_after_auth_bootstrap() -> None:
    js = (ROOT / "assets" / "js" / "watchlist.js").read_text(encoding="utf-8")

    assert "retryInitAfterAuth" in js
    assert "cw-auth-setup-pending" in js
    assert "initWatchlist();" in js


def test_watchlist_delete_all_option_has_no_badge() -> None:
    js = (ROOT / "assets" / "js" / "watchlist.js").read_text(encoding="utf-8")

    assert 'providerSelectOptionData(value, option, "All", false)' in js
    assert 'providerSelectOptionData(value, option, "ALL (default)", false)' in js


def test_watchlist_allows_full_managed_users_to_select_delete() -> None:
    js = (ROOT / "assets" / "js" / "watchlist.js").read_text(encoding="utf-8")

    assert 'cwPermWrite !== "on"' in js
    assert 'doc?.dataset?.cwRole === "user" && doc?.dataset?.cwPermWrite !== "on"' in js
    assert "const setFilteredSelection = checked => {" in js
    assert "if (isProfileUser()) return;" in js


def test_watchlist_filter_chip_selects_visible_items() -> None:
    js = (ROOT / "assets" / "js" / "watchlist.js").read_text(encoding="utf-8")
    css = (ROOT / "assets" / "css" / "pages.css").read_text(encoding="utf-8")

    assert '<button id="wl-filter-state" type="button"' in js
    assert "const selectedFilteredKeys = () => filtered.map(it => normKey(it)).filter(Boolean);" in js
    assert 'filterStateEl?.addEventListener("click"' in js
    assert "const actionText = readOnly" in js
    assert 'filterStateEl.textContent = actionText;' in js
    assert 'snackbar(willSelect ? "Selected visible items" : "Selection cleared");' in js
    assert "#page-watchlist #wl-filter-state:disabled" in css


def test_watchlist_column_resize_can_truncate_without_overflow() -> None:
    js = (ROOT / "assets" / "js" / "watchlist.js").read_text(encoding="utf-8")
    css = (ROOT / "assets" / "css" / "pages.css").read_text(encoding="utf-8")

    assert "title:120" in js
    assert "prefs.colUser[column] = true" in js
    assert "availableWidth > minTotal" in js
    assert "text-overflow:ellipsis" in css
    assert "fillerWidth" in js
    assert "wl-fill-cell" in js


def test_watchlist_provider_filter_lists_only_active_providers() -> None:
    js = (ROOT / "assets" / "js" / "watchlist.js").read_text(encoding="utf-8")

    assert "const visibleProviders = () => PROVIDERS.filter((p) => activeProviders.has(p));" in js
    assert "p !== \"CROSSWATCH\" || activeProviders.has(\"CROSSWATCH\")" not in js


def test_watchlist_provider_branding_uses_shared_metadata() -> None:
    meta = (ROOT / "assets" / "helpers" / "provider-meta.js").read_text(encoding="utf-8")
    js = (ROOT / "assets" / "js" / "watchlist.js").read_text(encoding="utf-8")
    css = (ROOT / "assets" / "css" / "pages.css").read_text(encoding="utf-8")
    flat = (ROOT / "assets" / "themes" / "flat.css").read_text(encoding="utf-8")

    assert 'logoFile: "NUVIO.png"' in meta
    assert 'logoFile: "STREMIO.png"' in meta
    assert "shortLabel: shortLabel(k)" in meta
    assert "logIcon: logLogoPath(k) || \"\"" in meta
    assert "tone: tone(k)" in meta
    assert "const providerBrandInfo = (value) =>" in js
    assert "const info = meta.brandInfo?.(key);" in js
    assert "const providerLogoPath = name => providerBrandInfo(name).icon || \"\";" in js
    assert '["CROSSWATCH","PLEX","JELLYFIN","EMBY","SIMKL","TRAKT","ANILIST","TMDB","MDBLIST","PUBLICMETADB","PUNCHPLAY","FLICKLIST","FLOPPY","SCROB","NUVIO","STREMIO"]' in js
    assert 'const brandClass = [brandInfo.cls, providerClass, count ? "is-live" : "is-idle"].filter(Boolean).join(" ");' in js
    assert 'brandVars.push(`--pulse-rgb:${esc(brandInfo.tone.rgb)}`);' in js
    assert 'brandVars.push(`--pulse-logo:url("${esc(src)}")`);' in js
    assert 'const brandStyle = brandVars.length ? ` style="${brandVars.join(";")}"` : "";' in js
    assert 'class="wl-provider-brand${squareClass}"' in js
    assert ".wl-provider-brand.is-square img{height:20px;max-width:22px}" in css
    assert ".wl-mat.is-square img{height:18px;max-width:18px}" in css
    assert "html[data-cw-theme] #page-watchlist .wl-provider-brand.is-square img" in flat


def test_watchlist_empty_state_is_styled_and_resettable() -> None:
    js = (ROOT / "assets" / "js" / "watchlist.js").read_text(encoding="utf-8")
    css = (ROOT / "assets" / "css" / "pages.css").read_text(encoding="utf-8")
    flat = (ROOT / "assets" / "themes" / "flat.css").read_text(encoding="utf-8")

    assert '<div id="wl-empty" class="wl-empty wl-muted" style="display:none"></div>' in js
    assert "function renderEmptyState()" in js
    assert 'filteredOut ? "No matching items" : "No watchlist items"' in js
    assert 'data-wl-empty-reset' in js
    assert 'empty.querySelector("[data-wl-empty-reset]")?.addEventListener("click", resetFilters, true);' in js
    assert 'clearBtn.addEventListener("click", resetFilters, true);' in js
    assert ".wl-empty{display:grid;place-items:center;align-content:center;" in css
    assert ".wl-empty-icon{display:grid;place-items:center;" in css
    assert ".wl-empty-copy strong" in css
    assert 'html[data-cw-theme="flat-light"] #page-watchlist .wl-empty' in flat


def test_watchlist_profile_filter_covers_every_provider() -> None:
    js = (ROOT / "assets" / "js" / "watchlist.js").read_text(encoding="utf-8")

    assert "crosswatchProfiles" not in js
    assert 'mkFilterControl("wl-profile", "Profile", providerSel)' in js
    assert "loadProviderInstances" in js
    assert '"/api/provider-instances"' in js
    assert "const filterOn = !!provider && instancesFor(provider).length > 0;" in js
    assert "const insts = instancesOfProvider(it, provider);" in js


def test_watchlist_user_profile_filter_scopes_the_fetch() -> None:
    js = (ROOT / "assets" / "js" / "watchlist.js").read_text(encoding="utf-8")
    css = (ROOT / "assets" / "css" / "pages.css").read_text(encoding="utf-8")

    assert 'mkFilterControl("wl-user-profile", "User profile", profileSel)' in js
    assert "const usersOn = isAdminViewer() && userProfiles.length > 0;" in js
    assert "user_profile=${encodeURIComponent(appliedUserProfile)}" in js
    assert "applyUserProfileScope" in js
    assert "#page-watchlist .wl-profile-select" in css


def test_watchlist_user_profile_follows_the_global_view_as_picker() -> None:
    js = (ROOT / "assets" / "js" / "watchlist.js").read_text(encoding="utf-8")

    assert "const overviewProfile = () => window.CW?.OverviewProfile || null;" in js
    assert "const shellProfileId = () => document.documentElement?.dataset?.cwRole === \"user\"" in js
    assert 'const effectiveUserProfile = () => activeUserProfile || (userProfileTouched ? "" : globalUserProfile());' in js
    assert "try { await op?.ready; } catch (_) {}" in js
    assert 'window.addEventListener("cw:overview-profile-changed"' in js
    assert "effectiveUserProfile() !== appliedUserProfile" in js
    assert "userProfileTouched = true;" in js


def test_watchlist_page_uses_payload_and_derived_metadata_cache() -> None:
    js = (ROOT / "assets" / "js" / "watchlist.js").read_text(encoding="utf-8")

    assert 'const DATA_CACHE_KEY = "cw.watchlist.page.v2";' in js
    assert 'const DERIVED_CACHE_KEY = "cw.watchlist.derived.v1";' in js
    assert "const readWatchlistCache = () =>" in js
    assert "const writeWatchlistCache = payload =>" in js
    assert "const clearWatchlistCache = () =>" in js
    assert "function applyWatchlistPayload(payload)" in js
    assert "if (!payload || !Array.isArray(payload.items)) return false;" in js
    assert 'known_version=${encodeURIComponent(version)}' in js
    assert "if (j?.not_modified && Array.isArray(cached?.items)) return cached;" in js
    assert "else clearWatchlistCache();" in js
    assert "revealWatchlistCache();" in js
    assert "loadDerivedCache();" in js
    assert "scheduleDerivedCacheSave();" in js
    assert "const derivedKeyFor = it =>" in js
    assert "function rememberDerived(k, value, persist = false)" in js
    assert 'const derivedCacheScope = () => "watchlist-row-meta:v2";' in js
    assert "const hasStableDerivedText = it =>" in js
    assert "targets = targets.filter((it) => !hasStableDerivedText(it));" in js
    assert "if (d.relFmt || d.genresText) hydrateRow(it, tr);" not in js
    init = js.split("async function initWatchlist()", 1)[1]
    before_config = init.split("const cfg = await fetchConfig();", 1)[0]
    assert "activeUserProfile = String(prefs.userProfile || \"\").trim();" in before_config
    assert "loadDerivedCache();" in before_config
    assert "revealWatchlistCache();" in before_config


def test_watchlist_page_omits_local_visibility_actions() -> None:
    js = (ROOT / "assets" / "js" / "watchlist.js").read_text(encoding="utf-8")

    assert "Hide local" not in js
    assert "Unhide all" not in js
    assert "Local view only" not in js
    assert "wl-show-hidden" not in js
    assert 'id="wl-hide"' not in js
    assert 'id="wl-unhide"' not in js
    assert "hiddenSet" not in js
