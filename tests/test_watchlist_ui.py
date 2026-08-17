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
    assert 'if (!isProfileUser() && chk.checked)' in js


def test_watchlist_column_resize_can_truncate_without_overflow() -> None:
    js = (ROOT / "assets" / "js" / "watchlist.js").read_text(encoding="utf-8")
    css = (ROOT / "assets" / "css" / "pages.css").read_text(encoding="utf-8")

    assert "title:120" in js
    assert "prefs.colUser[column] = true" in js
    assert "availableWidth > minTotal" in js
    assert "text-overflow:ellipsis" in css
    assert "fillerWidth" in js
    assert "wl-fill-cell" in js
