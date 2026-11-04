# tests/test_tracker_page.py
# CrossWatch - Tracker tab and browsing API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import trackerAPI
from cw_platform.tracker_store import TrackerStore

REPO = Path(__file__).resolve().parents[1]

MOVIE = {"type": "movie", "title": "Heat", "year": 1995, "ids": {"tmdb": "949"}, "watched_at": "2026-01-02T00:00:00Z", "rating": 9}
EPISODE = {
    "type": "episode", "title": "S01E02", "series_title": "Severance", "series_year": 2022, "season": 1, "episode": 2,
    "ids": {"tvdb": "1"}, "show_ids": {"tmdb": "95396"}, "watched_at": "2026-03-01T00:00:00Z",
}
BARE = {"type": "movie", "title": "Arrival", "ids": {"imdb": "tt2543164"}, "watched_at": "2025-12-01T00:00:00Z"}


@pytest.fixture()
def client(tmp_path, monkeypatch):
    base = tmp_path / ".cw_provider"
    TrackerStore(base).write_state("history", {"tmdb:949": MOVIE, "tmdb:95396#s01e02": EPISODE, "imdb:tt2543164": BARE})
    TrackerStore(base).write_state("ratings", {"tmdb:949": MOVIE})
    TrackerStore(base / "profiles" / "CW-P01").write_state("watchlist", {"tmdb:949": MOVIE})
    cfg = {"crosswatch": {"root_dir": str(base), "instances": {"CW-P01": {"label": "Desk"}}}}
    monkeypatch.setattr(trackerAPI, "load_config", lambda: cfg)
    app = FastAPI()
    app.include_router(trackerAPI.router)
    return TestClient(app)


def test_summary_counts_records_per_feature_and_lists_instances(client):
    data = client.get("/api/tracker/summary").json()
    assert data["counts"]["history"] == 3
    assert data["counts"]["ratings"] == 1
    assert data["counts"]["watchlist"] == 0
    assert [row["id"] for row in data["instances"]] == ["default", "CW-P01"]
    assert data["instances"][1]["label"] == "Desk"
    profile = client.get("/api/tracker/summary", params={"provider_instance": "CW-P01"}).json()
    assert profile["counts"]["watchlist"] == 1


def test_items_sort_newest_first_with_cover_ids(client):
    data = client.get("/api/tracker/items", params={"feature": "history"}).json()
    assert data["total"] == 3
    assert [row["key"] for row in data["items"]] == ["tmdb:95396#s01e02", "tmdb:949", "imdb:tt2543164"]
    episode, movie, bare = data["items"]
    assert episode["art"] == {"kind": "tv", "tmdb": "95396"}
    assert (episode["season"], episode["episode"], episode["series_title"]) == (1, 2, "Severance")
    assert movie["art"] == {"kind": "movie", "tmdb": "949"}
    assert movie["rating"] == 9
    assert bare["art"] is None


@pytest.mark.parametrize(("params", "keys"), [
    ({"q": "sever"}, ["tmdb:95396#s01e02"]),
    ({"media": "movie"}, ["tmdb:949", "imdb:tt2543164"]),
    ({"media": "show"}, ["tmdb:95396#s01e02"]),
    ({"sort": "title"}, ["imdb:tt2543164", "tmdb:949", "tmdb:95396#s01e02"]),
    ({"sort": "oldest", "offset": 1, "limit": 1}, ["tmdb:949"]),
    ({"q": "100%"}, []),
])
def test_items_filter_sort_and_page(client, params, keys):
    data = client.get("/api/tracker/items", params={"feature": "history", **params}).json()
    assert [row["key"] for row in data["items"]] == keys


def test_unknown_feature_is_rejected(client):
    assert client.get("/api/tracker/items", params={"feature": "secrets"}).status_code == 400


def test_unreadable_tracker_reports_an_error_instead_of_an_empty_list(tmp_path, monkeypatch):
    base = tmp_path / "broken"
    base.mkdir()
    (base / "history.json").write_text("{broken", "utf-8")
    monkeypatch.setattr(trackerAPI, "load_config", lambda: {"crosswatch": {"root_dir": str(base)}})
    app = FastAPI()
    app.include_router(trackerAPI.router)
    res = TestClient(app).get("/api/tracker/items", params={"feature": "history"})
    assert res.status_code == 503
    assert res.json()["error"] == "tracker_unreadable"


def test_tracker_tab_sits_between_playlists_and_editor():
    from ui_frontend import get_index_html

    html = get_index_html()
    assert html.index('id="tab-playlists"') < html.index('id="tab-tracker"') < html.index('id="tab-editor"')
    assert 'id="page-tracker"' in html
    core = (REPO / "assets" / "helpers" / "core.js").read_text("utf-8")
    assert '"/assets/js/tracker.js"' in core
    assert 'byId("page-tracker")' in core


@pytest.mark.parametrize("perms", [{"write": True, "dashboard": True}, {"write": False, "dashboard": True}])
def test_managed_users_never_get_the_tracker(perms):
    import ui_frontend

    html = ui_frontend._managed_user_shell(ui_frontend._get_index_html_static(), {"permissions": perms})
    assert 'id="tab-tracker"' not in html
    assert 'id="page-tracker"' not in html


def test_tracker_api_is_open_to_managed_users_with_write_access():
    from api.appAuthAPI import non_admin_api_allowed
    from crosswatch import _non_admin_permission_allowed

    for path in ("/api/tracker/items", "/api/tracker/summary"):
        assert non_admin_api_allowed(path, "GET")
        assert _non_admin_permission_allowed({"permissions": {"write": True}}, path, "GET")
        assert not _non_admin_permission_allowed({"permissions": {"write": False}}, path, "GET")


def test_tracker_page_uses_the_editor_layout():
    js = (REPO / "assets" / "js" / "tracker.js").read_text("utf-8")
    for markup in ('class="cw-topline cw-page-hero', 'class="cw-controls cw-page-toolbar"', 'class="cw-table-wrap cw-page-table"',
                   '<aside class="cw-side">', 'class="cw-merged-title"', 'class="cw-editor-filters"'):
        assert markup in js
    assert json.dumps("/api/tracker/items")[1:-1] in js
    assert "__cwAuthBootstrapPromise" in js


def test_tracker_shares_the_editor_styles_without_editor_only_rules():
    pages = (REPO / "assets" / "css" / "pages.css").read_text("utf-8")
    flat = (REPO / "assets" / "themes" / "flat.css").read_text("utf-8")
    assert "#page-editor,#page-editor.card,#page-tracker,#page-tracker.card{" in pages
    assert "html[data-cw-theme] #page-tracker .cw-side{width:340px" in flat
    for css in (pages, flat):
        assert "#page-tracker #cw-" not in css
        assert "#page-tracker.cw-editor-wide" not in css


def test_profile_and_tracker_share_the_month_timeline():
    import ui_frontend

    root = Path(__file__).resolve().parents[1]
    profile = (root / "assets/js/profile-page.js").read_text(encoding="utf-8")
    tracker = (root / "assets/js/tracker.js").read_text(encoding="utf-8")
    components = (root / "assets/css/components.css").read_text(encoding="utf-8")
    assert "window.CW.MonthBar.create(" in profile and "function syncZoomUi" not in profile
    assert "window.CW.MonthBar.create(" in tracker
    assert ".cw-hist-bars{" in components and ".cw-hist-tip{" in components
    assert ".cw-hist-bars{" not in (root / "assets/css/profile-page.css").read_text(encoding="utf-8")
    assert "month-bar.js" in ui_frontend.get_index_html()
    page = (root / "ui_frontend.py").read_text(encoding="utf-8")
    assert page.index("/assets/helpers/month-bar.js") < page.index("/assets/js/profile-page.js")


def test_tracker_views_are_tabs_and_paging_sits_under_the_table():
    root = Path(__file__).resolve().parents[1]
    tracker = (root / "assets/js/tracker.js").read_text(encoding="utf-8")
    pages = (root / "assets/css/pages.css").read_text(encoding="utf-8")
    controls = tracker[tracker.index("  function renderControls()"):tracker.index("  function pager()")]
    pager = tracker[tracker.index("  function pager()"):tracker.index("  function emptyState(")]
    assert '<nav class="trk-tabs" role="tablist"' in controls and 'role="tab" class="trk-tab' in tracker
    assert 'data-trk-input="size"' not in controls and 'data-trk-input="size"' in pager
    assert 'class="cw-btn cw-icon-only trk-icon-btn" data-trk-action="changes"' in controls
    assert '{ id: "show", label: "TV", icon: "tv" }' in tracker
    assert "#page-tracker .trk-toolbar{display:grid;grid-template-columns:minmax(160px,280px) minmax(0,1fr) auto" in pages


def test_every_record_and_whole_titles_can_be_edited():
    root = Path(__file__).resolve().parents[1]
    tracker = (root / "assets/js/tracker.js").read_text(encoding="utf-8")
    mapping = (root / "assets/js/interactive-sync-mapping.js").read_text(encoding="utf-8")
    assert 'data-trk-row-edit="${index}"' in tracker and 'openSheet("edit", { item, feature: listFeature() })' in tracker
    assert "data-trk-rec-edit" in tracker and "function findRecord(key, feature = \"history\")" in tracker
    assert '"/api/tracker/edit-title"' in tracker and '"title-edit": renderTitleEdit' in tracker
    assert 'scope === "title"' in tracker and "scopeNote" in mapping


def test_title_panel_sets_a_status_in_one_click():
    root = Path(__file__).resolve().parents[1]
    tracker = (root / "assets/js/tracker.js").read_text(encoding="utf-8")
    theme = (root / "assets/css/page-theme.css").read_text(encoding="utf-8")
    assert 'data-trk-detail="status-menu" aria-haspopup="menu"' in tracker and 'role="menuitemradio"' in tracker
    assert "data-trk-set-status" in tracker and "async function quickStatus(status)" in tracker
    assert "if (sheet.mode === \"title\" && handlePanelClick(event)) {\n      event.stopPropagation();" in tracker
    assert ":is(#page-tracker :is(.trk-status,.trk-status-btn,.trk-status-item),.cw-mm-trk-status)[data-status=\"dropped\"]" in theme


def test_tracker_has_my_titles_and_personal_notes():
    root = Path(__file__).resolve().parents[1]
    tracker = (root / "assets/js/tracker.js").read_text(encoding="utf-8")
    modal = (root / "assets/js/profile-media-modal.js").read_text(encoding="utf-8")
    pages = (root / "assets/css/pages.css").read_text(encoding="utf-8")
    theme = (root / "assets/css/page-theme.css").read_text(encoding="utf-8")
    assert '{ id: "titles", label: "My titles", icon: "bookmarks" }' in tracker
    assert "/api/tracker/notes?" in tracker and '"/api/tracker/note"' in tracker and '"/api/tracker/note-fields"' in tracker
    assert "notes: renderNotes, fields: renderFields" in tracker and 'data-trk-detail="complete"' in tracker
    assert 'data-mm-trk="notes"' not in modal and 'button("notes", "edit_note"' in modal
    assert "#page-tracker .trk-label{display:inline-flex" in pages and "#page-tracker .trk-label{border:" in theme
    assert '[data-status="dropped"]{--trk-status:var(--cw-theme-danger)}' in theme


def test_tracker_opens_the_shared_media_card():
    root = Path(__file__).resolve().parents[1]
    tracker = (root / "assets/js/tracker.js").read_text(encoding="utf-8")
    modal = (root / "assets/js/profile-media-modal.js").read_text(encoding="utf-8")
    mapping = (root / "assets/js/interactive-sync-mapping.js").read_text(encoding="utf-8")
    mapping_css = (root / "assets/css/interactive-sync.css").read_text(encoding="utf-8")
    assert "cardModal().open(item, { tracker: { label: instanceLabel(), data, onAction: cardAction } })" in tracker
    assert "cardModal()?.updateTracker?.(data)" in tracker
    assert "cardModal().open(item, { stacked: true })" in tracker
    assert 'data-trk-detail="card"' in tracker and "data-trk-log-info" in tracker
    assert "updateTracker" in modal and "state.stacked" in modal
    assert "card.open(preview, {host: dialog})" in mapping
    assert ".is-mapping-dialog h2:where(:not(.cw-mm *))" in mapping_css
