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
from tests.i18n_source import english_source

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
    assert data["instances"][0]["label"] == "Default"
    assert data["instances"][1]["label"] == "Desk"
    trackerAPI.load_config()["crosswatch"]["label"] = "My tracker"
    renamed = client.get("/api/tracker/summary").json()
    assert renamed["instances"][0] == {"id": "default", "label": "My tracker"}
    sources = client.get("/api/tracker/import/sources").json()
    assert sources["instances"][0] == {"id": "default", "label": "My tracker"}
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


def test_grouped_movies_groups_rewatches_and_keeps_title_details(client):
    store = TrackerStore(Path(trackerAPI.load_config()["crosswatch"]["root_dir"]))
    store.write_state("history", {
        "tmdb:949@first": MOVIE,
        "tmdb:949@latest": {**MOVIE, "watched_at": "2026-11-01T12:00:00Z"},
        "tmdb:949": {key: value for key, value in MOVIE.items() if key != "watched_at"},
        "imdb:tt2543164": BARE,
        "tmdb:95396#s01e02": EPISODE,
    })
    data = client.get("/api/tracker/history/titles", params={"media": "movie"}).json()
    assert data["total"] == 2
    movie = data["items"][0]
    assert movie["key"] == "tmdb:949@latest"
    assert movie["title_key"] == "movie:tmdb:949"
    assert movie["plays"] == 3
    assert movie["date"] == "2026-11-01T12:00:00Z"
    assert movie["user_rating"] == 9
    detail = client.get("/api/tracker/title", params={"key": movie["key"], "feature": "history"}).json()
    assert detail["ok"] is True
    preview = client.post("/api/tracker/titles/remove", json={"keys": [movie["title_key"]], "scope": "history"}).json()
    assert preview["counts"]["history"] == 3
    assert client.get("/api/tracker/items").json()["total"] == 5


@pytest.mark.parametrize(("params", "title"), [
    ({"sort": "title", "limit": 1}, "Arrival"),
    ({"sort": "title", "limit": 1, "offset": 1}, "Heat"),
    ({"sort": "oldest", "limit": 1}, "Arrival"),
    ({"q": "HEAT"}, "Heat"),
])
def test_grouped_movies_filters_and_pages_titles(client, params, title):
    data = client.get("/api/tracker/history/titles", params={"media": "movie", **params}).json()
    assert [row["title"] for row in data["items"]] == [title]
    assert data["total"] == (1 if "q" in params else 2)


def test_grouped_movies_status_tags_and_instance_scope(client):
    store = TrackerStore(Path(trackerAPI.load_config()["crosswatch"]["root_dir"]))
    store.save_note("movie:tmdb:949", {"title": "Heat", "status": "completed", "tags": ["Favorite"]})
    data = client.get("/api/tracker/history/titles", params={"media": "movie", "status": "completed", "tag": "Favorite"}).json()
    assert data["total"] == 1
    assert data["items"][0]["status"] == "completed"
    assert client.get("/api/tracker/history/titles", params={"media": "movie", "status": "none"}).json()["total"] == 1
    assert client.get("/api/tracker/history/titles", params={"media": "movie", "status": "invalid"}).status_code == 400
    assert client.get("/api/tracker/history/titles", params={"media": "movie", "provider_instance": "CW-P01"}).json()["total"] == 0


def test_grouped_movies_keeps_unknown_dates_and_refreshes_after_writes(client):
    store = TrackerStore(Path(trackerAPI.load_config()["crosswatch"]["root_dir"]))
    assert client.get("/api/tracker/history/titles", params={"media": "movie"}).json()["total"] == 2
    unknown = {key: value for key, value in MOVIE.items() if key != "watched_at"}
    store.write_state("history", {"tmdb:949": unknown})
    data = client.get("/api/tracker/history/titles", params={"media": "movie"}).json()
    assert data["total"] == 1
    assert data["items"][0]["plays"] == 1
    assert not data["items"][0]["date"]
    store.write_state("history", {})
    assert client.get("/api/tracker/history/titles", params={"media": "movie"}).json()["items"] == []


def test_grouped_history_combines_movies_and_series_with_stable_paging(client):
    store = TrackerStore(Path(trackerAPI.load_config()["crosswatch"]["root_dir"]))
    store.write_state("history", {
        "tmdb:949": MOVIE,
        "imdb:tt2543164": BARE,
        "tmdb:95396#s01e02": EPISODE,
        "tmdb:95396#s01e02@rewatch": {**EPISODE, "watched_at": "2026-05-01T00:00:00Z"},
        "tmdb:95396#s01e03": {**EPISODE, "episode": 3, "title": "S01E03"},
    })
    store.write_state("ratings", {"tmdb:95396": {"type": "show", "title": "Severance", "ids": {"tmdb": "95396"}, "rating": 8}})
    data = client.get("/api/tracker/history/titles").json()
    assert data["total"] == 3
    show = data["items"][0]
    assert (show["title"], show["type"], show["plays"]) == ("Severance", "show", 3)
    assert show["season"] is None and show["episode"] is None
    assert show["user_rating"] == 8
    assert show["ids"] == {"tmdb": "95396"}
    assert show["title_key"] == "show:tmdb:95396"
    assert show["key"] == "tmdb:95396#s01e02@rewatch"
    matched = client.get("/api/tracker/history/titles", params={"q": "S01E03"}).json()
    assert matched["total"] == 1
    assert matched["items"][0]["key"] == show["key"]
    assert matched["items"][0]["plays"] == 3
    assert client.get("/api/tracker/title", params={"key": show["key"]}).json()["show"] is True
    pages = [client.get("/api/tracker/history/titles", params={"sort": "title", "offset": n, "limit": 1}).json() for n in range(3)]
    assert [page["items"][0]["title"] for page in pages] == ["Arrival", "Heat", "Severance"]
    assert client.get("/api/tracker/history/titles", params={"media": "show"}).json()["total"] == 1
    assert client.get("/api/tracker/history/titles", params={"media": "invalid"}).status_code == 400
    preview = client.post("/api/tracker/titles/remove", json={"keys": [show["title_key"], "movie:tmdb:949"], "scope": "history"}).json()
    assert preview["counts"]["history"] == 4


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


def test_tracker_tab_sits_between_main_and_hub():
    from ui_frontend import get_index_html

    html = get_index_html()
    assert html.index('id="tab-main"') < html.index('id="tab-tracker"') < html.index('id="tab-profile"')
    assert 'id="page-tracker"' in html
    core = english_source((REPO / "assets" / "helpers" / "core.js").read_text("utf-8"))
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
    js = english_source((REPO / "assets" / "js" / "tracker.js").read_text("utf-8"))
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
    profile = english_source((root / "assets/js/profile-page.js").read_text(encoding="utf-8"))
    tracker = english_source((root / "assets/js/tracker.js").read_text(encoding="utf-8"))
    components = (root / "assets/css/components.css").read_text(encoding="utf-8")
    assert "window.CW.MonthBar.create(" in profile and "function syncZoomUi" not in profile
    assert "window.CW.MonthBar.create(" in tracker
    assert ".cw-hist-bars{" in components and ".cw-hist-tip{" in components
    assert ".cw-hist-bars{" not in (root / "assets/css/profile-page.css").read_text(encoding="utf-8")
    assert "month-bar.js" in ui_frontend.get_index_html()
    page = english_source((root / "ui_frontend.py").read_text(encoding="utf-8"))
    assert page.index("/assets/helpers/month-bar.js") < page.index("/assets/js/profile-page.js")


def test_tracker_views_are_tabs_and_paging_sits_under_the_table():
    root = Path(__file__).resolve().parents[1]
    tracker = english_source((root / "assets/js/tracker.js").read_text(encoding="utf-8"))
    pages = (root / "assets/css/pages.css").read_text(encoding="utf-8")
    controls = tracker[tracker.index("  function renderControls()"):tracker.index("  function pager()")]
    navigation = tracker[tracker.index("  function renderNavigation()"):tracker.index("  function overviewRows(")]
    pager = tracker[tracker.index("  function pager()"):tracker.index("  function emptyState(")]
    assert 'renderNavigation()' in controls and '<nav class="trk-tabs" role="tablist"' in navigation and 'role="tab" class="trk-tab' in tracker
    assert 'data-trk-input="size"' not in controls and 'data-trk-input="size"' in pager
    assert "['changes','manage_history',\"Changes\"]" in controls
    assert 'class="cw-btn trk-icon-btn" data-trk-action="${action}"' in controls
    assert '{ id: "show", label: "TV", icon: "tv" }' in tracker
    assert "#page-tracker .trk-toolbar{display:grid;" in pages


def test_every_record_and_whole_titles_can_be_edited():
    root = Path(__file__).resolve().parents[1]
    tracker = english_source((root / "assets/js/tracker.js").read_text(encoding="utf-8"))
    mapping = english_source((root / "assets/js/interactive-sync-mapping.js").read_text(encoding="utf-8"))
    assert 'data-trk-row-edit="${index}"' in tracker and 'openSheet("edit", { item, feature: listFeature() })' in tracker
    assert "data-trk-rec-edit" in tracker and "function findRecord(key, feature = \"history\")" in tracker
    assert '"/api/tracker/edit-title"' in tracker and '"title-edit": renderTitleEdit' in tracker
    assert 'scope === "title"' in tracker and "scopeNote" in mapping


def test_title_panel_sets_a_status_in_one_click():
    root = Path(__file__).resolve().parents[1]
    tracker = english_source((root / "assets/js/tracker.js").read_text(encoding="utf-8"))
    theme = (root / "assets/css/page-theme.css").read_text(encoding="utf-8")
    assert 'data-trk-detail="status-menu" aria-haspopup="menu"' in tracker and 'role="menuitemradio"' in tracker
    assert "data-trk-set-status" in tracker and "async function quickStatus(status)" in tracker
    assert "if (sheet.mode === \"title\" && handlePanelClick(event)) {\n      event.stopPropagation();" in tracker
    assert ":is(:is(#page-tracker,.cw-mm-trk) :is(.trk-status,.trk-status-btn,.trk-status-item),.cw-mm-trk-status)[data-status=\"dropped\"]" in theme


def test_tracker_has_my_titles_and_personal_notes():
    root = Path(__file__).resolve().parents[1]
    tracker = english_source((root / "assets/js/tracker.js").read_text(encoding="utf-8"))
    modal = english_source((root / "assets/js/profile-media-modal.js").read_text(encoding="utf-8"))
    pages = (root / "assets/css/pages.css").read_text(encoding="utf-8")
    theme = (root / "assets/css/page-theme.css").read_text(encoding="utf-8")
    assert '{ id: "titles", label: "My titles", icon: "bookmarks" }' in tracker
    assert "/api/tracker/notes?" in tracker and '"/api/tracker/note"' in tracker and '"/api/tracker/note-fields"' in tracker
    assert "notes: renderNotes, fields: renderFields" in tracker and 'data-trk-detail="complete"' in tracker
    assert 'data-mm-trk="notes">${icon("add")}<span>Add notes</span>' in modal
    assert 'data-mm-trk="notes" title="Edit notes" aria-label="Edit notes"' in modal
    assert "#page-tracker .trk-label{display:inline-flex" in pages and "#page-tracker .trk-label{border:" in theme
    assert '[data-status="dropped"]{--trk-status:var(--cw-theme-danger)}' in theme


def test_tracker_opens_the_shared_media_card():
    root = Path(__file__).resolve().parents[1]
    tracker = english_source((root / "assets/js/tracker.js").read_text(encoding="utf-8"))
    modal = english_source((root / "assets/js/profile-media-modal.js").read_text(encoding="utf-8"))
    mapping = english_source((root / "assets/js/interactive-sync-mapping.js").read_text(encoding="utf-8"))
    mapping_css = (root / "assets/css/interactive-sync.css").read_text(encoding="utf-8")
    assert "cardModal().open(item, { tracker: { label: instanceLabel(), data, onAction: cardAction } })" in tracker
    assert "cardModal()?.updateTracker?.(data)" in tracker
    assert "cardModal().open(item, { stacked: true })" in tracker
    assert 'data-trk-detail="card"' in tracker and "data-trk-log-info" in tracker
    assert "updateTracker" in modal and "state.stacked" in modal
    assert "card.open(preview, {host: dialog})" in mapping
    assert ".is-mapping-dialog h2:where(:not(.cw-mm *))" in mapping_css
