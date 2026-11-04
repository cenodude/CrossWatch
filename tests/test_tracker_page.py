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


def test_tracker_api_is_admin_only_for_managed_users():
    from api.appAuthAPI import non_admin_api_allowed

    assert not non_admin_api_allowed("/api/tracker/items", "GET")
    assert not non_admin_api_allowed("/api/tracker/summary", "GET")


def test_tracker_page_uses_icons_and_shared_controls():
    js = (REPO / "assets" / "js" / "tracker.js").read_text("utf-8")
    assert 'class="cw-btn trk-feature"' in js
    assert "cw-page-hero" in js and "cw-page-toolbar" in js and "cw-page-panel" in js
    assert json.dumps("/api/tracker/items")[1:-1] in js
