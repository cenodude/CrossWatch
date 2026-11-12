# tests/test_tracker_bulk_status.py
# CrossWatch - Status changes from history and show selections
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import pytest

from cw_platform.tracker_store import TrackerStore
from test_tracker_notes import api, base, EPISODE, MOVIE, _save


def test_episode_selection_changes_each_show_once_and_undo_keeps_history(api, base):
    store = TrackerStore(base)
    store.write_state("history", {"first": EPISODE, "second": {**EPISODE, "episode": 3}, "movie": MOVIE})
    _save(api, "movie:tmdb:949", title="Heat", status="watching", notes="Keep", tags=["Crime"])
    before = store.note("movie:tmdb:949")
    history = store.read_current("history")
    response = api.post("/api/tracker/titles/status", json={"keys": ["first", "second", "movie", "first"], "status": "completed"})
    assert response.status_code == 200, response.text
    assert response.json()["count"] == 2
    assert store.note("show:tmdb:95396")["title"] == "Severance"
    assert store.note("show:tmdb:95396")["status"] == "completed"
    assert store.note("movie:tmdb:949") == {**before, "status": "completed"}
    assert store.read_current("history") == history
    assert api.post("/api/tracker/undo", json={"batch": response.json()["batch"]}).status_code == 200
    assert store.note("show:tmdb:95396") is None
    assert store.note("movie:tmdb:949") == before
    assert store.read_current("history") == history


def test_show_selection_and_movie_with_same_id_have_separate_statuses(api, base):
    store = TrackerStore(base)
    store.write_state("history", {"episode": EPISODE, "movie": {**MOVIE, "ids": {"tmdb": "95396"}}})
    shows = api.get("/api/tracker/shows").json()["shows"]
    response = api.post("/api/tracker/titles/status", json={"view": "shows", "keys": [shows[0]["key"]], "status": "dropped"})
    assert response.status_code == 200, response.text
    assert store.note("show:tmdb:95396")["status"] == "dropped"
    assert store.note("movie:tmdb:95396") is None


def test_missing_record_does_not_partially_create_notes(api, base):
    store = TrackerStore(base)
    before = store.change_count()
    response = api.post("/api/tracker/titles/status", json={"keys": ["tmdb:949", "gone"], "status": "completed"})
    assert response.status_code == 404
    assert store.note("movie:tmdb:949") is None
    assert store.change_count() == before


def test_clearing_status_without_notes_excludes_automatic_updates(api, base):
    response = api.post("/api/tracker/titles/status", json={"keys": ["tmdb:949"], "status": ""})
    assert response.status_code == 200, response.text
    assert response.json()["count"] == 1
    assert TrackerStore(base).note("movie:tmdb:949") is None
    assert TrackerStore(base).status_control("movie:tmdb:949")["manual"] is True


@pytest.mark.parametrize("extra", [{"status": None}, {"status": "none"}, {"keys": []}, {"keys": [{}]}, {"view": "shows", "feature": "ratings"}])
def test_invalid_requests_leave_records_untouched(api, base, extra):
    store = TrackerStore(base)
    before = store.read_current("history")
    response = api.post("/api/tracker/titles/status", json={"keys": ["tmdb:949"], "status": "completed", **extra})
    assert response.status_code == 400
    assert store.read_current("history") == before
    assert store.note("movie:tmdb:949") is None
