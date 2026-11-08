# tests/test_tracker_notes_bulk.py
# CrossWatch - Atomic My titles status changes, removal and undo
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import pytest

from api import trackerAPI
from cw_platform.tracker_store import TrackerStore
from test_tracker_notes import api, base, _save


@pytest.fixture
def notes(api, base):
    _save(api, "movie:tmdb:949", title="Heat", status="watching", notes="Keep this note", tags=["Crime"], started="2026-01-01")
    _save(api, "show:tmdb:95396", title="Severance", status="dropped")
    store = TrackerStore(base)
    keys = ["movie:tmdb:949", "show:tmdb:95396"]
    return store, keys, {key: store.note(key) for key in keys}


def test_bulk_status_preserves_details_and_history_and_undo_restores_every_title(api, notes):
    store, keys, before = notes
    history = store.read_current("history")
    response = api.post("/api/tracker/notes/bulk", json={"keys": keys, "action": "status", "status": "completed"})
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["count"] == 2
    for key in keys:
        assert store.note(key) == {**before[key], "status": "completed"}
    assert store.read_current("history") == history
    assert api.post("/api/tracker/undo", json={"batch": data["batch"]}).status_code == 200
    assert {key: store.note(key) for key in keys} == before


def test_bulk_remove_only_removes_my_titles_and_is_one_undoable_batch(api, notes):
    store, keys, before = notes
    history = store.read_current("history")
    response = api.post("/api/tracker/notes/bulk", json={"keys": keys, "action": "remove"})
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["count"] == 2
    assert all(store.note(key) is None for key in keys)
    assert store.read_current("history") == history
    assert api.post("/api/tracker/undo", json={"batch": data["batch"]}).status_code == 200
    assert {key: store.note(key) for key in keys} == before


def test_bulk_clear_status_keeps_notes_but_removes_empty_entries(api, notes):
    store, keys, before = notes
    response = api.post("/api/tracker/notes/bulk", json={"keys": keys, "action": "status", "status": ""})
    assert response.status_code == 200, response.text
    assert store.note(keys[0]) == {**before[keys[0]], "status": None}
    assert store.note(keys[1]) is None
    assert api.post("/api/tracker/undo", json={"batch": response.json()["batch"]}).status_code == 200
    assert {key: store.note(key) for key in keys} == before


@pytest.mark.parametrize("action", ["status", "remove"])
def test_missing_title_rolls_back_the_entire_batch(api, notes, action):
    store, keys, before = notes
    count = store.change_count()
    response = api.post("/api/tracker/notes/bulk", json={"keys": [*keys, "movie:tmdb:404"], "action": action, "status": "completed"})
    assert response.status_code == 404
    assert {key: store.note(key) for key in keys} == before
    assert store.change_count() == count


def test_repeated_keys_are_counted_once_and_unchanged_status_has_no_batch(api, notes):
    store, keys, _ = notes
    response = api.post("/api/tracker/notes/bulk", json={"keys": [keys[0], keys[0]], "action": "status", "status": "completed"})
    assert response.json()["count"] == 1
    count = store.change_count()
    response = api.post("/api/tracker/notes/bulk", json={"keys": [keys[0]], "action": "status", "status": "completed"})
    assert response.json()["count"] == 0 and response.json()["batch"] is None
    assert store.change_count() == count


@pytest.mark.parametrize("extra", [{"status": "unknown"}, {"status": None}, {"keys": []}, {"keys": ["bad"]}, {"action": "delete"}, {"keys": ["movie:tmdb:949"] * 1001}])
def test_invalid_bulk_requests_leave_notes_untouched(api, notes, extra):
    store, keys, before = notes
    response = api.post("/api/tracker/notes/bulk", json={"keys": keys, "action": "status", "status": "completed", **extra})
    assert response.status_code == 400
    assert {key: store.note(key) for key in keys} == before


def test_undo_conflict_does_not_partially_restore_other_titles(api, notes):
    store, keys, before = notes
    response = api.post("/api/tracker/notes/bulk", json={"keys": keys, "action": "remove"})
    store.save_note(keys[0], {**before[keys[0]], "notes": "Changed elsewhere"})
    undo = api.post("/api/tracker/undo", json={"batch": response.json()["batch"]})
    assert undo.status_code == 409
    assert store.note(keys[0])["notes"] == "Changed elsewhere"
    assert store.note(keys[1]) is None


def test_bulk_notes_rejects_pair_scoped_tracker(api, notes, monkeypatch):
    store, keys, before = notes
    monkeypatch.setattr(trackerAPI, "_pair_scoped", lambda: True)
    response = api.post("/api/tracker/notes/bulk", json={"keys": keys, "action": "remove"})
    assert response.status_code == 409
    assert {key: store.note(key) for key in keys} == before
