# tests/test_tracker_large_selection.py
# CrossWatch - Large Tracker selections retain atomic changes and complete undo
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import pytest

from cw_platform.tracker_store import TrackerStore
from test_tracker_notes import api, base


@pytest.mark.parametrize("operation", ["delete", "status", "notes-status", "notes-remove"])
def test_large_selection_is_undoable_without_changing_unselected_rows(api, base, monkeypatch, operation):
    monkeypatch.setattr("cw_platform.tracker_store.CHANGE_LIMIT", 1000)
    monkeypatch.setattr("api.trackerAPI._sync_running", lambda: False)
    store = TrackerStore(base)
    rows = {f"tmdb:{i}": {"type": "movie", "title": f"Movie {i}", "ids": {"tmdb": str(i)}} for i in range(10000, 11302)}
    store.write_state("history", rows)
    keys = list(rows)[:-1]
    note_keys = [f"movie:{key}" for key in rows]
    store.change_notes(note_keys, status="watching", defaults={key: {"type": "movie", "title": key, "notes": "Keep"} for key in note_keys})
    notes = store.notes_for(note_keys)
    if operation == "delete":
        path, payload = "delete", {"feature": "history", "keys": keys}
    elif operation == "status":
        path, payload = "titles/status", {"keys": keys, "status": "completed"}
    else:
        path, payload = "notes/bulk", {"keys": note_keys[:-1], "action": operation.split("-")[1], "status": "completed"}

    response = api.post(f"/api/tracker/{path}", json=payload)

    assert response.status_code == 200, response.text
    assert response.json()["count"] == 1301
    assert store.item("history", list(rows)[-1]) == rows[list(rows)[-1]]
    assert store.note(note_keys[-1]) == notes[note_keys[-1]]
    if operation == "delete":
        assert len(store.read_current("history")["items"]) == 1
    elif operation == "notes-remove":
        assert len(store.notes_for(note_keys)) == 1
    else:
        changed = store.notes_for(note_keys[:-1])
        assert len(changed) == 1301
        assert all(note["status"] == "completed" and note["notes"] == "Keep" for note in changed.values())
    undone = api.post("/api/tracker/undo", json={"batch": response.json()["batch"]})
    assert undone.status_code == 200, undone.text
    assert store.read_current("history")["items"] == rows
    assert store.notes_for(note_keys) == notes
