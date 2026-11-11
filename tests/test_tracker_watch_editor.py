# tests/test_tracker_watch_editor.py
# CrossWatch - Episode watch editor snapshots, atomic changes and rewatch preservation
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import trackerAPI
from cw_platform.tracker_store import TrackerStore

SHOW = {"title": "Example show", "year": 2020, "ids": {"tmdb": "123"}}
OLD = "2025-01-01T12:00:00.000Z"
LATEST = "2025-02-01T12:00:00.000Z"
CHOSEN = "2025-03-01T12:00:00.000Z"


def episode(number, stamp):
    return {"type": "episode", "series_title": SHOW["title"], "show_ids": SHOW["ids"], "ids": {}, "season": 1, "episode": number, "watched_at": stamp}


@pytest.fixture()
def setup(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path / "tracker")
    store.write_state("history", {"tmdb:123#s01e01": episode(1, OLD)})
    monkeypatch.setattr(trackerAPI, "load_config", lambda: {"crosswatch": {"root_dir": str(store.root)}})
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    monkeypatch.setattr(trackerAPI, "_show_detail", lambda *_: {"seasons": [{"season_number": 1, "episode_count": 3, "name": "Season 1"}]})
    app = FastAPI()
    app.include_router(trackerAPI.router)
    return TestClient(app), store


def payload(store, action="add", number=2, **extra):
    return {"show": SHOW, "action": action, "revision": store.revision(), "episodes": [{"season": 1, "episode": number, "watched_at": CHOSEN}], **extra}


def post(client, store, action="add", number=2, **extra):
    return client.post("/api/tracker/history/bulk", json=payload(store, action, number, **extra))


def history(store):
    return store.read_current("history")["items"]


def add_rewatch(client, store):
    result = post(client, store, "rewatch", 1, episodes=[{"season": 1, "episode": 1, "watched_at": LATEST}])
    assert result.status_code == 200, result.text


def test_snapshot_returns_every_play_and_revision(setup):
    client, store = setup
    add_rewatch(client, store)
    data = client.get("/api/tracker/watch-editor?tmdb=123").json()
    assert data["revision"] == store.revision()
    assert sorted(p["date"] for p in data["plays"]) == [OLD, LATEST]
    assert data["seasons"] == [{"season": 1, "episodes": 3, "name": "Season 1"}]
    assert all(p["season"] == p["episode"] == 1 for p in data["plays"])


def test_add_and_unlist_are_atomic_and_undoable(setup):
    client, store = setup
    store.write_state("watchlist", {"tmdb:123": {"type": "show", **SHOW}})
    before = history(store)
    response = post(client, store, remove_from_watchlist="tmdb:123")
    assert response.status_code == 200, response.text
    assert response.json()["count"] == 1
    assert len(history(store)) == 2
    assert store.item("watchlist", "tmdb:123") is None
    store.undo(response.json()["batch"])
    assert history(store) == before
    assert store.item("watchlist", "tmdb:123") is not None


def test_rewatch_preserves_original_base_record(setup):
    client, store = setup
    original = history(store)
    add_rewatch(client, store)
    assert history(store)["tmdb:123#s01e01"] == original["tmdb:123#s01e01"]
    assert len(history(store)) == 2


@pytest.mark.parametrize("action", ["dates", "remove"])
@pytest.mark.parametrize("scope", ["latest", "all"])
def test_edit_scope_and_undo(setup, action, scope):
    client, store = setup
    add_rewatch(client, store)
    before = history(store)
    response = post(client, store, action, 1, **({"scope": scope} if scope == "all" else {}))
    assert response.status_code == 200, response.text
    dates = sorted(item["watched_at"] for item in history(store).values())
    if action == "remove":
        assert dates == ([] if scope == "all" else [OLD])
    elif scope == "latest":
        assert dates == [OLD, CHOSEN]
    else:
        times = [datetime.fromisoformat(d.replace("Z", "+00:00")) for d in dates]
        assert times[-1] == datetime(2025, 3, 1, 12, tzinfo=timezone.utc)
        assert (times[-1] - times[0]).days == 31
    store.undo(response.json()["batch"])
    assert history(store) == before


def test_changed_snapshot_and_mixed_eligibility_do_not_partially_write(setup):
    client, store = setup
    stale = payload(store)
    add_rewatch(client, store)
    before = history(store)
    assert client.post("/api/tracker/history/bulk", json=stale).status_code == 409
    response = post(client, store, episodes=[{"season": 1, "episode": n, "watched_at": CHOSEN} for n in [2, 1]])
    assert response.status_code == 409
    assert history(store) == before


def test_date_collision_rolls_back_entire_batch(setup):
    client, store = setup
    add_rewatch(client, store)
    store.write_state("history", {**history(store), "tmdb:123#s01e02": episode(2, OLD)})
    before = history(store)
    response = post(client, store, "rewatch", episodes=[{"season": 1, "episode": n, "watched_at": LATEST} for n in [2, 1]])
    assert response.status_code == 409
    assert history(store) == before


@pytest.mark.parametrize("action", ["rewatch", "dates"])
def test_duplicate_time_is_rejected_even_with_a_base_key(setup, action):
    client, store = setup
    add_rewatch(client, store)
    before = history(store)
    response = post(client, store, action, 1, episodes=[{"season": 1, "episode": 1, "watched_at": OLD}])
    assert response.status_code == 409
    assert response.json()["error"] == "duplicate_watch_times"
    assert history(store) == before


@pytest.mark.parametrize("extra", [
    {"action": []}, {"scope": {}}, {"episodes": []}, {"revision": None},
    {"episodes": [{"season": 1, "episode": 2, "watched_at": "2099-01-01T00:00:00Z"}]},
    {"episodes": [{"season": 1, "episode": 2, "watched_at": "bad"}]},
    {"episodes": [{"season": 1, "episode": 0, "watched_at": CHOSEN}]},
])
def test_invalid_payload_does_not_write(setup, extra):
    client, store = setup
    before = history(store)
    response = client.post("/api/tracker/history/bulk", json={**payload(store), **extra})
    assert response.status_code in (400, 409), response.text
    assert history(store) == before


def test_sync_in_progress_blocks_bulk_changes(setup, monkeypatch):
    client, store = setup
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: True)
    before = history(store)
    assert post(client, store).status_code == 409
    assert history(store) == before
