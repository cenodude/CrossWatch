# tests/test_tracker_integrity.py
# CrossWatch - Tracker data stays whole: corrupt state, concurrent writers, wrong instances and atomic edits
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import trackerAPI
from cw_platform import tracker_store
from cw_platform.tracker_store import TrackerConflict, TrackerStore, TrackerUnreadable
from providers.sync._mod_CROSSWATCH import OPS

HEAT = {"type": "movie", "title": "Heat", "year": 1995, "ids": {"tmdb": "949"}, "watched_at": "2026-01-02T00:00:00.000Z"}
ARRIVAL = {"type": "movie", "title": "Arrival", "year": 2016, "ids": {"tmdb": "329865"}, "watched_at": "2026-01-03T00:00:00.000Z"}


@pytest.mark.parametrize("payload", [{"ts": 1, "items": None}, {"ts": 1, "items": "x"}, [None, "junk"]])
def test_structurally_corrupt_legacy_state_blocks_the_import(tmp_path, payload):
    (tmp_path / "history.json").write_text(json.dumps(payload), "utf-8")
    with pytest.raises(TrackerUnreadable):
        TrackerStore(tmp_path).read_state("history")
    assert (tmp_path / "history.json").exists()


def test_a_stored_value_that_is_not_a_record_makes_the_tracker_unreadable(tmp_path):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"tmdb:949": HEAT})
    assert store.write_state("history", {"tmdb:949": None}) is False
    assert store.read_current("history")["items"] == {"tmdb:949": HEAT}

    conn = sqlite3.connect(str(store.db_path))
    conn.execute("UPDATE items SET data='null' WHERE item_key='tmdb:949'")
    conn.commit()
    conn.close()
    with pytest.raises(TrackerUnreadable):
        TrackerStore(tmp_path).read_state("history")


def test_an_adapter_write_retries_when_another_writer_changed_the_tracker(tmp_path, monkeypatch):
    TrackerStore(tmp_path).write_state("history", {"tmdb:949": HEAT})
    cfg = {"crosswatch": {"root_dir": str(tmp_path)}}
    reads = {"n": 0}
    real = TrackerStore.read_state

    def read_then_edit(self, feature, **kwargs):
        found = real(self, feature, **kwargs)
        reads["n"] += 1
        if feature == "history" and reads["n"] == 1:
            outside = TrackerStore(tmp_path)
            outside.change_items("history", [("tmdb:949", "tmdb:949", {**HEAT, "title": "Heat (edited)"})], action="edit")
        return found

    monkeypatch.setattr(TrackerStore, "read_state", read_then_edit)
    res = OPS.add(cfg, [ARRIVAL], feature="history")

    assert res["ok"] is True
    items = TrackerStore(tmp_path).read_current("history")["items"]
    assert items["tmdb:949"]["title"] == "Heat (edited)"
    assert any(v.get("title") == "Arrival" for v in items.values())


def test_a_write_against_a_changed_tracker_is_refused_inside_a_guard(tmp_path):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"tmdb:949": HEAT})
    with store.guarded():
        store.read_state("history")
        other = threading.Thread(target=lambda: TrackerStore(tmp_path).write_state("history", {"tmdb:949": {**HEAT, "year": 1996}}))
        other.start()
        other.join()
        with pytest.raises(TrackerConflict):
            store.write_state("history", {"tmdb:949": HEAT, "tmdb:329865": ARRIVAL})
    assert store.write_state("history", {"tmdb:949": HEAT})


@pytest.fixture()
def api(tmp_path, monkeypatch):
    root = tmp_path / "tracker"
    TrackerStore(root).write_state("history", {"tmdb:949": HEAT, "tmdb:1": {**ARRIVAL, "ids": {"tmdb": "1"}}})
    monkeypatch.setattr(trackerAPI, "load_config", lambda: {"crosswatch": {"root_dir": str(root)}})
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    app = FastAPI()
    app.include_router(trackerAPI.router)
    return TestClient(app), root


def test_writes_to_an_unknown_instance_are_refused_instead_of_hitting_default(api):
    client, root = api
    res = client.post("/api/tracker/delete", json={"feature": "history", "provider_instance": "deleted-instance", "keys": ["tmdb:949"]})
    assert (res.status_code, res.json()["detail"]) == (404, "unknown_instance")
    assert "tmdb:949" in TrackerStore(root).read_current("history")["items"]
    assert client.get("/api/tracker/summary?provider_instance=deleted-instance").json()["provider_instance"] == "default"


def test_a_failing_note_move_rolls_back_the_record_change(api, monkeypatch):
    client, root = api
    store = TrackerStore(root)
    store.save_note("movie:tmdb:1", {"type": "movie", "title": "Arrival", "status": "completed"})
    real = TrackerStore._note_put

    def broken(conn, key, note, now):
        if key == "movie:tmdb:329865":
            raise RuntimeError("note move failed")
        return real(conn, key, note, now)

    monkeypatch.setattr(TrackerStore, "_note_put", staticmethod(broken))
    with pytest.raises(RuntimeError):
        client.post("/api/tracker/replace", json={"feature": "history", "key": "tmdb:1", "item": {"type": "movie", "title": "Arrival", "year": 2016, "ids": {"tmdb": "329865"}}})

    assert set(store.read_current("history")["items"]) == {"tmdb:949", "tmdb:1"}
    assert store.note("movie:tmdb:1")["status"] == "completed"


def test_editing_a_whole_title_reaches_every_record_past_the_display_limit(api):
    client, root = api
    plays: dict[str, Any] = {}
    for i in range(5001):
        epoch = 1700000000 + i * 3600
        when = datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        plays[f"tmdb:949@{epoch}"] = {**HEAT, "watched_at": when}
    TrackerStore(root).write_state("history", {"tmdb:949": HEAT, **plays})

    res = client.post("/api/tracker/edit-title", json={"feature": "history", "key": "tmdb:949", "changes": {"year": 1996}}).json()

    assert res["count"] == 5002
    items = TrackerStore(root).read_current("history")["items"]
    assert all(v["year"] == 1996 for v in items.values() if v["title"] == "Heat")
    tracker_store._CACHE.clear()


def test_invalid_uploads_leave_the_existing_list_alone(tmp_path):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"tmdb:949": HEAT})
    for payload in ({"ts": 1, "items": None}, {"items": [1, 2]}, {"tmdb:1": None}):
        with pytest.raises(ValueError):
            store.import_json(json.dumps(payload).encode(), "history.json")
    assert store.read_current("history")["items"] == {"tmdb:949": HEAT}

    keyed = {"items": [{"key": "tmdb:329865", **ARRIVAL}]}
    assert store.import_json(json.dumps(keyed).encode(), "history.json")["states"] == 1
    assert store.read_current("history")["items"] == {"tmdb:329865": ARRIVAL}


def test_a_malformed_snapshot_blocks_the_import_and_the_fallback(tmp_path):
    snaps = tmp_path / "snapshots"
    snaps.mkdir()
    (snaps / "20260101T000000Z-history.json").write_text(json.dumps({"ts": 1, "items": None}), "utf-8")
    with pytest.raises(TrackerUnreadable):
        TrackerStore(tmp_path).read_state("history")
    assert (snaps / "20260101T000000Z-history.json").exists()

    other = tmp_path / "db"
    store = TrackerStore(other)
    store.write_state("watchlist", {})
    conn = sqlite3.connect(str(store.db_path))
    conn.execute(
        "INSERT INTO restore_points(scope, feature, name, ts, created_ns, data) VALUES('', 'history', 'x-history.json', 1, 1, ?)",
        (json.dumps({"ts": 1, "items": None}),),
    )
    conn.commit()
    conn.close()
    with pytest.raises(TrackerUnreadable):
        TrackerStore(other).read_state("history")


def test_a_database_created_meanwhile_is_not_overwritten(tmp_path):
    store = TrackerStore(tmp_path)
    with store.guarded():
        assert store.read_state("history") is None
        other = threading.Thread(target=lambda: TrackerStore(tmp_path).write_state("history", {"tmdb:949": HEAT}))
        other.start()
        other.join()
        with pytest.raises(TrackerConflict):
            store.write_state("history", {"tmdb:329865": ARRIVAL})
    assert TrackerStore(tmp_path).read_current("history")["items"] == {"tmdb:949": HEAT}


def test_a_title_edit_refuses_records_changed_after_it_read_them(api, monkeypatch):
    client, root = api
    real = TrackerStore.related

    def read_then_change(self, **kwargs):
        rows = real(self, **kwargs)
        changed = {**HEAT, "watched_at": "2026-05-05T00:00:00.000Z"}
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("UPDATE items SET data=? WHERE feature='history' AND item_key='tmdb:949'", (tracker_store._dumps(changed),))
        conn.commit()
        conn.close()
        return rows

    monkeypatch.setattr(TrackerStore, "related", read_then_change)
    res = client.post("/api/tracker/edit-title", json={"feature": "history", "key": "tmdb:949", "changes": {"year": 1996}})

    assert (res.status_code, res.json()["error"]) == (409, "changed_since")
    item = TrackerStore(root).read_current("history")["items"]["tmdb:949"]
    assert (item["watched_at"], item["year"]) == ("2026-05-05T00:00:00.000Z", 1995)


def test_restoring_a_malformed_named_snapshot_keeps_the_current_records(tmp_path):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"tmdb:949": HEAT})
    conn = sqlite3.connect(str(store.db_path))
    conn.execute(
        "INSERT INTO restore_points(scope, feature, name, ts, created_ns, data) VALUES('', 'history', ?, 1, 1, ?)",
        ("20260101T000000Z-history.json", json.dumps({"ts": 1, "items": None})),
    )
    conn.commit()
    conn.close()

    index = OPS.build_index({"crosswatch": {"root_dir": str(tmp_path), "restore_history": "20260101T000000Z-history.json"}}, feature="history")

    assert index
    assert TrackerStore(tmp_path).read_current("history")["items"] == {"tmdb:949": HEAT}
    assert TrackerStore(tmp_path).read_meta("history.restore_state") is None
    with pytest.raises(TrackerUnreadable):
        TrackerStore(tmp_path).read_restore_point("history", "20260101T000000Z-history.json")
