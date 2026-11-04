# tests/test_tracker_edit.py
# CrossWatch - Tracker edits, deletes, change history and undo
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import sqlite3
import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import trackerAPI
from cw_platform import tracker_store
from cw_platform.tracker_store import TrackerConflict, TrackerStore

MOVIE = {"type": "movie", "title": "Heat", "year": 1995, "ids": {"tmdb": "949", "imdb": "tt0113277"}, "watched_at": "2026-01-02T00:00:00.000Z"}
RATED = {"type": "movie", "title": "Heat", "year": 1995, "ids": {"tmdb": "949"}, "rating": 9, "rated_at": "2026-01-03T00:00:00.000Z"}
EPISODE = {
    "type": "episode", "title": "S01E02", "series_title": "Severance", "season": 1, "episode": 2,
    "ids": {}, "show_ids": {"tmdb": "95396"}, "watched_at": "2026-03-01T00:00:00.000Z",
}
ARRIVAL = {"type": "movie", "title": "Arrival", "year": 2016, "ids": {"tmdb": "329865"}, "watched_at": "2025-12-01T00:00:00.000Z"}


def _items(base, feature):
    return TrackerStore(base).read_current(feature)["items"]


@pytest.fixture()
def base(tmp_path):
    root = tmp_path / ".cw_provider"
    store = TrackerStore(root)
    store.write_state("history", {"tmdb:949": MOVIE, "tmdb:95396#s01e02": EPISODE, "tmdb:329865": ARRIVAL})
    store.write_state("ratings", {"tmdb:949": RATED})
    store.write_state("watchlist", {"tmdb:949": {"type": "movie", "title": "Heat", "ids": {"tmdb": "949"}}})
    store.write_state("progress", {"tmdb:949": {"type": "movie", "title": "Heat", "ids": {"tmdb": "949"}, "progress_percent": 40.0, "duration_ms": 10_000, "progress_ms": 4_000}})
    return root


@pytest.fixture()
def cfg(base):
    return {"crosswatch": {"root_dir": str(base)}}


@pytest.fixture()
def client(cfg, monkeypatch):
    monkeypatch.setattr(trackerAPI, "load_config", lambda: cfg)
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    app = FastAPI()
    app.include_router(trackerAPI.router)
    return TestClient(app)


def test_edit_and_undo_restore_the_record_and_log_both(base):
    store = TrackerStore(base)
    edited = {**MOVIE, "watched_at": "2026-02-02T00:00:00.000Z"}
    batch = store.change_items("history", [("tmdb:949", "tmdb:949", edited)], action="edit")
    assert _items(base, "history")["tmdb:949"]["watched_at"] == "2026-02-02T00:00:00.000Z"
    store.undo(batch)
    assert _items(base, "history")["tmdb:949"] == MOVIE
    total, log = store.change_log()
    assert total == 2
    assert [b.action for b in log] == ["undo", "edit"]
    assert log[1].undone_ns is not None and log[0].undo_of == batch
    with pytest.raises(TrackerConflict) as again:
        store.undo(batch)
    assert again.value.code == "already_undone"
    with pytest.raises(TrackerConflict) as nested:
        store.undo(log[0].batch)
    assert nested.value.code == "not_undoable"


def test_bulk_delete_is_one_change_and_one_undo(base):
    store = TrackerStore(base)
    batch = store.change_items("history", [("tmdb:949", None, None), ("tmdb:329865", None, None)], action="delete")
    assert set(_items(base, "history")) == {"tmdb:95396#s01e02"}
    assert store.change_log()[1][0].count == 2
    store.undo(batch)
    assert set(_items(base, "history")) == {"tmdb:949", "tmdb:95396#s01e02", "tmdb:329865"}


def test_missing_record_rolls_back_the_whole_batch(base):
    with pytest.raises(KeyError):
        TrackerStore(base).change_items("history", [("tmdb:949", None, None), ("tmdb:1", None, None)], action="delete")
    assert "tmdb:949" in _items(base, "history")
    assert TrackerStore(base).change_log()[0] == 0


def test_undo_refuses_when_sync_changed_the_record_since(base):
    store = TrackerStore(base)
    batch = store.change_items("ratings", [("tmdb:949", "tmdb:949", {**RATED, "rating": 7})], action="edit")
    store.write_state("ratings", {"tmdb:949": {**RATED, "rating": 5}})
    with pytest.raises(TrackerConflict) as exc:
        store.undo(batch)
    assert exc.value.code == "changed_since"
    assert _items(base, "ratings")["tmdb:949"]["rating"] == 5


def test_undo_of_delete_refuses_when_sync_added_it_back(base):
    store = TrackerStore(base)
    batch = store.change_items("history", [("tmdb:949", None, None)], action="delete")
    current = _items(base, "history")
    store.write_state("history", {**current, "tmdb:949": MOVIE})
    with pytest.raises(TrackerConflict) as exc:
        store.undo(batch)
    assert exc.value.code == "changed_since"


def test_undo_ignores_fields_the_adapter_adds_on_rewrite(base):
    store = TrackerStore(base)
    batch = store.change_items("history", [("tmdb:949", "tmdb:949", {**MOVIE, "watched_at": "2026-02-02T00:00:00.000Z"})], action="edit")
    current = _items(base, "history")
    current["tmdb:949"] = {**current["tmdb:949"], "_cw_anime_map": {"x": 1}}
    store.write_state("history", current)
    store.undo(batch)
    assert _items(base, "history")["tmdb:949"]["watched_at"] == MOVIE["watched_at"]


def test_rekey_refuses_to_overwrite_another_record(base):
    with pytest.raises(TrackerConflict) as exc:
        TrackerStore(base).change_items("history", [("tmdb:949", "tmdb:329865", MOVIE)], action="edit")
    assert exc.value.code == "key_exists"
    assert set(_items(base, "history")) == {"tmdb:949", "tmdb:95396#s01e02", "tmdb:329865"}


def test_change_log_keeps_ninety_days_and_a_count_limit(base, monkeypatch):
    store = TrackerStore(base)
    store.change_items("history", [("tmdb:949", None, None)], action="delete")
    with sqlite3.connect(base / "tracker.db") as conn:
        conn.execute("UPDATE changes SET created_ns=?", (time.time_ns() - 91 * 86_400 * 1_000_000_000,))
    store.change_items("history", [("tmdb:329865", None, None)], action="delete")
    assert store.change_log()[0] == 1
    monkeypatch.setattr(tracker_store, "CHANGE_LIMIT", 1)
    store.change_items("ratings", [("tmdb:949", None, None)], action="delete")
    total, log = store.change_log()
    assert total == 1 and log[0].feature == "ratings"


def test_clearing_the_tracker_also_clears_its_change_log(base):
    store = TrackerStore(base)
    store.change_items("history", [("tmdb:949", None, None)], action="delete")
    store.clear(state=True, snapshots=False)
    assert store.change_log()[0] == 0


def test_version_one_database_gains_the_change_log(tmp_path):
    root = tmp_path / "old"
    TrackerStore(root).write_state("history", {"tmdb:949": MOVIE})
    with sqlite3.connect(root / "tracker.db") as conn:
        conn.execute("DROP TABLE changes")
        conn.execute("PRAGMA user_version=1")
    tracker_store._READY.discard(str(root / "tracker.db"))
    store = TrackerStore(root)
    store.change_items("history", [("tmdb:949", None, None)], action="delete")
    assert store.change_log()[0] == 1


def test_deleted_record_leaves_the_sync_index(base):
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

    adapter = CROSSWATCHModule({"CrossWatch": {"connected": True, "enabled": True, "root_dir": str(base)}})
    assert "tmdb:949" in adapter.build_index("history")
    TrackerStore(base).change_items("history", [("tmdb:949", None, None)], action="delete")
    assert "tmdb:949" not in adapter.build_index("history")


def test_api_edit_normalizes_the_date_and_keeps_the_key(client, base):
    res = client.post("/api/tracker/edit", json={"feature": "history", "key": "tmdb:949", "changes": {"date": "2026-02-02T10:30:00+02:00"}})
    assert res.status_code == 200, res.text
    data = res.json()
    assert data["key"] == "tmdb:949"
    assert _items(base, "history")["tmdb:949"]["watched_at"] == "2026-02-02T08:30:00.000Z"
    assert client.post("/api/tracker/undo", json={"batch": data["batch"]}).status_code == 200
    assert _items(base, "history")["tmdb:949"]["watched_at"] == MOVIE["watched_at"]


def test_api_identity_fix_moves_the_record_to_its_new_key(client, base):
    res = client.post("/api/tracker/edit", json={"feature": "history", "key": "tmdb:949", "changes": {"ids": {"tmdb": "603", "imdb": ""}, "title": "The Matrix", "year": 1999}})
    assert res.status_code == 200, res.text
    assert res.json()["key"] == "tmdb:603"
    moved = _items(base, "history")
    assert "tmdb:949" not in moved
    assert moved["tmdb:603"]["ids"] == {"tmdb": "603"}
    assert (moved["tmdb:603"]["title"], moved["tmdb:603"]["year"]) == ("The Matrix", 1999)


def test_api_episode_show_fix_rekeys_by_show(client, base):
    res = client.post("/api/tracker/edit", json={"feature": "history", "key": "tmdb:95396#s01e02", "changes": {"show_ids": {"tmdb": "1399"}, "title": "Game of Thrones"}})
    assert res.status_code == 200, res.text
    assert res.json()["key"] == "tmdb:1399#s01e02"
    assert _items(base, "history")["tmdb:1399#s01e02"]["series_title"] == "Game of Thrones"


def test_api_rewatch_key_follows_the_new_watch_date(client, base):
    store = TrackerStore(base)
    store.write_state("history", {"tmdb:949@1767312000": MOVIE})
    res = client.post("/api/tracker/edit", json={"feature": "history", "key": "tmdb:949@1767312000", "changes": {"date": "2026-02-02T00:00:00Z"}})
    assert res.status_code == 200, res.text
    assert res.json()["key"] == "tmdb:949@1769990400"


def test_api_progress_edit_keeps_milliseconds_in_step(client, base):
    res = client.post("/api/tracker/edit", json={"feature": "progress", "key": "tmdb:949", "changes": {"progress_percent": 75}})
    assert res.status_code == 200, res.text
    row = _items(base, "progress")["tmdb:949"]
    assert (row["progress_percent"], row["progress_ms"]) == (75.0, 7_500)


@pytest.mark.parametrize(("feature", "key", "changes", "error"), [
    ("ratings", "tmdb:949", {"rating": 11}, "invalid_rating"),
    ("ratings", "tmdb:949", {"rating": 7.5}, "invalid_rating"),
    ("history", "tmdb:949", {"rating": 7}, "invalid_rating"),
    ("history", "tmdb:949", {"date": "2999-01-01T00:00:00Z"}, "invalid_date"),
    ("history", "tmdb:949", {"date": "yesterday"}, "invalid_date"),
    ("watchlist", "tmdb:949", {"date": "2026-01-01T00:00:00Z"}, "invalid_date"),
    ("history", "tmdb:949", {"ids": {"imdb": "0113277"}}, "invalid_ids"),
    ("history", "tmdb:949", {"ids": {"plex": "1"}}, "invalid_ids"),
    ("history", "tmdb:949", {"ids": {"tmdb": "", "imdb": ""}}, "invalid_ids"),
    ("history", "tmdb:949", {"show_ids": {"tmdb": "1"}}, "invalid_ids"),
    ("history", "tmdb:949", {"year": 3000}, "invalid_year"),
    ("history", "tmdb:949", {"secret": 1}, "unknown_field"),
    ("history", "tmdb:949", {"title": "Heat"}, "no_changes"),
])
def test_api_rejects_bad_edits(client, base, feature, key, changes, error):
    res = client.post("/api/tracker/edit", json={"feature": feature, "key": key, "changes": changes})
    assert res.status_code == 400
    assert res.json()["error"] == error
    assert TrackerStore(base).change_log()[0] == 0


def test_api_unknown_record_and_feature(client):
    assert client.post("/api/tracker/edit", json={"feature": "history", "key": "tmdb:1", "changes": {"title": "x"}}).status_code == 404
    assert client.post("/api/tracker/delete", json={"feature": "history", "keys": ["tmdb:1"]}).status_code == 404
    assert client.post("/api/tracker/delete", json={"keys": ["tmdb:949"]}).status_code == 400
    assert client.post("/api/tracker/undo", json={"batch": "nope"}).status_code == 404


def test_api_delete_lists_the_change_and_undo_marks_it(client, base):
    res = client.post("/api/tracker/delete", json={"feature": "history", "keys": ["tmdb:949", "tmdb:329865", "tmdb:949"]})
    assert res.status_code == 200, res.text
    assert res.json()["count"] == 2
    assert client.get("/api/tracker/summary").json()["counts"]["history"] == 1
    changes = client.get("/api/tracker/changes").json()["changes"]
    assert changes[0]["action"] == "delete" and changes[0]["count"] == 2 and changes[0]["undoable"]
    assert changes[0]["items"][0]["row"]["title"] == "Heat"
    client.post("/api/tracker/undo", json={"batch": res.json()["batch"]})
    changes = client.get("/api/tracker/changes").json()["changes"]
    assert [c["action"] for c in changes] == ["undo", "delete"]
    assert changes[1]["undone"] and not changes[1]["undoable"] and not changes[0]["undoable"]
    assert client.get("/api/tracker/summary").json()["counts"]["history"] == 3


def test_api_edit_lists_the_changed_fields(client):
    client.post("/api/tracker/edit", json={"feature": "ratings", "key": "tmdb:949", "changes": {"rating": 6}})
    item = client.get("/api/tracker/changes").json()["changes"][0]["items"][0]
    assert item["fields"] == ["rating"]
    assert (item["before"]["rating"], item["after"]["rating"]) == (9, 6)


def test_api_writes_wait_for_a_running_sync(client, monkeypatch, base):
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: True)
    for path, body in [
        ("/api/tracker/edit", {"feature": "history", "key": "tmdb:949", "changes": {"title": "x"}}),
        ("/api/tracker/delete", {"feature": "history", "keys": ["tmdb:949"]}),
        ("/api/tracker/undo", {"batch": "x"}),
    ]:
        res = client.post(path, json=body)
        assert res.status_code == 409 and res.json()["error"] == "sync_running"
    assert "tmdb:949" in _items(base, "history")


def test_summary_explains_where_a_delete_goes(client, cfg):
    cfg["pairs"] = [
        {"source": "CROSSWATCH", "target": "TRAKT", "mode": "one-way", "features": {"history": {"enable": True, "add": True, "remove": True}}},
        {"source": "PLEX", "target": "CROSSWATCH", "mode": "one-way", "features": {"history": {"enable": True, "add": True}}},
        {"source": "CROSSWATCH", "target": "SIMKL", "mode": "two-way", "features": {"ratings": {"enable": True, "add": True, "remove": False}}},
        {"source": "CROSSWATCH", "target": "MDBLIST", "enabled": False, "features": {"history": {"enable": True, "remove": True}}},
        {"source": "CROSSWATCH", "source_instance": "CW-P01", "target": "TMDB", "features": {"history": {"enable": True, "remove": True}}},
        {"source": "JELLYFIN", "target": "CROSSWATCH", "target_instance": "CW-P01", "features": {"history": {"enable": True}}},
    ]
    effects = client.get("/api/tracker/summary").json()["delete_effects"]
    assert effects["history"] == {"spreads": [{"provider": "TRAKT", "instance": "default"}], "returns": [{"provider": "PLEX", "instance": "default"}]}
    assert effects["ratings"] == {"spreads": [], "returns": [{"provider": "SIMKL", "instance": "default"}]}
    assert effects["watchlist"] == {"spreads": [], "returns": []}


def test_tracker_writes_are_admin_only():
    from api.appAuthAPI import non_admin_api_allowed

    for path in ("/api/tracker/edit", "/api/tracker/delete", "/api/tracker/undo"):
        assert not non_admin_api_allowed(path, "POST")
    assert not non_admin_api_allowed("/api/tracker/changes", "GET")


def test_tracker_page_wires_edit_delete_and_undo():
    from pathlib import Path

    js = (Path(__file__).resolve().parents[1] / "assets" / "js" / "tracker.js").read_text("utf-8")
    for endpoint in ("/api/tracker/edit", "/api/tracker/delete", "/api/tracker/undo", "/api/tracker/changes", "/api/metadata/search"):
        assert endpoint in js
    assert '"cw-ia-sheet-wrap cw-page-item-actions trk-sheet-wrap"' in js


def test_undo_notices_sync_changing_the_show_fields(base):
    store = TrackerStore(base)
    batch = store.change_items("history", [("tmdb:95396#s01e02", "tmdb:95396#s01e02", {**EPISODE, "series_title": "Edited"})], action="edit")
    current = _items(base, "history")
    current["tmdb:95396#s01e02"] = {**current["tmdb:95396#s01e02"], "series_year": 1999}
    store.write_state("history", current)
    with pytest.raises(TrackerConflict) as exc:
        store.undo(batch)
    assert exc.value.code == "changed_since"
    assert _items(base, "history")["tmdb:95396#s01e02"]["series_year"] == 1999


def test_api_edit_refuses_when_the_record_changed_after_it_was_read(client, base, monkeypatch):
    real_item = TrackerStore.item

    def stale_then_changed(self, feature, key):
        seen = real_item(self, feature, key)
        self.write_state("ratings", {"tmdb:949": {**RATED, "rating": 1}})
        return seen

    monkeypatch.setattr(TrackerStore, "item", stale_then_changed)
    res = client.post("/api/tracker/edit", json={"feature": "ratings", "key": "tmdb:949", "changes": {"title": "Heat 2"}})
    assert res.status_code == 409 and res.json()["error"] == "changed_since"
    assert _items(base, "ratings")["tmdb:949"]["rating"] == 1


def test_api_writes_are_off_for_pair_scoped_trackers(client, base, monkeypatch):
    monkeypatch.setenv("CW_CROSSWATCH_PAIR_SCOPED", "1")
    for path, body in [
        ("/api/tracker/edit", {"feature": "history", "key": "tmdb:949", "changes": {"title": "x"}}),
        ("/api/tracker/delete", {"feature": "history", "keys": ["tmdb:949"]}),
        ("/api/tracker/undo", {"batch": "x"}),
    ]:
        res = client.post(path, json=body)
        assert res.status_code == 409 and res.json()["error"] == "pair_scoped"
    assert "tmdb:949" in _items(base, "history")


def test_api_reports_a_corrupt_row_as_unreadable(client, base):
    import sqlite3

    with sqlite3.connect(base / "tracker.db") as conn:
        conn.execute("UPDATE items SET data='{bad' WHERE item_key='tmdb:949' AND feature='history'")
    res = client.get("/api/tracker/items", params={"feature": "history"})
    assert res.status_code == 503 and res.json()["error"] == "tracker_unreadable"
