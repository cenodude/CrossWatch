# tests/test_tracker_automation.py
# CrossWatch - Local status ownership, inference, queues and title removal
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import io
import sqlite3
import time

import pytest

from cw_platform.tracker_store import TrackerStore, TrackerConflict
from cw_platform.tracker_automation import instance_options
from services import tracker_automation as worker, tracker_month
from test_tracker_notes import api, base, EPISODE, MOVIE

SHOW = "show:tmdb:95396"
FILM = "movie:tmdb:949"
DETAIL = {"id": 95396, "status": "Ended", "seasons": [{"season_number": 1, "episode_count": 2}]}


def seeded(tmp_path):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"one": {**EPISODE, "episode": 1}, "two": EPISODE, "movie": MOVIE})
    return store


def test_local_rules_distinct_episodes_and_noop_revision(tmp_path):
    store = seeded(tmp_path)
    assert len(store.pending_statuses()) == 2
    assert store.evaluate_status(FILM)["after"] == "completed"
    assert store.evaluate_status(SHOW)["after"] == "watching"
    assert store.evaluate_status(SHOW, DETAIL)["after"] == "completed"
    rev = store.revision()
    assert "batch" not in store.evaluate_status(SHOW, DETAIL)
    assert store.revision() == rev
    store.change_items("history", [("one", None, None)], action="delete")
    assert store.evaluate_status(SHOW, DETAIL)["after"] == "watching"
    store.change_items("history", [("two", None, None)], action="delete")
    assert store.evaluate_status(SHOW, DETAIL)["after"] is None


@pytest.mark.parametrize("detail", [None, {**DETAIL, "status": "Returning Series"}, {**DETAIL, "next_episode_to_air": {"episode_number": 3}}, {**DETAIL, "number_of_episodes": 3}, {**DETAIL, "number_of_seasons": 2}])
def test_no_completion_without_reliable_finished_series(tmp_path, detail):
    store = seeded(tmp_path)
    assert store.evaluate_status(SHOW, detail)["after"] == "watching"


def test_manual_same_value_clear_sync_and_restart_remain_protected(tmp_path):
    store = seeded(tmp_path)
    store.evaluate_status(SHOW)
    batch = store.save_note(SHOW, store.note(SHOW), origin="manual")
    assert batch
    assert store.evaluate_status(SHOW, DETAIL)["skipped"] == "protected"
    store.save_note(SHOW, None, origin="manual")
    store = TrackerStore(tmp_path)
    assert store.evaluate_status(SHOW, DETAIL)["skipped"] == "protected"
    store.save_note(SHOW, {"type": "show", "status": "watching"}, origin="sync")
    assert store.status_control(SHOW)["manual"] is True
    store.enable_title_automation(SHOW)
    assert store.evaluate_status(SHOW, DETAIL)["after"] == "completed"


def test_legacy_status_is_unknown_and_sync_is_not_manual(tmp_path):
    store = seeded(tmp_path)
    store.save_note(SHOW, {"type": "show", "status": "on_hold"})
    assert store.evaluate_status(SHOW, DETAIL)["skipped"] == "protected"
    store.save_note(FILM, {"type": "movie", "status": "watching"}, origin="sync")
    assert store.status_control(FILM) == {"origin": "sync", "manual": False}
    assert store.evaluate_status(FILM)["after"] == "completed"
    assert store.status_control(SHOW) == {"origin": "existing", "manual": False}


def test_queue_deduplicates_history_ignores_progress_and_disabled_writes(tmp_path):
    store = seeded(tmp_path)
    initial = store.pending_statuses()
    store.write_state("progress", {"one": EPISODE})
    assert store.pending_statuses() == initial
    store.write_state("history", {"one": {**EPISODE, "episode": 1}, "two": EPISODE, "movie": MOVIE})
    assert store.pending_statuses() == initial
    store.configure_automation(False)
    store.write_state("history", {"one": EPISODE})
    assert store.pending_statuses() == []
    store.configure_automation(True)
    store.write_state("history", {"one": EPISODE, "again": EPISODE})
    assert len(store.pending_statuses()) == 1
    assert store.evaluate_status(SHOW, DETAIL)["after"] == "watching"


def test_stale_work_and_preview_do_not_write(tmp_path):
    store = seeded(tmp_path)
    rev = store.revision()
    assert store.evaluate_status(SHOW, DETAIL, preview=True)["after"] == "completed"
    assert store.revision() == rev and store.note(SHOW) is None
    assert store.evaluate_status(SHOW, DETAIL, version="old")["skipped"] == "changed"


def test_manual_protection_undo_and_export_restore(tmp_path):
    store = seeded(tmp_path / "a")
    store.evaluate_status(FILM)
    batch = store.save_note(FILM, store.note(FILM), origin="manual")
    store.undo(batch)
    assert store.status_control(FILM)["origin"] == "automatic"
    store.save_note(FILM, None, origin="manual")
    other = TrackerStore(tmp_path / "b")
    other.import_zip(io.BytesIO(store.export_zip()))
    assert other.status_control(FILM)["manual"] is True


def test_bulk_empty_status_records_each_manual_exclusion(tmp_path):
    store = seeded(tmp_path)
    batch, count = store.change_notes([SHOW, FILM], status="", origin="manual", defaults={SHOW: {"type": "show"}, FILM: {"type": "movie"}})
    assert count == 2
    total, changes = store.change_log(kind="notes")
    assert total == 1 and changes[0].count == 2 and changes[0].feature == "notes"
    store.undo(batch)
    assert store.status_control(SHOW)["manual"] is False


def test_worker_rotates_between_busy_trackers(tmp_path, monkeypatch):
    first, second = seeded(tmp_path / "first"), seeded(tmp_path / "second")
    first.write_state("history", {str(n): {**MOVIE, "ids": {"tmdb": str(n)}} for n in range(1, 100)})
    monkeypatch.setattr(worker, "tracker_roots", lambda *args: [("default", first.root), ("other", second.root)])
    monkeypatch.setattr(worker, "busy", lambda: False)
    monkeypatch.setattr(worker, "cached_metadata", lambda key: DETAIL)
    monkeypatch.setattr(worker, "_NEXT_ROOT", 0)
    worker.run_once({}, tmp_path)
    assert second.note(FILM) is None
    worker.run_once({}, tmp_path)
    assert second.note(FILM)["status"] == "completed"


def test_title_removal_is_complete_atomic_conflict_checked_and_undoable(tmp_path):
    store = seeded(tmp_path)
    history = {f"ep:{n}": {**EPISODE, "episode": n + 1} for n in range(5002)}
    history["movie"] = MOVIE
    store.write_state("history", history)
    store.write_state("ratings", {"show": {"type": "show", "ids": {"tmdb": "95396"}, "rating": 8}})
    store.save_note(SHOW, {"type": "show", "status": "dropped", "notes": "Keep on undo"}, origin="manual")
    preview = store.title_removal([SHOW], True)
    assert preview["counts"] == {"history": 5002, "ratings": 1, "notes": 1}
    with pytest.raises(TrackerConflict):
        store.title_removal([SHOW], True, expected="stale")
    result = store.title_removal([SHOW], True, expected=preview["token"])
    assert store.read_current("history")["items"] == {"movie": MOVIE}
    assert store.note(SHOW) is None
    assert all(k != SHOW for k, _ in store.pending_statuses())
    store.undo(result["batch"])
    assert store.read_current("history")["items"] == history
    assert store.note(SHOW)["status"] == "dropped"
    assert store.status_control(SHOW)["manual"] is True


def test_worker_disabled_and_busy_skip_metadata_and_instances_are_independent(tmp_path, monkeypatch):
    store = seeded(tmp_path / "tracker")
    cfg = {"crosswatch": {"automatic_status": False, "month_review": False, "instances": {"other": {}}}}
    assert instance_options(cfg, "other") == {"automatic_status": True, "month_review": True}
    monkeypatch.setattr(worker, "tracker_roots", lambda *args: [("default", store.root)])
    monkeypatch.setattr(worker, "cached_metadata", lambda *args: pytest.fail("metadata should not be read"))
    worker.run_once(cfg, tmp_path)
    assert store.pending_statuses() == []
    store.configure_automation(True)
    store.queue_statuses([SHOW])
    monkeypatch.setattr(worker, "busy", lambda: True)
    worker.run_once({}, tmp_path)
    assert store.note(SHOW) is None


def test_history_removal_and_automatic_status_undo_together(tmp_path):
    store = seeded(tmp_path)
    store.evaluate_status(SHOW, DETAIL)
    before = store.note(SHOW)
    preview = store.title_removal([SHOW])
    assert preview["counts"] == {"history": 2, "statuses": 1}
    result = store.title_removal([SHOW], expected=preview["token"])
    assert store.note(SHOW) is None
    store.undo(result["batch"])
    assert store.note(SHOW) == before
    assert len(store.read_current("history")["items"]) == 3


def test_history_removal_keeps_ui_override(tmp_path):
    store = seeded(tmp_path)
    store.save_note(SHOW, {"type": "show", "status": "dropped"}, origin="manual")
    preview = store.title_removal([SHOW])
    store.title_removal([SHOW], expected=preview["token"])
    assert store.note(SHOW)["status"] == "dropped"
    assert store.status_control(SHOW)["manual"] is True


def test_disabled_month_reports_do_not_schedule_or_read_reports(tmp_path, monkeypatch):
    store = seeded(tmp_path)
    cfg = {"crosswatch": {"month_review": False}}
    monkeypatch.setattr(tracker_month, "_saved", lambda *args: pytest.fail("report read"))
    assert tracker_month.get_report(store, cfg)["disabled"] is True
    assert tracker_month.available_month(store, cfg) == ""
    assert not (tmp_path / "month-reviews.db").exists()
    cfg = {"crosswatch": {"month_review": True, "instances": {"other": {"root_dir": str(tmp_path), "month_review": False}}}}
    assert tracker_month.get_report(store, cfg)["disabled"] is True


def test_api_removal_preview_and_manual_status_lock(api, base, monkeypatch):
    monkeypatch.setattr("api.trackerAPI._sync_running", lambda: False)
    first = api.post("/api/tracker/titles/remove", json={"keys": [SHOW], "scope": "history"}).json()
    assert first["count"] == 1
    assert TrackerStore(base).read_current("history")["items"]
    assert api.post("/api/tracker/titles/remove", json={"keys": [SHOW], "scope": "history", "token": first["token"]}).status_code == 200
    api.post("/api/tracker/note", json={"key": FILM, "status": "", "status_manual": True})
    assert TrackerStore(base).status_control(FILM)["manual"] is True


def test_status_queries_use_title_index(tmp_path):
    store = seeded(tmp_path)
    where, args = store._status_where(SHOW)
    with sqlite3.connect(store.db_path) as conn:
        plan = conn.execute("EXPLAIN QUERY PLAN SELECT DISTINCT season,episode FROM items WHERE scope='' AND feature='history' AND " + where, args).fetchall()
    assert any("items_show" in row[3] for row in plan)


def test_worker_budget_and_instance_isolation(tmp_path, monkeypatch):
    first, second = seeded(tmp_path / "first"), seeded(tmp_path / "second")
    first.write_state("history", {str(n): {**MOVIE, "ids": {"tmdb": str(n)}} for n in range(1, 40)})
    monkeypatch.setattr(worker, "tracker_roots", lambda *args: [("default", first.root), ("other", second.root)])
    monkeypatch.setattr(worker, "busy", lambda: False)
    calls = []
    monkeypatch.setattr(worker, "cached_metadata", lambda key: calls.append(key))
    pending = len(first.pending_statuses(100))
    worker.run_once({"crosswatch": {"instances": {"other": {"automatic_status": False}}}}, tmp_path)
    assert len(first.pending_statuses(100)) == pending - 25
    assert calls == []
    assert second.note(FILM) is None
    assert second.pending_statuses() == []
    second.configure_automation(True)
    second.queue_statuses([FILM, SHOW])
    worker.run_once({"crosswatch": {"automatic_status": False}}, tmp_path)
    assert second.note(FILM)["status"] == "completed"
    assert first.pending_statuses() == []


def test_manual_titles_never_read_metadata(tmp_path):
    store = seeded(tmp_path)
    store.save_note(SHOW, {"type": "show", "status": "on_hold"}, origin="manual")
    assert store.evaluate_status(SHOW, lambda: pytest.fail("Manual titles must not read metadata"))["skipped"] == "protected"


def test_month_report_checkpoint_stops_after_disable(tmp_path, monkeypatch):
    monkeypatch.setattr(tracker_month, "_busy", lambda: False)
    monkeypatch.setattr(tracker_month, "_LOAD_CONFIG", lambda: {"crosswatch": {"root_dir": str(tmp_path), "month_review": False}})
    monkeypatch.setattr(tracker_month._WORK, "root", tmp_path, raising=False)
    monkeypatch.setattr(tracker_month._WORK, "checked", 0, raising=False)
    with pytest.raises(InterruptedError, match="monthly_review_disabled"):
        tracker_month._checkpoint()


def test_specials_and_rewatches_cannot_complete_regular_seasons(tmp_path):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"special": {**EPISODE, "season": 0}})
    assert store.evaluate_status(SHOW, DETAIL)["after"] is None
    store.write_state("history", {"one": EPISODE, "rewatch": {**EPISODE, "watched_at": "2026-11-01T12:00:00Z"}})
    assert store.evaluate_status(SHOW, DETAIL)["after"] == "watching"


def test_synced_status_without_history_is_retained(tmp_path):
    store = TrackerStore(tmp_path)
    store.save_note(FILM, {"type": "movie", "status": "planned"}, origin="sync")
    assert store.evaluate_status(FILM)["after"] == "planned"
    assert store.status_control(FILM)["manual"] is False


def test_automation_api_disabled_and_preview_does_not_mutate(api, base, monkeypatch):
    from api import trackerAPI
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    monkeypatch.setattr(worker, "cached_metadata", lambda key: DETAIL)
    store = TrackerStore(base)
    revision = store.revision()
    result = api.post("/api/tracker/automatic-status", json={"action": "preview"})
    assert result.status_code == 200 and result.json()["items"]
    assert store.revision() == revision
    monkeypatch.setattr(trackerAPI, "load_config", lambda: {"crosswatch": {"root_dir": str(base), "automatic_status": False}})
    assert api.post("/api/tracker/automatic-status", json={"action": "recalculate"}).status_code == 409


@pytest.mark.parametrize("status", ["dropped", "on_hold", "completed"])
@pytest.mark.parametrize("metadata", [None, {**DETAIL, "status": "Returning Series"}])
def test_retry_preserves_synced_status_until_new_history(tmp_path, monkeypatch, status, metadata):
    store = seeded(tmp_path)
    store.evaluate_status(SHOW)
    store.set_show_status([(SHOW, {})], status)
    store.queue_statuses([SHOW])
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("UPDATE status_pending SET due=1 WHERE title_key=?", (SHOW,))
    history, rev = store.read_current("history"), store.revision()
    version = dict(store.pending_statuses())[SHOW]
    result = store.evaluate_status(SHOW, lambda: pytest.fail("Retries must retain synced status without reading metadata"), version=version)
    assert result["after"] == status and not result["deferred"]
    assert store.revision() == rev and store.read_current("history") == history
    assert SHOW not in dict(store.pending_statuses())
    assert store.status_control(SHOW) == {"origin": "sync", "manual": False}
    if status == "dropped":
        assert "tmdb:95396" in store.note_show_tokens("dropped")
        assert store.set_show_status([(SHOW, {})], status)[1] == 0
    store.write_state("history", {**history["items"], "new": {**EPISODE, "episode": 3}})
    fresh = dict(store.pending_statuses())[SHOW]
    assert fresh != version
    assert store.evaluate_status(SHOW, DETAIL, version=version)["skipped"] == "changed"
    assert store.evaluate_status(SHOW, metadata, version=fresh)["after"] == (status if metadata is None else "watching")


@pytest.mark.parametrize("metadata", [DETAIL, {**DETAIL, "status": "Returning Series"}])
def test_known_incomplete_show_leaves_queue_until_history_changes(tmp_path, metadata):
    store = TrackerStore(tmp_path)
    store.write_state("history", {"one": EPISODE})
    result = store.evaluate_status(SHOW, metadata, version=dict(store.pending_statuses())[SHOW])
    assert result["after"] == "watching" and not result["deferred"]
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute("SELECT count(*) FROM status_pending").fetchone()[0] == 0
    store.write_state("history", {"one": EPISODE, "two": {**EPISODE, "episode": 1}})
    assert SHOW in dict(store.pending_statuses())


def test_missing_metadata_retries_daily_and_can_complete(tmp_path, monkeypatch):
    store = seeded(tmp_path)
    now = time.time()
    monkeypatch.setattr("cw_platform.tracker_automation.time.time", lambda: now)
    result = store.evaluate_status(SHOW)
    assert result["deferred"]
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute("SELECT due FROM status_pending WHERE title_key=?", (SHOW,)).fetchone()[0] == now + 86400
    assert SHOW not in dict(store.pending_statuses())
    monkeypatch.setattr("cw_platform.tracker_automation.time.time", lambda: now + 86401)
    version = dict(store.pending_statuses())[SHOW]
    assert store.evaluate_status(SHOW, DETAIL, version=version)["after"] == "completed"
    assert SHOW not in dict(store.pending_statuses())


@pytest.mark.parametrize("interrupt", [None, "busy", "error"])
def test_worker_reuses_connection_and_prunes_once_even_when_interrupted(tmp_path, monkeypatch, interrupt):
    store = TrackerStore(tmp_path)
    store.write_state("history", {str(n): {**MOVIE, "ids": {"tmdb": str(n)}} for n in range(1, 31)})
    store.configure_automation(True)
    monkeypatch.setattr(worker, "tracker_roots", lambda *args: [("default", store.root)])
    monkeypatch.setattr(worker, "cached_metadata", lambda *args: pytest.fail("Movies need no metadata"))
    connections, statements, prunes, busy_calls, evaluations = [], [], [], [], []
    connect, prune, evaluate = TrackerStore._connect, TrackerStore._prune_changes, TrackerStore.evaluate_status
    def counted_connect(self):
        conn = connect(self)
        connections.append(conn)
        conn.set_trace_callback(statements.append)
        return conn
    def counted_prune(conn, now):
        prunes.append(conn)
        return prune(conn, now)
    def is_busy():
        busy_calls.append(1)
        return interrupt == "busy" and len(busy_calls) == 3
    def evaluate_with_failure(self, *args, **kwargs):
        evaluations.append(1)
        if interrupt == "error" and len(evaluations) == 2:
            raise RuntimeError("Synthetic interruption")
        return evaluate(self, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(TrackerStore, "_connect", counted_connect)
        patch.setattr(TrackerStore, "_prune_changes", staticmethod(counted_prune))
        patch.setattr(TrackerStore, "evaluate_status", evaluate_with_failure)
        patch.setattr(worker, "busy", is_busy)
        worker.run_once({}, tmp_path)
    changed = 1 if interrupt else 25
    assert len(connections) == 2 and len(prunes) == 1
    assert statements.count("BEGIN IMMEDIATE") == changed + 1
    assert sum(s.startswith("INSERT INTO meta(key, value) VALUES('rev',") for s in statements) == changed
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute("SELECT count(*) FROM notes WHERE status='completed'").fetchone()[0] == changed
        assert conn.execute("SELECT count(*) FROM changes WHERE feature='status_control'").fetchone()[0] == changed
        batch = conn.execute("SELECT batch FROM changes WHERE feature='status_control' ORDER BY id DESC LIMIT 1").fetchone()[0]
    store.undo(batch)
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute("SELECT count(*) FROM notes WHERE status='completed'").fetchone()[0] == changed - 1


def test_preview_can_read_while_another_connection_holds_write_lock(tmp_path):
    store = seeded(tmp_path)
    with store.session(), sqlite3.connect(store.db_path) as writer:
        writer.execute("BEGIN IMMEDIATE")
        store._shared.execute("PRAGMA busy_timeout=50")
        result = store.evaluate_status(SHOW, DETAIL, preview=True)
        assert result["after"] == "completed"
        assert not store._shared.in_transaction
        assert store.note(SHOW) is None


@pytest.mark.parametrize("writer", ["tag", "note", "bulk"])
def test_synced_drop_settles_pending_history_without_locking_later_activity(tmp_path, monkeypatch, writer):
    store = seeded(tmp_path)
    def drop():
        if writer == "tag":
            return store.set_show_status([(SHOW, {})], "dropped")[0]
        if writer == "bulk":
            return store.change_notes([SHOW], status="dropped", origin="sync", defaults={SHOW: {"type": "show"}})[0]
        return store.save_note(SHOW, {**(store.note(SHOW) or {"type": "show"}), "status": "dropped"}, origin="sync")
    stale = dict(store.pending_statuses())[SHOW]
    assert drop()
    assert SHOW not in dict(store.pending_statuses()) and FILM in dict(store.pending_statuses())
    assert store.status_control(SHOW) == {"origin": "sync", "manual": False}
    assert store.evaluate_status(SHOW, lambda: pytest.fail("Superseded work must not read metadata"), version=stale)["skipped"] == "changed"
    monkeypatch.setattr(worker, "tracker_roots", lambda *args: [("default", store.root)])
    monkeypatch.setattr(worker, "busy", lambda: False)
    monkeypatch.setattr(worker, "cached_metadata", lambda *args: pytest.fail("Dropped title must not be evaluated"))
    worker.run_once({}, tmp_path)
    assert store.note(SHOW)["status"] == "dropped"
    assert "tmdb:95396" in store.note_show_tokens("dropped")
    history = store.read_current("history")["items"]
    history["new"] = {**EPISODE, "episode": 3}
    store.write_state("history", history)
    assert SHOW in dict(store.pending_statuses())
    rev, log_count = store.revision(), store.change_log()[0]
    assert drop() is None
    assert SHOW not in dict(store.pending_statuses())
    assert store.revision() == rev and store.change_log()[0] == log_count
    history["later"] = {**EPISODE, "episode": 4}
    store.write_state("history", history)
    monkeypatch.setattr(worker, "cached_metadata", lambda key: {**DETAIL, "status": "Returning Series"})
    worker.run_once({}, tmp_path)
    assert store.note(SHOW)["status"] == "watching"
    assert store.status_control(SHOW)["manual"] is False


def test_failed_synced_drop_does_not_discard_pending_history(tmp_path):
    store = seeded(tmp_path)
    pending = store.pending_statuses()
    with pytest.raises(TrackerConflict):
        store.save_note(SHOW, {"type": "show", "status": "dropped"}, expected={"type": "show", "status": "watching"}, origin="sync")
    assert store.pending_statuses() == pending and store.note(SHOW) is None
    with pytest.raises(KeyError):
        store.change_notes([SHOW, "show:tmdb:123456"], status="dropped", origin="sync", defaults={SHOW: {"type": "show"}})
    assert store.pending_statuses() == pending and store.note(SHOW) is None


def test_apply_preview_saves_only_requested_changes_and_supports_undo(api, base, monkeypatch):
    from api import trackerAPI
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    store = TrackerStore(base)
    preview = api.post("/api/tracker/automatic-status", json={"action": "preview"}).json()
    item = next(row for row in preview["items"] if row["key"] == FILM)
    result = api.post("/api/tracker/automatic-status", json={"action": "apply", "items": [item]})
    assert result.status_code == 200, result.text
    data = result.json()
    assert data["updated"] == 1
    assert store.note(FILM)["status"] == "completed"
    assert store.note("movie:tmdb:1") is None
    assert store.note(SHOW) is None
    again = api.post("/api/tracker/automatic-status", json={"action": "apply", "items": [item]}).json()
    assert again["updated"] == 0 and again["items"][0]["skipped"] == "changed"
    store.undo(data["items"][0]["batch"])
    assert store.note(FILM) is None


def test_apply_preview_keeps_manual_and_unknown_statuses_and_checks_stale_data(api, base, monkeypatch):
    from api import trackerAPI
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    monkeypatch.setattr(worker, "cached_metadata", lambda _key: {**DETAIL, "status": "Returning Series"})
    store = TrackerStore(base)
    store.save_note(FILM, {"type": "movie", "status": "watching"}, origin="manual")
    store.save_note("movie:tmdb:1", {"type": "movie", "status": "watching"})
    suggestions = [{"key": key, "before": "watching", "after": "completed"} for key in (FILM, "movie:tmdb:1")]
    suggestions.append({"key": SHOW, "before": None, "after": "completed"})
    data = api.post("/api/tracker/automatic-status", json={"action": "apply", "items": suggestions}).json()
    assert data["updated"] == 0
    assert [row["skipped"] for row in data["items"]] == ["protected", "protected", "changed"]
    assert data["items"][0]["reason"] == "Manual status kept"
    assert "Existing status kept" in data["items"][1]["reason"]
    assert store.note(FILM)["status"] == store.note("movie:tmdb:1")["status"] == "watching"
    assert store.note(SHOW) is None


def test_apply_preview_can_explicitly_update_a_synced_status(api, base, monkeypatch):
    from api import trackerAPI
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    store = TrackerStore(base)
    store.save_note(FILM, {"type": "movie", "status": "watching"}, origin="sync")
    result = api.post("/api/tracker/automatic-status", json={"action": "apply", "items": [{"key": FILM, "before": "watching", "after": "completed"}]}).json()
    assert result["updated"] == 1
    assert store.status_control(FILM)["manual"] is False
    assert store.note(FILM)["status"] == "completed"


@pytest.mark.parametrize("items", [[], [{}], [{"key": FILM, "before": [], "after": "completed"}], [{"key": FILM, "before": None, "after": "invalid"}], [{"key": FILM, "before": None, "after": "completed"}] * 101])
def test_apply_preview_rejects_invalid_or_unbounded_batches(api, monkeypatch, items):
    from api import trackerAPI
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    response = api.post("/api/tracker/automatic-status", json={"action": "apply", "items": items})
    assert response.status_code == 400


def test_preview_explains_fully_watched_ongoing_and_inconsistent_shows(tmp_path):
    store = seeded(tmp_path)
    ongoing = store.evaluate_status(SHOW, {**DETAIL, "status": "Returning Series"}, preview=True)
    assert ongoing["after"] == "watching"
    assert "not marked ended" in ongoing["reason"]
    missing = store.evaluate_status(SHOW, {**DETAIL, "number_of_episodes": 3}, preview=True)
    assert missing["reason"] == "Completion metadata is missing or inconsistent"
    assert missing["deferred"] is True
