# /tests/test_notification_center.py
# CrossWatch - Server-side notification items, permissions and source errors
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import json
import re
from types import SimpleNamespace

from fastapi import HTTPException

from cw_platform import notification_store as store
from services import notification_center as center


def _ctx(user=None, **kwargs):
    request = SimpleNamespace(state=SimpleNamespace(cw_user=user), query_params={}, headers={})
    return center.context(request, kwargs.pop("cfg", {}), kwargs.pop("user_id", "alice"), kwargs.pop("scope", "home"), **kwargs)


def test_keys_match_the_browser_format():
    assert center.notification_key("sync-reviews", {"id": "one", "revision": "review"}) == '["sync-reviews","one","review"]'
    assert center.notification_key("updates", {"id": "0.12.0"}) == '["updates","0.12.0",""]'
    assert center.notification_key("database", {"href": "/x"}) == '["database","/x",""]'


def test_release_announcements_link_to_official_releases():
    [item] = center.update_items({"current": "0.11.7", "latest": "v0.12.0", "update_available": True})
    assert item["title"] == "CrossWatch v0.12.0 is available"
    assert item["detail"] == "Installed version: v0.11.7"
    assert item["href"] == "https://github.com/cenodude/CrossWatch/releases/tag/v0.12.0"
    assert center.update_items({"current": "0.12.0", "latest": "0.12.0", "update_available": False}) == []
    assert center.update_items(None) == []


def test_database_warnings_are_reduced_for_managed_users():
    mb = 1024 * 1024
    status = {"busy": True, "operation": "state.save_pair_blocks", "seconds": 42, "since": 1760000000, "filesystem": "cifs",
              "network_filesystem": True, "reclaim_suggested": True, "reclaimable_bytes": 640 * mb, "file_bytes": 1200 * mb}
    busy, network, reclaim = center.database_items(status, managed=False)
    assert "state.save_pair_blocks) has been running for 42 seconds" in busy["detail"]
    assert busy["quiet"] and busy["href"] == "/#settings/maintenance"
    assert "The local database is on cifs" in network["detail"]
    assert "640 MB of the 1.2 GB local database is unused space" in reclaim["detail"]
    [managed] = center.database_items({**status, "seconds": 300}, managed=True)
    assert "running for 5 minutes" in managed["detail"]
    assert managed["action"] == "Got it" and managed["href"] == "#"
    assert center.database_items({"busy": False}, managed=False) == []


def test_maintenance_keeps_one_item_from_running_to_completed():
    job = {"id": "job-1", "action": "events-optimize", "state": "running", "step": "vacuum", "started_at": 123}
    [running] = center.maintenance_items(job)
    assert running["title"] == "Optimize database - running"
    assert "Reclaiming unused space" in running["detail"]
    assert running["href"] == "/#maintenance" and running["quiet"] and running["active"]
    assert center.maintenance_items({**job, "step": "analyze"})[0]["revision"] == running["revision"]
    [done] = center.maintenance_items({**job, "state": "completed", "result": {"ok": True}})
    assert done["revision"] != running["revision"] and "completed" in done["title"] and done["action"] == "View result"
    assert not done["quiet"]


def test_maintenance_failures_report_attention():
    job = {"id": "job-2", "action": "database-health", "state": "failed", "result": {"ok": False}}
    assert center.maintenance_items(job)[0]["icon"] == "error_outline"
    assert "worker stopped" in center.maintenance_items({**job, "state": "interrupted"})[0]["detail"]
    assert "needs attention" in center.maintenance_items({**job, "state": "completed", "result": {"healthy": False}})[0]["title"]


def test_metadata_check_progress_attention_and_stops():
    job = {"id": "c1", "instance": "default", "label": "Default", "state": "running", "done": 120, "total": 1742, "started_at": 10}
    [first] = center.tracker_items({"job": job})
    assert "Metadata check - running" in first["title"]
    assert first["detail"] == "120 of 1,742 records checked."
    assert center.tracker_items({"job": {**job, "done": 900}})[0]["revision"] == first["revision"]
    [attention] = center.tracker_items({"job": {"id": "c2", "instance": "CW-P01", "label": "Desk", "state": "completed", "attention": 12}})
    assert "needs attention (Desk)" in attention["title"] and "12 records need attention" in attention["detail"]
    assert attention["href"] == "/#tracker?attention=1"
    assert "Every record matches TMDB" in center.tracker_items({"job": {"id": "c3", "state": "completed", "attention": 0}})[0]["detail"]
    assert "rejected the API key" in center.tracker_items({"job": {"id": "c4", "state": "failed", "error": "tmdb_key_invalid"}})[0]["detail"]
    assert "restarted" in center.tracker_items({"job": {"id": "c4", "state": "interrupted"}})[0]["detail"]
    assert "cancelled" in center.tracker_items({"job": {"id": "c5", "state": "cancelled"}})[0]["title"]


def test_tracker_move_progress_and_failures():
    move = {"id": "m1", "state": "running", "instance": "default", "instance_index": 1, "instance_count": 2, "files_done": 312,
            "files_total": 1200, "bytes_done": 250e6, "bytes_total": 1000e6}
    [item] = center.tracker_items({"migration": move})
    assert "312 of 1,200 files (250 of 1,000 MB) · tracker 1 of 2. Syncs wait" in item["detail"]
    [done] = center.tracker_items({"migration": {**move, "state": "completed", "results": [{"instance": "default", "backup": "json-backup-20261104T100000Z"}]}})
    assert "original files are in json-backup-20261104T100000Z" in done["detail"] and done["revision"] != item["revision"]
    move, check = center.tracker_items({"job": {"id": "c9", "state": "completed", "attention": 0},
                                        "migration": {"id": "m2", "state": "failed", "results": [{"instance": "CW-P01"}], "errors": [{"instance": "default", "file": "history.json"}]}})
    assert re.search(r"history\.json could not be read, so the tracker still uses its old files.*1 other tracker was moved", move["detail"])
    assert "Metadata check - completed" in check["title"]
    [legacy] = center.tracker_items({"migration": {"id": "r", "state": "completed", "results": [{"restore_points": 14, "snapshots_backup_only": 50, "backup": "b"}]}})
    assert "14 snapshots imported; 50 snapshots kept only in the backup" in legacy["detail"]
    [pending] = center.tracker_items({"migration": {"id": "r", "state": "failed", "errors": [{"instance": "default", "reason": "backup_pending"}]}})
    assert "restart CrossWatch to retry" in pending["detail"]


def test_import_phases_and_results():
    run = {"id": "i1", "state": "running", "instance": "CW-P01", "label": "Desk", "source": "TRAKT", "source_label": "Trakt", "source_instance": "default", "phase": "reading"}
    [item] = center.tracker_items({"import": run})
    assert item["id"] == "import:i1" and "Import - running into Desk" in item["title"]
    assert "From Trakt. Reading Trakt and matching IDs." in item["detail"]
    writing = center.tracker_items({"import": {**run, "phase": "writing", "done": 1200, "total": 4800}})[0]
    assert "Writing 1,200 of 4,800." in writing["detail"] and writing["revision"] == item["revision"]
    [done] = center.tracker_items({"import": {**run, "state": "completed", "added": 4700, "unresolved": 3}})
    assert done["detail"] == "4,700 records added from Trakt. 3 records could not be matched."
    assert "restarted during the import" in center.tracker_items({"import": {**run, "state": "interrupted"}})[0]["detail"]


def test_snapshot_maintenance_keeps_one_item():
    job = {"id": "s1", "instance": "default", "state": "running", "phase": "compressing", "done": 2, "total": 10, "bytes_saved": 5e6}
    [first] = center.tracker_items({"storage": [job]})
    assert re.search(r"2 of 10 snapshots compressed; 5.*MB less snapshot data", first["detail"]) and first["active"]
    assert center.tracker_items({"storage": [{**job, "done": 8}]})[0]["revision"] == first["revision"]
    idle = center.tracker_items({"storage": [{**job, "phase": "waiting", "reason": "idle"}]})[0]
    assert "idle window" in idle["detail"] and idle["quiet"] and not idle["active"]
    disk = center.tracker_items({"storage": [{**job, "phase": "waiting", "reason": "disk_space"}]})[0]
    assert "More free disk space" in disk["detail"] and not disk["quiet"]
    assert "completed" in center.tracker_items({"storage": [{**job, "state": "completed"}]})[0]["title"]
    assert "will retry" in center.tracker_items({"storage": [{**job, "state": "failed"}]})[0]["detail"]
    assert "1 MB more snapshot data" in center.tracker_items({"storage": [{**job, "state": "completed", "bytes_saved": -1e6}]})[0]["detail"]


def test_plex_recovery_changes_when_ready_or_imported():
    reading = center.plex_recovery_items({"id": "one", "status": "reading"})[0]
    ready = center.plex_recovery_items({"id": "one", "status": "review"})[0]
    imported = center.plex_recovery_items({"id": "one", "status": "review", "imported": 5})[0]
    assert reading["action"] == "View progress" and reading["quiet"]
    assert "Ready to review" in ready["detail"] and ready["revision"] != reading["revision"]
    assert "5 items imported" in imported["detail"] and imported["revision"] != ready["revision"]
    assert imported["href"] == "/#import_export?recovery=1"
    assert center.plex_recovery_items(None) == []


def test_sync_reviews_and_ready_reviews():
    [item] = center.sync_review_items([{"id": "s 1", "status": "review", "planned_at": 1760000000.0,
                                        "pair": {"source": "PLEX", "target": "TRAKT", "target_instance": "TRAKT-P01", "mode": "two-way"}}])
    assert item["title"] == "PLEX ↔ TRAKT · TRAKT-P01" and item["detail"] == "Ready to review"
    assert item["revision"] == "review:1760000000"
    assert item["href"] == "/#interactive_sync?session=s%201"
    month = center.review_item({"kind": "month", "instance": "default", "period": "2026-10", "label": "Tracker", "ready_revision": "5"})
    assert month["title"] == "Your October review is ready" and month["detail"] == "Tracker · 2026"
    assert month["href"] == "#tracker?instance=default&month=2026-10"
    assert json.loads(month["id"]) == ["default", "2026-10"]
    wrapped = center.review_item({"kind": "wrapped", "instance": "CW-P01", "period": "2026", "label": "Desk"})
    assert wrapped["title"] == "Your 2026 CrossWatch Wrapped is ready" and wrapped["href"].endswith("&year=2026")


def test_collect_gates_sources_and_reports_errors(monkeypatch):
    calls = []

    def source(name, items=(), fail=None):
        def run(ctx):
            calls.append(name)
            if fail:
                raise fail
            return list(items)
        return run

    monkeypatch.setattr(center, "SOURCES", (
        ("sync-reviews", source("reviews", [{"id": "r", "title": "R", "quiet": True, "active": True}]), "Could not load sync reviews."),
        ("tracker-check", source("tracker", fail=RuntimeError("secret path")), "Could not refresh the tracker status."),
        ("updates", source("updates", fail=RuntimeError("x")), ""),
    ))
    result = center.collect(_ctx())
    assert [item["key"] for item in result["items"]] == ['["sync-reviews","r",""]']
    assert result["items"][0]["scope"] == "profile"
    assert result["errors"] == ["Could not refresh the tracker status."]
    assert result["poll"] == center.ACTIVE_POLL
    assert "secret" not in json.dumps(result)


def test_permissions_follow_the_managed_user():
    viewer = _ctx({"id": "bob", "is_admin": False, "permissions": {"write": False}})
    assert not viewer.admin and not viewer.write and not viewer.tracker
    assert center._maintenance(viewer) == [] and center._plex_recovery(viewer) == [] and center._tracker(viewer) == []
    admin = _ctx({"id": "admin", "is_admin": True})
    assert admin.admin and admin.write and admin.tracker
    local = _ctx(None)
    assert local.admin


def test_handler_denials_hide_a_source():
    assert center._handler(lambda: (_ for _ in ()).throw(HTTPException(403))) is None


def test_review_waits_turn_ready_once(monkeypatch, tmp_path):
    monkeypatch.setenv("CONFIG_BASE", str(tmp_path))
    monkeypatch.setattr(center, "_review_checked", {})
    store.wait_for_review("alice", "home", "month", "default", "2026-10", "Tracker")
    answers = iter([None, "77"])
    checks = []
    monkeypatch.setattr(center, "_review_ready", lambda ctx, row: checks.append(row["period"]) or next(answers))
    ctx = _ctx()
    assert center._reviews(ctx, "month") == []
    monkeypatch.setattr(center, "_review_checked", {})
    [item] = center._reviews(ctx, "month")
    assert item["revision"] == "77"
    assert center._reviews(ctx, "month")[0]["revision"] == "77"
    assert checks == ["2026-10", "2026-10"]
    store.review_viewed("alice", "month", "default", "2026-10")
    assert center._reviews(ctx, "month") == []


def test_unknown_reviews_are_dropped(monkeypatch, tmp_path):
    monkeypatch.setenv("CONFIG_BASE", str(tmp_path))
    monkeypatch.setattr(center, "_review_checked", {})
    store.wait_for_review("alice", "home", "wrapped", "gone", "2026", "Gone")
    monkeypatch.setattr(center, "_review_ready", lambda ctx, row: "")
    assert center._reviews(_ctx(), "wrapped") == []
    assert store.review_waits("alice", "home") == []


def test_a_review_left_while_away_is_still_checked_before_it_expires(monkeypatch, tmp_path):
    monkeypatch.setenv("CONFIG_BASE", str(tmp_path))
    monkeypatch.setattr(center, "_review_checked", {})
    store.wait_for_review("alice", "home", "month", "default", "2026-10", "Tracker")
    store.wait_for_review("alice", "home", "month", "default", "2026-09", "Tracker")
    later = center.time.time() + store.WAIT_SECONDS + 60
    monkeypatch.setattr(center.time, "time", lambda: later)
    monkeypatch.setattr(center, "_review_ready", lambda ctx, row: "5" if row["period"] == "2026-10" else None)
    [item] = center._reviews(_ctx(), "month")
    assert item["title"] == "Your October review is ready"
    assert [row["period"] for row in store.review_waits("alice", "home")] == ["2026-10"]
