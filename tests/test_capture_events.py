# tests/test_capture_events.py
# CrossWatch - Capture and backup events in the Events archive tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from typing import Any

import pytest


@pytest.fixture()
def archive(monkeypatch):
    from cw_platform.event_archive import db, recorder

    conn = db.connect(":memory:")
    monkeypatch.setattr(recorder, "get_conn", lambda: conn)
    return conn


def _events(conn) -> list[dict[str, Any]]:
    cols = ("event_type", "domain", "feature", "severity", "source_provider", "source_instance", "source_kind", "title", "reason", "detail")
    rows = conn.execute(f"SELECT {', '.join(cols)} FROM events WHERE domain IN ('capture', 'backup') ORDER BY id").fetchall()
    return [dict(zip(cols, tuple(row))) for row in rows]


def test_capture_and_backup_events_are_recorded_one_group_each(archive):
    from cw_platform.event_archive import record_backup, record_backup_cleanup, record_backup_upload, record_capture, record_capture_cleanup

    record_capture("plex", "P2", "watchlist", trigger="scheduler", count=42, label="nightly", path="snap.json")
    record_capture("trakt", "default", "history", ok=False, reason="Provider not configured: TRAKT#default")
    record_capture_cleanup("plex", "P2", "watchlist", 3, trigger="scheduler")
    record_capture_cleanup("plex", "P2", "watchlist", 0, trigger="scheduler")
    record_backup("full", trigger="pre_restore", size=2048, path="backup.zip")
    record_backup_upload("webdav", ok=False, trigger="scheduler", reason="Upload failed")
    record_backup_cleanup(2, trigger="scheduler")

    rows = _events(archive)
    assert [r["event_type"] for r in rows] == [
        "capture_completed", "capture_failed", "capture_cleanup", "backup_completed", "backup_upload_failed", "backup_cleanup",
    ]
    first = rows[0]
    assert (first["source_provider"], first["source_instance"], first["source_kind"], first["feature"]) == ("PLEX", "P2", "scheduler", "capture")
    assert json.loads(first["detail"]) == {"capture_feature": "watchlist", "items": 42, "label": "nightly", "path": "snap.json"}
    assert rows[1]["severity"] == "error" and "not configured" in rows[1]["reason"]
    assert [r["domain"] for r in rows] == ["capture"] * 3 + ["backup"] * 3
    assert rows[3]["source_provider"] is None and rows[3]["feature"] == "backup" and rows[3]["source_kind"] == "pre_restore"

    from cw_platform.event_archive.groups import correlate

    correlate(conn=archive)
    groups = archive.execute("SELECT status, summary FROM event_groups WHERE domain IN ('capture', 'backup') ORDER BY id").fetchall()
    assert [g[0] for g in groups] == ["completed", "failed", "completed", "completed", "failed", "completed"]
    assert groups[0][1].startswith("Watchlist capture")


def test_create_snapshot_records_success_and_failure(monkeypatch):
    import cw_platform.event_archive as event_archive
    from services import snapshots

    seen: list[dict[str, Any]] = []
    monkeypatch.setattr(event_archive, "record_capture", lambda *a, **kw: seen.append({"args": a, **kw}))
    monkeypatch.setattr(snapshots, "_create_snapshot", lambda *a, **kw: {"ok": True, "feature": "watchlist", "label": "x", "path": "p.json", "stats": {"count": 7}})

    snapshots.create_snapshot("plex", "watchlist", instance_id="P2", trigger="scheduler")

    def boom(*_a, **_kw):
        raise ValueError("Provider not configured: PLEX#P2")

    monkeypatch.setattr(snapshots, "_create_snapshot", boom)
    with pytest.raises(ValueError):
        snapshots.create_snapshot("plex", "watchlist", instance_id="P2")

    assert seen[0]["args"] == ("PLEX", "P2", "watchlist") and seen[0]["trigger"] == "scheduler" and seen[0]["ok"] is True and seen[0]["count"] == 7
    assert seen[1]["trigger"] == "manual" and seen[1]["ok"] is False and "not configured" in seen[1]["reason"]


def test_create_backup_records_its_trigger(monkeypatch):
    import cw_platform.event_archive as event_archive
    from services import backups

    seen: list[dict[str, Any]] = []
    monkeypatch.setattr(event_archive, "record_backup", lambda *a, **kw: seen.append({"args": a, **kw}))
    monkeypatch.setattr(backups, "_create_backup", lambda **kw: {"ok": True, "scope": "full", "size": 10, "label": "l", "path": "b.zip"})

    backups.create_backup(scope="full", trigger="upgrade")

    assert seen == [{"args": ("full",), "trigger": "upgrade", "ok": True, "size": 10, "label": "l", "path": "b.zip"}]


def test_capture_retention_records_removed_captures(monkeypatch):
    import cw_platform.event_archive as event_archive
    from services import snapshots

    seen: list[tuple[Any, ...]] = []
    rows = [{"provider": "PLEX", "instance": "default", "feature": "watchlist", "path": f"s{i}.json", "created_at": f"2026-11-1{i}T00:00:00Z"} for i in range(4)]
    monkeypatch.setattr(snapshots, "list_snapshots", lambda: rows)
    monkeypatch.setattr(snapshots, "delete_snapshot", lambda rel: {"deleted": [rel], "errors": []})
    monkeypatch.setattr(event_archive, "record_capture_cleanup", lambda *a, **kw: seen.append((*a, kw.get("trigger"))))

    result = snapshots.enforce_capture_retention("plex", "watchlist", max_captures=1, auto_delete_old=True, trigger="scheduler")

    assert len(result["deleted"]) == 3
    assert seen == [("PLEX", "default", "watchlist", 3, "scheduler")]


def test_backup_prune_records_cleanup_with_trigger(monkeypatch):
    import cw_platform.event_archive as event_archive
    from services import backup_manager

    seen: list[tuple[Any, ...]] = []
    monkeypatch.setattr(backup_manager, "_jobs", lambda: [])
    monkeypatch.setattr(backup_manager, "read", lambda _name, fallback: fallback)
    monkeypatch.setattr(backup_manager.backups, "enforce_backup_retention", lambda **_kw: {"ok": True, "applied": True, "deleted": ["a.zip", "b.zip"], "errors": []})
    monkeypatch.setattr(event_archive, "record_backup_cleanup", lambda removed, **kw: seen.append((removed, kw.get("trigger"))))

    backup_manager.prune_local(max_backups=1, auto_delete_old=True, trigger="scheduler")

    assert seen == [(2, "scheduler")]


@pytest.mark.parametrize("status,event_type,source_kind,shown", [
    ("failed", "capture_failed", "manual", True),
    ("completed", "capture_completed", "scheduler", True),
    ("completed", "backup_completed", "scheduler", True),
    ("completed", "capture_completed", "manual", False),
    ("completed", "backup_completed", "pre_restore", False),
    ("completed", "backup_uploaded", "scheduler", False),
    ("completed", "capture_cleanup", "scheduler", False),
])
def test_feed_shows_failures_and_scheduled_runs_only(status, event_type, source_kind, shown):
    from api import eventsAPI

    backup = event_type.startswith("backup")
    group = {"id": 1, "domain": "backup" if backup else "capture", "feature": "backup" if backup else "capture", "status": status, "summary": "Full backup"}
    events = [{"event_type": event_type, "source_kind": source_kind}]

    assert eventsAPI._capture_in_feed(group, events) is shown
    item = eventsAPI._feed_item(group, events)
    assert item["kind"] == ("backup" if backup else "capture")
    assert item["title"] == "Full backup"


def test_capture_reasons_are_redacted(archive):
    from cw_platform.event_archive import record_backup_upload, record_capture

    record_capture("mdblist", "default", "history", ok=False, reason="HTTP 500 for https://api.mdblist.com/sync?apikey=SECRETKEY123&x=1")
    record_backup_upload("webdav", ok=False, reason="PUT https://alice:hunter2@dav.example/backups failed")

    stored = " ".join(str(r["reason"]) for r in _events(archive))
    assert "SECRETKEY123" not in stored
    assert "hunter2" not in stored and "alice" not in stored
    assert "****" in stored


@pytest.mark.parametrize("user,monitor,default_scope,allowed", [
    ({"is_admin": True}, False, False, True),
    ({"is_admin": True}, False, True, True),
    ({"is_admin": True}, True, False, False),
    ({"is_admin": False, "id": "u1"}, False, False, False),
])
def test_backup_events_are_admin_only_and_survive_the_admin_scope(monkeypatch, user, monitor, default_scope, allowed):
    from api import eventsAPI

    monkeypatch.setattr(eventsAPI, "request_user", lambda _request: user)
    monkeypatch.setattr(eventsAPI, "request_is_monitor", lambda _request: monitor)
    monkeypatch.setattr(eventsAPI, "_default_overview_requested", lambda _request: default_scope)
    row = {"id": 1, "domain": "backup", "feature": "backup", "event_type": "backup_completed"}

    assert eventsAPI._event_row_allowed({}, object(), row) is allowed
    assert eventsAPI._admin_only_domain_requested("backup") is True


def _managed_job(monkeypatch, tmp_path, *, valid: bool, encrypt_error: Exception | None = None):
    import cw_platform.event_archive as event_archive
    from services import backup_manager as bm

    jobs = {"j1": {"id": "j1", "trigger": "scheduler", "settings": {}, "local_status": "pending", "remote_status": "pending", "attempts": 0, "path": "", "manifest": {}}}
    recorded: list[tuple[str, dict[str, Any]]] = []
    created: list[dict[str, Any]] = []

    def update(ident: str, **fields: Any) -> dict[str, Any]:
        jobs[ident].update(fields)
        return dict(jobs[ident])

    def encrypt(*_a: Any) -> None:
        if encrypt_error:
            raise encrypt_error

    monkeypatch.setattr(bm, "_update", update)
    monkeypatch.setattr(bm, "_jobs", lambda: list(jobs.values()))
    monkeypatch.setattr(bm, "reveal", lambda _s: {"components": [], "trackers": None, "password": "pw", "encrypt_local": False, "destination": {"kind": "webdav"}, "installation": "inst"})
    monkeypatch.setattr(bm.backups, "create_backup", lambda **kw: created.append(kw) or {"path": "b.zip", "scope": "selected", "size": 5, "label": "automatic"})
    monkeypatch.setattr(bm.backups, "_resolve_backup_file", lambda p: (p, tmp_path / "b.zip"))
    monkeypatch.setattr(bm.backups, "validate_backup", lambda _p, _pw: {"ok": valid, "manifest": {}})
    monkeypatch.setattr(bm, "_local_retention", lambda _job: None)
    monkeypatch.setattr(bm, "private_dir", lambda: tmp_path)
    monkeypatch.setattr(bm, "_encrypt_archive", encrypt)
    monkeypatch.setattr(event_archive, "record_backup", lambda scope, **kw: recorded.append(("backup", kw)))
    monkeypatch.setattr(event_archive, "record_backup_upload", lambda kind, **kw: recorded.append((f"upload:{kind}", kw)))
    bm.run_job(dict(jobs["j1"]))
    return created, recorded


def test_managed_backup_failing_validation_is_only_a_failure(monkeypatch, tmp_path):
    created, recorded = _managed_job(monkeypatch, tmp_path, valid=False)

    assert created[0]["record_event"] is False
    assert [(kind, kw["ok"]) for kind, kw in recorded] == [("backup", False)]


def test_managed_backup_failing_before_upload_is_recorded(monkeypatch, tmp_path):
    _created, recorded = _managed_job(monkeypatch, tmp_path, valid=True, encrypt_error=OSError("No space left on device"))

    assert [(kind, kw["ok"]) for kind, kw in recorded] == [("backup", True), ("upload:webdav", False)]


def test_capture_and_backup_events_have_retention():
    from cw_platform.event_archive import retention

    assert retention.THREAD_DAYS["capture"] > 0
    assert retention.THREAD_DAYS["backup"] > 0
