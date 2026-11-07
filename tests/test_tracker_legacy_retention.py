# tests/test_tracker_legacy_retention.py
# CrossWatch - Retention-aware JSON migration and resumable original backups
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import json
import os
import sqlite3
import time
from pathlib import Path

import pytest

from cw_platform import config_base
from cw_platform.tracker_store import TrackerBackupPending, TrackerStore, TrackerUnreadable
from services import tracker_migration


def legacy(root, count=20, scope="", state=True, feature="history"):
    folder = root / "snapshots" / scope
    folder.mkdir(parents=True, exist_ok=True)
    payload = json.dumps({"ts": 1, "items": {"tmdb:1": {"type": "movie", "title": "Original"}}})
    if state:
        (root / f"{feature}{'.' + scope if scope else ''}.json").write_text(payload, encoding="utf-8")
    paths = []
    for day in range(count):
        path = folder / f"202601{day + 1:02d}T000000Z-{feature}.json"
        path.write_text(payload, encoding="utf-8")
        age = time.time() - (count - day) * 86400
        os.utime(path, (age, age))
        paths.append(path)
    return paths


def test_default_filters_per_scope_and_feature_and_backs_up_every_original(tmp_path):
    paths = []
    for scope in ("", "pair-a"):
        for feature in ("history", "ratings"):
            paths += legacy(tmp_path, scope=scope, feature=feature)
    originals = {p.relative_to(tmp_path): p.read_bytes() for p in paths}
    stats = TrackerStore(tmp_path, migration_settings={}).migrate()
    assert stats["restore_points"] == 56
    assert stats["snapshots_backup_only"] == 24
    for scope in ("", "pair-a"):
        store = TrackerStore(tmp_path, scope=scope)
        for feature in ("history", "ratings"):
            assert len(store.restore_points(feature)) == 14
    for name, content in originals.items():
        assert (tmp_path / stats["backup"] / name).read_bytes() == content


def test_custom_limits_pin_and_missing_state_recovery(tmp_path):
    paths = legacy(tmp_path)
    settings = {"max_snapshots": 2, "retention_days": 3, "restore_history": paths[0].stem}
    store = TrackerStore(tmp_path, migration_settings=settings)
    stats = store.migrate()
    assert {p.name for p in store.restore_points("history")} == {p.name for p in [paths[0], *paths[-2:]]}
    assert stats["snapshots_backup_only"] == 17
    missing = tmp_path / "missing"
    paths = legacy(missing, state=False)
    store = TrackerStore(missing, migration_settings={"retention_days": 1, "max_snapshots": 2})
    store.migrate()
    assert [p.name for p in store.restore_points("history")] == [paths[-1].name]
    assert store.read_state("history").raw["items"]["tmdb:1"]["title"] == "Original"


def test_excluded_corrupt_snapshot_is_backed_up_but_selected_corruption_blocks_import(tmp_path):
    paths = legacy(tmp_path)
    paths[0].write_text("{broken", encoding="utf-8")
    os.utime(paths[0], (1, 1))
    store = TrackerStore(tmp_path, migration_settings={})
    stats = store.migrate()
    assert (tmp_path / stats["backup"] / "snapshots" / paths[0].name).read_text() == "{broken"
    other = tmp_path / "corrupt"
    paths = legacy(other)
    paths[-1].write_text("{broken", encoding="utf-8")
    with pytest.raises(TrackerUnreadable):
        TrackerStore(other, migration_settings={}).migrate()
    assert paths[-1].exists()
    assert not list(other.glob("json-backup-*"))


def test_lazy_import_resolves_named_instance_limits(tmp_path, monkeypatch):
    root = tmp_path / "profiles" / "CW-P01"
    legacy(root)
    cfg = {"crosswatch": {"root_dir": str(tmp_path), "max_snapshots": 9,
                          "instances": {"CW-P01": {"max_snapshots": 3}}}}
    monkeypatch.setattr(config_base, "load_config", lambda: cfg)
    store = TrackerStore(root)
    assert len(store.restore_points("history")) == 3


def test_background_migration_preserves_custom_unlimited_instance(tmp_path):
    legacy(tmp_path)
    legacy(tmp_path / "profiles" / "CW-P01")
    cfg = {"crosswatch": {"root_dir": str(tmp_path), "max_snapshots": 2,
                          "instances": {"CW-P01": {"max_snapshots": 0, "retention_days": 0}}}}
    assert tracker_migration.start(cfg, tmp_path, background=False)
    assert tracker_migration.status()["state"] == "completed"
    assert len(TrackerStore(tmp_path).restore_points("history")) == 2
    assert len(TrackerStore(tmp_path / "profiles" / "CW-P01").restore_points("history")) == 20


@pytest.mark.parametrize("after_move", [False, True])
def test_backup_retry_does_not_reimport_or_nest_directories(tmp_path, monkeypatch, after_move):
    legacy(tmp_path)
    store = TrackerStore(tmp_path, migration_settings={})
    rename = Path.rename

    def interrupted(path, destination):
        if path.name == "snapshots":
            if after_move:
                rename(path, destination)
            raise OSError("interrupted backup move")
        return rename(path, destination)

    monkeypatch.setattr(Path, "rename", interrupted)
    with pytest.raises(TrackerBackupPending):
        store.migrate()
    assert store.needs_migration()
    conn = store._connect()
    conn.execute("UPDATE items SET data=?", (json.dumps({"type": "movie", "title": "Changed after import"}),))
    conn.close()
    monkeypatch.setattr(Path, "rename", rename)
    stats = store.migrate()
    assert stats["backup_pending"] == []
    assert not store.needs_migration()
    assert len(list((tmp_path / stats["backup"] / "snapshots").glob("*.json"))) == 20
    assert len(list(tmp_path.glob("json-backup-*"))) == 1
    assert store.read_current("history")["items"]["tmdb:1"]["title"] == "Changed after import"


def test_old_import_marker_finishes_remaining_backup_without_reimport(tmp_path):
    legacy(tmp_path)
    old_backup = tmp_path / "json-backup-old"
    old_backup.mkdir()
    (old_backup / "keep.json").write_bytes(b"unchanged backup")
    store = TrackerStore(tmp_path, migration_settings={})
    conn = store._connect()
    store._put_state(conn, "", "history", {"new": {"type": "movie", "title": "Newer state"}}, ts=2)
    conn.execute("INSERT INTO meta(key,value) VALUES('legacy_import',?)",
                 (json.dumps({"ts": 1, "states": 1, "docs": 0, "restore_points": 20}),))
    conn.close()
    cfg = {"crosswatch": {"root_dir": str(tmp_path)}}
    assert tracker_migration.start(cfg, tmp_path, background=False)
    status = tracker_migration.status()
    assert status["state"] == "completed"
    stats = status["results"][0]
    assert (tmp_path / stats["backup"] / "history.json").is_file()
    assert len(list((tmp_path / stats["backup"] / "snapshots").glob("*.json"))) == 20
    assert (old_backup / "keep.json").read_bytes() == b"unchanged backup"
    assert store.read_current("history")["items"] == {"new": {"type": "movie", "title": "Newer state"}}
    assert not store.needs_migration()
    assert not tracker_migration.start(cfg, tmp_path, background=False)


def test_metadata_failure_after_final_move_reports_backup_pending_and_recovers(tmp_path, monkeypatch):
    legacy(tmp_path)
    rename = Path.rename
    store = TrackerStore(tmp_path, migration_settings={})

    def fail_metadata(path, destination):
        result = rename(path, destination)
        if path.name == "snapshots":
            conn = store._connect()
            conn.execute("CREATE TRIGGER reject_backup BEFORE UPDATE ON meta WHEN OLD.key='legacy_import' "
                         "BEGIN SELECT RAISE(FAIL, 'injected metadata error'); END")
            conn.close()
        return result

    monkeypatch.setattr(Path, "rename", fail_metadata)
    cfg = {"crosswatch": {"root_dir": str(tmp_path)}}
    assert tracker_migration.start(cfg, tmp_path, background=False)
    status = tracker_migration.status()
    assert status["errors"] == [{"instance": "default", "file": None, "reason": "backup_pending"}]
    assert not (tmp_path / "snapshots").exists()
    assert store.needs_migration()
    with pytest.raises(TrackerBackupPending) as raised:
        store.migrate()
    assert isinstance(raised.value.__cause__, sqlite3.Error)
    conn = store._connect()
    conn.execute("DROP TRIGGER reject_backup")
    conn.close()
    monkeypatch.setattr(Path, "rename", rename)
    assert tracker_migration.start(cfg, tmp_path, background=False)
    assert tracker_migration.status()["state"] == "completed"
    assert not store.needs_migration()
    assert len(list(tmp_path.glob("json-backup-*"))) == 1
