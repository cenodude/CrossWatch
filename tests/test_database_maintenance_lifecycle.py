# tests/test_database_maintenance_lifecycle.py
# CrossWatch - Pair scope retention and database maintenance regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import threading
import time
from copy import deepcopy

import pytest

from cw_platform.local_db.db import get_conn
from cw_platform.local_db.diagnostics import diagnostics
from cw_platform.local_db.operation_lock import exclusive_operation
from cw_platform.local_db.state import pair_state_counts
from cw_platform.orchestrator._state_store import StateStore
from cw_platform.pair_scope import pair_feature_scope
from services.pair_state import configured_scopes, prune_pair_scopes, stale_pair_scopes
from services import maintenance_jobs


def seed(base, cfg, pair):
    scope = pair_feature_scope(cfg, pair, "history")
    StateStore(base).for_pair(scope, pair_id=pair["id"]).save_feature_blocks({
        ("PLEX", "default", "history"): {"baseline": {"items": {"imdb:tt1": {"title": "One", "type": "movie"}}}}
    })
    return scope


def test_changed_settings_prune_old_scopes_preserve_disabled_pairs(config_base):
    pair = {"id": "one", "source": "PLEX", "target": "TRAKT", "features": {"history": {"enable": True}}}
    other = {**deepcopy(pair), "id": "two", "enabled": False}
    cfg = {"pairs": [pair, other]}
    old = seed(config_base, cfg, pair)
    kept = seed(config_base, cfg, other)
    pair["mode"] = "two-way"
    current = seed(config_base, cfg, pair)
    assert stale_pair_scopes(config_base, cfg)["scopes"] == [old]
    result = prune_pair_scopes(config_base, cfg)
    assert result["items"] == 1
    assert pair_state_counts(config_base, old) == (0, 0)
    assert pair_state_counts(config_base, current) == (1, 1)
    assert pair_state_counts(config_base, kept) == (1, 1)
    assert prune_pair_scopes(config_base, cfg)["items"] == 0


def test_scope_inventory_uses_real_user_expansion(config_base):
    from test_sync_users import _pair_cfg, LINKS
    from cw_platform.orchestrator._pairs import expand_sync_user_pairs

    cfg = _pair_cfg(match_users=True, user_matches=LINKS)
    expected = {pair_feature_scope(cfg, pair, "history", i) for i, pair in enumerate(expand_sync_user_pairs(cfg), 1)}
    assert len(expected) == 2
    assert expected <= set(configured_scopes(cfg))
    for pair in expand_sync_user_pairs(cfg):
        seed(config_base, cfg, pair)
    assert not stale_pair_scopes(config_base, cfg)["scopes"]
    cfg["jellyfin"]["sync_users"] = {}
    assert stale_pair_scopes(config_base, cfg)["deferred"]
    assert prune_pair_scopes(config_base, cfg)["items"] == 0


def test_archive_rebuild_preserves_pair_baselines(config_base):
    from cw_platform.event_archive.maintenance import rebuild
    from cw_platform.event_archive.recorder import record_events, make_event

    pair = {"id": "one", "source": "PLEX", "target": "TRAKT"}
    scope = seed(config_base, {"pairs": [pair]}, pair)
    record_events([make_event(event_type="test", item_key="imdb:tt1")])
    result = rebuild(reimport=False)
    assert result["ok"] and result["events"] == 0
    assert pair_state_counts(config_base, scope) == (1, 1)


def test_startup_avoids_full_checks_and_duplicate_index_is_removed(config_base):
    from cw_platform.local_db.schema import apply_schema

    conn = get_conn(config_base)
    conn.execute("CREATE INDEX idx_pair_items_state_key ON pair_baseline_items(provider_state_id,item_key)")
    apply_schema(conn)
    indexes = [row[1] for row in conn.execute("PRAGMA index_list(pair_baseline_items)")]
    assert indexes == ["sqlite_autoindex_pair_baseline_items_1"]
    statements = []
    conn.set_trace_callback(statements.append)
    try:
        result = diagnostics(config_base, full=False)
    finally:
        conn.set_trace_callback(None)
    assert result["healthy"]
    assert not any("integrity_check" in sql or "foreign_key_check" in sql or "dbstat" in sql for sql in statements)


def test_optimization_reclaims_deleted_pages_and_reports_wal(config_base):
    from cw_platform.event_archive.maintenance import optimize

    conn = get_conn(config_base)
    with conn:
        conn.execute("CREATE TABLE ballast(data BLOB)")
        conn.execute("INSERT INTO ballast VALUES(zeroblob(2000000))")
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
    with conn:
        conn.execute("DELETE FROM ballast")
    result = optimize(conn=conn)
    assert result["ok"]
    assert result["reclaimed_bytes"] > 1000000
    assert result["wal_bytes"] == 0
    assert not result["checkpoint_pending"]
    assert list(result["checkpoints"]) == ["checkpoint_before", "checkpoint_after"]


def test_job_survives_client_disconnect_and_excludes_sync(config_base):
    entered, release = threading.Event(), threading.Event()

    def work(progress):
        progress("vacuum")
        entered.set()
        assert release.wait(5)
        return {"ok": True, "reclaimed_bytes": 42}

    job = maintenance_jobs.start(config_base, "events-optimize", work)
    try:
        assert entered.wait(5)
        assert maintenance_jobs.status(config_base)["id"] == job["id"]
        assert maintenance_jobs.start(config_base, "events-optimize", work)["id"] == job["id"]
        assert maintenance_jobs.start(config_base, "events-health", work)["error"] == "maintenance_busy"
        with pytest.raises(RuntimeError, match="database_operation_busy"):
            with exclusive_operation(config_base):
                pass
    finally:
        release.set()
    for _ in range(200):
        result = maintenance_jobs.status(config_base)
        if result["state"] == "completed":
            break
        time.sleep(.01)
    assert result["result"]["reclaimed_bytes"] == 42
    assert json.loads((config_base / ".maintenance-job.json").read_text())["state"] == "completed"


def test_restart_marks_unfinished_job_interrupted(config_base):
    (config_base / ".maintenance-job.json").write_text(json.dumps({"id": "old", "state": "running"}))
    assert maintenance_jobs.status(config_base)["result"]["error"] == "worker_interrupted"


def test_busy_checkpoint_does_not_start_vacuum(config_base):
    import sqlite3
    from cw_platform.local_db.db import crosswatch_db_path
    from cw_platform.event_archive.maintenance import optimize

    conn = get_conn(config_base)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
    reader = sqlite3.connect(str(crosswatch_db_path(config_base)))
    try:
        reader.execute("BEGIN")
        reader.execute("SELECT COUNT(*) FROM baseline_items").fetchone()
        with conn:
            conn.execute("INSERT INTO state_meta(key,value_type,updated_at) VALUES('busy-test','text',0)")
        conn.execute("PRAGMA busy_timeout=1")
        steps = []
        result = optimize(conn=conn, progress=steps.append)
        assert result["error"] == "checkpoint_busy"
        assert steps == ["checkpoint_before"]
    finally:
        reader.close()


def test_http_maintenance_refuses_conflicting_operation(config_base, monkeypatch):
    from fastapi import HTTPException, Request
    from api import maintenanceAPI

    monkeypatch.setattr(maintenanceAPI, "_cw", lambda: (None, config_base))
    with exclusive_operation(config_base):
        request = Request({"type": "http", "method": "POST", "path": "/api/maintenance/events-optimize", "headers": []})
        with pytest.raises(HTTPException) as caught:
            next(maintenanceAPI._maintenance_guard(request))
        assert caught.value.status_code == 409
        request = Request({"type": "http", "method": "GET", "path": "/api/maintenance/jobs/current", "headers": []})
        guard = maintenanceAPI._maintenance_guard(request)
        next(guard)
        guard.close()


def test_prune_maintenance_removes_legacy_pair_scopes_after_backup(config_base, monkeypatch):
    from api import maintenanceAPI
    from services import backups

    pair = {"id": "one", "source": "PLEX", "target": "TRAKT"}
    cfg = {"pairs": [pair]}
    old = seed(config_base, cfg, pair)
    pair["mode"] = "two-way"
    current = seed(config_base, cfg, pair)
    calls = []
    monkeypatch.setattr(maintenanceAPI, "_cw", lambda: (None, config_base, config_base / ".cw_state"))
    monkeypatch.setattr(maintenanceAPI, "_load_config_for_state_prune", lambda _: cfg)
    monkeypatch.setattr(maintenanceAPI, "_orphaned_state_files", lambda: [])
    monkeypatch.setattr(backups, "create_backup", lambda **kw: calls.append(pair_state_counts(config_base, old)) or {"path": "backup.zip"})
    result = maintenanceAPI.prune_state_file()
    assert result["ok"]
    assert calls == [(1, 1)]
    assert result["removed"]["removed_pair_scopes"] == 1
    assert pair_state_counts(config_base, old) == (0, 0)
    assert pair_state_counts(config_base, current) == (1, 1)


def test_sync_preserves_historical_scopes_without_running_cleanup(config_base, monkeypatch):
    from cw_platform import config_base as config_module
    from cw_platform.orchestrator import Orchestrator
    from test_interactive_sync_features import feature_item, feature_setup

    import services.pair_state as pair_state

    def unexpected(*args, **kwargs):
        raise RuntimeError("pair_scope_cleanup_failed")

    monkeypatch.setattr(pair_state, "prune_pair_scopes", unexpected)
    item = feature_item("history", 1)
    cfg, _, _ = feature_setup(config_base, monkeypatch, "history", [item], [item], "one-way")
    (config_base / "config.json").write_text("{}")
    monkeypatch.setattr(config_module, "load_config", lambda: cfg)
    assert not Orchestrator(cfg).run()["errors"]
    old = pair_feature_scope(cfg, cfg["pairs"][0], "history")
    assert pair_state_counts(config_base, old)[1] > 0
    cfg["pairs"][0]["mode"] = "two-way"
    assert not Orchestrator(cfg).run()["errors"]
    current = pair_feature_scope(cfg, cfg["pairs"][0], "history")
    assert pair_state_counts(config_base, old)[1] > 0
    assert pair_state_counts(config_base, current)[1] > 0


def test_pair_deletion_removes_owned_historical_scopes(config_base, monkeypatch):
    from api import syncAPI

    pair = {"id": "one", "source": "PLEX", "target": "TRAKT"}
    other = {**pair, "id": "two"}
    cfg = {"pairs": [pair, other]}
    old = seed(config_base, cfg, pair)
    pair["mode"] = "two-way"
    current = seed(config_base, cfg, pair)
    retained = seed(config_base, cfg, other)
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: cfg, lambda cfg: None))
    monkeypatch.setattr(syncAPI, "_cw_state_dir", lambda: config_base / ".cw_state")
    monkeypatch.setattr(syncAPI, "_managed_pair_blocked", lambda *args: False)
    result = syncAPI.api_pairs_delete("one", True, None)
    assert result["ok"] and result["state_errors"] == 0
    assert pair_state_counts(config_base, old) == (0, 0)
    assert pair_state_counts(config_base, current) == (0, 0)
    assert pair_state_counts(config_base, retained) == (1, 1)


def test_raw_configuration_fallback_cannot_prune_scopes(config_base, monkeypatch):
    from api import maintenanceAPI
    from cw_platform import config_base as config_module

    cfg = {"pairs": [{"id": "one", "source": "PLEX", "target": "TRAKT"}]}
    scope = seed(config_base, cfg, cfg["pairs"][0])
    (config_base / "config.json").write_text(json.dumps(cfg))
    monkeypatch.setattr(maintenanceAPI, "_cw", lambda: (None, config_base, config_base / ".cw_state"))

    def unavailable():
        raise ValueError("cannot decrypt")

    monkeypatch.setattr(config_module, "load_config", unavailable)
    assert maintenanceAPI._load_config_for_state_prune(config_base) is None
    assert maintenanceAPI.prune_state_file()["error"] == "config_unavailable"
    assert maintenanceAPI.maintenance_database_health()["pair_scopes"]["deferred"]
    assert pair_state_counts(config_base, scope) == (1, 1)


@pytest.mark.parametrize("disabled", [False, True])
def test_partial_user_resolution_defers_only_affected_pair(config_base, disabled):
    from test_sync_users import _pair_cfg, LINKS
    from cw_platform.orchestrator._pairs import expand_sync_user_pairs

    cfg = _pair_cfg(match_users=True, user_matches=LINKS)
    user_scopes = [seed(config_base, cfg, pair) for pair in expand_sync_user_pairs(cfg)]
    other = {"id": "other", "source": "PLEX", "target": "TRAKT"}
    cfg["pairs"].append(other)
    stale = seed(config_base, cfg, other)
    other["mode"] = "two-way"
    current = seed(config_base, cfg, other)
    cfg["pairs"][0]["enabled"] = not disabled
    cfg["emby"]["sync_users"]["u3"]["username"] = ""
    inventory = stale_pair_scopes(config_base, cfg)
    assert inventory["deferred_pairs"] == ["p1"]
    assert inventory["scopes"] == [stale]
    prune_pair_scopes(config_base, cfg)
    assert all(pair_state_counts(config_base, scope) == (1, 1) for scope in user_scopes)
    assert pair_state_counts(config_base, stale) == (0, 0)
    assert pair_state_counts(config_base, current) == (1, 1)


@pytest.mark.parametrize("path", ["restart", "clear-metadata-cache", "clear-cache", "database-health", "events-health"])
def test_unrelated_and_read_only_routes_do_not_take_sync_lock(config_base, monkeypatch, path):
    from fastapi import Request
    from api import maintenanceAPI

    monkeypatch.setattr(maintenanceAPI, "_cw", lambda: (None, config_base))
    with exclusive_operation(config_base):
        guard = maintenanceAPI._maintenance_guard(Request({"type": "http", "method": "POST", "path": f"/api/maintenance/{path}", "headers": []}))
        next(guard)
        guard.close()


def test_read_only_job_runs_during_sync_and_still_excludes_other_jobs(config_base):
    entered, release = threading.Event(), threading.Event()

    def work(progress):
        entered.set()
        release.wait(5)
        return {"ok": True}

    with exclusive_operation(config_base):
        job = maintenance_jobs.start(config_base, "database-health", work)
        try:
            assert entered.wait(5)
            assert job["ok"]
            assert maintenance_jobs.start(config_base, "events-optimize", work)["error"] == "maintenance_busy"
        finally:
            release.set()
    wait_job(config_base)


def wait_job(base):
    for _ in range(500):
        job = maintenance_jobs.status(base)
        if job["state"] not in {"running", "queued"}:
            return job
        time.sleep(.01)
    raise AssertionError("Maintenance job did not finish")


def test_failed_result_persistence_keeps_truthful_in_memory_result(config_base, monkeypatch):
    save = maintenance_jobs._save

    def fail_final(base, job):
        if job["state"] == "completed":
            raise OSError("disk full")
        save(base, job)

    monkeypatch.setattr(maintenance_jobs, "_save", fail_final)
    maintenance_jobs.start(config_base, "events-optimize", lambda progress: {"ok": True})
    result = wait_job(config_base)
    assert result["state"] == "completed"
    assert result["result"]["warnings"] == ["status_persistence_failed"]
    with exclusive_operation(config_base):
        pass


def test_base_exception_does_not_leave_job_running(config_base):
    def work(progress):
        raise SystemExit("worker stopped")

    maintenance_jobs.start(config_base, "events-optimize", work)
    assert wait_job(config_base)["state"] == "failed"
    with exclusive_operation(config_base):
        pass


def test_sync_lock_waits_for_maintenance_and_can_cancel(config_base):
    entered = threading.Event()

    def wait():
        with exclusive_operation(config_base, wait=True):
            entered.set()

    with exclusive_operation(config_base):
        worker = threading.Thread(target=wait)
        worker.start()
        assert not entered.wait(.1)
    worker.join(3)
    assert entered.is_set()
    from cw_platform.run_control import SyncCancelled

    with exclusive_operation(config_base), pytest.raises(SyncCancelled):
        with exclusive_operation(config_base, wait=True, cancelled=lambda: True):
            pass


def test_operation_lock_excludes_another_process(config_base):
    import subprocess
    import sys

    code = "from cw_platform.local_db.operation_lock import exclusive_operation\nimport sys\ntry:\n with exclusive_operation(sys.argv[1]):\n  print('acquired')\nexcept RuntimeError:\n print('busy')\n"
    with exclusive_operation(config_base):
        result = subprocess.run([sys.executable, "-c", code, str(config_base)], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "busy"
    result = subprocess.run([sys.executable, "-c", code, str(config_base)], capture_output=True, text=True, timeout=15)
    assert result.stdout.strip() == "acquired"


def test_pair_edit_keeps_old_baselines_and_delete_reports_cleanup_warning(config_base, monkeypatch):
    from api import syncAPI

    pair = {"id": "one", "source": "PLEX", "target": "TRAKT"}
    cfg = {"pairs": [pair]}
    scope = seed(config_base, cfg, pair)
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: cfg, lambda cfg: None))
    monkeypatch.setattr(syncAPI, "_managed_pair_blocked", lambda *args: False)
    result = syncAPI.api_pairs_update("one", syncAPI.PairPatch(mode="two-way"), None)
    assert result["ok"] and pair["mode"] == "two-way"
    assert pair_state_counts(config_base, scope) == (1, 1)

    def fail(*args, **kwargs):
        raise OSError("locked cleanup file")

    monkeypatch.setattr(syncAPI, "_purge_pair_state", fail)
    result = syncAPI.api_pairs_delete("one", True, None)
    assert result["ok"] and result["deleted"] == 1
    assert result["warnings"] == ["pair_state_cleanup_failed"]
    assert cfg["pairs"] == []
    update = next(route for route in syncAPI.router.routes if route.path == "/api/pairs/{pair_id}" and "PUT" in route.methods)
    assert not any(dep.call == syncAPI._pair_mutation_guard for dep in update.dependant.dependencies)


@pytest.mark.parametrize("configured", [False, True])
def test_noninteractive_dry_run_acquires_sync_lock(config_base, monkeypatch, configured):
    from cw_platform.orchestrator import Orchestrator
    from cw_platform import run_control
    from test_interactive_sync_features import feature_item, feature_setup

    item = feature_item("history", 1)
    cfg, _, _ = feature_setup(config_base, monkeypatch, "history", [item], [item], "one-way")
    cfg.setdefault("sync", {})["dry_run"] = configured
    monkeypatch.setattr(run_control, "cancel_requested", lambda: True)
    with exclusive_operation(config_base), pytest.raises(run_control.SyncCancelled):
        Orchestrator(cfg).run(dry_run=not configured)


def test_large_scope_cleanup_commits_in_batches(config_base):
    pair = {"id": "one", "source": "PLEX", "target": "TRAKT"}
    scope = seed(config_base, {"pairs": [pair]}, pair)
    conn = get_conn(config_base)
    parent = conn.execute("SELECT id FROM pair_feature_state WHERE pair_scope=?", (scope,)).fetchone()[0]
    with conn:
        conn.executemany("INSERT INTO pair_baseline_items(provider_state_id,item_key,updated_at) VALUES(?,?,0)",
                         ((parent, f"item:{i}") for i in range(2500)))
    statements = []
    conn.set_trace_callback(statements.append)
    try:
        from cw_platform.local_db.state import clear_pair_state

        assert clear_pair_state(config_base, scope) == (1, 2501)
    finally:
        conn.set_trace_callback(None)
    assert sum(sql == "COMMIT" for sql in statements) >= 4
    assert pair_state_counts(config_base, scope) == (0, 0)


def test_idless_user_pairs_preserve_historical_positional_owners(config_base):
    from test_sync_users import _pair_cfg, LINKS
    from cw_platform.orchestrator._pairs import expand_sync_user_pairs

    cfg = _pair_cfg(match_users=True, user_matches=LINKS)
    cfg["pairs"][0].pop("id")
    cfg["pairs"].append({**deepcopy(cfg["pairs"][0]), "mode": "one-way"})
    scopes = []
    for index, pair in enumerate(expand_sync_user_pairs(cfg), 1):
        scope = pair_feature_scope(cfg, pair, "history", index)
        scopes.append(scope)
        StateStore(config_base).for_pair(scope).save_feature_blocks({
            ("EMBY", pair["source_instance"], "history"): {"baseline": {"items": {"one": {"title": "One", "type": "movie"}}}}
        })
    prune_pair_scopes(config_base, cfg)
    assert len(scopes) == 4
    assert {row[0] for row in get_conn(config_base).execute("SELECT pair_id FROM pair_scope_owners")} == {"1", "2", "3", "4"}
    cfg["emby"]["sync_users"]["u3"]["username"] = ""
    assert stale_pair_scopes(config_base, cfg)["scopes"] == []
    assert prune_pair_scopes(config_base, cfg)["items"] == 0
    assert all(pair_state_counts(config_base, scope) == (1, 1) for scope in scopes)


def test_interactive_preview_still_skips_sync_lock(config_base, monkeypatch):
    from cw_platform.orchestrator import Orchestrator
    from cw_platform.orchestrator._interactive import InteractivePlan
    from test_interactive_sync_features import feature_item, feature_setup

    item = feature_item("history", 1)
    cfg, _, _ = feature_setup(config_base, monkeypatch, "history", [item], [item], "one-way")
    with exclusive_operation(config_base):
        assert not Orchestrator(cfg, interactive=InteractivePlan()).run(dry_run=True)["errors"]
