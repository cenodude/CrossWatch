# tests/test_backup_scheduling.py
# CrossWatch - Independent automatic backup and sync schedules
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import threading
from copy import deepcopy
from datetime import datetime, timedelta
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import backupsAPI, configAPI, schedulingAPI
from cw_platform import config_base
from services import health_details, scheduling


@pytest.fixture
def saved_config(monkeypatch):
    cfg = {"scheduling": {"enabled": True, "mode": "daily_time", "daily_time": "11:00", "advanced": {"enabled": False}}}

    def save(value):
        normalized = deepcopy(value)
        config_base._normalize_scheduling(normalized)
        cfg.clear()
        cfg.update(normalized)

    worker = Mock()
    worker.status.return_value = {}
    monkeypatch.setattr(config_base, "load_config", lambda: deepcopy(cfg))
    monkeypatch.setattr(config_base, "save_config", save)
    monkeypatch.setattr(backupsAPI, "_refresh_scheduler", worker.refresh)
    monkeypatch.setattr(schedulingAPI, "_env", lambda: (lambda: deepcopy(cfg), save, worker, {}, lambda _: 0, Mock()))
    return cfg, worker


@pytest.mark.parametrize("advanced_enabled", [False, True])
def test_saving_backups_preserves_sync_plan(saved_config, advanced_enabled):
    cfg, _ = saved_config
    cfg["scheduling"]["advanced"]["enabled"] = advanced_enabled
    if advanced_enabled:
        cfg["scheduling"]["advanced"]["jobs"] = [{"id": "sync", "pair_id": "pair", "at": "12:00"}]
    for enabled in (True, False, True):
        response = backupsAPI.api_backups_schedule_post({"enabled": enabled, "at": "08:00", "days": [7]})
        assert response.status_code == 200
        sch = schedulingAPI.sched_get()
        assert sch["enabled"] is True
        assert sch["daily_time"] == "11:00"
        assert sch["advanced"]["enabled"] is advanced_enabled
        reloaded = json.loads(backupsAPI.api_backups_schedule_get().body)
        assert reloaded["schedule"]["active"] is enabled


@pytest.mark.parametrize("field", ["backup_jobs", "backupJobs"])
@pytest.mark.parametrize("empty_canonical", [False, True])
def test_saving_sync_plan_preserves_backup_jobs(saved_config, field, empty_canonical):
    cfg, worker = saved_config
    jobs = [{"id": "app-backup-default", "active": True, "at": "08:00", "days": [7]}]
    if empty_canonical:
        cfg["scheduling"]["advanced"]["backup_jobs"] = []
    cfg["scheduling"]["advanced"][field] = deepcopy(jobs)
    for enabled in (True, False, True):
        result = schedulingAPI.sched_post({"enabled": enabled, "mode": "daily_time", "daily_time": "11:00", "advanced": {"enabled": False, "jobs": []}})
        assert result["ok"] is True
        assert schedulingAPI.sched_get()["enabled"] is enabled
        assert schedulingAPI.sched_get()["advanced"]["backup_jobs"] == jobs
        assert json.loads(backupsAPI.api_backups_schedule_get().body)["schedule"] == jobs[0]
    assert worker.stop.call_count == 0
    assert worker.start.call_count == 3
    assert "backupJobs" not in schedulingAPI.sched_get()["advanced"]


def test_explicit_backup_job_removal_is_preserved(saved_config):
    cfg, _ = saved_config
    cfg["scheduling"]["advanced"]["backup_jobs"] = [{"id": "backup", "active": True, "at": "08:00"}]
    schedulingAPI.sched_post({"enabled": False, "advanced": {"enabled": False, "backup_jobs": []}})
    assert schedulingAPI.sched_get()["advanced"]["backup_jobs"] == []


def test_config_save_keeps_backup_only_worker_running(saved_config):
    cfg, worker = saved_config
    cfg["scheduling"]["enabled"] = False
    cfg["scheduling"]["advanced"]["backup_jobs"] = [{"id": "backup", "active": True, "at": "08:00"}]
    configAPI._after_config_save({"scheduler": worker}, cfg)
    worker.start.assert_called_once()
    worker.refresh.assert_called_once()
    worker.stop.assert_not_called()
    cfg["scheduling"]["advanced"]["backup_jobs"][0]["active"] = False
    assert scheduling.scheduler_should_run(cfg) is False


def test_backup_only_worker_is_included_in_health(saved_config):
    cfg, _ = saved_config
    cfg["scheduling"]["enabled"] = False
    cfg["scheduling"]["advanced"]["backup_jobs"] = [{"id": "backup", "active": True, "at": "08:00"}]
    issues = []
    status = health_details.scheduler_health(cfg, {"running": False}, False, 0, issues)
    assert status["enabled"] is True
    assert issues == [{"level": "failing", "component": "scheduler", "message": "Scheduler is enabled but not running"}]


@pytest.mark.parametrize("standard_enabled", [False, True])
@pytest.mark.parametrize("backup_time", ["08:00", "08:14"])
def test_scheduler_runs_backups_independently_of_sync_plan(monkeypatch, standard_enabled, backup_time):
    now = datetime(2026, 3, 15, 7, 59, 30)
    finish = now + timedelta(minutes=20)
    monkeypatch.setattr(scheduling, "_as_now_in_tz", lambda _tz=None: now)
    monkeypatch.setattr(scheduling, "_now_ts", lambda: int(now.timestamp()))
    cfg = {"scheduling": {
        "enabled": standard_enabled,
        "mode": "custom_interval",
        "custom_interval_minutes": 15,
        "jitter_seconds": 0,
        "advanced": {
            "enabled": False,
            "jobs": [{"id": "disabled-sync", "pair_id": "pair", "at": "08:00"}],
            "backup_jobs": [{"id": "backup", "active": True, "at": backup_time, "days": [7]}],
        },
    }}
    runs = []
    next_runs = []
    worker = scheduling.SyncScheduler(lambda: cfg, lambda _: None, lambda payload=None: runs.append((now, payload)) or True, is_sync_running_fn=lambda: False)

    def tick(seconds):
        nonlocal now
        next_runs.append(worker.status()["next_run_at"])
        now += timedelta(seconds=max(seconds, 0.5))
        if now >= finish:
            worker._stop.set()

    monkeypatch.setattr(worker, "_sleep_or_poke", tick)
    assert scheduling.scheduler_should_run(cfg) is True
    worker._loop()
    backup_at = datetime(2026, 3, 15, 8, int(backup_time.split(":")[1]))
    assert next_runs[0] == int(backup_at.timestamp())
    assert [(at, payload["scheduler_mode"]) for at, payload in runs] == [
        (backup_at, "advanced_backup"),
        *([(datetime(2026, 3, 15, 8, 14), "standard")] if standard_enabled else []),
    ]


def test_backup_does_not_run_again_after_switching_sync_plan(monkeypatch):
    now = datetime(2026, 3, 15, 8)
    monkeypatch.setattr(scheduling, "_as_now_in_tz", lambda _tz=None: now)
    cfg = {"scheduling": {"advanced": {"enabled": False, "backup_jobs": [{"id": "backup", "at": "08:00", "active": True}]}}}
    runs = []
    worker = scheduling.SyncScheduler(lambda: cfg, lambda _: None, lambda payload=None: runs.append(payload) or True, is_sync_running_fn=lambda: False)
    assert worker._adv_run_due(cfg["scheduling"], None) is True
    cfg["scheduling"]["advanced"]["enabled"] = True
    assert worker._adv_run_due(cfg["scheduling"], None) is False
    assert len(runs) == 1


def test_backup_save_preserves_other_legacy_jobs(saved_config):
    cfg, _ = saved_config
    legacy = {"id": "other-backup", "at": "09:00", "active": True}
    cfg["scheduling"]["advanced"].update({"backup_jobs": [], "backupJobs": [legacy]})
    response = backupsAPI.api_backups_schedule_post({"enabled": True, "at": "08:00"})
    assert response.status_code == 200
    adv = schedulingAPI.sched_get()["advanced"]
    assert adv["backup_jobs"][1] == legacy
    assert "backupJobs" not in adv


@pytest.mark.parametrize("enabled", [False, True])
def test_old_backup_conflict_recovers_on_load_and_save(tmp_path, monkeypatch, enabled):
    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    config_base.invalidate_config_cache()
    job = {"id": "app-backup-default", "at": "08:00", "active": enabled}
    (tmp_path / "config.json").write_text(json.dumps({"scheduling": {
        "enabled": True, "mode": "daily_time", "daily_time": "11:00",
        "advanced": {"enabled": True, "backup_jobs": [], "backupJobs": [job]},
    }}), encoding="utf-8")
    monkeypatch.setattr(backupsAPI, "_refresh_scheduler", lambda: None)
    try:
        cfg = config_base.load_config()
        assert cfg["scheduling"]["enabled"] is True
        assert cfg["scheduling"]["advanced"]["enabled"] is False
        assert scheduling.SyncScheduler(lambda: cfg, lambda _: None, lambda _: True)._effective(cfg["scheduling"])["mode"] == "daily_time"
        response = backupsAPI.api_backups_schedule_post({"enabled": enabled, "at": "08:00"})
        assert response.status_code == 200
        config_base.invalidate_config_cache()
        loaded = config_base.load_config()["scheduling"]
        assert loaded["enabled"] is True
        assert loaded["daily_time"] == "11:00"
        assert loaded["advanced"]["enabled"] is False
        assert loaded["advanced"]["backup_jobs"][0]["active"] is enabled
    finally:
        config_base.invalidate_config_cache()


@pytest.mark.parametrize("field", ["jobs", "workflows", "capture_jobs", "captureJobs", "event_rules", "eventRules"])
def test_backup_recovery_preserves_advanced_content(field):
    cfg = {"scheduling": {"enabled": True, "mode": "daily_time", "advanced": {
        "enabled": True, field: [{"id": "draft", "active": False}],
        "backup_jobs": [{"id": "app-backup-default", "at": "08:00"}],
    }}}
    config_base._normalize_scheduling(cfg)
    assert cfg["scheduling"]["advanced"]["enabled"] is True
    assert cfg["scheduling"]["advanced"][field][0]["id"] == "draft"


@pytest.mark.parametrize("standard_enabled,mode,job_id", [(False, "daily_time", "app-backup-default"), (True, "disabled", "app-backup-default"), (True, "daily_time", "custom-backup")])
def test_backup_recovery_requires_evidence_of_original_conflict(standard_enabled, mode, job_id):
    cfg = {"scheduling": {"enabled": standard_enabled, "mode": mode, "advanced": {
        "enabled": True, "backup_jobs": [{"id": job_id, "at": "08:00"}],
    }}}
    config_base._normalize_scheduling(cfg)
    assert cfg["scheduling"]["advanced"]["enabled"] is True


@pytest.mark.parametrize("change", ["disable", "advanced", "reschedule"])
def test_standard_plan_is_revalidated_after_backup_wait(monkeypatch, change):
    now = datetime(2026, 3, 15, 7, 59, 30)
    busy = True
    changed = False
    cfg = {"scheduling": {
        "enabled": True, "mode": "daily_time", "daily_time": "08:00", "jitter_seconds": 0,
        "advanced": {"enabled": False, "backup_jobs": [{"id": "backup", "at": "08:00", "active": True}]},
    }}
    monkeypatch.setattr(scheduling, "_as_now_in_tz", lambda _tz=None: now)
    monkeypatch.setattr(scheduling, "_now_local_naive", lambda: now)
    monkeypatch.setattr(scheduling, "_now_ts", lambda: int(now.timestamp()))
    runs = []
    worker = scheduling.SyncScheduler(lambda: deepcopy(cfg), lambda _: None, lambda payload=None: runs.append(payload) or True,
                                      is_sync_running_fn=lambda: busy and now.hour >= 8)

    def tick(seconds):
        nonlocal now, busy, changed
        if seconds == 2 and not changed:
            changed = True
            if change == "disable":
                cfg["scheduling"]["enabled"] = False
            elif change == "advanced":
                cfg["scheduling"]["enabled"] = False
                cfg["scheduling"]["advanced"].update({"enabled": True, "jobs": [{"id": "later", "pair_id": "pair", "at": "09:00"}]})
            else:
                cfg["scheduling"]["daily_time"] = "09:00"
            busy = False
        elif runs or now >= datetime(2026, 3, 15, 8, 1):
            worker._stop.set()
        now += timedelta(seconds=seconds)

    monkeypatch.setattr(worker, "_sleep_or_poke", tick)
    worker._loop()
    assert changed is True
    assert [run["scheduler_mode"] for run in runs] == ["advanced_backup"]


@pytest.mark.parametrize("zone", ["Asia/Tokyo", "America/New_York"])
@pytest.mark.parametrize("mode", ["daily_time", "hourly", "every_n_hours", "custom_interval", "backup"])
def test_next_run_uses_absolute_time_across_timezones(monkeypatch, zone, mode):
    tz = ZoneInfo(zone)
    now = datetime(2026, 3, 15, 15, tzinfo=tz)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls.fromtimestamp(now.timestamp(), tz=tz)

    monkeypatch.setattr(scheduling, "datetime", Clock)
    monkeypatch.setattr(scheduling, "_now_ts", lambda: int(now.timestamp()))
    cfg = {"scheduling": {
        "enabled": mode != "backup", "mode": mode if mode != "backup" else "disabled", "daily_time": "17:00",
        "every_n_hours": 2, "custom_interval_minutes": 120, "timezone": zone, "jitter_seconds": 0,
        "advanced": {"enabled": False, "backup_jobs": [{"id": "backup", "at": "16:00", "active": True}]},
    }}
    worker = scheduling.SyncScheduler(lambda: cfg, lambda _: None, lambda _: True, is_sync_running_fn=lambda: False)
    waits = []

    def stop(seconds):
        waits.append(seconds)
        worker._stop.set()

    monkeypatch.setattr(worker, "_sleep_or_poke", stop)
    worker._loop()
    expected = datetime(2026, 3, 15, 16, tzinfo=tz)
    assert worker.status()["next_run_at"] == int(expected.timestamp())
    assert waits == [30.0]
    if mode != "backup":
        standard_at = datetime(2026, 3, 15, 16 if mode == "hourly" else 17, tzinfo=tz)
        assert worker._std_next_ts == int(standard_at.timestamp())


@pytest.fixture
def waiting_backup(monkeypatch, request):
    options = getattr(request, "param", {})
    clock = {"now": options.get("now", datetime(2026, 3, 15, 8))}
    monkeypatch.setattr(scheduling, "_as_now_in_tz", lambda tz=None: datetime.fromtimestamp(clock["now"].timestamp(), tz=tz).replace(tzinfo=None))
    monkeypatch.setattr(scheduling, "_now_ts", lambda: int(clock["now"].timestamp()))
    cfg = {"scheduling": {
        "enabled": False, "mode": "disabled", "jitter_seconds": 0,
        "advanced": {"enabled": False, "backup_jobs": [{"id": "app-backup-default", "at": options.get("at", "08:00"), "days": options.get("days", []), "active": True}]},
    }}
    lock = threading.Lock()
    busy = threading.Event()
    busy.set()
    waiting = threading.Event()
    idle = threading.Event()
    dispatched = threading.Event()
    runs = []

    def load():
        with lock:
            return deepcopy(cfg)

    def save(value):
        with lock:
            cfg.clear()
            cfg.update(deepcopy(value))

    def run(payload):
        runs.append((payload, busy.is_set(), load()["scheduling"]["advanced"]["backup_jobs"]))
        dispatched.set()
        return True

    worker = scheduling.SyncScheduler(load, save, run, is_sync_running_fn=busy.is_set)
    sleep = worker._sleep_or_poke

    def observe_wait(seconds):
        if worker.status()["busy"]:
            waiting.set()
        else:
            idle.set()
        sleep(seconds)

    monkeypatch.setattr(worker, "_sleep_or_poke", observe_wait)
    monkeypatch.setattr(config_base, "load_config", load)
    monkeypatch.setattr(config_base, "save_config", save)
    monkeypatch.setattr(backupsAPI, "_refresh_scheduler", worker.refresh)
    monkeypatch.setattr(configAPI, "_env", lambda: {"load": load, "save": save, "cfg_base": config_base, "scheduler": worker})
    monkeypatch.setattr(configAPI, "_watch_runtime_changed", lambda *_: False)
    app = FastAPI()
    app.include_router(configAPI.router)
    app.include_router(backupsAPI.router)
    with TestClient(app) as client:
        worker.start()
        thread = worker._thread
        try:
            assert waiting.wait(3), "Scheduler did not wait for the busy sync"
            yield client, worker, busy, idle, dispatched, runs, load, save, clock
        finally:
            worker.stop()
            assert thread is not None and not thread.is_alive()


@pytest.mark.parametrize("endpoint", ["/api/config", "/api/backups/schedule"])
def test_disabling_waiting_backup_cancels_dispatch(waiting_backup, endpoint):
    client, worker, busy, idle, dispatched, runs, load, _, _ = waiting_backup
    if endpoint == "/api/config":
        payload = load()
        payload["scheduling"]["advanced"]["backup_jobs"][0]["active"] = False
    else:
        payload = {"enabled": False, "at": "08:00"}
    response = client.post(endpoint, json=payload)
    assert response.status_code == 200
    assert not load()["scheduling"]["advanced"]["backup_jobs"][0]["active"]
    assert busy.is_set()
    if endpoint == "/api/config":
        assert worker.status()["running"] is False
    else:
        assert idle.wait(3), "Refresh did not cancel the pending backup while sync stayed busy"
        idle.clear()
        busy.clear()
        worker.refresh()
        assert idle.wait(3)
    assert runs == []
    assert not dispatched.is_set()


def test_stopping_waiting_backup_never_dispatches(waiting_backup):
    _, worker, busy, _, dispatched, runs, _, _, _ = waiting_backup
    worker.stop()
    assert busy.is_set()
    assert worker.status()["running"] is False
    assert not dispatched.is_set()
    assert runs == []


@pytest.mark.parametrize("change", ["remove", "time", "days", "timezone"])
def test_changing_waiting_backup_rechecks_whether_it_is_due(waiting_backup, change):
    client, worker, busy, idle, dispatched, runs, load, _, _ = waiting_backup
    payload = load()
    payload["scheduling"].update({"enabled": True, "mode": "custom_interval", "custom_interval_minutes": 60})
    jobs = payload["scheduling"]["advanced"]["backup_jobs"]
    if change == "remove":
        jobs.clear()
    elif change == "time":
        jobs[0]["at"] = "09:00"
    elif change == "days":
        jobs[0]["days"] = [1]
    else:
        payload["scheduling"]["timezone"] = "Asia/Tokyo"
    assert client.post("/api/config", json=payload).status_code == 200
    assert idle.wait(3), "Changed backup kept waiting instead of being reevaluated"
    idle.clear()
    busy.clear()
    worker.refresh()
    assert idle.wait(3)
    assert not dispatched.is_set()
    assert runs == []


def test_waiting_backup_uses_updated_settings_once_sync_finishes(waiting_backup):
    client, worker, busy, idle, dispatched, runs, _, _, _ = waiting_backup
    response = client.post("/api/backups/schedule", json={"enabled": True, "at": "08:00", "scope": "config_only"})
    assert response.status_code == 200
    assert not dispatched.is_set()
    busy.clear()
    worker.refresh()
    assert dispatched.wait(3), "Enabled backup did not run after sync finished"
    assert idle.wait(3)
    idle.clear()
    worker.refresh()
    assert idle.wait(3)
    assert len(runs) == 1
    payload, was_busy, jobs = runs[0]
    assert was_busy is False
    assert jobs[0]["active"] is True
    assert payload["backup"]["scope"] == "config_only"


@pytest.mark.parametrize("waiting_backup", [
    {"now": datetime(2026, 3, 15, 23, 59), "at": "23:59", "days": [7]},
    {"now": datetime(2026, 3, 15, 23, 59), "at": "23:59", "days": []},
], indirect=True)
def test_waiting_backup_keeps_original_occurrence_after_midnight(waiting_backup):
    client, worker, busy, idle, dispatched, runs, load, _, clock = waiting_backup
    clock["now"] = datetime(2026, 3, 16, 0, 1)
    payload = load()
    payload["scheduling"]["advanced"]["backup_jobs"][0]["scope"] = "config_only"
    assert client.post("/api/config", json=payload).status_code == 200
    busy.clear()
    worker.refresh()
    assert dispatched.wait(3), "Waiting backup was discarded at midnight"
    assert idle.wait(3)
    assert worker._adv_last_key["app-backup-default"] == "2026-03-15@23:59"
    days = payload["scheduling"]["advanced"]["backup_jobs"][0]["days"]
    expected = datetime(2026, 3, 22 if days else 16, 23, 59)
    assert worker.status()["next_run_at"] == int(expected.timestamp())
    idle.clear()
    worker.refresh()
    assert idle.wait(3)
    assert len(runs) == 1
    assert runs[0][1] is False
    assert runs[0][0]["backup"]["scope"] == "config_only"


@pytest.mark.parametrize("waiting_backup", [{"now": datetime(2026, 3, 15, 8, 10), "at": "08:10", "days": [7]}], indirect=True)
def test_rescheduling_waiting_backup_backward_cancels_pending_occurrence(waiting_backup):
    client, worker, busy, idle, dispatched, runs, _, _, _ = waiting_backup
    response = client.post("/api/backups/schedule", json={"enabled": True, "at": "07:00", "days": [7]})
    assert response.status_code == 200
    busy.clear()
    worker.refresh()
    assert idle.wait(3)
    assert runs == []
    assert not dispatched.is_set()
    idle.clear()
    worker.refresh()
    assert idle.wait(3)
    assert runs == []
