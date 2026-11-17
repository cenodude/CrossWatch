# tests/test_scheduler_persistence.py
# CrossWatch - Scheduler last-run history across restarts.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from cw_platform.local_db import close_conn, get_conn
from services import scheduling


@pytest.fixture
def scheduler_config(config_base, monkeypatch):
    monkeypatch.setenv("CROSSWATCH_DB", str(config_base / "scheduler.sqlite3"))
    now = datetime(2026, 3, 15, 8, 5)
    monkeypatch.setattr(scheduling, "_now_ts", lambda: int(now.timestamp()))
    monkeypatch.setattr(scheduling, "_as_now_in_tz", lambda _tz=None: now)
    yield {"pairs": [{"id": "pair", "enabled": True}], "scheduling": {
        "enabled": True, "mode": "hourly", "jitter_seconds": 0, "advanced": {"enabled": True},
    }}
    close_conn()


def scheduler(cfg, ok=True, **kwargs):
    return scheduling.SyncScheduler(lambda: cfg, lambda _cfg: None, lambda _payload=None: ok, **kwargs)


@pytest.mark.parametrize("ok", [True, False])
@pytest.mark.parametrize("kind", ["standard", "trigger", "jobs", "workflows", "capture_jobs", "backup_jobs"])
def test_last_run_survives_restart(scheduler_config, kind, ok):
    cfg = scheduler_config
    jobs = {
        "jobs": {"id": "job", "pair_id": "pair", "at": "08:00"},
        "workflows": {"id": "workflow", "mode": "daily_time", "daily_time": "08:00", "steps": [{"id": "step", "pair_id": "pair"}]},
        "capture_jobs": {"id": "capture", "provider": "TRAKT", "feature": "history", "at": "08:00"},
        "backup_jobs": {"id": "backup", "at": "08:00"},
    }
    if kind in jobs:
        cfg["scheduling"]["advanced"][kind] = [jobs[kind]]
    worker = scheduler(cfg, ok)
    if kind == "standard":
        worker._std_run_due()
    elif kind == "trigger":
        worker.trigger_payload()
    else:
        worker._adv_run_due(cfg["scheduling"], None)
    before = worker.status()
    assert before["last_run_at"] == scheduling._now_ts()
    assert before["last_run_ok"] is ok
    worker._set_busy(True)
    close_conn()
    restored = scheduler(cfg).status()
    assert {key: restored[key] for key in ("last_run_at", "last_run_ok", "last_error")} == {
        key: before[key] for key in ("last_run_at", "last_run_ok", "last_error")
    }
    assert restored["running"] is False and restored["busy"] is False
    assert restored["next_run_at"] == 0


@pytest.mark.parametrize("ok", [True, False])
def test_profile_and_main_history_survive_restart_independently(scheduler_config, monkeypatch, ok):
    cfg = scheduler_config
    profile = "22222222222242228222222222222222"
    worker = scheduler(cfg)
    worker._std_run_due()
    main_at = worker.status()["last_run_at"]
    cfg["user_profiles"] = {profile: {"label": "Family", "instances": {}}}
    cfg["pairs"][0]["profile_id"] = profile
    cfg["scheduling"]["profiles"] = {profile: {"enabled": True, "mode": "hourly"}}
    monkeypatch.setattr(scheduling, "_now_ts", lambda: main_at + 120)
    monkeypatch.setattr(scheduling, "compute_next_run", lambda now, _sch: now - timedelta(minutes=1))
    worker = scheduler(cfg, ok)
    assert worker._profiles_tick(cfg["scheduling"], None)
    close_conn()
    restored = scheduler(cfg).status()
    assert restored["last_run_at"] == main_at and restored["last_run_ok"] is True
    assert restored["profiles"][profile]["last_run_at"] == main_at + 120
    assert restored["profiles"][profile]["last_run_ok"] is ok


def test_busy_trigger_does_not_overwrite_last_run(scheduler_config, monkeypatch):
    worker = scheduler(scheduler_config)
    worker.trigger_payload()
    recorded = worker.status()["last_run_at"]
    monkeypatch.setattr(scheduling, "_now_ts", lambda: recorded + 120)
    assert not scheduler(scheduler_config, is_sync_running_fn=lambda: True).trigger_payload()
    assert scheduler(scheduler_config).status()["last_run_at"] == recorded


@pytest.mark.parametrize("saved", ["not json", "[]", '{"": {"last_run_at": "invalid", "last_run_ok": true}}'])
def test_invalid_saved_history_keeps_empty_state(scheduler_config, saved):
    conn = get_conn()
    with conn:
        conn.execute("INSERT INTO local_meta(key,value_text,value_type,updated_at) VALUES('scheduler_last_runs',?,'text',0)", (saved,))
    status = scheduler(scheduler_config).status()
    assert status["last_run_at"] == 0 and status["last_run_ok"] is None


def test_storage_failure_does_not_fail_scheduled_run(scheduler_config, monkeypatch):
    worker = scheduler(scheduler_config)
    def unavailable():
        raise OSError("database unavailable")
    monkeypatch.setattr(scheduling, "get_conn", unavailable)
    assert worker.trigger_payload()
    assert worker.status()["last_run_ok"] is True
