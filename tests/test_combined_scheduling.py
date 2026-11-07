# tests/test_combined_scheduling.py
# CrossWatch - Concurrent standard and advanced schedule plans
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
import ast
from pathlib import Path
import threading
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

from api import schedulingAPI
from cw_platform import config_base
from cw_platform import run_control
from services import scheduling


def config():
    return {
        "pairs": [{"id": "a", "enabled": True}, {"id": "b", "enabled": True}],
        "scheduling": {
            "enabled": True, "mode": "daily_time", "daily_time": "08:00", "jitter_seconds": 0,
            "_independent_plans_v1": True,
            "advanced": {"enabled": True, "jobs": [{"id": "first", "pair_id": "a", "at": "08:00"}]},
        },
    }


def simulate(monkeypatch, cfg, *, advance=None, on_run=None, finish=None):
    clock = {"now": datetime(2026, 3, 15, 7, 59, 30)}
    end = finish or datetime(2026, 3, 15, 8, 2)
    monkeypatch.setattr(scheduling, "_as_now_in_tz", lambda _tz=None: clock["now"])
    monkeypatch.setattr(scheduling, "_now_local_naive", lambda: clock["now"])
    monkeypatch.setattr(scheduling, "_now_ts", lambda: int(clock["now"].timestamp()))
    runs = []
    states = []

    def run(payload):
        runs.append((clock["now"], deepcopy(payload)))
        return on_run(payload, clock) if on_run else True

    worker = scheduling.SyncScheduler(lambda: deepcopy(cfg), lambda _: None, run, is_sync_running_fn=lambda: False)

    def tick(seconds):
        states.append(worker.status())
        clock["now"] += timedelta(seconds=max(seconds, 0.5))
        if advance:
            advance(cfg, worker, clock)
        if clock["now"] >= end:
            worker._stop.set()

    monkeypatch.setattr(worker, "_sleep_or_poke", tick)
    worker._loop()
    return runs, states, worker


@pytest.mark.parametrize("kind", ["jobs", "workflows"])
@pytest.mark.parametrize("success", [False, True])
def test_overlapping_plans_skip_accepted_advanced_dispatches(monkeypatch, kind, success):
    cfg = config()
    if kind == "workflows":
        cfg["scheduling"]["advanced"].update({"jobs": [], "workflows": [{
            "id": "chain", "mode": "daily_time", "daily_time": "08:00", "steps": [{"id": "step", "pair_id": "a"}],
        }]})
    runs, states, _ = simulate(monkeypatch, cfg, on_run=lambda payload, _: success if payload.get("pair_id") else True)
    assert len(runs) == 2
    assert runs[0][1]["pair_id"] == "a"
    assert runs[1][1]["scheduler_mode"] == "standard"
    assert runs[1][1].get("pair_scope_ids") == (["b"] if success else None)
    assert all(state["effective"]["mode"] == "combined" for state in states)
    assert states[0]["next_run_at"] == int(datetime(2026, 3, 15, 8).timestamp())


def test_all_pairs_already_dispatched_skips_standard_dispatch(monkeypatch):
    cfg = config()
    cfg["pairs"] = cfg["pairs"][:1]
    runs, _, _ = simulate(monkeypatch, cfg)
    assert [payload["scheduler_mode"] for _, payload in runs] == ["advanced"]


def test_separate_occurrences_do_not_skip_pairs(monkeypatch):
    cfg = config()
    cfg["scheduling"]["daily_time"] = "08:01"
    runs, _, _ = simulate(monkeypatch, cfg)
    assert [payload["scheduler_mode"] for _, payload in runs] == ["advanced", "standard"]
    assert "pair_scope_ids" not in runs[1][1]


@pytest.mark.parametrize("kind", ["jobs", "workflows"])
def test_async_advanced_run_delays_standard_and_preserves_duplicate_skip(monkeypatch, kind):
    cfg = config()
    if kind == "workflows":
        cfg["scheduling"]["advanced"].update({"jobs": [], "workflows": [{
            "id": "chain", "mode": "daily_time", "daily_time": "08:00", "steps": [{"id": "step", "pair_id": "a"}],
        }]})
    busy = threading.Event()
    started = threading.Event()
    waiting = threading.Event()
    idle = threading.Event()
    dispatched = threading.Event()
    now = datetime(2026, 3, 15, 7, 59, 30)
    monkeypatch.setattr(scheduling, "_as_now_in_tz", lambda _tz=None: now)
    monkeypatch.setattr(scheduling, "_now_local_naive", lambda: now)
    monkeypatch.setattr(scheduling, "_now_ts", lambda: int(now.timestamp()))
    runs = []

    def run(payload):
        assert not busy.is_set()
        runs.append(deepcopy(payload))
        if payload.get("pair_id"):
            busy.set()
            started.set()
        else:
            dispatched.set()
        return True

    worker = scheduling.SyncScheduler(lambda: deepcopy(cfg), lambda _: None, run, is_sync_running_fn=busy.is_set)
    sleep = worker._sleep_or_poke

    def observe(seconds):
        (waiting if started.is_set() else idle).set()
        sleep(seconds)

    monkeypatch.setattr(worker, "_sleep_or_poke", observe)
    worker.start()
    thread = worker._thread
    try:
        assert idle.wait(3)
        now = datetime(2026, 3, 15, 8)
        worker.refresh()
        assert started.wait(3)
        assert waiting.wait(3)
        assert len(runs) == 1
        now += timedelta(minutes=2)
        busy.clear()
        worker.refresh()
        assert dispatched.wait(3)
    finally:
        worker.stop()
        assert thread is not None and not thread.is_alive()
    assert len(runs) == 2
    assert runs[1]["scheduler_mode"] == "standard"
    assert runs[1]["pair_scope_ids"] == ["b"]


def test_overlap_skip_does_not_leak_into_next_standard_run(monkeypatch):
    cfg = config()
    cfg["scheduling"].update({"mode": "custom_interval", "custom_interval_minutes": 15})
    cfg["scheduling"]["advanced"]["jobs"][0]["at"] = "08:14"
    runs, _, _ = simulate(monkeypatch, cfg, finish=datetime(2026, 3, 15, 8, 31))
    assert [payload["scheduler_mode"] for _, payload in runs] == ["advanced", "standard", "standard"]
    assert runs[1][1]["pair_scope_ids"] == ["b"]
    assert "pair_scope_ids" not in runs[2][1]


def test_advanced_toggle_does_not_reset_standard_interval(monkeypatch):
    cfg = config()
    cfg["scheduling"].update({"mode": "custom_interval", "custom_interval_minutes": 15})
    cfg["scheduling"]["advanced"]["jobs"][0]["at"] = "09:00"

    def change(cfg, _worker, clock):
        cfg["scheduling"]["advanced"]["enabled"] = clock["now"].minute >= 5

    runs, _, _ = simulate(monkeypatch, cfg, advance=change, finish=datetime(2026, 3, 15, 8, 16))
    assert [(at.hour, at.minute, payload["scheduler_mode"]) for at, payload in runs] == [(8, 14, "standard")]


def test_standard_timer_is_set_before_long_advanced_run(monkeypatch):
    cfg = config()
    cfg["scheduling"].update({"mode": "custom_interval", "custom_interval_minutes": 15})

    def run(payload, clock):
        if payload.get("pair_id"):
            clock["now"] = datetime(2026, 3, 15, 8, 20)
        return True

    cfg["scheduling"]["advanced"]["jobs"][0]["at"] = "08:00"
    runs, _, _ = simulate(monkeypatch, cfg, on_run=run, finish=datetime(2026, 3, 15, 8, 21))
    assert [payload["scheduler_mode"] for _, payload in runs] == ["advanced", "standard"]
    assert runs[1][0] == datetime(2026, 3, 15, 8, 20)


@pytest.mark.parametrize("disabled", ["standard", "advanced"])
def test_disabling_one_plan_keeps_the_other(monkeypatch, disabled):
    cfg = config()
    if disabled == "standard":
        cfg["scheduling"]["enabled"] = False
    else:
        cfg["scheduling"]["advanced"]["enabled"] = False
    runs, _, _ = simulate(monkeypatch, cfg)
    assert [payload["scheduler_mode"] for _, payload in runs] == ["advanced" if disabled == "standard" else "standard"]


def test_combined_plans_survive_api_save_and_reload(tmp_path, monkeypatch):
    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    config_base.invalidate_config_cache()
    cfg = config()
    cfg["scheduling"]["advanced"].update({"jobs": [], "backup_jobs": [{"id": "app-backup-default", "at": "03:00"}]})
    worker = Mock()
    worker.status.return_value = {}
    monkeypatch.setattr(schedulingAPI, "_env", lambda: (config_base.load_config, config_base.save_config, worker, {}, lambda _: 0, Mock()))
    try:
        assert schedulingAPI.sched_post(cfg["scheduling"])["ok"] is True
        config_base.invalidate_config_cache()
        loaded = schedulingAPI.sched_get()
        assert loaded["enabled"] is True
        assert loaded["advanced"]["enabled"] is True
        restarted = scheduling.SyncScheduler(config_base.load_config, config_base.save_config, lambda _: True)
        assert restarted.status()["effective"] == {"enabled": True, "mode": "combined"}
    finally:
        config_base.invalidate_config_cache()


def test_legacy_backup_recovery_runs_only_once():
    cfg = config()
    cfg["scheduling"].pop("_independent_plans_v1")
    cfg["scheduling"]["advanced"].update({"jobs": [], "backup_jobs": [{"id": "app-backup-default", "at": "03:00"}]})
    config_base._normalize_scheduling(cfg)
    assert cfg["scheduling"]["advanced"]["enabled"] is False
    cfg["scheduling"]["advanced"]["enabled"] = True
    config_base._normalize_scheduling(cfg)
    assert cfg["scheduling"]["advanced"]["enabled"] is True


def test_combined_plan_reports_standard_warnings():
    cfg = config()
    cfg["scheduling"].update({"mode": "custom_interval", "custom_interval_minutes": 15})
    assert any(message.startswith("Standard schedule:") for message in schedulingAPI._scheduling_warning_summary(cfg)["warnings"])


def test_inactive_standard_fields_do_not_reset_interval(monkeypatch):
    cfg = config()
    cfg["scheduling"].update({"mode": "custom_interval", "custom_interval_minutes": 15, "every_n_hours": 2})
    cfg["scheduling"]["advanced"]["enabled"] = False

    def change(cfg, _worker, clock):
        if clock["now"] >= datetime(2026, 3, 15, 8, 5):
            cfg["scheduling"].update({"every_n_hours": 1, "daily_time": "16:00"})
            cfg["scheduling"]["advanced"]["enabled"] = True

    runs, _, _ = simulate(monkeypatch, cfg, advance=change, finish=datetime(2026, 3, 15, 8, 16))
    assert [(at.hour, at.minute, p["scheduler_mode"]) for at, p in runs] == [(8, 14, "standard")]


def test_partial_scheduling_save_preserves_standard_and_advanced_settings(monkeypatch):
    cfg = config()
    cfg["scheduling"].update({"timezone": "Asia/Tokyo", "jitter_seconds": 17, "every_n_hours": 2})
    before = deepcopy(cfg["scheduling"])
    worker = Mock()
    worker.status.return_value = {}
    monkeypatch.setattr(schedulingAPI, "_env", lambda: (lambda: cfg, lambda _: None, worker, {}, lambda _: 0, Mock()))
    schedulingAPI.sched_post({"advanced": {"enabled": False}})
    before["advanced"]["enabled"] = False
    assert cfg["scheduling"] == before


@pytest.mark.parametrize("edit", ["backup", "event", "workflow", "cancel", "cancel_last_step", "cancel_later_standard", "cancel_finishes_late"])
def test_pending_workflow_with_real_async_starter(monkeypatch, edit):
    from api import syncAPI

    cfg = config()
    cfg["pairs"] = [{"id": p, "enabled": True} for p in "abcd"]
    later_standard = edit in {"cancel_later_standard", "cancel_finishes_late"}
    cfg["scheduling"]["daily_time"] = "08:00" if edit.startswith("cancel") and not later_standard else "09:00"
    cfg["scheduling"]["advanced"].update({
        "jobs": [],
        "workflows": [{"id": "chain", "mode": "daily_time", "daily_time": "08:00",
                       "steps": [{"id": p, "pair_id": p} for p in ("a" if edit == "cancel_last_step" or later_standard else "abc")]}],
        "backup_jobs": [] if later_standard else [{"id": "backup", "at": "08:00", "scope": "app_state"}],
    })
    clock = {"now": datetime(2026, 3, 15, 7, 59, 30)}
    monkeypatch.setattr(scheduling, "_as_now_in_tz", lambda _tz=None: clock["now"])
    monkeypatch.setattr(scheduling, "_now_local_naive", lambda: clock["now"])
    monkeypatch.setattr(scheduling, "_now_ts", lambda: int(clock["now"].timestamp()))
    monkeypatch.setattr(run_control, "time", SimpleNamespace(time=lambda: clock["now"].timestamp()))
    monkeypatch.setattr(syncAPI, "SUMMARY", {})
    config_lock = threading.Lock()
    release = threading.Event()
    waiting = threading.Event()
    idle = threading.Event()
    procs = {}
    payloads = []
    backups = []
    runtime = ({}, procs, threading.Lock(), None, (), None, None, None, lambda *_: None)
    monkeypatch.setattr(syncAPI, "_rt", lambda: runtime)
    monkeypatch.setattr(syncAPI, "_request_user", lambda _: None)

    def load():
        with config_lock:
            return deepcopy(cfg)

    monkeypatch.setattr(syncAPI, "_env", lambda: (load, lambda _: None))

    def provider(_run_id, overrides):
        if overrides.get("pair_id") == "a":
            release.wait(5)
            run_control.clear_cancel()

    monkeypatch.setattr(syncAPI, "_run_pairs_thread_entry", provider)
    source = ast.parse((Path(__file__).parents[1] / "crosswatch.py").read_text(encoding="utf-8-sig"))
    starter = next(node for node in source.body if isinstance(node, ast.FunctionDef) and node.name == "_start_sync_from_scheduler")
    namespace = {"Any": Any, "api_run_sync": syncAPI.api_run_sync, "_append_log": lambda *_: None}
    exec(compile(ast.Module(body=[starter], type_ignores=[]), "crosswatch.py", "exec"), namespace)

    def run(payload):
        assert not syncAPI._is_sync_running()
        if payload.get("scheduler_mode") == "advanced_backup":
            backups.append(deepcopy(payload))
            return True
        payloads.append(deepcopy(payload))
        return namespace["_start_sync_from_scheduler"](payload)

    worker = scheduling.SyncScheduler(load, lambda _: None, run, is_sync_running_fn=syncAPI._is_sync_running)
    sleep = worker._sleep_or_poke

    def observe(seconds):
        (waiting if syncAPI._is_sync_running() else idle).set()
        sleep(seconds)

    monkeypatch.setattr(worker, "_sleep_or_poke", observe)
    run_control.clear_queue_stop()
    run_control.clear_cancel()
    worker.start()
    thread = worker._thread
    try:
        assert idle.wait(3)
        clock["now"] = datetime(2026, 3, 15, 8)
        worker.refresh()
        assert waiting.wait(3)
        clock["now"] += timedelta(minutes=1)
        with config_lock:
            if edit == "backup":
                cfg["scheduling"]["advanced"]["backup_jobs"][0]["scope"] = "config_only"
            elif edit == "event":
                cfg["scheduling"]["advanced"]["event_rules"] = [{"id": "event", "source": "watcher", "event": "stop", "action": {"pair_id": "d"}}]
            elif edit == "workflow":
                cfg["scheduling"]["advanced"]["workflows"][0]["steps"][1]["pair_id"] = "d"
            else:
                assert syncAPI.api_cancel_sync()["ok"] is True
        waiting.clear()
        worker.refresh()
        assert waiting.wait(3)
        idle.clear()
        clock["now"] += timedelta(minutes=1)
        if edit == "cancel_finishes_late":
            clock["now"] = datetime(2026, 3, 15, 9, 2)
        release.set()
        procs["SYNC"].join(3)
        worker.refresh()
        assert idle.wait(3)
        idle.clear()
        worker.refresh()
        assert idle.wait(3)
        if later_standard:
            if edit == "cancel_later_standard":
                assert [p.get("pair_id") for p in payloads] == ["a"]
                idle.clear()
                clock["now"] = datetime(2026, 3, 15, 9)
                worker.refresh()
                assert idle.wait(3)
            assert [p["scheduler_mode"] for p in payloads] == ["advanced_workflow", "standard"]
            assert "pair_scope_ids" not in payloads[-1]
            assert not backups
        else:
            assert [p.get("pair_id") for p in payloads] == (["a", "b", "c"] if edit in {"backup", "event"} else ["a"])
            assert len(backups) == 1
            assert backups[0]["backup"]["scope"] == ("config_only" if edit == "backup" else "app_state")
        if edit.startswith("cancel"):
            assert worker._std_next_ts > int(clock["now"].timestamp())
    finally:
        release.set()
        worker.stop()
        assert thread is not None and not thread.is_alive()
        if procs.get("SYNC"):
            procs["SYNC"].join(3)
        run_control.clear_queue_stop()
        run_control.clear_cancel()


@pytest.mark.parametrize("kind", ["jobs", "workflows", "captures"])
@pytest.mark.parametrize("disabled", ["standard", "advanced"])
def test_plan_changes_while_waiting_are_respected(monkeypatch, kind, disabled):
    cfg = config()
    if kind == "workflows":
        cfg["scheduling"]["advanced"].update({"jobs": [], "workflows": [{
            "id": "chain", "mode": "daily_time", "daily_time": "08:00", "steps": [{"id": "step", "pair_id": "a"}],
        }]})
    elif kind == "captures":
        cfg["scheduling"]["advanced"].update({"jobs": [], "capture_jobs": [{
            "id": "capture", "provider": "PLEX", "feature": "watchlist", "at": "08:00",
        }]})
    now = datetime(2026, 3, 15, 7, 59, 30)
    monkeypatch.setattr(scheduling, "_as_now_in_tz", lambda _tz=None: now)
    monkeypatch.setattr(scheduling, "_now_local_naive", lambda: now)
    monkeypatch.setattr(scheduling, "_now_ts", lambda: int(now.timestamp()))
    lock = threading.Lock()
    busy = threading.Event()
    busy.set()
    idle = threading.Event()
    waiting = threading.Event()
    dispatched = threading.Event()
    runs = []

    def load():
        with lock:
            return deepcopy(cfg)

    def run(payload):
        assert not busy.is_set()
        runs.append(payload)
        dispatched.set()
        return True

    worker = scheduling.SyncScheduler(load, lambda _: None, run, is_sync_running_fn=busy.is_set)
    sleep = worker._sleep_or_poke

    def observe(seconds):
        (waiting if worker.status()["busy"] else idle).set()
        sleep(seconds)

    monkeypatch.setattr(worker, "_sleep_or_poke", observe)
    worker.start()
    thread = worker._thread
    try:
        assert idle.wait(3)
        idle.clear()
        now = datetime(2026, 3, 15, 8)
        worker.refresh()
        assert waiting.wait(3)
        with lock:
            if disabled == "advanced":
                cfg["scheduling"]["advanced"]["enabled"] = False
            else:
                cfg["scheduling"]["enabled"] = False
        busy.clear()
        worker.refresh()
        assert dispatched.wait(3)
        assert idle.wait(3)
    finally:
        worker.stop()
        assert thread is not None and not thread.is_alive()
    expected = "standard" if disabled == "advanced" else {"jobs": "advanced", "workflows": "advanced_workflow", "captures": "advanced_capture"}[kind]
    assert [payload["scheduler_mode"] for payload in runs] == [expected]
    if disabled == "advanced":
        assert "pair_scope_ids" not in runs[0]
