# tests/test_profile_schedules.py
# CrossWatch - Per-profile sync schedules
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from datetime import timedelta
from typing import Any

import services.scheduling as scheduling

BOB = "22222222222242228222222222222222"
ALICE = "11111111111141118111111111111111"


def _cfg(*, bob_enabled: bool = True) -> dict[str, Any]:
    return {
        "user_profiles": {BOB: {"label": "Bob", "instances": {}}, ALICE: {"label": "Alice", "instances": {}}},
        "pairs": [
            {"id": "pair_admin", "source": "PLEX", "target": "SIMKL", "enabled": True},
            {"id": "pair_bob", "source": "PLEX", "target": "SIMKL", "enabled": True, "profile_id": BOB},
            {"id": "pair_alice", "source": "PLEX", "target": "TRAKT", "enabled": True, "profile_id": ALICE},
        ],
        "scheduling": {
            "enabled": True,
            "mode": "every_n_hours",
            "every_n_hours": 12,
            "profiles": {BOB: {"enabled": bob_enabled, "mode": "every_n_hours", "every_n_hours": 6}, "f" * 32: {"enabled": True}},
        },
    }


def _scheduler(cfg: dict[str, Any], runs: list[dict[str, Any]], *, busy: bool = False) -> scheduling.SyncScheduler:
    def run(payload: dict[str, Any] | None = None) -> bool:
        runs.append(dict(payload or {}))
        return True

    return scheduling.SyncScheduler(lambda: cfg, lambda _cfg: None, run, is_sync_running_fn=lambda: busy)


def test_normalize_profile_schedule_enforces_one_hour_minimum() -> None:
    out = scheduling.normalize_profile_schedule({"enabled": "true", "mode": "custom_interval", "custom_interval_minutes": 15, "daily_time": "nope"})
    assert out == {"enabled": True, "mode": "custom_interval", "every_n_hours": 12, "daily_time": "03:30", "custom_interval_minutes": 60}
    assert scheduling.normalize_profile_schedule({"mode": "weird"})["mode"] == "every_n_hours"


def test_profile_schedules_ignore_unknown_profiles() -> None:
    cfg = _cfg()
    assert set(scheduling.profile_schedules(cfg)) == {BOB}
    assert scheduling.profile_pair_ids(cfg, BOB) == ["pair_bob"]
    assert scheduling.profile_scheduled_pair_ids(cfg) == {"pair_bob"}
    assert scheduling.profile_scheduled_pair_ids(_cfg(bob_enabled=False)) == set()


def test_scheduler_should_run_for_profile_schedule_only() -> None:
    cfg = _cfg()
    cfg["scheduling"]["enabled"] = False
    assert scheduling.scheduler_should_run(cfg) is True
    assert scheduling.scheduler_should_run(_cfg(bob_enabled=False) | {"scheduling": {"enabled": False}}) is False


def test_global_run_skips_pairs_on_profile_schedules() -> None:
    runs: list[dict[str, Any]] = []
    _scheduler(_cfg(), runs)._std_run_due()
    assert runs[0]["pair_scope_ids"] == ["pair_admin", "pair_alice"]

    plain: list[dict[str, Any]] = []
    _scheduler(_cfg(bob_enabled=False), plain)._std_run_due()
    assert "pair_scope_ids" not in plain[0]


def test_global_run_does_nothing_when_every_pair_is_profile_scheduled() -> None:
    cfg = _cfg()
    cfg["pairs"] = [cfg["pairs"][1]]
    runs: list[dict[str, Any]] = []
    assert _scheduler(cfg, runs)._std_run_due() is True
    assert runs == []


def test_due_profile_schedule_runs_only_its_pairs(monkeypatch) -> None:
    monkeypatch.setattr(scheduling, "compute_next_run", lambda now, sch: scheduling._now_local_naive() - timedelta(minutes=1))
    cfg = _cfg()
    runs: list[dict[str, Any]] = []
    sched = _scheduler(cfg, runs)
    assert sched._profiles_tick(scheduling.merge_defaults(cfg["scheduling"]), None) is True
    assert runs == [{"source": "scheduler", "scheduler_mode": "profile", "profile_id": BOB, "pair_scope_ids": ["pair_bob"]}]
    assert sched.status()["profiles"][BOB]["last_run_ok"] is True


def test_due_profile_schedule_waits_while_sync_is_busy(monkeypatch) -> None:
    monkeypatch.setattr(scheduling, "compute_next_run", lambda now, sch: scheduling._now_local_naive() - timedelta(minutes=1))
    cfg = _cfg()
    runs: list[dict[str, Any]] = []
    assert _scheduler(cfg, runs, busy=True)._profiles_tick(scheduling.merge_defaults(cfg["scheduling"]), None) is False
    assert runs == []


def test_deleting_profile_removes_its_schedule() -> None:
    from cw_platform.user_profile_resources import clear_profile_resource_assignments

    cfg = _cfg()
    clear_profile_resource_assignments(cfg, BOB)
    assert BOB not in cfg["scheduling"]["profiles"]
