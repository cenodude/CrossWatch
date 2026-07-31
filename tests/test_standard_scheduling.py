from __future__ import annotations

from datetime import datetime, timezone


def test_normalize_scheduling_clamps_custom_interval_to_minimum() -> None:
    from cw_platform.config_base import _normalize_scheduling

    cfg = {
        "scheduling": {
            "enabled": True,
            "mode": "custom",
            "custom_interval_minutes": 5,
        }
    }

    _normalize_scheduling(cfg)

    assert cfg["scheduling"]["mode"] == "custom_interval"
    assert cfg["scheduling"]["custom_interval_minutes"] == 15


def test_normalize_scheduling_promotes_one_hour_interval_to_hourly() -> None:
    from cw_platform.config_base import _normalize_scheduling

    cfg = {
        "scheduling": {
            "enabled": True,
            "mode": "every_n_hours",
            "every_n_hours": 1,
        }
    }

    _normalize_scheduling(cfg)

    assert cfg["scheduling"]["mode"] == "hourly"
    assert cfg["scheduling"]["every_n_hours"] == 1


def test_compute_next_run_supports_custom_minute_interval() -> None:
    from services.scheduling import compute_next_run

    now = datetime(2026, 3, 18, 10, 5, 42)
    sch = {
        "enabled": True,
        "mode": "custom_interval",
        "custom_interval_minutes": 45,
        "jitter_seconds": 0,
        "timezone": "",
    }

    nxt = compute_next_run(now, sch)

    assert nxt == datetime(2026, 3, 18, 10, 50, 0)


def test_format_display_datetime_uses_configured_timezone() -> None:
    from services.scheduling import _format_display_datetime

    ts = int(datetime(2026, 3, 18, 13, 56, 0, tzinfo=timezone.utc).timestamp())

    text = _format_display_datetime(ts, {"timezone": "Europe/Amsterdam"})

    assert text.startswith("2026-03-18 14:56:00")


def test_normalize_scheduling_preserves_after_job_id() -> None:
    from cw_platform.config_base import _normalize_scheduling

    cfg = {
        "scheduling": {
            "advanced": {
                "enabled": True,
                "jobs": [
                    {"id": "first", "pair_id": "a", "at": "10:00"},
                    {"id": "second", "pair_id": "b", "at": "10:00", "after": "first"},
                ],
            }
        }
    }

    _normalize_scheduling(cfg)

    assert cfg["scheduling"]["advanced"]["jobs"][1]["after"] == "first"


def test_recurring_workflow_runs_pair_steps_in_order(monkeypatch) -> None:
    import services.scheduling as scheduling
    from services.scheduling import SyncScheduler

    now = datetime(2026, 3, 18, 10, 5, 0)
    monkeypatch.setattr(scheduling, "_as_now_in_tz", lambda _tz=None: now)
    monkeypatch.setattr(scheduling, "_now_ts", lambda: int(now.timestamp()))

    seen: list[str] = []
    config = {
        "scheduling": {
            "enabled": False,
            "mode": "disabled",
            "advanced": {
                "enabled": True,
                "jobs": [],
                "workflows": [
                    {
                        "id": "hourly-chain",
                        "mode": "hourly",
                        "steps": [
                            {"id": "one", "pair_id": "nuvio-crosswatch"},
                            {"id": "two", "pair_id": "crosswatch-publicmetadb"},
                        ],
                        "active": True,
                    }
                ],
            },
        }
    }

    scheduler = SyncScheduler(
        load_config=lambda: config,
        save_config=lambda next_cfg: config.update(next_cfg),
        run_sync_fn=lambda payload=None: seen.append(str((payload or {}).get("pair_id") or "")) or True,
        is_sync_running_fn=lambda: False,
    )

    ok = scheduler._adv_run_due(config["scheduling"], None)
    ran_again = scheduler._adv_run_due(config["scheduling"], None)

    assert ok is True
    assert ran_again is False
    assert seen == ["nuvio-crosswatch", "crosswatch-publicmetadb"]
