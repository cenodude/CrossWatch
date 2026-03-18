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
