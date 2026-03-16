from __future__ import annotations

from datetime import datetime


def test_label_template() -> None:
    from services.snapshots import render_capture_label_template

    rendered = render_capture_label_template(
        "nightly-{provider}-{instance}-{feature}-{date}-{time}",
        provider="trakt",
        instance="profile-a",
        feature="history",
        ts=datetime(2026, 3, 15, 8, 5, 0),
    )

    assert rendered == "nightly-TRAKT-profile-a-history-2026-03-15-08-05"


def test_due_capture_job_payload(monkeypatch) -> None:
    import services.scheduling as scheduling
    from services.scheduling import SyncScheduler

    now = datetime(2026, 3, 15, 8, 5, 0)
    monkeypatch.setattr(scheduling, "_as_now_in_tz", lambda _tz=None: now)
    monkeypatch.setattr(scheduling, "_now_ts", lambda: int(now.timestamp()))

    seen: list[dict] = []
    config = {
        "scheduling": {
            "enabled": False,
            "mode": "disabled",
            "advanced": {
                "enabled": True,
                "jobs": [],
                "capture_jobs": [
                    {
                        "id": "morning-capture",
                        "provider": "TRAKT",
                        "instance": "default",
                        "feature": "watchlist",
                        "at": "08:00",
                        "days": [7],
                        "label_template": "auto-{provider}-{feature}-{date}",
                        "active": True,
                    }
                ],
                "event_rules": [],
            },
        }
    }

    scheduler = SyncScheduler(
        load_config=lambda: config,
        save_config=lambda next_cfg: config.update(next_cfg),
        run_sync_fn=lambda payload=None: seen.append(payload or {}) or True,
        is_sync_running_fn=lambda: False,
    )

    ok = scheduler._adv_run_due(config["scheduling"], None)

    assert ok is True
    assert seen == [
        {
            "source": "scheduler",
            "scheduler_mode": "advanced_capture",
            "capture_job_id": "morning-capture",
            "capture": {
                "provider": "TRAKT",
                "instance": "default",
                "feature": "watchlist",
                "label_template": "auto-{provider}-{feature}-{date}",
            },
        }
    ]

    status = scheduler.status()
    assert status["last_capture_job_id"] == "morning-capture"
    assert status["last_capture_provider"] == "TRAKT"
    assert status["last_capture_feature"] == "watchlist"


def test_capture_job_no_backfill_on_start(monkeypatch) -> None:
    import services.scheduling as scheduling
    from services.scheduling import SyncScheduler

    now = datetime(2026, 3, 15, 14, 30, 0)
    monkeypatch.setattr(scheduling, "_as_now_in_tz", lambda _tz=None: now)
    monkeypatch.setattr(scheduling, "_now_ts", lambda: int(now.timestamp()))

    seen: list[dict] = []
    config = {
        "scheduling": {
            "enabled": False,
            "mode": "disabled",
            "advanced": {
                "enabled": True,
                "jobs": [],
                "capture_jobs": [
                    {
                        "id": "night-capture",
                        "provider": "PLEX",
                        "instance": "default",
                        "feature": "all",
                        "at": "01:00",
                        "days": [7],
                        "label_template": "auto-{provider}-{feature}-{date}",
                        "active": True,
                    }
                ],
                "event_rules": [],
            },
        }
    }

    scheduler = SyncScheduler(
        load_config=lambda: config,
        save_config=lambda next_cfg: config.update(next_cfg),
        run_sync_fn=lambda payload=None: seen.append(payload or {}) or True,
        is_sync_running_fn=lambda: False,
    )

    scheduler._adv_seed_past_due_today(config["scheduling"], None)
    ok = scheduler._adv_run_due(config["scheduling"], None)

    assert ok is False
    assert seen == []
    assert scheduler._adv_last_key["night-capture"] == "2026-03-15@01:00"
