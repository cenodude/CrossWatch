from __future__ import annotations

from datetime import datetime


def test_capture_job_legacy_fields() -> None:
    from services.scheduling import _normalize_capture_job

    job = _normalize_capture_job(
        {
            "id": "plex-ratings",
            "provider": "plex",
            "profile": "kids",
            "feature": "ratings",
            "labelTemplate": "auto-{provider}-{feature}-{date}",
            "at": "07:30",
            "days": [1, "3", 3, 9],
            "active": True,
        }
    )

    assert job == {
        "id": "plex-ratings",
        "provider": "PLEX",
        "instance": "kids",
        "feature": "ratings",
        "label_template": "auto-{provider}-{feature}-{date}",
        "retention_days": 0,
        "max_captures": 0,
        "auto_delete_old": False,
        "at": "07:30",
        "days": [1, 3],
        "active": True,
    }


def test_capture_jobs_run_in_order(monkeypatch) -> None:
    import services.scheduling as scheduling
    from services.scheduling import SyncScheduler

    now = datetime(2026, 3, 16, 9, 0, 0)
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
                "capture_jobs": [
                    {
                        "id": "second",
                        "provider": "PLEX",
                        "instance": "default",
                        "feature": "history",
                        "at": "08:45",
                        "days": [1],
                        "label_template": "auto-{provider}-{feature}-{date}",
                        "retention_days": 0,
                        "max_captures": 0,
                        "auto_delete_old": False,
                        "active": True,
                    },
                    {
                        "id": "first",
                        "provider": "PLEX",
                        "instance": "default",
                        "feature": "watchlist",
                        "at": "08:15",
                        "days": [1],
                        "label_template": "auto-{provider}-{feature}-{date}",
                        "retention_days": 14,
                        "max_captures": 5,
                        "auto_delete_old": True,
                        "active": True,
                    },
                ],
                "event_rules": [],
            },
        }
    }

    scheduler = SyncScheduler(
        load_config=lambda: config,
        save_config=lambda next_cfg: config.update(next_cfg),
        run_sync_fn=lambda payload=None: seen.append(str((payload or {}).get("capture_job_id") or "")) or True,
        is_sync_running_fn=lambda: False,
    )

    ok = scheduler._adv_run_due(config["scheduling"], None)

    assert ok is True
    assert seen == ["first", "second"]
