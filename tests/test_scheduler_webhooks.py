from __future__ import annotations

import threading
import types
from typing import Any

import pytest


def test_resolve_completion_event_keeps_unresolved_runs_successful() -> None:
    from services.scheduler_webhooks import resolve_completion_event

    cfg = {"scheduling": {"webhooks": {"enabled": True, "base_url": "https://hc-ping.com/abc123"}}}

    assert resolve_completion_event(cfg, {"exit_code": 0, "unresolved": 3, "errors": 2, "blocked": 1}) == "success"
    assert resolve_completion_event(cfg, {"exit_code": 1}) == "failure"


def test_resolve_completion_event_alerts_on_unresolved_when_opted_in() -> None:
    from services.scheduler_webhooks import resolve_completion_event

    def cfg_with(**hooks: Any) -> dict[str, Any]:
        return {"scheduling": {"webhooks": {"enabled": True, "base_url": "https://hc-ping.com/abc123", **hooks}}}

    cfg = cfg_with(alert_on_unresolved=True)

    assert resolve_completion_event(cfg, {"exit_code": 0, "unresolved": 1}) == "failure"
    assert resolve_completion_event(cfg, {"exit_code": 0, "errors": 1}) == "failure"
    assert resolve_completion_event(cfg, {"exit_code": 0, "unresolved": 0, "skipped": 4, "blocked": 2}) == "success"


def test_scheduler_webhook_healthchecks_urls_preserve_query() -> None:
    from services.scheduler_webhooks import callback_url, normalize_scheduler_webhooks

    webhooks = normalize_scheduler_webhooks({
        "enabled": True,
        "base_url": "https://hc-ping.com/abc123?rid=crosswatch",
    })

    assert callback_url(webhooks, "start") == "https://hc-ping.com/abc123/start?rid=crosswatch"
    assert callback_url(webhooks, "success") == "https://hc-ping.com/abc123?rid=crosswatch"
    assert callback_url(webhooks, "failure") == "https://hc-ping.com/abc123/fail?rid=crosswatch"


def test_scheduler_webhook_explicit_url_wins_over_healthchecks_base() -> None:
    from services.scheduler_webhooks import callback_url, normalize_scheduler_webhooks

    webhooks = normalize_scheduler_webhooks({
        "enabled": True,
        "base_url": "https://hc-ping.com/abc123",
        "failure_url": "https://monitor.example/fail",
    })

    assert callback_url(webhooks, "failure") == "https://monitor.example/fail"


def test_scheduler_webhook_common_url_wins_over_compatible_base() -> None:
    from services.scheduler_webhooks import callback_url, normalize_scheduler_webhooks

    webhooks = normalize_scheduler_webhooks({
        "enabled": True,
        "url": "https://notifiarr.com/api/v1/notification/passthrough/key",
        "base_url": "https://hc-ping.com/abc123",
    })

    assert callback_url(webhooks, "start") == "https://notifiarr.com/api/v1/notification/passthrough/key"
    assert callback_url(webhooks, "success") == "https://notifiarr.com/api/v1/notification/passthrough/key"
    assert callback_url(webhooks, "failure") == "https://notifiarr.com/api/v1/notification/passthrough/key"


def test_scheduler_webhook_notifiarr_format_uses_single_passthrough_url() -> None:
    from services.scheduler_webhooks import callback_url, normalize_scheduler_webhooks

    webhooks = normalize_scheduler_webhooks({
        "enabled": True,
        "payload_format": "notifiarr",
        "url": "https://notifiarr.com/api/v1/notification/passthrough/key",
        "failure_url": "https://hooks.example.com/old-failure-url",
        "base_url": "https://hc-ping.com/abc123",
    })

    assert callback_url(webhooks, "start") == "https://notifiarr.com/api/v1/notification/passthrough/key"
    assert callback_url(webhooks, "success") == "https://notifiarr.com/api/v1/notification/passthrough/key"
    assert callback_url(webhooks, "failure") == "https://notifiarr.com/api/v1/notification/passthrough/key"


def test_scheduler_webhook_urls_must_be_http_or_https() -> None:
    from services.scheduler_webhooks import callback_url, normalize_scheduler_webhooks

    webhooks = normalize_scheduler_webhooks({
        "enabled": True,
        "base_url": "ftp://monitor.example/ping",
        "start_url": "monitor.example/start",
        "success_url": "http://monitor.example/success",
        "failure_url": "https://monitor.example/failure",
    })

    assert webhooks["base_url"] == ""
    assert webhooks["start_url"] == ""
    assert callback_url(webhooks, "start") == ""
    assert callback_url(webhooks, "success") == "http://monitor.example/success"
    assert callback_url(webhooks, "failure") == "https://monitor.example/failure"


def test_scheduler_webhook_payload_includes_context_and_summary_counts() -> None:
    from services.scheduler_webhooks import build_payload

    payload = build_payload(
        "success",
        {
            "run_id": "run-1",
            "scheduler_mode": "advanced_workflow",
            "workflow_id": "wf",
            "workflow_step_id": "step",
            "pair_id": "plex-trakt",
        },
        {
            "started_at": "2026-08-03T10:00:00Z",
            "finished_at": "2026-08-03T10:00:04Z",
            "duration_sec": 4.2,
            "exit_code": 0,
            "features": {
                "watchlist": {"added": 2, "removed": 1, "updated": 0},
                "ratings": {"added": 3, "removed": 0, "updated": 1},
            },
        },
    )

    assert payload["event"] == "success"
    assert payload["status"] == "ok"
    assert payload["run_id"] == "run-1"
    assert payload["scheduler_mode"] == "advanced_workflow"
    assert payload["workflow_id"] == "wf"
    assert payload["workflow_step_id"] == "step"
    assert payload["pair_id"] == "plex-trakt"
    assert payload["summary"] == {"added": 5, "removed": 1, "updated": 1}


def test_scheduler_webhook_notifiarr_payload_shape() -> None:
    from services.scheduler_webhooks import build_payload

    payload = build_payload(
        "failure",
        {
            "run_id": "run-9",
            "scheduler_mode": "standard",
            "pair_id": "plex-trakt",
        },
        {
            "exit_code": 1,
            "errors": 2,
        },
        "notifiarr",
    )

    assert payload["notification"]["name"] == "CrossWatch"
    assert payload["notification"]["event"] == "run-9"
    assert payload["discord"]["color"] == "FF5A5F"
    assert payload["discord"]["text"]["title"] == "CrossWatch scheduled sync failed"
    assert {"title": "Pair", "text": "plex-trakt", "inline": True} in payload["discord"]["text"]["fields"]
    assert {"title": "Errors", "text": "2", "inline": True} in payload["discord"]["text"]["fields"]


def test_scheduler_webhook_notifiarr_notify_adds_channel(monkeypatch) -> None:
    import services.scheduler_webhooks as webhooks

    posted: dict[str, Any] = {}

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

    def fake_post(url: str, **kwargs: Any) -> FakeResponse:
        posted["url"] = url
        posted["json"] = kwargs.get("json")
        return FakeResponse()

    monkeypatch.setattr(webhooks.requests, "post", fake_post)

    ok = webhooks.notify_scheduler_webhook(
        {
            "scheduling": {
                "webhooks": {
                    "enabled": True,
                    "url": "https://notifiarr.com/api/v1/notification/passthrough/key",
                    "payload_format": "notifiarr",
                    "notifiarr_channel_id": "123456789012345678",
                }
            }
        },
        "success",
        {"source": "scheduler", "scheduler_mode": "standard", "run_id": "run-10"},
        {"exit_code": 0},
    )

    assert ok is True
    assert posted["url"] == "https://notifiarr.com/api/v1/notification/passthrough/key"
    assert posted["json"]["discord"]["ids"]["channel"] == 123456789012345678


def test_scheduler_webhook_notify_is_best_effort(monkeypatch) -> None:
    import services.scheduler_webhooks as webhooks

    def fail_post(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("offline")

    logs: list[str] = []
    monkeypatch.setattr(webhooks.requests, "post", fail_post)

    ok = webhooks.notify_scheduler_webhook(
        {
            "scheduling": {
                "webhooks": {
                    "enabled": True,
                    "failure_url": "https://monitor.example/fail",
                    "timeout_seconds": 1,
                }
            }
        },
        "failure",
        {"source": "scheduler", "scheduler_mode": "standard", "run_id": "run-2"},
        {"exit_code": 1},
        log_fn=logs.append,
    )

    assert ok is False
    assert logs and "Scheduler webhook failure" in logs[0]


def test_scheduling_webhook_config_is_normalized_and_sensitive() -> None:
    from cw_platform.config_base import _is_sensitive_path, _normalize_scheduling

    cfg = {
        "scheduling": {
                "webhooks": {
                "enabled": True,
                "url": "https://notifiarr.com/api/v1/notification/passthrough/key",
                "base_url": "https://hc-ping.com/abc123",
                "start_url": "ftp://monitor.example/start",
                "payload_format": "notifiarr-passthrough",
                "notifiarr_channel_id": "123456789012345678",
                "timeout_seconds": 999,
            }
        }
    }

    _normalize_scheduling(cfg)

    hooks = cfg["scheduling"]["webhooks"]
    assert hooks["enabled"] is True
    assert hooks["url"] == "https://notifiarr.com/api/v1/notification/passthrough/key"
    assert hooks["base_url"] == "https://hc-ping.com/abc123"
    assert hooks["start_url"] == ""
    assert hooks["payload_format"] == "notifiarr"
    assert hooks["notifiarr_channel_id"] == "123456789012345678"
    assert hooks["timeout_seconds"] == 60
    assert _is_sensitive_path(("scheduling", "webhooks", "url")) is True
    assert _is_sensitive_path(("scheduling", "webhooks", "base_url")) is True
    assert _is_sensitive_path(("scheduling", "webhooks", "failure_url")) is True


def test_config_save_preserves_redacted_scheduler_webhook_url() -> None:
    from api.configAPI import _preserve_sensitive_config_values

    mask = "••••••••"
    current = {
        "scheduling": {
            "webhooks": {
                "enabled": True,
                "base_url": "https://hc-ping.com/secret-token",
            }
        }
    }
    incoming = {
        "scheduling": {
            "webhooks": {
                "enabled": True,
                "base_url": mask,
            }
        }
    }
    merged = {
        "scheduling": {
            "webhooks": {
                "enabled": True,
                "base_url": mask,
            }
        }
    }

    _preserve_sensitive_config_values(
        current,
        incoming,
        merged,
        lambda value: str(value or "").strip() in {"", mask},
        lambda _key: False,
    )

    assert merged["scheduling"]["webhooks"]["base_url"] == "https://hc-ping.com/secret-token"


def test_redact_config_masks_scheduler_webhook_urls() -> None:
    from cw_platform.config_base import redact_config

    redacted = redact_config({
        "scheduling": {
            "webhooks": {
                "enabled": True,
                "base_url": "https://hc-ping.com/secret-token",
            }
        }
    })

    assert redacted["scheduling"]["webhooks"]["base_url"] == "••••••••"


def test_scheduler_webhook_save_logs_enabled_without_secret_url() -> None:
    from api.schedulingAPI import _log_scheduler_webhook_status

    logs: list[str] = []

    def fake_logger(_kind: str, _channel: str):
        def inner(message: str, **_kwargs: Any) -> None:
            logs.append(message)
        return inner

    _log_scheduler_webhook_status(
        {"webhooks": {"enabled": False}},
        {
            "webhooks": {
                "enabled": True,
                "payload_format": "crosswatch",
                "base_url": "https://hc-ping.com/secret-token",
            }
        },
        fake_logger,
    )

    assert logs == ["Scheduler webhooks enabled (CrossWatch JSON)"]
    assert "secret-token" not in logs[0]


def test_scheduler_webhook_save_log_dedupes_unchanged_enabled_state() -> None:
    from api.schedulingAPI import _log_scheduler_webhook_status

    logs: list[str] = []
    scfg = {
        "webhooks": {
            "enabled": True,
            "payload_format": "crosswatch",
            "base_url": "https://hc-ping.com/secret-token",
        }
    }

    def fake_logger(_kind: str, _channel: str):
        def inner(message: str, **_kwargs: Any) -> None:
            logs.append(message)
        return inner

    _log_scheduler_webhook_status(scfg, scfg, fake_logger)

    assert logs == []


def test_scheduler_start_logs_webhooks_enabled_without_secret_url() -> None:
    from services.scheduling import SyncScheduler

    logs: list[str] = []
    cfg = {
        "scheduling": {
            "enabled": True,
            "mode": "hourly",
            "webhooks": {
                "enabled": True,
                "payload_format": "crosswatch",
                "base_url": "https://hc-ping.com/secret-token",
            },
        }
    }

    scheduler = SyncScheduler(
        load_config=lambda: cfg,
        save_config=lambda _cfg: None,
        run_sync_fn=lambda _payload=None: True,
        is_sync_running_fn=lambda: False,
        log_fn=lambda message, **_kwargs: logs.append(message),
    )

    scheduler._log_webhook_state()
    scheduler._log_webhook_state()

    assert logs == ["scheduler webhooks enabled (CrossWatch JSON)"]
    assert "secret-token" not in logs[0]


def test_advanced_workflow_rejected_start_dispatches_failure_webhook(monkeypatch) -> None:
    import services.scheduling as scheduling
    from services.scheduling import SyncScheduler

    cfg = {
        "scheduling": {
            "advanced": {
                "enabled": True,
                "workflows": [
                    {
                        "id": "workflow-1",
                        "mode": "every_n_hours",
                        "every_n_hours": 1,
                        "steps": [{"id": "step-1", "pair_id": "pair-1", "active": True}],
                        "active": True,
                    }
                ],
            },
            "webhooks": {
                "enabled": True,
                "failure_url": "https://monitor.example/fail",
            },
        }
    }
    calls: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

    def fake_notify(cfg_arg: dict[str, Any] | None, event: str, context: dict[str, Any] | None, summary: dict[str, Any] | None = None, **_kwargs: Any) -> bool:
        calls.append((event, dict(context or {}), dict(summary or {})))
        return True

    monkeypatch.setattr(scheduling, "notify_scheduler_webhook", fake_notify)

    scheduler = SyncScheduler(
        load_config=lambda: cfg,
        save_config=lambda _cfg: None,
        run_sync_fn=lambda _payload=None: False,
        is_sync_running_fn=lambda: False,
        log_fn=lambda *_args, **_kwargs: None,
    )

    assert scheduler._adv_run_due(cfg["scheduling"], None) is False
    assert [event for event, _, _ in calls] == ["failure"]
    assert calls[0][1]["scheduler_mode"] == "advanced_workflow"
    assert calls[0][1]["workflow_id"] == "workflow-1"
    assert calls[0][1]["workflow_step_id"] == "step-1"
    assert calls[0][2]["exit_code"] == 1


def test_scheduled_sync_thread_dispatches_start_and_success_webhooks(monkeypatch, tmp_path) -> None:
    import api.syncAPI as sync
    import sys

    log_buffers: dict[str, list[str]] = {"SYNC": []}

    def append_log(kind: str, message: str) -> None:
        log_buffers.setdefault(kind, []).append(str(message))

    fake_crosswatch = types.SimpleNamespace(
        LOG_BUFFERS=log_buffers,
        RUNNING_PROCS={"SYNC": object()},
        SYNC_PROC_LOCK=threading.Lock(),
        STATS=types.SimpleNamespace(refresh_from_state=lambda _state: None, record_summary=lambda *_args: None),
        REPORT_DIR=tmp_path,
        strip_ansi=lambda value: str(value),
        _append_log=append_log,
        minimal=lambda value: value,
        canonical_key=lambda value: str(value),
    )
    monkeypatch.setitem(sys.modules, "crosswatch", fake_crosswatch)

    cfg = {
        "pairs": [{"id": "plex-trakt", "enabled": True, "source": "PLEX", "target": "TRAKT"}],
        "scheduling": {
            "webhooks": {
                "enabled": True,
                "base_url": "https://hc-ping.com/abc123",
            }
        },
    }
    monkeypatch.setattr(sync, "_env", lambda: (lambda: cfg, lambda _cfg: None))
    monkeypatch.setattr(sync, "_load_state", lambda: None)
    monkeypatch.setattr(sync, "_counts_from_state", lambda _state: None)
    monkeypatch.setattr(sync, "_provider_count_defaults", lambda: {})

    class FakeOrchestrator:
        def __init__(self, config: dict[str, Any]) -> None:
            self.config = config

        def run_pairs(self, **kwargs: Any) -> dict[str, int]:
            progress = kwargs["progress"]
            progress('{"event":"apply:add:done","feature":"watchlist","count":2}')
            return {"added": 2, "removed": 0, "updated": 0, "errors": 0}

    fake_orchestrator_module = types.SimpleNamespace(Orchestrator=FakeOrchestrator, __file__="fake-orchestrator.py")
    monkeypatch.setattr(
        sync.importlib,
        "import_module",
        lambda name: fake_orchestrator_module if name == "cw_platform.orchestrator" else __import__(name),
    )

    calls: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

    def fake_notify(cfg_arg: dict[str, Any] | None, event: str, context: dict[str, Any] | None, summary: dict[str, Any] | None = None, **_kwargs: Any) -> bool:
        calls.append((event, dict(context or {}), dict(summary or {})))
        return True

    monkeypatch.setattr(sync, "notify_scheduler_webhook", fake_notify)

    sync._run_pairs_thread(
        "run-123",
        {
            "source": "scheduler",
            "scheduler_mode": "advanced",
            "job_id": "job-1",
            "pair_id": "plex-trakt",
        },
    )

    assert [event for event, _, _ in calls] == ["start", "success"]
    assert calls[0][1]["run_id"] == "run-123"
    assert calls[0][1]["scheduler_mode"] == "advanced"
    assert calls[0][1]["job_id"] == "job-1"
    assert calls[0][1]["pair_id"] == "plex-trakt"
    assert calls[1][2]["exit_code"] == 0
    assert calls[1][2]["added_last"] == 2


@pytest.mark.parametrize(
    ("alert_on_unresolved", "expected_event"),
    [(False, "success"), (True, "failure")],
)
def test_scheduled_sync_thread_event_on_unresolved(monkeypatch, tmp_path, alert_on_unresolved, expected_event) -> None:
    import api.syncAPI as sync
    import sys

    log_buffers: dict[str, list[str]] = {"SYNC": []}

    def append_log(kind: str, message: str) -> None:
        log_buffers.setdefault(kind, []).append(str(message))

    fake_crosswatch = types.SimpleNamespace(
        LOG_BUFFERS=log_buffers,
        RUNNING_PROCS={"SYNC": object()},
        SYNC_PROC_LOCK=threading.Lock(),
        STATE_PATH=tmp_path / "state.json",
        STATE_PATHS=[],
        STATS=types.SimpleNamespace(refresh_from_state=lambda _state: None, record_summary=lambda *_args: None),
        REPORT_DIR=tmp_path,
        strip_ansi=lambda value: str(value),
        _append_log=append_log,
        minimal=lambda value: value,
        canonical_key=lambda value: str(value),
    )
    monkeypatch.setitem(sys.modules, "crosswatch", fake_crosswatch)

    cfg = {
        "pairs": [{"id": "plex-mdblist", "enabled": True, "source": "PLEX", "target": "MDBLIST"}],
        "scheduling": {
            "webhooks": {
                "enabled": True,
                "base_url": "https://hc-ping.com/abc123",
                "alert_on_unresolved": alert_on_unresolved,
            }
        },
    }
    monkeypatch.setattr(sync, "_env", lambda: (lambda: cfg, lambda _cfg: None))
    monkeypatch.setattr(sync, "_load_state", lambda: None)
    monkeypatch.setattr(sync, "_counts_from_state", lambda _state: None)
    monkeypatch.setattr(sync, "_provider_count_defaults", lambda: {})

    class UnresolvedOrchestrator:
        def __init__(self, config: dict[str, Any]) -> None:
            self.config = config

        def run_pairs(self, **kwargs: Any) -> dict[str, int]:
            progress = kwargs["progress"]
            progress('{"event":"apply:add:done","feature":"history","attempted":1,"added":0,"unresolved":1,"errors":0}')
            return {"added": 0, "removed": 0, "updated": 0, "unresolved": 1, "errors": 0}

    fake_orchestrator_module = types.SimpleNamespace(Orchestrator=UnresolvedOrchestrator, __file__="fake-orchestrator.py")
    monkeypatch.setattr(
        sync.importlib,
        "import_module",
        lambda name: fake_orchestrator_module if name == "cw_platform.orchestrator" else __import__(name),
    )

    calls: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

    def fake_notify(cfg_arg: dict[str, Any] | None, event: str, context: dict[str, Any] | None, summary: dict[str, Any] | None = None, **_kwargs: Any) -> bool:
        calls.append((event, dict(context or {}), dict(summary or {})))
        return True

    monkeypatch.setattr(sync, "notify_scheduler_webhook", fake_notify)

    sync._run_pairs_thread(
        "run-unresolved",
        {
            "source": "scheduler",
            "scheduler_mode": "advanced",
            "job_id": "job-mdblist",
            "pair_id": "plex-mdblist",
        },
    )

    assert [event for event, _, _ in calls] == ["start", expected_event]
    assert calls[1][2]["exit_code"] == 0
    assert calls[1][2]["unresolved"] == 1


def test_scheduled_sync_thread_dispatches_failure_webhook_on_exception(monkeypatch, tmp_path) -> None:
    import api.syncAPI as sync
    import sys

    log_buffers: dict[str, list[str]] = {"SYNC": []}

    def append_log(kind: str, message: str) -> None:
        log_buffers.setdefault(kind, []).append(str(message))

    fake_crosswatch = types.SimpleNamespace(
        LOG_BUFFERS=log_buffers,
        RUNNING_PROCS={"SYNC": object()},
        SYNC_PROC_LOCK=threading.Lock(),
        STATS=types.SimpleNamespace(refresh_from_state=lambda _state: None, record_summary=lambda *_args: None),
        REPORT_DIR=tmp_path,
        strip_ansi=lambda value: str(value),
        _append_log=append_log,
        minimal=lambda value: value,
        canonical_key=lambda value: str(value),
    )
    monkeypatch.setitem(sys.modules, "crosswatch", fake_crosswatch)

    cfg = {
        "pairs": [{"id": "plex-trakt", "enabled": True, "source": "PLEX", "target": "TRAKT"}],
        "scheduling": {
            "webhooks": {
                "enabled": True,
                "failure_url": "https://monitor.example/fail",
            }
        },
    }
    monkeypatch.setattr(sync, "_env", lambda: (lambda: cfg, lambda _cfg: None))
    monkeypatch.setattr(sync, "_load_state", lambda: None)
    monkeypatch.setattr(sync, "_counts_from_state", lambda _state: None)
    monkeypatch.setattr(sync, "_provider_count_defaults", lambda: {})

    class FailingOrchestrator:
        def __init__(self, config: dict[str, Any]) -> None:
            self.config = config

        def run_pairs(self, **_kwargs: Any) -> dict[str, int]:
            raise RuntimeError("boom")

    fake_orchestrator_module = types.SimpleNamespace(Orchestrator=FailingOrchestrator, __file__="fake-orchestrator.py")
    monkeypatch.setattr(
        sync.importlib,
        "import_module",
        lambda name: fake_orchestrator_module if name == "cw_platform.orchestrator" else __import__(name),
    )

    calls: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

    def fake_notify(cfg_arg: dict[str, Any] | None, event: str, context: dict[str, Any] | None, summary: dict[str, Any] | None = None, **_kwargs: Any) -> bool:
        calls.append((event, dict(context or {}), dict(summary or {})))
        return True

    monkeypatch.setattr(sync, "notify_scheduler_webhook", fake_notify)

    sync._run_pairs_thread(
        "run-456",
        {
            "source": "scheduler",
            "scheduler_mode": "standard",
        },
    )

    assert [event for event, _, _ in calls] == ["start", "failure"]
    assert calls[1][1]["run_id"] == "run-456"
    assert calls[1][2]["exit_code"] == 1
