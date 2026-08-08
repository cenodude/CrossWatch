from __future__ import annotations

import sys
import threading
import types
from typing import Any

import pytest

from cw_platform import run_control
from cw_platform.orchestrator import _applier, _snapshots


@pytest.fixture(autouse=True)
def _clear_cancel():
    run_control.clear_cancel()
    yield
    run_control.clear_cancel()


def test_cancel_requested_targets_only_the_named_run(monkeypatch) -> None:
    monkeypatch.delenv("CW_RUN_ID", raising=False)
    run_control.request_cancel("123")

    assert run_control.cancel_requested("123") is True
    assert run_control.cancel_requested("456") is False

    monkeypatch.setenv("CW_RUN_ID", "456")
    assert run_control.cancel_requested() is False
    monkeypatch.setenv("CW_RUN_ID", "123")
    assert run_control.cancel_requested() is True


def test_cancel_without_run_id_matches_any_run(monkeypatch) -> None:
    monkeypatch.setenv("CW_RUN_ID", "999")
    run_control.request_cancel()

    assert run_control.cancel_requested() is True
    assert run_control.cancel_requested("anything") is True

    run_control.clear_cancel()
    assert run_control.cancel_requested() is False


def test_apply_chunked_stops_after_current_chunk() -> None:
    items = [{"n": i} for i in range(10)]
    seen: list[int] = []

    def call(chunk):
        seen.append(len(chunk))
        if len(seen) == 2:
            run_control.request_cancel()
        return {"ok": True, "count": len(chunk)}

    res = _applier._apply_chunked(
        "add",
        dst="TRAKT",
        feature="watchlist",
        items=items,
        call=call,
        emit=lambda *a, **k: None,
        dbg=lambda *a, **k: None,
        chunk_size=2,
        chunk_pause_ms=0,
    )

    assert seen == [2, 2]
    assert res["cancelled"] is True
    assert res["attempted"] == 4


def test_apply_chunked_completes_when_not_cancelled() -> None:
    items = [{"n": i} for i in range(6)]
    calls = 0

    def call(chunk):
        nonlocal calls
        calls += 1
        return {"ok": True, "count": len(chunk)}

    res = _applier._apply_chunked(
        "add",
        dst="TRAKT",
        feature="watchlist",
        items=items,
        call=call,
        emit=lambda *a, **k: None,
        dbg=lambda *a, **k: None,
        chunk_size=2,
        chunk_pause_ms=0,
    )

    assert calls == 3
    assert "cancelled" not in res
    assert res["attempted"] == 6


def test_snapshot_build_aborts_instead_of_returning_a_partial_set() -> None:
    touched: list[str] = []

    class Ops:
        def __init__(self, name: str) -> None:
            self._name = name

        def features(self):
            touched.append(self._name)
            return {"watchlist": True}

        def build_index(self, config, feature):
            touched.append(f"{self._name}:index")
            return {"imdb:tt1": {"type": "movie", "ids": {"imdb": "tt1"}}}

    providers = {"PLEX": Ops("PLEX"), "TRAKT": Ops("TRAKT")}
    run_control.request_cancel()

    with pytest.raises(run_control.SyncCancelled):
        _snapshots.build_snapshots_for_feature(
            feature="watchlist",
            config={"plex": {}, "trakt": {}},
            providers=providers,
            snap_cache={},
            snap_ttl_sec=0,
            dbg=lambda *a, **k: None,
            emit_info=lambda *a, **k: None,
        )

    assert touched == []


def _run_thread_harness(monkeypatch, tmp_path, run_result: dict[str, Any]) -> tuple[Any, list[str], dict[str, list[str]]]:
    import api.syncAPI as sync

    log_buffers: dict[str, list[str]] = {"SYNC": []}

    def append_log(kind: str, message: str) -> None:
        log_buffers.setdefault(kind, []).append(str(message))

    monkeypatch.setitem(sys.modules, "crosswatch", types.SimpleNamespace(
        LOG_BUFFERS=log_buffers,
        RUNNING_PROCS={"SYNC": object()},
        SYNC_PROC_LOCK=threading.Lock(),
        STATS=types.SimpleNamespace(refresh_from_state=lambda _s: None, record_summary=lambda *_a: None),
        REPORT_DIR=tmp_path,
        strip_ansi=lambda value: str(value),
        _append_log=append_log,
        minimal=lambda value: value,
        canonical_key=lambda value: str(value),
    ))

    cfg = {
        "pairs": [{"id": "plex-trakt", "enabled": True, "source": "PLEX", "target": "TRAKT"}],
        "scheduling": {"webhooks": {"enabled": True, "base_url": "https://hc-ping.com/abc123"}},
    }
    monkeypatch.setattr(sync, "_env", lambda: (lambda: cfg, lambda _cfg: None))
    monkeypatch.setattr(sync, "_load_state", lambda: None)
    monkeypatch.setattr(sync, "_counts_from_state", lambda _s: None)
    monkeypatch.setattr(sync, "_provider_count_defaults", lambda: {})

    class FakeOrchestrator:
        def __init__(self, config: dict[str, Any]) -> None:
            self.config = config

        def run_pairs(self, **kwargs: Any) -> dict[str, Any]:
            return dict(run_result)

    monkeypatch.setattr(
        sync.importlib,
        "import_module",
        lambda name: types.SimpleNamespace(Orchestrator=FakeOrchestrator, __file__="fake.py")
        if name == "cw_platform.orchestrator"
        else __import__(name),
    )

    events: list[str] = []
    monkeypatch.setattr(
        sync,
        "notify_scheduler_webhook",
        lambda _cfg, event, _ctx, _snap=None, **_kw: (events.append(event), True)[1],
    )
    return sync, events, log_buffers


def test_cancelled_run_marks_summary_and_reports_failure(monkeypatch, tmp_path) -> None:
    sync, events, log = _run_thread_harness(monkeypatch, tmp_path, {"added": 1, "cancelled": True})

    sync._run_pairs_thread("run-cancel-1", {"source": "scheduler", "scheduler_mode": "standard"})
    snap = sync._summary_snapshot()

    assert snap["cancelled"] is True
    assert snap["exit_code"] == 0
    assert events == ["start", "failure"]
    assert any("Sync cancelled" in line for line in log["SYNC"])
    assert any("Cancelled. Total added: 1" in line for line in log["SYNC"])
    assert run_control.cancel_state()["run_id"] is None


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({}, []),
        ({"source": "scheduler", "scheduler_mode": "standard"}, ["https://hc-ping.com/abc123/fail"]),
    ],
    ids=["manual", "scheduled"],
)
def test_cancelled_run_only_alerts_for_scheduled_syncs(monkeypatch, tmp_path, overrides, expected) -> None:
    from services import scheduler_webhooks

    sync, _events, _log = _run_thread_harness(monkeypatch, tmp_path, {"added": 1, "cancelled": True})
    monkeypatch.setattr(sync, "notify_scheduler_webhook", scheduler_webhooks.notify_scheduler_webhook)

    posted: list[str] = []

    def fake_post(url, **_kwargs):
        posted.append(str(url))
        return types.SimpleNamespace(raise_for_status=lambda: None)

    monkeypatch.setattr(scheduler_webhooks.requests, "post", fake_post)

    sync._run_pairs_thread("run-cancel-2", overrides)

    assert sync._summary_snapshot()["cancelled"] is True
    assert [url for url in posted if url.endswith("/fail")] == expected


def test_resolve_completion_event_treats_cancelled_as_failure() -> None:
    from services.scheduler_webhooks import resolve_completion_event

    assert resolve_completion_event({}, {"exit_code": 0, "cancelled": True}) == "failure"
    assert resolve_completion_event({}, {"exit_code": 0, "cancelled": False}) == "success"


def test_normal_run_is_not_marked_cancelled(monkeypatch, tmp_path) -> None:
    sync, events, log = _run_thread_harness(monkeypatch, tmp_path, {"added": 1, "cancelled": False})

    sync._run_pairs_thread("run-ok-1", {"source": "scheduler", "scheduler_mode": "standard"})

    assert sync._summary_snapshot()["cancelled"] is False
    assert events == ["start", "success"]
    assert any("Done. Total added: 1" in line for line in log["SYNC"])


def test_cancel_endpoint_requires_a_running_sync(monkeypatch, tmp_path) -> None:
    sync, _events, _log = _run_thread_harness(monkeypatch, tmp_path, {})

    monkeypatch.setattr(sync, "_is_sync_running", lambda: False)
    assert sync.api_cancel_sync() == {"ok": False, "error": "No sync running", "running": False}
    assert run_control.cancel_state()["run_id"] is None

    monkeypatch.setattr(sync, "_is_sync_running", lambda: True)
    sync._summary_set("run_id", "run-77")
    result = sync.api_cancel_sync()

    assert result["ok"] is True
    assert result["run_id"] == "run-77"
    assert run_control.cancel_requested("run-77") is True
    assert sync.api_cancel_status()["cancel_requested"] is True


def test_scheduler_queues_guard_on_cancel() -> None:
    import inspect
    from services import scheduling

    src = inspect.getsource(scheduling.SyncScheduler._adv_run_due)
    guards = src.count("consume_queue_stop()")

    assert guards == 2, "both the advanced job loop and the workflow step loop must guard on cancel"
    assert "skipping remaining jobs" in src
    assert "skipping remaining steps" in src


def test_manual_run_clears_a_stale_queue_stop(monkeypatch, tmp_path) -> None:
    sync, _events, _log = _run_thread_harness(monkeypatch, tmp_path, {"added": 0, "cancelled": False})
    run_control.request_cancel("old-run")
    assert run_control.queue_stopped() is True

    sync.clear_queue_stop()

    assert run_control.queue_stopped() is False
