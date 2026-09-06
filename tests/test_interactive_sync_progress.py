# tests/test_interactive_sync_progress.py
# CrossWatch - Interactive Sync Progress Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace

import pytest

from services.interactive_sync_progress import SyncProgress
from test_interactive_sync import api_client, setup_ops, item
from test_orchestrator_dry_run_no_side_effects import _cfg


def test_provider_counts_are_scoped_and_unknown_totals_stay_indeterminate():
    progress = SyncProgress()
    progress.event(dict(event="snapshot:start", provider="TRAKT", feature="history"))
    assert progress.public()["percent"] is None
    progress.event(dict(event="snapshot:progress", dst="TRAKT", feature="history", done=12500, total=50000))
    state = progress.public()
    assert state["percent"] == 25
    assert state["items_read"] == 12500
    progress.event(dict(event="snapshot:done", provider="TRAKT", feature="history", count=50000))
    progress.event(dict(event="snapshot:start", provider="PLEX", feature="history"))
    state = progress.public()
    assert state["done"] == 0 and state["percent"] is None
    assert state["items_read"] == 50000
    assert state["provider"] == "PLEX"


def test_elapsed_and_quiet_times_work_beyond_an_hour(monkeypatch):
    from services import interactive_sync_progress as module

    now = [100.0]
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: now[0], time=lambda: now[0] + 1000000))
    progress = SyncProgress()
    progress.event(dict(event="snapshot:start", provider="PLEX", feature="history"))
    now[0] += 3900
    state = progress.public()
    assert state["elapsed_seconds"] == state["quiet_seconds"] == 3900
    assert state["running"] and state["percent"] is None
    progress.event(dict(event="api:request", provider="PLEX"))
    assert progress.public()["quiet_seconds"] == 0
    assert progress.public()["requests"] == 1
    progress.finish("review", "Ready")
    now[0] += 3600
    assert progress.public()["elapsed_seconds"] == 3900
    assert not progress.public()["running"]


def test_apply_progress_counts_processed_items_and_ignores_duplicate_outer_events():
    progress = SyncProgress()
    progress.begin("apply")
    progress.event(dict(event="apply:add:start", dst="TRAKT", feature="history", count=50000))
    progress.event(dict(event="apply:add:progress", dst="TRAKT", feature="history", done=10000, total=50000))
    progress.event(dict(event="two:apply:add:A:done", dst="TRAKT", feature="history", count=10000))
    assert progress.public()["percent"] == 20
    assert progress.public()["unit"] == "changes processed"
    progress.event(dict(event="apply:add:done", dst="TRAKT", feature="history", attempted=50000, count=49900))
    assert progress.public()["done"] == 50000
    progress.finish("cancelled", "Cancelled")
    progress.event(dict(event="api:request"))
    assert progress.public()["stage"] == "cancelled"
    assert not progress.public()["running"]


def test_recent_activity_is_bounded_and_does_not_include_raw_provider_errors():
    progress = SyncProgress()
    for _ in range(1000):
        progress.event(dict(event="feature:error", provider="PLEX", feature="history", error="secret-token", traceback="private-data"))
    state = progress.public()
    assert len(state["recent"]) == 6
    assert "secret-token" not in str(state) and "private-data" not in str(state)


def test_preview_exposes_provider_progress_while_index_is_being_read(config_base, monkeypatch):
    from services import interactive_sync as svc

    src, dst = setup_ops(config_base, monkeypatch, [item(1)])
    session = svc.Session(pair_id="p1", owner="local")
    observed = []
    original = src.build_index

    def build_index(*args, **kwargs):
        src.ctx.emit("snapshot:progress", dst="SRC", feature="watchlist", done=12500, total=50000)
        observed.append(session.public()["progress"])
        return original(*args, **kwargs)

    monkeypatch.setattr(src, "build_index", build_index)
    try:
        svc.refresh(session, _cfg(False), {})
        assert observed[0]["percent"] == 25
        assert observed[0]["provider"] == "SRC"
        assert session.public()["progress"]["stage"] == "review"
        assert not dst.add_calls
    finally:
        session.close()


def test_active_review_does_not_expire_after_an_hour(api_client):
    client, session, api = api_client
    session.status = "reading"
    session.touched -= 7200
    result = client.get(f"/api/interactive-sync/{session.id}")
    assert result.status_code == 200
    assert result.json()["status"] == "reading"


@pytest.mark.parametrize("verbose", [False, True])
def test_http_attempts_count_without_verbose_logs_or_health_double_counting(monkeypatch, verbose):
    import requests
    from providers.sync._mod_common import build_session

    progress = SyncProgress()
    events = []
    ctx = SimpleNamespace(interactive=SimpleNamespace(on_progress=progress.event), emit=lambda event, **data: (events.append(event), progress.event(dict(event=event, **data))))
    progress.event(dict(event="api:hit", provider="PLEX", endpoint="health:status"))
    assert progress.public()["requests"] == 0
    outcomes = iter([requests.Response(), requests.Timeout("Test timeout"), requests.Response()])

    def transport(*args, **kwargs):
        outcome = next(outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(requests.Session, "request", transport)
    with build_session("PLEX", ctx, emit_hits=verbose) as http:
        http.get("https://example.invalid/history")
        assert progress.public()["requests"] == 1
        with pytest.raises(requests.Timeout):
            http.get("https://example.invalid/history")
        assert progress.public()["requests"] == 2
        http.get("https://example.invalid/history")
        assert progress.public()["requests"] == 3
    assert len(events) == (3 if verbose else 0)


@pytest.mark.parametrize("verbose", [False, True])
def test_normal_http_session_has_no_interactive_observer(monkeypatch, verbose):
    import requests
    from providers.sync._mod_common import build_session

    calls, events = [], []
    response = requests.Response()
    response.status_code = 200

    def transport(session, method, url, **kwargs):
        calls.append((method, url, kwargs))
        return response

    monkeypatch.setattr(requests.Session, "request", transport)
    ctx = SimpleNamespace(interactive=None, emit=lambda event, **data: events.append(event))
    with build_session("PLEX", ctx, emit_hits=verbose) as http:
        assert http._on_request is None
        assert http.get("https://example.invalid/history", params={"page": 2}, timeout=5) is response
    assert len(calls) == 1
    assert calls[0][2]["params"] == {"page": 2}
    assert calls[0][2]["timeout"] == 5
    assert events == (["api:hit"] if verbose else [])


def test_request_counter_advances_during_preview_and_apply(config_base, monkeypatch):
    from copy import deepcopy
    import requests
    from api import syncAPI
    from providers.sync._mod_common import build_session
    from services import interactive_sync as svc

    monkeypatch.delenv("CW_API_HITS", raising=False)
    monkeypatch.setattr(requests.Session, "request", lambda *_a, **_k: requests.Response())
    src, dst = setup_ops(config_base, monkeypatch, [item(1)])
    original_read, original_add = src.build_index, dst.add
    seen = []
    session = svc.Session(pair_id="p1", owner="local")

    def read(*args, **kwargs):
        with build_session("SRC", src.ctx) as http:
            for _ in range(5):
                http.get("https://example.invalid/watchlist")
                seen.append(session.public()["progress"]["requests"])
        return original_read(*args, **kwargs)

    def add(*args, **kwargs):
        with build_session("DST", dst.ctx) as http:
            http.post("https://example.invalid/watchlist")
        return original_add(*args, **kwargs)

    monkeypatch.setattr(src, "build_index", read)
    monkeypatch.setattr(dst, "add", add)
    cfg = _cfg(False)
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: deepcopy(cfg), lambda *_: None))
    try:
        svc.refresh(session, cfg, {})
        assert seen == [1, 2, 3, 4, 5]
        assert session.public()["progress"]["requests"] == 5
        svc.apply(session, cfg, set(session.plan.rows))
        assert session.status == "complete"
        assert session.public()["progress"]["requests"] == 11
        assert len(dst.add_calls) == 1
    finally:
        session.close()
