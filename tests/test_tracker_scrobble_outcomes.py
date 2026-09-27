# tests/test_tracker_scrobble_outcomes.py
# CrossWatch - Tracker scrobble filtering and delivery outcome regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from dataclasses import replace
from importlib import import_module
from types import SimpleNamespace

import pytest

from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent


def event(**changes):
    return replace(ScrobbleEvent("start", "movie", {"tmdb": "123"}, "Test movie", 2020, None, None, 60,
                                "test-user", "test-server", "test-session", {}), **changes)


@pytest.fixture(params=["simkl", "mdblist", "trakt"])
def tracker(request, monkeypatch, config_base):
    provider = request.param
    module = import_module(f"providers.scrobble.{provider}.sink")
    cls = getattr(module, {"simkl": "SimklSink", "mdblist": "MDBListSink", "trakt": "TraktSink"}[provider])
    cfg = {provider: {"client_id": "test-client", "api_key": "test-key", "access_token": "test-token"},
           "scrobble": {"trakt": {"progress_step": 25, "watched_at": 90}, "watch": {"pause_debounce_seconds": 5}}}
    sink = cls()
    calls = []
    monkeypatch.setattr(sink, "_note_watch", lambda *a, **kw: None)
    monkeypatch.setattr(module, "record_scrobble_event", lambda *a, **kw: None)
    monkeypatch.setattr(module, "_auto_remove_across", lambda *a, **kw: None)
    if provider == "simkl":
        monkeypatch.setattr(module, "_fresh_token", lambda block, *a: block.get("access_token", ""))
    if provider == "trakt":
        monkeypatch.setattr(sink, "_enqueue", sink._deliver)
        monkeypatch.setattr(sink, "_bind_event", lambda ev, cfg: ev)

    def send_http(path, body, *args):
        calls.append((path, dict(body)))
        return {"ok": True, "status": 201, "resp": {"action": path.rsplit("/", 1)[-1]}}

    monkeypatch.setattr(sink, "_send_http", send_http)
    return SimpleNamespace(provider=provider, module=module, sink=sink, cfg=cfg, calls=calls)


def test_progress_skip_reports_filter_values_without_sending(tracker):
    tracker.sink.send(event(), tracker.cfg)
    count = len(tracker.calls)
    assert count > 0
    result = tracker.sink.send(event(progress=72), tracker.cfg)
    assert result["ok"] and result["log_status"] == "skipped"
    assert result["reason"] == "progress_step"
    assert result["progress_step"] == 25
    if tracker.provider in ("simkl", "mdblist"):
        assert result["previous_bucket"] == result["progress_bucket"] == 50
        assert "previous_progress" not in result
    else:
        assert result["previous_progress"] == 60
    assert len(tracker.calls) == count
    tracker.sink.send(event(progress=73, raw={"_cw_seek": True}), tracker.cfg)
    assert len(tracker.calls) > count


@pytest.mark.parametrize("tracker", ["simkl", "mdblist"], indirect=True)
@pytest.mark.parametrize("progress,bucket", [(72, 50), (2, 1)])
def test_bucket_skip_log_uses_comparison_values(tracker, monkeypatch, progress, bucket):
    tracker.sink.send(event(), tracker.cfg)
    count = len(tracker.calls)
    assert count > 0
    tracker.cfg["scrobble"]["watch"].update(route_id="R1", route_provider="plex", route_sink=tracker.provider)
    messages = []
    monkeypatch.setattr("providers.scrobble.scrobble._log", lambda message, *a: messages.append(message))
    monkeypatch.setattr("providers.scrobble.scrobble.maybe_enrich_event_for_sink", lambda ev, *a: ev)
    dispatcher = Dispatcher([tracker.sink], cfg_provider=lambda: tracker.cfg)

    assert dispatcher.dispatch(event(progress=progress))
    assert len(tracker.calls) == count
    matching = [message for message in messages if "reason=progress_step" in message]
    assert len(matching) == 1
    assert f"step=25 previous_bucket=50 bucket={bucket}" in matching[0]
    assert "previous_p=" not in matching[0]


def test_progress_step_boundary_still_sends(tracker):
    tracker.sink.send(event(progress=50), tracker.cfg)
    count = len(tracker.calls)
    result = tracker.sink.send(event(progress=75), tracker.cfg)
    assert result is None or (not result.get("skipped") and result.get("log_status") != "skipped")
    assert len(tracker.calls) > count


def test_debounced_pause_is_reported_without_sending(tracker):
    tracker.sink.send(event(action="pause", progress=30), tracker.cfg)
    count = len(tracker.calls)
    assert count > 0
    result = tracker.sink.send(event(action="pause", progress=30), tracker.cfg)
    assert result == {"ok": True, "log_status": "skipped", "reason": "debounced"}
    assert len(tracker.calls) == count


def test_missing_connection_is_reported_without_sending(tracker):
    cfg = {**tracker.cfg, tracker.provider: {}}
    result = tracker.sink.send(event(), cfg)
    assert result == {"ok": True, "log_status": "skipped", "reason": "not_configured"}
    assert not tracker.calls


@pytest.mark.parametrize("tracker", ["simkl", "mdblist"], indirect=True)
def test_suppressed_start_and_duplicate_completion_are_reported(tracker):
    assert tracker.sink.send(event(progress=100), tracker.cfg)["reason"] == "start_suppressed"
    assert not tracker.calls
    tracker.sink.send(event(action="stop", progress=95), tracker.cfg)
    count = len(tracker.calls)
    assert count > 0
    result = tracker.sink.send(event(action="stop", progress=95), tracker.cfg)
    assert result == {"ok": True, "log_status": "skipped", "reason": "session_completed"}
    assert len(tracker.calls) == count


def test_trakt_accepts_start_for_queued_delivery(monkeypatch):
    from providers.scrobble.trakt.sink import TraktSink

    sink = TraktSink()
    queued = []
    monkeypatch.setattr(sink, "_enqueue", queued.append)
    monkeypatch.setattr(sink, "_bind_event", lambda ev, cfg: ev)
    monkeypatch.setattr(sink, "_note_watch", lambda *a, **kw: None)
    cfg = {"trakt": {"client_id": "test-client", "access_token": "test-token"}}
    result = sink.send(event(), cfg)
    assert result is None or (not result.get("skipped") and result.get("log_status") != "skipped")
    assert len(queued) == 1
    assert queued[0]["path"] == "/scrobble/start"


@pytest.mark.parametrize("action,progress,reason", [("pause", 0, "no_progress"), ("stop", 0, "no_progress"),
                                                  ("pause", 85, "completion_held"), ("stop", 85, "completion_held")])
def test_trakt_held_events_report_why_nothing_was_queued(monkeypatch, action, progress, reason):
    from providers.scrobble.trakt.sink import TraktSink

    sink = TraktSink()
    queued = []
    monkeypatch.setattr(sink, "_enqueue", queued.append)
    monkeypatch.setattr(sink, "_bind_event", lambda ev, cfg: ev)
    cfg = {"trakt": {"client_id": "test-client", "access_token": "test-token"}, "scrobble": {"trakt": {"watched_at": 90}}}
    assert sink.send(event(action=action, progress=progress), cfg) == {"ok": True, "log_status": "skipped", "reason": reason}
    assert not queued


@pytest.mark.parametrize("provider,cls_name", [("crosswatch", "CrossWatchSink"), ("floppy", "FloppySink"),
                                            ("punchplay", "PunchPlaySink"), ("bingebase", "BingeBaseSink"),
                                            ("flicklist", "FlickListSink"), ("scrob", "ScrobSink")])
def test_unconfigured_destinations_log_skipped_without_retrying(monkeypatch, config_base, provider, cls_name):
    module = import_module(f"providers.scrobble.{provider}.sink")
    cfg = {provider: {"enabled": False}, "scrobble": {"watch": {"route_id": "R1", "route_provider": "plex", "route_sink": provider}}}
    sink = getattr(module, cls_name)(cfg_provider=lambda: cfg)
    messages = []
    monkeypatch.setattr("providers.scrobble.scrobble._log", lambda message, *a: messages.append(message))
    monkeypatch.setattr("providers.scrobble.scrobble.maybe_enrich_event_for_sink", lambda ev, *a: ev)
    monkeypatch.setattr("requests.sessions.Session.request", lambda *a, **kw: pytest.fail("Skipped event made a request"))
    dispatcher = Dispatcher([sink], cfg_provider=lambda: cfg)

    assert dispatcher.dispatch(event())
    assert any(f"plex->{provider}: skipped start" in message and "reason=not_configured" in message for message in messages)
    assert not any(": accepted " in message or ": failed " in message for message in messages)
    assert not dispatcher._pending
    assert not dispatcher._retry_after
    assert not dispatcher._failed_ids


@pytest.mark.parametrize("source", ["plex", "jellyfin", "emby"])
@pytest.mark.parametrize("action,first_progress,next_progress,reason", [
    ("start", 60, 72, "progress_step"),
    ("pause", 30, 30, "debounced"),
])
def test_webhook_diagnostic_skip_preserves_activity(tracker, monkeypatch, source, action, first_progress, next_progress, reason):
    from providers.webhooks import dispatch

    monkeypatch.setattr(dispatch, "webhook_sinks", lambda *a: [tracker.provider])
    monkeypatch.setattr(dispatch, "sink_configured", lambda *a: True)
    monkeypatch.setattr(dispatch, "_make_sink", lambda *a: tracker.sink)
    monkeypatch.setattr(dispatch, "maybe_enrich_event_for_sink", lambda ev, *a: ev)
    args = {"media_type": "movie", "ids": {"tmdb": "123"}, "title": "Test movie",
            "session_key": "test-session", "cfg": tracker.cfg}
    first = dispatch.dispatch_scrobble(source, f"/scrobble/{action}", progress=first_progress, **args).json()
    count = len(tracker.calls)
    second = dispatch.dispatch_scrobble(source, f"/scrobble/{action}", progress=next_progress, **args).json()

    assert count > 0
    assert len(tracker.calls) == count
    assert first["activity_recorded"] is second["activity_recorded"] is True
    assert second["targets"][0]["reason"] == reason
    assert not second["targets"][0].get("skipped")


@pytest.mark.parametrize("source", ["plex", "jellyfin", "emby"])
@pytest.mark.parametrize("tracker", ["simkl", "mdblist"], indirect=True)
def test_webhook_duplicate_completion_preserves_activity(tracker, monkeypatch, source):
    from providers.webhooks import dispatch

    monkeypatch.setattr(dispatch, "webhook_sinks", lambda *a: [tracker.provider])
    monkeypatch.setattr(dispatch, "sink_configured", lambda *a: True)
    monkeypatch.setattr(dispatch, "_make_sink", lambda *a: tracker.sink)
    monkeypatch.setattr(dispatch, "maybe_enrich_event_for_sink", lambda ev, *a: ev)
    args = {"media_type": "movie", "ids": {"tmdb": "123"}, "title": "Test movie",
            "session_key": "test-session", "progress": 95, "cfg": tracker.cfg}
    first = dispatch.dispatch_scrobble(source, "/scrobble/stop", **args).json()
    count = len(tracker.calls)
    second = dispatch.dispatch_scrobble(source, "/scrobble/stop", **args).json()

    assert count > 0
    assert len(tracker.calls) == count
    assert first["activity_recorded"] is second["activity_recorded"] is True
    assert second["targets"][0]["reason"] == "session_completed"
