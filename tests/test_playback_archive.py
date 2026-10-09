# tests/test_playback_archive.py
# CrossWatch - Playback lifecycle storage and delivery regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from dataclasses import replace

import pytest

from cw_platform.event_archive import playback_recorder, recorder, scrobble_recorder
from cw_platform.event_archive.db import connect
from cw_platform.event_archive.groups import correlate, list_groups
from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent
from providers.webhooks.dispatch import _event


@pytest.fixture
def archive(monkeypatch, tmp_path):
    c = connect(tmp_path / "events.db")
    for module in (playback_recorder, recorder, scrobble_recorder):
        monkeypatch.setattr(module, "get_conn", lambda: c)
    monkeypatch.setattr(scrobble_recorder, "_SEEN", scrobble_recorder.OrderedDict())
    clock = [1000]
    monkeypatch.setattr(playback_recorder.time, "time", lambda: clock[0])
    cfg = {"scrobble": {"watch": {"route_provider": "jellyfin", "route_sink": "trakt"}}}
    yield c, cfg, clock
    c.close()


def event(action="start", **changes):
    return replace(ScrobbleEvent(action, "movie", {"imdb": "tt0298203"}, "8 Mile", 2002,
                                None, None, 2, "alice", "server", "session", {}), **changes)


def detail(c):
    return json.loads(c.execute("SELECT detail FROM events WHERE event_type='scrobble_playback'").fetchone()[0])


def test_pause_resume_stop_share_one_record_and_survive_refresh(archive):
    c, cfg, clock = archive
    for action, status in [("start", "running"), ("pause", "paused"), ("start", "running"), ("stop", "stopped")]:
        playback_recorder.record_playback(event(action), cfg)
        correlate(conn=c)
        assert list_groups(domain="scrobble", conn=c)["items"][0]["status"] == status
        clock[0] += 10
    assert c.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
    assert [x[1] for x in detail(c)["playback"]] == ["start", "pause", "resume", "stop"]
    correlate(conn=c, reset=True)
    group = list_groups(domain="scrobble", status="stopped", conn=c)["items"][0]
    assert group["first_event_at"] == 1000
    assert group["last_event_at"] == 1030


def test_repeated_pauses_do_not_write_and_noisy_sessions_are_bounded(archive):
    c, cfg, clock = archive
    playback_recorder.record_playback(event(), cfg)
    playback_recorder.record_playback(event("pause"), cfg)
    writes = c.total_changes
    for i in range(100):
        clock[0] += 1
        playback_recorder.record_playback(event("pause", progress=i), cfg)
    assert c.total_changes == writes
    for i in range(200):
        playback_recorder.record_playback(event("start" if i % 2 == 0 else "pause"), cfg)
    playback_recorder.record_playback(event("stop"), cfg)
    d = detail(c)
    assert len(d["playback"]) == 51
    assert d["playback"][0][1] == "start"
    assert d["playback"][-1][1] == "stop"
    assert d["omitted"] == 152
    assert c.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1


def test_delivery_failure_does_not_replace_stopped_state(archive):
    c, cfg, clock = archive
    playback_recorder.record_playback(event("stop"), cfg)
    playback_recorder.record_playback(event("stop"), cfg, delivery="failed", reason="503")
    correlate(conn=c)
    assert list_groups(domain="scrobble", conn=c)["items"][0]["status"] == "stopped"
    assert detail(c)["delivery"]["status"] == "failed"
    assert list_groups(domain="scrobble", status="failed", conn=c)["total"] == 1
    playback_recorder.record_playback(event("stop"), cfg, delivery="succeeded")
    assert detail(c)["delivery"]["status"] == "succeeded"
    assert len(detail(c)["playback"]) == 1


def test_confirmed_completion_is_not_downgraded_by_stop(archive):
    c, cfg, clock = archive
    playback_recorder.record_playback(event(), cfg)
    scrobble_recorder.record_watch(event("stop", progress=95), action="stop", source_provider="jellyfin", destination_provider="trakt")
    clock[0] += 10
    playback_recorder.record_playback(event("stop", progress=95), cfg)
    correlate(conn=c)
    assert list_groups(domain="scrobble", conn=c)["items"][0]["status"] == "completed"
    assert c.execute("SELECT COUNT(*) FROM event_groups").fetchone()[0] == 1


def test_dispatch_records_original_stop_even_if_sink_deduplicates_pause(archive):
    c, cfg, clock = archive
    sent = []
    class Sink:
        def send(self, ev, cfg=None):
            sent.append(ev.action)
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    dispatcher.dispatch(event("pause"))
    dispatcher.dispatch(event("pause", raw={"_cw_playback_action": "stop"}))
    assert sent == ["pause"]
    assert detail(c)["state"] == "stopped"


@pytest.mark.parametrize("raw", [{"event": "media.stop"}, {"NotificationType": "PlaybackStop"}, {"Event": "playback.stopped"}])
def test_webhook_preserves_source_stop_when_dispatching_pause(raw):
    ev = _event(provider="jellyfin", path="pause", media_type="movie", ids={"imdb": "tt0298203"},
                title="8 Mile", year=2002, season=None, episode=None, progress=2, account="alice",
                server_uuid=None, session_key="session", raw=raw)
    assert ev.action == "pause"
    assert ev.raw["_cw_playback_action"] == "stop"


def test_filtered_route_does_not_record_playback(archive):
    c, cfg, clock = archive
    cfg["scrobble"]["watch"]["route_enabled"] = False
    Dispatcher([], cfg_provider=lambda: cfg).dispatch(event())
    assert c.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0


def test_late_pause_does_not_reopen_stopped_session(archive):
    c, cfg, clock = archive
    playback_recorder.record_playback(event("stop"), cfg)
    clock[0] += 1
    playback_recorder.record_playback(event("pause"), cfg)
    assert detail(c)["state"] == "stopped"
    assert len(detail(c)["playback"]) == 1


def test_enriched_destination_ids_keep_the_source_thread(archive):
    c, cfg, clock = archive
    source = playback_recorder.prepare_playback(event())
    playback_recorder.record_playback(source, cfg)
    mapped = replace(source, ids={"tmdb": "111"}, title="Mapped title")
    playback_recorder.record_playback(mapped, cfg, delivery="succeeded")
    scrobble_recorder.record_watch(mapped, action="stop", source_provider="jellyfin", destination_provider="trakt")
    correlate(conn=c)
    assert c.execute("SELECT COUNT(*) FROM event_groups").fetchone()[0] == 1
    assert list_groups(domain="scrobble", conn=c)["items"][0]["status"] == "completed"
    assert detail(c)["delivery"]["status"] == "succeeded"
