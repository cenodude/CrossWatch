# tests/test_playback_completed_progress.py
# CrossWatch - Completed watches supersede stale playback progress
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from datetime import datetime, timezone
import time

import pytest

from cw_platform.event_archive import query, recorder, scrobble_recorder
from cw_platform.event_archive.db import connect
from services.playback_progress import service as playback
from services.playback_progress.models import PlaybackListResult, PlaybackRecord


@pytest.fixture
def archive(monkeypatch, tmp_path):
    conn = connect(tmp_path / "events.db")
    monkeypatch.setattr(query, "get_conn", lambda: conn)
    monkeypatch.setattr(recorder, "get_conn", lambda: conn)
    monkeypatch.setattr(scrobble_recorder, "get_conn", lambda: conn)
    monkeypatch.setattr(scrobble_recorder, "_SEEN", scrobble_recorder.OrderedDict())
    yield conn
    conn.close()


def complete(**overrides):
    values = dict(
        event_type="scrobble_completed", source_provider="plex", destination_provider="crosswatch",
        ids={"imdb": "tt123", "tmdb": "123"}, media_type="movie", progress=100, created_at=2000,
    )
    scrobble_recorder.record(**(values | overrides))


def record(timestamp=1000, **overrides):
    values = dict(
        provider="crosswatch", provider_label="CrossWatch", instance_id="default", instance_label="Default",
        remote_id="123", canonical_key="tmdb:123", media_type="movie", ids={"tmdb": "123", "imdb": "tt123"},
        progress_percent=49, updated_at=datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
    )
    return PlaybackRecord(**(values | overrides))


def test_confirmed_completion_removes_each_stale_destination_but_preserves_rewatch(archive):
    for provider in ("crosswatch", "trakt", "simkl"):
        complete(destination_provider=provider)
    archive.execute("UPDATE events SET acknowledged_at=2100")
    rows = [record(provider=provider).to_dict() for provider in ("crosswatch", "trakt", "simkl")]
    newer = record(timestamp=2100).to_dict()
    completed_second = record(timestamp=2000.5).to_dict()
    assert playback._without_completed_progress([*rows, completed_second, newer]) == [newer]


def test_completion_is_scoped_to_account_and_media_type(archive):
    complete()
    other_account = record(instance_id="CW-P02").to_dict()
    other_provider = record(provider="trakt").to_dict()
    episode = record(media_type="episode", season=1, episode=1).to_dict()
    unknown_time = record(updated_at=None).to_dict()
    expected = [other_account, other_provider, episode, unknown_time]
    assert playback._without_completed_progress([record().to_dict(), *expected]) == expected


def test_episode_completion_preserves_next_episode(archive):
    complete(media_type="episode", season=3, episode=7)
    watched = record(media_type="episode", season=3, episode=7).to_dict()
    upcoming = record(media_type="episode", season=3, episode=8).to_dict()
    assert playback._without_completed_progress([watched, upcoming]) == [upcoming]


def test_failed_scrobble_and_unavailable_archive_keep_progress(archive, monkeypatch):
    complete(event_type="scrobble_failed")
    rows = [record().to_dict()]
    assert playback._without_completed_progress(rows) == rows
    monkeypatch.setattr(query, "get_conn", lambda: None)
    assert playback._without_completed_progress(rows) == rows


def test_completed_query_batches_keys_and_uses_latest_watch(archive):
    complete(created_at=1500, session_key="first")
    complete(created_at=2000, session_key="second")
    keys = [f"tmdb:{index}" for index in range(600)] + ["imdb:tt123"]
    rows = query.latest_completed_watches(keys)
    assert len(rows) == 1
    assert rows[0]["completed_at"] == 2000


def test_cached_service_response_reconciles_completion_without_provider_reload(archive, monkeypatch):
    monkeypatch.setattr(playback, "load_config", lambda: {"crosswatch": {"enabled": True}})
    monkeypatch.setattr(playback, "_overlay_live_streams", lambda *args, **kwargs: None)
    service = playback.PlaybackProgressService()
    adapter = service.adapters["crosswatch"]
    monkeypatch.setattr(adapter.ops, "is_configured", lambda cfg: True)
    cached = PlaybackListResult(True, "crosswatch", "default", items=[record()])
    key = service._cache_key("crosswatch", "default", adapter)
    service._cache[key] = {"ts": time.time(), "result": cached}
    first = service.items(provider="crosswatch")
    assert first["total"] == 1
    complete()
    result = service.items(provider="crosswatch")
    assert result["total"] == 0
    assert result["items"] == []
    assert result["months"] == []
    assert len(cached.items) == 1
