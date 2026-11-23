# tests/test_playback_progressive_list.py
# CrossWatch - Playback progress list returns fast providers first
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import threading
import time

import pytest

from services.playback_progress.adapters.base import PlaybackProgressAdapter
from services.playback_progress.models import PlaybackCapabilities, PlaybackListResult, PlaybackRecord
from services.playback_progress.service import PlaybackProgressService
import services.playback_progress.service as playback_service


class _SlowAdapter(PlaybackProgressAdapter):
    provider = "jellyfin"
    provider_label = "Jellyfin"

    def __init__(self) -> None:
        self.release = threading.Event()
        self.started = threading.Event()
        self.calls = 0

    def capabilities(self, config_view, *, instance_id, instance_label):
        return PlaybackCapabilities(
            provider=self.provider,
            provider_label=self.provider_label,
            instance_id=instance_id,
            instance_label=instance_label,
            configured=True,
            read=True,
        )

    def list_progress(self, config_view, *, instance_id, instance_label, force_refresh=False):
        self.calls += 1
        self.started.set()
        self.release.wait(5)
        return PlaybackListResult(False, self.provider, instance_id, error_code="provider_unavailable", message="down", retryable=True)


def _service(monkeypatch):
    cfg = {"jellyfin": {"server": "http://jf", "access_token": "t", "user_id": "u"}}
    monkeypatch.setattr(playback_service, "load_config", lambda: cfg)
    monkeypatch.setattr(playback_service, "FIRST_RESPONSE_SECONDS", 0.05)
    service = PlaybackProgressService()
    adapter = _SlowAdapter()
    service.adapters["jellyfin"] = adapter
    return service, adapter


def test_progressive_list_reports_slow_provider_as_loading(monkeypatch):
    service, adapter = _service(monkeypatch)
    try:
        first = service.items(provider="jellyfin", force_refresh=True, progressive=True)
        assert first["errors"] == []
        assert [row["provider"] for row in first["loading"]] == ["jellyfin"]

        again = service.items(provider="jellyfin", progressive=True)
        assert [row["provider"] for row in again["loading"]] == ["jellyfin"]
        assert adapter.calls == 1

        adapter.release.set()
        done = service.items(provider="jellyfin", progressive=True)
        while done["loading"]:
            done = service.items(provider="jellyfin", progressive=True)
        assert done["loading"] == []
        assert [row["error_code"] for row in done["errors"]] == ["provider_unavailable"]
        assert adapter.calls == 1
    finally:
        adapter.release.set()


def test_non_progressive_list_waits_for_provider(monkeypatch):
    service, adapter = _service(monkeypatch)
    adapter.release.set()
    result = service.items(provider="jellyfin", force_refresh=True)
    assert result["loading"] == []
    assert [row["error_code"] for row in result["errors"]] == ["provider_unavailable"]


def test_progressive_refresh_keeps_previous_items_while_provider_is_loading(monkeypatch):
    service, adapter = _service(monkeypatch)
    record = PlaybackRecord("jellyfin", "Jellyfin", "default", "Default", "movie-1", "tmdb:1", "movie", title="Cached movie", ids={"tmdb": "1"}, progress_percent=40)
    key = service._cache_key("jellyfin", "default", adapter)
    service._cache[key] = {"ts": time.time() - 300, "result": PlaybackListResult(True, "jellyfin", "default", items=[record])}
    try:
        result = service.items(provider="jellyfin", progressive=True)
        assert [item["title"] for item in result["items"]] == ["Cached movie"]
        assert [row["provider"] for row in result["loading"]] == ["jellyfin"]
        assert result["loading"][0]["cached"] is True
        assert result["errors"] == []
        again = service.items(provider="jellyfin", progressive=True)
        assert [item["title"] for item in again["items"]] == ["Cached movie"]
        adapter.release.set()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            done = service.items(provider="jellyfin", progressive=True)
            if not done["loading"]:
                break
        assert done["loading"] == []
        assert [item["title"] for item in done["items"]] == ["Cached movie"]
        assert done["errors"][0]["error_code"] == "provider_unavailable"
        assert adapter.calls == 1
    finally:
        adapter.release.set()


def test_progressive_refresh_does_not_restore_invalidated_or_expired_items(monkeypatch):
    service, adapter = _service(monkeypatch)
    record = PlaybackRecord("jellyfin", "Jellyfin", "default", "Default", "movie-1", "tmdb:1", "movie", title="Old movie", progress_percent=40)
    key = service._cache_key("jellyfin", "default", adapter)
    try:
        service._cache[key] = {"ts": time.time() - 90000, "result": PlaybackListResult(True, "jellyfin", "default", items=[record])}
        assert service.items(provider="jellyfin", progressive=True)["items"] == []
        service._cache[key]["ts"] = time.time() - 300
        service.invalidate("jellyfin", "default")
        assert service._cached_result({"provider": "jellyfin", "instance_id": "default"}) is None
    finally:
        adapter.release.set()


def test_unchanged_activity_renews_cache_freshness(monkeypatch):
    service, adapter = _service(monkeypatch)
    monkeypatch.setattr(adapter, "activity_marker", lambda *args, **kwargs: "same", raising=False)
    key = service._cache_key("jellyfin", "default", adapter)
    result = PlaybackListResult(True, "jellyfin", "default")
    service._cache[key] = {"ts": time.time() - 300, "result": result, "activity_marker": "same"}
    spec = {"provider": "jellyfin", "instance_id": "default", "instance_label": "Default"}
    assert service._list_one(playback_service.load_config(), spec, False) is result
    assert service._fresh_cached_result(spec) is result
    assert adapter.calls == 0


def test_fresh_provider_results_do_not_queue_background_work(monkeypatch):
    service, adapter = _service(monkeypatch)
    key = service._cache_key("jellyfin", "default", adapter)
    service._cache[key] = {"ts": time.time(), "result": PlaybackListResult(True, "jellyfin", "default")}

    def unexpected(*args, **kwargs):
        raise AssertionError("Fresh provider cache should not need a worker")

    monkeypatch.setattr(service, "_submit_list", unexpected)
    result = service.items(provider="jellyfin", progressive=True)
    assert result["loading"] == []
    assert result["errors"] == []


def test_unchanged_activity_does_not_keep_an_expired_failure(monkeypatch):
    service, adapter = _service(monkeypatch)
    monkeypatch.setattr(adapter, "activity_marker", lambda *args, **kwargs: "same", raising=False)
    key = service._cache_key("jellyfin", "default", adapter)
    service._cache[key] = {"ts": time.time() - 300, "result": PlaybackListResult(False, "jellyfin", "default"), "activity_marker": "same"}
    adapter.release.set()
    service._list_one(playback_service.load_config(), {"provider": "jellyfin", "instance_id": "default", "instance_label": "Default"}, False)
    assert adapter.calls == 1


def test_forced_refresh_still_fetches_a_fresh_provider(monkeypatch):
    service, adapter = _service(monkeypatch)
    key = service._cache_key("jellyfin", "default", adapter)
    service._cache[key] = {"ts": time.time(), "result": PlaybackListResult(True, "jellyfin", "default")}
    adapter.release.set()
    result = service.items(provider="jellyfin", force_refresh=True)
    assert result["errors"][0]["error_code"] == "provider_unavailable"
    assert adapter.calls == 1


def test_provider_cache_expires_after_five_minutes_and_retains_success_time(monkeypatch):
    service, adapter = _service(monkeypatch)
    now = [1000.0]
    monkeypatch.setattr(playback_service.time, "time", lambda: now[0])
    key = service._cache_key("jellyfin", "default", adapter)
    service._cache[key] = {"ts": now[0], "result": PlaybackListResult(True, "jellyfin", "default")}
    now[0] += 299
    assert service.items(provider="jellyfin")["checked_at"] == 1000
    assert adapter.calls == 0
    now[0] += 1
    adapter.release.set()
    result = service.items(provider="jellyfin")
    assert adapter.calls == 1
    assert result["errors"]
    assert result["checked_at"] == 1000
    service.items(provider="jellyfin")
    assert adapter.calls == 1


def test_activity_invalidates_only_affected_instance_and_keeps_snapshot(monkeypatch):
    service, adapter = _service(monkeypatch)
    key = service._cache_key("jellyfin", "default", adapter)
    other = service._cache_key("jellyfin", "other", adapter)
    record = PlaybackRecord("jellyfin", "Jellyfin", "default", "Default", "movie-1", "tmdb:1", "movie", title="Cached movie", progress_percent=40)
    for cache_key in (key, other):
        service._cache[cache_key] = {"ts": time.time(), "result": PlaybackListResult(True, "jellyfin", "default", items=[record])}
    service.mark_stale("JELLYFIN", "default")
    assert service._cache[key]["stale"] is True
    assert not service._cache[other].get("stale")
    try:
        first = service.items(provider="jellyfin", progressive=True)
        assert first["loading"][0]["cached"] is True
        assert first["items"][0]["title"] == "Cached movie"
        service.items(provider="jellyfin", progressive=True)
        assert adapter.started.wait(2)
        assert adapter.calls == 1
    finally:
        adapter.release.set()


def test_polls_follow_manual_refresh_even_when_previous_snapshot_is_fresh(monkeypatch):
    service, adapter = _service(monkeypatch)
    key = service._cache_key("jellyfin", "default", adapter)
    service._cache[key] = {"ts": time.time(), "result": PlaybackListResult(True, "jellyfin", "default")}
    try:
        first = service.items(provider="jellyfin", force_refresh=True, progressive=True)
        assert first["loading"][0]["cached"] is True
        assert adapter.started.wait(2)
        again = service.items(provider="jellyfin", progressive=True)
        assert again["loading"][0]["cached"] is True
        assert adapter.calls == 1
        adapter.release.set()
        done = service.items(provider="jellyfin")
        assert done["errors"][0]["error_code"] == "provider_unavailable"
        assert adapter.calls == 1
    finally:
        adapter.release.set()


@pytest.mark.parametrize("seeded", [False, True])
@pytest.mark.parametrize("ok", [False, True])
@pytest.mark.parametrize("invalidate", ["mark_stale", "invalidate"])
def test_changes_during_provider_read_are_not_overwritten(monkeypatch, seeded, ok, invalidate):
    service, adapter = _service(monkeypatch)
    key = service._cache_key("jellyfin", "default", adapter)
    spec = {"provider": "jellyfin", "instance_id": "default", "instance_label": "Default"}
    if seeded:
        service._cache[key] = {"ts": time.time() - 301, "result": PlaybackListResult(True, "jellyfin", "default")}

    def read(*args, **kwargs):
        adapter.calls += 1
        adapter.started.set()
        assert adapter.release.wait(5)
        return PlaybackListResult(ok, "jellyfin", "default")

    monkeypatch.setattr(adapter, "list_progress", read)
    try:
        future, _ = service._submit_list(playback_service.load_config(), spec, False)
        assert adapter.started.wait(2)
        getattr(service, invalidate)("jellyfin", "default")
        adapter.release.set()
        with pytest.raises(playback_service._PlaybackReadSuperseded):
            future.result(timeout=5)
        assert service._fresh_cached_result(spec) is None
        service.items(provider="jellyfin")
        assert adapter.calls == 2
        service.items(provider="jellyfin")
        assert adapter.calls == 2
    finally:
        adapter.release.set()
        service._pool.shutdown(wait=True)


@pytest.mark.parametrize("invalidate", ["mark_stale", "invalidate"])
def test_superseded_read_keeps_client_polling_without_reporting_provider_failure(monkeypatch, invalidate):
    from concurrent.futures import ThreadPoolExecutor

    service, adapter = _service(monkeypatch)
    record = PlaybackRecord("jellyfin", "Jellyfin", "default", "Default", "movie-1", "tmdb:1", "movie", title="Cached movie", progress_percent=40)
    key = service._cache_key("jellyfin", "default", adapter)
    service._cache[key] = {"ts": time.time() - 301, "result": PlaybackListResult(True, "jellyfin", "default", items=[record])}
    try:
        with ThreadPoolExecutor(max_workers=1) as caller:
            response = caller.submit(service.items, provider="jellyfin")
            assert adapter.started.wait(2)
            getattr(service, invalidate)("jellyfin", "default")
            adapter.release.set()
            result = response.result(timeout=5)
        assert result["loading"][0]["provider"] == "jellyfin"
        assert result["errors"] == []
        assert len(result["items"]) == (1 if invalidate == "mark_stale" else 0)
        assert adapter.calls == 1
        service.items(provider="jellyfin")
        assert adapter.calls == 2
    finally:
        adapter.release.set()
        service._pool.shutdown(wait=True)
