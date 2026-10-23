# tests/test_playback_progressive_list.py
# CrossWatch - Playback progress list returns fast providers first
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import threading

from services.playback_progress.adapters.base import PlaybackProgressAdapter
from services.playback_progress.models import PlaybackCapabilities, PlaybackListResult
from services.playback_progress.service import PlaybackProgressService
import services.playback_progress.service as playback_service


class _SlowAdapter(PlaybackProgressAdapter):
    provider = "jellyfin"
    provider_label = "Jellyfin"

    def __init__(self) -> None:
        self.release = threading.Event()
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
