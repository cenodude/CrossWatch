# tests/test_crosswatch_capture_scope.py
# CrossWatch - Capture mode for the CrossWatch tracker stays inside the operation that asked for it
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import os
import threading
from datetime import datetime, timezone

import pytest

from cw_platform.modules_registry import load_sync_ops
from cw_platform.provider_instances import build_provider_config_view
from cw_platform.tracker_store import TrackerStore
from providers.sync.crosswatch import _common as cw_common
from services import snapshots

RATING = {"type": "movie", "title": "Heat", "year": 1995, "ids": {"tmdb": 949}, "rating": 8, "rated_at": "2026-11-01T20:00:00Z"}


@pytest.fixture
def tracker(tmp_path, monkeypatch):
    for name in ("CW_CAPTURE_MODE", "CW_CAPTURE_PROVIDER", "CW_CAPTURE_INSTANCE", "CW_CAPTURE_FEATURE", "CW_CAPTURE_ID"):
        monkeypatch.delenv(name, raising=False)
    cfg = {"crosswatch": {"root_dir": str(tmp_path / "tracker"), "connected": True}}
    return tmp_path / "tracker", build_provider_config_view(cfg, "CROSSWATCH", "default")


def add_rating(view):
    load_sync_ops("CROSSWATCH").add(view, [dict(RATING)], feature="ratings")


def test_a_crosswatch_read_leaves_the_process_environment_alone(tracker):
    seen = {}

    class Ops:
        def build_index(self, cfg, *, feature):
            seen.update(env=os.environ.get("CW_CAPTURE_MODE"), capture=cw_common._capture_mode())
            return {}

    with snapshots._capture_mode_env(pid="CROSSWATCH", instance="default", feat="ratings"):
        Ops().build_index({}, feature="ratings")
    snapshots._build_index_capture_mode(ops=Ops(), cfg_view={}, pid="CROSSWATCH", instance="default", feat="ratings",
                                        ts=datetime(2026, 11, 21, tzinfo=timezone.utc))
    assert seen == {"env": None, "capture": True}
    assert os.environ.get("CW_CAPTURE_MODE") is None


def test_tracker_writes_from_other_threads_persist_during_a_crosswatch_read(tracker):
    root, view = tracker
    with snapshots._capture_mode_env(pid="CROSSWATCH", instance="default", feat="ratings"):
        assert cw_common._capture_mode() is True
        writer = threading.Thread(target=add_rating, args=(view,))
        writer.start()
        writer.join(10)
    assert list(TrackerStore(root).read_current("ratings")["items"]) == ["tmdb:949"]


def test_another_providers_capture_never_blocks_tracker_writes(tracker, monkeypatch):
    root, view = tracker
    monkeypatch.setenv("CW_CAPTURE_MODE", "1")
    monkeypatch.setenv("CW_CAPTURE_PROVIDER", "PLEX")
    assert cw_common._capture_mode() is False
    add_rating(view)
    assert list(TrackerStore(root).read_current("ratings")["items"]) == ["tmdb:949"]


def test_the_tracker_write_scope_never_touches_the_process_environment(tracker, monkeypatch):
    monkeypatch.setenv("CW_CAPTURE_MODE", "1")
    with snapshots._capture_mode_env(pid="CROSSWATCH", instance="default", feat="ratings"):
        with snapshots._crosswatch_write_env():
            assert cw_common._capture_mode() is False
            assert os.environ.get("CW_CAPTURE_MODE") == "1"
        assert cw_common._capture_mode() is True


@pytest.mark.parametrize("provider,expected", [("", True), ("CROSSWATCH", True), ("TRAKT", False)])
def test_a_process_wide_capture_flag_still_applies_when_it_names_no_other_provider(tracker, monkeypatch, provider, expected):
    monkeypatch.setenv("CW_CAPTURE_MODE", "1")
    monkeypatch.setenv("CW_CAPTURE_PROVIDER", provider)
    assert cw_common._capture_mode() is expected
