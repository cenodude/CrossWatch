# /tests/test_plex_cloud_rate_limit.py
# Plex cloud lookups wait out rate limits, keep ids on disk and never store a rate-limited miss
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from providers.sync.plex import _common as c


class Clock:
    def __init__(self) -> None:
        self.now = 1_000_000.0
        self.slept = 0.0

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds
        self.slept += seconds


class Resp:
    def __init__(self, status: int, payload: dict[str, Any] | None = None, retry_after: str | None = None) -> None:
        self.status_code = status
        self.ok = 200 <= status < 300
        self.headers = {"content-type": "application/json", **({"Retry-After": retry_after} if retry_after else {})}
        self._payload = payload or {}
        self.text = "{}"

    def json(self) -> dict[str, Any]:
        return self._payload


GUIDS = {"MediaContainer": {"Metadata": [{"Guid": [{"id": "tmdb://603"}]}]}}


@pytest.fixture
def cloud(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(c, "time", clock)
    monkeypatch.setattr(c, "_CLOUD_BLOCKED_UNTIL", 0.0)
    monkeypatch.setattr(c, "_CLOUD_LAST_WAIT", 0.0)
    with c._HYDRATE_LOCK:
        c._GUID_CACHE.clear()
        c._HYDRATE_404.clear()
    c.configure_plex_context(baseurl=None, token="ACCOUNT", account_token="ACCOUNT")
    state = SimpleNamespace(clock=clock, replies=[], calls=[])

    def get(url, **kwargs):
        state.calls.append(url)
        return state.replies.pop(0) if state.replies else Resp(200, GUIDS)

    monkeypatch.setattr(c.requests, "get", get)
    return state


def test_hydrate_waits_for_retry_after_and_returns_ids(cloud):
    cloud.replies = [Resp(429, retry_after="30")]
    assert c.hydrate_external_ids("ACCOUNT", "abc") == {"tmdb": "603"}
    assert cloud.clock.slept == 30 and len(cloud.calls) == 2


def test_rate_limit_beyond_budget_raises_and_is_not_cached(cloud):
    cloud.replies = [Resp(429, retry_after="300")]
    with c.cloud_wait_budget(60):
        with pytest.raises(c.PlexRateLimited):
            c.hydrate_external_ids("ACCOUNT", "abc")
    cloud.clock.now += 301
    assert c.hydrate_external_ids("ACCOUNT", "abc") == {"tmdb": "603"}


def test_all_threads_respect_an_active_block(cloud):
    cloud.replies = [Resp(429, retry_after="20")]
    c.hydrate_external_ids("ACCOUNT", "one")
    c._CLOUD_BLOCKED_UNTIL = cloud.clock.now + 10
    c.hydrate_external_ids("ACCOUNT", "two")
    assert cloud.clock.slept == 30 and len(cloud.calls) == 3


def test_episode_show_lookup_raises_instead_of_caching_a_miss(cloud):
    cloud.replies = [Resp(429, retry_after="300")]
    with c.cloud_wait_budget(0), pytest.raises(c.PlexRateLimited):
        c._hydrate_show_ids_from_episode_rk("ACCOUNT", "ep1")
    assert not any(key.endswith(":ep1") for key in c._EP_SHOW_IDS_CACHE)


def test_title_search_raises_instead_of_reporting_no_match(cloud):
    cloud.replies = [Resp(429, retry_after="300")]
    with c.cloud_wait_budget(0), pytest.raises(c.PlexRateLimited):
        c._discover_search_title("ACCOUNT", "The Matrix", "movie", 1999)


def test_wait_reports_progress_and_can_be_cancelled(cloud):
    cloud.replies = [Resp(429, retry_after="5")]
    waits: list[float] = []

    def cancel():
        raise InterruptedError("stop")

    with c.cloud_wait_budget(60, on_wait=waits.append, cancel=cancel), pytest.raises(InterruptedError):
        c.hydrate_external_ids("ACCOUNT", "abc")
    assert waits == [5]


def test_recovery_explains_a_rate_limit():
    from services.plex_history_recovery import _scan_error

    job = SimpleNamespace(database="", tracearr="", tautulli="")
    assert "limiting the lookups" in _scan_error(job, c.PlexRateLimited("x"))


def test_repeated_rate_limits_wait_longer_and_reset_after_success(cloud):
    cloud.replies = [Resp(429), Resp(429), Resp(429)]
    c.hydrate_external_ids("ACCOUNT", "one")
    assert cloud.clock.slept == 60 + 90 + 135
    cloud.replies = [Resp(429)]
    c.hydrate_external_ids("ACCOUNT", "two")
    assert cloud.clock.slept == 60 + 90 + 135 + 60


def test_growing_wait_never_drops_below_retry_after_or_exceeds_the_cap(cloud):
    c._CLOUD_LAST_WAIT = 250.0
    c._CLOUD_BLOCKED_UNTIL = cloud.clock.now - 1
    cloud.replies = [Resp(429, retry_after="20")]
    with c.cloud_wait_budget(900):
        c.hydrate_external_ids("ACCOUNT", "one")
    assert cloud.clock.slept == 300
    cloud.replies = [Resp(429, retry_after="200"), Resp(429, retry_after="10")]
    with c.cloud_wait_budget(900):
        c.hydrate_external_ids("ACCOUNT", "two")
    assert cloud.clock.slept == 300 + 200 + 300


def test_a_long_retry_after_is_honoured_and_old_waits_are_forgotten(cloud):
    cloud.replies = [Resp(429, retry_after="600")]
    with c.cloud_wait_budget(900):
        c.hydrate_external_ids("ACCOUNT", "one")
    assert cloud.clock.slept == 600
    c._CLOUD_LAST_WAIT = 300.0
    c._CLOUD_BLOCKED_UNTIL = cloud.clock.now - 3600
    cloud.replies = [Resp(429)]
    c.hydrate_external_ids("ACCOUNT", "two")
    assert cloud.clock.slept == 600 + 60


def _restart():
    c._CLOUD_IDS = None
    with c._HYDRATE_LOCK:
        c._GUID_CACHE.clear()
        c._HYDRATE_404.clear()


def test_cloud_ids_are_kept_on_disk_between_runs(cloud):
    assert c.hydrate_external_ids("ACCOUNT", "abc") == {"tmdb": "603"}
    c.cloud_ids_flush()
    _restart()
    cloud.calls.clear()
    assert c.hydrate_external_ids("ACCOUNT", "abc") == {"tmdb": "603"}
    assert cloud.calls == []


def test_cloud_misses_are_kept_for_a_day(cloud):
    cloud.replies = [Resp(404)]
    assert c.hydrate_external_ids("ACCOUNT", "gone") == {}
    c.cloud_ids_flush()
    _restart()
    cloud.calls.clear()
    assert c.hydrate_external_ids("ACCOUNT", "gone") == {}
    assert cloud.calls == []
    _restart()
    cloud.clock.now += c._CLOUD_IDS_MISS_TTL + 1
    assert c.hydrate_external_ids("ACCOUNT", "gone") == {"tmdb": "603"}


def test_rate_limited_lookups_never_reach_the_disk(cloud):
    cloud.replies = [Resp(429, retry_after="300")]
    with c.cloud_wait_budget(0), pytest.raises(c.PlexRateLimited):
        c.hydrate_external_ids("ACCOUNT", "abc")
    c.cloud_ids_flush()
    _restart()
    assert c.cloud_ids_get("abc") is None


def test_capture_mode_neither_reads_nor_writes_the_disk_cache(cloud, monkeypatch):
    monkeypatch.setattr(c, "_is_capture_mode", lambda: True)
    c.hydrate_external_ids("ACCOUNT", "abc")
    c.cloud_ids_flush()
    assert not (c.STATE_DIR / c._CLOUD_IDS_NAME).exists()
