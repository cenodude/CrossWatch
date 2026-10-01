# CrossWatch test scripts
from __future__ import annotations

from types import SimpleNamespace

import pytest

from cw_platform import run_control, trakt_http


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.slept = 0.0

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept += seconds
        self.now += seconds


@pytest.fixture
def clock(monkeypatch):
    fake = Clock()
    monkeypatch.setenv(trakt_http.DISABLE_ENV, "1")
    monkeypatch.delenv(trakt_http.READ_LIMIT_ENV, raising=False)
    monkeypatch.setattr(trakt_http, "time", fake)
    trakt_http.reset()
    yield fake
    trakt_http.reset()


def _gate(method: str, token: str = "token", **kwargs) -> None:
    with trakt_http.request_gate(method, token, **kwargs):
        pass


def test_writes_are_spaced_one_second_per_account(clock):
    _gate("POST")
    assert clock.slept == 0
    _gate("DELETE")
    _gate("POST")
    assert clock.slept == pytest.approx(2.0)


def test_accounts_are_paced_independently(clock):
    _gate("POST", "first")
    _gate("POST", "second")
    _gate("POST", "Bearer first")
    assert clock.slept == pytest.approx(1.0)


def test_reads_do_not_wait_below_the_window_limit(clock, monkeypatch):
    monkeypatch.setenv(trakt_http.READ_LIMIT_ENV, "5")
    for _ in range(5):
        _gate("GET")
    assert clock.slept == 0


def test_foreground_read_over_the_limit_waits_only_briefly(clock, monkeypatch):
    monkeypatch.setenv(trakt_http.READ_LIMIT_ENV, "3")
    for _ in range(3):
        _gate("GET")
    _gate("GET")
    assert clock.slept == pytest.approx(trakt_http.MAX_WAIT_FOREGROUND_S)


def test_background_read_keeps_a_reserve_and_waits_for_the_window(clock, monkeypatch):
    monkeypatch.setenv(trakt_http.READ_LIMIT_ENV, "4")
    monkeypatch.setattr(trakt_http, "READ_RESERVE", 2)
    _gate("GET", background=True)
    clock.now += 10
    _gate("GET", background=True)
    assert clock.slept == 0
    _gate("GET", background=True)
    assert clock.slept == pytest.approx(trakt_http.READ_WINDOW_S - 10)
    _gate("GET")
    assert clock.slept == pytest.approx(trakt_http.READ_WINDOW_S - 10)


def test_reads_free_up_after_the_window(clock, monkeypatch):
    monkeypatch.setenv(trakt_http.READ_LIMIT_ENV, "2")
    _gate("GET")
    _gate("GET")
    clock.now += trakt_http.READ_WINDOW_S + 1
    _gate("GET")
    assert clock.slept == 0


def test_rate_limited_response_blocks_only_that_bucket(clock):
    trakt_http.record_response("POST", "token", 429, {"Retry-After": "7"})
    _gate("GET")
    assert clock.slept == 0
    _gate("POST")
    assert clock.slept == pytest.approx(7.0)


def test_rate_limit_block_uses_the_ratelimit_header_when_retry_after_is_missing(clock):
    trakt_http.record_response("GET", "token", 429, {"x-ratelimit": '{"until":"1970-01-01T00:16:45Z"}'})
    _gate("GET")
    assert clock.slept == pytest.approx(5.0)


def test_successful_responses_do_not_block(clock):
    trakt_http.record_response("GET", "token", 200, {"Retry-After": "30"})
    _gate("GET")
    assert clock.slept == 0


def test_unauthenticated_calls_share_the_app_bucket(clock):
    _gate("POST", "", client_id="app")
    _gate("POST", "", client_id="app")
    assert clock.slept == pytest.approx(1.0)


def test_pacing_can_be_disabled(clock, monkeypatch):
    monkeypatch.setenv(trakt_http.DISABLE_ENV, "0")
    _gate("POST")
    _gate("POST")
    trakt_http.record_response("POST", "token", 429, {"Retry-After": "30"})
    _gate("POST")
    assert clock.slept == 0


def test_paced_request_forwards_the_call_and_records_the_response(clock):
    calls = []

    def sender(url, **kwargs):
        calls.append((url, kwargs))
        return SimpleNamespace(status_code=429, headers={"Retry-After": "4"})

    headers = {"Authorization": "Bearer token", "trakt-api-key": "app"}
    response = trakt_http.paced_request(sender, "POST", "https://api.trakt.tv/scrobble/start", headers=headers, json={"a": 1})
    assert response.status_code == 429
    assert calls == [("https://api.trakt.tv/scrobble/start", {"headers": headers, "json": {"a": 1}})]
    _gate("POST")
    assert clock.slept == pytest.approx(4.0)


def test_paced_request_tolerates_responses_without_headers(clock):
    assert trakt_http.paced_request(lambda url, **kwargs: None, "GET", "https://api.trakt.tv/x") is None


def test_background_wait_stops_when_the_sync_is_cancelled(clock, monkeypatch):
    monkeypatch.setattr(trakt_http, "raise_if_cancelled", lambda: (_ for _ in ()).throw(run_control.SyncCancelled("sync cancelled")))
    _gate("POST")
    _gate("POST")
    with pytest.raises(run_control.SyncCancelled):
        _gate("POST", background=True)


def test_session_mount_covers_the_trakt_api(clock):
    import requests

    session = requests.Session()
    trakt_http.pace_session(session)
    assert isinstance(session.get_adapter("https://api.trakt.tv/sync/history"), trakt_http.TraktAdapter)
    assert not isinstance(session.get_adapter("https://api.simkl.com/sync"), trakt_http.TraktAdapter)
