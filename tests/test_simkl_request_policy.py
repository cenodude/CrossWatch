# tests/test_simkl_request_policy.py
# CrossWatch - SIMKL request policy regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
import requests

from cw_platform import simkl_http as http
from cw_platform import connection_status
from providers.sync.simkl import _common


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    monkeypatch.setattr(connection_status, "_root", lambda: tmp_path / "connections")
    monkeypatch.setattr(_common, "STATE_DIR", tmp_path)
    monkeypatch.setattr(_common, "_QUOTA_BLOCKS", {})
    monkeypatch.setattr(_common, "_QUOTA_SEEN", {})
    monkeypatch.setattr(_common, "_SETTINGS_MEMO", {})
    monkeypatch.setattr(_common, "_ACT_MEMO", {})
    monkeypatch.setattr(http, "_OUTCOMES", {})
    monkeypatch.setattr(http, "_USER_LOCKS", {})
    monkeypatch.setattr(http, "_USER_IDENTITIES", {})
    monkeypatch.setattr(http, "_NEXT_REQUEST", 0.0)
    monkeypatch.setattr(http, "_NEXT_POST", 0.0)
    clock = [100.0]
    monkeypatch.setattr(http, "time", SimpleNamespace(
        monotonic=lambda: clock[0], time=time.time,
        sleep=lambda delay: clock.__setitem__(0, clock[0] + delay),
    ))
    return clock


def response(status=200, body=None):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(body or {}).encode()
    return result


def test_global_pacing_combines_users_methods_and_scrobbles(isolated):
    starts = []

    def send(url, **kwargs):
        starts.append(isolated[0])
        return response()

    for method, token in [("GET", "a"), ("GET", "b"), ("POST", "a"), ("GET", "b"), ("POST", "b"), ("POST", "a")]:
        http.paced_request(send, method, "https://api.simkl.com/scrobble/start", headers={"Authorization": f"Bearer {token}"})
    assert starts == [100.0, 100.5, 101.0, 101.5, 102.0, 103.0]


@pytest.mark.parametrize("method,gap", [("GET", 0.5), ("POST", 1.0)])
def test_slow_connection_cannot_compress_requests_into_a_burst(isolated, method, gap):
    isolated[0] = 100.2
    arrivals = []

    def send(url, **kwargs):
        if not arrivals:
            isolated[0] += 0.85
        arrivals.append(isolated[0])
        return response()

    for _ in range(3):
        http.paced_request(send, method, "https://api.simkl.com/sync/activities", headers={"Authorization": "Bearer token"})
    assert arrivals == pytest.approx([101.05, 101.05 + gap, 101.05 + 2 * gap])


@pytest.mark.parametrize("method,gap", [("GET", 0.5), ("POST", 1.0)])
def test_failed_requests_preserve_spacing(isolated, method, gap):
    starts = []

    def send(url, **kwargs):
        starts.append(isolated[0])
        if len(starts) == 1:
            isolated[0] += 0.85
            raise requests.Timeout("connection timed out")
        return response()

    with pytest.raises(requests.Timeout):
        http.paced_request(send, method, "https://api.simkl.com/sync/activities")
    http.paced_request(send, method, "https://api.simkl.com/sync/activities")
    assert starts == pytest.approx([100.0, 100.85 + gap])


@pytest.mark.parametrize("other_token,account_id", [("same-user", 123), ("another-token", 123), ("another-user", 456)])
def test_uncached_calls_are_sequential_across_profiles_and_users(other_token, account_id):
    for token, identity in (("same-user", 123), (other_token, account_id)):
        _common.account_settings_store(http.token_key(token), {"account": {"id": identity}})
    entered = threading.Event()
    release = threading.Event()
    second_started = threading.Event()

    def first():
        with http.request_gate("GET", "same-user"):
            entered.set()
            assert release.wait(3)

    def second():
        with http.request_gate("POST", other_token):
            second_started.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        one = pool.submit(first)
        assert entered.wait(3)
        two = pool.submit(second)
        try:
            assert not second_started.wait(0.05)
        finally:
            release.set()
        one.result(timeout=3)
        two.result(timeout=3)
    assert second_started.is_set()


def test_daily_quota_stops_other_request_paths_until_reset(monkeypatch):
    calls = []

    def send(url, **kwargs):
        calls.append(url)
        result = response(429, {"error": "user_limit_exceeded"})
        result.headers["Retry-After"] = "60"
        return result

    for method in ("GET", "POST"):
        with pytest.raises(_common.SIMKLQuotaError):
            http.paced_request(send, method, "https://api.simkl.com/sync/activities", headers={"Authorization": "Bearer a"})
    assert len(calls) == 1
    monkeypatch.setitem(_common._QUOTA_BLOCKS, http.token_key("a"), time.time() - 1)
    with pytest.raises(_common.SIMKLQuotaError):
        http.paced_request(send, "GET", "https://api.simkl.com/sync/activities", headers={"Authorization": "Bearer a"})
    assert len(calls) == 2


def test_status_and_user_info_never_poll_after_startup_verification(monkeypatch):
    from api import probesAPI as probes

    monkeypatch.setattr(probes, "_SIMKL_VERIFY_AFTER", 0.0)
    monkeypatch.setattr(probes, "_SIMKL_SETTINGS_CACHE", {})
    monkeypatch.setattr(probes, "_simkl_settings_post", lambda *a, **kw: pytest.fail("status polled SIMKL"))
    cfg = {"simkl": {"client_id": "client", "access_token": "token", "auth_version": 2, "token_expires_at": int(time.time()) + 3600}}
    connection_status.update("simkl", cfg, checked_at=time.time())
    _common.account_settings_store(http.token_key("token"), {"account": {"type": "vip"}})
    path = _common._account_cache_path(http.token_key("token"))
    data = json.loads(path.read_text())
    data["ts"] = 1
    path.write_text(json.dumps(data))
    for _ in range(3):
        assert probes._probe_simkl_detail(cfg) == (True, "")
        assert probes.simkl_user_info(cfg)["vip"] is True
    http.record_outcome("token", 401)
    http._OUTCOMES.clear()
    assert probes._probe_simkl_detail(cfg)[0] is False
    http.record_outcome("token", 200)
    assert probes._probe_simkl_detail(cfg)[0] is True
    cfg["simkl"]["token_expires_at"] = 1
    assert probes._probe_simkl_detail(cfg)[0] is False


def test_explicit_refresh_only_fetches_settings_once(monkeypatch):
    from api import probesAPI as probes

    monkeypatch.setattr(probes, "_SIMKL_VERIFY_AFTER", 0.0)
    monkeypatch.setattr(probes, "_SIMKL_SETTINGS_CACHE", {})
    calls = []

    def settings(*args, **kwargs):
        calls.append(args)
        return 200, b'{"account":{"type":"pro"}}'

    monkeypatch.setattr(probes, "_simkl_settings_post", settings)
    cfg = {"simkl": {"client_id": "client", "access_token": "token"}}
    assert probes._probe_simkl_detail(cfg, max_age_sec=0)[0] is True
    assert probes.simkl_user_info(cfg, max_age_sec=0)["account_type"] == "pro"
    assert len(calls) == 1


def test_reconnect_verifies_once_then_returns_to_local_status(monkeypatch):
    from api import probesAPI as probes

    monkeypatch.setattr(probes, "_SIMKL_VERIFY_AFTER", 0.0)
    monkeypatch.setattr(probes, "_SIMKL_SETTINGS_CACHE", {})
    calls = []

    def settings(*args, **kwargs):
        calls.append(args)
        return 200, b'{"account":{"type":"pro"}}'

    monkeypatch.setattr(probes, "_simkl_settings_post", settings)
    cfg = {"simkl": {"client_id": "client", "access_token": "token"}}
    probes.invalidate_provider_caches("simkl")
    assert probes._probe_simkl_detail(cfg)[0] is True
    assert probes._probe_simkl_detail(cfg)[0] is True
    assert len(calls) == 1


def test_activities_cache_is_scoped_to_the_user():
    calls = []

    class Session:
        def get(self, url, **kwargs):
            token = kwargs["headers"]["Authorization"]
            calls.append(token)
            return response(200, {"user": token})

    for token in ("Bearer a", "Bearer b", "Bearer a"):
        data, _ = _common.fetch_activities(Session(), {"Authorization": token})
        assert data == {"user": token}
    assert calls == ["Bearer a", "Bearer b"]


def test_sync_retry_and_scrobble_share_the_same_limiter(monkeypatch, isolated):
    from providers.sync import _mod_SIMKL as sync
    from providers.scrobble.simkl import sink

    starts = []

    def send(adapter, request, **kwargs):
        starts.append((request.method, isolated[0]))
        result = response(401 if len(starts) == 1 else 200)
        result.request = request
        return result

    def post(url, **kwargs):
        starts.append(("POST", isolated[0]))
        return response(201)

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    monkeypatch.setattr(sync, "_recover_access_token", lambda *args: "new-token")
    monkeypatch.setattr(sink.requests, "post", post)
    client = sync.SIMKLClient(sync.SIMKLConfig("client", "token"), {"access_token": "token"})
    client.session._rate_limiter = None
    assert client._request("POST", "https://api.simkl.com/sync/history").status_code == 200
    sink._post("/scrobble/start", {}, {"simkl": {"client_id": "client", "access_token": "new-token"}})
    assert starts == [("POST", 100.0), ("POST", 101.0), ("POST", 102.0)]


def test_scrobble_daily_limit_does_not_enter_retry_loop(monkeypatch):
    from providers.scrobble.simkl import sink

    calls = []

    def post(url, **kwargs):
        calls.append(url)
        return response(429, {"error": "user_limit_exceeded"})

    monkeypatch.setattr(sink.requests, "post", post)
    cfg = {"simkl": {"client_id": "client", "access_token": "token"}}
    result = sink.SimklSink()._send_http("/scrobble/start", {}, cfg)
    assert result["retryable"] is False
    assert result["status"] == 429
    assert len(calls) == 1


def test_sync_refreshes_the_persistent_account_cache(monkeypatch):
    from providers.sync import _mod_SIMKL as sync

    monkeypatch.setattr(sync, "supported_features", lambda: {"watchlist": True})
    calls = []

    def send(adapter, request, **kwargs):
        calls.append(request.url.split("?", 1)[0])
        result = response(200, {"account": {"type": "pro"}} if "/users/settings" in request.url else {})
        result.request = request
        return result

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    adapter = sync.SIMKLModule({"simkl": {"client_id": "client", "access_token": "token"}})
    adapter.client.session._rate_limiter = None
    adapter.health()
    assert calls == ["https://api.simkl.com/sync/activities", "https://api.simkl.com/users/settings"]
    assert _common.account_settings_cached(http.token_key("token"), float("inf"))["account"]["type"] == "pro"


def test_unchanged_sync_uses_one_activity_call_with_warm_caches(monkeypatch):
    from providers.sync import _mod_SIMKL as sync
    from providers.sync.simkl import _progress, _watchlist

    stamp = "2026-01-01T00:00:00Z"
    calls = []
    monkeypatch.setattr(sync, "supported_features", lambda: {"watchlist": True, "progress": True})
    _common.account_settings_store(http.token_key("token"), {"account": {"type": "pro"}})

    def send(adapter, request, **kwargs):
        calls.append(request.url.split("?", 1)[0])
        result = response(200, {"settings": {"all": stamp}, "movies": {"playback": stamp}})
        result.request = request
        return result

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    monkeypatch.setattr(_watchlist, "_shadow_load", lambda: {
        "ts": "m:-/-|s:-/-|a:-/-", "items": {},
        "buckets_seen": {"movies": True, "shows": True, "anime": True},
    })
    monkeypatch.setattr(_watchlist, "_shadow_age_seconds", lambda: 3600)
    monkeypatch.setattr(_watchlist, "normalize_flat_watermarks", lambda: None)
    monkeypatch.setattr(_progress, "_shadow_load", lambda: ({}, time.time() - 3600))
    monkeypatch.setattr(_progress, "get_watermark", lambda _: stamp)
    adapter = sync.SIMKLModule({"simkl": {"client_id": "client", "access_token": "token"}})
    adapter.client.session._rate_limiter = None
    adapter.health()
    assert _watchlist._build_index_live(adapter) == {}
    assert _progress.build_index(adapter) == {}
    assert adapter.client.activities()["movies"]["playback"] == stamp
    assert calls == ["https://api.simkl.com/sync/activities"]


@pytest.mark.parametrize("stamp", [None, "2026-01-01T00:00:00Z"])
def test_empty_ratings_snapshot_is_cached_until_activities_change(monkeypatch, tmp_path, stamp):
    from providers.sync.simkl import _ratings

    monkeypatch.setenv("CW_PAIR_KEY", "ratings-policy")
    monkeypatch.delenv("CW_CAPTURE_MODE", raising=False)
    activities = {kind: {"rated_at": stamp} for kind in ("movies", "tv_shows", "anime")}
    calls = []
    rows = []

    class Session:
        def get(self, url, **kwargs):
            calls.append(url)
            kind = url.split("/")[-2]
            return response(200, {kind: list(rows) if kind == "movies" else []})

    monkeypatch.setattr(_ratings, "_shadow_path", lambda: str(tmp_path / "ratings.json"))
    monkeypatch.setattr(_ratings, "_headers", lambda *a, **kw: {})
    monkeypatch.setattr(_ratings, "normalize_flat_watermarks", lambda: None)
    monkeypatch.setattr(_ratings, "get_watermark", lambda _: stamp)
    monkeypatch.setattr(_ratings, "update_watermark_if_new", lambda *a: None)
    monkeypatch.setattr(_ratings, "fetch_activities", lambda *a, **kw: (activities, {}))
    adapter = SimpleNamespace(client=SimpleNamespace(session=Session()), cfg=SimpleNamespace(timeout=5))

    assert _ratings.build_index(adapter) == {}
    assert len(calls) == 3
    assert _ratings.build_index(adapter) == {}
    assert len(calls) == 3

    activities["movies"]["rated_at"] = "2026-02-01T00:00:00Z"
    rows.append({"movie": {"title": "Example", "ids": {"simkl": 123}}, "user_rating": 8})
    assert len(_ratings.build_index(adapter)) == 1
    assert len(calls) == 6

    activities["movies"]["rated_at"] = "2026-03-01T00:00:00Z"
    rows.clear()
    assert _ratings.build_index(adapter) == {}
    assert _ratings.build_index(adapter) == {}
    assert len(calls) == 9


def test_changed_settings_refresh_even_with_warm_memory_cache():
    key = http.token_key("token")
    _common.account_settings_store(key, {"account": {"type": "free"}})
    path = _common._account_cache_path(key)
    data = json.loads(path.read_text())
    data["ts"] = 1
    path.write_text(json.dumps(data))
    _common._SETTINGS_MEMO[key] = (time.time(), {"account": {"type": "free"}})
    calls = []

    class Session:
        def post(self, url, **kwargs):
            calls.append(url)
            return response(200, {"account": {"type": "pro"}})

    result = _common.refresh_user_settings_from_activities(
        Session(), {"Authorization": "Bearer token"}, {"settings": {"all": "2026-01-01T00:00:00Z"}},
    )
    assert result["account"]["type"] == "pro"
    assert len(calls) == 1


def test_concurrent_settings_consumers_share_one_request():
    entered = threading.Event()
    release = threading.Event()
    calls = []

    def post(*args, **kwargs):
        calls.append(True)
        entered.set()
        assert release.wait(3)
        return response(200, {"account": {"type": "pro"}})

    session = SimpleNamespace(post=post)
    headers = {"Authorization": "Bearer token"}
    with ThreadPoolExecutor(max_workers=3) as pool:
        first = pool.submit(_common.fetch_user_settings, session, headers)
        assert entered.wait(3)
        second = pool.submit(_common.fetch_user_settings, session, headers)
        third = pool.submit(_common.refresh_user_settings_from_activities, session, headers, {})
        release.set()
        assert first.result(timeout=3) == second.result(timeout=3) == third.result(timeout=3)
    assert len(calls) == 1


def test_quota_headers_feed_the_status_fields_and_zero_on_daily_limit():
    def send(url, **kwargs):
        result = response(200)
        result.headers.update({"X-RateLimit-Limit": "1000", "X-RateLimit-Remaining": "321"})
        return result

    http.paced_request(send, "GET", "https://api.simkl.com/sync/activities", headers={"Authorization": "Bearer token"})
    fields = _common.latest_quota(_common.quota_account_key({"access_token": "token"}))
    assert fields["daily_remaining"] == 321
    assert fields["daily_limit"] == 1000
    assert fields["daily_resets_label"]
    _common.block_quota(http.token_key("token"), 60)
    assert _common.latest_quota(http.token_key("token"))["daily_remaining"] == 0


def test_end_of_sync_refresh_updates_warm_settings_and_quota(monkeypatch):
    from api import probesAPI as probes
    from providers.sync import _mod_SIMKL as simkl

    cfg = {"simkl": {"client_id": "client", "access_token": "token"}}
    key = http.token_key("token")
    _common.account_settings_store(key, {"account": {"type": "free"}})
    probes.STATUS_CACHE.update(ts=time.time(), data={"old": True})
    session = requests.Session()
    session.headers.update({"Authorization": "Bearer token", "simkl-api-key": "client"})
    calls = []

    def send(url, **kwargs):
        calls.append(url)
        result = response(200, {"account": {"type": "pro"}})
        result.headers.update({"X-RateLimit-Limit": "1000", "X-RateLimit-Remaining": "123"})
        return result

    monkeypatch.setattr(session, "post", lambda url, **kw: http.paced_request(send, "POST", url, **kw))
    adapter = SimpleNamespace(client=SimpleNamespace(session=session), cfg=SimpleNamespace(timeout=5))
    monkeypatch.setattr(simkl.OPS, "_adapter", lambda cfg: adapter)
    for _ in range(2):
        assert simkl.OPS.refresh_account(cfg) is True
        assert probes.simkl_user_info(cfg)["account_type"] == "pro"
        assert _common.latest_quota(key)["daily_remaining"] == 123
        assert probes.STATUS_CACHE["data"] is None
    assert calls == [_common.URL_USER_SETTINGS] * 2
    assert _common.fetch_user_settings(session, session.headers)["account"]["type"] == "pro"
    assert len(calls) == 2
    _common.block_quota(key, 60)
    assert simkl.OPS.refresh_account(cfg) is False
    assert len(calls) == 2


def test_empty_progress_snapshot_is_reused_until_playback_changes(monkeypatch):
    from providers.sync.simkl import _progress

    monkeypatch.setattr(_progress, "_activities_latest", lambda _: "2026-09-20T00:00:00Z")
    monkeypatch.setattr(_progress, "_shadow_load", lambda: ({}, time.time()))
    monkeypatch.setattr(_progress, "get_watermark", lambda _: "2026-09-20T00:00:00Z")
    monkeypatch.setattr(_progress, "_playback_rows", lambda _: pytest.fail("unchanged playback fetched"))
    assert _progress.build_index(object()) == {}


def test_default_scheduler_jitter_is_up_to_300_seconds(monkeypatch):
    from datetime import datetime, timedelta
    from services import scheduling

    monkeypatch.setattr(scheduling.random, "randint", lambda low, high: high)
    start = datetime(2026, 9, 20, 12)
    assert scheduling._apply_jitter(start, {}) == start + timedelta(seconds=300)
    assert scheduling._apply_jitter(start, {"jitter_seconds": 0}) == start
