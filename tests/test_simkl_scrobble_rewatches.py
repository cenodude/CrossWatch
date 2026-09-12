from __future__ import annotations

import copy
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest
import requests

from cw_platform.local_db.db import close_conn
from providers.scrobble.scrobble import ScrobbleEvent
from providers.scrobble.simkl import rewatches, sink


def config(enabled=True, token="test-account-a"):
    return {
        "simkl": {"api_key": "test-client", "access_token": token},
        "scrobble": {
            "trakt": {"watched_at": 90},
            "watch": {"route_provider": "plex", "route_provider_instance": "P01",
                      "route_options": {"watch": {"simkl_rewatches": enabled}}},
        },
    }


def event(action="stop", progress=95, session="session-a"):
    return ScrobbleEvent(action=action, media_type="movie", ids={"simkl": "123"}, title="Test movie",
                         year=2020, season=None, number=None, progress=progress, account="test-user",
                         server_uuid="test-server", session_key=session, raw={})


class Response:
    def __init__(self, payload, status=201):
        self.payload = payload
        self.status_code = status
        self.headers = {}
        self.text = ""

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError()


@pytest.fixture
def api(monkeypatch, tmp_path):
    monkeypatch.setenv("CROSSWATCH_DB", str(tmp_path / "test.sqlite3"))
    rewatches._PLANS.clear()
    state = {"plan": "pro", "status": "completed", "http": 201, "calls": [], "records": [], "removed": [], "logs": [], "original_log": sink._log}

    def post(url, **kwargs):
        state["calls"].append((url, kwargs))
        if url.endswith("/users/settings"):
            if state.get("settings_failure"):
                raise requests.Timeout()
            account = kwargs["headers"]["Authorization"]
            return Response({"account": {"type": state["plan"], "id": account}}, 200)
        if state.get("failure"):
            raise requests.Timeout()
        action = "scrobble" if url.endswith("/stop") else url.rsplit("/", 1)[-1]
        payload = {"action": state.get("action", action), "rewatch_id": 12345}
        if "allow_rewatch" in kwargs["params"]:
            payload["rewatch_status"] = state["status"]
        return Response(payload, state["http"])

    monkeypatch.setattr(sink.requests, "post", post)
    monkeypatch.setattr(sink, "record_scrobble_event", lambda *a, **k: state["records"].append(k))
    monkeypatch.setattr(sink, "_auto_remove_across", lambda *a, **k: state["removed"].append(k))
    monkeypatch.setattr(sink, "record_watch", lambda *a, **k: None)
    monkeypatch.setattr(sink, "_log", lambda message, level="INFO": state["logs"].append((message, level)))
    yield state
    close_conn()
    rewatches._PLANS.clear()


def scrobbles(api):
    return [(url, kw) for url, kw in api["calls"] if "/scrobble/" in url]


def test_default_off_has_no_plan_request_or_rewatch_flag(api):
    sink.SimklSink().send(event(), config(False))
    assert len(api["calls"]) == 1
    assert "allow_rewatch" not in scrobbles(api)[0][1]["params"]
    assert len(api["records"]) == 1


@pytest.mark.parametrize("plan,flag", [("pro", True), ("vip", True), ("free", False)])
def test_account_plan_gate(api, plan, flag):
    api["plan"] = plan
    sink.SimklSink().send(event(), config())
    assert (scrobbles(api)[0][1]["params"].get("allow_rewatch") == "yes") is flag
    assert "allow_rewatch" not in scrobbles(api)[0][1]["json"]


def test_start_pause_stop_lifecycle_and_no_seek_posts(api):
    target = sink.SimklSink()
    target.send(event("start", 10), config())
    target.send(replace(event("start", 45), raw={"_cw_seek": True}), config())
    target.send(event("pause", 55), config())
    target.send(event("start", 60), config())
    target.send(event("stop", 95), config())
    calls = scrobbles(api)
    assert [url.rsplit("/", 1)[-1] for url, _ in calls] == ["start", "pause", "start", "stop"]
    assert [kw["params"].get("allow_rewatch") for _, kw in calls] == [None, None, None, "yes"]


@pytest.mark.parametrize("threshold,progress,flag", [(90, 89, False), (90, 90, True), (70, 79, False), (70, 80, True), (95, 94, False), (95, 95, True)])
def test_user_threshold_and_simkl_floor(api, threshold, progress, flag):
    cfg = config()
    cfg["scrobble"]["trakt"]["watched_at"] = threshold
    sink.SimklSink().send(event(progress=progress), cfg)
    assert (scrobbles(api)[0][1]["params"].get("allow_rewatch") == "yes") is flag
    assert len(api["records"]) == int(flag)


@pytest.mark.parametrize("status,saved", [("active", True), ("completed", True), ("closed", True), ("first_watch", True), ("too_soon", False), ("not_eligible", False), ("pro_required", False), (None, False), ("future_status", False)])
def test_status_controls_completion_even_with_rewatch_id(api, status, saved):
    api["status"] = status
    result = sink.SimklSink().send(event(), config())
    assert len(api["records"]) == int(saved)
    assert len(api["removed"]) == int(saved)
    if not saved:
        assert result["skipped"] is True
    sink.SimklSink().send(event(), config())
    assert len(scrobbles(api)) == 1


def test_pause_response_is_not_a_completed_watch(api):
    api["action"] = "pause"
    result = sink.SimklSink().send(event(), config())
    assert result["skipped"] is True
    assert not api["records"]


def test_downgrade_stops_flag_on_next_session(api):
    api["status"] = "pro_required"
    sink.SimklSink().send(event(), config())
    sink.SimklSink().send(event(session="session-b"), config())
    assert [kw["params"].get("allow_rewatch") for _, kw in scrobbles(api)] == ["yes", None]
    assert len([url for url, _ in api["calls"] if url.endswith("/users/settings")]) == 1


def test_duplicate_survives_sink_and_database_restart(api):
    sink.SimklSink().send(event(), config())
    close_conn()
    rewatches._PLANS.clear()
    result = sink.SimklSink().send(event(), config())
    assert result["reason"] == "completion_already_processed"
    assert len(scrobbles(api)) == 1


def test_uncertain_delivery_is_not_retried_or_reported_saved(api):
    api["failure"] = True
    result = sink.SimklSink().send(event(), config())
    assert result["retryable"] is False
    api["failure"] = False
    result = sink.SimklSink().send(event(), config())
    assert result["error"] == "rewatch_delivery_unconfirmed"
    assert len(scrobbles(api)) == 1
    assert not api["records"]


@pytest.mark.parametrize("status", [400, 401, 403, 404, 423, 429])
def test_definite_rejection_allows_later_event_without_retry_loop(api, status):
    api["http"] = status
    result = sink.SimklSink().send(event(), config())
    assert result["retryable"] is False
    assert len(scrobbles(api)) == 1
    api["http"] = 201
    sink.SimklSink().send(event(), config())
    assert len(api["records"]) == 1


@pytest.mark.parametrize("status", [409, 500])
def test_duplicate_and_uncertain_server_errors_do_not_repeat(api, status):
    api["http"] = status
    sink.SimklSink().send(event(), config())
    sink.SimklSink().send(event(), config())
    assert len(scrobbles(api)) == 1
    assert not api["records"]


def test_unknown_plan_waits_and_does_not_send_watch(api):
    api["settings_failure"] = True
    result = sink.SimklSink().send(event(), config())
    assert result["error"] == "simkl_plan_unavailable"
    assert not scrobbles(api)


def test_missing_session_never_opts_into_rewatch(api):
    sink.SimklSink().send(event(session=None), config())
    assert "allow_rewatch" not in scrobbles(api)[0][1]["params"]


def test_new_sessions_accounts_users_and_profiles_are_independent(api):
    target = sink.SimklSink()
    cfg = config()
    target.send(event(), cfg)
    target.send(event(session="session-b"), cfg)
    target.send(event(), config(token="test-account-b"))
    cfg = copy.deepcopy(cfg)
    cfg["scrobble"]["watch"]["route_provider_instance"] = "P02"
    target.send(event(), cfg)
    target.send(replace(event(), account="other-user"), cfg)
    assert len(scrobbles(api)) == 5


def test_episodes_are_independent_and_server_enforces_gap(api):
    first = replace(event(), media_type="episode", ids={"tvdb_show": "123"}, season=1, number=1)
    sink.SimklSink().send(first, config())
    sink.SimklSink().send(replace(first, number=2), config())
    api["status"] = "too_soon"
    result = sink.SimklSink().send(replace(first, session_key="later-session"), config())
    assert result["reason"] == "too_soon"
    assert len(api["records"]) == 2


def test_atomic_claim_across_workers_and_expiry(api, monkeypatch):
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: rewatches.claim("test-key"), range(8)))
    assert results.count("new") == 1
    assert results.count("uncertain") == 7
    now = rewatches.time.time()
    monkeypatch.setattr(rewatches.time, "time", lambda: now + 49 * 3600)
    assert rewatches.claim("test-key") == "new"


def test_plan_cache_is_account_scoped_and_refreshes(api, monkeypatch):
    assert rewatches.account(config(), sink._post)[0] == "pro"
    api["plan"] = "free"
    assert rewatches.account(config(token="test-account-b"), sink._post)[0] == "free"
    assert rewatches.account(config(), sink._post)[0] == "pro"
    now = rewatches.time.time()
    monkeypatch.setattr(rewatches.time, "time", lambda: now + 301)
    assert rewatches.account(config(), sink._post)[0] == "free"


def test_persistent_store_failure_prevents_opt_in(api, monkeypatch):
    monkeypatch.setattr(rewatches, "get_conn", lambda: None)
    result = sink.SimklSink().send(event(), config())
    assert result["error"] == "rewatch_dedupe_unavailable"
    assert not scrobbles(api)


def test_request_boundary_never_flags_start_or_pause(api):
    for path in ("/scrobble/start", "/scrobble/pause", "/users/settings"):
        sink._post(path, {"progress": 99}, config(), allow_rewatch=True)
    assert all("allow_rewatch" not in kw["params"] for _, kw in api["calls"])


def test_watcher_and_webhook_use_same_completion_guard(api):
    from providers.scrobble.routes import build_route_cfg
    from providers.webhooks.dispatch import _route_cfg

    cfg = config()
    cfg["scrobble"]["webhook"] = {"simkl_rewatches": True, "sinks": ["simkl"]}
    route = {"id": "R1", "provider": "plex", "provider_instance": "P01", "sink": "simkl",
             "sink_instance": "default", "enabled": True, "options": {"watch": {"simkl_rewatches": True}}}
    watcher = build_route_cfg(cfg, route)
    webhook = _route_cfg(cfg, "plex", "P01", "simkl", "default")
    assert rewatches.enabled(watcher)
    assert rewatches.enabled(webhook)
    sink.SimklSink().send(event(), watcher)
    result = sink.SimklSink().send(event(), webhook)
    assert result["skipped"] is True
    assert len(scrobbles(api)) == 1


def test_webhook_option_is_normalized_and_scoped(api):
    from api.scrobblerManagementAPI import _normalize_webhook_settings
    from providers.webhooks.dispatch import _route_cfg

    cfg = config()
    settings = _normalize_webhook_settings(cfg, "plex", {"sinks": ["simkl"], "simkl_rewatches": True})
    cfg["scrobble"]["webhook"] = {"profiles": {"plex": {"P01": settings}}}
    assert rewatches.enabled(_route_cfg(cfg, "plex", "P01", "simkl", "default"))
    assert not rewatches.enabled(_route_cfg(cfg, "plex", "P02", "simkl", "default"))


@pytest.mark.parametrize("plan,allowed", [("pro", True), ("vip", True), ("free", False)])
def test_enable_validation_uses_destination_plan(api, plan, allowed):
    from api.scrobblerManagementAPI import ValidationFailure, _require_simkl_rewatches

    api["plan"] = plan
    if allowed:
        _require_simkl_rewatches(config(), "default", "simkl_rewatches")
    else:
        with pytest.raises(ValidationFailure):
            _require_simkl_rewatches(config(), "default", "simkl_rewatches")


@pytest.mark.parametrize("provider", ["plex", "jellyfin", "emby"])
def test_real_webhook_dispatch_preserves_rewatch_outcome(api, monkeypatch, provider):
    from providers.webhooks import dispatch

    monkeypatch.setattr(dispatch, "_SINKS", {})
    cfg = config()
    cfg["scrobble"]["webhook"] = {"sinks": ["simkl"], "simkl_rewatches": True}
    api["status"] = "too_soon"
    result = dispatch.dispatch_scrobble(provider, "/scrobble/stop", media_type="movie", ids={"simkl": "123"},
                                       progress=95, session_key="webhook-session", account="test-user", cfg=cfg)
    assert result.status_code == 200
    assert result.json()["targets"][0]["reason"] == "too_soon"
    assert result.json()["activity_recorded"] is False
    assert scrobbles(api)[0][1]["params"]["allow_rewatch"] == "yes"


def test_current_progress_cannot_be_raised_over_rewatch_threshold(api):
    target = sink.SimklSink()
    target.send(event("start", 92), config())
    target.send(event("stop", 89), config())
    assert scrobbles(api)[-1][0].endswith("/pause")
    assert not api["records"]


def test_watcher_opt_in_requires_boolean_true(api):
    from providers.scrobble.routes import normalize_route_options

    options = normalize_route_options({"watch": {"simkl_rewatches": "false"}})
    assert options["watch"]["simkl_rewatches"] is False


def diagnostics(api, kind):
    prefix = f"rewatch.{kind} "
    return [json.loads(message[len(prefix):]) for message, level in api["logs"] if level == "DEBUG" and message.startswith(prefix)]


def test_diagnostics_correlate_route_profiles_session_and_results(api):
    cfg = config()
    cfg["scrobble"]["watch"].update(route_id="R12", route_effective_profile_id="profile-a")
    sink.SimklSink(instance_id="S02").send(event(), cfg)
    result = diagnostics(api, "result")[0]
    assert result["route"] == "R12"
    assert result["source"] == "plex"
    assert result["source_instance"] == "P01"
    assert result["destination_instance"] == "S02"
    assert result["profile"] == "profile-a"
    assert result["user"] == "te***"
    assert len(result["session_ref"]) == 12
    assert result["session_ref"] != "session-a"
    assert result["saved"] is True
    assert result["rewatch_status"] == "completed"
    for kind in ("plan", "dedupe", "request", "response", "eligibility"):
        assert diagnostics(api, kind)[0]["session_ref"] == result["session_ref"]
    assert diagnostics(api, "request")[0]["allow_rewatch"] is True
    assert diagnostics(api, "response")[0]["http_status"] == 201
    assert diagnostics(api, "response")[0]["elapsed_ms"] >= 0


def test_diagnostics_show_persistent_duplicate_and_cached_plan(api):
    sink.SimklSink().send(event(), config())
    sink.SimklSink().send(event(), config())
    assert [row["decision"] for row in diagnostics(api, "dedupe")] == ["new", "finished", "done"]
    assert [row["cache"] for row in diagnostics(api, "plan")] == ["miss", "hit"]
    assert diagnostics(api, "plan")[-1]["expires_in_s"] > 0


@pytest.mark.parametrize("failure,cause", [(requests.Timeout, "timeout"), (requests.ConnectionError, "connection_error"), (RuntimeError, "request_error")])
def test_transport_diagnostics_never_log_exception_credentials(api, monkeypatch, failure, cause):
    cfg = config()
    rewatches.account(cfg, sink._post)

    def fail(*args, **kwargs):
        raise failure("Authorization: Bearer test-account-a; api_key=test-client; private-server")

    monkeypatch.setattr(sink.requests, "post", fail)
    target = sink.SimklSink()
    target.send(event(), cfg)
    target.send(event(), cfg)
    assert diagnostics(api, "response")[0]["outcome"] == cause
    assert diagnostics(api, "failure")[0]["retryable"] is False
    assert diagnostics(api, "dedupe")[-1]["decision"] == "uncertain"
    messages = "\n".join(message for message, _ in api["logs"])
    for secret in ("test-account-a", "test-client", "private-server", "Authorization", "session-a"):
        assert secret not in messages


@pytest.mark.parametrize("case,cause,status", [("timeout", "timeout", 0), ("connection", "connection_error", 0), ("http", "http_error", 401), ("invalid", "invalid_response", 200), ("plan", "invalid_plan", 200)])
def test_plan_failure_diagnostics_survive_cache_hit(api, monkeypatch, case, cause, status):
    def post(*args, **kwargs):
        if case == "timeout":
            raise requests.Timeout("secret")
        if case == "connection":
            raise requests.ConnectionError("secret")
        return Response({"account": {"type": "unexpected"}} if case == "plan" else {}, status)

    monkeypatch.setattr(sink.requests, "post", post)
    sink.SimklSink().send(event(), config())
    sink.SimklSink().send(event(), config())
    rows = diagnostics(api, "plan")
    assert [row["cache"] for row in rows] == ["miss", "hit"]
    assert all(row["reason"] == cause and row["http_status"] == status for row in rows)
    assert diagnostics(api, "skip")[0]["reason"] == "simkl_plan_unavailable"


@pytest.mark.parametrize("payload,status,outcome", [({}, 201, "invalid_response"), ("private error page", 201, "invalid_response"), ({"error": "RATE_LIMIT"}, 400, "http_error")])
def test_response_diagnostics_distinguish_malformed_and_rate_limited(api, monkeypatch, payload, status, outcome):
    cfg = config()
    rewatches.account(cfg, sink._post)
    monkeypatch.setattr(sink.requests, "post", lambda *a, **k: Response(payload, status))
    sink.SimklSink().send(event(), cfg)
    row = diagnostics(api, "response")[0]
    assert row["outcome"] == outcome
    assert row["http_status"] == status
    assert row["api_error"] == ("RATE_LIMIT" if status == 400 else None)
    assert "private error page" not in str(api["logs"])
    assert not api["records"]


def test_diagnostics_show_threshold_missing_session_and_downgrade(api):
    sink.SimklSink().send(event(progress=79), config())
    assert diagnostics(api, "threshold")[0]["action"] == "pause"
    assert diagnostics(api, "threshold")[0]["watched_at"] == 90
    sink.SimklSink().send(event(session=None), config())
    assert diagnostics(api, "eligibility")[-1]["reason"] == "missing_session"
    api["status"] = "pro_required"
    sink.SimklSink().send(event(), config())
    assert diagnostics(api, "plan")[-1]["reason"] == "pro_required"
    assert diagnostics(api, "plan")[-1]["plan"] == "free"


def test_diagnostics_record_storage_failures(api, monkeypatch):
    monkeypatch.setattr(rewatches, "get_conn", lambda: None)
    sink.SimklSink().send(event(), config())
    assert diagnostics(api, "dedupe")[0]["decision"] == "storage_error"


def test_webhook_diagnostics_show_skip_without_watcher_dispatcher(api, monkeypatch):
    from providers.webhooks import dispatch

    monkeypatch.setattr(dispatch, "_SINKS", {})
    cfg = config()
    cfg["scrobble"]["webhook"] = {"sinks": ["simkl"], "simkl_rewatches": True}
    api["status"] = "too_soon"
    for _ in range(2):
        dispatch.dispatch_scrobble("jellyfin", "/scrobble/stop", media_type="movie", ids={"simkl": "123"},
                                   progress=95, session_key="webhook-session", account="test-user", cfg=cfg)
    result = diagnostics(api, "result")[0]
    assert result["route"] == "webhook:jellyfin:default:simkl:default"
    assert result["method"] == "webhook"
    assert result["rewatch_status"] == "too_soon"
    assert result["saved"] is False
    assert diagnostics(api, "dedupe")[-1]["decision"] == "done"


def test_new_diagnostics_obey_debug_setting(api, monkeypatch):
    output = []
    monkeypatch.setattr(sink, "_log", api["original_log"])
    monkeypatch.setattr(sink, "BASE_LOG", lambda message, **kwargs: output.append(message))
    monkeypatch.setattr(sink, "_is_debug", lambda: False)
    target = sink.SimklSink()
    target._rewatch_log(event(), config(), "skip", reason="test")
    assert not output
    monkeypatch.setattr(sink, "_is_debug", lambda: True)
    target._rewatch_log(event(), config(), "skip", reason="test")
    assert len(output) == 1
