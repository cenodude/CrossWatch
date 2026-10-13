# tests/test_health_alerts.py
# CrossWatch - Watcher and provider sign-in alert tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import types
from typing import Any

import pytest

from services import health_alerts
from services import scheduler_webhooks as webhooks

PUSHOVER_KEY = "k" * 30
ROUTE = {"id": "R1", "enabled": True, "provider": "plex", "provider_instance": "default", "sink": "trakt", "sink_instance": "P02"}


def _cfg(**hooks: Any) -> dict[str, Any]:
    return {
        "pairs": [
            {"id": "p1", "source": "PLEX", "target": "TRAKT", "target_instance": "P02"},
            {"id": "p2", "source": "SIMKL", "target": "MDB", "enabled": False},
            {"id": "p3", "source": "CROSSWATCH", "target": "TMDB"},
        ],
        "scrobble": {
            "enabled": True,
            "sources": {"watcher": True},
            "watch": {"routes": [dict(ROUTE), {"id": "R2", "enabled": False, "provider": "emby", "sink": "simkl"}]},
        },
        "scheduling": {"webhooks": {"enabled": True, "url": "https://hooks.example.com/cw", **hooks}},
    }


def _mock_dns(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        webhooks.socket,
        "getaddrinfo",
        lambda _host, port, *_a, **_k: [(0, 0, 0, "", ("93.184.216.34", int(port or 443)))],
    )


def _capture_posts(monkeypatch: pytest.MonkeyPatch, status: Any = 1) -> list[dict[str, Any]]:
    posted: list[dict[str, Any]] = []

    class FakeResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, Any]:
            return {"status": status}

    def fake_post(url: str, **kwargs: Any) -> FakeResponse:
        posted.append({"url": url, **kwargs})
        return FakeResponse()

    _mock_dns(monkeypatch)
    monkeypatch.setattr(webhooks.requests, "post", fake_post)
    return posted


def test_state_tracker_reports_each_change_once() -> None:
    tracker = health_alerts.StateTracker()
    item = {"label": "Route R1"}

    assert tracker.observe({"R1": (True, item)}) == ([], [])
    assert tracker.observe({"R1": (True, item)}) == ([item], [])
    assert tracker.observe({"R1": (True, item)}) == ([], [])
    assert tracker.observe({"R1": (None, item)}) == ([], [])
    assert tracker.observe({"R1": (False, item)}) == ([], [item])
    assert tracker.observe({"R1": (False, item)}) == ([], [])


def test_state_tracker_needs_consecutive_failures_and_forgets_removed_keys() -> None:
    tracker = health_alerts.StateTracker()
    item = {"label": "Route R1"}

    tracker.observe({"R1": (True, item)})
    tracker.observe({"R1": (False, item)})
    assert tracker.observe({"R1": (True, item)}) == ([], [])

    tracker.observe({"R1": (True, item)})
    assert tracker.observe({}) == ([], [])
    assert tracker.observe({"R1": (False, item)}) == ([], [])


@pytest.mark.parametrize(
    ("reason", "lost"),
    [
        ("Trakt: unauthorized (token expired/revoked)", True),
        ("TRAKT: login expired, reconnect Trakt", True),
        ("PunchPlay: reconnect required", True),
        ("Stremio: authentication failed", True),
        ("HTTP 401", True),
        ("unauthorized", True),
        ("Kitsu: reconnect required", True),
        ("Kodi: invalid credentials", True),
        ("Floppy: invalid API token", True),
        ("invalid_credentials", True),
        ("invalid_api_token", True),
        ("auth_failed", True),
        ("Plex: network error/timeout (timed out)", False),
        ("Plex: network error/timeout (certificate has expired)", False),
        ("Nuvio: token refresh failed", False),
        ("Nuvio: service unavailable", False),
        ("Trakt: service error (502)", False),
        ("Daily API limit reached.", False),
        ("SIMKL: connection not verified; refresh to check", False),
        ("Kodi: probe failed", False),
        ("", False),
    ],
)
def test_auth_lost_reason_separates_sign_in_loss_from_outages(reason: str, lost: bool) -> None:
    assert health_alerts.auth_lost_reason(reason) is lost


def test_used_provider_instances_covers_enabled_pairs_and_routes() -> None:
    used = health_alerts.used_provider_instances(_cfg())

    assert used == {
        ("PLEX", "default"): ["Sync pair p1", "Watcher route R1"],
        ("TRAKT", "P02"): ["Sync pair p1", "Watcher route R1"],
        ("CROSSWATCH", "default"): ["Sync pair p3"],
        ("TMDB", "default"): ["Sync pair p3"],
    }

    cfg = _cfg()
    cfg["scrobble"]["sources"]["watcher"] = False
    cfg["pairs"] = []
    assert health_alerts.used_provider_instances(cfg) == {}


def test_used_provider_instances_covers_active_scrobble_webhooks(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.webhooks import config as webhook_config

    cfg = _cfg()
    cfg["pairs"] = []
    cfg["scrobble"]["sources"]["watcher"] = False
    endpoints = [{"provider": "jellyfin", "instance": "default", "sinks": ["simkl", "trakt"]}]
    monkeypatch.setattr(webhook_config, "active_webhook_endpoints", lambda _cfg: endpoints)
    monkeypatch.setattr(webhook_config, "webhook_settings", lambda *_args: {})
    monkeypatch.setattr(webhook_config, "webhook_sink_instance", lambda _settings, sink: "P02" if sink == "trakt" else "default")

    assert health_alerts.used_provider_instances(cfg) == {
        ("JELLYFIN", "default"): ["Jellyfin webhook"],
        ("SIMKL", "default"): ["Jellyfin webhook"],
        ("TRAKT", "P02"): ["Jellyfin webhook"],
    }


def test_provider_states_classifies_probe_results() -> None:
    results: dict[str, Any] = {
        "PLEX": (True, ""),
        "TRAKT": (False, "Trakt: unauthorized (token expired/revoked)"),
        "TMDB": (False, "TMDb: network error/timeout"),
        "CROSSWATCH": (False, "CrossWatch Local Tracker: not connected"),
    }

    states = health_alerts.provider_states(_cfg(), lambda _cfg, provider, _instance: results[provider])

    assert states["PLEX:default"][0] is False
    assert states["TMDB:default"][0] is None
    assert states["CROSSWATCH:default"][0] is None
    bad, item = states["TRAKT:P02"]
    assert bad is True
    assert item["label"] == "Trakt P02"
    assert item["detail"] == "Trakt: unauthorized (token expired/revoked)"
    assert item["used_by"] == ["Sync pair p1", "Watcher route R1"]

    skipped = health_alerts.provider_states(_cfg(), lambda *_args: None)
    assert skipped == {}


def test_provider_states_keeps_a_provider_whose_check_raised() -> None:
    def probe(_cfg: Any, provider: str, _instance: str) -> tuple[bool, str]:
        if provider == "TRAKT":
            raise RuntimeError("boom")
        return True, ""

    states = health_alerts.provider_states(_cfg(), probe)

    assert states["TRAKT:P02"][0] is None
    assert states["PLEX:default"][0] is False


def test_probe_provider_checks_profiles_that_need_a_new_sign_in(monkeypatch: pytest.MonkeyPatch) -> None:
    from api import probesAPI

    seen: list[tuple[str, Any]] = []

    def fake_probe(view: dict[str, Any], max_age_sec: int = 0) -> tuple[bool, str]:
        seen.append((str(view["_cw_probe"]["instance"]), max_age_sec))
        return False, "Kitsu: reconnect required"

    monkeypatch.setitem(probesAPI.DETAIL_PROBES, "KITSU", fake_probe)
    cfg = {"kitsu": {"access_token": "tok", "reauth_required": True}}

    assert probesAPI._prov_configured(cfg, "KITSU", "default") is False
    assert health_alerts.probe_provider(cfg, "KITSU", "default") == (False, "Kitsu: reconnect required")
    assert seen == [("default", probesAPI.PROBE_TTL)]
    assert health_alerts.probe_provider(cfg, "NOPE", "default") is None


def test_lost_sign_in_survives_an_unknown_check_and_still_recovers(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[str] = []
    results: list[Any] = [
        (False, "Trakt: unauthorized (token expired/revoked)"),
        (False, "Trakt: unauthorized (token expired/revoked)"),
        RuntimeError("boom"),
        (False, "Trakt: network error/timeout"),
        (True, ""),
    ]
    cfg = _cfg(alert_on_provider_auth=True)
    cfg["pairs"] = [{"id": "p1", "source": "TRAKT", "target": "TRAKT"}]
    cfg["scrobble"]["sources"]["watcher"] = False
    monkeypatch.setattr(health_alerts, "PROVIDER_EVERY_TICKS", 1)

    def probe(*_args: Any) -> tuple[bool, str]:
        result = results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    monitor = health_alerts.HealthMonitor(
        None,
        lambda: cfg,
        probe=probe,
        notify=lambda _cfg, event, _items, **_k: sent.append(event) or True,
    )
    for _ in range(5):
        monitor.tick()

    assert sent == ["provider_auth_lost", "provider_auth_recovered"]


def test_monitor_keeps_checking_providers_when_the_watcher_check_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    cfg = _cfg(alert_on_watcher=True, alert_on_provider_auth=True)

    def broken(_app: Any, _cfg: Any) -> dict[str, Any]:
        raise RuntimeError("boom")

    monkeypatch.setattr(health_alerts, "watcher_states", broken)
    monkeypatch.setattr(health_alerts, "provider_states", lambda _cfg, _probe: calls.append("providers") or {})

    health_alerts.HealthMonitor(None, lambda: cfg).tick()

    assert calls == ["providers"]


def test_stopped_monitor_does_not_check_or_send(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[str] = []
    stopped = {"value": False}
    cfg = _cfg(alert_on_watcher=True)
    monkeypatch.setattr(health_alerts, "watcher_states", lambda _app, _cfg: {"R1": (True, {"label": "Route R1"})})

    monitor = health_alerts.HealthMonitor(
        None,
        lambda: cfg,
        notify=lambda _cfg, event, _items, **_k: sent.append(event) or True,
        stopped=lambda: stopped["value"],
    )
    monitor.tick()
    stopped["value"] = True
    monitor.tick()
    monitor.tick()

    assert sent == []


def test_watcher_states_flags_enabled_routes_that_are_not_running(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble import watch_manager

    app = types.SimpleNamespace(state=types.SimpleNamespace())
    rows: list[dict[str, Any]] = []
    monkeypatch.setattr(watch_manager, "status", lambda _app: {"groups": [], "routes": rows})

    states = health_alerts.watcher_states(app, _cfg())
    assert list(states) == ["R1"]
    bad, item = states["R1"]
    assert bad is True
    assert item["label"] == "Route R1"
    assert item["detail"] == "Plex to Trakt P02"

    rows.append({"id": "R1", "running": True})
    assert health_alerts.watcher_states(app, _cfg())["R1"][0] is False

    cfg = _cfg()
    cfg["scrobble"]["sources"]["watcher"] = False
    assert health_alerts.watcher_states(app, cfg) == {}


def test_watcher_states_flags_enabled_routes_when_the_watcher_never_started() -> None:
    app = types.SimpleNamespace(state=types.SimpleNamespace(watch_groups={}, watch_manager=None))

    states = health_alerts.watcher_states(app, _cfg())

    assert {key: bad for key, (bad, _item) in states.items()} == {"R1": True}


def test_monitor_sends_one_alert_per_state_change(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[tuple[str, list[str]]] = []
    watcher: dict[str, Any] = {"R1": (True, {"label": "Route R1"})}
    providers: dict[str, Any] = {"TRAKT:default": (True, {"label": "Trakt"})}
    cfg = _cfg(alert_on_watcher=True, alert_on_provider_auth=True)
    monkeypatch.setattr(health_alerts, "watcher_states", lambda _app, _cfg: dict(watcher))
    monkeypatch.setattr(health_alerts, "provider_states", lambda _cfg, _probe: dict(providers))
    monkeypatch.setattr(health_alerts, "PROVIDER_EVERY_TICKS", 1)

    def notify(_cfg: Any, event: str, items: list[dict[str, Any]], **_kwargs: Any) -> bool:
        sent.append((event, [item["label"] for item in items]))
        return True

    monitor = health_alerts.HealthMonitor(None, lambda: cfg, notify=notify)

    monitor.tick()
    assert sent == []
    monitor.tick()
    assert sent == [("watcher_stopped", ["Route R1"]), ("provider_auth_lost", ["Trakt"])]
    monitor.tick()
    assert len(sent) == 2

    watcher["R1"] = (False, {"label": "Route R1"})
    providers["TRAKT:default"] = (None, {"label": "Trakt"})
    monitor.tick()
    assert sent[2:] == [("watcher_recovered", ["Route R1"])]

    providers["TRAKT:default"] = (False, {"label": "Trakt"})
    monitor.tick()
    assert sent[3:] == [("provider_auth_recovered", ["Trakt"])]


def test_monitor_ignores_checks_that_are_switched_off(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[str] = []
    calls: list[str] = []
    cfg = _cfg(alert_on_watcher=False, alert_on_provider_auth=False)
    monkeypatch.setattr(health_alerts, "watcher_states", lambda _app, _cfg: calls.append("watcher") or {})
    monkeypatch.setattr(health_alerts, "provider_states", lambda _cfg, _probe: calls.append("providers") or {})

    monitor = health_alerts.HealthMonitor(None, lambda: cfg, notify=lambda *_a, **_k: sent.append("x") or True)
    monitor.tick()
    monitor.tick()

    assert calls == []
    assert sent == []


def test_monitor_checks_providers_less_often_than_the_watcher(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    cfg = _cfg(alert_on_watcher=True, alert_on_provider_auth=True)
    monkeypatch.setattr(health_alerts, "watcher_states", lambda _app, _cfg: calls.append("watcher") or {})
    monkeypatch.setattr(health_alerts, "provider_states", lambda _cfg, _probe: calls.append("providers") or {})

    monitor = health_alerts.HealthMonitor(None, lambda: cfg)
    for _ in range(health_alerts.PROVIDER_EVERY_TICKS + 1):
        monitor.tick()

    assert calls.count("watcher") == health_alerts.PROVIDER_EVERY_TICKS + 1
    assert calls.count("providers") == 2


def test_health_webhook_posts_crosswatch_json_to_the_common_url(monkeypatch: pytest.MonkeyPatch) -> None:
    posted = _capture_posts(monkeypatch)
    items = [{"label": "Trakt P02", "detail": "Trakt: unauthorized", "provider": "trakt", "instance": "P02"}]

    assert webhooks.notify_health_webhook(_cfg(alert_on_provider_auth=True), "provider_auth_lost", items) is True

    assert posted[0]["url"] == "https://hooks.example.com/cw"
    body = posted[0]["json"]
    assert body["event"] == "provider_auth_lost"
    assert body["status"] == "failed"
    assert body["items"] == items
    assert posted[0]["allow_redirects"] is False


def test_health_webhook_respects_flags_and_destination(monkeypatch: pytest.MonkeyPatch) -> None:
    posted = _capture_posts(monkeypatch)
    items = [{"label": "Route R1", "detail": "Plex to Trakt"}]

    assert webhooks.notify_health_webhook(_cfg(), "watcher_stopped", items) is False
    assert webhooks.notify_health_webhook(_cfg(alert_on_provider_auth=True), "watcher_stopped", items) is False
    assert webhooks.notify_health_webhook(_cfg(alert_on_watcher=True, enabled=False), "watcher_stopped", items) is False
    assert webhooks.notify_health_webhook(_cfg(alert_on_watcher=True), "watcher_stopped", []) is False
    assert webhooks.notify_health_webhook(_cfg(alert_on_watcher=True), "start", items) is False
    only_failure = _cfg(alert_on_watcher=True, url="", failure_url="https://hooks.example.com/fail")
    assert webhooks.notify_health_webhook(only_failure, "watcher_stopped", items) is False
    assert posted == []


def test_health_webhook_pushover_and_ntfy_priorities(monkeypatch: pytest.MonkeyPatch) -> None:
    posted = _capture_posts(monkeypatch)
    items = [{"label": "Route R1", "detail": "Plex to Trakt"}, {"label": "Route R3", "detail": ""}]
    pushover = _cfg(alert_on_watcher=True, payload_format="pushover", pushover_token=PUSHOVER_KEY, pushover_user=PUSHOVER_KEY, url="")
    ntfy = _cfg(alert_on_watcher=True, payload_format="ntfy", url="https://ntfy.sh/crosswatch")

    assert webhooks.notify_health_webhook(pushover, "watcher_stopped", items) is True
    assert webhooks.notify_health_webhook(pushover, "watcher_recovered", items) is True
    assert webhooks.notify_health_webhook(ntfy, "watcher_stopped", items) is True
    assert webhooks.notify_health_webhook(ntfy, "watcher_recovered", items) is True

    assert posted[0]["url"] == webhooks.PUSHOVER_URL
    assert posted[0]["data"]["priority"] == 1
    assert posted[0]["data"]["title"] == "CrossWatch watcher stopped"
    assert posted[0]["data"]["message"].splitlines() == [
        "These enabled watcher routes are not running:",
        "- Route R1: Plex to Trakt",
        "- Route R3",
    ]
    assert posted[1]["data"]["priority"] == 0
    assert posted[2]["url"] == "https://ntfy.sh/crosswatch"
    assert posted[2]["headers"]["Priority"] == "4"
    assert posted[2]["headers"]["Title"] == "CrossWatch watcher stopped"
    assert posted[3]["headers"]["Priority"] == "3"


def test_health_payload_notifiarr_shape() -> None:
    payload = webhooks.build_health_payload(
        "provider_auth_lost",
        [{"label": "Trakt", "detail": "Trakt: unauthorized"}],
        "notifiarr",
    )

    assert payload["notification"] == {"update": False, "name": "CrossWatch", "event": "crosswatch-provider_auth_lost"}
    assert payload["discord"]["color"] == "FF5A5F"
    assert payload["discord"]["text"]["title"] == "CrossWatch provider sign-in lost"
    assert payload["discord"]["text"]["fields"] == [{"title": "Trakt", "text": "Trakt: unauthorized", "inline": False}]
    assert payload["discord"]["text"]["footer"] == "CrossWatch Health"


def test_send_test_webhook_uses_unsaved_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    posted = _capture_posts(monkeypatch)

    assert webhooks.send_test_webhook({"payload_format": "ntfy", "url": "https://ntfy.sh/crosswatch"}) == (True, "")
    assert posted[0]["headers"]["Title"] == "CrossWatch test notification"
    assert posted[0]["data"] == b"Scheduler webhooks are set up correctly."

    assert webhooks.send_test_webhook({"base_url": "https://hc-ping.com/abc"}) == (True, "")
    assert posted[1]["url"] == "https://hc-ping.com/abc"
    assert posted[1]["json"]["event"] == "test"

    ok, error = webhooks.send_test_webhook({"payload_format": "pushover"})
    assert ok is False and error == "No destination configured for this payload format."
    assert len(posted) == 2


def test_send_test_webhook_reports_why_delivery_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    _capture_posts(monkeypatch, status=0)
    keys = {"payload_format": "pushover", "pushover_token": PUSHOVER_KEY, "pushover_user": PUSHOVER_KEY}

    assert webhooks.send_test_webhook(keys) == (False, "rejected by Pushover")

    monkeypatch.setattr(webhooks.socket, "getaddrinfo", lambda _h, port, *_a, **_k: [(0, 0, 0, "", ("10.0.0.5", int(port)))])
    assert webhooks.send_test_webhook({"url": "https://hooks.example.com/cw"}) == (False, "blocked unsafe URL")


def test_health_alert_flags_are_normalized_everywhere() -> None:
    from cw_platform.config_base import _normalize_scheduling
    from services.scheduling import merge_defaults

    raw = {"enabled": True, "alert_on_watcher": "true", "alertOnProviderAuth": True}
    cfg = {"scheduling": {"webhooks": dict(raw)}}
    _normalize_scheduling(cfg)

    for hooks in (cfg["scheduling"]["webhooks"], merge_defaults({"webhooks": dict(raw)})["webhooks"]):
        assert hooks["alert_on_watcher"] is True
        assert hooks["alert_on_provider_auth"] is True

    conflict = {"alert_on_watcher": False, "alertOnWatcher": True, "alert_on_unresolved": False, "alertOnUnresolved": True}
    cfg = {"scheduling": {"webhooks": dict(conflict)}}
    _normalize_scheduling(cfg)
    for hooks in (cfg["scheduling"]["webhooks"], webhooks.normalize_scheduler_webhooks(conflict)):
        assert hooks["alert_on_watcher"] is False
        assert hooks["alert_on_unresolved"] is False

    defaults = merge_defaults({})["webhooks"]
    assert defaults["alert_on_watcher"] is False
    assert defaults["alert_on_provider_auth"] is False


def test_worker_starts_once_and_stops(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(health_alerts, "_worker", None)
    monkeypatch.setattr(health_alerts, "INTERVAL_S", 30)
    try:
        assert health_alerts.start_worker(None, lambda: {}) is True
        assert health_alerts.start_worker(None, lambda: {}) is False
    finally:
        health_alerts.stop_worker()
        worker = health_alerts._worker
        if worker is not None:
            worker.join(timeout=5)
    assert worker is not None and not worker.is_alive()
