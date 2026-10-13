# tests/test_health_details.py
# CrossWatch - Detailed health endpoint tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from services import health_alerts, health_details

NOW = 1_800_000_000
MONITOR_ID = "c" * 32
OTHER_MONITOR_ID = "e" * 32
PLAIN_ID = "d" * 32
PROFILE_ID = "cccccccccccc4ccc8ccccccccccccccc"


def _cfg() -> dict[str, Any]:
    return {
        "pairs": [
            {"id": "p1", "source": "PLEX", "target": "TRAKT", "target_instance": "P02"},
            {"id": "p3", "source": "CROSSWATCH", "target": "TMDB"},
        ],
        "scrobble": {
            "enabled": True,
            "sources": {"watcher": True},
            "watch": {"routes": [{"id": "R1", "enabled": True, "provider": "plex", "sink": "trakt", "sink_instance": "P02"}]},
        },
        "scheduling": {"enabled": True},
    }


def _routes(monkeypatch: pytest.MonkeyPatch, **down: bool) -> None:
    states = {route_id: (bad, {"route_id": route_id}) for route_id, bad in down.items()}
    monkeypatch.setattr(health_alerts, "watcher_states", lambda _app, _cfg: states)


def _details(
    results: dict[Any, tuple[bool, str]] | None = None,
    *,
    cfg: dict[str, Any] | None = None,
    scheduler_status: dict[str, Any] | None = None,
    last_run: dict[str, Any] | None = None,
    sync_running: bool = False,
    tokens: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    probes = results or {}
    status = {"running": True, "last_run_ok": True, "next_run_at": NOW + 600} if scheduler_status is None else scheduler_status
    run = {"finished_at": NOW - 60, "exit_code": 0} if last_run is None else last_run

    def probe(_cfg: Any, provider: str, instance: str) -> tuple[bool, str]:
        return probes.get((provider, instance), probes.get(provider, (True, "")))

    return health_details.health_details(
        None,
        _cfg() if cfg is None else cfg,
        scheduler_status=lambda: status,
        sync_state=lambda: (sync_running, run),
        tokens=lambda: tokens or [],
        probe=probe,
        now=NOW,
    )


def _messages(payload: dict[str, Any]) -> list[tuple[str, str]]:
    return [(issue["level"], issue["message"]) for issue in payload["issues"]]


def test_everything_healthy_reports_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    _routes(monkeypatch, R1=False)

    payload = _details()

    assert payload["ok"] is True
    assert payload["status"] == "ok"
    assert payload["issues"] == []
    assert payload["checked_at"] == NOW
    assert payload["watcher"] == {"enabled": True, "alive": True, "routes": 1, "routes_not_running": []}
    assert payload["scheduler"]["enabled"] is True
    assert payload["scheduler"]["overdue"] is False
    assert payload["last_run"] == {"running": False, "finished_at": NOW - 60, "exit_code": 0}
    assert [(row["provider"], row["instance"], row["connected"]) for row in payload["providers"]] == [
        ("CROSSWATCH", "default", True),
        ("PLEX", "default", True),
        ("TMDB", "default", True),
        ("TRAKT", "P02", True),
    ]
    assert payload["api_tokens_expiring"] == []


def test_watcher_partly_down_is_degraded_and_fully_down_is_failing(monkeypatch: pytest.MonkeyPatch) -> None:
    _routes(monkeypatch, R1=False, R2=True)
    partly = _details()
    assert partly["status"] == "degraded"
    assert partly["watcher"] == {"enabled": True, "alive": True, "routes": 2, "routes_not_running": ["R2"]}

    _routes(monkeypatch, R1=True, R2=True)
    down = _details()
    assert down["status"] == "failing"
    assert down["ok"] is False
    assert down["watcher"]["alive"] is False
    assert _messages(down) == [("failing", "Watcher is not running")]

    _routes(monkeypatch)
    unused = _details()
    assert unused["status"] == "ok"
    assert unused["watcher"] == {"enabled": False, "alive": False, "routes": 0, "routes_not_running": []}


def test_scheduler_states(monkeypatch: pytest.MonkeyPatch) -> None:
    _routes(monkeypatch, R1=False)

    stopped = _details(scheduler_status={"running": False})
    assert stopped["status"] == "failing"
    assert _messages(stopped) == [("failing", "Scheduler is enabled but not running")]

    failed = _details(scheduler_status={"running": True, "last_run_ok": False, "last_error": "standard run failed"})
    assert failed["status"] == "degraded"
    assert _messages(failed) == [("degraded", "Last scheduled run failed: standard run failed")]

    late = {"running": True, "last_run_ok": True, "next_run_at": NOW - health_details.OVERDUE_GRACE_S - 1}
    overdue = _details(scheduler_status=dict(late))
    assert overdue["status"] == "degraded"
    assert overdue["scheduler"]["overdue"] is True
    syncing = _details(scheduler_status=dict(late), sync_running=True)
    assert syncing["status"] == "ok"
    assert syncing["scheduler"]["overdue"] is False
    busy = _details(scheduler_status={**late, "busy": True})
    assert busy["status"] == "ok"
    assert busy["scheduler"]["overdue"] is False

    cfg = _cfg()
    cfg["scheduling"] = {"enabled": False}
    disabled = _details(cfg=cfg, scheduler_status={"running": False})
    assert disabled["status"] == "ok"
    assert disabled["scheduler"]["enabled"] is False

    cfg["scheduling"] = {"enabled": False, "advanced": {"enabled": True}}
    assert _details(cfg=cfg, scheduler_status={"running": False})["status"] == "failing"


def test_scheduler_reports_busy_while_a_scheduled_run_executes(monkeypatch: pytest.MonkeyPatch) -> None:
    from services import scheduling

    config = {"scheduling": {"enabled": True, "mode": "hourly"}}
    seen: list[bool] = []

    def run_sync(_payload: Any = None) -> bool:
        seen.append(bool(scheduler.status()["busy"]))
        scheduler._stop.set()
        return True

    scheduler = scheduling.SyncScheduler(
        load_config=lambda: config,
        save_config=lambda next_cfg: config.update(next_cfg),
        run_sync_fn=run_sync,
        is_sync_running_fn=lambda: False,
    )
    monkeypatch.setattr(scheduling, "compute_next_run", lambda *_args, **_kwargs: datetime.fromtimestamp(time.time() - 10))
    update_next = scheduler._update_next
    published: list[bool] = []

    def tracked_update(*args: Any, **kwargs: Any) -> None:
        published.append(bool(scheduler._status["busy"]))
        update_next(*args, **kwargs)

    monkeypatch.setattr(scheduler, "_update_next", tracked_update)
    assert scheduler.status()["busy"] is False

    worker = threading.Thread(target=scheduler._loop, daemon=True)
    worker.start()
    worker.join(15)
    scheduler._stop.set()

    assert not worker.is_alive()
    assert seen == [True]
    assert published == [False, True]
    assert scheduler.status()["busy"] is False


def test_scheduler_clears_busy_when_a_scheduled_run_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    from services import scheduling

    config = {"scheduling": {"enabled": True, "mode": "hourly"}}

    def run_sync(_payload: Any = None) -> bool:
        raise RuntimeError("boom")

    scheduler = scheduling.SyncScheduler(
        load_config=lambda: config,
        save_config=lambda next_cfg: config.update(next_cfg),
        run_sync_fn=run_sync,
        is_sync_running_fn=lambda: False,
    )
    monkeypatch.setattr(scheduling, "compute_next_run", lambda *_args, **_kwargs: datetime.fromtimestamp(time.time() - 10))

    with pytest.raises(RuntimeError):
        scheduler._loop()

    assert scheduler.status()["busy"] is False
    assert scheduler.status()["running"] is False


def test_failed_sync_run_is_degraded(monkeypatch: pytest.MonkeyPatch) -> None:
    _routes(monkeypatch, R1=False)

    failed = _details(last_run={"finished_at": NOW - 60, "exit_code": "2"})
    assert failed["status"] == "degraded"
    assert failed["last_run"]["exit_code"] == 2
    assert _messages(failed) == [("degraded", "Last sync run failed with exit code 2")]

    running = _details(last_run={"running": True, "finished_at": 0, "exit_code": None})
    assert running["status"] == "ok"
    assert running["last_run"] == {"running": True, "finished_at": 0, "exit_code": None}


def test_provider_sign_in_loss_fails_only_when_scrobbling_uses_it(monkeypatch: pytest.MonkeyPatch) -> None:
    _routes(monkeypatch, R1=False)
    expired = (False, "unauthorized (token expired/revoked)")

    scrobble = _details({"TRAKT": expired})
    assert scrobble["status"] == "failing"
    trakt = next(row for row in scrobble["providers"] if row["provider"] == "TRAKT")
    assert trakt == {
        "provider": "TRAKT",
        "instance": "P02",
        "used_by": ["Sync pair p1", "Watcher route R1"],
        "connected": False,
        "reauth_required": True,
        "reason": "unauthorized (token expired/revoked)",
    }

    pair_only = _details({"TMDB": expired})
    assert pair_only["status"] == "degraded"
    assert [issue["level"] for issue in pair_only["issues"]] == ["degraded"]
    assert pair_only["issues"][0]["message"].endswith("needs a new sign-in")

    outage = _details({"TRAKT": (False, "Trakt: network error/timeout")})
    assert outage["status"] == "degraded"
    trakt = next(row for row in outage["providers"] if row["provider"] == "TRAKT")
    assert trakt["connected"] is False and trakt["reauth_required"] is False


def test_each_instance_of_a_provider_is_reported_on_its_own(monkeypatch: pytest.MonkeyPatch) -> None:
    _routes(monkeypatch, R1=False)
    cfg = _cfg()
    cfg["pairs"].append({"id": "p4", "source": "CROSSWATCH", "target": "TRAKT"})

    payload = _details({("TRAKT", "default"): (False, "unauthorized")}, cfg=cfg)

    trakt = {row["instance"]: row for row in payload["providers"] if row["provider"] == "TRAKT"}
    assert trakt["default"]["connected"] is False and trakt["default"]["used_by"] == ["Sync pair p4"]
    assert trakt["P02"]["connected"] is True
    assert payload["status"] == "degraded"


def test_webhook_only_provider_sign_in_loss_is_failing(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.webhooks import config as webhook_config

    _routes(monkeypatch)
    cfg = _cfg()
    cfg["pairs"] = []
    cfg["scrobble"]["sources"]["watcher"] = False
    endpoints = [{"provider": "jellyfin", "instance": "default", "sinks": ["trakt"]}]
    monkeypatch.setattr(webhook_config, "active_webhook_endpoints", lambda _cfg: endpoints)
    monkeypatch.setattr(webhook_config, "webhook_settings", lambda *_args: {})
    monkeypatch.setattr(webhook_config, "webhook_sink_instance", lambda _settings, _sink: "default")

    payload = _details({"TRAKT": (False, "unauthorized")}, cfg=cfg)

    assert [(row["provider"], row["connected"]) for row in payload["providers"]] == [("JELLYFIN", True), ("TRAKT", False)]
    assert payload["status"] == "failing"


def test_unused_and_unprobed_providers_do_not_count(monkeypatch: pytest.MonkeyPatch) -> None:
    _routes(monkeypatch, R1=False)
    cfg = _cfg()
    cfg["pairs"].append({"id": "p9", "source": "SIMKL", "target": "MDBLIST", "enabled": False})

    payload = _details({"SIMKL": (False, "unauthorized")}, cfg=cfg)
    assert payload["status"] == "ok"
    assert "SIMKL" not in {row["provider"] for row in payload["providers"]}

    skipped = health_details.health_details(
        None,
        _cfg(),
        scheduler_status=lambda: {"running": True},
        sync_state=lambda: (False, {}),
        tokens=lambda: [],
        probe=lambda *_args: None,
        now=NOW,
    )
    assert skipped["status"] == "ok"
    assert skipped["providers"] == []


def test_a_broken_check_degrades_without_hiding_the_rest(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(_app: Any, _cfg: Any) -> dict[str, Any]:
        raise RuntimeError("boom")

    def raising() -> Any:
        raise OSError("boom")

    def probe(_cfg: Any, provider: str, _instance: str) -> tuple[bool, str]:
        if provider == "TMDB":
            raise RuntimeError("boom")
        return True, ""

    monkeypatch.setattr(health_alerts, "watcher_states", broken)

    payload = health_details.health_details(
        None,
        _cfg(),
        scheduler_status=raising,
        sync_state=raising,
        tokens=raising,
        probe=probe,
        now=NOW,
    )

    assert payload["status"] == "degraded"
    assert payload["watcher"] == {}
    assert payload["scheduler"] == {}
    assert payload["last_run"] == {}
    assert payload["api_tokens_expiring"] == []
    assert sorted((issue["component"], issue["message"]) for issue in payload["issues"] if issue["component"] != "providers") == [
        ("api_tokens", "Health check failed: OSError"),
        ("last_run", "Health check failed: OSError"),
        ("scheduler", "Health check failed: OSError"),
        ("watcher", "Health check failed: RuntimeError"),
    ]
    tmdb = next(row for row in payload["providers"] if row["provider"] == "TMDB")
    assert tmdb["connected"] is False
    assert tmdb["reason"] == "probe failed: RuntimeError"
    assert len(payload["providers"]) == 4


def test_slow_provider_check_does_not_hold_the_response(monkeypatch: pytest.MonkeyPatch) -> None:
    _routes(monkeypatch, R1=False)
    monkeypatch.setattr(health_details, "PROBE_DEADLINE_S", 0.5)
    release = threading.Event()
    calls: list[str] = []

    def probe(_cfg: Any, provider: str, _instance: str) -> tuple[bool, str]:
        calls.append(provider)
        if provider == "TRAKT":
            release.wait(10)
        return True, ""

    def run() -> dict[str, Any]:
        return health_details.health_details(
            None,
            _cfg(),
            scheduler_status=lambda: {"running": True},
            sync_state=lambda: (False, {}),
            tokens=lambda: [],
            probe=probe,
            now=NOW,
        )

    try:
        started = time.monotonic()
        first = run()
        second = run()
        elapsed = time.monotonic() - started
    finally:
        release.set()

    trakt = next(row for row in first["providers"] if row["provider"] == "TRAKT")
    assert trakt == {
        "provider": "TRAKT",
        "instance": "P02",
        "used_by": ["Sync pair p1", "Watcher route R1"],
        "connected": None,
        "reauth_required": False,
        "reason": "health check timed out",
    }
    assert first["status"] == "degraded"
    assert [issue["message"] for issue in first["issues"]] == ["Trakt P02 check timed out"]
    assert second["status"] == "degraded"
    assert calls.count("TRAKT") == 1
    assert elapsed < 5

    for _ in range(100):
        if not health_details._IN_FLIGHT:
            break
        time.sleep(0.05)
    assert health_details._IN_FLIGHT == {}
    assert run()["status"] == "ok"


def test_in_flight_check_is_not_reused_after_credentials_or_probe_change(monkeypatch: pytest.MonkeyPatch) -> None:
    release = threading.Event()
    calls: list[str] = []

    def stuck(cfg: Any, _provider: str, _instance: str) -> tuple[bool, str]:
        calls.append(str(cfg["plex"]["account_token"]))
        release.wait(10)
        return False, "unauthorized"

    def other(_cfg: Any, _provider: str, _instance: str) -> tuple[bool, str]:
        return True, ""

    old_cfg = {"plex": {"account_token": "old"}}
    new_cfg = {"plex": {"account_token": "new"}}
    key = ("PLEX", "default")

    try:
        first = health_details._submit(old_cfg, stuck, key)
        assert health_details._submit(old_cfg, stuck, key) is first
        reconnected = health_details._submit(new_cfg, stuck, key)
        swapped = health_details._submit(old_cfg, other, key)
        assert reconnected is not first
        assert swapped is not first
        assert swapped.result(5) == (True, "")
        assert all("old" not in str(part) and "new" not in str(part) for flight in health_details._IN_FLIGHT for part in flight[:3])
    finally:
        release.set()

    assert first.result(5) == (False, "unauthorized")
    assert reconnected.result(5) == (False, "unauthorized")
    assert sorted(calls) == ["new", "old"]


def test_default_probe_is_the_cached_alert_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    _routes(monkeypatch, R1=False)
    seen: list[tuple[str, str]] = []

    def probe(_cfg: Any, provider: str, instance: str) -> tuple[bool, str]:
        seen.append((provider, instance))
        return True, ""

    monkeypatch.setattr(health_alerts, "probe_provider", probe)

    payload = health_details.health_details(
        None,
        _cfg(),
        scheduler_status=lambda: {"running": True},
        sync_state=lambda: (False, {}),
        tokens=lambda: [],
        now=NOW,
    )

    assert payload["status"] == "ok"
    assert sorted(seen) == [("CROSSWATCH", "default"), ("PLEX", "default"), ("TMDB", "default"), ("TRAKT", "P02")]
    assert not hasattr(health_details, "probe_provider")


def test_expiring_tokens_are_listed_without_changing_the_status(monkeypatch: pytest.MonkeyPatch) -> None:
    _routes(monkeypatch, R1=False)
    window = health_details.TOKEN_EXPIRY_WINDOW_S
    tokens = [
        {"id": "a" * 16, "name": "forever", "expires_at": 0, "prefix": "cwt_aaaaaa"},
        {"id": "b" * 16, "name": "later", "expires_at": NOW + window + 1},
        {"id": "c" * 16, "name": "soon", "expires_at": NOW + window},
        {"id": "d" * 16, "name": "sooner", "expires_at": NOW + 3600},
    ]

    payload = _details(tokens=tokens)

    assert payload["status"] == "ok"
    assert payload["api_tokens_expiring"] == [
        {"id": "d" * 16, "name": "sooner", "expires_at": NOW + 3600},
        {"id": "c" * 16, "name": "soon", "expires_at": NOW + window},
    ]


@pytest.fixture()
def gated_app(config_base: Path, monkeypatch: pytest.MonkeyPatch):
    import importlib

    from cw_platform import config_base as cfg_base

    importlib.reload(cfg_base)
    from api import apiTokensAPI

    importlib.reload(apiTokensAPI)

    import crosswatch
    from api import appAuthAPI as auth
    from cw_platform.config_base import load_config

    def _user(name: str, permissions: dict[str, bool]) -> dict[str, Any]:
        return {
            "username": name,
            "enabled": True,
            "role": "user",
            "profile_id": PROFILE_ID,
            "permissions": permissions,
            "password": auth._password_hash("secrett3"),
        }

    cfg = {
        "user_profiles": {PROFILE_ID: {"label": "Ops", "instances": {}}},
        "app_auth": {
            "enabled": True,
            "username": "admin",
            "password": auth._password_hash("secrett1"),
            "sessions": [],
            "api_tokens": [],
            "users": {
                MONITOR_ID: _user("monitor", {"monitor": True}),
                OTHER_MONITOR_ID: _user("monitor2", {"monitor": True}),
                PLAIN_ID: _user("plain", {"dashboard": True, "watchlist": True, "playback": True, "write": True}),
            },
        },
    }
    (config_base / "config.json").write_text(json.dumps(cfg), encoding="utf-8")
    apiTokensAPI._TOUCH_CACHE.clear()
    monkeypatch.setattr(crosswatch, "load_config", load_config)
    monkeypatch.setattr(crosswatch, "app_resolve_api_token", apiTokensAPI.resolve_api_token)
    monkeypatch.setattr(crosswatch, "app_touch_api_token", lambda _tid: None)
    _routes(monkeypatch)
    return apiTokensAPI


def test_details_endpoint_needs_an_admin_or_monitor_token(gated_app, monkeypatch: pytest.MonkeyPatch) -> None:
    mod = gated_app
    from fastapi.testclient import TestClient

    import crosswatch
    from cw_platform.access_policy import monitor_api_allowed
    from cw_platform.config_base import load_config

    admin_raw, _a = mod.issue_api_token(load_config(), name="admin", expires_days=365)
    monitor_raw, monitor_entry = mod.issue_api_token(load_config(), name="uptime", user_id="monitor", expires_days=7)
    other_raw, other_entry = mod.issue_api_token(load_config(), name="other", user_id="monitor2", expires_days=3)
    plain_raw, _p = mod.issue_api_token(load_config(), name="plain", user_id="plain")
    client = TestClient(crosswatch.app)
    path = "/api/health/details"

    assert client.get("/api/health").status_code == 200
    assert client.get(path).status_code == 401
    assert client.get(path, headers={mod.TOKEN_HEADER: "cwt_nope"}).status_code == 401
    assert client.get(path, headers={mod.TOKEN_HEADER: plain_raw}).status_code == 403
    assert client.post(path, headers={mod.TOKEN_HEADER: monitor_raw}).status_code == 403
    assert monitor_api_allowed({"is_admin": False, "permissions": {"monitor": True}}, path, "GET") is True
    assert path not in crosswatch._PUBLIC_HEALTH_PATHS
    assert path not in crosswatch._APP_AUTH_SETUP_ALLOWED_PATHS

    seen = client.get(path, headers={mod.TOKEN_HEADER: monitor_raw})
    assert seen.status_code == 200
    assert seen.headers["cache-control"] == "no-store"
    body = seen.json()
    assert body["status"] == "ok"
    assert set(body) == {"ok", "status", "checked_at", "issues", "watcher", "scheduler", "last_run", "providers", "api_tokens_expiring"}
    assert [row["id"] for row in body["api_tokens_expiring"]] == [monitor_entry["id"]]
    assert set(body["api_tokens_expiring"][0]) == {"id", "name", "expires_at"}

    other = client.get(path, headers={mod.TOKEN_HEADER: other_raw})
    assert [row["id"] for row in other.json()["api_tokens_expiring"]] == [other_entry["id"]]

    admin = client.get(path, headers={mod.TOKEN_HEADER: admin_raw})
    assert admin.status_code == 200
    assert [row["name"] for row in admin.json()["api_tokens_expiring"]] == ["other", "uptime"]

    _routes(monkeypatch, R1=True)
    failing = client.get(path, headers={mod.TOKEN_HEADER: monitor_raw})
    assert failing.status_code == 200
    assert failing.json()["status"] == "failing"
    strict = client.get(path, params={"strict": 1}, headers={mod.TOKEN_HEADER: monitor_raw})
    assert strict.status_code == 503
    assert strict.json()["status"] == "failing"

    _routes(monkeypatch, R1=False, R2=True)
    degraded = client.get(path, params={"strict": 1}, headers={mod.TOKEN_HEADER: monitor_raw})
    assert degraded.status_code == 200
    assert degraded.json()["status"] == "degraded"


def test_details_endpoint_reports_a_failed_input_instead_of_ok(gated_app, monkeypatch: pytest.MonkeyPatch) -> None:
    mod = gated_app
    from fastapi.testclient import TestClient

    import crosswatch
    from api import healthAPI
    from cw_platform.config_base import load_config

    def raising() -> Any:
        raise OSError("boom")

    admin_raw, _a = mod.issue_api_token(load_config(), name="admin")
    monkeypatch.setattr(healthAPI, "_sync_state", raising)
    monkeypatch.setattr(healthAPI, "_scheduler_status", raising)

    res = TestClient(crosswatch.app).get("/api/health/details", params={"strict": 1}, headers={mod.TOKEN_HEADER: admin_raw})

    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "degraded"
    assert sorted(issue["component"] for issue in body["issues"]) == ["last_run", "scheduler"]


def test_details_endpoint_stays_closed_until_app_auth_is_set_up(config_base: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    from cw_platform import config_base as cfg_base

    importlib.reload(cfg_base)
    from fastapi.testclient import TestClient

    import crosswatch
    from cw_platform.config_base import load_config

    (config_base / "config.json").write_text(json.dumps({}), encoding="utf-8")
    monkeypatch.setattr(crosswatch, "load_config", load_config)
    client = TestClient(crosswatch.app)

    assert client.get("/api/health").status_code == 200
    assert client.get("/api/health/details").status_code == 403
