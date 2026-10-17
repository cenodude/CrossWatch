# services/health_details.py
# CrossWatch - combined watcher, scheduler, sync and provider health for monitoring tools
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Any, Callable

from services import health_alerts

OVERDUE_GRACE_S = 300
TOKEN_EXPIRY_WINDOW_S = 14 * 86400
PROBE_DEADLINE_S = 10
MAX_PROBE_WORKERS = 8
SLOW_WRITER_S = 30
BUSY_NOTICE_S = 10
LEVELS = ("ok", "degraded", "failing")

Issues = list[dict[str, str]]
ProbeResult = tuple[bool, str] | None
Probe = Callable[[dict[str, Any], str, str], ProbeResult]

_POOL = ThreadPoolExecutor(max_workers=MAX_PROBE_WORKERS, thread_name_prefix="health-probe")
FlightKey = tuple[str, str, str, Probe]

_IN_FLIGHT: dict[FlightKey, Future[ProbeResult]] = {}
_IN_FLIGHT_LOCK = threading.Lock()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _add(issues: Issues, level: str, component: str, message: str) -> None:
    issues.append({"level": level, "component": component, "message": message})


def _run_probe(cfg: dict[str, Any], probe: Probe, key: tuple[str, str]) -> ProbeResult:
    try:
        return probe(cfg, key[0], key[1])
    except Exception as exc:
        return False, f"probe failed: {type(exc).__name__}"


def _credential_tag(cfg: dict[str, Any], provider: str, instance: str) -> str:
    try:
        from api import probesAPI as probes

        return probes._probe_key(probes._cfg_key(provider), probes._cfg_view_for(cfg, provider, instance))
    except Exception:
        return ""


def _forget(flight: FlightKey, future: Future[ProbeResult]) -> None:
    with _IN_FLIGHT_LOCK:
        if _IN_FLIGHT.get(flight) is future:
            _IN_FLIGHT.pop(flight, None)


def _submit(cfg: dict[str, Any], probe: Probe, key: tuple[str, str]) -> Future[ProbeResult]:
    flight: FlightKey = (key[0], key[1], _credential_tag(cfg, key[0], key[1]), probe)
    with _IN_FLIGHT_LOCK:
        running = _IN_FLIGHT.get(flight)
        if running is not None:
            return running
        future = _POOL.submit(_run_probe, cfg, probe, key)
        _IN_FLIGHT[flight] = future
    future.add_done_callback(lambda done: _forget(flight, done))
    return future


def watcher_health(app: Any, cfg: dict[str, Any], issues: Issues) -> dict[str, Any]:
    states = health_alerts.watcher_states(app, cfg)
    down = sorted(route_id for route_id, (bad, _item) in states.items() if bad)
    alive = len(down) < len(states)
    if states and not alive:
        _add(issues, "failing", "watcher", "Watcher is not running")
    elif down:
        _add(issues, "degraded", "watcher", f"Watcher routes not running: {', '.join(down)}")
    return {"enabled": bool(states), "alive": alive, "routes": len(states), "routes_not_running": down}


def scheduler_health(
    cfg: dict[str, Any],
    status: dict[str, Any],
    sync_running: bool,
    now: int,
    issues: Issues,
) -> dict[str, Any]:
    sch = _as_dict(cfg.get("scheduling"))
    enabled = bool(sch.get("enabled") or _as_dict(sch.get("advanced")).get("enabled"))
    running = bool(status.get("running"))
    working = bool(sync_running or status.get("busy"))
    next_run_at = int(status.get("next_run_at") or 0)
    last_run_ok = status.get("last_run_ok") if isinstance(status.get("last_run_ok"), bool) else None
    last_error = str(status.get("last_error") or "")
    overdue = bool(enabled and running and next_run_at and not working and now - next_run_at > OVERDUE_GRACE_S)
    if enabled and not running:
        _add(issues, "failing", "scheduler", "Scheduler is enabled but not running")
    elif overdue:
        _add(issues, "degraded", "scheduler", "Scheduled run is overdue")
    if enabled and last_run_ok is False:
        _add(issues, "degraded", "scheduler", f"Last scheduled run failed: {last_error}" if last_error else "Last scheduled run failed")
    return {
        "enabled": enabled,
        "running": running,
        "last_run_ok": last_run_ok,
        "last_run_at": int(status.get("last_run_at") or 0),
        "last_error": last_error,
        "next_run_at": next_run_at,
        "overdue": overdue,
    }


def last_run_health(last_run: dict[str, Any], issues: Issues) -> dict[str, Any]:
    raw_code = last_run.get("exit_code")
    try:
        exit_code = None if raw_code in (None, "") else int(raw_code)
    except Exception:
        exit_code = None
    if exit_code not in (None, 0):
        _add(issues, "degraded", "last_run", f"Last sync run failed with exit code {exit_code}")
    return {
        "running": bool(last_run.get("running")),
        "finished_at": int(last_run.get("finished_at") or 0),
        "exit_code": exit_code,
    }


def provider_health(cfg: dict[str, Any], probe: Probe, issues: Issues) -> list[dict[str, Any]]:
    used = health_alerts.used_provider_instances(cfg)
    scrobble = set(health_alerts.used_provider_instances({**cfg, "pairs": []}))
    keys = sorted(used)
    futures = [_submit(cfg, probe, key) for key in keys]
    wait(futures, timeout=PROBE_DEADLINE_S)

    rows: list[dict[str, Any]] = []
    for key, future in zip(keys, futures):
        provider, instance = key
        label = health_alerts._provider_label(provider.lower(), instance)
        row: dict[str, Any] = {"provider": provider, "instance": instance, "used_by": list(used[key])}
        if not future.done():
            _add(issues, "degraded", "providers", f"{label} check timed out")
            rows.append({**row, "connected": None, "reauth_required": False, "reason": "health check timed out"})
            continue
        result = future.result()
        if result is None:
            continue
        ok, reason = bool(result[0]), str(result[1] or "")
        reauth = not ok and health_alerts.auth_lost_reason(reason)
        if not ok:
            _add(
                issues,
                "failing" if reauth and key in scrobble else "degraded",
                "providers",
                f"{label} needs a new sign-in" if reauth else f"{label} is not connected",
            )
        rows.append({**row, "connected": ok, "reauth_required": reauth, "reason": "" if ok else reason})
    return rows


def database_health(
    issues: Issues,
    storage: Callable[[Any], dict[str, Any]] | None = None,
    writers: Callable[[], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    from cw_platform.local_db.db import active_writers, crosswatch_db_path
    from cw_platform.local_db.storage import storage_info

    path = crosswatch_db_path()
    info = (storage or storage_info)(path)
    held = (writers or active_writers)()
    if info.get("network"):
        _add(issues, "degraded", "database", f"Database is on a network filesystem ({info.get('filesystem')}); SQLite locking is unreliable there")
    if held and float(held[0].get("seconds") or 0) >= SLOW_WRITER_S:
        _add(issues, "degraded", "database", f"Database write held by {held[0].get('operation')} for {int(held[0].get('seconds') or 0)}s")
    wal = path.with_name(path.name + "-wal")
    return {
        "path": str(path),
        "size_bytes": path.stat().st_size if path.exists() else 0,
        "wal_size_bytes": wal.stat().st_size if wal.exists() else 0,
        "filesystem": info.get("filesystem"),
        "network_filesystem": bool(info.get("network")),
        "active_writers": held,
    }


def database_status(
    storage: Callable[[Any], dict[str, Any]] | None = None,
    writers: Callable[[], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    from cw_platform.local_db.db import active_writers, crosswatch_db_path
    from cw_platform.local_db.storage import storage_info

    info = (storage or storage_info)(crosswatch_db_path())
    held = (writers or active_writers)()
    top = held[0] if held and float(held[0].get("seconds") or 0) >= BUSY_NOTICE_S else None
    return {
        "busy": top is not None,
        "operation": top.get("operation") if top else None,
        "seconds": int(top.get("seconds") or 0) if top else 0,
        "since": top.get("since") if top else None,
        "filesystem": info.get("filesystem"),
        "network_filesystem": bool(info.get("network")),
    }


def expiring_tokens(tokens: list[dict[str, Any]], now: int) -> list[dict[str, Any]]:
    rows = [
        {"id": str(token.get("id") or ""), "name": str(token.get("name") or ""), "expires_at": int(token.get("expires_at") or 0)}
        for token in tokens
        if isinstance(token, dict) and 0 < int(token.get("expires_at") or 0) <= now + TOKEN_EXPIRY_WINDOW_S
    ]
    return sorted(rows, key=lambda row: row["expires_at"])


def _checked(issues: Issues, component: str, fallback: Any, check: Callable[[], Any]) -> Any:
    try:
        return check()
    except Exception as exc:
        _add(issues, "degraded", component, f"Health check failed: {type(exc).__name__}")
        return fallback


def health_details(
    app: Any,
    cfg: dict[str, Any],
    *,
    scheduler_status: Callable[[], dict[str, Any]],
    sync_state: Callable[[], tuple[bool, dict[str, Any]]],
    tokens: Callable[[], list[dict[str, Any]]],
    probe: Probe | None = None,
    now: int | None = None,
) -> dict[str, Any]:
    now_ts = int(time.time() if now is None else now)
    issues: Issues = []
    watcher = _checked(issues, "watcher", {}, lambda: watcher_health(app, cfg, issues))
    sync_running, last_run = _checked(issues, "last_run", (False, None), sync_state)
    run = {} if last_run is None else _checked(issues, "last_run", {}, lambda: last_run_health(_as_dict(last_run), issues))
    scheduler = _checked(
        issues,
        "scheduler",
        {},
        lambda: scheduler_health(cfg, _as_dict(scheduler_status()), bool(sync_running), now_ts, issues),
    )
    providers = _checked(issues, "providers", [], lambda: provider_health(cfg, probe or health_alerts.probe_provider, issues))
    expiring = _checked(issues, "api_tokens", [], lambda: expiring_tokens(tokens(), now_ts))
    database = _checked(issues, "database", {}, lambda: database_health(issues))
    status = max((issue["level"] for issue in issues), key=LEVELS.index, default="ok")
    return {
        "ok": status == "ok",
        "status": status,
        "checked_at": now_ts,
        "issues": issues,
        "watcher": watcher,
        "scheduler": scheduler,
        "last_run": run,
        "providers": providers,
        "api_tokens_expiring": expiring,
        "database": database,
    }
