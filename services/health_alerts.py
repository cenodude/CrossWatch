# services/health_alerts.py
# CrossWatch - watcher and provider sign-in alerts sent through the scheduler webhooks
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
import threading
from typing import Any, Callable

from _logging import log as BASE_LOG
from services.scheduler_webhooks import LOG_MODULE, debug, normalize_scheduler_webhooks, notify_health_webhook

INTERVAL_S = 60
PROVIDER_EVERY_TICKS = 5
CONFIRM_CHECKS = 2
AUTH_LOST_HINTS = (
    "unauthorized",
    "reconnect",
    "expired",
    "revoked",
    "forbidden",
    "authentication failed",
    "auth error",
    "auth_failed",
    "invalid api key",
    "invalid api token",
    "invalid_api_token",
    "invalid credentials",
    "invalid_credentials",
)
OUTAGE_HINTS = ("network error", "timeout", "timed out")
AUTH_LOST_CODES = re.compile(r"\b(401|403)\b")

Observation = tuple[bool | None, dict[str, Any]]

_worker: threading.Thread | None = None
_worker_lock = threading.Lock()
_stop = threading.Event()
_stop.set()


class StateTracker:
    def __init__(self, confirm_checks: int = CONFIRM_CHECKS) -> None:
        self._confirm = max(1, int(confirm_checks))
        self._streak: dict[str, int] = {}
        self._alerted: dict[str, dict[str, Any]] = {}

    def reset(self) -> None:
        self._streak.clear()
        self._alerted.clear()

    def observe(self, seen: dict[str, Observation]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        for key in [key for key in (*self._streak, *self._alerted) if key not in seen]:
            self._streak.pop(key, None)
            self._alerted.pop(key, None)

        lost: list[dict[str, Any]] = []
        recovered: list[dict[str, Any]] = []
        for key, (bad, item) in seen.items():
            if bad is None:
                continue
            if not bad:
                self._streak.pop(key, None)
                if self._alerted.pop(key, None) is not None:
                    recovered.append(item)
                continue
            self._streak[key] = self._streak.get(key, 0) + 1
            if self._streak[key] >= self._confirm and key not in self._alerted:
                self._alerted[key] = item
                lost.append(item)
        return lost, recovered


def _error(message: str) -> None:
    try:
        BASE_LOG(message, level="ERROR", module=LOG_MODULE)
    except Exception:
        pass


def auth_lost_reason(reason: Any) -> bool:
    text = str(reason or "").strip().lower()
    if any(hint in text for hint in OUTAGE_HINTS):
        return False
    return any(hint in text for hint in AUTH_LOST_HINTS) or bool(AUTH_LOST_CODES.search(text))


def _provider_label(provider: str, instance: str) -> str:
    try:
        from cw_platform.provider_usage import provider_label

        return provider_label(provider, instance)
    except Exception:
        return provider if instance == "default" else f"{provider} {instance}"


def watcher_states(app: Any, cfg: dict[str, Any]) -> dict[str, Observation]:
    from providers.scrobble.routes import normalize_routes, route_is_self_target
    from providers.scrobble.sources import source_enabled
    from providers.scrobble.watch_manager import status

    if not source_enabled(cfg, "watcher"):
        return {}

    running = {str(row.get("id") or ""): bool(row.get("running")) for row in status(app).get("routes") or []}
    out: dict[str, Observation] = {}
    for route in normalize_routes(cfg):
        route_id = str(route.get("id") or "").strip()
        sink = str(route.get("sink") or "").strip().lower()
        if not route_id or not sink or not route.get("enabled") or route_is_self_target(route):
            continue
        provider = str(route.get("provider") or "plex").strip().lower() or "plex"
        instance = str(route.get("provider_instance") or "default").strip() or "default"
        sink_instance = str(route.get("sink_instance") or "default").strip() or "default"
        out[route_id] = (
            not running.get(route_id, False),
            {
                "label": f"Route {route_id}",
                "detail": f"{_provider_label(provider, instance)} to {_provider_label(sink, sink_instance)}",
                "route_id": route_id,
                "provider": provider,
                "provider_instance": instance,
                "sink": sink,
                "sink_instance": sink_instance,
            },
        )
    return out


def used_provider_instances(cfg: dict[str, Any]) -> dict[tuple[str, str], list[str]]:
    from cw_platform.provider_instances import normalize_instance_id
    from providers.scrobble.routes import normalize_routes
    from providers.scrobble.sources import source_enabled
    from providers.webhooks.config import active_webhook_endpoints, webhook_settings, webhook_sink_instance

    used: dict[tuple[str, str], list[str]] = {}

    def add(provider: Any, instance: Any, where: str) -> None:
        if isinstance(provider, dict):
            provider = provider.get("provider") or provider.get("name") or provider.get("id") or provider.get("type")
        code = str(provider or "").strip().upper()
        if not code:
            return
        labels = used.setdefault((code, normalize_instance_id(instance)), [])
        if where not in labels:
            labels.append(where)

    for index, pair in enumerate(cfg.get("pairs") or []):
        if not isinstance(pair, dict) or pair.get("enabled", True) is False:
            continue
        where = f"Sync pair {str(pair.get('id') or pair.get('name') or index + 1).strip()}"
        add(pair.get("source"), pair.get("source_instance") or pair.get("src_instance"), where)
        add(pair.get("target"), pair.get("target_instance") or pair.get("dst_instance"), where)

    if source_enabled(cfg, "watcher"):
        for route in normalize_routes(cfg):
            if not route.get("enabled"):
                continue
            where = f"Watcher route {str(route.get('id') or '').strip()}"
            add(route.get("provider"), route.get("provider_instance"), where)
            add(route.get("sink"), route.get("sink_instance"), where)

    for endpoint in active_webhook_endpoints(cfg):
        source = str(endpoint.get("provider") or "")
        source_instance = normalize_instance_id(endpoint.get("instance"))
        where = f"{_provider_label(source, source_instance)} webhook"
        settings = webhook_settings(cfg, source, source_instance)
        add(source, source_instance, where)
        for sink in endpoint.get("sinks") or []:
            add(sink, webhook_sink_instance(settings, sink), where)
    return used


def probe_provider(cfg: dict[str, Any], provider: str, instance: str) -> tuple[bool, str] | None:
    from api import probesAPI as probes

    fn = probes.DETAIL_PROBES.get(provider)
    if fn is None:
        return None
    return probes._safe_probe_detail(fn, probes._cfg_view_for(cfg, provider, instance), probes.PROBE_TTL)


def provider_states(
    cfg: dict[str, Any],
    probe: Callable[[dict[str, Any], str, str], tuple[bool, str] | None] = probe_provider,
) -> dict[str, Observation]:
    out: dict[str, Observation] = {}
    for (provider, instance), used_by in used_provider_instances(cfg).items():
        try:
            result = probe(cfg, provider, instance)
        except Exception as exc:
            _error(f"Health check {provider} sign-in check failed: {type(exc).__name__}")
            result = (False, "")
        if result is None:
            continue
        ok, reason = result
        bad = False if ok else True if auth_lost_reason(reason) else None
        out[f"{provider}:{instance}"] = (
            bad,
            {
                "label": _provider_label(provider.lower(), instance),
                "detail": "" if ok else str(reason or ""),
                "provider": provider.lower(),
                "instance": instance,
                "used_by": used_by,
            },
        )
    return out


class HealthMonitor:
    def __init__(
        self,
        app: Any,
        load_config: Callable[[], dict[str, Any] | None],
        *,
        notify: Callable[..., bool] = notify_health_webhook,
        probe: Callable[[dict[str, Any], str, str], tuple[bool, str] | None] = probe_provider,
        log_fn: Callable[[str], None] | None = None,
        stopped: Callable[[], bool] = lambda: False,
    ) -> None:
        self._app = app
        self._load_config = load_config
        self._notify = notify
        self._probe = probe
        self._log_fn = log_fn
        self._stopped = stopped
        self._watcher = StateTracker()
        self._providers = StateTracker()
        self._ticks = 0

    def tick(self) -> None:
        cfg = self._load_config() or {}
        sch = cfg.get("scheduling")
        hooks = normalize_scheduler_webhooks(sch.get("webhooks") if isinstance(sch, dict) else None)
        enabled = bool(hooks.get("enabled"))
        provider_due = self._ticks % PROVIDER_EVERY_TICKS == 0
        self._ticks += 1

        if enabled and hooks.get("alert_on_watcher"):
            self._check(
                cfg,
                self._watcher,
                lambda: watcher_states(self._app, cfg),
                "watcher_stopped",
                "watcher_recovered",
            )
        else:
            self._watcher.reset()

        if not (enabled and hooks.get("alert_on_provider_auth")):
            self._providers.reset()
        elif provider_due:
            self._check(
                cfg,
                self._providers,
                lambda: provider_states(cfg, self._probe),
                "provider_auth_lost",
                "provider_auth_recovered",
            )

    def _check(
        self,
        cfg: dict[str, Any],
        tracker: StateTracker,
        collect: Callable[[], dict[str, Observation]],
        lost_event: str,
        recovered_event: str,
    ) -> None:
        try:
            if self._stopped():
                return
            seen = collect()
            lost, recovered = tracker.observe(seen)
            bad = sum(1 for state, _ in seen.values() if state)
            unknown = sum(1 for state, _ in seen.values() if state is None)
            debug(
                f"Health check {lost_event}: {len(seen)} checked, {bad} failing, {unknown} unknown, "
                f"{len(lost)} new, {len(recovered)} recovered"
            )
            for event, items in ((lost_event, lost), (recovered_event, recovered)):
                if items and not self._stopped():
                    self._notify(cfg, event, items, log_fn=self._log_fn)
        except Exception as exc:
            _error(f"Health check {lost_event} failed: {type(exc).__name__}")


def _loop(monitor: HealthMonitor, stop: threading.Event) -> None:
    while not stop.wait(INTERVAL_S):
        try:
            monitor.tick()
        except Exception as exc:
            _error(f"Health alerts worker error: {type(exc).__name__}")


def start_worker(
    app: Any,
    load_config: Callable[[], dict[str, Any] | None],
    *,
    log_fn: Callable[[str], None] | None = None,
) -> bool:
    global _worker, _stop
    with _worker_lock:
        if _worker is not None and _worker.is_alive() and not _stop.is_set():
            return False
        _stop.set()
        _stop = threading.Event()
        monitor = HealthMonitor(app, load_config, log_fn=log_fn, stopped=_stop.is_set)
        _worker = threading.Thread(target=_loop, args=(monitor, _stop), name="health-alerts", daemon=True)
        _worker.start()
        return True


def stop_worker() -> None:
    with _worker_lock:
        _stop.set()
