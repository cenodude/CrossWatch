# providers/scrobble/_media_server.py
# CrossWatch - Media Server Scrobble Delivery
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import math
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from typing import Any

from cw_platform.event_archive.scrobble_recorder import record_watch
from cw_platform.provider_instances import normalize_instance_id
from providers.scrobble._auto_remove_watchlist import remove_across_providers_by_ids
from providers.scrobble._episode_ids import PUBLIC_IDS, episode_ids
from providers.scrobble._watched_gate import resolve_stop_action
from providers.scrobble.routes import same_scrobble_endpoint, scrobble_sink_config
from providers.scrobble.scrobble import ScrobbleEvent
from services.activity import record_scrobble_event

CACHE_TTL = 900
CACHE_MAX = 1024


def event_item(event: ScrobbleEvent) -> dict[str, Any]:
    ids = {key: str(event.ids[key]) for key in PUBLIC_IDS if event.ids.get(key)}
    show_ids = {key: str(event.ids[f"{key}_show"]) for key in PUBLIC_IDS if event.ids.get(f"{key}_show")}
    if event.media_type == "episode":
        return {"type": "episode", "ids": episode_ids(event.ids, show_ids), "show_ids": show_ids,
                "series_title": event.title, "season": event.season, "episode": event.number}
    return {"type": "movie", "ids": ids, "title": event.title, "year": event.year}


def ids_match(wanted: dict, actual: dict) -> bool:
    common = set(wanted) & set(actual) & set(PUBLIC_IDS)
    return bool(common) and all(str(wanted[key]) == str(actual[key]) for key in common)


def auto_remove(event: ScrobbleEvent, cfg: dict[str, Any], provider: str, instance: str) -> None:
    sc = cfg.get("scrobble") or {}
    mode = str(((sc.get("watch") or {}).get("route_options") or {}).get("auto_remove_watchlist") or "inherit").lower()
    if mode == "off" or (mode != "on" and not sc.get("delete_plex")):
        return
    types = sc.get("delete_plex_types") or []
    types = [types] if isinstance(types, str) else types
    if event.media_type not in {str(t).lower().rstrip("s") for t in types}:
        return
    ids = event_item(event).get("ids") or {}
    if ids:
        remove_across_providers_by_ids(ids, event.media_type, scope=f"{provider}:{instance}")


class DeliveryError(RuntimeError):
    pass


class MediaServerSink:
    name = ""

    def __init__(self, cfg_provider: Callable[[], dict[str, Any]] | None = None, instance_id: str | None = None) -> None:
        self._cfg_provider = cfg_provider
        self._instance_id = normalize_instance_id(instance_id)
        self._lock = threading.RLock()
        self._adapter: Any = None
        self._identity = ""
        self._connected_at = 0.0
        self._resolved: OrderedDict[str, tuple[float, str]] = OrderedDict()
        self._sent: OrderedDict[str, tuple[float, str, float]] = OrderedDict()
        self._catalogs: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self._misses: OrderedDict[str, tuple[float, str]] = OrderedDict()

    def _new_adapter(self, cfg: dict[str, Any]) -> Any:
        raise NotImplementedError

    def _deliver(self, adapter: Any, item: dict[str, Any], complete: bool, progress: float) -> dict[str, Any]:
        raise NotImplementedError

    def _delivery_context(self, adapter: Any) -> Any:
        return None

    def _connect(self, cfg: dict[str, Any]) -> Any:
        identity = hashlib.sha256(json.dumps(cfg.get(self.name) or {}, sort_keys=True, default=str).encode()).hexdigest()
        if self._adapter is None or identity != self._identity or time.monotonic() - self._connected_at >= CACHE_TTL:
            old_adapter = self._adapter
            self._adapter = None
            if old_adapter is not None:
                try:
                    session = getattr(getattr(old_adapter, "client", None), "session", None)
                    if session is not None:
                        session.close()
                except Exception:
                    pass
            self._resolved.clear()
            if identity != self._identity:
                self._sent.clear()
                self._misses.clear()
                self._catalogs.clear()
            self._adapter = self._new_adapter(cfg)
            self._adapter.instance_id = self._instance_id
            self._identity = identity
            self._connected_at = time.monotonic()
        return self._adapter

    def _catalog(self, scope: Any, load: Callable) -> Any:
        key = json.dumps(scope, sort_keys=True)
        cached = self._catalogs.get(key)
        if cached and time.monotonic() - cached[0] < 21600:
            self._catalogs.move_to_end(key)
            return cached[1]
        rows = load()
        self._catalogs[key] = (time.monotonic(), rows)
        if len(self._catalogs) > 8:
            self._catalogs.popitem(last=False)
        return rows

    def _cached_target(self, item: dict, scope: Any, *, fetch: Callable, matches: Callable, resolve: Callable, key_of: Callable) -> Any:
        key = json.dumps([item, scope], sort_keys=True)
        now = time.monotonic()
        miss = self._misses.get(key)
        if miss and now - miss[0] < 300:
            self._misses.move_to_end(key)
            raise DeliveryError(miss[1])
        self._misses.pop(key, None)
        cached = self._resolved.get(key)
        if cached and now - cached[0] < CACHE_TTL:
            try:
                row = fetch(cached[1])
                if matches(row):
                    return row
            except Exception:
                pass
            self._resolved.pop(key, None)
        try:
            row = resolve()
        except DeliveryError as exc:
            if str(exc).startswith("unmatched_in_") or str(exc) == "ambiguous_ids":
                self._misses[key] = (now, str(exc))
                if len(self._misses) > CACHE_MAX:
                    self._misses.popitem(last=False)
            raise
        self._resolved[key] = (now, str(key_of(row)))
        if len(self._resolved) > CACHE_MAX:
            self._resolved.popitem(last=False)
        return row

    def send(self, event: ScrobbleEvent, cfg: dict[str, Any] | None = None) -> dict[str, Any]:
        cfg = cfg if isinstance(cfg, dict) else (self._cfg_provider() if self._cfg_provider else {})
        watch = ((cfg.get("scrobble") or {}).get("watch") or {})
        source = str(watch.get("route_provider") or "").lower()
        source_instance = normalize_instance_id(watch.get("route_provider_instance"))
        if same_scrobble_endpoint(source, source_instance, self.name, self._instance_id):
            return {"ok": False, "skipped": True, "error": "same_source_destination"}
        try:
            progress = float(event.progress)
            if event.action not in {"start", "pause", "stop"} or event.media_type not in {"movie", "episode"} or not math.isfinite(progress):
                raise ValueError
        except (ValueError, TypeError):
            return {"ok": False, "skipped": True, "error": "invalid_event"}
        item = event_item(event)
        if not item.get("ids") and not item.get("show_ids"):
            return {"ok": False, "skipped": True, "error": "missing_external_ids"}
        from providers.webhooks.config import sink_configured
        if not sink_configured(cfg, self.name, self._instance_id):
            return {"ok": False, "skipped": True, "error": "not_configured"}
        view = scrobble_sink_config(cfg, self.name, self._instance_id)
        policy = ((watch.get("route_options") or {}).get("scrobble") or {})
        defaults = ((cfg.get("scrobble") or {}).get("trakt") or {})
        threshold = float(policy.get("watched_at", defaults.get("watched_at", 90)))
        step = float(policy.get("progress_step", defaults.get("progress_step", 25)))
        progress = max(0.0, min(100.0, progress))
        action = resolve_stop_action(progress, threshold) if event.action == "stop" else event.action
        complete = action == "stop"
        session = json.dumps([source, source_instance, event.server_uuid, event.account, event.session_key, item], sort_keys=True)
        record = {"source_provider": source, "source_instance": source_instance,
                  "destination_provider": self.name, "destination_instance": self._instance_id,
                  "action": action, "progress": progress}
        with self._lock:
            try:
                adapter = self._connect(view)
                session = json.dumps([session, self._delivery_context(adapter)])
                now = time.monotonic()
                sent = self._sent.get(session)
                if sent and now - sent[0] < CACHE_TTL:
                    if sent[1] == "complete":
                        return {"ok": True, "skipped": True, "reason": "duplicate"}
                    if event.action == "start" and sent[1] == "start" and abs(progress - sent[2]) < step:
                        reason = "duplicate" if progress == sent[2] else "progress_step_not_reached"
                        return {"ok": True, "skipped": True, "reason": reason}
                result = self._deliver(adapter, item, complete, progress)
                if not result.get("ok") or result.get("skipped"):
                    return result
                self._sent[session] = (now, "complete" if complete else event.action, progress)
                self._sent.move_to_end(session)
                if len(self._sent) > CACHE_MAX:
                    self._sent.popitem(last=False)
            except Exception as exc:
                reason = str(exc) if isinstance(exc, DeliveryError) else f"{self.name}_scrobble_failed"
                record_watch(event, **record, status="fail", reason=reason)
                retryable = not (reason.startswith("unmatched_in_") or reason in {
                    "ambiguous_ids", "missing_destination_duration", "jellyfin_progress_write_unsupported",
                    "home_scope_not_applied",
                } or reason.endswith(("_http_400", "_http_401", "_http_403")))
                return {"ok": False, "error": reason, "retryable": retryable}
            record_watch(event, **record)
            if complete:
                try:
                    record_scrobble_event(event, source=source, source_instance=source_instance,
                                          target=self.name, target_instance=self._instance_id, progress=progress)
                except Exception:
                    pass
                try:
                    auto_remove(event, cfg, self.name, self._instance_id)
                except Exception:
                    pass
            return result
