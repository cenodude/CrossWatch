# providers/scrobble/kitsu/sink.py
# CrossWatch - Kitsu watched anime scrobble sink
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import math
from typing import Any
from cw_platform.event_archive import record_watch
from cw_platform.provider_instances import build_provider_config_view, normalize_instance_id
from providers.scrobble._watched_gate import resolve_stop_action
from providers.scrobble.anime_mapping import _event_item
from providers.scrobble.scrobble import ScrobbleEvent, ScrobbleSink
from providers.sync._mod_KITSU import KITSUModule, OPS
from services.activity import record_scrobble_event


class KitsuSink(ScrobbleSink):
    def __init__(self, cfg_provider: Any = None, instance_id: str | None = None) -> None:
        self._cfg_provider = cfg_provider
        self._instance_id = normalize_instance_id(instance_id)

    def send(self, ev: ScrobbleEvent, cfg: dict[str, Any] | None = None) -> dict[str, Any]:
        cfg = cfg if isinstance(cfg, dict) else (self._cfg_provider() if self._cfg_provider else {})
        if not isinstance(cfg, dict):
            cfg = {}
        view = build_provider_config_view(cfg, "kitsu", self._instance_id)
        view["_cw_provider_instance"] = self._instance_id
        if not OPS.is_configured(view):
            return {"ok": True, "skipped": True, "reason": "not_configured"}
        scrobble = cfg.get("scrobble") or {}
        try:
            threshold = float((scrobble.get("trakt") or {}).get("watched_at", 90))
            progress = float(ev.progress)
        except (TypeError, ValueError):
            return {"ok": True, "skipped": True, "reason": "invalid_progress"}
        if not math.isfinite(progress) or not 0 <= progress <= 100:
            return {"ok": True, "skipped": True, "reason": "invalid_progress"}
        if ev.action != "stop" or resolve_stop_action(progress, threshold) != "stop":
            return {"ok": True, "skipped": True, "reason": "not_watched"}
        item = _event_item(ev)
        if item is None:
            return {"ok": True, "skipped": True, "reason": "missing_media_identity"}
        result = KITSUModule(view).add("history", [item])
        unresolved = result.get("unresolved") or []
        if unresolved:
            reason = str(unresolved[0].get("reason") or "request_failed")
            if reason.startswith("request_failed"):
                return {"ok": False, "error": reason, "retryable": "KitsuAuthError" not in reason}
            return {"ok": True, "skipped": True, "reason": reason}
        watch = scrobble.get("watch") or {}
        source = str(watch.get("route_provider") or "watcher")
        source_instance = str(watch.get("route_provider_instance") or "default")
        record_watch(ev, action="stop", source_provider=source, source_instance=source_instance,
                     destination_provider="kitsu", destination_instance=self._instance_id, progress=ev.progress)
        try:
            record_scrobble_event(ev, source=source, source_instance=source_instance,
                                  target="kitsu", target_instance=self._instance_id, progress=ev.progress)
        except Exception:
            pass
        return {"ok": True}
