# providers/scrobble/silo/sink.py
# CrossWatch - Silo Scrobble Sink
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from providers.scrobble._media_server import DeliveryError, MediaServerSink
from providers.sync._mod_SILO import SILOModule
from providers.sync.silo._common import as_int, cfg_section, item_detail, library, resolve_item, safe_json, selected_libraries, silo_request


class SiloSink(MediaServerSink):
    name = "silo"

    def _new_adapter(self, cfg: dict[str, Any]) -> Any:
        return SILOModule({**cfg, "_cw_provider_instance": self._instance_id})

    def _active_elsewhere(self, adapter: Any, content_id: str) -> bool:
        resp = silo_request(adapter, "GET", "/profiles/household/sessions")
        if resp.status_code != 200:
            return False
        data = safe_json(resp)
        rows = data.get("items") if isinstance(data, dict) else None
        profile = str(cfg_section(adapter).get("profile_id") or "")
        return any(isinstance(r, dict) and r.get("content_id") == content_id and str(r.get("profile_id") or "") == profile for r in rows or [])

    def _deliver(self, adapter: Any, item: dict[str, Any], complete: bool, progress: float, destination: dict[str, Any], played_at: int) -> dict[str, Any]:
        allowed = set(destination.get("libraries") or []) or selected_libraries(adapter, "history" if complete else "progress")
        lib = library(adapter, libraries=allowed)
        cid, kind = resolve_item(adapter, lib, item, feature="scrobble")
        if not cid or kind not in ("movie", "episode"):
            raise DeliveryError("unmatched_in_silo")
        detail = item_detail(adapter, cid, feature="scrobble")
        if not detail:
            raise DeliveryError("silo_http_404")
        raw_state = detail.get("user_state")
        state: dict[str, Any] = raw_state if isinstance(raw_state, dict) else {}
        watched = bool(state.get("played"))
        if watched:
            return {"ok": True, "skipped": True, "reason": "destination_already_watched"}
        if self._active_elsewhere(adapter, cid):
            return {"ok": False, "retryable": True, "error": "active_destination_session"}
        raw_data = detail.get("user_data")
        data: dict[str, Any] = raw_data if isinstance(raw_data, dict) else {}
        seconds = as_int(data.get("duration_seconds")) or ((as_int(detail.get("runtime")) or 0) * 60)
        if not seconds or seconds <= 0:
            raise DeliveryError("missing_destination_duration")
        duration_ms = seconds * 1000
        entry: dict[str, Any] = {"media_item_id": cid, "duration_ms": duration_ms}
        if complete:
            entry["position_ms"] = duration_ms
            if time.time() - played_at > 120:
                entry["updated_at"] = datetime.fromtimestamp(played_at, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            entry["position_ms"] = int(round(duration_ms * progress / 100.0))
            if entry["position_ms"] <= 0:
                return {"ok": True, "skipped": True, "reason": "no_progress"}
        resp = silo_request(adapter, "POST", "/sync/progress", json_body={"items": [entry]})
        if resp.status_code != 200:
            raise DeliveryError(f"silo_http_{resp.status_code}")
        body = safe_json(resp)
        results = body.get("items") if isinstance(body, dict) else None
        if not isinstance(results, list) or not results or not isinstance(results[0], dict) or results[0].get("status") != "success":
            raise DeliveryError("silo_progress_write_failed")
        if complete:
            check = item_detail(adapter, cid, feature="scrobble")
            check_state = check.get("user_state")
            if not (isinstance(check_state, dict) and check_state.get("played")):
                return {"ok": True, "skipped": True, "reason": "ignored_by_server"}
        return {"ok": True}
