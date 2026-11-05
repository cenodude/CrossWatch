# providers/scrobble/tofa/sink.py
# CrossWatch - Tofa Scrobble Sink
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Mapping
from urllib.parse import quote

from cw_platform.played_writes import note_write
from providers.scrobble._media_server import DeliveryError, MediaServerSink
from providers.scrobble.scrobble import _log
from providers.sync._mod_TOFA import TOFAModule
from providers.sync.tofa._common import (
    MIN_POSITION_MS,
    as_int,
    cfg_section,
    default_edition_files,
    durations,
    library,
    resolve_item,
    safe_json,
    selected_libraries,
    tofa_request,
)


class TofaSink(MediaServerSink):
    name = "tofa"

    def _new_adapter(self, cfg: dict[str, Any]) -> Any:
        return TOFAModule({**cfg, "_cw_provider_instance": self._instance_id})

    def _progress_row(self, adapter: Any, file_id: str) -> Mapping[str, Any]:
        resp = tofa_request(adapter, "GET", f"/media/{quote(file_id, safe='')}/progress")
        if resp.status_code != 200:
            raise DeliveryError(f"tofa_http_{resp.status_code}")
        if not (resp.text or "").strip() or resp.text.strip() == "null":
            return {"completed": False}
        data = safe_json(resp)
        if not isinstance(data, Mapping) or "completed" not in data or not isinstance(data.get("completed"), (bool, type(None))):
            raise DeliveryError("tofa_invalid_progress")
        return {**data, "completed": bool(data.get("completed"))}

    def _active_elsewhere(self, adapter: Any, files: list[str]) -> bool:
        resp = tofa_request(adapter, "GET", "/system/playback/sessions")
        if resp.status_code != 200:
            return False
        rows = safe_json(resp)
        user = str(cfg_section(adapter).get("user_id") or "")
        wanted = set(files)
        return any(isinstance(r, Mapping) and str(r.get("media_file_id") or "") in wanted and (not user or str(r.get("user_id") or "") == user)
                   for r in rows or [])

    def _deliver(self, adapter: Any, item: dict[str, Any], complete: bool, progress: float, destination: dict[str, Any], played_at: int) -> dict[str, Any]:
        allowed = set(destination.get("libraries") or []) or selected_libraries(adapter, "history" if complete else "progress")
        lib = library(adapter, libraries=allowed)
        target, kind = resolve_item(lib, item)
        files = default_edition_files(lib, kind, target) if target and kind in ("movie", "episode") else []
        if not files:
            raise DeliveryError("unmatched_in_tofa")
        done = {fid for fid in files if self._progress_row(adapter, fid).get("completed")}
        if len(done) == len(files) or (done and not complete):
            return {"ok": True, "skipped": True, "reason": "destination_already_watched"}
        if self._active_elsewhere(adapter, files):
            return {"ok": False, "retryable": True, "error": "active_destination_session"}
        profile = cfg_section(adapter).get("profile_id")
        if complete:
            todo = [fid for fid in files if fid not in done]
            entries: list[dict[str, Any]] = [{"media_file_id": fid, "watched": True} for fid in todo]
            if time.time() - played_at > 120:
                stamp = datetime.fromtimestamp(played_at, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                for entry in entries:
                    entry["watched_at"] = stamp
            for fid in todo:
                note_write("tofa", profile, fid)
            resp = tofa_request(adapter, "POST", "/media/watched/batch", json_body={"items": entries})
            if resp.status_code != 200:
                raise DeliveryError(f"tofa_http_{resp.status_code}")
            if not all(self._progress_row(adapter, fid).get("completed") for fid in todo):
                _log(f"tofa watched write not confirmed file={todo[0]}", "DEBUG")
                return {"ok": True, "skipped": True, "reason": "ignored_by_server"}
            _log(f"tofa marked watched file={todo[0]} files={len(todo)} watched_at={entries[0].get('watched_at') or 'now'}", "DEBUG")
            return {"ok": True}
        duration_ms = durations(adapter, lib, [(kind, target)], feature="scrobble").get(files[0])
        if not duration_ms:
            raise DeliveryError("missing_destination_duration")
        position_ms = int(round(duration_ms * progress / 100.0))
        if position_ms < MIN_POSITION_MS:
            return {"ok": True, "skipped": True, "reason": "below_server_minimum"}
        note_write("tofa", profile, files[0])
        resp = tofa_request(adapter, "PUT", f"/media/{quote(files[0], safe='')}/progress", json_body={"position_ms": position_ms})
        if resp.status_code != 200:
            raise DeliveryError(f"tofa_http_{resp.status_code}")
        body = safe_json(resp)
        got = as_int(body.get("position_ms")) if isinstance(body, Mapping) else None
        _log(f"tofa resume written file={files[0]} position_ms={position_ms} stored={got}", "DEBUG")
        return {"ok": True}
