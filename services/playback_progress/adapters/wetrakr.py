# services/playback_progress/adapters/wetrakr.py
# CrossWatch - WeTrakr Playback Progress Adapter
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Mapping

from providers.sync._mod_WETRAKR import OPS, WETRAKRModule
from providers.sync.wetrakr._common import WeTrakrSyncError
from providers.sync.wetrakr._progress import number
from ..models import PlaybackActionResult, PlaybackCapabilities, PlaybackListResult, PlaybackRecord, utc_now_iso
from .base import PlaybackProgressAdapter, public_failure


class WeTrakrPlaybackAdapter(PlaybackProgressAdapter):
    provider = "wetrakr"
    provider_label = "WeTrakr"
    ops = OPS

    def capabilities(self, config_view: Mapping[str, Any], *, instance_id: str, instance_label: str) -> PlaybackCapabilities:
        try:
            configured = OPS.is_configured({**config_view, "_cw_provider_instance": instance_id})
        except Exception:
            configured = False
        return PlaybackCapabilities(
            provider=self.provider, provider_label=self.provider_label, instance_id=instance_id, instance_label=instance_label,
            configured=configured, read=configured, mark_watched=configured, update_progress=configured,
            remove_progress=configured, bulk_remove_progress=configured,
            bulk_mark_watched=configured, bulk_update_progress=configured, supports_movies=configured, supports_episodes=configured,
            reason="" if configured else "WeTrakr is not connected for this profile.")

    def list_progress(self, config_view: Mapping[str, Any], *, instance_id: str, instance_label: str,
                      force_refresh: bool = False) -> PlaybackListResult:
        try:
            index = WETRAKRModule(config_view, instance_id).build_index("progress", force_refresh=force_refresh)
            items = []
            for key, item in index.items():
                duration_ms = number(item.get("duration_ms"))
                duration = duration_ms / 1000 if duration_ms else None
                active = item.get("_wetrakr_playback_status") == "playing"
                items.append(PlaybackRecord(
                    provider=self.provider, provider_label=self.provider_label, instance_id=instance_id, instance_label=instance_label,
                    remote_id=str(item["ids"].get("wetrakr") or key), canonical_key=key, media_type=item["type"],
                    title=str(item.get("series_title") or item.get("title") or ""),
                    episode_title=str(item.get("title") or "") if item["type"] == "episode" else "",
                    series_title=str(item.get("series_title") or ""), season=item.get("season"), episode=item.get("episode"),
                    year=item.get("year"), ids=dict(item["ids"]), progress_percent=item["progress_percent"],
                    duration_seconds=round(duration) if duration else None,
                    remaining_seconds=max(0, round(duration * (100 - item["progress_percent"]) / 100)) if duration else None,
                    progress_at=item.get("progress_at"), updated_at=item.get("progress_at"),
                    can_mark_watched=True, can_update_progress=not active, can_remove_progress=not active,
                    capability_messages=["Playback is active."] if active else [],
                    provider_metadata={"history_item": dict(item), "show_ids": item.get("show_ids", {})}))
            return PlaybackListResult(ok=True, provider=self.provider, instance_id=instance_id, items=items, refreshed_at=utc_now_iso())
        except WeTrakrSyncError as exc:
            return PlaybackListResult(ok=False, provider=self.provider, instance_id=instance_id, error_code=exc.reason,
                                      message="WeTrakr playback request failed.", remote_status=exc.status_code or None,
                                      retryable=exc.status_code in (429, 500, 502, 503, 504) or bool(exc.retry_after))

    def remove_progress(self, config_view: Mapping[str, Any], record: Mapping[str, Any], *, instance_id: str,
                        instance_label: str) -> PlaybackActionResult:
        return self._apply(config_view, record, instance_id=instance_id, operation="remove_progress")

    def _apply(self, config_view: Mapping[str, Any], record: Mapping[str, Any], *, instance_id: str,
               operation: str, progress_percent: float | None = None, watched_at: str | None = None) -> PlaybackActionResult:
        metadata = record.get("provider_metadata")
        source = metadata.get("history_item") if isinstance(metadata, Mapping) else None
        if not isinstance(source, Mapping):
            return public_failure(provider=self.provider, instance_id=instance_id, operation=operation,
                                  error_code="missing_media", message="Missing WeTrakr playback media.")
        item = dict(source)
        if operation == "update_progress":
            percent, duration = number(progress_percent), number(item.get("duration_ms"))
            if percent is None or not 0 < percent < 100:
                return public_failure(provider=self.provider, instance_id=instance_id, operation=operation,
                                      error_code="invalid_progress", message="A valid playback position is required.")
            item.update(progress_percent=percent, progress_at=utc_now_iso())
            if duration is not None and duration > 0:
                item["progress_ms"] = round(duration * percent / 100)
        elif operation == "mark_watched":
            item["watched_at"] = watched_at or utc_now_iso()
        try:
            module = WETRAKRModule(config_view, instance_id)
            result = (module.remove("progress", [item]) if operation == "remove_progress"
                      else module.add("progress" if operation == "update_progress" else "history", [item]))
            skipped = next((row for row in result.get("results", []) if row.get("status") == "skipped"), None)
            ok = bool(result.get("ok"))
            return PlaybackActionResult(
                ok=ok, provider=self.provider, instance_id=instance_id, operation=operation,
                remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""),
                status="skipped" if skipped else "applied" if ok else "failed", reason=skipped.get("reason", "") if skipped else "",
                message="Playback update skipped." if skipped else "WeTrakr updated." if ok else "WeTrakr update could not be verified.",
                error_code="" if ok else "write_not_verified", history_result=result if operation == "mark_watched" else None)
        except WeTrakrSyncError as exc:
            return public_failure(provider=self.provider, instance_id=instance_id, operation=operation,
                                  error_code=exc.reason, message="WeTrakr playback update failed.")

    def mark_watched(self, config_view: Mapping[str, Any], record: Mapping[str, Any], *, instance_id: str,
                     instance_label: str, watched_at: str | None = None) -> PlaybackActionResult:
        return self._apply(config_view, record, instance_id=instance_id, operation="mark_watched", watched_at=watched_at)

    def update_progress(self, config_view: Mapping[str, Any], record: Mapping[str, Any], progress_percent: float, *,
                        instance_id: str, instance_label: str) -> PlaybackActionResult:
        return self._apply(config_view, record, instance_id=instance_id, operation="update_progress", progress_percent=progress_percent)
