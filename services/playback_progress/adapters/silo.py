# services/playback_progress/adapters/silo.py
# CrossWatch - Silo Playback Progress Adapter
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Mapping

from providers.auth._auth_SILO import SiloError
from providers.sync._mod_SILO import OPS, SILOModule
from providers.sync.silo._common import SiloSyncError, as_int, cfg_section, library, resolve_item, safe_json, silo_request
from ..models import PlaybackActionResult, PlaybackCapabilities, PlaybackListResult, PlaybackRecord, utc_now_iso
from .base import PlaybackProgressAdapter, public_failure


class SiloPlaybackAdapter(PlaybackProgressAdapter):
    provider = "silo"
    provider_label = "Silo"
    ops = OPS

    def _module(self, config_view: Mapping[str, Any], instance_id: str) -> SILOModule:
        return SILOModule({**config_view, "_cw_provider_instance": instance_id})

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
            reason="" if configured else "Silo is not connected for this profile.")

    def _playing(self, module: SILOModule) -> set[str]:
        try:
            resp = silo_request(module, "GET", "/profiles/household/sessions")
        except SiloError:
            return set()
        if resp.status_code != 200:
            return set()
        data = safe_json(resp)
        rows = data.get("items") if isinstance(data, Mapping) else None
        profile = str(cfg_section(module).get("profile_id") or "")
        return {str(r.get("content_id") or "") for r in rows or [] if isinstance(r, Mapping) and str(r.get("profile_id") or "") == profile}

    def list_progress(self, config_view: Mapping[str, Any], *, instance_id: str, instance_label: str,
                      force_refresh: bool = False) -> PlaybackListResult:
        try:
            module = self._module(config_view, instance_id)
            index = module.build_index("progress")
            lib = library(module)
            playing = self._playing(module)
            items = []
            for key, item in index.items():
                cid, _kind = resolve_item(module, lib, item, feature="progress")
                duration_ms = as_int(item.get("duration_ms"))
                duration = duration_ms / 1000 if duration_ms else None
                percent = float(item.get("progress_percent") or 0.0)
                active = bool(cid and cid in playing)
                items.append(PlaybackRecord(
                    provider=self.provider, provider_label=self.provider_label, instance_id=instance_id, instance_label=instance_label,
                    remote_id=cid or key, canonical_key=key, media_type=item["type"],
                    title=str(item.get("series_title") or item.get("title") or ""),
                    episode_title=str(item.get("title") or "") if item["type"] == "episode" else "",
                    series_title=str(item.get("series_title") or ""), season=item.get("season"), episode=item.get("episode"),
                    year=item.get("year"), ids=dict(item.get("ids") or {}), progress_percent=percent,
                    duration_seconds=round(duration) if duration else None,
                    remaining_seconds=max(0, round(duration * (100 - percent) / 100)) if duration else None,
                    progress_at=item.get("progress_at"), updated_at=item.get("progress_at"),
                    can_mark_watched=bool(cid), can_update_progress=bool(cid) and not active, can_remove_progress=bool(cid) and not active,
                    capability_messages=["Playback is active."] if active else ([] if cid else ["Not found in the Silo library."]),
                    provider_metadata={"history_item": dict(item), "show_ids": item.get("show_ids", {}), "content_id": cid}))
            return PlaybackListResult(ok=True, provider=self.provider, instance_id=instance_id, items=items, refreshed_at=utc_now_iso())
        except (SiloSyncError, SiloError) as exc:
            return PlaybackListResult(ok=False, provider=self.provider, instance_id=instance_id, error_code=str(exc) or "silo_error",
                                      message="Silo playback request failed.", retryable=True)

    def _apply(self, config_view: Mapping[str, Any], record: Mapping[str, Any], *, instance_id: str,
               operation: str, progress_percent: float | None = None, watched_at: str | None = None) -> PlaybackActionResult:
        metadata = record.get("provider_metadata")
        source = metadata.get("history_item") if isinstance(metadata, Mapping) else None
        if not isinstance(source, Mapping):
            return public_failure(provider=self.provider, instance_id=instance_id, operation=operation,
                                  error_code="missing_media", message="Missing Silo playback media.")
        item = dict(source)
        if operation == "update_progress":
            duration = as_int(item.get("duration_ms"))
            if progress_percent is None or not 0 < float(progress_percent) < 100 or not duration:
                return public_failure(provider=self.provider, instance_id=instance_id, operation=operation,
                                      error_code="invalid_progress", message="A valid playback position is required.")
            item.update(progress_ms=round(duration * float(progress_percent) / 100), progress_at=utc_now_iso())
        elif operation == "mark_watched":
            item["watched_at"] = watched_at or utc_now_iso()
        try:
            module = self._module(config_view, instance_id)
            if operation == "remove_progress":
                result = module.remove("progress", [item])
            elif operation == "update_progress":
                result = module.add("progress", [item])
            else:
                result = module.add("history", [item])
        except (SiloSyncError, SiloError) as exc:
            return public_failure(provider=self.provider, instance_id=instance_id, operation=operation,
                                  error_code=str(exc) or "silo_error", message="Silo playback update failed.", retryable=True)
        confirmed = bool(result.get("confirmed_keys"))
        skipped = bool(result.get("skipped_keys"))
        unresolved = result.get("unresolved") or []
        reason = str((unresolved[0] or {}).get("status") or "") if unresolved else ""
        return PlaybackActionResult(
            ok=confirmed, provider=self.provider, instance_id=instance_id, operation=operation,
            remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""),
            status="applied" if confirmed else "skipped" if skipped else "failed", reason=reason or ("server_newer" if skipped else ""),
            message="Silo updated." if confirmed else "Silo kept a newer position." if skipped else "Silo update could not be verified.",
            error_code="" if confirmed else reason or "write_not_verified",
            history_result=result if operation == "mark_watched" else None)

    def remove_progress(self, config_view: Mapping[str, Any], record: Mapping[str, Any], *, instance_id: str,
                        instance_label: str) -> PlaybackActionResult:
        return self._apply(config_view, record, instance_id=instance_id, operation="remove_progress")

    def mark_watched(self, config_view: Mapping[str, Any], record: Mapping[str, Any], *, instance_id: str,
                     instance_label: str, watched_at: str | None = None) -> PlaybackActionResult:
        return self._apply(config_view, record, instance_id=instance_id, operation="mark_watched", watched_at=watched_at)

    def update_progress(self, config_view: Mapping[str, Any], record: Mapping[str, Any], progress_percent: float, *,
                        instance_id: str, instance_label: str) -> PlaybackActionResult:
        return self._apply(config_view, record, instance_id=instance_id, operation="update_progress", progress_percent=progress_percent)
