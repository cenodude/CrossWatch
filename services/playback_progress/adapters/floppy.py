# /services/playback_progress/adapters/floppy.py
# CrossWatch - Floppy Playback Progress Adapter
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from typing import Any, Mapping, cast

from cw_platform.id_map import canonical_key, minimal as id_minimal
from providers.sync._mod_FLOPPY import OPS as FLOPPY_OPS

from ..models import PlaybackActionResult, PlaybackCapabilities, PlaybackListResult, PlaybackRecord, clean_mapping, utc_now_iso
from .base import PlaybackProgressAdapter, public_failure


def _mapping(value: Any) -> Mapping[str, Any]:
    return cast(Mapping[str, Any], value) if isinstance(value, Mapping) else {}


def _num(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(float(str(value).strip()))
    except Exception:
        return None


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).strip())
    except Exception:
        return None


def _duration_seconds(item: Mapping[str, Any]) -> int | None:
    duration = _num(item.get("duration_ms") or item.get("duration"))
    return max(1, round(duration / 1000)) if duration and duration > 0 else None


def _remaining_seconds(item: Mapping[str, Any]) -> int | None:
    duration = _num(item.get("duration_ms") or item.get("duration"))
    position = _num(item.get("progress_ms") or item.get("position_ms") or item.get("position"))
    if duration is None or position is None or duration <= 0:
        return None
    return max(0, round((duration - position) / 1000))


def _progress_percent(item: Mapping[str, Any]) -> float | None:
    direct = _float(item.get("progress_percent") or item.get("progress"))
    if direct is not None:
        return round(direct, 3)
    position = _float(item.get("progress_ms") or item.get("position_ms") or item.get("position"))
    duration = _float(item.get("duration_ms") or item.get("duration"))
    if position is None or duration is None or duration <= 0:
        return None
    return round((position / duration) * 100.0, 3)


def _position_seconds(item: Mapping[str, Any]) -> int | None:
    position = _num(item.get("progress_ms") or item.get("position_ms") or item.get("position"))
    return max(0, round(position / 1000)) if position is not None else None


def _progress_item(record: Mapping[str, Any]) -> dict[str, Any]:
    meta = _mapping(record.get("provider_metadata"))
    stored = meta.get("progress_item")
    if isinstance(stored, Mapping):
        return clean_mapping(stored)
    media_type = str(record.get("media_type") or "").strip().lower()
    item: dict[str, Any] = {
        "type": "episode" if media_type in {"episode", "anime_episode"} else "movie",
        "ids": clean_mapping(_mapping(record.get("ids"))),
        "title": record.get("episode_title") or record.get("title") or record.get("series_title"),
        "year": record.get("year"),
    }
    if item["type"] == "episode":
        item["show_ids"] = clean_mapping(_mapping(meta.get("show_ids"))) or clean_mapping(_mapping(record.get("ids")))
        item["series_title"] = record.get("series_title")
        item["season"] = record.get("season")
        item["episode"] = record.get("episode")
    duration_seconds = _num(record.get("duration_seconds"))
    if duration_seconds:
        item["duration_ms"] = duration_seconds * 1000
    return clean_mapping(item)


def _progress_item_with_percent(record: Mapping[str, Any], progress_percent: float) -> dict[str, Any]:
    item = _progress_item(record)
    duration_ms = _num(item.get("duration_ms") or item.get("duration"))
    if duration_ms is None:
        duration_seconds = _num(record.get("duration_seconds"))
        if duration_seconds:
            duration_ms = duration_seconds * 1000
    if duration_ms and duration_ms > 0:
        item["duration_ms"] = duration_ms
        item["progress_ms"] = max(1, min(duration_ms - 1, round((float(progress_percent) / 100.0) * float(duration_ms))))
    else:
        item["progress_percent"] = round(max(0.0, min(100.0, float(progress_percent))), 3)
    item["progress_at"] = utc_now_iso()
    return clean_mapping(item)


class FloppyPlaybackAdapter(PlaybackProgressAdapter):
    provider = "floppy"
    provider_label = "Floppy"
    ops = FLOPPY_OPS

    def capabilities(self, config_view: Mapping[str, Any], *, instance_id: str, instance_label: str) -> PlaybackCapabilities:
        try:
            configured = bool(FLOPPY_OPS.is_configured(config_view))
        except Exception:
            configured = False
        caps = _mapping(FLOPPY_OPS.capabilities())
        progress = _mapping(caps.get("progress"))
        history = _mapping(caps.get("history"))
        types = _mapping(progress.get("types"))
        reason = "" if configured else "Floppy is not connected for this instance."
        return PlaybackCapabilities(
            provider=self.provider,
            provider_label=self.provider_label,
            instance_id=instance_id,
            instance_label=instance_label,
            configured=configured,
            read=bool(configured and progress.get("read")),
            remove_progress=bool(configured and progress.get("remove")),
            mark_watched=bool(configured and history.get("upsert")),
            update_progress=bool(configured and progress.get("upsert")),
            bulk_remove_progress=bool(configured and progress.get("remove")),
            bulk_mark_watched=bool(configured and history.get("upsert")),
            bulk_update_progress=bool(configured and progress.get("upsert")),
            supports_movies=bool(configured and types.get("movies")),
            supports_episodes=bool(configured and types.get("episodes")),
            supports_anime=False,
            reason=reason,
        )

    def list_progress(self, config_view: Mapping[str, Any], *, instance_id: str, instance_label: str, force_refresh: bool = False) -> PlaybackListResult:
        caps = self.capabilities(config_view, instance_id=instance_id, instance_label=instance_label)
        if not caps.read:
            return PlaybackListResult(False, self.provider, instance_id, error_code="unsupported" if caps.configured else "not_configured", message=caps.reason or "Floppy does not support playback listing.")
        try:
            index = FLOPPY_OPS.build_index(config_view, feature="progress") or {}
        except Exception:
            return PlaybackListResult(False, self.provider, instance_id, error_code="provider_error", message="Floppy progress request failed.", retryable=True)
        items = [self._record(key, item, instance_id, instance_label, caps) for key, item in dict(index).items() if isinstance(item, Mapping)]
        return PlaybackListResult(True, self.provider, instance_id, items=[item for item in items if item], refreshed_at=utc_now_iso())

    def _record(self, key: Any, item: Mapping[str, Any], instance_id: str, instance_label: str, caps: PlaybackCapabilities) -> PlaybackRecord | None:
        mini = id_minimal(item)
        typ = str(mini.get("type") or item.get("type") or "").strip().lower()
        media_type = "episode" if typ == "episode" else "movie"
        ids = clean_mapping(_mapping(mini.get("ids") or item.get("ids")))
        show_ids = clean_mapping(_mapping(mini.get("show_ids") or item.get("show_ids")))
        title = str(mini.get("title") or item.get("title") or item.get("series_title") or "").strip()
        series_title = str(mini.get("series_title") or item.get("series_title") or "").strip()
        canonical = canonical_key(mini) or str(key or "")
        progress = _progress_percent(item)
        position = _position_seconds(item)
        if not canonical or (progress is None and position is None):
            return None
        return PlaybackRecord(
            provider=self.provider,
            provider_label=self.provider_label,
            instance_id=instance_id,
            instance_label=instance_label,
            remote_id=str(key or canonical),
            canonical_key=canonical,
            media_type=media_type,
            title=series_title or title,
            episode_title=title if media_type == "episode" and title != series_title else "",
            series_title=series_title,
            season=_num(mini.get("season") or item.get("season")),
            episode=_num(mini.get("episode") or item.get("episode")),
            year=_num(mini.get("year") or item.get("year")),
            ids=ids,
            progress_percent=progress,
            remaining_seconds=_remaining_seconds(item),
            duration_seconds=_duration_seconds(item),
            progress_at=str(item.get("progress_at") or "").strip() or None,
            updated_at=str(item.get("progress_at") or "").strip() or None,
            can_remove_progress=caps.remove_progress,
            can_mark_watched=caps.mark_watched,
            can_update_progress=bool(caps.update_progress and _duration_seconds(item)),
            capability_messages=[] if caps.configured else [caps.reason],
            poster_url=str(item.get("poster") or item.get("poster_url") or "").strip(),
            backdrop_url=str(item.get("backdrop") or item.get("backdrop_url") or "").strip(),
            provider_metadata={"progress_item": clean_mapping(item), "show_ids": show_ids, "position_seconds": position},
        )

    def remove_progress(self, config_view: Mapping[str, Any], record: Mapping[str, Any], *, instance_id: str, instance_label: str) -> PlaybackActionResult:
        try:
            result = FLOPPY_OPS.remove(config_view, [_progress_item(record)], feature="progress", dry_run=False) or {}
            ok = bool(result.get("ok"))
            return PlaybackActionResult(ok, self.provider, instance_id, "remove_progress", remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""), message="Playback record removed." if ok else "Floppy remove progress failed.", error_code="" if ok else "progress_failed", decision_context=clean_mapping(result))
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="Floppy remove progress failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))

    def mark_watched(self, config_view: Mapping[str, Any], record: Mapping[str, Any], *, instance_id: str, instance_label: str, watched_at: str | None = None) -> PlaybackActionResult:
        item = _progress_item(record)
        item["watched_at"] = watched_at or utc_now_iso()
        try:
            result = FLOPPY_OPS.add(config_view, [item], feature="history", dry_run=False) or {}
            ok = bool(result.get("ok"))
            return PlaybackActionResult(ok, self.provider, instance_id, "mark_watched", remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""), message="Marked watched on Floppy." if ok else "Floppy mark watched failed.", error_code="" if ok else "history_failed", history_result=clean_mapping(result))
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="mark_watched", message="Floppy mark watched failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))

    def update_progress(self, config_view: Mapping[str, Any], record: Mapping[str, Any], progress_percent: float, *, instance_id: str, instance_label: str) -> PlaybackActionResult:
        try:
            result = FLOPPY_OPS.add(config_view, [_progress_item_with_percent(record, progress_percent)], feature="progress", dry_run=False) or {}
            ok = bool(result.get("ok"))
            return PlaybackActionResult(ok, self.provider, instance_id, "update_progress", remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""), message=f"Progress updated on Floppy to {progress_percent:g}%." if ok else "Floppy update progress failed.", error_code="" if ok else "progress_failed", decision_context=clean_mapping(result))
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="update_progress", message="Floppy update progress failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))
