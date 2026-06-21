# /services/playback_progress/models.py
# CrossWatch - Playback Progress Models
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from dataclasses import asdict, dataclass as dc_dataclass, field
from typing import Any, Mapping


def utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def clean_mapping(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    blocked = {"token", "access_token", "refresh_token", "authorization", "api_key", "apikey", "cookie", "secret"}
    out: dict[str, Any] = {}
    for key, val in value.items():
        k = str(key)
        if k.lower() in blocked or any(part in k.lower() for part in blocked):
            continue
        if isinstance(val, Mapping):
            out[k] = clean_mapping(val)
        elif isinstance(val, list):
            out[k] = [
                clean_mapping(item) if isinstance(item, Mapping) else item
                for item in val[:50]
            ]
        elif isinstance(val, (str, int, float, bool)) or val is None:
            out[k] = val
    return out


@dc_dataclass
class PlaybackCapabilities:
    provider: str
    provider_label: str
    instance_id: str
    instance_label: str
    included: bool = True
    configured: bool = False
    read: bool = False
    remove_progress: bool = False
    mark_watched: bool = False
    update_progress: bool = False
    bulk_remove_progress: bool = False
    bulk_mark_watched: bool = False
    bulk_update_progress: bool = False
    supports_movies: bool = False
    supports_episodes: bool = False
    supports_anime: bool = False
    reason: str = ""
    last_refresh: str | None = None
    last_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["capabilities"] = {
            "read": self.read,
            "remove_progress": self.remove_progress,
            "mark_watched": self.mark_watched,
            "update_progress": self.update_progress,
            "bulk_remove_progress": self.bulk_remove_progress,
            "bulk_mark_watched": self.bulk_mark_watched,
            "bulk_update_progress": self.bulk_update_progress,
            "supports_movies": self.supports_movies,
            "supports_episodes": self.supports_episodes,
            "supports_anime": self.supports_anime,
        }
        return data


@dc_dataclass
class PlaybackRecord:
    provider: str
    provider_label: str
    instance_id: str
    instance_label: str
    remote_id: str
    canonical_key: str
    media_type: str
    title: str = ""
    episode_title: str = ""
    series_title: str = ""
    season: int | None = None
    episode: int | None = None
    year: int | None = None
    ids: dict[str, Any] = field(default_factory=dict)
    progress_percent: float | None = None
    remaining_seconds: int | None = None
    duration_seconds: int | None = None
    progress_at: str | None = None
    updated_at: str | None = None
    source_app: str = ""
    source_device: str = ""
    rating: float | None = None
    poster_url: str = ""
    backdrop_url: str = ""
    can_remove_progress: bool = False
    can_mark_watched: bool = False
    can_update_progress: bool = False
    capability_messages: list[str] = field(default_factory=list)
    provider_metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["ids"] = clean_mapping(self.ids)
        data["provider_metadata"] = clean_mapping(self.provider_metadata)
        return data


@dc_dataclass
class PlaybackActionResult:
    ok: bool
    provider: str
    instance_id: str
    operation: str
    remote_id: str = ""
    canonical_key: str = ""
    error_code: str = ""
    message: str = ""
    retryable: bool = False
    remote_status: int | None = None
    history_result: dict[str, Any] | None = None
    playback_cleanup_result: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return clean_mapping(asdict(self))


@dc_dataclass
class PlaybackListResult:
    ok: bool
    provider: str
    instance_id: str
    items: list[PlaybackRecord] = field(default_factory=list)
    error_code: str = ""
    message: str = ""
    retryable: bool = False
    remote_status: int | None = None
    refreshed_at: str | None = None

    def to_error(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "provider": self.provider,
            "instance_id": self.instance_id,
            "operation": "list_progress",
            "error_code": self.error_code,
            "message": self.message,
            "retryable": self.retryable,
            "remote_status": self.remote_status,
        }
