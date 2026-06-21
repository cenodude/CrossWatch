# /services/playback_progress/adapters/base.py
# CrossWatch - Playback Progress Adapters
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from typing import Any, Mapping

from ..models import PlaybackActionResult, PlaybackCapabilities, PlaybackListResult


class PlaybackProgressAdapter:
    provider = ""
    provider_label = ""

    def capabilities(
        self,
        config_view: Mapping[str, Any],
        *,
        instance_id: str,
        instance_label: str,
    ) -> PlaybackCapabilities:
        raise NotImplementedError

    def list_progress(
        self,
        config_view: Mapping[str, Any],
        *,
        instance_id: str,
        instance_label: str,
        force_refresh: bool = False,
    ) -> PlaybackListResult:
        raise NotImplementedError

    def remove_progress(
        self,
        config_view: Mapping[str, Any],
        record: Mapping[str, Any],
        *,
        instance_id: str,
        instance_label: str,
    ) -> PlaybackActionResult:
        raise NotImplementedError

    def mark_watched(
        self,
        config_view: Mapping[str, Any],
        record: Mapping[str, Any],
        *,
        instance_id: str,
        instance_label: str,
        watched_at: str | None = None,
    ) -> PlaybackActionResult:
        raise NotImplementedError

    def update_progress(
        self,
        config_view: Mapping[str, Any],
        record: Mapping[str, Any],
        progress_percent: float,
        *,
        instance_id: str,
        instance_label: str,
    ) -> PlaybackActionResult:
        raise NotImplementedError


def public_failure(
    *,
    provider: str,
    instance_id: str,
    operation: str,
    message: str,
    error_code: str = "provider_error",
    remote_status: int | None = None,
    retryable: bool = False,
    remote_id: str = "",
    canonical_key: str = "",
) -> PlaybackActionResult:
    return PlaybackActionResult(
        ok=False,
        provider=provider,
        instance_id=instance_id,
        operation=operation,
        remote_id=remote_id,
        canonical_key=canonical_key,
        error_code=error_code,
        message=message,
        remote_status=remote_status,
        retryable=retryable,
    )


def configured_label(block: Mapping[str, Any] | None, fallback: str) -> str:
    if isinstance(block, Mapping):
        for key in ("label", "name", "account_label"):
            value = str(block.get(key) or "").strip()
            if value:
                return value
    return fallback
