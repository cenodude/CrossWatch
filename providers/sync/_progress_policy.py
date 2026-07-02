# /ptoviders/sync/_progress_policy.py
# CrossWatch - Progress Policy Utilities
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping


DEFAULT_TIMESTAMP_TOLERANCE_SECONDS = 30
DEFAULT_PROGRESS_TOLERANCE_PERCENT = 0.1
DEFAULT_PROGRESS_TOLERANCE_MS = 1_000


def as_epoch(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        current = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return current.timestamp()
    try:
        text = str(value).strip()
        if not text:
            return None
        if text.replace(".", "", 1).isdigit():
            number = float(text)
            return number / 1000.0 if number >= 10_000_000_000 else number
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _positive_int(value: Any) -> int | None:
    try:
        number = int(float(value))
        return number if number > 0 else None
    except Exception:
        return None


def progress_ratio(progress_ms: Any, duration_ms: Any) -> float | None:
    progress = _positive_int(progress_ms)
    duration = _positive_int(duration_ms)
    if progress is None or duration is None:
        return None
    return progress / duration


def _remote_id(record: Mapping[str, Any]) -> str:
    raw_ids = record.get("ids")
    ids: Mapping[str, Any] = raw_ids if isinstance(raw_ids, Mapping) else {}
    return str(
        record.get("remote_id")
        or record.get("emby_item_id")
        or record.get("jellyfin_item_id")
        or record.get("ratingKey")
        or record.get("_item_id")
        or ids.get("emby")
        or ids.get("jellyfin")
        or ids.get("plex")
        or ""
    )


def select_progress_record(
    current: Mapping[str, Any] | None,
    candidate: Mapping[str, Any],
) -> tuple[dict[str, Any], str]:
    """Select a canonical duplicate without depending on input order."""
    if not isinstance(current, Mapping):
        return dict(candidate), "new"

    current_ts = as_epoch(current.get("progress_at") or current.get("updated_at"))
    candidate_ts = as_epoch(candidate.get("progress_at") or candidate.get("updated_at"))
    if current_ts is not None or candidate_ts is not None:
        if current_ts is None:
            return dict(candidate), "newer_timestamp"
        if candidate_ts is None:
            return dict(current), "keep_newer_timestamp"
        if candidate_ts != current_ts:
            return (
                (dict(candidate), "newer_timestamp")
                if candidate_ts > current_ts
                else (dict(current), "keep_newer_timestamp")
            )

    current_ratio = progress_ratio(current.get("progress_ms"), current.get("duration_ms"))
    candidate_ratio = progress_ratio(candidate.get("progress_ms"), candidate.get("duration_ms"))
    if current_ratio is not None and candidate_ratio is not None and candidate_ratio != current_ratio:
        return (
            (dict(candidate), "higher_progress_percent")
            if candidate_ratio > current_ratio
            else (dict(current), "keep_higher_progress_percent")
        )

    current_ms = _positive_int(current.get("progress_ms")) or 0
    candidate_ms = _positive_int(candidate.get("progress_ms")) or 0
    if candidate_ms != current_ms:
        return (
            (dict(candidate), "higher_progress_offset")
            if candidate_ms > current_ms
            else (dict(current), "keep_higher_progress_offset")
        )

    current_key = (
        _remote_id(current),
        str(current.get("library_id") or ""),
        str(current.get("title") or ""),
    )
    candidate_key = (
        _remote_id(candidate),
        str(candidate.get("library_id") or ""),
        str(candidate.get("title") or ""),
    )
    if candidate_key < current_key:
        return dict(candidate), "deterministic_fallback"
    return dict(current), "keep_deterministic_fallback"


def progress_materially_equal(
    source_ms: Any,
    source_duration_ms: Any,
    target_ms: Any,
    target_duration_ms: Any,
    *,
    percent_tolerance: float = DEFAULT_PROGRESS_TOLERANCE_PERCENT,
    ms_tolerance: int = DEFAULT_PROGRESS_TOLERANCE_MS,
) -> bool:
    source_ratio = progress_ratio(source_ms, source_duration_ms)
    target_ratio = progress_ratio(target_ms, target_duration_ms)
    if source_ratio is not None and target_ratio is not None:
        return abs(source_ratio - target_ratio) * 100.0 <= max(0.0, float(percent_tolerance))
    source = _positive_int(source_ms)
    target = _positive_int(target_ms)
    return source is not None and target is not None and abs(source - target) <= max(0, int(ms_tolerance))


@dataclass(frozen=True)
class ProgressDecision:
    apply: bool
    reason: str
    unwatch_first: bool = False


def decide_progress_write(
    *,
    active_session: bool,
    source_timestamp: Any,
    target_timestamp: Any,
    source_progress_ms: Any,
    source_duration_ms: Any,
    target_progress_ms: Any,
    target_duration_ms: Any,
    target_watched: bool,
    same_origin: bool,
    replay_enabled: bool,
    timestamp_tolerance_seconds: int = DEFAULT_TIMESTAMP_TOLERANCE_SECONDS,
) -> ProgressDecision:
    if active_session:
        return ProgressDecision(False, "active_session")
    if same_origin:
        return ProgressDecision(False, "same_origin")
    source_ts = as_epoch(source_timestamp)
    if source_ts is None:
        return ProgressDecision(False, "missing_source_timestamp")
    target_ts = as_epoch(target_timestamp)
    tolerance = max(0, int(timestamp_tolerance_seconds))
    if target_ts is not None and target_ts > source_ts + tolerance:
        return ProgressDecision(False, "target_newer")
    if target_watched and not replay_enabled:
        return ProgressDecision(False, "already_watched")
    if progress_materially_equal(
        source_progress_ms,
        source_duration_ms,
        target_progress_ms,
        target_duration_ms,
    ):
        return ProgressDecision(False, "progress_unchanged")
    return ProgressDecision(True, "apply", unwatch_first=bool(target_watched and replay_enabled))
