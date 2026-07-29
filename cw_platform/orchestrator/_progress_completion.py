# cw_platform/orchestrator/_progress_completion.py
# Progress completion policy helpers for provider-aware planning.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _as_float(value: Any) -> float | None:
    try:
        if value is None or isinstance(value, bool):
            return None
        out = float(value)
        if out == out:
            return out
    except Exception:
        return None
    return None


def _as_int(value: Any) -> int | None:
    try:
        if value is None or isinstance(value, bool):
            return None
        return int(float(value))
    except Exception:
        return None


def progress_caps_from_ops(ops: Any) -> Mapping[str, Any]:
    try:
        caps = ops.capabilities() or {}
    except Exception:
        return {}
    if not isinstance(caps, Mapping):
        return {}
    progress = caps.get("progress")
    return progress if isinstance(progress, Mapping) else {}


def progress_write_completion_policy(progress_caps: Mapping[str, Any] | None) -> dict[str, Any]:
    caps = progress_caps if isinstance(progress_caps, Mapping) else {}
    raw = caps.get("completion_policy")
    policy = raw if isinstance(raw, Mapping) else {}
    write_raw = policy.get("progress_write")
    write = write_raw if isinstance(write_raw, Mapping) else {}

    percent = (
        _as_float(write.get("percent"))
        or _as_float(write.get("auto_completes_at_percent"))
        or _as_float(write.get("default_percent"))
        or _as_float(caps.get("server_completion_percent"))
    )
    min_duration = _as_int(write.get("min_duration_seconds") or write.get("minDurationSeconds"))

    mode = str(write.get("mode") or "").strip().lower()
    if not mode and percent is not None:
        mode = "auto_complete"

    if percent is None or mode in {"", "none", "stop_only", "stop_scrobble"}:
        return {}

    out: dict[str, Any] = {
        "mode": mode,
        "percent": max(0.0, min(100.0, float(percent))),
    }
    if min_duration is not None and min_duration > 0:
        out["min_duration_seconds"] = int(min_duration)
    setting = str(write.get("setting") or "").strip()
    if setting:
        out["setting"] = setting
    return out


def fcfg_for_progress_target(
    fcfg: Mapping[str, Any] | None,
    target_ops: Any = None,
    *,
    target_caps: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    out = dict(fcfg or {})
    caps = target_caps if isinstance(target_caps, Mapping) else progress_caps_from_ops(target_ops)
    policy = progress_write_completion_policy(caps)
    percent = _as_float(policy.get("percent"))
    if percent is None:
        return out

    configured = _as_float(out.get("max_percent") or out.get("maxPercent"))
    out["max_percent"] = min(float(configured), percent) if configured is not None else percent
    min_duration = _as_int(policy.get("min_duration_seconds"))
    if min_duration is not None and min_duration > 0:
        out["max_percent_min_duration_seconds"] = int(min_duration)
    out["target_progress_completion_policy"] = policy
    return out
