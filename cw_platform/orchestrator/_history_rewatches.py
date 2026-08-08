# cw_platform/orchestrator/_history_rewatches.py
# CrossWatch - rewatch-aware history planning helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from ..history_events import (
    base_key_from_history_event,
    history_epoch_from_item,
    history_epoch_from_key,
    history_event_key,
    history_sync_key,
    minimal_history_item,
)


def history_rewatches_requested(feature: str, fcfg: Mapping[str, Any]) -> bool:
    return str(feature or "").strip().lower() == "history" and bool((fcfg or {}).get("rewatches"))


def config_with_history_rewatches(config: Mapping[str, Any], enabled: bool) -> dict[str, Any]:
    out = dict(config or {})
    if enabled:
        out["_cw_history_rewatches"] = True
    else:
        out.pop("_cw_history_rewatches", None)
    return out


def history_rewatch_supported(provider: str, ops: Any, op: str) -> bool:
    try:
        caps = ops.capabilities() or {}
    except Exception:
        caps = {}
    hist = caps.get("history") if isinstance(caps, Mapping) else None
    if isinstance(hist, Mapping):
        rw = hist.get("rewatches")
        if isinstance(rw, Mapping):
            value = rw.get(op)
            if value is None and op == "read":
                value = rw.get("enabled")
            if value is not None:
                return bool(value)
        if isinstance(rw, bool):
            return rw
        event_history = hist.get("event_history")
        if event_history is not None:
            return bool(event_history)
    return str(provider or "").strip().upper() in {"TRAKT", "SIMKL", "PUBLICMETADB", "FLOPPY", "CROSSWATCH"}


def history_rewatch_pair_enabled(
    feature: str,
    fcfg: Mapping[str, Any],
    a: str,
    aops: Any,
    b: str,
    bops: Any,
    *,
    bidirectional: bool = False,
) -> bool:
    if not history_rewatches_requested(feature, fcfg):
        return False
    if bidirectional:
        return (
            history_rewatch_supported(a, aops, "read")
            and history_rewatch_supported(a, aops, "write")
            and history_rewatch_supported(b, bops, "read")
            and history_rewatch_supported(b, bops, "write")
        )
    return (
        history_rewatch_supported(a, aops, "read")
        and history_rewatch_supported(b, bops, "read")
        and history_rewatch_supported(b, bops, "write")
    )


def collapse_history_latest(idx: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    score: dict[str, int] = {}
    for key, value in (idx or {}).items():
        if not isinstance(value, Mapping):
            continue
        base = base_key_from_history_event(key)
        if not base:
            base = history_sync_key(value, key, event_mode=False)
        if not base:
            continue
        item = minimal_history_item(value, key, event_mode=False)
        item.pop("_cw_event_key", None)
        item.pop("_cw_rewatch_sync", None)
        ts = history_epoch_from_item(value) or history_epoch_from_key(key) or 0
        if base not in out or ts >= score.get(base, 0):
            out[base] = item
            score[base] = ts
    return out


def filter_history_events(idx: Mapping[str, Any], *, event_mode: bool) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for key, value in (idx or {}).items():
        if not isinstance(value, Mapping):
            continue
        if not (value.get("watched_at") or value.get("last_watched_at")):
            continue
        item = minimal_history_item(value, key, event_mode=event_mode)
        sync_key = history_sync_key(item, key, event_mode=event_mode)
        if sync_key:
            out[sync_key] = item
    return out


def history_event_present(
    item: Mapping[str, Any],
    fallback_key: Any,
    other_idx: Mapping[str, Any],
    typed_tokens: Callable[[Mapping[str, Any]], set[str]],
    bucket_sec: int,
) -> bool:
    key = history_event_key(item, fallback_key)
    if key and key in other_idx:
        return True
    ts = history_epoch_from_item(item) or history_epoch_from_key(key)
    if ts is None:
        return False
    b = max(0, int(bucket_sec or 0))
    ts_cmp = ts if b <= 1 else (ts // b) * b
    for other_key, other_item in (other_idx or {}).items():
        if not isinstance(other_item, Mapping):
            continue
        other_ts = history_epoch_from_item(other_item) or history_epoch_from_key(other_key)
        if other_ts is None:
            continue
        other_cmp = other_ts if b <= 1 else (other_ts // b) * b
        if other_cmp != ts_cmp:
            continue
        if typed_tokens(item) & typed_tokens(other_item):
            return True
    return False


def history_event_diff(
    src_idx: Mapping[str, Any],
    dst_idx: Mapping[str, Any],
    typed_tokens: Callable[[Mapping[str, Any]], set[str]],
    *,
    bucket_sec: int = 0,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    adds: list[dict[str, Any]] = []
    removes: list[dict[str, Any]] = []
    for key, value in (src_idx or {}).items():
        if isinstance(value, Mapping) and not history_event_present(value, key, dst_idx, typed_tokens, bucket_sec):
            adds.append(minimal_history_item(value, key, event_mode=True))
    for key, value in (dst_idx or {}).items():
        if isinstance(value, Mapping) and not history_event_present(value, key, src_idx, typed_tokens, bucket_sec):
            removes.append(minimal_history_item(value, key, event_mode=True))
    return adds, removes
