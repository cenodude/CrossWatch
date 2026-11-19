# cw_platform/history_dates.py
# CrossWatch - Explicit unknown watch dates and adapter capabilities
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any


UNKNOWN_DATE_UNSUPPORTED = "unknown_watch_date_unsupported"
UNKNOWN_REWATCH_UNSUPPORTED = "unknown_rewatch_unsupported"
UNKNOWN_REMOVAL_UNSAFE = "unknown_watch_removal_unsafe"


def unknown_date_capabilities(ops: Any) -> dict[str, Any]:
    try:
        capabilities = ops.capabilities()
    except Exception:
        capabilities = {}
    history = capabilities.get("history") if isinstance(capabilities, Mapping) else None
    raw = history.get("unknown_dates") if isinstance(history, Mapping) else None
    raw = raw if isinstance(raw, Mapping) else {}
    rewatches = raw.get("rewatches")
    rewatches = rewatches if isinstance(rewatches, Mapping) else {}
    multiple = raw.get("multiple")
    multiple = multiple if isinstance(multiple, Mapping) else {}
    return {
        "read": raw.get("read") is True,
        "write": raw.get("write") is True,
        "rewatches": {op: raw.get(op) is True and rewatches.get(op) is True for op in ("read", "write")},
        "multiple": {op: raw.get(op) is True and rewatches.get(op) is True and multiple.get(op) is True for op in ("read", "write")},
    }


def is_unknown_watch(item: Mapping[str, Any]) -> bool:
    return (
        item.get("type") in {"movie", "episode"}
        and item.get("watched") is True
        and item.get("_cw_watched_state") is True
        and not (item.get("watched_at") or item.get("last_watched_at"))
    )


def unknown_watch(item: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(item)
    for field in ("watched_at", "last_watched_at", "watchedAt"):
        out.pop(field, None)
    out.update(watched=True, _cw_watched_state=True)
    return out


def unknown_date_reason(ops: Any, item: Mapping[str, Any], *, event_mode: bool = False) -> str:
    if not is_unknown_watch(item):
        return ""
    caps = unknown_date_capabilities(ops)
    if not caps["write"]:
        return UNKNOWN_DATE_UNSUPPORTED
    if event_mode and not caps["rewatches"]["write"]:
        return UNKNOWN_REWATCH_UNSUPPORTED
    return ""


def split_unknown_dates(
    feature: str, ops: Any, items: list[dict[str, Any]], *, event_mode: bool = False,
    emit: Callable[..., Any] | None = None, provider: str = "", instance: str = "default", operation: str = "add",
) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], str]]]:
    if feature != "history":
        return items, []
    kept: list[dict[str, Any]] = []
    skipped: list[tuple[dict[str, Any], str]] = []
    for item in items:
        reason = unknown_date_reason(ops, item, event_mode=event_mode)
        if reason:
            skipped.append((item, UNKNOWN_REMOVAL_UNSAFE if operation == "remove" else reason))
        else:
            kept.append(item)
    if emit and skipped:
        for reason in dict.fromkeys(reason for _, reason in skipped):
            rows = [dict(item) for item, why in skipped if why == reason]
            for start in range(0, len(rows), 100):
                batch = rows[start:start + 100]
                emit("history:skipped", feature=feature, provider=provider, destination_instance=instance,
                     op=operation, reason=reason, count=len(batch), items=batch)
    return kept, skipped
