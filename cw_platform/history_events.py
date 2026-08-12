# cw_platform/history_events.py
# CrossWatch - history event identity helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import datetime as _dt
import re
from collections.abc import Mapping
from typing import Any

from .id_map import canonical_key, minimal as id_minimal

EVENT_KEY_RE = re.compile(r"^(?P<base>.+?)@(?P<event>(?:\d{7,}(?:~.+)?|id:.+))$")


def _event_part_ok(event: str) -> bool:
    if event.startswith("id:"):
        return len(event) > 3
    end = 0
    size = len(event)
    while end < size and event[end].isdigit():
        end += 1
    if end < 7:
        return False
    if end == size:
        return True
    return event[end] == "~" and size > end + 1


def split_history_event_key(key: Any) -> tuple[str, str] | None:
    raw = str(key or "").strip().lower()
    at = raw.find("@", 1)
    while at != -1:
        event = raw[at + 1 :]
        if _event_part_ok(event):
            return raw[:at], event
        at = raw.find("@", at + 1)
    return None

EVENT_ID_FIELDS = (
    "provider_event_id",
    "_trakt_history_id",
    "trakt_history_id",
    "_publicmetadb_history_id",
    "_floppy_consumption_id",
    "consumption_id",
    "history_id",
    "_history_id",
    "_simkl_history_id",
    "_simkl_rewatch_id",
    "_punchplay_history_id",
    "_scrob_history_id",
    "rewatch_id",
)

EVENT_META_FIELDS = (
    *EVENT_ID_FIELDS,
    "_punchplay_history_ids",
    "_scrob_media_id",
    "is_rewatch",
    "rewatch_status",
    "_cw_event_key",
    "_cw_rewatch_sync",
)


def is_history_event_key(key: Any) -> bool:
    return split_history_event_key(key) is not None


def base_key_from_history_event(key: Any) -> str:
    raw = str(key or "").strip().lower()
    parts = split_history_event_key(raw)
    return parts[0] if parts else raw


def history_epoch_from_value(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        try:
            ts = int(value)
            return ts if ts > 0 else None
        except Exception:
            return None
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.isdigit():
        try:
            ts = int(raw)
            return ts if ts > 0 else None
        except Exception:
            return None
    try:
        dt = _dt.datetime.fromisoformat(raw.replace("Z", "+00:00").replace(" ", "T"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_dt.timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def history_epoch_from_key(key: Any) -> int | None:
    parts = split_history_event_key(key)
    if not parts:
        return None
    event = parts[1].split("~", 1)[0]
    if not event.isdigit():
        return None
    try:
        return int(event)
    except Exception:
        return None


def history_epoch_from_item(item: Mapping[str, Any]) -> int | None:
    for field in ("watched_at", "last_watched_at", "collected_at"):
        ts = history_epoch_from_value(item.get(field))
        if ts is not None:
            return ts
    return None


def provider_event_id(item: Mapping[str, Any]) -> str:
    for field in EVENT_ID_FIELDS:
        value = str(item.get(field) or "").strip()
        if value:
            return value
    return ""


def history_event_key(item: Mapping[str, Any], fallback_key: Any = None) -> str:
    fallback = str(fallback_key or "").strip().lower()
    if is_history_event_key(fallback):
        return fallback
    base = fallback.split("@", 1)[0] if "@" in fallback else (base_key_from_history_event(fallback) if fallback else "")
    if not base or ":" not in base:
        try:
            base = canonical_key(id_minimal(item)) or ""
        except Exception:
            base = ""
    base = str(base or "").strip().lower()
    if not base:
        return ""
    ts = history_epoch_from_item(item) or history_epoch_from_key(fallback)
    if ts is not None:
        return f"{base}@{ts}"
    event_id = provider_event_id(item)
    if event_id:
        return f"{base}@id:{event_id.lower()}"
    return base


def history_sync_key(item: Mapping[str, Any], fallback_key: Any = None, *, event_mode: bool = False) -> str:
    if event_mode:
        return history_event_key(item, fallback_key)
    try:
        return canonical_key(id_minimal(item)) or base_key_from_history_event(fallback_key)
    except Exception:
        return base_key_from_history_event(fallback_key)


def minimal_history_item(item: Mapping[str, Any], fallback_key: Any = None, *, event_mode: bool = False) -> dict[str, Any]:
    out = id_minimal(item)
    for field in EVENT_META_FIELDS:
        if field in item and item.get(field) not in (None, ""):
            out[field] = item.get(field)
    if not event_mode:
        out.pop("_cw_event_key", None)
        out.pop("_cw_rewatch_sync", None)
        return out
    if event_mode:
        key = history_event_key(out, fallback_key)
        if key:
            out["_cw_event_key"] = key
            out["_cw_rewatch_sync"] = True
    return out
