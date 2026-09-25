# /cw_platform/profile_preferences.py
# CrossWatch - Profile display preferences
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from datetime import timezone, tzinfo
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError, available_timezones


@lru_cache(maxsize=1)
def _timezone_names() -> frozenset[str]:
    return frozenset(available_timezones())


def clean_timezone(value: Any) -> str:
    name = str(value or "auto").strip()
    if name == "auto":
        return name
    if name not in _timezone_names():
        return "auto"
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        return "auto"
    return name


def display_timezone(value: Any) -> tzinfo:
    name = clean_timezone(value)
    return timezone.utc if name in {"auto", "UTC"} else ZoneInfo(name)


def clean_user_preferences(raw: Any) -> dict[str, Any]:
    src = raw if isinstance(raw, dict) else {}
    time_format = str(src.get("time_format") or "auto")
    return {
        "playing_card": src.get("playing_card") is not False,
        "quick_add": src.get("quick_add") is not False,
        "timezone": clean_timezone(src.get("timezone")),
        "time_format": time_format if time_format in {"auto", "12h", "24h"} else "auto",
    }
