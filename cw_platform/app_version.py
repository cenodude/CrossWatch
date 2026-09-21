# /cw_platform/app_version.py
# CrossWatch - Application version and HTTP client identification
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

VERSION_FILE = Path(__file__).resolve().parent.parent / "VERSION"
FALLBACK_VERSION = "v0.12.4"


@lru_cache(maxsize=16)
def _version_stamp(version_file: Path) -> str:
    try:
        return version_file.read_text(encoding="utf-8")
    except Exception:
        return ""


def app_version(*, version_file: Path | None = None, fallback: str | None = None) -> str:
    stamped = _version_stamp(version_file if version_file is not None else VERSION_FILE)
    for candidate in (stamped, os.getenv("APP_VERSION")):
        raw = str(candidate or "").strip()
        if raw.lower().lstrip("v") not in ("", "0.0.0"):
            return raw
    return fallback if fallback is not None else FALLBACK_VERSION


def user_agent(component: str = "", *, override_env: str | None = None) -> str:
    override = str((os.getenv(override_env) if override_env else "") or os.getenv("CW_UA") or "").strip()
    if override:
        return override
    agent = f"CrossWatch/{app_version()}"
    return f"{agent} ({component})" if component else agent
