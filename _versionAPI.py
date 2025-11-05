# _versionAPI.py
# CrossWatch version API
# Copyright (c) 2025 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import re
import time
from functools import lru_cache
from importlib import import_module
from typing import Any, Dict, List, Tuple

import requests
from fastapi import APIRouter
from packaging.version import InvalidVersion, Version

__all__ = ["router"]

router = APIRouter(prefix="/api", tags=["version"])

# ── Environment & constants
CURRENT_VERSION = os.getenv("APP_VERSION", "v0.3.2")
REPO = os.getenv("GITHUB_REPO", "cenodude/CrossWatch")


def _github_api() -> str:
    return f"https://api.github.com/repos/{REPO}/releases/latest"


# ── Helpers -------------------------------------------------------------------
def _env_modules() -> Dict[str, Dict[str, str]]:
    try:
        from cw_platform.modules_registry import MODULES  # type: ignore
        return MODULES  # {"group": {"name": "module.path", ...}, ...}
    except Exception:
        return {}


def _norm(v: str) -> str:
    """Normalize version strings: trim and drop a leading 'v'."""
    return re.sub(r"^\s*v", "", (v or "").strip(), flags=re.IGNORECASE)


def _ttl_marker(seconds: int = 300) -> int:
    """Integer marker that changes every `seconds`; safe for lru_cache keys."""
    return int(time.time() // seconds)


@lru_cache(maxsize=1)
def _cached_latest_release(_marker: int) -> Dict[str, Any]:
    """Fetch latest release from GitHub (cached by TTL marker)."""
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "CrossWatch"}
    url = _github_api()
    try:
        r = requests.get(url, headers=headers, timeout=8)
        r.raise_for_status()
        data = r.json() or {}
        tag = _norm(data.get("tag_name") or "")
        return {
            "latest": tag or None,
            "html_url": data.get("html_url") or f"https://github.com/{REPO}/releases",
            "body": data.get("body") or "",
            "published_at": data.get("published_at"),
        }
    except Exception:
        return {
            "latest": None,
            "html_url": f"https://github.com/{REPO}/releases",
            "body": "",
            "published_at": None,
        }


def _is_update_available(current: str, latest: str | None) -> bool:
    if not latest:
        return False
    try:
        return Version(_norm(latest)) > Version(_norm(current))
    except InvalidVersion:
        # Fallback for non-PEP440 tags
        return latest != current


def _ver_tuple(s: str) -> Tuple[int, ...]:
    try:
        return tuple(int(p) for p in re.findall(r"\d+", (s or "")))
    except Exception:
        return (0,)


def _get_module_version(mod_path: str) -> str:
    try:
        m = import_module(mod_path)
        return str(
            getattr(
                m,
                "__VERSION__",
                getattr(m, "VERSION", getattr(m, "__version__", "0.0.0")),
            )
        )
    except Exception:
        return "0.0.0"


# ── API -----------------------------------------------------------------------
@router.get("/update")
def api_update() -> Dict[str, Any]:
    cache = _cached_latest_release(_ttl_marker(300))
    cur = _norm(CURRENT_VERSION)
    lat = cache.get("latest") or cur
    html_url = cache.get("html_url")
    return {
        "current_version": cur,
        "latest_version": lat,
        "update_available": _is_update_available(cur, lat),
        "html_url": html_url,
        "url": html_url,
        "body": cache.get("body", ""),
        "published_at": cache.get("published_at"),
    }


@router.get("/version")
def get_version() -> Dict[str, Any]:
    cache = _cached_latest_release(_ttl_marker(300))
    cur = _norm(CURRENT_VERSION)
    lat = cache.get("latest")
    return {
        "current": cur,
        "latest": lat,
        "update_available": _is_update_available(cur, lat or cur),
        "html_url": cache.get("html_url"),
    }


@router.get("/version/check")
def api_version_check() -> Dict[str, Any]:
    cache = _cached_latest_release(_ttl_marker(300))
    cur = CURRENT_VERSION
    lat = cache.get("latest") or cur
    return {
        "current": cur,
        "latest": lat,
        "update_available": _ver_tuple(lat) > _ver_tuple(cur),
        "name": None,
        "url": cache.get("html_url"),
        "notes": "",
        "published_at": None,
    }


@router.get("/modules/versions")
def get_module_versions() -> Dict[str, Any]:
    groups = {
        g: {name: _get_module_version(path) for name, path in mods.items()}
        for g, mods in _env_modules().items()
    }
    flat: Dict[str, str] = {name: ver for mods in groups.values() for name, ver in mods.items()}
    return {"groups": groups, "flat": flat}
