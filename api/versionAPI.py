# /api/versionAPI.py
# CrossWatch - Version Management API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from __future__ import annotations

import os
import re
import time
from functools import lru_cache
from importlib import import_module
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from fastapi import APIRouter
from packaging.version import InvalidVersion, Version

__all__ = ["router"]

router = APIRouter(prefix="/api", tags=["version"])

VERSION_FILE = Path(__file__).resolve().parent.parent / "VERSION"
FALLBACK_VERSION = "v0.12.1"


def _usable_version(value: Any) -> str:
    raw = str(value or "").strip()
    return "" if raw.lower().lstrip("v") in ("", "0.0.0") else raw


def resolve_current_version() -> str:
    try:
        stamped = VERSION_FILE.read_text(encoding="utf-8")
    except Exception:
        stamped = ""
    for candidate in (stamped, os.getenv("APP_VERSION")):
        usable = _usable_version(candidate)
        if usable:
            return usable
    return FALLBACK_VERSION


CURRENT_VERSION = resolve_current_version()
REPO = os.getenv("GITHUB_REPO", "cenodude/CrossWatch")


def _github_api() -> str:
    return f"https://api.github.com/repos/{REPO}/releases/latest"


def _env_modules() -> dict[str, dict[str, str]]:
    try:
        from cw_platform.modules_registry import MODULES  # type: ignore
        return MODULES
    except Exception:
        return {}


def _norm(v: str) -> str:
    return re.sub(r"^\s*v", "", (v or "").strip(), flags=re.IGNORECASE)


def _ttl_marker(seconds: int = 300) -> int:
    return int(time.time() // seconds)


@lru_cache(maxsize=1)
def _cached_latest_release(_marker: int) -> dict[str, Any]:
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
        return latest != current


def _ver_tuple(s: str) -> tuple[int, ...]:
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


@router.get("/update")
def api_update() -> dict[str, Any]:
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


@lru_cache(maxsize=8)
def _cached_installed_release(repo: str, version: str, _marker: int) -> dict[str, Any]:
    tag = quote(f"v{version}", safe="")
    release_url = f"https://github.com/{repo}/releases/tag/{tag}"
    result: dict[str, Any] = {"version": version, "body": "", "html_url": f"https://github.com/{repo}/releases"}
    try:
        response = requests.get(
            f"https://api.github.com/repos/{repo}/releases/tags/{tag}",
            headers={"Accept": "application/vnd.github+json", "User-Agent": "CrossWatch"},
            timeout=8,
        )
        response.raise_for_status()
        data = response.json() or {}
        # Development builds must not borrow notes from a different release.
        if _norm(str(data.get("tag_name") or "")) == version:
            result["body"] = str(data.get("body") or "")
            result["html_url"] = release_url
    except (requests.RequestException, ValueError, TypeError, AttributeError):
        pass
    return result


@router.get("/version/release-notes")
def api_installed_release_notes() -> dict[str, Any]:
    return _cached_installed_release(REPO, _norm(CURRENT_VERSION), _ttl_marker(300))


@router.get("/version")
def get_version() -> dict[str, Any]:
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
def api_version_check() -> dict[str, Any]:
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
def get_module_versions() -> dict[str, Any]:
    groups = {
        g: {name: _get_module_version(path) for name, path in mods.items()}
        for g, mods in _env_modules().items()
        if g != "AUTH"
    }
    flat: dict[str, str] = {
        name: ver for mods in groups.values() for name, ver in mods.items()
    }
    return {"groups": groups, "flat": flat}
