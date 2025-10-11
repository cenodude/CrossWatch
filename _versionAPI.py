# _versionAPI.py
from __future__ import annotations

import os, re, time, requests
from functools import lru_cache
from importlib import import_module
from packaging.version import Version, InvalidVersion
from fastapi import APIRouter

router = APIRouter(prefix="/api", tags=["version"])

STATUS_CACHE = {"ts": 0.0, "data": None}
STATUS_TTL = 3600
PROBE_TTL  = 30

# env-driven defaults
CURRENT_VERSION = os.getenv("APP_VERSION", "v0.2.1")
REPO = os.getenv("GITHUB_REPO", "cenodude/CrossWatch")
GITHUB_API = f"https://api.github.com/repos/{REPO}/releases/latest"

def _env_modules():
    try:
        from cw_platform.modules_registry import MODULES
        return MODULES
    except Exception:
        return {}
        
def _norm(v: str) -> str:
    return re.sub(r"^\s*v", "", (v or "").strip(), flags=re.IGNORECASE)

def _ttl_marker(seconds: int = 300) -> int:
    return int(time.time() // seconds)

@lru_cache(maxsize=1)
def _cached_latest_release(_marker: int) -> dict:
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "CrossWatch"}
    try:
        r = requests.get(GITHUB_API, headers=headers, timeout=8)
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
        return {"latest": None, "html_url": f"https://github.com/{REPO}/releases", "body": "", "published_at": None}

def _is_update_available(current: str, latest: str) -> bool:
    if not latest:
        return False
    try:
        return Version(_norm(latest)) > Version(_norm(current))
    except InvalidVersion:
        return latest != current

def _ver_tuple(s: str):
    try:
        return tuple(int(p) for p in re.split(r"[^\d]+", (s or "").strip()) if p)
    except Exception:
        return (0,)

def _get_module_version(mod_path: str) -> str:
    try:
        m = import_module(mod_path)
        return str(getattr(m, "__VERSION__", getattr(m, "VERSION", getattr(m, "__version__", "0.0.0"))))
    except Exception:
        return "0.0.0"

@router.get("/update")
def api_update():
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
def get_version():
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
def api_version_check():
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
def get_module_versions():
    groups = {g: {name: _get_module_version(path) for name, path in mods.items()} for g, mods in _env_modules().items()}
    flat = {name: ver for mods in groups.values() for name, ver in mods.items()}
    return {"groups": groups, "flat": flat}
