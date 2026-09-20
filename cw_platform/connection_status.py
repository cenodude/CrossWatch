# cw_platform/connection_status.py
# CrossWatch - Persistent provider connection state shared by probes and syncs
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import os
import secrets
import sys
import threading
import time
from pathlib import Path
from typing import Any, Mapping

from cw_platform.config_base import CONFIG_BASE
from cw_platform.provider_instances import provider_key

_GUARD = threading.Lock()
_LOCKS: dict[str, Any] = {}
_BOOT_ID = secrets.token_hex(16)
_FIELDS = (
    "account_token", "access_token", "token", "api_key", "key", "auth_key", "authKey",
    "client_id", "session_id", "server", "server_url", "base_url", "url", "root_dir",
    "profile_id", "stremio_profile_id", "user_id", "username", "password", "webhook_url",
    "auth_method", "oauth", "enabled", "connected", "connection_verified", "verify_ssl",
)


def _name(provider: str) -> str:
    name = provider_key(provider)
    return "tmdb" if name == "tmdb_sync" else name


def identity(provider: str, cfg: Mapping[str, Any]) -> str:
    name = _name(provider)
    block = (cfg.get("tmdb_sync") if name in {"tmdb", "tmdb_sync"} else None) or cfg.get(name) or cfg.get(str(provider).upper()) or {}
    auth = (cfg.get("auth") or {}).get(name) or {}
    values = {field: block.get(field, auth.get(field)) for field in _FIELDS}
    digest = hashlib.sha256(json.dumps(values, sort_keys=True, default=str).encode()).hexdigest()
    return f"{name}.{digest}"


def _root() -> Path:
    return CONFIG_BASE() / "state" / "connections"


def lock_for(provider: str, cfg: Mapping[str, Any]) -> Any:
    key = str(_root() / identity(provider, cfg))
    with _GUARD:
        return _LOCKS.setdefault(key, threading.RLock())


def _read(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text("utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _write(path: Path, data: Mapping[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(f".{threading.get_ident()}.tmp")
        temp.write_text(json.dumps(dict(data)), "utf-8")
        os.replace(temp, path)
    except OSError:
        pass


def read(provider: str, cfg: Mapping[str, Any]) -> dict[str, Any]:
    with lock_for(provider, cfg):
        data = _read(_root() / f"{identity(provider, cfg)}.json")
        invalidated = _read(_root() / f"{_name(provider)}.invalidated.json").get("ts", 0)
        if float(data.get("checked_at") or 0) < float(invalidated):
            return {}
        return data


def update(provider: str, cfg: Mapping[str, Any], **fields: Any) -> None:
    with lock_for(provider, cfg):
        path = _root() / f"{identity(provider, cfg)}.json"
        data = _read(path)
        data.update(fields)
        if "checked_at" in fields:
            data["boot_id"] = _BOOT_ID
        _write(path, data)
    bust_status()


def checked_this_boot(data: Mapping[str, Any]) -> bool:
    return data.get("boot_id") == _BOOT_ID


def bust_status() -> None:
    probes = sys.modules.get("api.probesAPI")
    if probes is not None:
        probes.STATUS_CACHE["ts"] = 0.0
        probes.STATUS_CACHE["data"] = None
        probes.STATUS_SCOPE_CACHE.clear()


def invalidate(provider: str) -> None:
    _write(_root() / f"{_name(provider)}.invalidated.json", {"ts": time.time()})
    bust_status()


def record_health(provider: str, cfg: Mapping[str, Any], health: Mapping[str, Any]) -> None:
    if "ok" not in health or provider_key(provider) in {"simkl", "crosswatch"}:
        return
    status = str(health.get("status") or "").lower()
    details = str(health.get("details") or "")
    if status == "cancelled" or any(word in details.lower() for word in ("daily_limit", "quota", "rate limit")):
        return
    update(provider, cfg, connected=bool(health["ok"]), reason="" if health["ok"] else details or status,
           checked_at=time.time())


def record_response(provider: str, cfg: Mapping[str, Any], status: int) -> None:
    if status == 401 or 200 <= status < 300:
        update(provider, cfg, connected=status != 401,
               reason="unauthorized" if status == 401 else "", checked_at=time.time())
