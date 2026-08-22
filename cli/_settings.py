# /cli/_settings.py
# CrossWatch - CLI endpoint and token resolution
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from typing import Any

from ._util import as_dict

DEFAULT_PORT = 8787
DEFAULT_HOST = "127.0.0.1"


def cli_home() -> Path:
    raw = os.getenv("CW_CLI_HOME") or ""
    if raw.strip():
        return Path(raw.strip()).expanduser()
    runtime = os.getenv("RUNTIME_DIR") or ""
    if runtime.strip():
        return Path(runtime.strip()).expanduser() / ".cw_cli"
    config = Path("/config")
    if config.is_dir():
        return config / ".cw_cli"
    return Path.home() / ".crosswatch"


def settings_path() -> Path:
    return cli_home() / "cli.json"


def load_settings() -> dict[str, Any]:
    path = settings_path()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return as_dict(raw)


def save_settings(data: dict[str, Any]) -> Path:
    path = settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    try:
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    except Exception:
        pass
    return path


def update_settings(**changes: Any) -> dict[str, Any]:
    data = load_settings()
    for key, value in changes.items():
        if value is None:
            data.pop(key, None)
        else:
            data[key] = value
    save_settings(data)
    return data


def config_dir() -> Path:
    raw = os.getenv("CONFIG_BASE") or os.getenv("RUNTIME_DIR") or ""
    if raw.strip():
        return Path(raw.strip()).expanduser()
    docker = Path("/config")
    if docker.is_dir():
        return docker
    return Path(__file__).resolve().parent.parent


def _config_snapshot() -> dict[str, Any]:
    try:
        raw = json.loads((config_dir() / "config.json").read_text(encoding="utf-8"))
    except Exception:
        return {}
    return as_dict(raw)


def discover_base_url() -> str:
    cfg = _config_snapshot()
    ui = as_dict(cfg.get("ui"))
    protocol = str(ui.get("protocol") or "http").strip().lower()
    if protocol not in ("http", "https"):
        protocol = "http"
    try:
        port = int(os.getenv("WEB_PORT") or DEFAULT_PORT)
    except Exception:
        port = DEFAULT_PORT
    return f"{protocol}://{DEFAULT_HOST}:{port}"


def normalize_url(raw: str) -> str:
    url = str(raw or "").strip().rstrip("/")
    if not url:
        return ""
    if "://" not in url:
        url = "http://" + url
    return url


def resolve_url(explicit: str = "") -> str:
    if explicit.strip():
        return normalize_url(explicit)
    env = os.getenv("CW_URL") or os.getenv("CROSSWATCH_URL") or ""
    if env.strip():
        return normalize_url(env)
    stored = str(load_settings().get("url") or "")
    if stored.strip():
        return normalize_url(stored)
    return discover_base_url()


def resolve_token(explicit: str = "") -> str:
    if explicit.strip():
        return explicit.strip()
    env = os.getenv("CW_TOKEN") or os.getenv("CROSSWATCH_TOKEN") or ""
    if env.strip():
        return env.strip()
    return str(load_settings().get("token") or "").strip()
