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


def _ownership_reference(path: Path) -> os.stat_result | None:
    path_stat: os.stat_result | None = None
    parent_stat: os.stat_result | None = None
    config_stat: os.stat_result | None = None
    try:
        if path.exists():
            path_stat = path.stat()
    except Exception:
        path_stat = None
    try:
        if path.parent.exists():
            parent_stat = path.parent.stat()
    except Exception:
        parent_stat = None
    try:
        cfg = config_dir()
        if cfg.exists():
            config_stat = cfg.stat()
    except Exception:
        config_stat = None
    if os.name != "nt":
        try:
            if os.geteuid() == 0:
                for candidate in (config_stat, parent_stat):
                    if candidate is not None and candidate.st_uid != 0:
                        return candidate
        except Exception:
            pass
    return path_stat or parent_stat or config_stat


def _apply_owner_mode(path: Path, ref: os.stat_result | None, *, mode: int) -> None:
    if ref is None:
        return
    if os.name != "nt":
        try:
            os.chown(path, ref.st_uid, ref.st_gid)
        except Exception:
            pass
    try:
        path.chmod(mode)
    except Exception:
        pass


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
    ref = _ownership_reference(path)
    _apply_owner_mode(path.parent, ref, mode=stat.S_IRWXU)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _apply_owner_mode(path, ref, mode=stat.S_IRUSR | stat.S_IWUSR)
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
