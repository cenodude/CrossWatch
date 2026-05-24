# providers/sync/publicmetadb/_common.py
# PUBLICMETADB Module for common functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping

from .._log import log as cw_log

STATE_DIR = Path("/config/.cw_state")
STATE_DIR.mkdir(parents=True, exist_ok=True)


def _pair_scope() -> str | None:
    for k in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        v = os.getenv(k)
        if v and str(v).strip():
            return str(v).strip()
    return None


def _is_capture_mode() -> bool:
    v = str(os.getenv("CW_CAPTURE_MODE") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _safe_scope(value: str) -> str:
    s = "".join(ch if (ch.isalnum() or ch in ("-", "_", ".")) else "_" for ch in str(value))
    s = s.strip("_ ")
    while "__" in s:
        s = s.replace("__", "_")
    return s[:96] if s else "default"


def state_file(name: str) -> Path:
    scope = _pair_scope()
    safe = _safe_scope(scope) if scope else "unscoped"
    p = Path(name)
    if p.suffix:
        return STATE_DIR / f"{p.stem}.{safe}{p.suffix}"
    return STATE_DIR / f"{name}.{safe}"


def read_json(path: Path) -> dict[str, Any]:
    if _is_capture_mode():
        return {}
    try:
        data = json.loads(path.read_text("utf-8") or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def write_json(path: Path, data: Mapping[str, Any], *, indent: int | None = 2, sort_keys: bool = True) -> None:
    if _is_capture_mode():
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp")
        tmp.write_text(json.dumps(dict(data), ensure_ascii=False, indent=indent, sort_keys=sort_keys), "utf-8")
        os.replace(tmp, path)
    except Exception as e:
        cw_log("PUBLICMETADB", "state", "warn", "state_write_failed", path=str(path), error=str(e))


def cfg_section(adapter: Any) -> Mapping[str, Any]:
    cfg = getattr(adapter, "config", {}) or {}
    if isinstance(cfg, dict) and isinstance(cfg.get("publicmetadb"), dict):
        return cfg["publicmetadb"]
    return {}


def as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        s = str(value).strip()
        return int(s) if s else None
    except Exception:
        return None


def media_type_for_item(item: Mapping[str, Any]) -> str:
    typ = str(item.get("type") or item.get("media_type") or "").strip().lower()
    if typ in ("tv", "show", "shows", "series"):
        return "tv"
    return "movie"


def tmdb_id_for_item(item: Mapping[str, Any]) -> int | None:
    ids_obj = item.get("ids")
    ids: Mapping[str, Any] = ids_obj if isinstance(ids_obj, Mapping) else {}
    return as_int(ids.get("tmdb") or item.get("tmdb") or item.get("tmdb_id"))
