# /providers/sync/anilist/_common.py
# AniList Module shared helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable, Mapping

from .._log import log as cw_log

STATE_DIR = Path("/config/.cw_state")


def _pair_scope() -> str | None:
    for k in ("CW_PAIR_SCOPE", "CW_PAIR_KEY", "CW_SYNC_PAIR", "CW_PAIR"):
        v = os.getenv(k)
        if not v:
            continue
        s = str(v).strip()
        if not s:
            continue
        if s.lower() in ("unscoped", "default", "none"):
            continue
        return s
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
    p = Path(name)
    if not scope:
        if p.suffix:
            return STATE_DIR / f"{p.stem}.unscoped{p.suffix}"
        return STATE_DIR / f"{name}.unscoped"
    safe = _safe_scope(scope)
    if p.suffix:
        return STATE_DIR / f"{p.stem}.{safe}{p.suffix}"
    return STATE_DIR / f"{name}.{safe}"


def read_json(path: Path) -> dict[str, Any]:
    if _is_capture_mode() or _pair_scope() is None:
        return {}
    try:
        return json.loads(path.read_text("utf-8") or "{}")
    except Exception:
        return {}


def write_json(path: Path, data: Mapping[str, Any], *, indent: int = 2, sort_keys: bool = True) -> None:
    if _pair_scope() is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp")
        tmp.write_text(json.dumps(dict(data), ensure_ascii=False, indent=indent, sort_keys=sort_keys), "utf-8")
        os.replace(tmp, path)
    except Exception:
        pass


def make_logger(feature: str) -> Callable[..., None]:
    def _log(msg: str, level: str = "debug", **fields: Any) -> None:
        cw_log("ANILIST", str(feature), str(level), str(msg), **fields)
    return _log
