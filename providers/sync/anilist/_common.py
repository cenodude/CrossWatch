# /providers/sync/anilist/_common.py
# AniList Module shared helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable, Mapping


STATE_DIR = Path("/config/.cw_state")


def _pair_scope() -> str | None:
    for k in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        v = os.getenv(k)
        if v and str(v).strip():
            return str(v).strip()
    return None


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
    if _pair_scope() is None:
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


def make_logger(tag: str) -> Callable[[str], None]:
    def _log(msg: str) -> None:
        if os.getenv("CW_DEBUG") or os.getenv("CW_ANILIST_DEBUG"):
            print(f"[ANILIST:{tag}] {msg}")
    return _log
