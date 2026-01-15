# cw_platform/orchestrator/_scope.py
# pair scope helpers for orchestrator state files.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import shutil
from pathlib import Path

_ENV_KEYS: tuple[str, ...] = ("CW_PAIR_SCOPE", "CW_PAIR_KEY", "CW_SYNC_PAIR", "CW_PAIR")

def pair_scope() -> str | None:
    for k in _ENV_KEYS:
        v = os.getenv(k)
        if v and str(v).strip():
            return str(v).strip()
    return None

def safe_scope(value: str) -> str:
    s = "".join(ch if (ch.isalnum() or ch in ("-", "_", ".")) else "_" for ch in str(value))
    s = s.strip("_ ")
    while "__" in s:
        s = s.replace("__", "_")
    return s[:96] if s else "default"

def scope_safe() -> str:
    raw = pair_scope()
    return safe_scope(raw) if raw else "unscoped"

def scoped_file(root: Path, name: str, *, migrate: bool = True) -> Path:
    scope = scope_safe()
    p = Path(name)
    if p.suffix:
        scoped = root / f"{p.stem}.{scope}{p.suffix}"
    else:
        scoped = root / f"{name}.{scope}"
    if migrate:
        legacy = root / name
        if not scoped.exists() and legacy.exists():
            try:
                root.mkdir(parents=True, exist_ok=True)
                shutil.copy2(legacy, scoped)
            except Exception:
                pass
    return scoped
