# /providers/sync/crosswatch/_common.py
# CrossWatch tracker Module shared helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import shutil
from pathlib import Path


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


def scope_safe() -> str:
    scope = _pair_scope()
    return _safe_scope(scope) if scope else "unscoped"


def scoped_file(root: Path, name: str) -> Path:
    safe = scope_safe()
    p = Path(name)

    if p.suffix:
        scoped = root / f"{p.stem}.{safe}{p.suffix}"
        legacy = root / f"{p.stem}{p.suffix}"
    else:
        scoped = root / f"{name}.{safe}"
        legacy = root / name

    # Auto-migrate legacy unscoped state to scoped file
    if not scoped.exists() and legacy.exists():
        try:
            root.mkdir(parents=True, exist_ok=True)
            shutil.copy2(legacy, scoped)
        except Exception:
            pass

    return scoped


def scoped_snapshots_dir(root: Path) -> Path:
    return root / "snapshots" / scope_safe()
