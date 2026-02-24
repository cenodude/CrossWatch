# /providers/sync/crosswatch/_common.py
# CrossWatch tracker Module shared helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import shutil
from pathlib import Path


_PAIR_SCOPE_ENV: tuple[str, ...] = ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR")


def _truthy_env(name: str) -> bool:
    v = str(os.getenv(name, "")).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def pair_scoped() -> bool:
    return _truthy_env("CW_CROSSWATCH_PAIR_SCOPED")


def _capture_mode() -> bool:
    return _truthy_env("CW_CAPTURE_MODE")


def _pair_scope() -> str | None:
    if not pair_scoped():
        return "unscoped"

    for k in _PAIR_SCOPE_ENV:
        v = os.getenv(k)
        if v and str(v).strip():
            return str(v).strip()
    return "unscoped"


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
    if not pair_scoped():
        return root / name

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
    if not pair_scoped():
        return root / "snapshots"
    return root / "snapshots" / scope_safe()


def latest_state_file(root: Path, stem: str) -> Path | None:
    candidates: list[Path] = []
    legacy = root / f"{stem}.json"
    if legacy.exists() and legacy.is_file():
        candidates.append(legacy)

    for p in root.glob(f"{stem}.*.json"):
        if not p.is_file():
            continue
        parts = p.name.split(".")
        if len(parts) != 3:
            continue
        if parts[0] != stem or parts[-1] != "json":
            continue
        candidates.append(p)

    if not candidates:
        return None
    try:
        return max(candidates, key=lambda x: x.stat().st_mtime)
    except Exception:
        return candidates[-1]


def latest_snapshot_file(root: Path, feature: str) -> Path | None:
    snaps = root / "snapshots"
    if not snaps.exists() or not snaps.is_dir():
        return None

    candidates: list[Path] = []
    for p in snaps.rglob(f"*-{feature}.json"):
        if p.is_file():
            candidates.append(p)

    if not candidates:
        return None
    try:
        return max(candidates, key=lambda x: x.stat().st_mtime)
    except Exception:
        return candidates[-1]
