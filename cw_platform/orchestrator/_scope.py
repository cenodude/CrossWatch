# cw_platform/orchestrator/_scope.py
# pair scope helpers for orchestrator state files.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import hashlib
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

def scope_safe(instance: str | None = None, *, pair: str | None = None) -> str:
    raw = pair or pair_scope()
    scope = safe_scope(raw) if raw else "unscoped"
    if instance is not None:
        suffix = hashlib.sha256(str(instance).encode("utf-8")).hexdigest()[:20]
        return f"{scope[:70]}_i{suffix}"
    return scope

def scoped_file(root: Path, name: str, *, migrate: bool = True, instance: str | None = None) -> Path:
    scope = scope_safe(instance)
    p = Path(name)
    if p.suffix:
        scoped = root / f"{p.stem}.{scope}{p.suffix}"
    else:
        scoped = root / f"{name}.{scope}"
    if migrate and instance is None and not scope.startswith("cw2_"):
        legacy = root / name
        if not scoped.exists() and legacy.exists():
            try:
                root.mkdir(parents=True, exist_ok=True)
                shutil.copy2(legacy, scoped)
            except Exception:
                pass
    return scoped


def provider_call(fn, config, *args, **kwargs):
    instance = (config or {}).get("_cw_provider_instance")
    if instance is None:
        return fn(config, *args, **kwargs)
    scope = scope_safe(instance, pair=config.get("_cw_pair_scope"))
    previous = {key: os.environ.get(key) for key in _ENV_KEYS}
    try:
        for key in _ENV_KEYS:
            os.environ[key] = scope
        return fn(config, *args, **kwargs)
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
