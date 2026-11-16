# providers/sync/_continue_watching.py
# CrossWatch - Continue Watching row reads and hides per media server, and their capability registry.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import importlib
from collections.abc import Iterable, Mapping
from types import ModuleType
from typing import Any

HIDE_UNITS = {"emby": "series", "plex": "item", "tofa": "title", "silo": "series"}
SHOWN = "_cw_continue_shown"
OPTION = "continue_watching_removals"


def _key(provider: str) -> str:
    return str(provider or "").strip().lower()


def supported(provider: str) -> bool:
    return _key(provider) in HIDE_UNITS


def module(provider: str) -> ModuleType | None:
    key = _key(provider)
    if key not in HIDE_UNITS:
        return None
    return importlib.import_module(f"providers.sync.{key}._continue")


def provider_caps(provider: str) -> dict[str, Any]:
    key = _key(provider)
    if key not in HIDE_UNITS:
        return {}
    return {"opt_in": f"progress.{OPTION}", "hide_unit": HIDE_UNITS[key], "beta": True}


def read_view(provider: str, ops: Any, cfg: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    mod = module(provider)
    if mod is None:
        raise RuntimeError(f"continue_watching_unsupported:{provider}")
    return dict(mod.view(ops._adapter(cfg)))


def hide(provider: str, ops: Any, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    mod = module(provider)
    if mod is None:
        raise RuntimeError(f"continue_watching_unsupported:{provider}")
    return dict(mod.hide(ops._adapter(cfg), list(items)))
