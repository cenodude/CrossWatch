# cw_platform/orchestrator/_providers.py
# provider management for orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path
from types import ModuleType
from collections.abc import Iterator

from ._types import InventoryOps


def _iter_sync_modules() -> Iterator[ModuleType]:
    import providers.sync as syncpkg  # user package

    pkg_path = Path(syncpkg.__file__).parent
    for m in pkgutil.iter_modules([str(pkg_path)]):
        if not m.name.startswith("_mod_"):
            continue
        try:
            yield importlib.import_module(f"providers.sync.{m.name}")
        except Exception as e:
            print(f"[providers] load_failed {m.name}: {e}")


def _resolve_ops_from_module(mod: ModuleType) -> InventoryOps | None:
    needed = ("name", "label", "features", "capabilities", "build_index", "add", "remove")
    for attr in ("OPS", "ADAPTER", "ProviderOps", "InventoryOps"):
        obj = getattr(mod, attr, None)
        if obj and all(hasattr(obj, fn) for fn in needed):
            return obj  # type: ignore[return-value]
    return None


def load_sync_providers() -> dict[str, InventoryOps]:
    out: dict[str, InventoryOps] = {}
    for mod in _iter_sync_modules():
        ops = _resolve_ops_from_module(mod)
        if not ops:
            continue
        try:
            out[str(ops.name()).upper()] = ops
        except Exception:
            continue
    return out
