# cw_platform/orchestrator/_providers.py
# provider management for orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from cw_platform.modules_registry import load_sync_ops, sync_provider_names
from ._types import InventoryOps


def load_sync_providers() -> dict[str, InventoryOps]:
    out: dict[str, InventoryOps] = {}
    needed = ("name", "label", "features", "capabilities", "build_index", "add", "remove")
    for name in sync_provider_names(upper=True):
        ops = load_sync_ops(name)
        if not ops:
            continue
        if not all(hasattr(ops, fn) for fn in needed):
            continue
        try:
            out[str(ops.name()).upper()] = ops  # type: ignore[assignment]
        except Exception:
            continue
    return out
