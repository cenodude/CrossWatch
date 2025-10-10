from __future__ import annotations
import importlib, pkgutil
from pathlib import Path
from typing import Dict, Optional
from ._types import InventoryOps

def _iter_sync_modules():
    import providers.sync as syncpkg  # user package
    pkg_path = Path(syncpkg.__file__).parent
    for m in pkgutil.iter_modules([str(pkg_path)]):
        if not m.name.startswith("_mod_"): continue
        try:
            yield importlib.import_module(f"providers.sync.{m.name}")
        except Exception as e:
            print(f"[providers] load_failed {m.name}: {e}")

def _resolve_ops_from_module(mod) -> Optional[InventoryOps]:
    for attr in ("OPS", "ADAPTER", "ProviderOps", "InventoryOps"):
        obj = getattr(mod, attr, None)
        needed = ("name","label","features","capabilities","build_index","add","remove")
        if obj and all(hasattr(obj, fn) for fn in needed):
            return obj  # type: ignore
    return None

def load_sync_providers() -> Dict[str, InventoryOps]:
    out: Dict[str, InventoryOps] = {}
    for mod in _iter_sync_modules():
        ops = _resolve_ops_from_module(mod)
        if not ops: continue
        try: out[str(ops.name()).upper()] = ops  # type: ignore
        except Exception: continue
    return out
