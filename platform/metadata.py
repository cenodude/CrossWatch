from __future__ import annotations
import importlib, pkgutil
from typing import Any, Dict, Optional
from _logging import log

class MetadataManager:
    def __init__(self, load_cfg, save_cfg):
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self.providers = self._discover()

    def _discover(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        try:
            import providers.metadata as md
        except Exception as e:
            log(f"Metadata package missing: {e}", level="ERROR", module="META")
            return out
        for p in getattr(md, "__path__", []):
            for m in pkgutil.iter_modules([str(p)]):
                if not m.name.startswith("_meta_"): continue
                mod = importlib.import_module(f"providers.metadata.{m.name}")
                inst = getattr(mod, "PROVIDER", None)
                if inst is None and hasattr(mod, "build"):
                    try:
                        inst = mod.build(self.load_cfg, self.save_cfg)
                    except Exception as e:
                        log(f"Provider build failed for {m.name}: {e}", level="ERROR", module="META")
                        continue
                if inst:
                    name = getattr(inst, "name", m.name.replace("_meta_",""))
                    out[name.upper()] = inst
        return out

    def resolve(self, *, entity: str, ids: Dict[str,str], locale: Optional[str] = None,
                need: Optional[Dict[str,bool]] = None, strategy: str = "first_success") -> dict:
        cfg = self.load_cfg() or {}
        md_cfg = cfg.get("metadata") or {}
        order = md_cfg.get("priority") or list(self.providers.keys())
        need = need or {"poster": True, "backdrop": True}
        results: list[dict] = []
        for name in order:
            prov = self.providers.get(name.upper())
            if not prov: continue
            try:
                res = prov.fetch(entity=entity, ids=ids, locale=locale, need=need)
                if res:
                    if strategy == "first_success": return res
                    results.append(res)
            except Exception as e:
                log(f"Provider {name} error: {e}", level="WARNING", module="META")
                continue
        if not results: return {}
        if strategy == "merge":
            return self._merge(results)
        return results[0]

    def _merge(self, results: list[dict]) -> dict:
        out: dict = {}
        for r in results:
            for k,v in r.items():
                if k == "images":
                    out.setdefault("images", {})
                    for kind, arr in v.items():
                        out["images"].setdefault(kind, [])
                        seen = {x["url"] for x in out["images"][kind]}
                        for x in arr:
                            if x["url"] not in seen:
                                out["images"][kind].append(x)
                else:
                    out.setdefault(k, v)
        return out
