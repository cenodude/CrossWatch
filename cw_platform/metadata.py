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
                if not m.name.startswith("_meta_"):
                    continue
                mod = importlib.import_module(f"providers.metadata.{m.name}")

                inst = getattr(mod, "PROVIDER", None)
                built = None
                if hasattr(mod, "build"):
                    try:
                        built = mod.build(self.load_cfg, self.save_cfg)
                    except Exception as e:
                        log(f"Provider build failed for {m.name}: {e}", level="ERROR", module="META")

                if built is not None:
                    inst = built
                elif isinstance(inst, type):  # PROVIDER is a class → instantiate
                    try:
                        inst = inst(self.load_cfg, self.save_cfg)
                    except Exception as e:
                        log(f"Provider init failed for {m.name}: {e}", level="ERROR", module="META")
                        inst = None

                if inst is None:
                    continue

                name = getattr(inst, "name", m.name.replace("_meta_", ""))
                out[name.upper()] = inst
        return out

    
    def resolve(self, *, entity: str, ids: dict, locale: Optional[str] = None,
                need: Optional[dict] = None, strategy: str = "first_success") -> dict:
        cfg = self.load_cfg() or {}
        md_cfg = cfg.get("metadata") or {}

        # normalize
        entity = {"show":"tv","series":"tv","movies":"movie"}.get(str(entity).lower(), str(entity).lower())
        order = [x.upper() for x in (md_cfg.get("priority") or self.providers.keys())]
        need = need or {"poster": True, "backdrop": True, "title": True, "year": True}
        locale = locale or md_cfg.get("locale") or (cfg.get("ui") or {}).get("locale")

        results: list[dict] = []
        for name in order:
            prov = self.providers.get(name.upper())
            if not prov:
                continue
            try:
                res = prov.fetch(entity=entity, ids=ids, locale=locale, need=need)
                if res:
                    if strategy == "first_success":
                        log(f"Provider {name} hit", module="META")
                        return res
                    results.append(res)
            except Exception as e:
                log(f"Provider {name} error: {e}", level="WARNING", module="META")
                continue

        if not results:
            raise ValueError(f"No metadata for {entity} ids={ids}")

        return self._merge(results) if strategy == "merge" else results[0]

    def _merge(self, results: list[dict]) -> dict:
        out: dict = {}
        for r in results:
            for k, v in r.items():
                if k == "images" and isinstance(v, dict):
                    out.setdefault("images", {})
                    for kind, arr in v.items():
                        out["images"].setdefault(kind, [])
                        seen = {x.get("url") for x in out["images"][kind] if isinstance(x, dict)}
                        for x in (arr or []):
                            url = x.get("url") if isinstance(x, dict) else None
                            if not url or url in seen:
                                continue
                            out["images"][kind].append(x)
                else:
                    if k not in out and v not in (None, ""):
                        out[k] = v
        return out