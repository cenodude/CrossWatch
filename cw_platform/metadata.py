# /cw_platform/metadata.py
from __future__ import annotations

import importlib, pkgutil
from typing import Any, Optional, List, Dict

try:
    from _logging import log  # type: ignore
except Exception:
    def log(msg: str, *, level: str = "INFO", module: str = "META"): pass  # noop

try:
    from id_map import ids_from_guid
except Exception:
    def ids_from_guid(_g: str) -> dict: return {}

try:
    from id_map import merge_ids as _merge_ids, KEY_PRIORITY as _KEY_PRIORITY  # type: ignore
except Exception:
    _KEY_PRIORITY = ("imdb", "tmdb", "tvdb", "trakt", "plex", "guid", "slug", "simkl")
    def _merge_ids(old: dict | None, new: dict | None) -> dict:
        old, new = (old or {}), (new or {})
        out: Dict[str, Any] = {}
        for k in _KEY_PRIORITY:
            out[k] = old.get(k) or new.get(k) or out.get(k)
        for k, v in new.items():
            if k not in out or out[k] is None: out[k] = v
        for k, v in old.items():
            if k not in out or out[k] is None: out[k] = v
        return {k: v for k, v in out.items() if v}

# helpers

def _norm_ids(ids: dict | None) -> dict:
    out = {}
    for k, v in (ids or {}).items():
        if v in (None, "", [], {}): 
            continue
        if isinstance(v, (int, float)):
            v = str(int(v)) if isinstance(v, float) and v.is_integer() else str(v)
        elif not isinstance(v, str):
            v = str(v)
        else:
            v = v.strip()
        out[str(k).lower()] = v
    return out

def _norm_entity(entity: Optional[str]) -> str:
    e = str(entity or "").strip().lower()
    return {"series": "show", "tv": "show", "shows": "show", "movies": "movie"}.get(e, e if e in ("movie", "show") else "movie")

def _norm_need(need: Optional[dict]) -> dict:
    n = dict(need or {})
    if n.get("images") and not any(n.get(k) for k in ("poster", "backdrop", "logo")): n["poster"] = True
    return n or {"poster": True, "backdrop": True, "title": True, "year": True}

def _first_non_empty(*vals):
    for v in vals:
        if v not in (None, "", [], {}): return v
    return None

# Meta Manager

class MetadataManager:
    def __init__(self, load_cfg, save_cfg):
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self.providers: dict[str, Any] = self._discover()

    # Discovery
    def _discover(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        try:
            import providers.metadata as md  # noqa: F401
        except Exception as e:
            log(f"Metadata package missing: {e}", level="ERROR", module="META"); return out

        for p in getattr(md, "__path__", []):
            for m in pkgutil.iter_modules([str(p)]):
                name = m.name
                if not name.startswith("_meta_"): continue
                try: mod = importlib.import_module(f"providers.metadata.{name}")
                except Exception as e:
                    log(f"Import failed for {name}: {e}", level="ERROR", module="META"); continue

                inst = getattr(mod, "PROVIDER", None); built = None
                if hasattr(mod, "build"):
                    try: built = mod.build(self.load_cfg, self.save_cfg)
                    except Exception as e: log(f"Provider build failed for {name}: {e}", level="ERROR", module="META")
                if built is not None:
                    inst = built
                elif isinstance(inst, type):
                    try: inst = inst(self.load_cfg, self.save_cfg)
                    except Exception as e:
                        log(f"Provider init failed for {name}: {e}", level="ERROR", module="META"); inst = None
                if inst is None: continue

                label = getattr(inst, "name", name.replace("_meta_", "")) or name.replace("_meta_", "")
                out[label.upper()] = inst
        return out

    # Resolve it through providers

    def resolve(self, *, entity: str, ids: dict, locale: Optional[str] = None,
                need: Optional[dict] = None, strategy: str = "first_success") -> dict:
        cfg = self.load_cfg() or {}
        md_cfg = cfg.get("metadata") or {}
        debug = bool((cfg.get("runtime") or {}).get("debug"))
        entity = _norm_entity(entity); req_need = _norm_need(need)
        eff_locale = locale or md_cfg.get("locale") or (cfg.get("ui") or {}).get("locale")
        
        ids = _norm_ids(ids)

        default_order = list(self.providers.keys())
        order = [str(x).upper() for x in (md_cfg.get("priority") or default_order) if str(x).upper() in self.providers]

        results: List[dict] = []
        for name in order:
            prov = self.providers.get(name)
            if not prov: continue
            try:
                if hasattr(prov, "fetch"):
                    r = prov.fetch(entity=entity, ids=ids, locale=eff_locale, need=req_need) or {}
                else:
                    resolver = getattr(prov, "resolve", None)
                    r = resolver(entity=entity, ids=ids, locale=eff_locale, need=req_need) if callable(resolver) else {}
                if not r: continue
                if "type" not in r: r["type"] = entity
                if strategy == "first_success":
                    if debug: log(f"Provider {name} hit", level="DEBUG", module="META")
                    return r
                results.append(r)
            except Exception as e:
                log(f"Provider {name} error: {e}", level="WARNING", module="META"); continue
        if not results: return {}
        return self._merge(results) if strategy == "merge" else (results[0] or {})

    # Resolve in batch
    def resolve_many(self, items: List[dict]) -> List[dict]:
        out: List[dict] = []
        for it in items or []:
            ids = dict(it.get("ids") or {})
            g = ids.get("guid")
            if g:
                try: ids.update(ids_from_guid(g))
                except Exception: pass
            ids = _norm_ids(ids)
            ent = _norm_entity((it.get("type") or it.get("entity") or "movie").rstrip("s"))
            title, year = it.get("title"), it.get("year")
            try:
                r = self.resolve(entity=ent, ids=ids, need={"ids": True}) if ids \
                    else self.resolve(entity=ent, ids={}, need={"title": True, "year": True, "ids": True})
            except Exception:
                r = None
            if r:
                r_ids = dict(r.get("ids") or {})
                out.append({
                    "type": r.get("type") or ent,
                    "title": _first_non_empty(r.get("title"), title),
                    "year": _first_non_empty(r.get("year"), year),
                    "ids": _merge_ids(ids, r_ids)
                })
            else:
                it2 = dict(it); it2["ids"] = ids; out.append(it2)
        return out

    # Reconcile
    def reconcile_ids(self, items: List[dict]) -> List[dict]:
        healed: List[dict] = []
        for it in items or []:
            ent = _norm_entity((it.get("type") or it.get("entity") or "movie").rstrip("s"))
            ids: Dict[str, Any] = _norm_ids(dict(it.get("ids") or {}))
            title, year = it.get("title"), it.get("year")

            try:
                r = {}
                if ent == "movie":
                    if ids.get("imdb"):
                        r = self.resolve(entity="movie", ids={"imdb": ids["imdb"]}, need={"ids": True})
                    elif ids.get("tmdb"):
                        r = self.resolve(entity="movie", ids={"tmdb": ids["tmdb"]}, need={"ids": True})
                    elif title:
                        payload = {"title": title}; 
                        if year: payload["year"] = year
                        r = self.resolve(entity="movie", ids=payload, need={"ids": True})
                else:
                    if ids.get("tmdb"):
                        r = self.resolve(entity="show", ids={"tmdb": ids["tmdb"]}, need={"ids": True})
                    elif ids.get("imdb"):
                        r = self.resolve(entity="show", ids={"imdb": ids["imdb"]}, need={"ids": True})
                    elif title:
                        payload = {"title": title}; 
                        if year: payload["year"] = year
                        r = self.resolve(entity="show", ids=payload, need={"ids": True})
            except Exception:
                r = {}

            rid = _norm_ids(dict((r or {}).get("ids") or {}))
            ids = _merge_ids(ids, rid)

            healed.append({"type": ent, "title": title, "year": year, "ids": ids})
        return healed

    # Merge policy
    def _merge(self, results: List[dict]) -> dict:
        out: dict = {}
        for r in results:
            if not isinstance(r, dict): continue
            for k, v in r.items():
                if k == "images" and isinstance(v, dict):
                    dst = out.setdefault("images", {})
                    for kind, arr in v.items():
                        bucket = dst.setdefault(kind, [])
                        seen = {(x.get("url") or x.get("file_path") or x.get("path")) for x in bucket if isinstance(x, dict)}
                        for x in (arr or []):
                            if not isinstance(x, dict): continue
                            key = x.get("url") or x.get("file_path") or x.get("path")
                            if not key or key in seen: continue
                            bucket.append(x); seen.add(key)
                else:
                    if k not in out and v not in (None, "", [], {}): out[k] = v
        if "type" not in out:
            for r in results:
                t = _norm_entity(r.get("type"))
                if t in ("movie", "show"): out["type"] = t; break
        return out
