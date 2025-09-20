from __future__ import annotations

import importlib
import pkgutil
from typing import Any, Optional

from _logging import log


class MetadataManager:
    """
    Aggregates metadata from pluggable providers under providers/metadata/_meta_*.py.

    Provider module contract (any one of):
      - build(load_cfg, save_cfg) -> provider_instance
      - PROVIDER (class): will be instantiated with (load_cfg, save_cfg)
      - PROVIDER (instance)

    Provider instance API (at least one of):
      - fetch(entity: str, ids: dict, locale: Optional[str], need: dict) -> dict
      - resolve(entity: str, ids: dict, locale: Optional[str], need: dict) -> dict  # legacy

    Entities:
      - We normalize to "movie" | "show" (aliases "tv"/"series" map to "show").
    """

    def __init__(self, load_cfg, save_cfg):
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self.providers: dict[str, Any] = self._discover()

    # ------------------------------------------------------------------ Discovery

    def _discover(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        try:
            import providers.metadata as md  # noqa: F401
        except Exception as e:
            log(f"Metadata package missing: {e}", level="ERROR", module="META")
            return out

        for p in getattr(md, "__path__", []):
            for m in pkgutil.iter_modules([str(p)]):
                name = m.name
                if not name.startswith("_meta_"):
                    continue

                try:
                    mod = importlib.import_module(f"providers.metadata.{name}")
                except Exception as e:
                    log(f"Import failed for {name}: {e}", level="ERROR", module="META")
                    continue

                inst = getattr(mod, "PROVIDER", None)
                built = None

                # Preferred: factory function
                if hasattr(mod, "build"):
                    try:
                        built = mod.build(self.load_cfg, self.save_cfg)
                    except Exception as e:
                        log(f"Provider build failed for {name}: {e}", level="ERROR", module="META")

                if built is not None:
                    inst = built
                elif isinstance(inst, type):
                    # Class provided: instantiate
                    try:
                        inst = inst(self.load_cfg, self.save_cfg)
                    except Exception as e:
                        log(f"Provider init failed for {name}: {e}", level="ERROR", module="META")
                        inst = None

                if inst is None:
                    continue

                label = getattr(inst, "name", name.replace("_meta_", "")) or name.replace("_meta_", "")
                out[label.upper()] = inst

        return out

    # ------------------------------------------------------------------ Resolve

    def resolve(
        self,
        *,
        entity: str,
        ids: dict,
        locale: Optional[str] = None,
        need: Optional[dict] = None,
        strategy: str = "first_success",
    ) -> dict:
        """
        Resolve metadata using configured providers in priority order.

        Parameters
        ----------
        entity : str
            "movie" | "show" (aliases: "tv"/"series" -> "show").
        ids : dict
            Identifier map, e.g. {"tmdb": "123", "imdb": "tt..."}; providers choose what they can use.
        locale : Optional[str]
            Locale override (e.g., "nl-NL"). If None, falls back to config (metadata.locale or ui.locale).
        need : Optional[dict]
            Field request flags. Keep defaults lean; request rich fields explicitly.
            Example: {"poster": True, "backdrop": True, "title": True, "year": True, "overview": True}
        strategy : str
            "first_success" (default) or "merge".
              - first_success: return the first non-empty provider result.
              - merge: field-wise merge; images concatenated/deduped by URL, scalars use first non-empty.

        Returns
        -------
        dict
            Normalized metadata dictionary. Empty dict on failure (fail-soft).
        """
        cfg = self.load_cfg() or {}
        md_cfg = cfg.get("metadata") or {}

        # Normalize entity
        e = str(entity or "").lower()
        entity = {"series": "show", "tv": "show", "shows": "show", "movies": "movie"}.get(e, e)
        if entity not in ("movie", "show"):
            entity = "movie"

        # Determine priority
        default_order = list(self.providers.keys())  # discovery order
        configured = md_cfg.get("priority") or default_order
        order = [str(x).upper() for x in configured if str(x).upper() in self.providers]

        # Default lean request
        req_need = need or {"poster": True, "backdrop": True, "title": True, "year": True}
        eff_locale = locale or md_cfg.get("locale") or (cfg.get("ui") or {}).get("locale")

        results: list[dict] = []
        for name in order:
            prov = self.providers.get(name)
            if not prov:
                continue

            try:
                if hasattr(prov, "fetch"):
                    r = prov.fetch(entity=entity, ids=ids, locale=eff_locale, need=req_need) or {}
                else:
                    # Compatibility shim
                    resolver = getattr(prov, "resolve", None)
                    r = resolver(entity=entity, ids=ids, locale=eff_locale, need=req_need) if callable(resolver) else {}

                if not r:
                    continue

                # Ensure type is normalized in the payload (helps callers)
                if "type" not in r:
                    r["type"] = entity

                if strategy == "first_success":
                    log(f"Provider {name} hit", module="META")
                    return r

                results.append(r)

            except Exception as e:
                log(f"Provider {name} error: {e}", level="WARNING", module="META")
                continue

        if not results:
            return {}

        return self._merge(results) if strategy == "merge" else (results[0] or {})

    # ------------------------------------------------------------------ Merge policy

    def _merge(self, results: list[dict]) -> dict:
        """
        Merge multiple provider payloads:
          - images.* : concatenate and de-duplicate by URL, preserving order
          - scalars  : first non-empty value wins
        """
        out: dict = {}
        for r in results:
            if not isinstance(r, dict):
                continue

            for k, v in r.items():
                if k == "images" and isinstance(v, dict):
                    dst = out.setdefault("images", {})
                    for kind, arr in v.items():
                        bucket = dst.setdefault(kind, [])
                        # dedupe by url with stable order
                        seen = {x.get("url") for x in bucket if isinstance(x, dict)}
                        for x in (arr or []):
                            url = x.get("url") if isinstance(x, dict) else None
                            if not url or url in seen:
                                continue
                            bucket.append(x)
                else:
                    if k not in out and v not in (None, "", [], {}):
                        out[k] = v

        # If no 'type' made it through but we can infer from any input, keep a sane default
        if "type" not in out:
            for r in results:
                t = str(r.get("type") or "").lower()
                if t in ("movie", "show"):
                    out["type"] = t
                    break

        return out
