from __future__ import annotations

import importlib
import pkgutil
from typing import Any, Optional

from _logging import log


class MetadataManager:
    """
    Aggregates metadata from pluggable providers under providers/metadata/_meta_*.py.
    - Providers may expose:
        * build(load_cfg, save_cfg) -> instance
        * or a PROVIDER class (instantiated with load_cfg, save_cfg)
        * or a PROVIDER instance
    - Provider interface:
        fetch(entity: str, ids: dict, locale: Optional[str], need: dict) -> dict
      (If a provider implements resolve(...), we call that as a compatibility path.)
    """

    def __init__(self, load_cfg, save_cfg):
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self.providers = self._discover()

    # ------------------------------ Discovery ------------------------------

    def _discover(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        try:
            import providers.metadata as md  # noqa: F401
        except Exception as e:
            log(f"Metadata package missing: {e}", level="ERROR", module="META")
            return out

        for p in getattr(md, "__path__", []):
            for m in pkgutil.iter_modules([str(p)]):
                if not m.name.startswith("_meta_"):
                    continue

                mod = None
                try:
                    mod = importlib.import_module(f"providers.metadata.{m.name}")
                except Exception as e:
                    log(f"Import failed for {m.name}: {e}", level="ERROR", module="META")
                    continue

                inst = getattr(mod, "PROVIDER", None)
                built = None

                if hasattr(mod, "build"):
                    try:
                        built = mod.build(self.load_cfg, self.save_cfg)
                    except Exception as e:
                        log(f"Provider build failed for {m.name}: {e}", level="ERROR", module="META")

                if built is not None:
                    inst = built
                elif isinstance(inst, type):
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

    # ------------------------------ Resolve ------------------------------

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
            "movie" | "tv" (aliases "show"/"series" map to "tv")
        ids : dict
            Identifier map, e.g. {"tmdb": "123", "imdb": "tt..."}; providers choose what they can use.
        locale : Optional[str]
            Optional locale override (e.g., "nl-NL"). Falls back to config if None.
        need : Optional[dict]
            Field request flags. Keep defaults lean; ask for rich fields explicitly.
            Example: {"poster": True, "backdrop": True, "title": True, "year": True, "overview": True}
        strategy : str
            "first_success" (default) or "merge".
            - first_success: return the first non-empty provider result.
            - merge: field-wise merge; images are concatenated/deduped by URL,
                     scalars use first non-empty value wins.

        Returns
        -------
        dict
            A normalized metadata dictionary. Empty dict on total failure (fail-soft).
        """
        cfg = self.load_cfg() or {}
        md_cfg = cfg.get("metadata") or {}

        # Normalize entity and prioritize providers
        entity = {"show": "tv", "series": "tv", "movies": "movie"}.get(str(entity).lower(), str(entity).lower())

        # Priority: config override or discovery order
        default_order = list(self.providers.keys())
        configured_order = md_cfg.get("priority") or default_order
        order = [str(x).upper() for x in configured_order if str(x).upper() in self.providers]

        # Default "lean" request; rich fields must be requested explicitly
        req_need = need or {"poster": True, "backdrop": True, "title": True, "year": True}
        eff_locale = locale or md_cfg.get("locale") or (cfg.get("ui") or {}).get("locale")

        results: list[dict] = []
        for name in order:
            prov = self.providers.get(name)
            if not prov:
                continue

            try:
                # Prefer provider.fetch; fall back to provider.resolve for compatibility.
                if hasattr(prov, "fetch"):
                    r = prov.fetch(entity=entity, ids=ids, locale=eff_locale, need=req_need) or {}
                else:
                    r = getattr(prov, "resolve")(entity=entity, ids=ids, locale=eff_locale, need=req_need) or {}

                if not r:
                    continue

                if strategy == "first_success":
                    log(f"Provider {name} hit", module="META")
                    return r

                results.append(r)

            except Exception as e:
                log(f"Provider {name} error: {e}", level="WARNING", module="META")
                continue

        if not results:
            # Fail-soft: callers handle empty result.
            return {}

        return self._merge(results) if strategy == "merge" else (results[0] or {})

    # ------------------------------ Merge policy ------------------------------

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
                    out.setdefault("images", {})
                    for kind, arr in v.items():
                        out["images"].setdefault(kind, [])
                        # Deduplicate by URL while preserving order
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
