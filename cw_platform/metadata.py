# cw_platform/metadata.py
# CrossWatch - Metadata Manager
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import importlib
import json
import pkgutil
from typing import Any, Callable, Mapping, Optional, cast

try:
    from _logging import log  # type: ignore
except Exception:  # pragma: no cover
    def log(msg: str, *, level: str = "INFO", module: str = "META") -> None:
        # noop fallback
        return


try:
    from id_map import ids_from_guid
except Exception:  # pragma: no cover
    def ids_from_guid(guid: Optional[str]) -> dict[str, str]:
        return {}


try:
    from id_map import merge_ids as _merge_ids, KEY_PRIORITY as _KEY_PRIORITY  # type: ignore
except Exception:  # pragma: no cover
    _KEY_PRIORITY: tuple[str, ...] = ("imdb", "tmdb", "tvdb", "trakt", "plex", "guid", "slug", "simkl")

    def _merge_ids(old: dict[str, Any] | None, new: dict[str, Any] | None) -> dict[str, Any]:
        old = old or {}
        new = new or {}
        out: dict[str, Any] = {}
        for k in _KEY_PRIORITY:
            out[k] = old.get(k) or new.get(k) or out.get(k)
        for k, v in new.items():
            if k not in out or out[k] is None:
                out[k] = v
        for k, v in old.items():
            if k not in out or out[k] is None:
                out[k] = v
        return {k: v for k, v in out.items() if v}


# helpers

def _norm_ids(ids: Mapping[str, Any] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
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
    mapping = {
        "series": "show",
        "tv": "show",
        "shows": "show",
        "movies": "movie",
    }
    if e in ("movie", "show"):
        return e
    return mapping.get(e, "movie")


def _norm_need(need: Mapping[str, Any] | None) -> dict[str, Any]:
    n: dict[str, Any] = dict(need or {})
    if n.get("images") and not any(n.get(k) for k in ("poster", "backdrop", "logo")):
        n["poster"] = True
    return n or {"poster": True, "backdrop": True, "title": True, "year": True}


def _first_non_empty(*vals: Any) -> Any:
    for v in vals:
        if v not in (None, "", [], {}):
            return v
    return None


# Meta Manager

class MetadataManager:
    def __init__(
        self,
        load_cfg: Callable[[], dict[str, Any]],
        save_cfg: Callable[[dict[str, Any]], None],
    ) -> None:
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self.providers: dict[str, Any] = self._discover()

    # Discovery
    def _discover(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        try:
            import providers.metadata as md  # noqa: F401
        except Exception as e:  # pragma: no cover
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
                if hasattr(mod, "build"):
                    try:
                        built = mod.build(self.load_cfg, self.save_cfg)
                    except Exception as e:
                        log(f"Provider build failed for {name}: {e}", level="ERROR", module="META")

                if built is not None:
                    inst = built
                elif isinstance(inst, type):
                    try:
                        inst = inst(self.load_cfg, self.save_cfg)
                    except Exception as e:
                        log(f"Provider init failed for {name}: {e}", level="ERROR", module="META")
                        inst = None

                if inst is None:
                    continue

                label = getattr(inst, "name", name.replace("_meta_", "")) or name.replace("_meta_", "")
                out[str(label).upper()] = inst

        return out

    # Resolve it through providers
    def resolve(
        self,
        *,
        entity: str,
        ids: Mapping[str, Any],
        locale: Optional[str] = None,
        need: Mapping[str, Any] | None = None,
        strategy: str = "first_success",
    ) -> dict[str, Any]:
        cfg = self.load_cfg() or {}
        md_cfg = cfg.get("metadata") or {}
        debug = bool((cfg.get("runtime") or {}).get("debug"))
        entity_norm = _norm_entity(entity)
        req_need = _norm_need(need)
        eff_locale = locale or md_cfg.get("locale") or (cfg.get("ui") or {}).get("locale")

        ids_norm = _norm_ids(ids)

        default_order = list(self.providers.keys())
        order: list[str] = [
            str(x).upper()
            for x in (md_cfg.get("priority") or default_order)
            if str(x).upper() in self.providers
        ]

        results: list[dict[str, Any]] = []

        for name in order:
            prov = self.providers.get(name)
            if not prov:
                continue

            try:
                raw: Any
                if hasattr(prov, "fetch"):
                    raw = prov.fetch(
                        entity=entity_norm,
                        ids=ids_norm,
                        locale=eff_locale,
                        need=req_need,
                    )
                else:
                    resolver = getattr(prov, "resolve", None)
                    raw = (
                        resolver(
                            entity=entity_norm,
                            ids=ids_norm,
                            locale=eff_locale,
                            need=req_need,
                        )
                        if callable(resolver)
                        else None
                    )

                if not raw:
                    continue
                if not isinstance(raw, dict):
                    continue

                r: dict[str, Any] = cast(dict[str, Any], raw)

                if "type" not in r:
                    r["type"] = entity_norm

                if strategy == "first_success":
                    #if debug:
                    #   log(f"Provider {name} hit", level="DEBUG", module="META")
                    return r

                results.append(r)
            except Exception as e:
                log(f"Provider {name} error: {e}", level="WARNING", module="META")
                continue

        if not results:
            return {}
        return self._merge(results) if strategy == "merge" else (results[0] or {})

    # Resolve in batch
    def resolve_many(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for it in items or []:
            ids_raw = dict(it.get("ids") or {})
            g = ids_raw.get("guid")
            if g:
                try:
                    ids_raw.update(ids_from_guid(g))
                except Exception:
                    pass

            ids_norm = _norm_ids(ids_raw)
            ent = _norm_entity((it.get("type") or it.get("entity") or "movie").rstrip("s"))
            title = it.get("title")
            year = it.get("year")

            try:
                if ids_norm:
                    r = self.resolve(entity=ent, ids=ids_norm, need={"ids": True})
                else:
                    r = self.resolve(entity=ent, ids={}, need={"title": True, "year": True, "ids": True})
            except Exception:
                r = None

            if r:
                r_ids = dict(r.get("ids") or {})
                out.append(
                    {
                        "type": r.get("type") or ent,
                        "title": _first_non_empty(r.get("title"), title),
                        "year": _first_non_empty(r.get("year"), year),
                        "ids": _merge_ids(ids_norm, r_ids),
                    }
                )
            else:
                it2 = dict(it)
                it2["ids"] = ids_norm
                out.append(it2)
        return out

    # Reconcile
    def reconcile_ids(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        healed: list[dict[str, Any]] = []
        for it in items or []:
            ent = _norm_entity((it.get("type") or it.get("entity") or "movie").rstrip("s"))
            ids: dict[str, Any] = _norm_ids(dict(it.get("ids") or {}))
            title = it.get("title")
            year = it.get("year")

            try:
                r: dict[str, Any] = {}
                if ent == "movie":
                    if ids.get("imdb"):
                        r = self.resolve(entity="movie", ids={"imdb": ids["imdb"]}, need={"ids": True})
                    elif ids.get("tmdb"):
                        r = self.resolve(entity="movie", ids={"tmdb": ids["tmdb"]}, need={"ids": True})
                    elif title:
                        payload: dict[str, Any] = {"title": title}
                        if year:
                            payload["year"] = year
                        r = self.resolve(entity="movie", ids=payload, need={"ids": True})
                else:
                    if ids.get("tmdb"):
                        r = self.resolve(entity="show", ids={"tmdb": ids["tmdb"]}, need={"ids": True})
                    elif ids.get("imdb"):
                        r = self.resolve(entity="show", ids={"imdb": ids["imdb"]}, need={"ids": True})
                    elif title:
                        payload2: dict[str, Any] = {"title": title}
                        if year:
                            payload2["year"] = year
                        r = self.resolve(entity="show", ids=payload2, need={"ids": True})
            except Exception:
                r = {}

            rid = _norm_ids(dict((r or {}).get("ids") or {}))
            ids = _merge_ids(ids, rid)

            healed.append({"type": ent, "title": title, "year": year, "ids": ids})
        return healed

    # Merge policy
    def _merge(self, results: list[dict[str, Any]]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        images: dict[str, list[dict[str, Any]]] = {}

        for r in results:
            if not isinstance(r, dict):
                continue

            r_images = r.get("images")
            if isinstance(r_images, dict):
                for kind, arr in r_images.items():
                    if not isinstance(arr, list):
                        continue

                    bucket = images.setdefault(kind, [])
                    seen = {
                        (x.get("url") or x.get("file_path") or x.get("path"))
                        for x in bucket
                        if isinstance(x, dict)
                    }

                    for x in arr:
                        if not isinstance(x, dict):
                            continue
                        x_dict: dict[str, Any] = x
                        key = (
                            x_dict.get("url")
                            or x_dict.get("file_path")
                            or x_dict.get("path")
                        )
                        if not key or key in seen:
                            continue
                        bucket.append(x_dict)
                        seen.add(key)

            for k, v in r.items():
                if k == "images":
                    continue
                if k not in out and v not in (None, "", [], {}):
                    out[k] = v

        if images:
            out["images"] = images

        if not isinstance(out.get("type"), str):
            for r in results:
                if not isinstance(r, dict):
                    continue
                t = _norm_entity(r.get("type"))
                if t in ("movie", "show"):
                    out["type"] = t
                    break

        return out
