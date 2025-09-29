# _syncAPI.py
from __future__ import annotations
from typing import Any, Dict, List
from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api", tags=["Synchronization"])

# lazy env (avoid cycles)
def _env():
    from cw_platform.config_base import load_config, save_config
    from crosswatch import _cfg_pairs, _normalize_features, _gen_id, _append_log
    return load_config, save_config, _cfg_pairs, _normalize_features, _gen_id, _append_log


# -------- Sync providers discovery --------
@router.get("/sync/providers")
def api_sync_providers() -> JSONResponse:
    """Discover sync providers & capabilities."""
    import importlib, pkgutil, dataclasses as _dc, inspect

    HIDDEN = {"BASE"}
    PKG_CANDIDATES = ("providers.sync",)
    FEATURE_KEYS = ("watchlist", "ratings", "history", "playlists")

    def _asdict_dc(obj):
        try:
            if _dc.is_dataclass(obj):
                return _dc.asdict(obj if not isinstance(obj, type) else obj())
        except Exception:
            pass
        return None

    def _norm_features(f: dict | None) -> dict:
        f = dict(f or {})
        return {k: bool((f.get(k, False) or {}).get("enable", False) if isinstance(f.get(k), dict) else f.get(k, False))
                for k in FEATURE_KEYS}

    def _norm_caps(caps: dict | None) -> dict:
        caps = dict(caps or {})
        return {"bidirectional": bool(caps.get("bidirectional", False))}

    def _manifest_from_module(mod) -> dict | None:
        # 1) explicit get_manifest()
        if hasattr(mod, "get_manifest") and callable(mod.get_manifest):
            try:
                mf = dict(mod.get_manifest())
            except Exception:
                mf = None
            if mf and not (mf.get("hidden") or mf.get("is_template")):
                return {
                    "name": (mf.get("name") or "").upper(),
                    "label": mf.get("label") or (mf.get("name") or "").title(),
                    "features": _norm_features(mf.get("features")),
                    "capabilities": _norm_caps(mf.get("capabilities")),
                    "version": mf.get("version"),
                    "vendor": mf.get("vendor"),
                    "description": mf.get("description"),
                }

        # 2) *Module.info + supported_features()
        cand = [cls for _, cls in inspect.getmembers(mod, inspect.isclass)
                if cls.__module__ == mod.__name__ and cls.__name__.endswith("Module")]
        if cand:
            cls = cand[0]
            info = getattr(cls, "info", None)
            if info is not None:
                caps = _asdict_dc(getattr(info, "capabilities", None)) or {}
                name = (getattr(info, "name", None) or getattr(cls, "__name__", "").replace("Module", "")).upper()
                label = (getattr(info, "name", None) or name).title()
                if bool(getattr(info, "hidden", False) or getattr(info, "is_template", False)):
                    return None
                try:
                    feats = dict(cls.supported_features()) if hasattr(cls, "supported_features") else {}
                except Exception:
                    feats = {}
                return {
                    "name": name,
                    "label": label,
                    "features": _norm_features(feats),
                    "capabilities": _norm_caps(caps),
                    "version": getattr(info, "version", None),
                    "vendor": getattr(info, "vendor", None),
                    "description": getattr(info, "description", None),
                }

        # 3) OPS fallback
        ops = getattr(mod, "OPS", None)
        if ops is not None:
            try:
                name = str(ops.name()).upper()
                label = str(getattr(ops, "label")() if hasattr(ops, "label") else name.title())
                feats = dict(ops.features()) if hasattr(ops, "features") else {}
                caps = dict(ops.capabilities()) if hasattr(ops, "capabilities") else {}
                return {
                    "name": name,
                    "label": label,
                    "features": _norm_features(feats),
                    "capabilities": _norm_caps(caps),
                    "version": None,
                    "vendor": None,
                    "description": None,
                }
            except Exception:
                return None
        return None

    items, seen = [], set()
    for pkg_name in PKG_CANDIDATES:
        try:
            pkg = importlib.import_module(pkg_name)
        except Exception:
            continue
        for pkg_path in getattr(pkg, "__path__", []):
            for m in pkgutil.iter_modules([str(pkg_path)]):
                if not m.name.startswith("_mod_"):
                    continue
                prov_key = m.name.replace("_mod_", "").upper()
                if prov_key in HIDDEN:
                    continue
                try:
                    mod = importlib.import_module(f"{pkg_name}.{m.name}")
                except Exception:
                    continue
                mf = _manifest_from_module(mod)
                if not mf:
                    continue
                mf["name"] = (mf["name"] or prov_key).upper()
                mf["label"] = mf.get("label") or mf["name"].title()
                mf["features"] = _norm_features(mf.get("features"))
                mf["capabilities"] = _norm_caps(mf.get("capabilities"))
                if mf["name"] in seen:
                    continue
                seen.add(mf["name"])
                items.append(mf)

    items.sort(key=lambda x: (x.get("label") or x.get("name") or "").lower())
    return JSONResponse(items)


# -------- Sync pairs --------
@router.get("/pairs")
def api_pairs_list() -> JSONResponse:
    load_config, save_config, _cfg_pairs, _normalize_features, *_ = _env()
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)
        dirty = False
        for it in arr:
            newf = _normalize_features(it.get("features"))
            if newf != (it.get("features") or {}):
                it["features"] = newf
                dirty = True
        if dirty:
            save_config(cfg)
        return JSONResponse(arr)
    except Exception as e:
        try:
            _env()[-1]("TRBL", f"/api/pairs GET failed: {e}")  # _append_log
        except Exception:
            pass
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@router.post("/pairs")
def api_pairs_add(payload: Any = Body(...)) -> Dict[str, Any]:
    load_config, save_config, _cfg_pairs, _normalize_features, _gen_id, _append_log = _env()
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)
        item = payload.model_dump() if hasattr(payload, "model_dump") else (payload.dict() if hasattr(payload, "dict") else dict(payload))
        item.setdefault("mode", "one-way")
        item["enabled"] = bool(item.get("enabled", False))  # default OFF
        item["features"] = _normalize_features(item.get("features") or {"watchlist": True})
        item["id"] = _gen_id("pair")
        arr.append(item)
        save_config(cfg)
        return {"ok": True, "id": item["id"]}
    except Exception as e:
        try: _append_log("TRBL", f"/api/pairs POST failed: {e}")
        except Exception: pass
        return {"ok": False, "error": str(e)}


@router.post("/pairs/reorder")
def api_pairs_reorder(order: List[str] = Body(...)) -> dict:
    load_config, save_config, _cfg_pairs, *_ = _env()
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)

        index_map = {str(p.get("id")): i for i, p in enumerate(arr)}
        wanted_ids = [pid for pid in (order or []) if pid in index_map]
        id_set = set(wanted_ids)

        head = [next(p for p in arr if str(p.get("id")) == pid) for pid in wanted_ids]
        tail = [p for p in arr if str(p.get("id")) not in id_set]
        new_arr = head + tail

        changed = any(a is not b for a, b in zip(arr, new_arr)) or (len(arr) != len(new_arr))
        if changed:
            cfg["pairs"] = new_arr
            save_config(cfg)

        unknown_ids = [pid for pid in (order or []) if pid not in index_map]
        return {"ok": True, "reordered": changed, "count": len(new_arr),
                "unknown_ids": unknown_ids, "final_order": [str(p.get("id")) for p in new_arr]}
    except Exception as e:
        try: _env()[-1]("TRBL", f"/api/pairs/reorder failed: {e}")  # _append_log
        except Exception: pass
        return {"ok": False, "error": str(e)}


@router.put("/pairs/{pair_id}")
def api_pairs_update(pair_id: str, payload: Any = Body(...)) -> Dict[str, Any]:
    load_config, save_config, _cfg_pairs, _normalize_features, *_ = _env()
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)
        upd = (payload.model_dump(exclude_unset=True, exclude_none=True)
               if hasattr(payload, "model_dump")
               else (payload.dict(exclude_unset=True, exclude_none=True) if hasattr(payload, "dict")
                     else {k: v for k, v in dict(payload).items() if v is not None}))
        for it in arr:
            if str(it.get("id")) == pair_id:
                if "features" in upd:
                    it["features"] = _normalize_features(upd.pop("features"))
                for k, v in upd.items():
                    it[k] = v
                save_config(cfg)
                return {"ok": True}
        return {"ok": False, "error": "not_found"}
    except Exception as e:
        try: _env()[-1]("TRBL", f"/api/pairs PUT failed: {e}")  # _append_log
        except Exception: pass
        return {"ok": False, "error": str(e)}


@router.delete("/pairs/{pair_id}")
def api_pairs_delete(pair_id: str) -> Dict[str, Any]:
    load_config, save_config, _cfg_pairs, *_ = _env()
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)
        before = len(arr)
        arr[:] = [it for it in arr if str(it.get("id")) != pair_id]
        save_config(cfg)
        return {"ok": True, "deleted": before - len(arr)}
    except Exception as e:
        try: _env()[-1]("TRBL", f"/api/pairs DELETE failed: {e}")  # _append_log
        except Exception: pass
        return {"ok": False, "error": str(e)}
