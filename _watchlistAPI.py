# _watchlistAPI.py
from fastapi import APIRouter, Query, Body, Path as FPath
from fastapi.responses import JSONResponse
from typing import Optional, Literal, Dict, Any, List
import urllib.parse

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])

@router.get("/")
def api_watchlist(
    overview: Literal["none", "short", "full"] = Query("none", description="Attach overview from TMDb"),
    locale: Optional[str] = Query(None, description="Override metadata locale (e.g., 'nl-NL')"),
    limit: int = Query(0, ge=0, le=5000, description="Slice the list"),
    max_meta: int = Query(250, ge=0, le=2000, description="Cap enriched items"),
) -> JSONResponse:
    # Late imports to dodge circulars
    try:
        from cw_platform.config_base import load_config
        from crosswatch import _load_state, CACHE_DIR
        from _watchlist import build_watchlist
        from _metaAPI import get_meta, _shorten
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"server import failed: {e}"}, status_code=200)

    cfg = load_config()
    st = _load_state()
    api_key = ((cfg.get("tmdb") or {}).get("api_key") or "").strip()

    if not st:
        return JSONResponse({"ok": False, "error": "No snapshot found or empty.", "missing_tmdb_key": not bool(api_key)}, status_code=200)

    # Backward-compat builder (yes, it’s ugly; yes, it’s safe)
    def _build_watchlist_compat(state, key: str):
        import inspect
        ok = bool(key)
        try:
            sig = inspect.signature(build_watchlist)
            params = sig.parameters
            if "tmdb_api_key_present" in params: return build_watchlist(state, tmdb_api_key_present=ok)
            if "tmdb_ok" in params:               return build_watchlist(state, tmdb_ok=ok)
            try: return build_watchlist(state, ok)
            except TypeError: return build_watchlist(state)
        except TypeError:
            try: return build_watchlist(state, ok)
            except Exception: return build_watchlist(state)

    try:
        items = _build_watchlist_compat(st, api_key) or []
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"{e.__class__.__name__}: {e}", "missing_tmdb_key": not bool(api_key)}, status_code=200)

    if not items:
        return JSONResponse({"ok": False, "error": "No snapshot data found.", "missing_tmdb_key": not bool(api_key)}, status_code=200)

    if limit and isinstance(limit, int) and limit > 0:
        items = items[:limit]

    # Optional TMDb enrichment; best-effort, never hard-fails
    enriched = 0
    if overview != "none" and api_key:
        eff_locale = locale or (cfg.get("metadata") or {}).get("locale") or (cfg.get("ui") or {}).get("locale") or None

        def _norm_type(x: Optional[str]) -> str:
            t = (x or "").strip().lower()
            if t in {"tv", "show", "shows", "series", "season", "episode"}: return "tv"
            if t in {"movie", "movies", "film", "films"}:                   return "movie"
            return "movie"

        for it in items:
            if enriched >= int(max_meta):
                break
            tmdb_id = it.get("tmdb")
            if not tmdb_id:
                continue
            it["type"] = _norm_type(it.get("type") or it.get("entity") or it.get("media_type"))
            try:
                meta = get_meta(
                    api_key, it["type"], tmdb_id, CACHE_DIR,
                    need={"overview": True, "tagline": True, "title": True, "year": True},
                    locale=eff_locale,
                ) or {}
                desc = meta.get("overview") or ""
                if not desc:
                    continue
                if overview == "short":
                    desc = _shorten(desc, 280)
                it["overview"] = desc
                if overview == "short" and meta.get("tagline"):
                    it["tagline"] = meta["tagline"]
                enriched += 1
            except Exception:
                # Soft-fail; keep going
                continue

    return JSONResponse(
        {
            "ok": True,
            "items": items,
            "missing_tmdb_key": not bool(api_key),
            "last_sync_epoch": st.get("last_sync_epoch"),
            "meta_enriched": enriched,
        },
        status_code=200,
    )

@router.delete("/{key}")
def api_watchlist_delete(key: str = FPath(...)) -> JSONResponse:
    from cw_platform.config_base import load_config
    from crosswatch import STATE_PATH, _load_state, STATS, _append_log
    from _watchlist import delete_watchlist_item

    try:
        if "%" in (key or ""):
            key = urllib.parse.unquote(key)

        res = delete_watchlist_item(key=key, state_path=STATE_PATH, cfg=load_config(), log=_append_log)
        if not isinstance(res, dict) or "ok" not in res:
            res = {"ok": False, "error": "unexpected server response"}

        if res.get("ok"):
            try:
                state = _load_state()
                P = (state.get("providers") or {})
                for prov in ("PLEX", "SIMKL", "TRAKT", "JELLYFIN"):
                    items = (((P.get(prov) or {}).get("watchlist") or {}).get("baseline") or {}).get("items") or {}
                    items.pop(key, None)
                STATS.refresh_from_state(state)
            except Exception:
                pass

        return JSONResponse(res, status_code=(200 if res.get("ok") else 400))
    except Exception as e:
        _append_log("TRBL", f"[WATCHLIST] ERROR: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@router.get("/providers")
def api_watchlist_providers():
    from cw_platform.config_base import load_config
    from _watchlist import detect_available_watchlist_providers
    return {"providers": detect_available_watchlist_providers(load_config())}

@router.post("/delete")
def api_watchlist_delete_multi(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    from cw_platform.config_base import load_config
    from crosswatch import STATE_PATH, _load_state, STATS, _append_log
    from _watchlist import delete_watchlist_item

    try:
        keys_raw = payload.get("keys") or []
        provider = (payload.get("provider") or "ALL").upper().strip()
        if not isinstance(keys_raw, list) or not keys_raw:
            return {"ok": False, "error": "keys must be a non-empty array"}
        if provider not in {"ALL", "PLEX", "SIMKL", "TRAKT", "JELLYFIN"}:
            return {"ok": False, "error": f"unknown provider '{provider}'"}

        try:
            cfg = load_config()
        except Exception as e:
            return {"ok": False, "error": f"failed to load config: {e}"}

        keys = list(dict.fromkeys([str(k).strip() for k in keys_raw if str(k).strip()]))
        results: List[Dict[str, Any]] = []
        ok_count = 0
        for k in keys:
            try:
                r = delete_watchlist_item(key=k, state_path=STATE_PATH, cfg=cfg, provider=provider, log=_append_log)
                r = r if isinstance(r, dict) else {"ok": bool(r)}
                results.append({"key": k, **r})
                if r.get("ok"):
                    ok_count += 1
            except Exception as e:
                _append_log("SYNC", f"[WL] delete {k} on {provider} failed: {e}")
                results.append({"key": k, "ok": False, "error": str(e)})

        try:
            state = _load_state()
            if state:
                STATS.refresh_from_state(state)
        except Exception:
            pass

        return {
            "ok": ok_count > 0,
            "partial": ok_count != len(keys),
            "provider": provider,
            "deleted_ok": ok_count,
            "deleted_total": len(keys),
            "results": results,
        }
    except Exception as e:
        _append_log("SYNC", f"[WL] delete fatal: {e}")
        return {"ok": False, "error": f"fatal: {e}"}

@router.post("/delete_batch")
def api_watchlist_delete_batch(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    from cw_platform.config_base import load_config
    from crosswatch import _load_state, STATS, _append_log
    from _watchlist import delete_watchlist_batch as _wl_delete_batch

    try:
        keys_raw = payload.get("keys") or []
        provider = (payload.get("provider") or "ALL").upper().strip()
        if not isinstance(keys_raw, list) or not keys_raw:
            return {"ok": False, "error": "keys must be a non-empty array"}
        if provider not in {"ALL", "PLEX", "SIMKL", "TRAKT", "JELLYFIN"}:
            return {"ok": False, "error": f"unknown provider '{provider}'"}

        keys = list(dict.fromkeys([str(k).strip() for k in keys_raw if str(k).strip()]))
        try:
            cfg = load_config()
            state = _load_state()
        except Exception as e:
            return {"ok": False, "error": f"load failed: {e}"}

        targets = ["PLEX", "SIMKL", "TRAKT", "JELLYFIN"] if provider == "ALL" else [provider]
        results: List[Dict[str, Any]] = []

        for prov in targets:
            try:
                res = _wl_delete_batch(keys, prov, state, cfg)
                ok = bool((res or {}).get("ok")) if isinstance(res, dict) else bool(res)
                results.append((res if isinstance(res, dict) else {"ok": ok}) | {"provider": prov})
                _append_log("SYNC", f"[WL] batch-delete {len(keys)} on {prov}: {'OK' if ok else 'NOOP'}")
            except Exception as e:
                _append_log("SYNC", f"[WL] batch-delete on {prov} failed: {e}")
                results.append({"provider": prov, "ok": False, "error": str(e)})

        try:
            if state:
                STATS.refresh_from_state(state)
        except Exception:
            pass

        any_ok = any((isinstance(r, dict) and r.get("ok")) for r in results)
        all_ok = all((isinstance(r, dict) and r.get("ok")) for r in results)
        return {"ok": any_ok, "partial": any_ok and not all_ok, "provider": provider, "targets": targets, "results": results}
    except Exception as e:
        _append_log("SYNC", f"[WL] batch-delete fatal: {e}")
        return {"ok": False, "error": f"fatal: {e}"}
