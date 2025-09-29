# _watchlistAPI.py
from fastapi import APIRouter, Query, Body, Path as FPath
from fastapi.responses import JSONResponse
from typing import Optional, Literal, Dict, Any, List
from _watchlist import delete_watchlist_batch
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
def api_watchlist_delete(
    key: str = FPath(...),
    provider: Optional[str] = Query("ALL", description="PLEX|TRAKT|SIMKL|JELLYFIN|ALL"),
) -> JSONResponse:
    from cw_platform.config_base import load_config
    from crosswatch import STATE_PATH, _load_state, STATS, _append_log
    from _watchlist import delete_watchlist_item

    if "%" in (key or ""):
        key = urllib.parse.unquote(key)
    prov = (provider or "ALL").upper().strip()

    res = delete_watchlist_item(key=key, state_path=STATE_PATH, cfg=load_config(), provider=prov, log=_append_log)
    if not isinstance(res, dict):
        res = {"ok": bool(res)}
    if res.get("ok"):
        try:
            state = _load_state()
            if state: STATS.refresh_from_state(state)
        except Exception:
            pass
    res.setdefault("provider", prov)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.get("/providers")
def api_watchlist_providers():
    from cw_platform.config_base import load_config
    from _watchlist import detect_available_watchlist_providers
    return {"providers": detect_available_watchlist_providers(load_config())}

@router.post("/delete")
def api_watchlist_delete_multi(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    # single/bulk delete via batch helper; simple + consistent
    from cw_platform.config_base import load_config
    from crosswatch import _load_state, STATS, _append_log
    from _watchlist import delete_watchlist_batch

    try:
        keys_raw = payload.get("keys") or []
        provider = (payload.get("provider") or "ALL").upper().strip()
        if not isinstance(keys_raw, list) or not keys_raw: return {"ok": False, "error": "keys must be a non-empty array"}
        if provider not in {"ALL", "PLEX", "SIMKL", "TRAKT", "JELLYFIN"}: return {"ok": False, "error": f"unknown provider '{provider}'"}

        # normalize keys
        keys = [urllib.parse.unquote(str(k).strip()) if "%" in str(k) else str(k).strip() for k in keys_raw]
        keys = list(dict.fromkeys([k for k in keys if k]))

        cfg = load_config()
        state = _load_state()

        targets = ["PLEX", "SIMKL", "TRAKT", "JELLYFIN"] if provider == "ALL" else [provider]
        results: List[Dict[str, Any]] = []
        deleted_sum = 0

        for prov in targets:
            try:
                res = delete_watchlist_batch(keys, prov, state, cfg) or {}
                deleted = int(res.get("deleted", 0)) if isinstance(res, dict) else 0
                results.append({"provider": prov, "ok": deleted > 0, "deleted": deleted, **(res if isinstance(res, dict) else {})})
                deleted_sum += deleted
                _append_log("SYNC", f"[WL] delete {len(keys)} on {prov}: {'OK' if deleted else 'NOOP'}")
            except Exception as e:
                results.append({"provider": prov, "ok": False, "error": str(e)})
                _append_log("SYNC", f"[WL] delete on {prov} failed: {e}")

        try:
            if state: STATS.refresh_from_state(state)
        except Exception:
            pass

        any_ok = any(r.get("ok") for r in results)
        all_ok = all(r.get("ok") for r in results)
        return {
            "ok": any_ok,
            "partial": any_ok and not all_ok,
            "provider": provider,
            "deleted_ok": deleted_sum,          # UI reads this
            "deleted_total": len(keys),
            "results": results,
        }
    except Exception as e:
        _append_log("SYNC", f"[WL] delete fatal: {e}")
        return {"ok": False, "error": f"fatal: {e}"}


@router.post("/delete_batch")
def api_watchlist_delete_batch(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    # same semantics; kept for compatibility
    from cw_platform.config_base import load_config
    from crosswatch import _load_state, STATS, _append_log
    from _watchlist import delete_watchlist_batch

    try:
        keys_raw = payload.get("keys") or []
        provider = (payload.get("provider") or "ALL").upper().strip()
        if not isinstance(keys_raw, list) or not keys_raw: return {"ok": False, "error": "keys must be a non-empty array"}
        if provider not in {"ALL", "PLEX", "SIMKL", "TRAKT", "JELLYFIN"}: return {"ok": False, "error": f"unknown provider '{provider}'"}

        keys = [urllib.parse.unquote(str(k).strip()) if "%" in str(k) else str(k).strip() for k in keys_raw]
        keys = list(dict.fromkeys([k for k in keys if k]))

        cfg = load_config()
        state = _load_state()

        targets = ["PLEX", "SIMKL", "TRAKT", "JELLYFIN"] if provider == "ALL" else [provider]
        results: List[Dict[str, Any]] = []
        deleted_sum = 0

        for prov in targets:
            try:
                res = delete_watchlist_batch(keys, prov, state, cfg) or {}
                deleted = int(res.get("deleted", 0)) if isinstance(res, dict) else 0
                results.append({"provider": prov, "ok": deleted > 0, "deleted": deleted, **(res if isinstance(res, dict) else {})})
                deleted_sum += deleted
                _append_log("SYNC", f"[WL] batch-delete {len(keys)} on {prov}: {'OK' if deleted else 'NOOP'}")
            except Exception as e:
                results.append({"provider": prov, "ok": False, "error": str(e)})
                _append_log("SYNC", f"[WL] batch-delete on {prov} failed: {e}")

        try:
            if state: STATS.refresh_from_state(state)
        except Exception:
            pass

        any_ok = any(r.get("ok") for r in results)
        all_ok = all(r.get("ok") for r in results)
        return {
            "ok": any_ok,
            "partial": any_ok and not all_ok,
            "provider": provider,
            "targets": targets,
            "deleted_ok": deleted_sum,
            "deleted_total": len(keys),
            "results": results,
        }
    except Exception as e:
        _append_log("SYNC", f"[WL] batch-delete fatal: {e}")
        return {"ok": False, "error": f"fatal: {e}"}

