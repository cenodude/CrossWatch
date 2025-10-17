# _watchlistAPI.py
from typing import Optional, Literal, Dict, Any, List, Tuple
import urllib.parse

from fastapi import APIRouter, Query, Body, Path as FPath
from fastapi.responses import JSONResponse

# Use registry-driven watchlist ops; no fallbacks
from _watchlist import (
    build_watchlist,
    delete_watchlist_batch,
    delete_watchlist_item,
    detect_available_watchlist_providers,
    _find_item_in_state_for_provider,
    _find_item_in_state,
    _type_from_item_or_guess,
)

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])


# ---------- helpers ----------
def _norm_key(x: Any) -> str:
    s = str((x.get("key") if isinstance(x, dict) else x) or "").strip()
    return urllib.parse.unquote(s) if "%" in s else s

def _active_providers(cfg: Dict[str, Any]) -> List[str]:
    # Only configured providers; dynamic (no hardcoded whitelist)
    try:
        manifest = detect_available_watchlist_providers(cfg) or []
    except Exception:
        manifest = []
    out: List[str] = []
    for it in manifest:
        if not isinstance(it, dict):
            continue
        pid = str(it.get("id") or "").strip().upper()
        if pid and pid != "ALL" and bool(it.get("configured")) and pid not in out:
            out.append(pid)
    return out

def _item_label(state: Dict[str, Any], key: str, prov: str) -> Tuple[str, str]:
    # Returns ("movie"|"show", "Title (Year)")
    it = (_find_item_in_state_for_provider(state, key, prov) or _find_item_in_state(state, key) or {})
    kind = _type_from_item_or_guess(it, key)
    title = it.get("title") or it.get("name") or key
    y = it.get("year") or it.get("release_year")
    return ("show" if kind == "tv" else "movie", f"{title} ({y})" if y else str(title))

def _bulk_delete(provider: str, keys_raw: List[Any]) -> Dict[str, Any]:
    from cw_platform.config_base import load_config
    from _syncAPI import _load_state
    from crosswatch import STATS, _append_log

    if not isinstance(keys_raw, list) or not keys_raw:
        return {"ok": False, "error": "keys must be a non-empty array"}

    keys = [k for k in (_norm_key(k) for k in keys_raw) if k]
    keys = list(dict.fromkeys(keys))

    cfg = load_config()
    state = _load_state()
    active = _active_providers(cfg)
    prov = (provider or "ALL").upper().strip()

    if prov == "ALL":
        targets = active[:]  # all connected
        if not targets:
            return {"ok": False, "error": "no connected providers"}
    else:
        if prov not in active:
            return {"ok": False, "error": f"provider '{prov}' not connected"}
        targets = [prov]

    results: List[Dict[str, Any]] = []
    deleted_sum = 0

    for p in targets:
        try:
            per_key: List[Dict[str, Any]] = []
            deleted = 0
            for k in keys:
                kind, label = _item_label(state, k, p)
                safe_label = (label or "").replace("'", "’")
                r = delete_watchlist_batch([k], p, state, cfg) or {}
                d = int(r.get("deleted", 0)) if isinstance(r, dict) else 0
                per_key.append({"key": k, "deleted": d})
                deleted += d
                _append_log("SYNC", f"[WL] delete 1 {kind} '{safe_label}' on {p}: {'OK' if d else 'NOOP'}")
            results.append({"provider": p, "ok": deleted > 0, "deleted": deleted, "per_key": per_key})
            deleted_sum += deleted
        except Exception as e:
            results.append({"provider": p, "ok": False, "error": str(e)})
            _append_log("SYNC", f"[WL] delete on {p} failed: {e}")

    try:
        fresh = _load_state()
        if fresh:
            STATS.refresh_from_state(fresh)
    except Exception:
        pass

    any_ok = any(r.get("ok") for r in results)
    all_ok = all(r.get("ok") for r in results)
    return {
        "ok": any_ok,
        "partial": any_ok and not all_ok,
        "provider": prov,
        "targets": targets,
        "deleted_ok": deleted_sum,
        "deleted_total": len(keys),
        "results": results,
    }


# ---------- routes ----------
@router.get("/")
def api_watchlist(
    overview: Literal["none", "short", "full"] = Query("none", description="Attach overview from TMDb"),
    locale: Optional[str] = Query(None, description="Override metadata locale (e.g., 'nl-NL')"),
    limit: int = Query(0, ge=0, le=5000, description="Slice the list"),
    max_meta: int = Query(250, ge=0, le=2000, description="Cap enriched items"),
) -> JSONResponse:
    try:
        from cw_platform.config_base import load_config
        from _syncAPI import _load_state
        from crosswatch import CACHE_DIR
        from _metaAPI import get_meta, _shorten
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"server import failed: {e}"}, status_code=200)

    cfg = load_config()
    st = _load_state()
    api_key = ((cfg.get("tmdb") or {}).get("api_key") or "").strip()
    has_key = bool(api_key)

    if not st:
        return JSONResponse({"ok": False, "error": "No snapshot found or empty.", "missing_tmdb_key": not has_key}, status_code=200)

    # No compat shim: expect new signature only
    try:
        items = build_watchlist(st, tmdb_ok=has_key) or []
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"{e.__class__.__name__}: {e}", "missing_tmdb_key": not has_key}, status_code=200)

    if not items:
        return JSONResponse({"ok": False, "error": "No snapshot data found.", "missing_tmdb_key": not has_key}, status_code=200)
    if limit:
        items = items[:limit]

    # Optional TMDb enrichment (skipped when key missing)
    enriched = 0
    eff_overview = overview if (overview != "none" and has_key) else "none"
    if eff_overview != "none":
        eff_locale = locale or (cfg.get("metadata") or {}).get("locale") or (cfg.get("ui") or {}).get("locale") or None

        def _norm_type(x: Optional[str]) -> str:
            t = (x or "").strip().lower()
            if t in {"tv", "show", "shows", "series", "season", "episode"}: return "tv"
            if t in {"movie", "movies", "film", "films"}:                   return "movie"
            return "movie"

        for it in items:
            if enriched >= int(max_meta): break
            tmdb_id = it.get("tmdb")
            if not tmdb_id: continue
            it["type"] = _norm_type(it.get("type") or it.get("entity") or it.get("media_type"))
            try:
                meta = get_meta(
                    api_key, it["type"], tmdb_id, CACHE_DIR,
                    need={"overview": True, "tagline": True, "title": True, "year": True},
                    locale=eff_locale,
                ) or {}
                desc = meta.get("overview") or ""
                if not desc: continue
                if eff_overview == "short": desc = _shorten(desc, 280)
                it["overview"] = desc
                if eff_overview == "short" and meta.get("tagline"): it["tagline"] = meta["tagline"]
                enriched += 1
            except Exception:
                continue

    return JSONResponse(
        {
            "ok": True,
            "items": items,
            "missing_tmdb_key": not has_key,
            "last_sync_epoch": st.get("last_sync_epoch"),
            "meta_enriched": enriched,
        },
        status_code=200,
    )


@router.delete("/{key}")
def api_watchlist_delete(
    key: str = FPath(...),
    provider: Optional[str] = Query("ALL", description="Provider id or ALL"),
) -> JSONResponse:
    from cw_platform.config_base import load_config
    from _syncAPI import _load_state
    from crosswatch import STATE_PATH, STATS, _append_log

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


@router.post("/delete")
def api_watchlist_delete_multi(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    provider = str(payload.get("provider") or "ALL").strip().upper()
    keys = payload.get("keys") or []
    return _bulk_delete(provider, keys)


@router.post("/delete_batch")
def api_watchlist_delete_batch(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    # Backward-compat alias; same behavior
    provider = str(payload.get("provider") or "ALL").strip().upper()
    keys = payload.get("keys") or []
    return _bulk_delete(provider, keys)
