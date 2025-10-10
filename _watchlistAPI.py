# _watchlistAPI.py

# --- stdlib / typing ---
from typing import Optional, Literal, Dict, Any, List
import urllib.parse

# --- third-party ---
from fastapi import APIRouter, Query, Body, Path as FPath
from fastapi.responses import JSONResponse

# --- app imports ---
from _watchlist import delete_watchlist_batch


router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])

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
        from _watchlist import build_watchlist
        from _metaAPI import get_meta, _shorten
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"server import failed: {e}"}, status_code=200)

    cfg = load_config()
    st = _load_state()
    api_key = ((cfg.get("tmdb") or {}).get("api_key") or "").strip()
    has_key = bool(api_key)

    if not st:
        return JSONResponse({"ok": False, "error": "No snapshot found or empty.", "missing_tmdb_key": not has_key}, status_code=200)

    def _build_watchlist_compat(state, key_present: bool):
        import inspect
        try:
            sig = inspect.signature(build_watchlist)
            params = sig.parameters
            if "tmdb_api_key_present" in params: return build_watchlist(state, tmdb_api_key_present=key_present)
            if "tmdb_ok" in params:               return build_watchlist(state, tmdb_ok=key_present)
            try: return build_watchlist(state, key_present)
            except TypeError: return build_watchlist(state)
        except TypeError:
            try: return build_watchlist(state, key_present)
            except Exception: return build_watchlist(state)

    try:
        items = _build_watchlist_compat(st, has_key) or []
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"{e.__class__.__name__}: {e}", "missing_tmdb_key": not has_key}, status_code=200)

    if not items:
        return JSONResponse({"ok": False, "error": "No snapshot data found.", "missing_tmdb_key": not has_key}, status_code=200)

    if limit and isinstance(limit, int) and limit > 0:
        items = items[:limit]

    # Optional TMDb enrichment; force-disabled when key missing
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
                if eff_overview == "short":
                    desc = _shorten(desc, 280)
                it["overview"] = desc
                if eff_overview == "short" and meta.get("tagline"):
                    it["tagline"] = meta["tagline"]
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
    provider: Optional[str] = Query("ALL", description="PLEX|TRAKT|SIMKL|JELLYFIN|ALL"),
) -> JSONResponse:
    from cw_platform.config_base import load_config
    from _syncAPI import _load_state
    from crosswatch import STATE_PATH, STATS, _append_log
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

def _wl_norm_key(x: Any) -> str:
    # Accept {"key": "..."} or plain string; decode if percent-encoded
    import urllib.parse
    s = str((x.get("key") if isinstance(x, dict) else x) or "").strip()
    return urllib.parse.unquote(s) if "%" in s else s

def _wl_active_providers(cfg) -> list[str]:
    # Only configured providers by id
    from _watchlist import detect_available_watchlist_providers
    VALID = {"PLEX", "SIMKL", "TRAKT", "JELLYFIN"}
    try:
        manifest = detect_available_watchlist_providers(cfg) or []
    except Exception:
        manifest = []
    out: list[str] = []
    for it in manifest:
        if not isinstance(it, dict):
            continue
        pid = str(it.get("id") or "").strip().upper()
        if pid and pid in VALID and bool(it.get("configured")) and pid not in out:
            out.append(pid)
    return out

def _wl_item_label(state: Dict[str, Any], key: str, prov: str) -> tuple[str, str]:
    # Returns (kind, label) → e.g. ("movie", "Inception (2010)")
    from _watchlist import _find_item_in_state_for_provider, _find_item_in_state, _type_from_item_or_guess
    it = (_find_item_in_state_for_provider(state, key, prov) or
          _find_item_in_state(state, key) or {})
    kind = _type_from_item_or_guess(it, key)  # "movie" | "tv"
    title = it.get("title") or it.get("name") or key
    y = it.get("year") or it.get("release_year")
    label = f"{title} ({y})" if y else str(title)
    return ("show" if kind == "tv" else "movie", label)


@router.post("/delete")
def api_watchlist_delete_multi(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    # Bulk delete; only configured providers; per-key; human logs
    from cw_platform.config_base import load_config
    from _syncAPI import _load_state
    from crosswatch import STATS, _append_log
    from _watchlist import delete_watchlist_batch

    try:
        keys_raw = payload.get("keys") or []
        provider = str(payload.get("provider") or "ALL").strip().upper()

        if not isinstance(keys_raw, list) or not keys_raw:
            return {"ok": False, "error": "keys must be a non-empty array"}
        if provider not in {"ALL", "PLEX", "SIMKL", "TRAKT", "JELLYFIN"}:
            return {"ok": False, "error": f"unknown provider '{provider}'"}

        keys = [k for k in (_wl_norm_key(k) for k in keys_raw) if k]
        keys = list(dict.fromkeys(keys))

        cfg = load_config()
        state = _load_state()
        active = _wl_active_providers(cfg)

        if provider == "ALL":
            targets = [p for p in ["PLEX", "SIMKL", "TRAKT", "JELLYFIN"] if p in active]
            if not targets:
                return {"ok": False, "error": "no connected providers"}
        else:
            if provider not in active:
                return {"ok": False, "error": f"provider '{provider}' not connected"}
            targets = [provider]

        results: List[Dict[str, Any]] = []
        deleted_sum = 0

        for prov in targets:
            try:
                per_key: List[Dict[str, Any]] = []
                deleted = 0
                for k in keys:
                    kind, label = _wl_item_label(state, k, prov)
                    safe_label = (label or "").replace("'", "’")
                    r = delete_watchlist_batch([k], prov, state, cfg) or {}
                    d = int(r.get("deleted", 0)) if isinstance(r, dict) else 0
                    per_key.append({"key": k, "deleted": d})
                    deleted += d
                    _append_log("SYNC", f"[WL] delete 1 {kind} '{safe_label}' on {prov}: {'OK' if d else 'NOOP'}")

                results.append({"provider": prov, "ok": deleted > 0, "deleted": deleted, "per_key": per_key})
                deleted_sum += deleted
            except Exception as e:
                results.append({"provider": prov, "ok": False, "error": str(e)})
                _append_log("SYNC", f"[WL] delete on {prov} failed: {e}")

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
            "provider": provider,
            "targets": targets,
            "deleted_ok": deleted_sum,
            "deleted_total": len(keys),
            "results": results,
        }
    except Exception as e:
        _append_log("SYNC", f"[WL] delete fatal: {e}")
        return {"ok": False, "error": f"fatal: {e}"}


@router.post("/delete_batch")
def api_watchlist_delete_batch(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    from cw_platform.config_base import load_config
    from _syncAPI import _load_state
    from crosswatch import STATS, _append_log
    from _watchlist import delete_watchlist_batch

    try:
        keys_raw = payload.get("keys") or []
        provider = str(payload.get("provider") or "ALL").strip().upper()

        if not isinstance(keys_raw, list) or not keys_raw:
            return {"ok": False, "error": "keys must be a non-empty array"}
        if provider not in {"ALL", "PLEX", "SIMKL", "TRAKT", "JELLYFIN"}:
            return {"ok": False, "error": f"unknown provider '{provider}'"}

        keys = [k for k in (_wl_norm_key(k) for k in keys_raw) if k]
        keys = list(dict.fromkeys(keys))

        cfg = load_config()
        state = _load_state()
        active = _wl_active_providers(cfg)

        if provider == "ALL":
            targets = [p for p in ["PLEX", "SIMKL", "TRAKT", "JELLYFIN"] if p in active]
            if not targets:
                return {"ok": False, "error": "no connected providers"}
        else:
            if provider not in active:
                return {"ok": False, "error": f"provider '{provider}' not connected"}
            targets = [provider]

        results: List[Dict[str, Any]] = []
        deleted_sum = 0

        for prov in targets:
            try:
                per_key: List[Dict[str, Any]] = []
                deleted = 0
                for k in keys:
                    kind, label = _wl_item_label(state, k, prov)
                    safe_label = (label or "").replace("'", "’")
                    r = delete_watchlist_batch([k], prov, state, cfg) or {}
                    d = int(r.get("deleted", 0)) if isinstance(r, dict) else 0
                    per_key.append({"key": k, "deleted": d})
                    deleted += d
                    _append_log("SYNC", f"[WL] delete 1 {kind} '{safe_label}' on {prov}: {'OK' if d else 'NOOP'}")


                results.append({"provider": prov, "ok": deleted > 0, "deleted": deleted, "per_key": per_key})
                deleted_sum += deleted
            except Exception as e:
                results.append({"provider": prov, "ok": False, "error": str(e)})
                _append_log("SYNC", f"[WL] batch-delete on {prov} failed: {e}")

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
            "provider": provider,
            "targets": targets,
            "deleted_ok": deleted_sum,
            "deleted_total": len(keys),
            "results": results,
        }
    except Exception as e:
        _append_log("SYNC", f"[WL] batch-delete fatal: {e}")
        return {"ok": False, "error": f"fatal: {e}"}