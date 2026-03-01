# /api/watchlistAPI.py
# CrossWatch - Unified watchlist manager for multiple services
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import urllib.parse
from typing import Any, Literal, cast

from fastapi import APIRouter, Body, Path as FPath, Query
from fastapi.responses import JSONResponse

from services.watchlist import (
    _find_item_in_state,
    _find_item_in_state_for_provider,
    build_watchlist,
    delete_watchlist_batch,
    delete_watchlist_item,
    detect_available_watchlist_providers,
)

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])


def _norm_key(x: Any) -> str:
    s = str((x.get("key") if isinstance(x, dict) else x) or "").strip()
    return urllib.parse.unquote(s) if "%" in s else s


def _active_providers(cfg: dict[str, Any]) -> list[str]:
    try:
        manifest = detect_available_watchlist_providers(cfg) or []
    except Exception:
        manifest = []

    out: list[str] = []
    for it in manifest:
        if not isinstance(it, dict):
            continue
        pid = str(it.get("id") or "").strip().upper()
        if pid and pid != "ALL" and (bool(it.get("configured")) or pid == "CROSSWATCH") and pid not in out:
            out.append(pid)

    if "CROSSWATCH" not in out:
        out.insert(0, "CROSSWATCH")
    return out


def _tmdb_api_key(cfg: dict[str, Any]) -> str:
    def _pick_from_block(blk: Any) -> str:
        if not isinstance(blk, dict):
            return ""
        k = str(blk.get("api_key") or "").strip()
        if k:
            return k
        insts = blk.get("instances")
        if isinstance(insts, dict):
            for v in insts.values():
                kk = str((v or {}).get("api_key") or "").strip() if isinstance(v, dict) else ""
                if kk:
                    return kk
        return ""

    for key in ("tmdb", "tmdb_sync"):
        found = _pick_from_block(cfg.get(key))
        if found:
            return found
    return ""

def _type_from_item_or_guess(item: dict[str, Any], key: str = "") -> str:
    t = str(item.get("type") or item.get("media_type") or item.get("entity") or "").lower().strip()
    if t in ("tv", "show", "shows", "series", "episode", "season", "anime"):
        return "tv"
    ids = item.get("ids") or {}
    if isinstance(ids, dict) and (ids.get("anilist") or ids.get("mal")):
        return "tv"
    pref = (key or "").split(":", 1)[0].lower().strip()
    if pref in ("anilist", "mal"):
        return "tv"
    return "movie"

def _item_label(state: dict[str, Any], key: str, prov: str) -> tuple[str, str]:
    it = (
        _find_item_in_state_for_provider(state, key, prov)
        or _find_item_in_state(state, key)
        or {}
    )
    kind = _type_from_item_or_guess(it, key)
    title = it.get("title") or it.get("name") or key
    year = it.get("year") or it.get("release_year")
    return "show" if kind == "tv" else "movie", f"{title} ({year})" if year else str(title)


def _candidate_keys_from_ids(ids: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    for k in ("tmdb", "imdb", "tvdb", "trakt", "simkl", "anilist", "mal"):
        v = ids.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        if k == "imdb":
            if s.isdigit():
                s = f"tt{s}"
            elif not s.startswith("tt"):
                s = f"tt{s}"
        keys.append(f"{k}:{s}")

    seen: set[str] = set()
    out: list[str] = []
    for k in keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out

def _bulk_delete(provider: str, keys_raw: list[Any], provider_instance: str | None = None) -> dict[str, Any]:
    from cw_platform.config_base import load_config
    from crosswatch import STATS, _append_log
    from .syncAPI import _load_state

    if not isinstance(keys_raw, list) or not keys_raw:
        return {"ok": False, "error": "keys must be a non-empty array"}

    keys = [k for k in (_norm_key(k) for k in keys_raw) if k]
    keys = list(dict.fromkeys(keys))

    cfg = load_config()
    state = _load_state() or {}
    active = _active_providers(cfg)
    prov = (provider or "ALL").upper().strip()
    inst_p = provider_instance if prov != "ALL" else None

    if prov == "ALL":
        targets = active[:]
        if not targets:
            return {"ok": False, "error": "no connected providers"}
    else:
        if prov not in active:
            return {"ok": False, "error": f"provider '{prov}' not connected"}
        targets = [prov]

    results: list[dict[str, Any]] = []
    deleted_sum = 0

    for p in targets:
        try:
            per_key: list[dict[str, Any]] = []
            deleted = 0
            for k in keys:
                if not _find_item_in_state_for_provider(state, k, p, instance_id=inst_p):
                    per_key.append({"key": k, "deleted": 0, "attempted": False, "reason": "not_in_state"})
                    continue
                kind, label = _item_label(state, k, p)
                safe_label = (label or "").replace("'", "’")
                r = delete_watchlist_batch([k], p, state, cfg, provider_instance=inst_p) or {}
                d = int(r.get("deleted", 0)) if isinstance(r, dict) else 0
                per_key.append({"key": k, "deleted": d, "attempted": True})
                deleted += d
                _append_log(
                    "SYNC",
                    f"[WL] delete 1 {kind} '{safe_label}' on {p}: {'OK' if d else 'NOOP'}",
                )
            results.append(
                {
                    "provider": p,
                    "ok": deleted > 0,
                    "deleted": deleted,
                    "per_key": per_key,
                }
            )
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

# Auto-remove code
def remove_across_providers_by_ids(
    ids: dict[str, Any],
    media_type: str | None = None,
) -> dict[str, Any]:
    from cw_platform.config_base import load_config
    from crosswatch import _append_log
    from .syncAPI import _load_state

    cfg = load_config()
    state = _load_state() or {}
    if not ids or not isinstance(ids, dict):
        return {"ok": False, "error": "missing ids"}

    keys = _candidate_keys_from_ids(ids)
    if not keys:
        return {"ok": False, "error": "no candidate keys from ids"}

    providers = _active_providers(cfg)
    if not providers:
        return {"ok": False, "error": "no connected providers"}

    results: list[dict[str, Any]] = []
    total_deleted = 0

    for prov in providers:
        found_key = None
        for k in keys:
            if _find_item_in_state_for_provider(state, k, prov):
                found_key = k
                break

        if not found_key:
            results.append(
                {
                    "provider": prov,
                    "ok": False,
                    "reason": "not_in_state",
                    "attempted": False,
                }
            )
            continue

        try:
            r = delete_watchlist_batch([found_key], prov, state, cfg) or {}
            deleted = int(r.get("deleted", 0)) if isinstance(r, dict) else 0
            total_deleted += deleted
            ok = deleted > 0
            results.append(
                {
                    "provider": prov,
                    "ok": ok,
                    "deleted": deleted,
                    "key": found_key,
                }
            )
            kind, label = _item_label(state, found_key, prov)
            safe_label = (label or "").replace("'", "’")
            _append_log(
                "SYNC",
                f"[WL] auto-remove by ids: {kind} '{safe_label}' on {prov}: {'OK' if ok else 'NOOP'}",
            )
        except Exception as e:
            results.append({"provider": prov, "ok": False, "error": str(e)})
            _append_log("SYNC", f"[WL] auto-remove on {prov} failed: {e}")

    any_ok = any(r.get("ok") for r in results)
    return {
        "ok": any_ok,
        "deleted_ok": sum(int(r.get("deleted", 0)) for r in results if r.get("ok")),
        "results": results,
    }


def remove_from_provider_by_ids(
    provider: str,
    ids: dict[str, Any],
    media_type: str | None = None,
) -> dict[str, Any]:
    from cw_platform.config_base import load_config
    from crosswatch import _append_log
    from .syncAPI import _load_state

    cfg = load_config()
    state = _load_state() or {}
    prov = (provider or "").strip().upper()
    if not prov:
        return {"ok": False, "error": "missing provider"}
    if prov not in _active_providers(cfg):
        return {"ok": False, "error": f"provider '{prov}' not connected"}

    keys = _candidate_keys_from_ids(ids)
    if not keys:
        return {"ok": False, "error": "no candidate keys from ids"}

    found_key = None
    for k in keys:
        if _find_item_in_state_for_provider(state, k, prov):
            found_key = k
            break

    if not found_key:
        return {"ok": False, "reason": "not_in_state"}

    try:
        r = delete_watchlist_batch([found_key], prov, state, cfg) or {}
        deleted = int(r.get("deleted", 0)) if isinstance(r, dict) else 0
        ok = deleted > 0
        kind, label = _item_label(state, found_key, prov)
        _append_log(
            "SYNC",
            f"[WL] remove_by_ids on {prov}: {kind} '{label}' → {'OK' if ok else 'NOOP'}",
        )
        return {"ok": ok, "deleted": deleted, "provider": prov, "key": found_key}
    except Exception as e:
        _append_log("SYNC", f"[WL] remove_by_ids on {prov} failed: {e}")
        return {"ok": False, "error": str(e), "provider": prov}


def remove_from_plex_by_ids(
    ids: dict[str, Any],
    media_type: str | None = None,
) -> dict[str, Any]:
    return remove_from_provider_by_ids("PLEX", ids, media_type)



@router.get("", include_in_schema=False)
@router.get("/")
def api_watchlist(
    overview: Literal["none", "short", "full"] = Query(
        "none",
        description="Attach overview from TMDb",
    ),
    locale: str | None = Query(
        None,
        description="Override metadata locale",
    ),
    limit: int = Query(
        0,
        ge=0,
        le=5000,
        description="Slice the list",
    ),
    max_meta: int = Query(
        250,
        ge=0,
        le=2000,
        description="Cap enriched items",
    ),
) -> JSONResponse:

    try:
        from cw_platform.config_base import load_config
        from crosswatch import CACHE_DIR
        from .metaAPI import _shorten, get_meta
        from .syncAPI import _load_state
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"server import failed: {e}"}, status_code=200)

    cfg = load_config()
    st = _load_state()
    api_key = _tmdb_api_key(cfg)
    has_key = bool(api_key)

    if not st:
        return JSONResponse(
            {"ok": False, "error": "No snapshot found or empty.", "missing_tmdb_key": not has_key},
            status_code=200,
        )

    try:
        items = build_watchlist(st, tmdb_ok=has_key) or []
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": f"{e.__class__.__name__}: {e}", "missing_tmdb_key": not has_key},
            status_code=200,
        )

    if not items:
        return JSONResponse(
            {"ok": False, "error": "No snapshot data found.", "missing_tmdb_key": not has_key},
            status_code=200,
        )

    if limit:
        items = items[:limit]

    enriched = 0
    eff_overview = overview if (overview != "none" and has_key) else "none"

    if eff_overview != "none":
        eff_locale = (
            locale
            or (cfg.get("metadata") or {}).get("locale")
            or (cfg.get("ui") or {}).get("locale")
            or None
        )

        def _norm_type(x: str | None) -> str:
            t = (x or "").strip().lower()
            if t in {"tv", "show", "shows", "series", "season", "episode"}:
                return "tv"
            if t in {"movie", "movies", "film", "films"}:
                return "movie"
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
                    api_key,
                    it["type"],
                    tmdb_id,
                    CACHE_DIR,
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
    provider: str | None = Query("ALL", description="Provider id or ALL"),
    provider_instance: str | None = Query(None, description="Provider instance id (optional)"),
) -> JSONResponse:
    from cw_platform.config_base import load_config
    from crosswatch import STATE_PATH, STATS, _append_log
    from .syncAPI import _load_state

    if "%" in (key or ""):
        key = urllib.parse.unquote(key)

    prov = (provider or "ALL").upper().strip()
    raw_res = delete_watchlist_item(
        key=key,
        state_path=STATE_PATH,
        cfg=load_config(),
        provider=prov,
        provider_instance=provider_instance,
        log=_append_log,
    )

    res: dict[str, Any]
    if isinstance(raw_res, dict):
        res = raw_res
    else:
        res = {"ok": bool(raw_res)}

    if res.get("ok"):
        try:
            state = _load_state()
            if state:
                STATS.refresh_from_state(state)
        except Exception:
            pass

    res.setdefault("provider", prov)
    
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))

@router.post("/delete")
def api_watchlist_delete_multi(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    provider = str(payload.get("provider") or "ALL").strip().upper()
    provider_instance = payload.get("provider_instance")
    keys = payload.get("keys") or []
    return _bulk_delete(provider, keys, provider_instance=provider_instance)


@router.post("/delete_batch")
def api_watchlist_delete_batch(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    provider = str(payload.get("provider") or "ALL").strip().upper()
    provider_instance = payload.get("provider_instance")
    keys = payload.get("keys") or []
    return _bulk_delete(provider, keys, provider_instance=provider_instance)