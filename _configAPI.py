# _configAPI.py
from __future__ import annotations
from typing import Any, Dict

import threading
from fastapi import APIRouter, Body, HTTPException
from fastapi.responses import JSONResponse
from pathlib import Path

from cw_platform.config_base import load_config, save_config, CONFIG as CONFIG_DIR

router = APIRouter(prefix="/api", tags=["config"])

def _env():
    try:
        import crosswatch as CW
        from cw_platform import config_base
        from cw_platform.config_base import load_config, save_config
        # helpers (best-effort)
        _prune   = getattr(CW, "_prune_legacy_ratings", lambda *_: None)
        _ensure  = getattr(CW, "_ensure_pair_ratings_defaults", lambda *_: None)
        _normpr  = getattr(CW, "_normalize_pair_ratings", lambda *_: None)
        PROBES_CACHE        = getattr(CW, "PROBES_CACHE", None)
        PROBES_STATUS_CACHE = getattr(CW, "PROBES_STATUS_CACHE", None)
        scheduler           = globals().get("scheduler") or getattr(CW, "scheduler", None)
        return CW, config_base, load_config, save_config, _prune, _ensure, _normpr, PROBES_CACHE, PROBES_STATUS_CACHE, scheduler
    except Exception:
        return (None, None, lambda: {}, lambda *_: None, lambda *_: None, lambda *_: None, lambda *_: None, None, None, None)

def _nostore(res: JSONResponse) -> JSONResponse:
    res.headers["Cache-Control"] = "no-store"; return res

@router.get("/config")
def api_config() -> JSONResponse:
    CW, config_base, load_config, _, _prune, _ensure, *_ = _env()
    cfg = dict(load_config() or {})
    try:
        _prune(cfg); _ensure(cfg)
    except Exception:
        pass
    try:
        cfg = config_base.redact_config(cfg)
    except Exception:
        pass
    return _nostore(JSONResponse(cfg))

@router.post("/config")
def api_config_save(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    CW, config_base, load_config, save_config, _prune, _ensure, _normpr, PROBES_CACHE, PROBES_STATUS_CACHE, scheduler = _env()

    incoming = dict(payload or {})
    current  = dict(load_config() or {})

    # deep merge with graceful fallback
    try:
        merged = config_base._deep_merge(current, incoming)  # type: ignore[attr-defined]
    except Exception:
        merged = {**current, **incoming}

    # keep secrets when masked/blank
    def _blank(v: Any) -> bool:
        s = ("" if v is None else str(v)).strip()
        return s == "" or s == "••••••••"

    SECRET_PATHS = [
        ("plex", "account_token"),
        ("simkl", "access_token"), ("simkl", "refresh_token"),
        ("trakt", "client_secret"), ("trakt", "access_token"), ("trakt", "refresh_token"),
        ("tmdb", "api_key"),
        ("jellyfin", "access_token"),
        ("emby", "api_key"),
        ("emby", "access_token"),
    ]

    for path in SECRET_PATHS:
        cur = current; inc = incoming; dst = merged
        for k in path[:-1]:
            cur = cur.get(k, {}) if isinstance(cur, dict) else {}
            inc = inc.get(k, {}) if isinstance(inc, dict) else {}
            dst = dst.setdefault(k, {}) if isinstance(dst, dict) else {}
        leaf = path[-1]
        if isinstance(inc, dict) and leaf in inc and _blank(inc[leaf]):
            dst[leaf] = (cur or {}).get(leaf, "")

    cfg = merged

    # scrobble mode normalization → feature.watch.enabled
    sc = cfg.setdefault("scrobble", {})
    sc_enabled = bool(sc.get("enabled", False))
    mode = (sc.get("mode") or "").strip().lower()
    if mode not in ("webhook", "watch"):
        legacy_webhook = bool((cfg.get("webhook") or {}).get("enabled"))
        mode = "webhook" if legacy_webhook else ("watch" if sc_enabled else "")
        if mode: sc["mode"] = mode
    if mode == "watch":
        pass
    elif mode == "webhook":
        sc.setdefault("watch", {}).setdefault("autostart", bool(sc.get("watch", {}).get("autostart", False)))
    else:
        sc["enabled"] = False

    features = cfg.setdefault("features", {})
    watch_feat = features.setdefault("watch", {})
    autostart = bool(sc.get("watch", {}).get("autostart", False))
    watch_feat["enabled"] = bool(sc_enabled and mode == "watch" and autostart)

    # ratings normalization
    try:
        _prune(cfg)
        for p in (cfg.get("pairs") or []):
            try: _normpr(p)
            except Exception: pass
    except Exception:
        pass

    save_config(cfg)

    # bust probe caches (best-effort)
    try:
        if isinstance(PROBES_CACHE, dict):
            PROBES_CACHE.update({k: (0.0, False) for k in ("plex","simkl","trakt","jellyfin","emby")})
        if isinstance(PROBES_STATUS_CACHE, dict):
            PROBES_STATUS_CACHE["ts"] = 0.0; PROBES_STATUS_CACHE["data"] = None
    except Exception:
        pass

    # nudge scheduler
    try:
        if hasattr(scheduler, "refresh_ratings_watermarks"):
            scheduler.refresh_ratings_watermarks()
    except Exception:
        pass
    try:
        s = (cfg.get("scheduling") or {})
        if scheduler is not None:
            if bool(s.get("enabled")):
                if hasattr(scheduler, "start"):   scheduler.start()
                if hasattr(scheduler, "refresh"): scheduler.refresh()
            else:
                if hasattr(scheduler, "stop"):    scheduler.stop()
    except Exception:
        pass

    return {"ok": True}
