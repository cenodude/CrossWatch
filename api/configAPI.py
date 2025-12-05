# api/configAPI.py
# CrossWatch - Configuration API for multiple services
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

def _env() -> dict[str, Any]:
    try:
        import crosswatch as CW
        from cw_platform import config_base
        from cw_platform.config_base import load_config, save_config
    except ImportError:
        return {
            "CW": None, "cfg_base": None,
            "load": lambda: {}, "save": lambda *_: None,
            "prune": lambda *_: None, "ensure": lambda *_: None, "norm_pair": lambda *_: None,
            "probes_cache": None, "probes_status_cache": None, "scheduler": None,
        }

    return {
        "CW": CW,
        "cfg_base": config_base,
        "load": load_config,
        "save": save_config,
        "prune": getattr(CW, "_prune_legacy_ratings", lambda *_: None),
        "ensure": getattr(CW, "_ensure_pair_ratings_defaults", lambda *_: None),
        "norm_pair": getattr(CW, "_normalize_pair_ratings", lambda *_: None),
        "probes_cache": getattr(CW, "PROBES_CACHE", None),
        "probes_status_cache": getattr(CW, "PROBES_STATUS_CACHE", None),
        "scheduler": getattr(CW, "scheduler", None),
    }

def _nostore(res: JSONResponse) -> JSONResponse:
    res.headers["Cache-Control"] = "no-store"
    return res

router = APIRouter(prefix="/api", tags=["config"])

@router.get("/config")
def api_config() -> JSONResponse:
    env = _env()
    cfg = dict(env["load"]() or {})
    try: env["prune"](cfg); env["ensure"](cfg)
    except Exception: pass
    try: cfg = env["cfg_base"].redact_config(cfg)  # type: ignore[attr-defined]
    except Exception: pass
    return _nostore(JSONResponse(cfg))

@router.post("/config")
def api_config_save(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    env = _env()
    incoming = dict(payload or {})
    current  = dict(env["load"]() or {})

    try:
        merged = env["cfg_base"].__dict__.get("_deep_merge", lambda a,b: {**a, **b})(current, incoming)  # type: ignore
    except Exception:
        merged = {**current, **incoming}

    def _blank(v: Any) -> bool:
        s = ("" if v is None else str(v)).strip()
        return s in {"", "••••••••"}

    secrets = [
        ("plex","account_token"),
        ("simkl","access_token"), ("simkl","refresh_token"),
        ("trakt","client_secret"), ("trakt","access_token"), ("trakt","refresh_token"),
        ("tmdb","api_key"),
        ("jellyfin","access_token"),
        ("emby","api_key"), ("emby","access_token"),
        ("mdblist","api_key"),
    ]
    for path in secrets:
        cur = current; inc = incoming; dst = merged
        for k in path[:-1]:
            cur = cur.get(k, {}) if isinstance(cur, dict) else {}
            inc = inc.get(k, {}) if isinstance(inc, dict) else {}
            dst = dst.setdefault(k, {}) if isinstance(dst, dict) else {}
        leaf = path[-1]
        if isinstance(inc, dict) and leaf in inc and _blank(inc[leaf]):
            dst[leaf] = (cur or {}).get(leaf, "")

    cfg = merged

    sc = cfg.setdefault("scrobble", {})
    sc_enabled = bool(sc.get("enabled", False))
    mode = str(sc.get("mode") or "").strip().lower()
    if mode not in {"webhook","watch"}:
        legacy_webhook = bool((cfg.get("webhook") or {}).get("enabled"))
        mode = "webhook" if legacy_webhook else ("watch" if sc_enabled else "")
        if mode: sc["mode"] = mode
    if mode == "webhook":
        sc.setdefault("watch", {}).setdefault("autostart", bool(sc.get("watch", {}).get("autostart", False)))
    elif mode != "watch":
        sc["enabled"] = False

    features = cfg.setdefault("features", {})
    watch_feat = features.setdefault("watch", {})
    watch_feat["enabled"] = bool(sc_enabled and mode == "watch" and sc.get("watch", {}).get("autostart", False))

    try:
        env["prune"](cfg)
        for p in (cfg.get("pairs") or []):
            try: env["norm_pair"](p)
            except Exception: pass
    except Exception:
        pass

    env["save"](cfg)

    try:
        pc = env["probes_cache"]; ps = env["probes_status_cache"]
        if isinstance(pc, dict):
            for k in ("plex","simkl","trakt","jellyfin","emby"):
                pc[k] = (0.0, False)
        if isinstance(ps, dict):
            ps["ts"] = 0.0; ps["data"] = None
    except Exception:
        pass

    try:
        sched = env["scheduler"]
        if hasattr(sched, "refresh_ratings_watermarks"):
            sched.refresh_ratings_watermarks()
        s = cfg.get("scheduling") or {}
        if sched is not None:
            if bool(s.get("enabled")):
                getattr(sched, "start", lambda: None)()
                getattr(sched, "refresh", lambda: None)()
            else:
                getattr(sched, "stop", lambda: None)()
    except Exception:
        pass

    return {"ok": True}
