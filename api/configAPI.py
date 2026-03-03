# /api/configAPI.py
# CrossWatch - Configuration API for multiple services
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import json
import os
from datetime import datetime, timezone

from packaging.version import InvalidVersion, Version

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


def _norm_ver(v: str | None) -> str:
    raw = (v or "").strip()
    if raw.lower().startswith("v"):
        raw = raw[1:]
    return raw


def _safe_ver(v: str | None) -> Version:
    try:
        return Version(_norm_ver(v) or "0.0.0")
    except InvalidVersion:
        return Version("0.0.0")


@router.get("/config/meta")
def api_config_meta() -> JSONResponse:
    env = _env()
    base = env.get("cfg_base")
    try:
        path = base.config_path()  # type: ignore[attr-defined]
    except Exception:
        path = None

    p = path
    exists = bool(p and getattr(p, "exists", lambda: False)())
    raw: dict[str, Any] = {}
    if exists and p is not None:
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            raw = {}

    cfg_ver = _norm_ver(raw.get("version") if isinstance(raw, dict) else None) or None
    try:
        from api.versionAPI import CURRENT_VERSION as _V
        cur_ver = _norm_ver(str(_V))
    except Exception:
        cur_ver = _norm_ver(os.getenv("APP_VERSION") or "v0.7.0")

    needs_upgrade = False
    is_legacy_pre_070 = False
    try:
        needs_upgrade = _safe_ver(cur_ver) > _safe_ver(cfg_ver)
        is_legacy_pre_070 = _safe_ver(cfg_ver) < Version("0.7.0")
    except Exception:
        needs_upgrade = False
        is_legacy_pre_070 = False

    mtime = None
    size = None
    if exists and p is not None:
        try:
            st = p.stat()
            size = int(st.st_size)
            mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat()
        except Exception:
            pass

    return _nostore(
        JSONResponse(
            {
                "exists": exists,
                "path": str(p) if p is not None else None,
                "size": size,
                "mtime": mtime,
                "current_version": cur_ver,
                "config_version": cfg_ver,
                "needs_upgrade": needs_upgrade,
                "legacy_pre_070": is_legacy_pre_070,
            }
        )
    )

@router.get("/config")
def api_config() -> JSONResponse:
    env = _env()
    cfg = dict(env["load"]() or {})
    try: env["prune"](cfg); env["ensure"](cfg)
    except Exception: pass
    base = env.get("cfg_base")
    if base is None or not hasattr(base, "redact_config"):
        return _nostore(JSONResponse({"ok": False, "error": "Config redaction unavailable"}, status_code=503))
    cfg = base.redact_config(cfg)  # type: ignore[attr-defined]
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

    MASK = "••••••••"

    def _blank(v: Any) -> bool:
        s = ("" if v is None else str(v)).strip()
        return s in {"", MASK}

    def _is_sensitive_key(key: Any) -> bool:
        k = str(key or "").strip().lower()
        if not k:
            return False

        exact = {
            "api_key", "apikey",
            "access_token", "refresh_token",
            "client_secret",
            "account_token", "pms_token", "home_pin",
            "session_id",
            "token_hash", "salt", "hash",
            "device_code",
            "_pending_request_token",
            "request_token",
            "token",
        }
        if k in exact:
            return True

        # Catch foo_token, but avoid common non-secret config like token_endpoint/url
        if k.endswith("_token") and k not in {"token_endpoint", "token_url"}:
            return True

        subs = (
            "access_token", "refresh_token", "client_secret",
            "api_key", "apikey",
            "token_hash", "session_id",
            "account_token", "pms_token", "home_pin",
            "device_code", "request_token",
            "password", "secret",
        )
        return any(s in k for s in subs)

    def _preserve_sensitive(cur: Any, inc: Any, dst: Any) -> None:
        """Preserve sensitive leaves when UI sends blank/masked placeholders."""
        if isinstance(inc, dict) and isinstance(dst, dict):
            cur_d = cur if isinstance(cur, dict) else {}
            for k, inc_v in inc.items():
                if k not in dst:
                    continue

                cur_v = cur_d.get(k) if isinstance(cur_d, dict) else None
                dst_v = dst.get(k)

                if isinstance(inc_v, dict) and isinstance(dst_v, dict):
                    _preserve_sensitive(cur_v, inc_v, dst_v)
                    continue

                if isinstance(inc_v, list) and isinstance(dst_v, list):
                    if isinstance(cur_v, list):
                        for i in range(min(len(inc_v), len(dst_v), len(cur_v))):
                            _preserve_sensitive(cur_v[i], inc_v[i], dst_v[i])
                    continue

                if _is_sensitive_key(k) and _blank(inc_v):
                    if isinstance(cur_d, dict) and k in cur_d:
                        dst[k] = cur_v
                    else:
                        dst[k] = ""
            return

        if isinstance(inc, list) and isinstance(cur, list) and isinstance(dst, list):
            for i in range(min(len(inc), len(cur), len(dst))):
                _preserve_sensitive(cur[i], inc[i], dst[i])

    _preserve_sensitive(current, incoming, merged)

    try:
        inc_a = incoming.get("app_auth")
        cur_a = current.get("app_auth")
        if isinstance(inc_a, dict) and "sessions" in inc_a and isinstance(cur_a, dict):
            cur_s = cur_a.get("sessions")
            if isinstance(cur_s, list):
                merged.setdefault("app_auth", {})["sessions"] = cur_s
    except Exception:
        pass

    cfg: dict[str, Any] = dict(merged or {})


    # Scrobble watcher: ensure routes exist when legacy fields are used
    try:
        from providers.scrobble.routes import ensure_routes
        cfg, _ = ensure_routes(cfg)
    except Exception:
        pass

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
