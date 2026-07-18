# /api/configAPI.py
# CrossWatch - Configuration API for multiple services
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import json
import os
from datetime import datetime, timezone

from packaging.version import InvalidVersion, Version

from fastapi import APIRouter, Body, Request
from fastapi.responses import JSONResponse
from providers.scrobble.sources import source_enabled
from _logging import log as BASE_LOG

BACKUP_LOG = BASE_LOG.child("BACKUP")
WATCH_LOG = BASE_LOG.child("WATCH")

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

    probes_cache = getattr(CW, "PROBES_CACHE", None)
    probes_status_cache = getattr(CW, "PROBES_STATUS_CACHE", None)

    if not isinstance(probes_cache, dict) or not isinstance(probes_status_cache, dict):
        try:
            from api.probesAPI import PROBE_CACHE, STATUS_CACHE
            probes_cache = PROBE_CACHE
            probes_status_cache = STATUS_CACHE
        except Exception:
            pass

    return {
        "CW": CW,
        "cfg_base": config_base,
        "load": load_config,
        "save": save_config,
        "prune": getattr(CW, "_prune_legacy_ratings", lambda *_: None),
        "ensure": getattr(CW, "_ensure_pair_ratings_defaults", lambda *_: None),
        "norm_pair": getattr(CW, "_normalize_pair_ratings", lambda *_: None),
        "probes_cache": probes_cache,
        "probes_status_cache": probes_status_cache,
        "scheduler": getattr(CW, "scheduler", None),
    }

def _nostore(res: JSONResponse) -> JSONResponse:
    res.headers["Cache-Control"] = "no-store"
    return res

router = APIRouter(prefix="/api", tags=["config"])

_LAST_SCROBBLE_SOURCE_LOG: tuple[bool, bool, bool, tuple[tuple[str, str], ...]] | None = None


def _log_scrobble_source_state(env: dict[str, Any], cfg: dict[str, Any]) -> None:
    global _LAST_SCROBBLE_SOURCE_LOG
    try:
        from providers.webhooks.config import active_webhook_endpoints, describe_active_webhooks

        sc = cfg.get("scrobble") if isinstance(cfg.get("scrobble"), dict) else {}
        enabled = bool((sc or {}).get("enabled"))
        webhook = source_enabled(cfg, "webhook") and bool(active_webhook_endpoints(cfg))
        watcher = source_enabled(cfg, "watcher")
        webhook_lines = describe_active_webhooks(cfg) if webhook else []
        state = (enabled, webhook, watcher, tuple(webhook_lines))
        if state == _LAST_SCROBBLE_SOURCE_LOG:
            return
        _LAST_SCROBBLE_SOURCE_LOG = state

        cw = env.get("CW")
        logger_cls = getattr(cw, "_UIHostLogger", None) if cw is not None else None
        base_log = getattr(cw, "LOG", None) if cw is not None else None
        def emit(message: str, level: str = "INFO", module: str = "SCROBBLE") -> None:
            if callable(base_log):
                base_log(message, level=level, module=module)
                return
            logger = logger_cls(module, module) if callable(logger_cls) else None
            if callable(logger):
                logger(message, level=level)

        for message, level in webhook_lines:
            emit(message, level=level, module="WEBHOOK")
        if watcher:
            emit("Watcher source enabled", module="WATCH")
        if webhook and watcher:
            emit(
                "Webhook and Watcher are both enabled, do not use both for the same tracker.",
                level="WARN",
                module="SCROBBLE",
            )
        if not webhook and not watcher:
            emit("Scrobble sources disabled")
    except Exception:
        pass


def _after_config_save(env: dict[str, Any], cfg: dict[str, Any]) -> None:
    _log_scrobble_source_state(env, cfg)

    try:
        pc = env["probes_cache"]; ps = env["probes_status_cache"]
        if isinstance(pc, dict):
            for k in list(pc.keys()):
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
        effective_enabled = bool(
            (s or {}).get("enabled") or ((s or {}).get("advanced") or {}).get("enabled")
        )
        if sched is not None:
            if effective_enabled:
                getattr(sched, "start", lambda: None)()
                getattr(sched, "refresh", lambda: None)()
            else:
                getattr(sched, "stop", lambda: None)()
    except Exception:
        pass

def _stable_json(v: Any) -> str:
    try:
        return json.dumps(v, sort_keys=True, separators=(",", ":"), default=str)
    except Exception:
        return str(v)


def _watch_runtime_fingerprint(cfg: dict[str, Any]) -> str:
    sc = cfg.get("scrobble")
    sc = sc if isinstance(sc, dict) else {}
    watch = sc.get("watch")
    watch = watch if isinstance(watch, dict) else {}
    sources = sc.get("sources")
    sources = sources if isinstance(sources, dict) else {}
    routes = watch.get("routes")
    relevant = {
        "scrobble_enabled": bool(sc.get("enabled")),
        "mode": str(sc.get("mode") or ""),
        "source_watcher": bool(sources.get("watcher", sources.get("watch", False))),
        "watch_autostart": bool(watch.get("autostart")),
        "routes": routes if isinstance(routes, list) else None,
    }
    return _stable_json(relevant)


def _watch_runtime_changed(before: dict[str, Any], after: dict[str, Any]) -> bool:
    return _watch_runtime_fingerprint(before) != _watch_runtime_fingerprint(after)


def _watcher_running(app: Any) -> bool:
    try:
        from providers.scrobble import watch_manager as wm
        st = wm.status(app) or {}
    except Exception:
        return False
    try:
        for g in (st.get("groups") or []):
            if isinstance(g, dict) and g.get("running"):
                return True
        for r in (st.get("routes") or []):
            if isinstance(r, dict) and r.get("running"):
                return True
    except Exception:
        pass
    return False


def _apply_watch_runtime(app: Any, cfg: dict[str, Any], watcher_was_running: bool) -> dict[str, Any]:
    out: dict[str, Any] = {}
    try:
        from providers.scrobble import watch_manager as wm
    except Exception as e:
        WATCH_LOG.error(f"watcher runtime refresh unavailable: {type(e).__name__}: {e}")
        return {"watcher_reload_error": "watch_manager_unavailable"}
    try:
        if not source_enabled(cfg, "watcher"):
            wm.stop_all(app, wait=False)
            out["watcher_stopped"] = True
            WATCH_LOG.info("Watcher source disabled; stopped watcher runtime")
        else:
            autostart = bool(((cfg.get("scrobble") or {}).get("watch") or {}).get("autostart"))
            if watcher_was_running or autostart:
                wm.start_from_config(app)
                out["watcher_reloaded"] = True
                WATCH_LOG.info("Watcher routing changed; refreshed watcher runtime")
    except Exception as e:
        out["watcher_reload_error"] = "watcher_reload_failed"
        WATCH_LOG.error(f"watcher runtime refresh failed: {type(e).__name__}: {e}")
    return out


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


def _pre_upgrade_backup_label(cfg: dict[str, Any]) -> str:
    ui = cfg.get("ui") if isinstance(cfg.get("ui"), dict) else {}
    pending = _norm_ver((ui or {}).get("_pending_upgrade_from_version")) if isinstance(ui, dict) else ""
    stored = _norm_ver(cfg.get("version"))
    version = pending or stored or "unknown"
    return f"pre-upgrade version {version}"


def _set_cfg_version_current(env: dict[str, Any], cfg: dict[str, Any]) -> None:
    try:
        base = env.get("cfg_base")
        cur_fn = getattr(base, "_current_version_norm", None) if base is not None else None
        if callable(cur_fn):
            cfg["version"] = str(cur_fn() or "").strip()
            return
    except Exception:
        pass

    try:
        from api.versionAPI import CURRENT_VERSION as _V
        raw = str(_V or "").strip()
    except Exception:
        raw = str(os.getenv("APP_VERSION") or "").strip()

    cfg["version"] = _norm_ver(raw)


@router.get("/config/meta")
def api_config_meta(request: Request) -> JSONResponse:
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

    autogen = False
    try:
        ui = raw.get("ui") if isinstance(raw, dict) else None
        if isinstance(ui, dict):
            autogen = bool(ui.get("_autogen"))
    except Exception:
        autogen = False

    auth_reset_required = False
    try:
        app_auth = raw.get("app_auth") if isinstance(raw, dict) else None
        password = app_auth.get("password") if isinstance(app_auth, dict) else None
        manual_disable_with_existing_credentials = bool(
            isinstance(app_auth, dict)
            and not bool(app_auth.get("enabled"))
            and str(app_auth.get("username") or "").strip()
            and isinstance(password, dict)
            and str(password.get("hash") or "").strip()
            and str(password.get("salt") or "").strip()
        )
        if isinstance(app_auth, dict):
            auth_reset_required = bool(app_auth.get("reset_required")) or manual_disable_with_existing_credentials
    except Exception:
        auth_reset_required = False

    auth_configured = False
    try:
        app_auth = raw.get("app_auth") if isinstance(raw, dict) else None
        password = app_auth.get("password") if isinstance(app_auth, dict) else None
        auth_configured = bool(
            isinstance(app_auth, dict)
            and not auth_reset_required
            and str(app_auth.get("username") or "").strip()
            and isinstance(password, dict)
            and str(password.get("hash") or "").strip()
            and str(password.get("salt") or "").strip()
        )
    except Exception:
        auth_configured = False

    if auth_configured:
        autogen = False

    first_run = (not exists) or autogen

    cfg_ver = _norm_ver(raw.get("version") if isinstance(raw, dict) else None) or None
    pending_upgrade_from_ver = None
    try:
        ui = raw.get("ui") if isinstance(raw, dict) else None
        if isinstance(ui, dict):
            pending_upgrade_from_ver = _norm_ver(ui.get("_pending_upgrade_from_version")) or None
    except Exception:
        pending_upgrade_from_ver = None
    effective_cfg_ver = pending_upgrade_from_ver or cfg_ver
    try:
        from api.versionAPI import CURRENT_VERSION as _V
        cur_ver = _norm_ver(str(_V))
    except Exception:
        cur_ver = _norm_ver(os.getenv("APP_VERSION") or "v0.7.0")

    needs_upgrade = False
    is_legacy_pre_070 = False
    try:
        needs_upgrade = _safe_ver(cur_ver) > _safe_ver(effective_cfg_ver)
        is_legacy_pre_070 = _safe_ver(effective_cfg_ver) < Version("0.7.0")
    except Exception:
        needs_upgrade = False
        is_legacy_pre_070 = False

    auth_setup_required = bool(auth_reset_required or not auth_configured)
    auth_reset_deferred_to_upgrade = bool(needs_upgrade and auth_reset_required)
    setup_wizard_required = bool(auth_setup_required and not needs_upgrade)

    mtime = None
    size = None
    if exists and p is not None:
        try:
            st = p.stat()
            size = int(st.st_size)
            mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat()
        except Exception:
            pass

    authenticated = False
    try:
        from . import appAuthAPI as app_auth

        token = request.cookies.get(app_auth.COOKIE_NAME)
        authenticated = app_auth.auth_required(raw) and app_auth.is_authenticated(raw, token)
    except Exception:
        authenticated = False

    payload = {
        "exists": exists,
        "first_run": first_run,
        "autogen": autogen,
        "auth_configured": auth_configured,
        "auth_setup_required": auth_setup_required,
        "auth_reset_required": auth_reset_required,
        "auth_reset_deferred_to_upgrade": auth_reset_deferred_to_upgrade,
        "setup_wizard_required": setup_wizard_required,
        "current_version": cur_ver,
        "config_version": effective_cfg_ver,
        "stored_config_version": cfg_ver,
        "pending_upgrade_from_version": pending_upgrade_from_ver,
        "needs_upgrade": needs_upgrade,
        "legacy_pre_070": is_legacy_pre_070,
    }
    if authenticated:
        payload.update(
            {
                "path": str(p) if p is not None else None,
                "size": size,
                "mtime": mtime,
            }
        )

    return _nostore(JSONResponse(payload))

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
def api_config_save(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
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

        if k.startswith("profile:"):
            return True

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
            # Webhook URL tokens (security.webhook_ids.*)
            "webhook_ids", "webhook_id",
            "plextrakt", "jellyfintrakt", "embytrakt", "plexwatcher",
        }
        if k in exact:
            return True

        # Catch token
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
    watch_feat["enabled"] = bool(source_enabled(cfg, "watcher") and sc.get("watch", {}).get("autostart", False))

    try:
        env["prune"](cfg)
        for p in (cfg.get("pairs") or []):
            try: env["norm_pair"](p)
            except Exception: pass
    except Exception:
        pass

    # Setup-wizard marker: saved config then clear any auto-generated flag.
    try:
        ui = cfg.get("ui")
        if isinstance(ui, dict):
            ui.pop("_autogen", None)
            ui.pop("_pending_upgrade_from_version", None)
            _set_cfg_version_current(env, cfg)
    except Exception:
        pass

    # Detect watcher-routing changes
    watch_runtime_changed = _watch_runtime_changed(current, cfg)
    watcher_was_running = _watcher_running(request.app) if watch_runtime_changed else False

    env["save"](cfg)

    _after_config_save(env, cfg)

    result: dict[str, Any] = {"ok": True}
    if watch_runtime_changed:
        try:
            result.update(_apply_watch_runtime(request.app, cfg, watcher_was_running))
        except Exception as e:
            result["watcher_reload_error"] = "watcher_reload_failed"
            WATCH_LOG.error(f"watcher runtime refresh failed: {type(e).__name__}: {e}")
    return result

@router.post("/config/migrate")
def api_config_migrate() -> dict[str, Any]:
    env = _env()
    base = env.get("cfg_base")
    if base is None:
        return {"ok": False, "error": "Configuration backend unavailable"}

    current = dict(env["load"]() or {})
    backup_path = None
    forced_paths: list[str] = []

    try:
        try:
            if not hasattr(base, "config_path"):
                raise RuntimeError("config backend has no file path")
            from services.backups import create_backup

            backup_label = _pre_upgrade_backup_label(current)
            BACKUP_LOG.info(f"creating {backup_label} backup")
            backup_result = create_backup(scope="config_only", label=backup_label, trigger="upgrade")
            backup_path = str(backup_result.get("path") or "")
            BACKUP_LOG.success(f"pre-upgrade backup created path={backup_path or 'backup created'}")
        except Exception as e:
            BACKUP_LOG.debug(f"pre-upgrade backup service unavailable: {type(e).__name__}")
            backup = getattr(base, "backup_config_file", None)
            if callable(backup):
                BACKUP_LOG.debug("pre-upgrade backup falling back to legacy config backup")
                legacy_backup = backup()
                if legacy_backup is not None:
                    backup_path = str(legacy_backup)
                    BACKUP_LOG.success("legacy pre-upgrade config backup created")
            if not backup_path:
                BACKUP_LOG.warn("pre-upgrade backup was not created")
    except Exception:
        BACKUP_LOG.error("pre-upgrade backup failed")
        return {"ok": False, "error": "config_backup_failed"}

    cfg: dict[str, Any] = dict(current or {})

    try:
        apply = getattr(base, "apply_migration_overrides", None)
        if callable(apply):
            result = apply(cfg)
            if isinstance(result, tuple) and len(result) == 2:
                next_cfg, next_paths = result
                if isinstance(next_cfg, dict):
                    cfg = next_cfg
                if isinstance(next_paths, list):
                    forced_paths = [str(path) for path in next_paths]
    except Exception:
        return {"ok": False, "error": "migration_overrides_failed"}

    sc = cfg.setdefault("scrobble", {})
    sc_enabled = bool(sc.get("enabled", False))
    mode = str(sc.get("mode") or "").strip().lower()
    if mode not in {"webhook","watch"}:
        legacy_webhook = bool((cfg.get("webhook") or {}).get("enabled"))
        mode = "webhook" if legacy_webhook else ("watch" if sc_enabled else "")
        if mode:
            sc["mode"] = mode
    if mode == "webhook":
        sc.setdefault("watch", {}).setdefault("autostart", bool(sc.get("watch", {}).get("autostart", False)))
    elif mode != "watch":
        sc["enabled"] = False

    features = cfg.setdefault("features", {})
    watch_feat = features.setdefault("watch", {})
    watch_feat["enabled"] = bool(source_enabled(cfg, "watcher") and sc.get("watch", {}).get("autostart", False))

    try:
        env["prune"](cfg)
        env["ensure"](cfg)
        for p in (cfg.get("pairs") or []):
            try:
                env["norm_pair"](p)
            except Exception:
                pass
    except Exception:
        pass

    try:
        ui = cfg.get("ui")
        if isinstance(ui, dict):
            ui.pop("_autogen", None)
            ui.pop("_pending_upgrade_from_version", None)
            _set_cfg_version_current(env, cfg)
    except Exception:
        pass

    try:
        env["save"](cfg)
    except Exception:
        return {"ok": False, "error": "config_save_failed"}

    _after_config_save(env, cfg)

    return {
        "ok": True,
        "backup": backup_path,
        "forced_paths": forced_paths,
    }
