# /api/anime-mapping
# CrossWatch - Anime Mapping API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
from typing import Any

from fastapi import APIRouter, Body, Path as PathParam, Response
from fastapi.responses import JSONResponse

from _logging import log

from cw_platform.anime_mapping.auto_update import refresh_from_config as refresh_auto_update
from cw_platform.anime_mapping.auto_update import status as auto_update_status
from cw_platform.anime_mapping.overrides import (
    IMPORT_MODES,
    MATCH_PROVIDERS,
    MEDIA_TYPES,
    TARGET_NAMESPACES,
    OverrideError,
    delete_override,
    export_payload,
    import_overrides,
    load_overrides,
    safe_override_error,
    stats as override_stats,
    upsert_override,
)
from cw_platform.anime_mapping.simkl_catalog import (
    SimklCatalogError,
    SimklNotConfigured,
    configured as simkl_configured,
    instances as simkl_instances,
    plan_rules as simkl_plan_rules,
    resolve_instance as simkl_resolve_instance,
    search as simkl_search,
)
from cw_platform.anime_mapping.storage import normalize_release_tag, rebuild_sqlite_from_mappings
from cw_platform.anime_mapping.updater import status as mapping_status, update as mapping_update
from cw_platform.config_base import ANIME_MAPPING_PAIRS_DEFAULT, load_config, save_config

router = APIRouter(prefix="/api/anime-mapping", tags=["anime-mapping"])


def _client_error_message(action: str) -> str:
    return f"Anime Mapping {action} failed. Check the server logs for details."


def _simkl_not_configured_message() -> str:
    return "SIMKL lookup is not configured. Connect SIMKL and try again."


def _release_tag(payload: dict[str, Any] | None = None) -> str:
    cfg = load_config() or {}
    data: dict[str, Any] = payload if isinstance(payload, dict) else {}
    raw_block = cfg.get("anime_mapping")
    block: dict[str, Any] = raw_block if isinstance(raw_block, dict) else {}
    return normalize_release_tag(data.get("release_tag") or block.get("release_tag"))


def _int_at_least(value: Any, default: int, minimum: int) -> int:
    try:
        return max(minimum, int(value))
    except Exception:
        return default


def _provider_list(value: Any) -> list[str]:
    if isinstance(value, str):
        raw = value.replace(",", " ").split()
    elif isinstance(value, (list, tuple, set)):
        raw = list(value)
    else:
        raw = []
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        name = str(item or "").strip().lower()
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


@router.get("/status")
def api_anime_mapping_status() -> JSONResponse:
    cfg = load_config() or {}
    st = mapping_status(cfg=cfg)
    st["auto_update_status"] = auto_update_status()
    return JSONResponse(st)


@router.post("/settings")
def api_anime_mapping_settings(payload: dict[str, Any] | None = Body(default=None)) -> JSONResponse:
    try:
        data = payload or {}
        cfg = load_config() or {}
        block = cfg.get("anime_mapping") if isinstance(cfg.get("anime_mapping"), dict) else {}
        block = dict(block or {})

        if "enabled" in data:
            block["enabled"] = bool(data.get("enabled"))
        if "auto_update" in data:
            block["auto_update"] = bool(data.get("auto_update"))
        if "provider" in data:
            provider = str(data.get("provider") or "anibridge").strip().lower() or "anibridge"
            block["provider"] = provider
        if "release_tag" in data:
            block["release_tag"] = normalize_release_tag(data.get("release_tag"))
        if "refresh_hours" in data:
            block["refresh_hours"] = _int_at_least(data.get("refresh_hours"), 24, 1)
        if "stale_after_days" in data:
            block["stale_after_days"] = _int_at_least(data.get("stale_after_days"), 14, 1)
        if "use_for_pairs" in data:
            providers = _provider_list(data.get("use_for_pairs"))
            block["use_for_pairs"] = providers or list(ANIME_MAPPING_PAIRS_DEFAULT)

        cfg["anime_mapping"] = block
        save_config(cfg)
        log(
            "settings_saved",
            level="debug",
            module="ANIME_MAPPING",
            extra={
                "enabled": bool(block.get("enabled", False)),
                "auto_update": bool(block.get("auto_update", True)),
                "release_tag": str(block.get("release_tag") or "v3"),
                "use_for_pairs": ",".join(_provider_list(block.get("use_for_pairs")) or ANIME_MAPPING_PAIRS_DEFAULT),
            },
        )
        st = mapping_status(cfg=cfg)
        bootstrap = None
        bootstrap_error = ""
        if bool(block.get("enabled", False)) and not bool(st.get("installed") and st.get("index_ready")):
            try:
                log(
                    "bootstrap_started",
                    level="debug",
                    module="ANIME_MAPPING",
                    extra={
                        "release_tag": str(block.get("release_tag") or "v3"),
                        "reason": "enabled_missing_index",
                    },
                )
                bootstrap = mapping_update(release_tag=str(block.get("release_tag") or "v3"), force=False)
                st = mapping_status(cfg=cfg)
                log(
                    "bootstrap_finished",
                    level="debug",
                    module="ANIME_MAPPING",
                    extra={
                        "release_tag": str(block.get("release_tag") or "v3"),
                        "installed": bool(st.get("installed")),
                        "index_ready": bool(st.get("index_ready")),
                    },
                )
            except Exception as boot_e:
                bootstrap_error = _client_error_message("bootstrap")
                st["error"] = boot_e.__class__.__name__
                st["message"] = bootstrap_error
                log(
                    "bootstrap_failed",
                    level="error",
                    module="ANIME_MAPPING",
                    extra={
                        "release_tag": str(block.get("release_tag") or "v3"),
                        "error_type": boot_e.__class__.__name__,
                        "error": str(boot_e),
                    },
                )
        try:
            refresh_auto_update(load_config)
        except Exception as sched_e:
            log(
                "auto_update_refresh_failed",
                level="error",
                module="ANIME_MAPPING",
                extra={"error_type": sched_e.__class__.__name__, "error": str(sched_e)},
            )
            st["auto_update_error"] = _client_error_message("auto-update refresh")
        st["auto_update_status"] = auto_update_status()
        return JSONResponse(
            {
                "ok": True,
                "anime_mapping": block,
                "status": st,
                "bootstrap": bootstrap,
                "bootstrap_error": bootstrap_error,
            }
        )
    except Exception as e:
        log(
            "settings_failed",
            level="error",
            module="ANIME_MAPPING",
            extra={"error_type": e.__class__.__name__, "error": str(e)},
        )
        return JSONResponse({"ok": False, "error": e.__class__.__name__, "message": _client_error_message("settings update")}, status_code=500)


@router.post("/update")
def api_anime_mapping_update(payload: dict[str, Any] | None = Body(default=None)) -> JSONResponse:
    try:
        tag = _release_tag(payload)
        force = bool((payload or {}).get("force", False))
        res = mapping_update(release_tag=tag, force=force)
        res["auto_update_status"] = auto_update_status()
        return JSONResponse(res)
    except Exception as e:
        log(
            "manual_update_failed",
            level="error",
            module="ANIME_MAPPING",
            extra={"error_type": e.__class__.__name__, "error": str(e)},
        )
        return JSONResponse({"ok": False, "error": e.__class__.__name__, "message": _client_error_message("manual update")}, status_code=500)


@router.post("/rebuild-index")
def api_anime_mapping_rebuild_index(payload: dict[str, Any] | None = Body(default=None)) -> JSONResponse:
    try:
        tag = _release_tag(payload)
        log("manual_rebuild_requested", level="debug", module="ANIME_MAPPING", extra={"release_tag": tag})
        res = rebuild_sqlite_from_mappings(release_tag=tag)
        cfg = load_config() or {}
        log(
            "manual_rebuild_finished",
            level="debug",
            module="ANIME_MAPPING",
            extra={
                "release_tag": tag,
                "source_count": int(res.get("source_count") or 0),
                "edge_count": int(res.get("edge_count") or 0),
            },
        )
        return JSONResponse({"ok": True, "rebuild": res, "status": mapping_status(cfg=cfg), "auto_update_status": auto_update_status()})
    except Exception as e:
        log(
            "manual_rebuild_failed",
            level="error",
            module="ANIME_MAPPING",
            extra={"error_type": e.__class__.__name__, "error": str(e)},
        )
        return JSONResponse({"ok": False, "error": e.__class__.__name__, "message": _client_error_message("index rebuild")}, status_code=500)


@router.get("/overrides")
def api_anime_mapping_overrides_list() -> JSONResponse:
    try:
        return JSONResponse(
            {
                "ok": True,
                "overrides": load_overrides(),
                "stats": override_stats(),
                "schema": {
                    "match_providers": list(MATCH_PROVIDERS),
                    "target_namespaces": list(TARGET_NAMESPACES),
                    "media_types": list(MEDIA_TYPES),
                },
            }
        )
    except Exception as e:
        log(
            "overrides_list_failed",
            level="error",
            module="ANIME_MAPPING",
            extra={"error_type": e.__class__.__name__, "error": str(e)},
        )
        return JSONResponse({"ok": False, "error": e.__class__.__name__, "message": _client_error_message("override list")}, status_code=500)


@router.get("/simkl/status")
def api_anime_mapping_simkl_status(instance: str = "") -> JSONResponse:
    resolved, key = simkl_resolve_instance(instance or None)
    return JSONResponse(
        {
            "ok": True,
            "configured": bool(key),
            "instance": resolved if key else "",
            "instances": simkl_instances(),
        }
    )


@router.get("/simkl/search")
def api_anime_mapping_simkl_search(q: str = "", limit: int = 25, instance: str = "") -> JSONResponse:
    term = str(q or "").strip()
    if not term:
        return JSONResponse({"ok": True, "results": [], "configured": bool(simkl_configured(instance or None))})
    try:
        results = simkl_search(term, limit=limit, instance_id=instance or None)
    except SimklNotConfigured as e:
        log(
            "simkl_search_not_configured",
            level="warn",
            module="ANIME_MAPPING",
            extra={"error_type": e.__class__.__name__, "error": str(e)},
        )
        return JSONResponse(
            {"ok": False, "error": "simkl_not_configured", "message": _simkl_not_configured_message()},
            status_code=409,
        )
    except SimklCatalogError as e:
        log(
            "simkl_search_failed",
            level="warn",
            module="ANIME_MAPPING",
            extra={"error_type": e.__class__.__name__, "error": str(e)},
        )
        return JSONResponse(
            {"ok": False, "error": "simkl_search_failed", "message": _client_error_message("SIMKL search")},
            status_code=502,
        )
    return JSONResponse({"ok": True, "configured": True, "results": [r.as_dict() for r in results]})


@router.post("/simkl/plan")
def api_anime_mapping_simkl_plan(payload: dict[str, Any] | None = Body(default=None)) -> JSONResponse:
    data = payload if isinstance(payload, dict) else {}
    try:
        plan = simkl_plan_rules(
            data.get("simkl"),
            match_provider=str(data.get("match_provider") or ""),
            match_id=str(data.get("match_id") or ""),
            match_season=data.get("match_season"),
            title=str(data.get("title") or ""),
            instance_id=str(data.get("instance") or "") or None,
        )
    except SimklNotConfigured as e:
        log(
            "simkl_plan_not_configured",
            level="warn",
            module="ANIME_MAPPING",
            extra={"error_type": e.__class__.__name__, "error": str(e)},
        )
        return JSONResponse(
            {"ok": False, "error": "simkl_not_configured", "message": _simkl_not_configured_message()},
            status_code=409,
        )
    except SimklCatalogError as e:
        log(
            "simkl_plan_failed",
            level="warn",
            module="ANIME_MAPPING",
            extra={"error_type": e.__class__.__name__, "error": str(e)},
        )
        return JSONResponse(
            {"ok": False, "error": "simkl_plan_failed", "message": _client_error_message("SIMKL season plan")},
            status_code=400,
        )
    except Exception as e:
        log(
            "simkl_plan_failed",
            level="error",
            module="ANIME_MAPPING",
            extra={"error_type": e.__class__.__name__, "error": str(e)},
        )
        return JSONResponse({"ok": False, "error": e.__class__.__name__, "message": _client_error_message("season plan")}, status_code=500)
    log(
        "simkl_plan_built",
        level="debug",
        module="ANIME_MAPPING",
        extra={"rules": len(plan.get("rules") or []), "episodes": plan.get("total_episodes")},
    )
    return JSONResponse({"ok": True, **plan})


@router.get("/overrides/export")
def api_anime_mapping_overrides_export() -> Response:
    try:
        payload = export_payload()
    except Exception as e:
        log(
            "overrides_export_failed",
            level="error",
            module="ANIME_MAPPING",
            extra={"error_type": e.__class__.__name__, "error": str(e)},
        )
        return JSONResponse({"ok": False, "error": e.__class__.__name__, "message": _client_error_message("override export")}, status_code=500)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    log("overrides_exported", level="debug", module="ANIME_MAPPING", extra={"rules": len(payload.get("overrides") or [])})
    return Response(
        content=json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="crosswatch-anime-mappings-{stamp}.json"'},
    )


@router.post("/overrides/import")
def api_anime_mapping_overrides_import(payload: dict[str, Any] | None = Body(default=None)) -> JSONResponse:
    data = payload if isinstance(payload, dict) else {}
    mode = str(data.get("mode") or "merge").strip().lower()
    if mode not in IMPORT_MODES:
        return JSONResponse({"ok": False, "error": "invalid_mode", "message": "mode must be 'merge' or 'replace'"}, status_code=400)
    source = data.get("payload") if "payload" in data else data
    try:
        result = import_overrides(source, mode=mode)
    except OverrideError as e:
        return JSONResponse({"ok": False, "error": "invalid_import", "message": safe_override_error(e)}, status_code=400)
    except Exception as e:
        log(
            "overrides_import_failed",
            level="error",
            module="ANIME_MAPPING",
            extra={"error_type": e.__class__.__name__, "error": str(e)},
        )
        return JSONResponse({"ok": False, "error": e.__class__.__name__, "message": _client_error_message("override import")}, status_code=500)
    log(
        "overrides_imported",
        level="debug",
        module="ANIME_MAPPING",
        extra={
            "mode": result.get("mode"),
            "added": result.get("added"),
            "updated": result.get("updated"),
            "skipped": result.get("skipped_count"),
            "total": result.get("total"),
        },
    )
    return JSONResponse({"ok": True, "result": result, "overrides": load_overrides(), "stats": override_stats()})


@router.post("/overrides")
def api_anime_mapping_overrides_save(payload: dict[str, Any] | None = Body(default=None)) -> JSONResponse:
    try:
        saved = upsert_override(payload or {})
    except OverrideError as e:
        return JSONResponse({"ok": False, "error": "invalid_rule", "message": safe_override_error(e)}, status_code=400)
    except Exception as e:
        log(
            "overrides_save_failed",
            level="error",
            module="ANIME_MAPPING",
            extra={"error_type": e.__class__.__name__, "error": str(e)},
        )
        return JSONResponse({"ok": False, "error": e.__class__.__name__, "message": _client_error_message("override save")}, status_code=500)
    log(
        "override_saved",
        level="debug",
        module="ANIME_MAPPING",
        extra={
            "rule": saved.get("id"),
            "match": f"{saved.get('match_provider')}:{saved.get('match_id')}",
            "target": f"{saved.get('target_namespace')}:{saved.get('target_id')}",
        },
    )
    return JSONResponse({"ok": True, "override": saved, "overrides": load_overrides(), "stats": override_stats()})


@router.delete("/overrides/{rule_id}")
def api_anime_mapping_overrides_delete(rule_id: str = PathParam(...)) -> JSONResponse:
    try:
        removed = delete_override(rule_id)
    except Exception as e:
        log(
            "overrides_delete_failed",
            level="error",
            module="ANIME_MAPPING",
            extra={"error_type": e.__class__.__name__, "error": str(e)},
        )
        return JSONResponse({"ok": False, "error": e.__class__.__name__, "message": _client_error_message("override delete")}, status_code=500)
    if not removed:
        return JSONResponse({"ok": False, "error": "not_found", "message": "That rule no longer exists."}, status_code=404)
    log("override_deleted", level="debug", module="ANIME_MAPPING", extra={"rule": rule_id})
    return JSONResponse({"ok": True, "overrides": load_overrides(), "stats": override_stats()})
