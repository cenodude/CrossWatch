# /api/anime-mapping
# CrossWatch - Anime Mapping API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

from _logging import log

from cw_platform.anime_mapping.auto_update import refresh_from_config as refresh_auto_update
from cw_platform.anime_mapping.auto_update import status as auto_update_status
from cw_platform.anime_mapping.storage import normalize_release_tag, rebuild_sqlite_from_mappings
from cw_platform.anime_mapping.updater import status as mapping_status, update as mapping_update
from cw_platform.config_base import load_config, save_config

router = APIRouter(prefix="/api/anime-mapping", tags=["anime-mapping"])


def _client_error_message(action: str) -> str:
    return f"Anime Mapping {action} failed. Check the server logs for details."


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
            block["use_for_pairs"] = providers or ["anilist"]

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
                "use_for_pairs": ",".join(_provider_list(block.get("use_for_pairs")) or ["anilist"]),
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
