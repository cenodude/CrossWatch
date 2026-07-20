# /api/playlistsAPI.py
# CrossWatch - Playlists API (endpoints, mapping profiles, preview, run)
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Path as FPath, Query
from fastapi.responses import JSONResponse

from cw_platform.config_base import load_config
from services import playlists as svc

router = APIRouter(prefix="/api/playlists", tags=["playlists"])


@router.get("/providers")
def api_playlist_providers() -> JSONResponse:
    cfg = load_config() or {}
    return JSONResponse({"ok": True, "providers": svc.list_playlist_providers(cfg)})


@router.get("/resources")
def api_playlist_resources(
    provider: str = Query(...),
    instance: str | None = Query(None),
) -> JSONResponse:
    cfg = load_config() or {}
    res = svc.list_resources(cfg, provider, instance)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.get("/overview")
def api_playlist_overview() -> JSONResponse:
    cfg = load_config() or {}
    return JSONResponse(svc.overview(cfg))


# Endpoints

@router.get("/endpoints")
def api_playlist_endpoints() -> JSONResponse:
    cfg = load_config() or {}
    return JSONResponse({"ok": True, "endpoints": svc.list_endpoints(cfg)})


@router.post("/endpoints")
def api_playlist_endpoint_upsert(payload: dict[str, Any] = Body(...)) -> JSONResponse:
    cfg = load_config() or {}
    res = svc.upsert_endpoint(cfg, payload)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.delete("/endpoints/{endpoint_id}")
def api_playlist_endpoint_delete(endpoint_id: str = FPath(...)) -> JSONResponse:
    cfg = load_config() or {}
    res = svc.delete_endpoint(cfg, endpoint_id)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.post("/endpoints/{endpoint_id}/sync")
def api_playlist_endpoint_sync(endpoint_id: str = FPath(...)) -> JSONResponse:
    cfg = load_config() or {}
    res = svc.sync_endpoint(cfg, endpoint_id)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.get("/activity")
def api_playlist_activity() -> JSONResponse:
    cfg = load_config() or {}
    return JSONResponse({"ok": True, "activity": svc.activity(cfg)})


@router.get("/rulesets")
def api_playlist_rulesets() -> JSONResponse:
    cfg = load_config() or {}
    return JSONResponse({"ok": True, "rulesets": svc.list_rulesets(cfg)})


@router.get("/rulesets/{ruleset_id}")
def api_playlist_ruleset_get(ruleset_id: str = FPath(...)) -> JSONResponse:
    cfg = load_config() or {}
    rs = svc.get_ruleset(cfg, ruleset_id)
    res = {"ok": bool(rs), "ruleset": rs, "error": None if rs else "ruleset not found"}
    return JSONResponse(res, status_code=(200 if rs else 404))


@router.post("/rulesets")
def api_playlist_ruleset_upsert(payload: dict[str, Any] = Body(...)) -> JSONResponse:
    cfg = load_config() or {}
    res = svc.upsert_ruleset(cfg, payload.get("ruleset") or payload)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.post("/rulesets/validate")
def api_playlist_ruleset_validate(payload: dict[str, Any] = Body(...)) -> JSONResponse:
    res = svc.validate_ruleset_payload(payload.get("ruleset") or payload)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.post("/rulesets/{ruleset_id}/clone")
def api_playlist_ruleset_clone(ruleset_id: str = FPath(...), payload: dict[str, Any] | None = Body(default=None)) -> JSONResponse:
    cfg = load_config() or {}
    res = svc.clone_ruleset(cfg, ruleset_id, payload or {})
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.delete("/rulesets/{ruleset_id}")
def api_playlist_ruleset_delete(ruleset_id: str = FPath(...)) -> JSONResponse:
    cfg = load_config() or {}
    res = svc.delete_ruleset(cfg, ruleset_id)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


# Mapping profiles

@router.get("/mappings")
def api_playlist_mappings() -> JSONResponse:
    cfg = load_config() or {}
    return JSONResponse({"ok": True, "mappings": svc.list_mappings(cfg)})


@router.post("/mappings")
def api_playlist_mapping_upsert(payload: dict[str, Any] = Body(...)) -> JSONResponse:
    cfg = load_config() or {}
    res = svc.upsert_mapping(cfg, payload.get("mapping") or payload)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.delete("/mappings/{mapping_id}")
def api_playlist_mapping_delete(mapping_id: str = FPath(...)) -> JSONResponse:
    cfg = load_config() or {}
    res = svc.delete_mapping(cfg, mapping_id)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.post("/mappings/{mapping_id}/preview")
def api_playlist_mapping_preview(mapping_id: str = FPath(...)) -> JSONResponse:
    cfg = load_config() or {}
    res = svc.preview_mapping(cfg, mapping_id)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.post("/mappings/{mapping_id}/run")
def api_playlist_mapping_run(
    mapping_id: str = FPath(...),
    dry_run: bool = Query(False),
) -> JSONResponse:
    cfg = load_config() or {}
    res = svc.run_mapping(cfg, mapping_id, dry_run=dry_run)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.get("/mappings/{mapping_id}/result")
def api_playlist_mapping_result(mapping_id: str = FPath(...)) -> JSONResponse:
    cfg = load_config() or {}
    res = svc.latest_result(cfg, mapping_id)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


# Pair-facing: compatible + available mappings for a given pair

@router.get("/pairs/{pair_id}/mappings")
def api_playlist_pair_mappings(pair_id: str = FPath(...)) -> JSONResponse:
    cfg = load_config() or {}
    res = svc.mappings_for_pair(cfg, pair_id)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))
