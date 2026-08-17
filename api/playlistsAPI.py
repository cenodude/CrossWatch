# /api/playlistsAPI.py
# CrossWatch - Playlists API (endpoints, mapping profiles, preview, run)
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, cast

from fastapi import APIRouter, Body, Path as FPath, Query, Request
from fastapi.responses import JSONResponse

from cw_platform.access_policy import request_user, user_can_access_instance, user_can_access_pair
from cw_platform.config_base import load_config
from cw_platform import playlists_runner as runner
from services import playlists as svc

router = APIRouter(prefix="/api/playlists", tags=["playlists"])


def _scope_denied() -> JSONResponse:
    return JSONResponse({"ok": False, "error": "profile_scope_denied"}, status_code=403)


def _is_admin_request(request: Request | None) -> bool:
    user = request_user(request)
    return not user or bool(user.get("is_admin"))


def _endpoint_allowed(cfg: dict[str, Any], request: Request | None, endpoint: dict[str, Any] | None) -> bool:
    if _is_admin_request(request):
        return True
    if not endpoint:
        return False
    return user_can_access_instance(cfg, request_user(request), endpoint.get("provider"), endpoint.get("instance") or "default")


def _mapping_allowed(cfg: dict[str, Any], request: Request | None, mapping: dict[str, Any] | None) -> bool:
    if _is_admin_request(request):
        return True
    if not mapping:
        return False
    source = str(mapping.get("source_endpoint") or "").strip()
    targets = mapping.get("target_endpoints")
    target_ids = [str(x).strip() for x in targets if str(x).strip()] if isinstance(targets, list) else []
    if not source or not target_ids:
        return False
    return _endpoint_allowed(cfg, request, runner.get_endpoint(cfg, source)) and all(
        _endpoint_allowed(cfg, request, runner.get_endpoint(cfg, tid)) for tid in target_ids
    )


def _filter_endpoints(cfg: dict[str, Any], request: Request | None, endpoints: list[dict[str, Any]]) -> list[dict[str, Any]]:
    user = request_user(request)
    if not user or bool(user.get("is_admin")):
        return endpoints
    return [endpoint for endpoint in endpoints if _endpoint_allowed(cfg, request, endpoint)]


def _filter_mappings(cfg: dict[str, Any], request: Request | None, mappings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    user = request_user(request)
    if not user or bool(user.get("is_admin")):
        return mappings
    return [mapping for mapping in mappings if _mapping_allowed(cfg, request, mapping)]


@router.get("/providers")
def api_playlist_providers(request: Request = cast(Request, None)) -> JSONResponse:
    cfg = load_config() or {}
    providers = [
        item for item in svc.list_playlist_providers(cfg)
        if user_can_access_instance(cfg, request_user(request), item.get("provider"), item.get("instance") or "default")
    ]
    return JSONResponse({"ok": True, "providers": providers})


@router.get("/resources")
def api_playlist_resources(
    provider: str = Query(...),
    instance: str | None = Query(None),
    request: Request = cast(Request, None),
) -> JSONResponse:
    cfg = load_config() or {}
    if not user_can_access_instance(cfg, request_user(request), provider, instance or "default"):
        return _scope_denied()
    res = svc.list_resources(cfg, provider, instance)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.get("/overview")
def api_playlist_overview(request: Request = cast(Request, None)) -> JSONResponse:
    cfg = load_config() or {}
    user = request_user(request)
    if user and not bool(user.get("is_admin")):
        mappings = _filter_mappings(cfg, request, svc.list_mappings(cfg))
        endpoints = _filter_endpoints(cfg, request, svc.list_endpoints(cfg))
        enabled = [m for m in mappings if m.get("enabled") and m.get("assigned_pair")]
        last_sync = 0
        unresolved = 0
        warnings: list[str] = []
        for mapping in mappings:
            res = mapping.get("last_result") or {}
            if isinstance(res, dict):
                last_sync = max(last_sync, int(res.get("finished_at") or 0))
                unresolved += int(res.get("unresolved_count") or 0)
                for warning in res.get("warnings") or []:
                    warnings.append(str(warning))
        return JSONResponse({
            "ok": True,
            "total_mappings": len(mappings),
            "enabled_mappings": len(enabled),
            "endpoints": len(endpoints),
            "last_sync_epoch": last_sync or None,
            "unresolved": unresolved,
            "warnings": sorted(set(warnings)),
        })
    return JSONResponse(svc.overview(cfg))


# Endpoints

@router.get("/endpoints")
def api_playlist_endpoints(request: Request = cast(Request, None)) -> JSONResponse:
    cfg = load_config() or {}
    return JSONResponse({"ok": True, "endpoints": _filter_endpoints(cfg, request, svc.list_endpoints(cfg))})


@router.post("/endpoints")
def api_playlist_endpoint_upsert(payload: dict[str, Any] = Body(...), request: Request = cast(Request, None)) -> JSONResponse:
    cfg = load_config() or {}
    if not user_can_access_instance(cfg, request_user(request), payload.get("provider"), payload.get("instance") or "default"):
        return _scope_denied()
    eid = str(payload.get("id") or "").strip()
    if eid and not _endpoint_allowed(cfg, request, runner.get_endpoint(cfg, eid)):
        return _scope_denied()
    res = svc.upsert_endpoint(cfg, payload)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.delete("/endpoints/{endpoint_id}")
def api_playlist_endpoint_delete(endpoint_id: str = FPath(...), request: Request = cast(Request, None)) -> JSONResponse:
    cfg = load_config() or {}
    if not _endpoint_allowed(cfg, request, runner.get_endpoint(cfg, endpoint_id)):
        return _scope_denied()
    res = svc.delete_endpoint(cfg, endpoint_id)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.post("/endpoints/{endpoint_id}/sync")
def api_playlist_endpoint_sync(endpoint_id: str = FPath(...), request: Request = cast(Request, None)) -> JSONResponse:
    cfg = load_config() or {}
    if not _endpoint_allowed(cfg, request, runner.get_endpoint(cfg, endpoint_id)):
        return _scope_denied()
    res = svc.sync_endpoint(cfg, endpoint_id)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.get("/activity")
def api_playlist_activity(request: Request = cast(Request, None)) -> JSONResponse:
    cfg = load_config() or {}
    user = request_user(request)
    if user and not bool(user.get("is_admin")):
        allowed_labels = {
            str(endpoint.get("provider") or "").upper()
            for endpoint in _filter_endpoints(cfg, request, svc.list_endpoints(cfg))
        }
        activity = [
            row for row in svc.activity(cfg)
            if any(label and label in str(row.get("label") or "").upper() for label in allowed_labels)
        ]
        return JSONResponse({"ok": True, "activity": activity})
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
def api_playlist_mappings(request: Request = cast(Request, None)) -> JSONResponse:
    cfg = load_config() or {}
    return JSONResponse({"ok": True, "mappings": _filter_mappings(cfg, request, svc.list_mappings(cfg))})


@router.post("/mappings")
def api_playlist_mapping_upsert(payload: dict[str, Any] = Body(...), request: Request = cast(Request, None)) -> JSONResponse:
    cfg = load_config() or {}
    mapping_payload = payload.get("mapping") or payload
    if not isinstance(mapping_payload, dict) or not _mapping_allowed(cfg, request, mapping_payload):
        return _scope_denied()
    mid = str(mapping_payload.get("id") or "").strip()
    if mid and not _mapping_allowed(cfg, request, svc.get_mapping(cfg, mid)):
        return _scope_denied()
    res = svc.upsert_mapping(cfg, mapping_payload)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.delete("/mappings/{mapping_id}")
def api_playlist_mapping_delete(mapping_id: str = FPath(...), request: Request = cast(Request, None)) -> JSONResponse:
    cfg = load_config() or {}
    if not _mapping_allowed(cfg, request, svc.get_mapping(cfg, mapping_id)):
        return _scope_denied()
    res = svc.delete_mapping(cfg, mapping_id)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.post("/mappings/{mapping_id}/preview")
def api_playlist_mapping_preview(mapping_id: str = FPath(...), request: Request = cast(Request, None)) -> JSONResponse:
    cfg = load_config() or {}
    if not _mapping_allowed(cfg, request, svc.get_mapping(cfg, mapping_id)):
        return _scope_denied()
    res = svc.preview_mapping(cfg, mapping_id)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.post("/mappings/{mapping_id}/run")
def api_playlist_mapping_run(
    mapping_id: str = FPath(...),
    dry_run: bool = Query(False),
    request: Request = cast(Request, None),
) -> JSONResponse:
    cfg = load_config() or {}
    if not _mapping_allowed(cfg, request, svc.get_mapping(cfg, mapping_id)):
        return _scope_denied()
    res = svc.run_mapping(cfg, mapping_id, dry_run=dry_run)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


@router.get("/mappings/{mapping_id}/result")
def api_playlist_mapping_result(mapping_id: str = FPath(...), request: Request = cast(Request, None)) -> JSONResponse:
    cfg = load_config() or {}
    if not _mapping_allowed(cfg, request, svc.get_mapping(cfg, mapping_id)):
        return _scope_denied()
    res = svc.latest_result(cfg, mapping_id)
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))


# Pair-facing: compatible + available mappings for a given pair

@router.get("/pairs/{pair_id}/mappings")
def api_playlist_pair_mappings(pair_id: str = FPath(...), request: Request = cast(Request, None)) -> JSONResponse:
    cfg = load_config() or {}
    pair = next((pair for pair in cfg.get("pairs") or [] if isinstance(pair, dict) and str(pair.get("id") or "") == str(pair_id)), None)
    if not user_can_access_pair(cfg, request_user(request), pair or {}):
        return _scope_denied()
    res = svc.mappings_for_pair(cfg, pair_id)
    mappings = res.get("mappings")
    if res.get("ok") and isinstance(mappings, list):
        res["mappings"] = _filter_mappings(cfg, request, [m for m in mappings if isinstance(m, dict)])
    return JSONResponse(res, status_code=(200 if res.get("ok") else 400))
