# /api/healthAPI.py
# Small API for health checks.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

__all__ = ["router"]

router = APIRouter(tags=["health"])


def _health_payload() -> dict[str, object]:
    return {
        "ok": True,
        "status": "ok",
    }


def _scheduler_status() -> dict[str, Any]:
    import crosswatch

    scheduler = crosswatch.scheduler
    if scheduler is None:
        raise RuntimeError("scheduler unavailable")
    return scheduler.status()


def _sync_state() -> tuple[bool, dict[str, Any]]:
    from api.syncAPI import _epoch_seconds, _is_sync_running, _summary_snapshot_for_response

    running = bool(_is_sync_running())
    snap = _summary_snapshot_for_response() or {}
    return running, {
        "running": running,
        "finished_at": _epoch_seconds(snap.get("finished_at")) or 0,
        "exit_code": snap.get("exit_code"),
    }


def _visible_tokens(cfg: dict[str, Any], request: Request) -> list[dict[str, Any]]:
    from api.apiTokensAPI import list_api_tokens

    user = getattr(request.state, "cw_user", None)
    tokens = list_api_tokens(cfg)
    if isinstance(user, dict) and user.get("api_token_access") == "monitor":
        own = str(user.get("api_token_id") or "")
        return [token for token in tokens if own and str(token.get("id") or "") == own]
    if isinstance(user, dict) and user.get("monitor_view"):
        own = str(user.get("id") or "")
        return [token for token in tokens if own and str(token.get("user_id") or "") == own]
    return tokens


@router.get("/api/health")
def api_health() -> JSONResponse:
    return JSONResponse(_health_payload(), headers={"Cache-Control": "no-store"})


@router.get("/api/health/details")
def api_health_details(request: Request, strict: int = Query(0)) -> JSONResponse:
    from cw_platform.config_base import load_config
    from services.health_details import health_details

    cfg = load_config() or {}
    payload = health_details(
        request.app,
        cfg,
        scheduler_status=_scheduler_status,
        sync_state=_sync_state,
        tokens=lambda: _visible_tokens(cfg, request),
    )
    status_code = 503 if strict and payload["status"] == "failing" else 200
    return JSONResponse(payload, status_code=status_code, headers={"Cache-Control": "no-store"})


@router.get("/healthz", include_in_schema=False)
def healthz() -> JSONResponse:
    return JSONResponse(_health_payload(), headers={"Cache-Control": "no-store"})
