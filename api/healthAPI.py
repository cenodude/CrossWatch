# /api/healthAPI.py
# Small API for health checks.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse

__all__ = ["router"]

router = APIRouter(tags=["health"])


def _health_payload() -> dict[str, object]:
    return {
        "ok": True,
        "status": "ok",
    }


@router.get("/api/health")
def api_health() -> JSONResponse:
    return JSONResponse(_health_payload(), headers={"Cache-Control": "no-store"})


@router.get("/healthz", include_in_schema=False)
def healthz() -> JSONResponse:
    return JSONResponse(_health_payload(), headers={"Cache-Control": "no-store"})
