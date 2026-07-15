# /api/provider_guard.py
# CrossWatch - Shared deletion guard for providers referenced by Scrobbling
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

from fastapi.responses import JSONResponse

from cw_platform.provider_usage import find_provider_usage, usage_conflict_payload

__all__ = ["usage_conflict_response"]


def usage_conflict_response(
    cfg: dict[str, Any],
    provider: str,
    instance_id: Any = "default",
) -> JSONResponse | None:
    usages = find_provider_usage(cfg, provider, instance_id)
    if not usages:
        return None
    return JSONResponse(usage_conflict_payload(provider, instance_id, usages), status_code=409)
