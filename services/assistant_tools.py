# services/assistant_tools.py
# CrossWatch - Shared read-only tools for built-in chat and MCP
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException, Request
from services import assistant_state
from services import assistant_diagnostics

TOOLS = {
    **assistant_state.TOOLS,
    **assistant_diagnostics.TOOLS,
    "list_anime_overrides": (
        "Read user-created Anime ID custom mappings from Settings > Metadata > Anime ID Mapping > Custom mappings. "
        "Includes enabled and disabled rules, source and target IDs, season and episode offsets. "
        "These are separate from Editor saved corrections and the downloaded anime database. Results are paginated.",
        {"type": "object", "properties": {
            "q": {"type": "string", "maxLength": 256},
            "offset": {"type": "integer", "minimum": 0},
            "limit": {"type": "integer", "minimum": 1, "maximum": 25},
        }},
    ),
    "list_saved_mappings": (
        "Read the Editor Mappings & blocks overview. Lists saved corrections or blocked items, with media IDs, "
        "source account, feature and shared or pair-specific scope. Call once for each entry_type to see both. "
        "Results are paginated; total is the number matching the filters, not just this page.",
        {"type": "object", "properties": {
            "entry_type": {"type": "string", "enum": ["mapping", "block"]},
            "q": {"type": "string", "maxLength": 256},
            "provider": {"type": "string"}, "instance": {"type": "string"},
            "feature": {"type": "string"}, "pair_id": {"type": "string", "description": "Pair ID, shared for all-pairs rules, or empty for all scopes."},
            "offset": {"type": "integer", "minimum": 0},
            "limit": {"type": "integer", "minimum": 1, "maximum": 25},
        }, "required": ["entry_type"]},
    ),
}


def run(name: str, request: Request, args: dict[str, Any]) -> Any:
    if name in assistant_diagnostics.TOOLS:
        return assistant_diagnostics.run(name, request, args)
    if name in assistant_state.TOOLS:
        return assistant_state.run(name, request, args)
    if name == "list_anime_overrides":
        assistant_diagnostics.admin(request)
        from cw_platform.anime_mapping.overrides import load_overrides

        try:
            offset = max(0, int(args.get("offset", 0)))
            limit = max(1, min(25, int(args.get("limit", 10))))
        except (TypeError, ValueError, OverflowError):
            return "offset and limit must be integers."
        query = str(args.get("q") or "")[:256].strip().casefold()
        rows = [row for row in load_overrides() if not query or query in json.dumps(row, ensure_ascii=False).casefold()]
        return {"ok": True, "items": rows[offset:offset + limit], "total": len(rows), "offset": offset, "limit": limit}
    if name == "list_saved_mappings":
        from api.editorAPI import api_saved_mappings

        entry_type = args.get("entry_type")
        if entry_type not in ("mapping", "block"):
            return "Choose entry_type mapping or block."
        try:
            offset = max(0, int(args.get("offset", 0)))
            limit = max(1, min(25, int(args.get("limit", 10))))
        except (TypeError, ValueError, OverflowError):
            return "offset and limit must be integers."
        filters = {key: str(args.get(key) or "")[:256] for key in ("q", "provider", "instance", "feature", "pair_id")}
        try:
            result = api_saved_mappings(request, **filters, entry_type=entry_type, offset=offset, limit=limit, user_profile="")
        except HTTPException as exc:
            return str(exc.detail)
        return result
    raise HTTPException(400, "Unknown tool")
